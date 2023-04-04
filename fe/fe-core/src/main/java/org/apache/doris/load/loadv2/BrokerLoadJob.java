// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.load.loadv2;

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DataQualityException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.DuplicatedRequestException;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.QuotaExceedException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
import org.apache.doris.common.util.MetaLockUtils;
import org.apache.doris.common.util.ProfileManager;
import org.apache.doris.common.util.RuntimeProfile;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.load.BrokerFileGroup;
import org.apache.doris.load.BrokerFileGroupAggInfo.FileGroupAggKey;
import org.apache.doris.load.EtlJobType;
import org.apache.doris.load.FailMsg;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.BeginTransactionException;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TransactionState.TxnCoordinator;
import org.apache.doris.transaction.TransactionState.TxnSourceType;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.RejectedExecutionException;

/**
 * There are 3 steps in BrokerLoadJob: BrokerPendingTask, LoadLoadingTask, CommitAndPublishTxn.
 * Step1: BrokerPendingTask will be created on method of unprotectedExecuteJob.
 * Step2: LoadLoadingTasks will be created by the method of onTaskFinished when BrokerPendingTask is finished.
 * Step3: CommitAndPublicTxn will be called by the method of onTaskFinished when all of LoadLoadingTasks are finished.
 */
public class BrokerLoadJob extends BulkLoadJob {

    private static final Logger LOG = LogManager.getLogger(BrokerLoadJob.class);

    // Profile of this load job, including all tasks' profiles
    private RuntimeProfile jobProfile;
    // If set to true, the profile of load job with be pushed to ProfileManager
    private boolean enableProfile = false;

    // for log replay and unit test
    public BrokerLoadJob() {
        this(EtlJobType.BROKER);
    }

    public BrokerLoadJob(long dbId, String label, BrokerDesc brokerDesc, OriginStatement originStmt,
            UserIdentity userInfo) throws MetaNotFoundException {
        this(EtlJobType.BROKER, dbId, label, brokerDesc, originStmt, userInfo);
    }

    public BrokerLoadJob(EtlJobType type) {
        super(type);
    }

    public BrokerLoadJob(EtlJobType type, long dbId, String label, BrokerDesc brokerDesc,
            OriginStatement originStmt, UserIdentity userInfo)
            throws MetaNotFoundException {
        super(type, dbId, label, originStmt, userInfo);
        this.brokerDesc = brokerDesc;
        if (ConnectContext.get() != null && ConnectContext.get().getSessionVariable().enableProfile()) {
            enableProfile = true;
        }
    }

    @Override
    public void beginTxn()
            throws LabelAlreadyUsedException, BeginTransactionException, AnalysisException, DuplicatedRequestException,
            QuotaExceedException, MetaNotFoundException {
        transactionId = Env.getCurrentGlobalTransactionMgr()
                .beginTransaction(dbId, Lists.newArrayList(fileGroupAggInfo.getAllTableIds()), label, null,
                        new TxnCoordinator(TxnSourceType.FE, FrontendOptions.getLocalHostAddress()),
                        TransactionState.LoadJobSourceType.BATCH_LOAD_JOB, id,
                        getTimeout());
    }

    @Override
    protected void unprotectedExecuteJob() {
        LoadTask task = createPendingTask();
        idToTasks.put(task.getSignature(), task);
        Env.getCurrentEnv().getPendingLoadTaskScheduler().submit(task);
    }

    protected LoadTask createPendingTask() {
        return new BrokerLoadPendingTask(this, fileGroupAggInfo.getAggKeyToFileGroups(), brokerDesc);
    }

    /**
     * Situation1: When attachment is instance of BrokerPendingTaskAttachment,
     * this method is called by broker pending task.
     * LoadLoadingTask will be created after BrokerPendingTask is finished.
     * Situation2: When attachment is instance of BrokerLoadingTaskAttachment, this method is called by LoadLoadingTask.
     * CommitTxn will be called after all of LoadingTasks are finished.
     *
     * @param attachment
     */
    @Override
    public void onTaskFinished(TaskAttachment attachment) {
        if (attachment instanceof BrokerPendingTaskAttachment) {
            onPendingTaskFinished((BrokerPendingTaskAttachment) attachment);
        } else if (attachment instanceof BrokerLoadingTaskAttachment) {
            onLoadingTaskFinished((BrokerLoadingTaskAttachment) attachment);
        }
    }

    /**
     * step1: divide job into loading task
     * step2: init the plan of task
     * step3: submit tasks into loadingTaskExecutor
     * @param attachment BrokerPendingTaskAttachment
     */
    private void onPendingTaskFinished(BrokerPendingTaskAttachment attachment) {
        writeLock();
        try {
            // check if job has been cancelled
            if (isTxnDone()) {
                LOG.warn(new LogBuilder(LogKey.LOAD_JOB, id)
                        .add("state", state)
                        .add("error_msg", "this task will be ignored when job is: " + state)
                        .build());
                return;
            }

            if (finishedTaskIds.contains(attachment.getTaskId())) {
                LOG.warn(new LogBuilder(LogKey.LOAD_JOB, id)
                        .add("task_id", attachment.getTaskId())
                        .add("error_msg", "this is a duplicated callback of pending task "
                                + "when broker already has loading task")
                        .build());
                return;
            }

            // add task id into finishedTaskIds
            finishedTaskIds.add(attachment.getTaskId());
        } finally {
            writeUnlock();
        }

        try {
            Database db = getDb();
            createLoadingTask(db, attachment);
        } catch (UserException e) {
            LOG.warn(new LogBuilder(LogKey.LOAD_JOB, id)
                    .add("database_id", dbId)
                    .add("error_msg", "Failed to divide job into loading task.")
                    .build(), e);
            cancelJobWithoutCheck(new FailMsg(FailMsg.CancelType.ETL_RUN_FAIL, e.getMessage()), true, true);
            return;
        } catch (RejectedExecutionException e) {
            LOG.warn(new LogBuilder(LogKey.LOAD_JOB, id)
                    .add("database_id", dbId)
                    .add("error_msg", "the task queque is full.")
                    .build(), e);
            cancelJobWithoutCheck(new FailMsg(FailMsg.CancelType.ETL_RUN_FAIL, e.getMessage()), true, true);
            return;
        }

        loadStartTimestamp = System.currentTimeMillis();
    }

    private void createLoadingTask(Database db, BrokerPendingTaskAttachment attachment) throws UserException {
        List<Table> tableList = db.getTablesOnIdOrderOrThrowException(
                Lists.newArrayList(fileGroupAggInfo.getAllTableIds()));
        // divide job into broker loading task by table
        List<LoadLoadingTask> newLoadingTasks = Lists.newArrayList();
        this.jobProfile = new RuntimeProfile("BrokerLoadJob " + id + ". " + label);
        MetaLockUtils.readLockTables(tableList);
        try {
            for (Map.Entry<FileGroupAggKey, List<BrokerFileGroup>> entry
                    : fileGroupAggInfo.getAggKeyToFileGroups().entrySet()) {
                FileGroupAggKey aggKey = entry.getKey();
                List<BrokerFileGroup> brokerFileGroups = entry.getValue();
                long tableId = aggKey.getTableId();
                OlapTable table = (OlapTable) db.getTableNullable(tableId);
                // Generate loading task and init the plan of task
                LoadLoadingTask task = new LoadLoadingTask(db, table, brokerDesc,
                        brokerFileGroups, getDeadlineMs(), getExecMemLimit(),
                        isStrictMode(), transactionId, this, getTimeZone(), getTimeout(),
                        getLoadParallelism(), getSendBatchParallelism(),
                        getMaxFilterRatio() <= 0, enableProfile ? jobProfile : null, isSingleTabletLoadPerSink(),
                        useNewLoadScanNode());

                UUID uuid = UUID.randomUUID();
                TUniqueId loadId = new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());

                LOG.info("loadId={}, BrokerLoadJobId={}, transactionId={}",
                        DebugUtil.printId(loadId), this.id, this.transactionId);
                if (Config.isNotCloudMode()) {
                    task.init(loadId, attachment.getFileStatusByTable(aggKey),
                            attachment.getFileNumByTable(aggKey), getUserInfo());
                } else {
                    if (Strings.isNullOrEmpty(clusterId)) {
                        throw new UserException("can not get a valid cluster");
                    }
                    task.init(loadId, attachment.getFileStatusByTable(aggKey),
                            attachment.getFileNumByTable(aggKey), getUserInfo(), clusterId);
                }
                idToTasks.put(task.getSignature(), task);
                // idToTasks contains previous LoadPendingTasks, so idToTasks is just used to save all tasks.
                // use newLoadingTasks to save new created loading tasks and submit them later.
                newLoadingTasks.add(task);
                // load id will be added to loadStatistic when executing this task

                // save all related tables and rollups in transaction state
                try {
                    Env.getCurrentGlobalTransactionMgr().addTableIndexes(db.getId(), transactionId, table);
                } catch (UserException e) {
                    throw new UserException(e.getMessage());
                }
            }
        } finally {
            MetaLockUtils.readUnlockTables(tableList);
        }
        // Submit task outside the database lock, cause it may take a while if task queue is full.
        for (LoadTask loadTask : newLoadingTasks) {
            Env.getCurrentEnv().getLoadingLoadTaskScheduler().submit(loadTask);
        }
    }

    private void onLoadingTaskFinished(BrokerLoadingTaskAttachment attachment) {
        writeLock();
        try {
            // check if job has been cancelled
            if (isTxnDone()) {
                LOG.warn(new LogBuilder(LogKey.LOAD_JOB, id)
                        .add("state", state)
                        .add("error_msg", "this task will be ignored when job is: " + state)
                        .build());
                return;
            }

            // check if task has been finished
            if (finishedTaskIds.contains(attachment.getTaskId())) {
                LOG.warn(new LogBuilder(LogKey.LOAD_JOB, id)
                        .add("task_id", attachment.getTaskId())
                        .add("error_msg", "this is a duplicated callback of loading task").build());
                return;
            }

            // update loading status
            finishedTaskIds.add(attachment.getTaskId());
            updateLoadingStatus(attachment);

            // begin commit txn when all of loading tasks have been finished
            if (finishedTaskIds.size() != idToTasks.size()) {
                return;
            }
        } finally {
            writeUnlock();
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug(new LogBuilder(LogKey.LOAD_JOB, id)
                    .add("commit_infos", Joiner.on(",").join(commitInfos))
                    .build());
        }

        // check data quality
        if (!checkDataQuality()) {
            cancelJobWithoutCheck(new FailMsg(FailMsg.CancelType.ETL_QUALITY_UNSATISFIED,
                            DataQualityException.QUALITY_FAIL_MSG), true, true);
            return;
        }
        Database db = null;
        List<Table> tableList = null;
        try {
            db = getDb();
            tableList = db.getTablesOnIdOrderOrThrowException(Lists.newArrayList(fileGroupAggInfo.getAllTableIds()));
            MetaLockUtils.writeLockTablesOrMetaException(tableList);
        } catch (MetaNotFoundException e) {
            LOG.warn(new LogBuilder(LogKey.LOAD_JOB, id)
                    .add("database_id", dbId)
                    .add("error_msg", "db has been deleted when job is loading")
                    .build(), e);
            cancelJobWithoutCheck(new FailMsg(FailMsg.CancelType.LOAD_RUN_FAIL, e.getMessage()), true, true);
            return;
        }
        try {
            LOG.info(new LogBuilder(LogKey.LOAD_JOB, id)
                    .add("txn_id", transactionId)
                    .add("msg", "Load job try to commit txn")
                    .build());
            Env.getCurrentGlobalTransactionMgr().commitTransaction(
                    dbId, tableList, transactionId, commitInfos, getLoadJobFinalOperation());
            afterCommit();
        } catch (UserException e) {
            LOG.warn(new LogBuilder(LogKey.LOAD_JOB, id)
                    .add("database_id", dbId)
                    .add("error_msg", "Failed to commit txn with error:" + e.getMessage())
                    .build(), e);

            String msg = Config.isCloudMode() ? e.getInternalMsg() : e.getMessage();
            cancelJobWithoutCheck(new FailMsg(FailMsg.CancelType.LOAD_RUN_FAIL, msg), true, true);
        } finally {
            MetaLockUtils.writeUnlockTables(tableList);
        }
    }

    protected LoadJobFinalOperation getLoadJobFinalOperation() {
        return new LoadJobFinalOperation(id, loadingStatus, progress, loadStartTimestamp, finishTimestamp, state,
                failMsg);
    }

    protected void afterCommit() throws DdlException {}

    private void writeProfile() {
        if (!enableProfile) {
            return;
        }

        RuntimeProfile summaryProfile = new RuntimeProfile("Summary");
        summaryProfile.addInfoString(ProfileManager.QUERY_ID, String.valueOf(id));
        summaryProfile.addInfoString(ProfileManager.START_TIME, TimeUtils.longToTimeString(createTimestamp));
        summaryProfile.addInfoString(ProfileManager.END_TIME, TimeUtils.longToTimeString(finishTimestamp));
        summaryProfile.addInfoString(ProfileManager.TOTAL_TIME,
                DebugUtil.getPrettyStringMs(finishTimestamp - createTimestamp));

        summaryProfile.addInfoString(ProfileManager.QUERY_TYPE, "Load");
        summaryProfile.addInfoString(ProfileManager.QUERY_STATE, "N/A");
        summaryProfile.addInfoString(ProfileManager.USER, "N/A");
        summaryProfile.addInfoString(ProfileManager.DEFAULT_DB, "N/A");
        summaryProfile.addInfoString(ProfileManager.SQL_STATEMENT, "N/A");
        summaryProfile.addInfoString(ProfileManager.IS_CACHED, "N/A");

        // Add the summary profile to the first
        jobProfile.addFirstChild(summaryProfile);
        jobProfile.computeTimeInChildProfile();
        ProfileManager.getInstance().pushProfile(jobProfile);
    }

    private void updateLoadingStatus(BrokerLoadingTaskAttachment attachment) {
        loadingStatus.replaceCounter(DPP_ABNORMAL_ALL,
                increaseCounter(DPP_ABNORMAL_ALL, attachment.getCounter(DPP_ABNORMAL_ALL)));
        loadingStatus.replaceCounter(DPP_NORMAL_ALL,
                increaseCounter(DPP_NORMAL_ALL, attachment.getCounter(DPP_NORMAL_ALL)));
        loadingStatus.replaceCounter(UNSELECTED_ROWS,
                increaseCounter(UNSELECTED_ROWS, attachment.getCounter(UNSELECTED_ROWS)));
        if (attachment.getTrackingUrl() != null) {
            loadingStatus.setTrackingUrl(attachment.getTrackingUrl());
        }
        commitInfos.addAll(attachment.getCommitInfoList());
        errorTabletInfos.addAll(attachment.getErrorTabletInfos());

        progress = (int) ((double) finishedTaskIds.size() / idToTasks.size() * 100);
        if (progress == 100) {
            progress = 99;
        }
    }

    @Override
    public void updateProgress(Long beId, TUniqueId loadId, TUniqueId fragmentId, long scannedRows,
                               long scannedBytes, boolean isDone) {
        super.updateProgress(beId, loadId, fragmentId, scannedRows, scannedBytes, isDone);
        progress = (int) ((double) loadStatistic.getLoadBytes() / loadStatistic.totalFileSizeB * 100);
        if (progress >= 100) {
            progress = 99;
        }
    }

    private String increaseCounter(String key, String deltaValue) {
        long value = 0;
        if (loadingStatus.getCounters().containsKey(key)) {
            value = Long.valueOf(loadingStatus.getCounters().get(key));
        }
        if (deltaValue != null) {
            value += Long.valueOf(deltaValue);
        }
        return String.valueOf(value);
    }

    @Override
    public void afterVisible(TransactionState txnState, boolean txnOperated) {
        super.afterVisible(txnState, txnOperated);
        writeProfile();
    }

    @Override
    public void onTaskFailed(long taskId, FailMsg failMsg) {
        if (!Config.isCloudMode() || Strings.isNullOrEmpty(this.clusterId)) {
            super.onTaskFailed(taskId, failMsg);
            return;
        }
        try {
            writeLock();
            if (isTxnDone()) {
                LOG.warn(new LogBuilder(LogKey.LOAD_JOB, id)
                        .add("label", label)
                        .add("transactionId", transactionId)
                        .add("state", state)
                        .add("error_msg", "this task will be ignored when job is: " + state)
                        .build());
                return;
            }
            LOG.info(new LogBuilder(LogKey.LOAD_JOB, id)
                    .add("label", label)
                    .add("transactionId", transactionId)
                    .add("state", state)
                    .add("retryTimes", retryTimes)
                    .add("failMsg", failMsg.getMsg())
                    .build());

            this.retryTimes--;
            if (this.retryTimes <= 0) {
                boolean abortTxn = this.transactionId > 0 ? true : false;
                unprotectedExecuteCancel(failMsg, abortTxn);
                logFinalOperation();
                return;
            } else {
                unprotectedExecuteRetry(failMsg);
            }
        } finally {
            writeUnlock();
        }

        boolean allTaskDone = false;
        while (!allTaskDone) {
            try {
                writeLock();
                // check if all task has been done
                // unprotectedExecuteRetry() will cancel all running task
                allTaskDone = true;
                for (Map.Entry<Long, LoadTask> entry : idToTasks.entrySet()) {
                    if (entry.getKey() != taskId && !entry.getValue().isDone()) {
                        LOG.info("LoadTask({}) has not been done", entry.getKey());
                        allTaskDone = false;
                    }
                }
            } finally {
                writeUnlock();
            }
            if (!allTaskDone) {
                try {
                    Thread.sleep(1000);
                    continue;
                } catch (InterruptedException e) {
                    LOG.warn("", e);
                }
            }
        }

        try {
            writeLock();
            this.state = JobState.PENDING;
            this.idToTasks.clear();
            this.failMsg = null;
            this.finishedTaskIds.clear();
            Env.getCurrentGlobalTransactionMgr().getCallbackFactory().addCallback(this);
            LoadTask task = createPendingTask();
            idToTasks.put(task.getSignature(), task);
            Env.getCurrentEnv().getPendingLoadTaskScheduler().submit(task);
        } finally {
            writeUnlock();
        }
    }
}
