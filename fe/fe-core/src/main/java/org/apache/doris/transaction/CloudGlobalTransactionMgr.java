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

package org.apache.doris.transaction;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DuplicatedRequestException;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.InternalErrorCode;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.MarkedCountDownLatch;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.QuotaExceedException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.MetaLockUtils;
import org.apache.doris.load.loadv2.LoadJobFinalOperation;
import org.apache.doris.load.routineload.RLTaskTxnCommitAttachment;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.persist.BatchRemoveTransactionsOperation;
import org.apache.doris.persist.BatchRemoveTransactionsOperationV2;
import org.apache.doris.persist.EditLog;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.service.FrontendServiceImpl;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.task.CalcDeleteBitmapTask;
import org.apache.doris.thrift.TCalcDeleteBitmapPartitionInfo;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TTaskType;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.thrift.TWaitingTxnStatusRequest;
import org.apache.doris.thrift.TWaitingTxnStatusResult;
import org.apache.doris.transaction.TransactionState.LoadJobSourceType;
import org.apache.doris.transaction.TransactionState.TxnCoordinator;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.selectdb.cloud.proto.SelectdbCloud.AbortTxnRequest;
import com.selectdb.cloud.proto.SelectdbCloud.AbortTxnResponse;
import com.selectdb.cloud.proto.SelectdbCloud.BeginTxnRequest;
import com.selectdb.cloud.proto.SelectdbCloud.BeginTxnResponse;
import com.selectdb.cloud.proto.SelectdbCloud.CheckTxnConflictRequest;
import com.selectdb.cloud.proto.SelectdbCloud.CheckTxnConflictResponse;
import com.selectdb.cloud.proto.SelectdbCloud.CleanTxnLabelRequest;
import com.selectdb.cloud.proto.SelectdbCloud.CleanTxnLabelResponse;
import com.selectdb.cloud.proto.SelectdbCloud.CommitTxnRequest;
import com.selectdb.cloud.proto.SelectdbCloud.CommitTxnResponse;
import com.selectdb.cloud.proto.SelectdbCloud.GetCurrentMaxTxnRequest;
import com.selectdb.cloud.proto.SelectdbCloud.GetCurrentMaxTxnResponse;
import com.selectdb.cloud.proto.SelectdbCloud.GetDeleteBitmapUpdateLockRequest;
import com.selectdb.cloud.proto.SelectdbCloud.GetDeleteBitmapUpdateLockResponse;
import com.selectdb.cloud.proto.SelectdbCloud.GetTxnRequest;
import com.selectdb.cloud.proto.SelectdbCloud.GetTxnResponse;
import com.selectdb.cloud.proto.SelectdbCloud.LoadJobSourceTypePB;
import com.selectdb.cloud.proto.SelectdbCloud.MetaServiceCode;
import com.selectdb.cloud.proto.SelectdbCloud.TxnInfoPB;
import com.selectdb.cloud.proto.SelectdbCloud.UniqueIdPB;
import com.selectdb.cloud.rpc.MetaServiceProxy;
import com.selectdb.cloud.transaction.TxnUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class CloudGlobalTransactionMgr implements GlobalTransactionMgrInterface {
    private static final Logger LOG = LogManager.getLogger(CloudGlobalTransactionMgr.class);
    private static final int DELETE_BITMAP_LOCK_EXPIRATION_SECONDS = 10;
    private static final int CALCULATE_DELETE_BITMAP_TASK_TIMEOUT_SECONDS = 15;

    private Env env;

    private TxnStateCallbackFactory callbackFactory = new TxnStateCallbackFactory();

    public CloudGlobalTransactionMgr(Env env) {
        this.env = env;
    }

    public TxnStateCallbackFactory getCallbackFactory() {
        return callbackFactory;
    }

    @Override
    public void addDatabaseTransactionMgr(Long dbId) {
        //do nothing
    }

    @Override
    public void removeDatabaseTransactionMgr(Long dbId) {
        //do nothing
    }

    @Override
    public long beginTransaction(long dbId, List<Long> tableIdList, String label, TxnCoordinator coordinator,
            LoadJobSourceType sourceType, long timeoutSecond)
            throws AnalysisException, LabelAlreadyUsedException, BeginTransactionException, DuplicatedRequestException,
            QuotaExceedException, MetaNotFoundException {
        return beginTransaction(dbId, tableIdList, label, null, coordinator, sourceType, -1, timeoutSecond);
    }

    @Override
    public long beginTransaction(long dbId, List<Long> tableIdList, String label, TUniqueId requestId,
            TxnCoordinator coordinator, LoadJobSourceType sourceType, long listenerId, long timeoutSecond)
            throws AnalysisException, LabelAlreadyUsedException, BeginTransactionException, DuplicatedRequestException,
            QuotaExceedException, MetaNotFoundException {

        LOG.info("try to begin transaction, dbId: {}, label: {}", dbId, label);
        if (Config.disable_load_job) {
            throw new AnalysisException("disable_load_job is set to true, all load jobs are prevented");
        }

        switch (sourceType) {
            case BACKEND_STREAMING:
                checkValidTimeoutSecond(timeoutSecond, Config.max_stream_load_timeout_second,
                        Config.min_load_timeout_second);
                break;
            default:
                checkValidTimeoutSecond(timeoutSecond, Config.max_load_timeout_second, Config.min_load_timeout_second);
        }

        BeginTxnResponse beginTxnResponse = null;
        int retryTime = 0;

        try {
            Preconditions.checkNotNull(coordinator);
            Preconditions.checkNotNull(label);
            FeNameFormat.checkLabel(label);

            TxnInfoPB.Builder txnInfoBuilder = TxnInfoPB.newBuilder();
            txnInfoBuilder.setDbId(dbId);
            txnInfoBuilder.addAllTableIds(tableIdList);
            txnInfoBuilder.setLabel(label);
            txnInfoBuilder.setListenerId(listenerId);

            if (requestId != null) {
                UniqueIdPB.Builder uniqueIdBuilder = UniqueIdPB.newBuilder();
                uniqueIdBuilder.setHi(requestId.getHi());
                uniqueIdBuilder.setLo(requestId.getLo());
                txnInfoBuilder.setRequestId(uniqueIdBuilder);
            }

            txnInfoBuilder.setCoordinator(TxnUtil.txnCoordinatorToPb(coordinator));
            txnInfoBuilder.setLoadJobSourceType(LoadJobSourceTypePB.forNumber(sourceType.value()));
            txnInfoBuilder.setTimeoutMs(timeoutSecond * 1000);
            txnInfoBuilder.setPrecommitTimeoutMs(Config.stream_load_default_precommit_timeout_second * 1000);

            final BeginTxnRequest beginTxnRequest = BeginTxnRequest.newBuilder()
                    .setTxnInfo(txnInfoBuilder.build())
                    .setCloudUniqueId(Config.cloud_unique_id)
                    .build();

            while (retryTime < Config.metaServiceRpcRetryTimes()) {
                LOG.debug("retryTime:{}, beginTxnRequest:{}", retryTime, beginTxnRequest);
                beginTxnResponse = MetaServiceProxy.getInstance().beginTxn(beginTxnRequest);
                LOG.debug("retryTime:{}, beginTxnResponse:{}", retryTime, beginTxnResponse);

                if (beginTxnResponse.getStatus().getCode() != MetaServiceCode.KV_TXN_CONFLICT) {
                    break;
                }
                LOG.info("beginTxn KV_TXN_CONFLICT, retryTime:{}", retryTime);
                backoff();
                retryTime++;
                continue;
            }

            Preconditions.checkNotNull(beginTxnResponse);
            Preconditions.checkNotNull(beginTxnResponse.getStatus());
        } catch (Exception e) {
            LOG.warn("beginTxn failed, exception:", e);
            throw new BeginTransactionException("beginTxn failed, errMsg:" + e.getMessage());
        }

        if (beginTxnResponse.getStatus().getCode() != MetaServiceCode.OK) {
            switch (beginTxnResponse.getStatus().getCode()) {
                case TXN_DUPLICATED_REQ:
                    throw new DuplicatedRequestException(DebugUtil.printId(requestId),
                            beginTxnResponse.getDupTxnId(), beginTxnResponse.getStatus().getMsg());
                case TXN_LABEL_ALREADY_USED:
                    throw new LabelAlreadyUsedException(beginTxnResponse.getStatus().getMsg(), false);
                default:
                    if (MetricRepo.isInit) {
                        MetricRepo.COUNTER_TXN_REJECT.increase(1L);
                    }
                    throw new BeginTransactionException(beginTxnResponse.getStatus().getMsg());
            }
        }

        long txnId = beginTxnResponse.getTxnId();
        if (MetricRepo.isInit) {
            MetricRepo.COUNTER_TXN_BEGIN.increase(1L);
        }
        return txnId;
    }

    @Override
    public void preCommitTransaction2PC(Database db, List<Table> tableList, long transactionId,
            List<TabletCommitInfo> tabletCommitInfos, long timeoutMillis, TxnCommitAttachment txnCommitAttachment)
            throws UserException {
        Preconditions.checkState(false, "should not implement this in derived class");
    }

    @Override
    public void commitTransaction(long dbId, List<Table> tableList,
            long transactionId, List<TabletCommitInfo> tabletCommitInfos)
            throws UserException {
        commitTransaction(dbId, tableList, transactionId, tabletCommitInfos, null);
    }

    @Override
    public void commitTransaction(long dbId, List<Table> tableList, long transactionId,
            List<TabletCommitInfo> tabletCommitInfos, TxnCommitAttachment txnCommitAttachment)
            throws UserException {
        commitTransaction(dbId, tableList, transactionId, tabletCommitInfos, txnCommitAttachment, false);
    }

    private void commitTransaction(long dbId, List<Table> tableList, long transactionId,
            List<TabletCommitInfo> tabletCommitInfos, TxnCommitAttachment txnCommitAttachment, boolean is2PC)
            throws UserException {

        LOG.info("try to commit transaction, transactionId: {}", transactionId);
        if (Config.disable_load_job) {
            throw new TransactionCommitFailedException(
                    "disable_load_job is set to true, all load jobs are not allowed");
        }

        List<OlapTable> mowTableList = getMowTableList(tableList);
        if (tabletCommitInfos != null && !tabletCommitInfos.isEmpty() && !mowTableList.isEmpty()) {
            calcDeleteBitmapForMow(dbId, mowTableList, transactionId, tabletCommitInfos);
        }

        CommitTxnRequest.Builder builder = CommitTxnRequest.newBuilder();
        builder.setDbId(dbId)
                .setTxnId(transactionId)
                .setIs2Pc(is2PC)
                .setCloudUniqueId(Config.cloud_unique_id);

        // if tablet commit info is empty, no need to pass mowTableList to meta service.
        if (tabletCommitInfos != null && !tabletCommitInfos.isEmpty()) {
            for (OlapTable olapTable : mowTableList) {
                builder.addMowTableIds(olapTable.getId());
            }
        }

        if (txnCommitAttachment != null) {
            if (txnCommitAttachment instanceof LoadJobFinalOperation) {
                LoadJobFinalOperation loadJobFinalOperation = (LoadJobFinalOperation) txnCommitAttachment;
                builder.setCommitAttachment(TxnUtil
                        .loadJobFinalOperationToPb(loadJobFinalOperation));
            } else if (txnCommitAttachment instanceof RLTaskTxnCommitAttachment) {
                RLTaskTxnCommitAttachment rlTaskTxnCommitAttachment = (RLTaskTxnCommitAttachment) txnCommitAttachment;
                builder.setCommitAttachment(TxnUtil
                        .rlTaskTxnCommitAttachmentToPb(rlTaskTxnCommitAttachment));
            } else {
                throw new UserException("invalid txnCommitAttachment");
            }
        }

        final CommitTxnRequest commitTxnRequest = builder.build();
        CommitTxnResponse commitTxnResponse = null;
        int retryTime = 0;

        try {
            while (retryTime < Config.metaServiceRpcRetryTimes()) {
                LOG.debug("retryTime:{}, commitTxnRequest:{}", retryTime, commitTxnRequest);
                commitTxnResponse = MetaServiceProxy.getInstance().commitTxn(commitTxnRequest);
                LOG.debug("retryTime:{}, commitTxnResponse:{}", retryTime, commitTxnResponse);
                if (commitTxnResponse.getStatus().getCode() != MetaServiceCode.KV_TXN_CONFLICT) {
                    break;
                }
                // sleep random [20, 200] ms, avoid txn conflict
                LOG.info("commitTxn KV_TXN_CONFLICT, transactionId:{}, retryTime:{}", transactionId, retryTime);
                backoff();
                retryTime++;
                continue;
            }

            Preconditions.checkNotNull(commitTxnResponse);
            Preconditions.checkNotNull(commitTxnResponse.getStatus());
        } catch (Exception e) {
            LOG.warn("commitTxn failed, transactionId:{}, exception:", transactionId, e);
            throw new UserException("commitTxn() failed, errMsg:" + e.getMessage());
        }

        if (commitTxnResponse.getStatus().getCode() != MetaServiceCode.OK
                && commitTxnResponse.getStatus().getCode() != MetaServiceCode.TXN_ALREADY_VISIBLE) {
            LOG.warn("commitTxn failed, transactionId:{}, retryTime:{}, commitTxnResponse:{}",
                    transactionId, retryTime, commitTxnResponse);
            if (commitTxnResponse.getStatus().getCode() == MetaServiceCode.LOCK_EXPIRED) {
                // DELETE_BITMAP_LOCK_ERR will be retried on be
                throw new UserException(InternalErrorCode.DELETE_BITMAP_LOCK_ERR,
                        "delete bitmap update lock expired, transactionId:" + transactionId);
            }
            StringBuilder internalMsgBuilder =
                    new StringBuilder("commitTxn failed, transactionId:");
            internalMsgBuilder.append(transactionId);
            internalMsgBuilder.append(" code:");
            internalMsgBuilder.append(commitTxnResponse.getStatus().getCode());
            StringBuilder msgBuilder =
                    new StringBuilder("detail msg: " + commitTxnResponse.getStatus().getMsg());
            throw new UserException("internal error, try later, " + msgBuilder.toString(),
                                    internalMsgBuilder.toString());
        }

        TransactionState txnState = TxnUtil.transactionStateFromPb(commitTxnResponse.getTxnInfo());
        TxnStateChangeCallback cb = callbackFactory.getCallback(txnState.getCallbackId());
        if (cb != null) {
            LOG.info("commitTxn, run txn callback, transactionId:{} callbackId:{}, txnState:{}",
                    txnState.getTransactionId(), txnState.getCallbackId(), txnState);
            cb.afterCommitted(txnState, true);
            cb.afterVisible(txnState, true);
        }
        if (MetricRepo.isInit) {
            MetricRepo.COUNTER_TXN_SUCCESS.increase(1L);
            MetricRepo.HISTO_TXN_EXEC_LATENCY.update(txnState.getCommitTime() - txnState.getPrepareTime());
        }

        FrontendServiceImpl.commitTxnResp(commitTxnResponse);
    }

    private List<OlapTable> getMowTableList(List<Table> tableList) {
        List<OlapTable> mowTableList = new ArrayList<>();
        for (Table table : tableList) {
            if ((table instanceof OlapTable)) {
                OlapTable olapTable = (OlapTable) table;
                if (olapTable.getEnableUniqueKeyMergeOnWrite()) {
                    mowTableList.add(olapTable);
                }
            }
        }
        return mowTableList;
    }

    private void calcDeleteBitmapForMow(long dbId, List<OlapTable> tableList, long transactionId,
            List<TabletCommitInfo> tabletCommitInfos)
            throws UserException {
        Map<Long, Map<Long, List<Long>>> backendToPartitionTablets = Maps.newHashMap();
        Map<Long, Partition> partitions = Maps.newHashMap();
        Map<Long, Set<Long>> tableToPartitions = Maps.newHashMap();
        getPartitionInfo(tableList, tabletCommitInfos, tableToPartitions, partitions, backendToPartitionTablets);
        if (backendToPartitionTablets.isEmpty()) {
            throw new UserException("The partition info is empty, table may be dropped, txnid=" + transactionId);
        }

        getDeleteBitmapUpdateLock(tableToPartitions, transactionId);
        Map<Long, Long> partitionVersions = getPartitionVersions(partitions);

        Map<Long, List<TCalcDeleteBitmapPartitionInfo>> backendToPartitionInfos = getCalcDeleteBitmapInfo(
                backendToPartitionTablets, partitionVersions);
        sendCalcDeleteBitmaptask(dbId, transactionId, backendToPartitionInfos);
    }

    private void getPartitionInfo(List<OlapTable> tableList,
            List<TabletCommitInfo> tabletCommitInfos,
            Map<Long, Set<Long>> tableToParttions,
            Map<Long, Partition> partitions,
            Map<Long, Map<Long, List<Long>>> backendToPartitionTablets) {
        Map<Long, OlapTable> tableMap = Maps.newHashMap();
        for (OlapTable olapTable : tableList) {
            tableMap.put(olapTable.getId(), olapTable);
        }

        List<Long> tabletIds = tabletCommitInfos.stream()
                .map(TabletCommitInfo::getTabletId).collect(Collectors.toList());
        TabletInvertedIndex tabletInvertedIndex = env.getTabletInvertedIndex();
        List<TabletMeta> tabletMetaList = tabletInvertedIndex.getTabletMetaList(tabletIds);
        for (int i = 0; i < tabletMetaList.size(); i++) {
            TabletMeta tabletMeta = tabletMetaList.get(i);
            long tableId = tabletMeta.getTableId();
            if (!tableMap.containsKey(tableId)) {
                continue;
            }

            long partitionId = tabletMeta.getPartitionId();
            long backendId = tabletCommitInfos.get(i).getBackendId();

            if (!tableToParttions.containsKey(tableId)) {
                tableToParttions.put(tableId, Sets.newHashSet());
            }
            tableToParttions.get(tableId).add(partitionId);

            if (!backendToPartitionTablets.containsKey(backendId)) {
                backendToPartitionTablets.put(backendId, Maps.newHashMap());
            }
            Map<Long, List<Long>> partitionToTablets = backendToPartitionTablets.get(backendId);
            if (!partitionToTablets.containsKey(partitionId)) {
                partitionToTablets.put(partitionId, Lists.newArrayList());
            }
            partitionToTablets.get(partitionId).add(tabletIds.get(i));
            partitions.putIfAbsent(partitionId, tableMap.get(tableId).getPartition(partitionId));
        }
    }

    private Map<Long, Long> getPartitionVersions(Map<Long, Partition> partitionMap) {
        Map<Long, Long> partitionToVersions = Maps.newHashMap();
        partitionMap.forEach((key, value) -> {
            long visibleVersion = value.getVisibleVersion();
            long newVersion = visibleVersion <= 0 ? 2 : visibleVersion + 1;
            partitionToVersions.put(key, newVersion);
        });
        return partitionToVersions;
    }

    private Map<Long, List<TCalcDeleteBitmapPartitionInfo>> getCalcDeleteBitmapInfo(
            Map<Long, Map<Long, List<Long>>> backendToPartitionTablets, Map<Long, Long> partitionVersions) {
        Map<Long, List<TCalcDeleteBitmapPartitionInfo>> backendToPartitionInfos = Maps.newHashMap();
        for (Map.Entry<Long, Map<Long, List<Long>>> entry : backendToPartitionTablets.entrySet()) {
            List<TCalcDeleteBitmapPartitionInfo> partitionInfos = Lists.newArrayList();
            for (Map.Entry<Long, List<Long>> partitionToTables : entry.getValue().entrySet()) {
                Long partitionId = partitionToTables.getKey();
                TCalcDeleteBitmapPartitionInfo partitionInfo = new TCalcDeleteBitmapPartitionInfo(partitionId,
                        partitionVersions.get(partitionId),
                        partitionToTables.getValue());
                partitionInfos.add(partitionInfo);
            }
            backendToPartitionInfos.put(entry.getKey(), partitionInfos);
        }
        return backendToPartitionInfos;
    }

    private void getDeleteBitmapUpdateLock(Map<Long, Set<Long>> tableToParttions, long transactionId)
            throws UserException {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        for (Map.Entry<Long, Set<Long>> entry : tableToParttions.entrySet()) {
            GetDeleteBitmapUpdateLockRequest.Builder builder = GetDeleteBitmapUpdateLockRequest.newBuilder();
            builder.setTableId(entry.getKey())
                    .setLockId(transactionId)
                    .setInitiator(-1)
                    .setExpiration(DELETE_BITMAP_LOCK_EXPIRATION_SECONDS);
            final GetDeleteBitmapUpdateLockRequest request = builder.build();
            GetDeleteBitmapUpdateLockResponse response = null;

            int retryTime = 0;
            while (retryTime++ < Config.metaServiceRpcRetryTimes()) {
                try {
                    response = MetaServiceProxy.getInstance().getDeleteBitmapUpdateLock(request);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("get delete bitmap lock, transactionId={}, Request: {}, Response: {}",
                                transactionId, request, response);
                    }
                    if (response.getStatus().getCode() != MetaServiceCode.LOCK_CONFLICT
                            && response.getStatus().getCode() != MetaServiceCode.KV_TXN_CONFLICT) {
                        break;
                    }
                } catch (Exception e) {
                    LOG.warn("ignore get delete bitmap lock exception, transactionId={}, retryTime={}",
                            transactionId, retryTime, e);
                }
                // sleep random millis [20, 200] ms, avoid txn conflict
                int randomMillis = 20 + (int) (Math.random() * (200 - 20));
                if (LOG.isDebugEnabled()) {
                    LOG.debug("randomMillis:{}", randomMillis);
                }
                try {
                    Thread.sleep(randomMillis);
                } catch (InterruptedException e) {
                    LOG.info("InterruptedException: ", e);
                }
            }
            Preconditions.checkNotNull(response);
            Preconditions.checkNotNull(response.getStatus());
            if (response.getStatus().getCode() != MetaServiceCode.OK) {
                LOG.warn("get delete bitmap lock failed, transactionId={}, for {} times, response:{}",
                        transactionId, retryTime, response);
                if (response.getStatus().getCode() == MetaServiceCode.LOCK_CONFLICT
                        || response.getStatus().getCode() == MetaServiceCode.KV_TXN_CONFLICT) {
                    // DELETE_BITMAP_LOCK_ERR will be retried on be
                    throw new UserException(InternalErrorCode.DELETE_BITMAP_LOCK_ERR,
                            "Failed to get delete bitmap lock due to confilct");
                }
                throw new UserException("Failed to get delete bitmap lock, code: " + response.getStatus().getCode());
            }
        }
        stopWatch.stop();
        LOG.info("get delete bitmap lock successfully. txns: {}. time cost: {} ms.",
                 transactionId, stopWatch.getTime());
    }

    private void sendCalcDeleteBitmaptask(long dbId, long transactionId,
            Map<Long, List<TCalcDeleteBitmapPartitionInfo>> backendToPartitionInfos)
            throws UserException {
        if (backendToPartitionInfos.isEmpty()) {
            return;
        }
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        int totalTaskNum = backendToPartitionInfos.size();
        MarkedCountDownLatch<Long, Long> countDownLatch = new MarkedCountDownLatch<Long, Long>(
                totalTaskNum);
        AgentBatchTask batchTask = new AgentBatchTask();
        for (Map.Entry<Long, List<TCalcDeleteBitmapPartitionInfo>> entry : backendToPartitionInfos.entrySet()) {
            CalcDeleteBitmapTask task = new CalcDeleteBitmapTask(entry.getKey(),
                    transactionId,
                    dbId,
                    entry.getValue(),
                    countDownLatch);
            countDownLatch.addMark(entry.getKey(), transactionId);
            // add to AgentTaskQueue for handling finish report.
            // not check return value, because the add will success
            AgentTaskQueue.addTask(task);
            batchTask.addTask(task);
        }
        AgentTaskExecutor.submit(batchTask);

        boolean ok;
        try {
            ok = countDownLatch.await(CALCULATE_DELETE_BITMAP_TASK_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOG.warn("InterruptedException: ", e);
            ok = false;
        }

        if (!ok || !countDownLatch.getStatus().ok()) {
            String errMsg = "Failed to calculate delete bitmap.";
            // clear tasks
            AgentTaskQueue.removeBatchTask(batchTask, TTaskType.CALCULATE_DELETE_BITMAP);

            if (!countDownLatch.getStatus().ok()) {
                errMsg += countDownLatch.getStatus().getErrorMsg();
                if (countDownLatch.getStatus().getErrorCode() != TStatusCode.DELETE_BITMAP_LOCK_ERROR) {
                    throw new UserException(errMsg);
                }
            } else {
                errMsg += " Timeout.";
                List<Entry<Long, Long>> unfinishedMarks = countDownLatch.getLeftMarks();
                // only show at most 3 results
                List<Entry<Long, Long>> subList = unfinishedMarks.subList(0,
                        Math.min(unfinishedMarks.size(), 3));
                if (!subList.isEmpty()) {
                    errMsg += " Unfinished mark: " + Joiner.on(", ").join(subList);
                }
            }
            LOG.warn(errMsg);
            // DELETE_BITMAP_LOCK_ERR will be retried on be
            throw new UserException(InternalErrorCode.DELETE_BITMAP_LOCK_ERR, errMsg);
        }
        stopWatch.stop();
        LOG.info("calc delete bitmap task successfully. txns: {}. time cost: {} ms.",
                transactionId, stopWatch.getTime());
    }

    @Override
    public boolean commitAndPublishTransaction(DatabaseIf db, List<Table> tableList, long transactionId,
                                               List<TabletCommitInfo> tabletCommitInfos, long timeoutMillis)
            throws UserException {
        return commitAndPublishTransaction(db, tableList, transactionId, tabletCommitInfos, timeoutMillis, null);
    }

    @Override
    public boolean commitAndPublishTransaction(DatabaseIf db, List<Table> tableList, long transactionId,
                                               List<TabletCommitInfo> tabletCommitInfos, long timeoutMillis,
                                               TxnCommitAttachment txnCommitAttachment) throws UserException {
        if (!MetaLockUtils.tryCloudCommitLockTables(tableList, timeoutMillis, TimeUnit.MILLISECONDS)) {
            // DELETE_BITMAP_LOCK_ERR will be retried on be
            throw new UserException(InternalErrorCode.DELETE_BITMAP_LOCK_ERR,
                    "get table cloud commit lock timeout, tableList=("
                            + StringUtils.join(tableList, ",") + ")");
        }
        try {
            commitTransaction(db.getId(), tableList, transactionId, tabletCommitInfos, txnCommitAttachment);
        } finally {
            MetaLockUtils.cloudCommitUnlockTables(tableList);
        }
        return true;
    }

    @Override
    public void commitTransaction2PC(Database db, List<Table> tableList, long transactionId, long timeoutMillis)
            throws UserException {
        commitTransaction(db.getId(), tableList, transactionId, null, null, true);
    }

    @Override
    public void abortTransaction(long dbId, long transactionId, String reason) throws UserException {
        abortTransaction(dbId, transactionId, reason, null);
    }

    @Override
    public void abortTransaction(Long dbId, Long transactionId, String reason,
            TxnCommitAttachment txnCommitAttachment, List<Table> tableList) throws UserException {
        abortTransaction(dbId, transactionId, reason, txnCommitAttachment);
    }

    @Override
    public void abortTransaction(Long dbId, Long transactionId, String reason,
            TxnCommitAttachment txnCommitAttachment) throws UserException {
        LOG.info("try to abort transaction, dbId:{}, transactionId:{}", dbId, transactionId);

        AbortTxnRequest.Builder builder = AbortTxnRequest.newBuilder();
        builder.setDbId(dbId);
        builder.setTxnId(transactionId);
        builder.setReason(reason);
        builder.setCloudUniqueId(Config.cloud_unique_id);

        final AbortTxnRequest abortTxnRequest = builder.build();
        AbortTxnResponse abortTxnResponse = null;
        int retryTime = 0;
        try {
            while (retryTime < Config.metaServiceRpcRetryTimes()) {
                LOG.debug("retryTime:{}, abortTxnRequest:{}", retryTime, abortTxnRequest);
                abortTxnResponse = MetaServiceProxy
                        .getInstance().abortTxn(abortTxnRequest);
                LOG.debug("retryTime:{}, abortTxnResponse:{}", retryTime, abortTxnResponse);
                if (abortTxnResponse.getStatus().getCode() != MetaServiceCode.KV_TXN_CONFLICT) {
                    break;
                }
                // sleep random [20, 200] ms, avoid txn conflict
                LOG.info("abortTxn KV_TXN_CONFLICT, transactionId:{}, retryTime:{}", transactionId, retryTime);
                backoff();
                retryTime++;
                continue;
            }
            Preconditions.checkNotNull(abortTxnResponse);
            Preconditions.checkNotNull(abortTxnResponse.getStatus());
        } catch (RpcException e) {
            LOG.warn("abortTxn failed, transactionId:{}, Exception", transactionId, e);
            throw new UserException("abortTxn failed, errMsg:" + e.getMessage());
        }

        TransactionState txnState = TxnUtil.transactionStateFromPb(abortTxnResponse.getTxnInfo());
        TxnStateChangeCallback cb = callbackFactory.getCallback(txnState.getCallbackId());
        if (cb != null) {
            LOG.info("run txn callback, txnId:{} callbackId:{}", txnState.getTransactionId(),
                    txnState.getCallbackId());
            cb.afterAborted(txnState, true, txnState.getReason());
        }
        if (MetricRepo.isInit) {
            MetricRepo.COUNTER_TXN_FAILED.increase(1L);
        }
    }

    @Override
    public void abortTransaction(Long dbId, String label, String reason) throws UserException {
        LOG.info("try to abort transaction, dbId:{}, label:{}", dbId, label);

        AbortTxnRequest.Builder builder = AbortTxnRequest.newBuilder();
        builder.setDbId(dbId);
        builder.setLabel(label);
        builder.setReason(reason);
        builder.setCloudUniqueId(Config.cloud_unique_id);

        final AbortTxnRequest abortTxnRequest = builder.build();
        AbortTxnResponse abortTxnResponse = null;
        int retryTime = 0;

        try {
            while (retryTime < Config.metaServiceRpcRetryTimes()) {
                LOG.debug("retyTime:{}, abortTxnRequest:{}", retryTime, abortTxnRequest);
                abortTxnResponse = MetaServiceProxy
                        .getInstance().abortTxn(abortTxnRequest);
                LOG.debug("retryTime:{}, abortTxnResponse:{}", retryTime, abortTxnResponse);
                if (abortTxnResponse.getStatus().getCode() != MetaServiceCode.KV_TXN_CONFLICT) {
                    break;
                }

                // sleep random [20, 200] ms, avoid txn conflict
                LOG.info("abortTxn KV_TXN_CONFLICT, dbId:{}, label:{}, retryTime:{}", dbId, label, retryTime);
                backoff();
                retryTime++;
                continue;
            }
            Preconditions.checkNotNull(abortTxnResponse);
            Preconditions.checkNotNull(abortTxnResponse.getStatus());
        } catch (Exception e) {
            LOG.warn("abortTxn failed, label:{}, exception:", label, e);
            throw new UserException("abortTxn failed, errMsg:" + e.getMessage());
        }

        TransactionState txnState = TxnUtil.transactionStateFromPb(abortTxnResponse.getTxnInfo());
        TxnStateChangeCallback cb = callbackFactory.getCallback(txnState.getCallbackId());
        if (cb == null) {
            LOG.info("no callback to run for this txn, txnId:{} callbackId:{}", txnState.getTransactionId(),
                        txnState.getCallbackId());
            return;
        }

        LOG.info("run txn callback, txnId:{} callbackId:{}", txnState.getTransactionId(), txnState.getCallbackId());
        cb.afterAborted(txnState, true, txnState.getReason());
        if (MetricRepo.isInit) {
            MetricRepo.COUNTER_TXN_FAILED.increase(1L);
        }
    }

    @Override
    public void abortTransaction2PC(Long dbId, long transactionId, List<Table> tableList) throws UserException {
        LOG.info("try to abortTransaction2PC, dbId:{}, transactionId:{}", dbId, transactionId);
        abortTransaction(dbId, transactionId, "User Abort", null);
        LOG.info("abortTransaction2PC successfully, dbId:{}, transactionId:{}", dbId, transactionId);
    }

    @Override
    public List<TransactionState> getReadyToPublishTransactions() {
        //do nothing for CloudGlobalTransactionMgr
        return new ArrayList<TransactionState>();
    }

    @Override
    public boolean existCommittedTxns(Long dbId, Long tableId, Long partitionId) {
        //do nothing for CloudGlobalTransactionMgr
        return false;
    }

    @Override
    public void finishTransaction(long dbId, long transactionId) throws UserException {
        throw new UserException("Disallow to call finishTransaction()");
    }

    @Override
    public boolean isPreviousTransactionsFinished(long endTransactionId, long dbId, List<Long> tableIdList)
            throws AnalysisException {
        LOG.info("isPreviousTransactionsFinished(), endTransactionId:{}, dbId:{}, tableIdList:{}",
                endTransactionId, dbId, tableIdList);

        if (endTransactionId <= 0) {
            throw new AnalysisException("Invaid endTransactionId:" + endTransactionId);
        }
        CheckTxnConflictRequest.Builder builder = CheckTxnConflictRequest.newBuilder();
        builder.setDbId(dbId);
        builder.setEndTxnId(endTransactionId);
        builder.addAllTableIds(tableIdList);
        builder.setCloudUniqueId(Config.cloud_unique_id);

        final CheckTxnConflictRequest checkTxnConflictRequest = builder.build();
        CheckTxnConflictResponse checkTxnConflictResponse = null;
        try {
            LOG.info("CheckTxnConflictRequest:{}", checkTxnConflictRequest);
            checkTxnConflictResponse = MetaServiceProxy
                    .getInstance().checkTxnConflict(checkTxnConflictRequest);
            LOG.info("CheckTxnConflictResponse: {}", checkTxnConflictResponse);
        } catch (RpcException e) {
            throw new AnalysisException(e.getMessage());
        }

        if (checkTxnConflictResponse.getStatus().getCode() != MetaServiceCode.OK) {
            throw new AnalysisException(checkTxnConflictResponse.getStatus().getMsg());
        }
        return checkTxnConflictResponse.getFinished();
    }

    public boolean isPreviousNonTimeoutTxnFinished(long endTransactionId, long dbId, List<Long> tableIdList)
            throws AnalysisException {
        LOG.info("isPreviousNonTimeoutTxnFinished(), endTransactionId:{}, dbId:{}, tableIdList:{}",
                endTransactionId, dbId, tableIdList);

        if (endTransactionId <= 0) {
            throw new AnalysisException("Invaid endTransactionId:" + endTransactionId);
        }
        CheckTxnConflictRequest.Builder builder = CheckTxnConflictRequest.newBuilder();
        builder.setDbId(dbId);
        builder.setEndTxnId(endTransactionId);
        builder.addAllTableIds(tableIdList);
        builder.setCloudUniqueId(Config.cloud_unique_id);
        builder.setIgnoreTimeoutTxn(true);

        final CheckTxnConflictRequest checkTxnConflictRequest = builder.build();
        CheckTxnConflictResponse checkTxnConflictResponse = null;
        try {
            LOG.info("CheckTxnConflictRequest:{}", checkTxnConflictRequest);
            checkTxnConflictResponse = MetaServiceProxy
                    .getInstance().checkTxnConflict(checkTxnConflictRequest);
            LOG.info("CheckTxnConflictResponse: {}", checkTxnConflictResponse);
        } catch (RpcException e) {
            throw new AnalysisException(e.getMessage());
        }

        if (checkTxnConflictResponse.getStatus().getCode() != MetaServiceCode.OK) {
            throw new AnalysisException(checkTxnConflictResponse.getStatus().getMsg());
        }
        return checkTxnConflictResponse.getFinished();
    }

    @Override
    public void removeExpiredAndTimeoutTxns() {

    }

    @Override
    public TransactionState getTransactionState(long dbId, long transactionId) {
        LOG.info("try to get transaction state, dbId:{}, transactionId:{}", dbId, transactionId);
        GetTxnRequest.Builder builder = GetTxnRequest.newBuilder();
        builder.setDbId(dbId);
        builder.setTxnId(transactionId);
        builder.setCloudUniqueId(Config.cloud_unique_id);

        final GetTxnRequest getTxnRequest = builder.build();
        GetTxnResponse getTxnResponse = null;
        try {
            LOG.info("getTxnRequest:{}", getTxnRequest);
            getTxnResponse = MetaServiceProxy
                    .getInstance().getTxn(getTxnRequest);
            LOG.info("getTxnRequest: {}", getTxnResponse);
        } catch (RpcException e) {
            LOG.info("getTransactionState exception: {}", e.getMessage());
            return null;
        }

        if (getTxnResponse.getStatus().getCode() != MetaServiceCode.OK || !getTxnResponse.hasTxnInfo()) {
            LOG.info("getTransactionState exception: {}, {}", getTxnResponse.getStatus().getCode(),
                    getTxnResponse.getStatus().getMsg());
            return null;
        }
        return TxnUtil.transactionStateFromPb(getTxnResponse.getTxnInfo());
    }

    public void setEditLog(EditLog editLog) {
        //do nothing
    }

    public List<List<String>> getDbInfo() throws AnalysisException {
        throw new AnalysisException("Not supported");
    }

    public List<List<String>> getDbTransStateInfo(long dbId) throws AnalysisException {
        throw new AnalysisException("Not supported");
    }

    public List<List<String>> getDbTransInfo(long dbId, boolean running, int limit) throws AnalysisException {
        throw new AnalysisException("Not supported");
    }

    public List<List<String>> getDbTransInfoByStatus(long dbId, TransactionStatus status) throws AnalysisException {
        throw new AnalysisException("Not supported");
    }

    public List<List<String>> getSingleTranInfo(long dbId, long txnId) throws AnalysisException {
        throw new AnalysisException("Not supported");
    }

    public List<List<Comparable>> getTableTransInfo(long dbId, long txnId) throws AnalysisException {
        throw new AnalysisException("Not supported");
    }

    public List<List<Comparable>> getPartitionTransInfo(long dbId, long tid, long tableId)
            throws AnalysisException {
        throw new AnalysisException("Not supported");
    }

    @Override
    public int getTransactionNum() {
        int txnNum = 0;
        return txnNum;
    }

    @Override
    public long getNextTransactionId(long dbId) throws AnalysisException {
        LOG.info("try to getNextTransactionId() dbId:{}", dbId);
        GetCurrentMaxTxnRequest.Builder builder = GetCurrentMaxTxnRequest.newBuilder();
        builder.setCloudUniqueId(Config.cloud_unique_id);

        final GetCurrentMaxTxnRequest getCurrentMaxTxnRequest = builder.build();
        GetCurrentMaxTxnResponse getCurrentMaxTxnResponse = null;
        try {
            LOG.info("GetCurrentMaxTxnRequest:{}", getCurrentMaxTxnRequest);
            getCurrentMaxTxnResponse = MetaServiceProxy
                    .getInstance().getCurrentMaxTxnId(getCurrentMaxTxnRequest);
            LOG.info("GetCurrentMaxTxnResponse: {}", getCurrentMaxTxnResponse);
        } catch (RpcException e) {
            LOG.warn("getNextTransactionId() RpcException: {}", e.getMessage());
            throw new AnalysisException("getNextTransactionId() RpcException: " + e.getMessage());
        }

        if (getCurrentMaxTxnResponse.getStatus().getCode() != MetaServiceCode.OK) {
            LOG.info("getNextTransactionId() failed, code: {}, msg: {}",
                    getCurrentMaxTxnResponse.getStatus().getCode(), getCurrentMaxTxnResponse.getStatus().getMsg());
            throw new AnalysisException("getNextTransactionId() failed, msg:"
                    + getCurrentMaxTxnResponse.getStatus().getMsg());
        }
        return getCurrentMaxTxnResponse.getCurrentMaxTxnId();
    }

    @Override
    public TransactionStatus getLabelState(long dbId, String label) throws AnalysisException {
        throw new AnalysisException("Not supported");
    }

    @Override
    public Long getTransactionId(long dbId, String label) throws AnalysisException {
        throw new AnalysisException("Not supported");
    }

    @Override
    public void abortTxnWhenCoordinateBeDown(String coordinateHost, int limit) {
        //do nothing
    }

    @Override
    public void updateDatabaseUsedQuotaData(long dbId, long usedQuotaDataBytes) throws AnalysisException {
        //do nothing
    }

    @Override
    public TWaitingTxnStatusResult getWaitingTxnStatus(TWaitingTxnStatusRequest request)
            throws AnalysisException, TimeoutException {
        long dbId = request.getDbId();
        int commitTimeoutSec = Config.commit_timeout_second;
        for (int i = 0; i < commitTimeoutSec; ++i) {
            Env.getCurrentInternalCatalog().getDbOrAnalysisException(dbId);
            TWaitingTxnStatusResult statusResult = new TWaitingTxnStatusResult();
            statusResult.status = new TStatus();
            TransactionStatus txnStatus = null;
            if (request.isSetTxnId()) {
                long txnId = request.getTxnId();
                TransactionState txnState = Env.getCurrentGlobalTransactionMgr()
                        .getTransactionState(dbId, txnId);
                if (txnState == null) {
                    throw new AnalysisException("txn does not exist: " + txnId);
                }
                txnStatus = txnState.getTransactionStatus();
                if (!txnState.getReason().trim().isEmpty()) {
                    statusResult.status.setErrorMsgsIsSet(true);
                    statusResult.status.addToErrorMsgs(txnState.getReason());
                }
            } else {
                txnStatus = getLabelState(dbId, request.getLabel());
            }
            if (txnStatus == TransactionStatus.UNKNOWN || txnStatus.isFinalStatus()) {
                statusResult.setTxnStatusId(txnStatus.value());
                return statusResult;
            }
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                LOG.info("commit sleep exception.", e);
            }
        }
        throw new TimeoutException("Operation is timeout");
    }

    @Override
    public int getRunningTxnNums(long dbId) {
        return 0;
    }

    @Override
    public List<TransactionState> getPreCommittedTxnList(long dbId) {
        //todo
        return new ArrayList<TransactionState>();
    }

    @Override
    public void addTableIndexes(long dbId, long transactionId, OlapTable table) throws UserException{
        //do nothing
    }

    private void checkValidTimeoutSecond(long timeoutSecond, int maxLoadTimeoutSecond,
            int minLoadTimeOutSecond) throws AnalysisException {
        if (timeoutSecond > maxLoadTimeoutSecond || timeoutSecond < minLoadTimeOutSecond) {
            throw new AnalysisException("Invalid timeout: " + timeoutSecond + ". Timeout should between "
                    + minLoadTimeOutSecond + " and " + maxLoadTimeoutSecond
                    + " seconds");
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        throw new IOException("Disallow to call wirte()");
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        throw new IOException("Disallow to call readFields()");
    }

    public void replayUpsertTransactionState(TransactionState transactionState) throws MetaNotFoundException {
        throw new MetaNotFoundException("Disallow to call replayUpsertTransactionState()");
    }

    @Deprecated
    // Use replayBatchDeleteTransactions instead
    public void replayDeleteTransactionState(TransactionState transactionState) throws MetaNotFoundException {
        throw new MetaNotFoundException("Disallow to call replayDeleteTransactionState()");
    }

    public void replayBatchRemoveTransactions(BatchRemoveTransactionsOperation operation) throws MetaNotFoundException {
        throw new MetaNotFoundException("Disallow to call replayBatchRemoveTransactions()");
    }

    @Override
    public long getTxnNumByStatus(TransactionStatus status) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public long getAllRunningTxnNum() {
        return 0;
    }

    @Override
    public long getAllPublishTxnNum() {
        return 0;
    }

    public void replayBatchRemoveTransactionV2(BatchRemoveTransactionsOperationV2 operation)
            throws MetaNotFoundException {
        throw new MetaNotFoundException("Disallow to call replayBatchRemoveTransactionV2()");
    }

    public boolean isPreviousTransactionsFinished(long endTransactionId, long dbId, long tableId,
                                                  long partitionId) throws AnalysisException {
        throw new AnalysisException("Disallow to call isPreviousTransactionsFinished()");
    }

    /**
     * backoff policy implement by sleep random ms in [20ms, 200ms]
     */
    private void backoff() {
        int randomMillis = 20 + (int) (Math.random() * (200 - 20));
        try {
            Thread.sleep(randomMillis);
        } catch (InterruptedException e) {
            LOG.info("InterruptedException: ", e);
        }
    }

    public void cleanLabel(long dbId, String label) throws UserException {
        LOG.info("try to cleanLabel dbId: {}, label:{}", dbId, label);
        CleanTxnLabelRequest.Builder builder = CleanTxnLabelRequest.newBuilder();
        builder.setDbId(dbId).setCloudUniqueId(Config.cloud_unique_id);

        if (!Strings.isNullOrEmpty(label)) {
            builder.addLabels(label);
        }

        final CleanTxnLabelRequest cleanTxnLabelRequest = builder.build();
        CleanTxnLabelResponse cleanTxnLabelResponse = null;
        int retryTime = 0;

        try {
            // 5 times retry is enough for clean label
            while (retryTime < 5) {
                LOG.debug("retryTime:{}, cleanTxnLabel:{}", retryTime, cleanTxnLabelRequest);
                cleanTxnLabelResponse = MetaServiceProxy.getInstance().cleanTxnLabel(cleanTxnLabelRequest);
                LOG.debug("retryTime:{}, cleanTxnLabel:{}", retryTime, cleanTxnLabelResponse);
                if (cleanTxnLabelResponse.getStatus().getCode() != MetaServiceCode.KV_TXN_CONFLICT) {
                    break;
                }
                // sleep random [20, 200] ms, avoid txn conflict
                LOG.info("cleanTxnLabel KV_TXN_CONFLICT, dbId:{}, label:{}, retryTime:{}", dbId, label, retryTime);
                backoff();
                retryTime++;
                continue;
            }

            Preconditions.checkNotNull(cleanTxnLabelResponse);
            Preconditions.checkNotNull(cleanTxnLabelResponse.getStatus());
        } catch (Exception e) {
            LOG.warn("cleanTxnLabel failed, dbId:{}, exception:", dbId, e);
            throw new UserException("cleanTxnLabel failed, errMsg:" + e.getMessage());
        }

        if (cleanTxnLabelResponse.getStatus().getCode() != MetaServiceCode.OK) {
            LOG.warn("cleanTxnLabel failed, dbId:{} label:{} retryTime:{} cleanTxnLabelResponse:{}",
                    dbId, label, retryTime, cleanTxnLabelResponse);
            throw new UserException("cleanTxnLabel failed, errMsg:" + cleanTxnLabelResponse.getStatus().getMsg());
        }
        return;
    }

    public TransactionIdGenerator getTransactionIDGenerator() {
        return null;
    }
}
