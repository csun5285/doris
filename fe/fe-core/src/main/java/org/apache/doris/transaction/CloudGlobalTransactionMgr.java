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
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DuplicatedRequestException;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.QuotaExceedException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.load.loadv2.LoadJobFinalOperation;
import org.apache.doris.persist.BatchRemoveTransactionsOperation;
import org.apache.doris.persist.EditLog;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.thrift.TWaitingTxnStatusRequest;
import org.apache.doris.thrift.TWaitingTxnStatusResult;
import org.apache.doris.transaction.TransactionState.LoadJobSourceType;
import org.apache.doris.transaction.TransactionState.TxnCoordinator;

import com.google.common.base.Preconditions;
import com.selectdb.cloud.proto.SelectdbCloud.AbortTxnRequest;
import com.selectdb.cloud.proto.SelectdbCloud.AbortTxnResponse;
import com.selectdb.cloud.proto.SelectdbCloud.BeginTxnRequest;
import com.selectdb.cloud.proto.SelectdbCloud.BeginTxnResponse;
import com.selectdb.cloud.proto.SelectdbCloud.CheckTxnConflictRequest;
import com.selectdb.cloud.proto.SelectdbCloud.CheckTxnConflictResponse;
import com.selectdb.cloud.proto.SelectdbCloud.CommitTxnRequest;
import com.selectdb.cloud.proto.SelectdbCloud.CommitTxnResponse;
import com.selectdb.cloud.proto.SelectdbCloud.GetCurrentMaxTxnRequest;
import com.selectdb.cloud.proto.SelectdbCloud.GetCurrentMaxTxnResponse;
import com.selectdb.cloud.proto.SelectdbCloud.GetTxnRequest;
import com.selectdb.cloud.proto.SelectdbCloud.GetTxnResponse;
import com.selectdb.cloud.proto.SelectdbCloud.LoadJobSourceTypePB;
import com.selectdb.cloud.proto.SelectdbCloud.MetaServiceCode;
import com.selectdb.cloud.proto.SelectdbCloud.PrecommitTxnRequest;
import com.selectdb.cloud.proto.SelectdbCloud.PrecommitTxnResponse;
import com.selectdb.cloud.proto.SelectdbCloud.TxnInfoPB;
import com.selectdb.cloud.proto.SelectdbCloud.UniqueIdPB;
import com.selectdb.cloud.rpc.MetaServiceProxy;
import com.selectdb.cloud.transaction.TxnUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeoutException;

public class CloudGlobalTransactionMgr implements GlobalTransactionMgrInterface {
    private static final Logger LOG = LogManager.getLogger(CloudGlobalTransactionMgr.class);

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
        LOG.info("try to begin transaction.");
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

        final BeginTxnRequest beginTxnRequest = BeginTxnRequest.newBuilder()
                .setTxnInfo(txnInfoBuilder.build())
                .setCloudUniqueId(Config.cloud_unique_id)
                .build();
        BeginTxnResponse beginTxnResponse = null;
        try {
            LOG.info("beginTxnRequest: {}", beginTxnRequest);
            beginTxnResponse = MetaServiceProxy.getInstance().beginTxn(beginTxnRequest);
            LOG.info("beginTxnResponse: {}", beginTxnResponse);
        } catch (RpcException e) {
            LOG.warn("beginTransaction() rpc failed, RpcException= {}", e);
            throw new BeginTransactionException("beginTransaction() rpc failed, RpcException=" + e.toString());
        }

        Preconditions.checkNotNull(beginTxnResponse);
        Preconditions.checkNotNull(beginTxnResponse.getStatus());
        switch (beginTxnResponse.getStatus().getCode()) {
            case OK:
                break;
            case TXN_DUPLICATED_REQ:
                throw new DuplicatedRequestException(DebugUtil.printId(requestId),
                        beginTxnResponse.getDupTxnId(), beginTxnResponse.getStatus().getMsg());
            case TXN_LABEL_ALREADY_USED:
                throw new LabelAlreadyUsedException(beginTxnResponse.getStatus().getMsg());
            default:
                throw new BeginTransactionException(beginTxnResponse.getStatus().getMsg());
        }

        long txnId = beginTxnResponse.getTxnId();
        return txnId;
    }

    @Override
    public void preCommitTransaction2PC(Database db, List<Table> tableList, long transactionId,
            List<TabletCommitInfo> tabletCommitInfos, long timeoutMillis, TxnCommitAttachment txnCommitAttachment)
            throws UserException {

        LOG.info("try to precommit transaction: {}", transactionId);
        if (Config.disable_load_job) {
            throw new TransactionCommitFailedException("disable_load_job is set to true, all load jobs are prevented");
        }

        PrecommitTxnRequest.Builder builder = PrecommitTxnRequest.newBuilder();
        builder.setDbId(db.getId());
        builder.setTxnId(transactionId);

        if (txnCommitAttachment != null) {
            if (txnCommitAttachment instanceof LoadJobFinalOperation) {
                LoadJobFinalOperation loadJobFinalOperation = (LoadJobFinalOperation) txnCommitAttachment;
                builder.setCommitAttachment(TxnUtil
                        .loadJobFinalOperationToPb(loadJobFinalOperation));
            } else {
                throw new UserException("Invalid txnCommitAttachment");
            }
        }

        builder.setPrecommitTimeoutMs(timeoutMillis);

        final PrecommitTxnRequest precommitTxnRequest = builder.build();
        PrecommitTxnResponse precommitTxnResponse = null;
        try {
            LOG.info("precommitTxnRequest: {}", precommitTxnRequest);
            precommitTxnResponse = MetaServiceProxy
                    .getInstance().precommitTxn(precommitTxnRequest);
            LOG.info("precommitTxnResponse: {}", precommitTxnResponse);
        } catch (RpcException e) {
            throw new UserException(e.getMessage());
        }

        if (precommitTxnResponse.getStatus().getCode() != MetaServiceCode.OK) {
            throw new UserException(precommitTxnResponse.getStatus().getMsg());
        }
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

        CommitTxnRequest.Builder builder = CommitTxnRequest.newBuilder();
        builder.setDbId(dbId)
                .setTxnId(transactionId)
                .setIs2Pc(is2PC)
                .setCloudUniqueId(Config.cloud_unique_id);

        if (txnCommitAttachment != null) {
            if (txnCommitAttachment instanceof LoadJobFinalOperation) {
                LoadJobFinalOperation loadJobFinalOperation = (LoadJobFinalOperation) txnCommitAttachment;
                builder.setCommitAttachment(TxnUtil
                        .loadJobFinalOperationToPb(loadJobFinalOperation));
            } else {
                throw new UserException("Invalid txnCommitAttachment");
            }
        }

        final CommitTxnRequest commitTxnRequest = builder.build();
        CommitTxnResponse commitTxnResponse = null;
        int retryTime = 0;
        while (retryTime++ < Config.meta_service_rpc_retry_times) {
            try {
                LOG.info("commitTxn, transactionId={}, commitTxnRequest:{}", transactionId, commitTxnRequest);
                commitTxnResponse = MetaServiceProxy.getInstance().commitTxn(commitTxnRequest);
                LOG.info("commitTxn, transactionId={}, commitTxnResponse: {}", transactionId, commitTxnResponse);
                if (commitTxnResponse.getStatus().getCode() != MetaServiceCode.KV_TXN_CONFLICT) {
                    break;
                }
            } catch (Exception e) {
                LOG.warn("ignore commitTxn exception, transactionId={}, retryTime={}", transactionId, retryTime, e);
            }
            // sleep random millis [20, 200] ms, avoid txn conflict
            int randomMillis = 20 + (int) (Math.random() * (200 - 20));
            LOG.debug("randomMillis:{}", randomMillis);
            try {
                Thread.sleep(randomMillis);
            } catch (InterruptedException e) {
                LOG.info("InterruptedException: ", e);
            }
        }

        if (commitTxnResponse == null || !commitTxnResponse.hasStatus()) {
            throw new UserException("internal error, txnid=" + transactionId);
        }

        if (commitTxnResponse.getStatus().getCode() != MetaServiceCode.OK
                && commitTxnResponse.getStatus().getCode() != MetaServiceCode.TXN_ALREADY_VISIBLE) {
            LOG.warn("commitTxn failed, transactionId={}, for {} times, commitTxnResponse:{}",
                    transactionId, retryTime, commitTxnResponse);
            throw new UserException("internal error, try later");
        }

        TransactionState txnState = TxnUtil.transactionStateFromPb(commitTxnResponse.getTxnInfo());
        TxnStateChangeCallback cb = callbackFactory.getCallback(txnState.getCallbackId());
        if (cb == null) {
            LOG.info("commitTxn, no callback to run for this txn, transactionId={} callbackId={}, txnState={}",
                    txnState.getTransactionId(), txnState.getCallbackId(), txnState);
            return;
        }

        LOG.info("commitTxn, run txn callback, transactionId={} callbackId={}, txnState={}",
                txnState.getTransactionId(), txnState.getCallbackId(), txnState);
        cb.afterCommitted(txnState, true);
        cb.afterVisible(txnState, true);
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

        commitTransaction(db.getId(), tableList, transactionId, tabletCommitInfos, txnCommitAttachment);
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
            TxnCommitAttachment txnCommitAttachment) throws UserException {
        LOG.info("try to abort transaction, dbId:{}, transactionId:{}", dbId, transactionId);

        AbortTxnRequest.Builder builder = AbortTxnRequest.newBuilder();
        builder.setDbId(dbId);
        builder.setTxnId(transactionId);
        builder.setReason(reason);
        builder.setCloudUniqueId(Config.cloud_unique_id);

        final AbortTxnRequest abortTxnRequest = builder.build();
        AbortTxnResponse abortTxnResponse = null;
        try {
            LOG.info("abortTxnRequest:{}", abortTxnRequest);
            abortTxnResponse = MetaServiceProxy
                    .getInstance().abortTxn(abortTxnRequest);
            LOG.info("abortTxnResponse: {}", abortTxnResponse);
        } catch (RpcException e) {
            throw new UserException(e);
        }

        if (abortTxnResponse.getStatus().getCode() != MetaServiceCode.OK) {
            throw new UserException(abortTxnResponse.getStatus().getMsg());
        }
        TransactionState txnState = TxnUtil.transactionStateFromPb(abortTxnResponse.getTxnInfo());
        TxnStateChangeCallback cb = callbackFactory.getCallback(txnState.getCallbackId());
        if (cb == null) {
            LOG.info("no callback to run for this txn, txnId={} callbackId={}", txnState.getTransactionId(),
                        txnState.getCallbackId());
            return;
        }

        LOG.info("run txn callback, txnId={} callbackId={}", txnState.getTransactionId(), txnState.getCallbackId());
        cb.afterAborted(txnState, true, txnState.getReason());
    }

    @Override
    public void abortTransaction(Long dbId, String label, String reason) throws UserException {
        LOG.info("try to abort transaction, label:{}, transactionId:{}", dbId, label);

        AbortTxnRequest.Builder builder = AbortTxnRequest.newBuilder();
        builder.setDbId(dbId);
        builder.setLabel(label);
        builder.setReason(reason);
        builder.setCloudUniqueId(Config.cloud_unique_id);

        final AbortTxnRequest abortTxnRequest = builder.build();
        AbortTxnResponse abortTxnResponse = null;
        try {
            LOG.info("abortTxnRequest:{}", abortTxnRequest);
            abortTxnResponse = MetaServiceProxy
                    .getInstance().abortTxn(abortTxnRequest);
            LOG.info("abortTxnResponse: {}", abortTxnResponse);
        } catch (RpcException e) {
            throw new UserException(e);
        }

        if (abortTxnResponse.getStatus().getCode() != MetaServiceCode.OK) {
            throw new UserException(abortTxnResponse.getStatus().getMsg());
        }

        TransactionState txnState = TxnUtil.transactionStateFromPb(abortTxnResponse.getTxnInfo());
        TxnStateChangeCallback cb = callbackFactory.getCallback(txnState.getCallbackId());
        if (cb == null) {
            LOG.info("no callback to run for this txn, txnId={} callbackId={}", txnState.getTransactionId(),
                        txnState.getCallbackId());
            return;
        }

        LOG.info("run txn callback, txnId={} callbackId={}", txnState.getTransactionId(), txnState.getCallbackId());
        cb.afterAborted(txnState, true, txnState.getReason());
    }

    @Override
    public void abortTransaction2PC(Long dbId, long transactionId) throws UserException {
        LOG.info("try to abortTransaction2PC, dbId:{}, transactionId:{}", dbId, transactionId);
        abortTransaction(dbId, transactionId, "User Abort", null);
        LOG.info(" abortTransaction2PC successfully, dbId:{}, transactionId:{}", dbId, transactionId);
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
    public void finishTransaction(long dbId, long transactionId, Set<Long> errorReplicaIds) throws UserException {
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

    public List<List<Comparable>> getDbInfo() throws AnalysisException {
        throw new AnalysisException("Not suppoted");
    }

    public List<List<String>> getDbTransStateInfo(long dbId) throws AnalysisException {
        throw new AnalysisException("Not suppoted");
    }

    public List<List<String>> getDbTransInfo(long dbId, boolean running, int limit) throws AnalysisException {
        throw new AnalysisException("Not suppoted");
    }

    public List<List<String>> getDbTransInfoByStatus(long dbId, TransactionStatus status) throws AnalysisException {
        throw new AnalysisException("Not suppoted");
    }

    public List<List<String>> getSingleTranInfo(long dbId, long txnId) throws AnalysisException {
        throw new AnalysisException("Not suppoted");
    }

    public List<List<Comparable>> getTableTransInfo(long dbId, long txnId) throws AnalysisException {
        throw new AnalysisException("Not suppoted");
    }

    public List<List<Comparable>> getPartitionTransInfo(long dbId, long tid, long tableId)
            throws AnalysisException {
        throw new AnalysisException("Not suppoted");
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
        throw new AnalysisException("Not suppoted");
    }

    @Override
    public Long getTransactionId(long dbId, String label) throws AnalysisException {
        throw new AnalysisException("Not suppoted");
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
    public long getAllRunningTxnReplicaNum() {
        return 0;
    }

    @Override
    public long getAllPublishTxnNum() {
        return 0;
    }

}
