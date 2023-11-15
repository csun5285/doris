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
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DuplicatedRequestException;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.QuotaExceedException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.BatchRemoveTransactionsOperation;
import org.apache.doris.persist.BatchRemoveTransactionsOperationV2;
import org.apache.doris.persist.EditLog;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.thrift.TWaitingTxnStatusRequest;
import org.apache.doris.thrift.TWaitingTxnStatusResult;
import org.apache.doris.transaction.TransactionState.LoadJobSourceType;
import org.apache.doris.transaction.TransactionState.TxnCoordinator;

import java.io.DataInput;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;

public interface GlobalTransactionMgrInterface extends Writable {

    public TxnStateCallbackFactory getCallbackFactory();

    public void addDatabaseTransactionMgr(Long dbId);

    public void removeDatabaseTransactionMgr(Long dbId);

    public long beginTransaction(long dbId, List<Long> tableIdList, String label, TxnCoordinator coordinator,
            LoadJobSourceType sourceType, long timeoutSecond)
            throws AnalysisException, LabelAlreadyUsedException, BeginTransactionException, DuplicatedRequestException,
            QuotaExceedException, MetaNotFoundException;

    public long beginTransaction(long dbId, List<Long> tableIdList, String label, TUniqueId requestId,
            TxnCoordinator coordinator, LoadJobSourceType sourceType, long listenerId, long timeoutSecond)
            throws AnalysisException, LabelAlreadyUsedException, BeginTransactionException, DuplicatedRequestException,
            QuotaExceedException, MetaNotFoundException;

    public void preCommitTransaction2PC(Database db, List<Table> tableList, long transactionId,
                                               List<TabletCommitInfo> tabletCommitInfos, long timeoutMillis,
                                               TxnCommitAttachment txnCommitAttachment) throws UserException;

    public void commitTransaction(long dbId, List<Table> tableList,
            long transactionId, List<TabletCommitInfo> tabletCommitInfos)
            throws UserException;

    public void commitTransaction(long dbId, List<Table> tableList, long transactionId,
            List<TabletCommitInfo> tabletCommitInfos, TxnCommitAttachment txnCommitAttachment)
            throws UserException;

    public boolean commitAndPublishTransaction(DatabaseIf db, List<Table> tableList, long transactionId,
                                               List<TabletCommitInfo> tabletCommitInfos, long timeoutMillis)
                                               throws UserException;

    public boolean commitAndPublishTransaction(DatabaseIf db, List<Table> tableList, long transactionId,
                                               List<TabletCommitInfo> tabletCommitInfos, long timeoutMillis,
                                               TxnCommitAttachment txnCommitAttachment) throws UserException;

    public void commitTransaction2PC(Database db, List<Table> tableList, long transactionId, long timeoutMillis)
            throws UserException;

    public void abortTransaction(long dbId, long transactionId, String reason) throws UserException;

    public void abortTransaction(Long dbId, Long txnId, String reason,
            TxnCommitAttachment txnCommitAttachment) throws UserException;

    public void abortTransaction(Long dbId, Long txnId, String reason,
            TxnCommitAttachment txnCommitAttachment, List<Table> tableList) throws UserException;

    public void abortTransaction(Long dbId, String label, String reason) throws UserException;

    public void abortTransaction2PC(Long dbId, long transactionId, List<Table> tableList) throws UserException;

    public List<TransactionState> getReadyToPublishTransactions();

    public boolean existCommittedTxns(Long dbId, Long tableId, Long partitionId);

    public void finishTransaction(long dbId, long transactionId) throws UserException;

    public boolean isPreviousTransactionsFinished(long endTransactionId, long dbId, List<Long> tableIdList)
            throws AnalysisException;

    public void removeExpiredAndTimeoutTxns();

    public TransactionStatus getLabelState(long dbId, String label) throws AnalysisException;

    public Long getTransactionId(long dbId, String label) throws AnalysisException;

    public TransactionState getTransactionState(long dbId, long transactionId);

    public void setEditLog(EditLog editLog);

    public void addTableIndexes(long dbId, long transactionId, OlapTable table) throws UserException;

    public List<List<String>> getDbInfo() throws AnalysisException;

    public List<List<String>> getDbTransStateInfo(long dbId) throws AnalysisException;

    public List<List<String>> getDbTransInfo(long dbId, boolean running, int limit) throws AnalysisException;

    public List<List<String>> getDbTransInfoByStatus(long dbId, TransactionStatus status) throws AnalysisException;

    public List<List<String>> getSingleTranInfo(long dbId, long txnId) throws AnalysisException;

    public List<List<Comparable>> getTableTransInfo(long dbId, long txnId) throws AnalysisException;

    public List<List<Comparable>> getPartitionTransInfo(long dbId, long tid, long tableId)
            throws AnalysisException;

    public int getTransactionNum();

    public int getRunningTxnNums(long dbId);

    public List<TransactionState> getPreCommittedTxnList(long dbId);

    public long getNextTransactionId(long dbId) throws AnalysisException;

    public void abortTxnWhenCoordinateBeDown(String coordinateHost, int limit);

    public void updateDatabaseUsedQuotaData(long dbId, long usedQuotaDataBytes) throws AnalysisException;

    public TWaitingTxnStatusResult getWaitingTxnStatus(TWaitingTxnStatusRequest request)
            throws AnalysisException, TimeoutException;

    public long getAllRunningTxnNum();

    public long getAllPublishTxnNum();

    public void readFields(DataInput in) throws IOException;

    public long getTxnNumByStatus(TransactionStatus status);

    public void replayUpsertTransactionState(TransactionState transactionState) throws MetaNotFoundException;

    @Deprecated
    // Use replayBatchDeleteTransactions instead
    public void replayDeleteTransactionState(TransactionState transactionState) throws MetaNotFoundException;

    public void replayBatchRemoveTransactions(BatchRemoveTransactionsOperation operation) throws MetaNotFoundException;

    public void replayBatchRemoveTransactionV2(BatchRemoveTransactionsOperationV2 operation)
            throws MetaNotFoundException;

    public boolean isPreviousTransactionsFinished(long endTransactionId, long dbId, long tableId, long partitionId)
            throws AnalysisException;

    public void cleanLabel(long dbId, String label) throws UserException;

    public TransactionIdGenerator getTransactionIDGenerator();
}

