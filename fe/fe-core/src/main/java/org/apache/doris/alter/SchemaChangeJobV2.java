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

package org.apache.doris.alter;

import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.BinlogConfig;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.OlapTable.OlapTableState;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Replica.ReplicaState;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.MarkedCountDownLatch;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.SchemaVersionAndHash;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.util.DbUtil;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.load.GroupCommitManager.SchemaChangeStatus;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.proto.OlapFile;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.task.AlterReplicaTask;
import org.apache.doris.task.CreateReplicaTask;
import org.apache.doris.thrift.TStorageFormat;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.thrift.TStorageType;
import org.apache.doris.thrift.TTaskType;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.google.common.collect.Table.Cell;
import com.google.gson.annotations.SerializedName;
import com.selectdb.cloud.proto.SelectdbCloud;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/*
 * Version 2 of SchemaChangeJob.
 * This is for replacing the old SchemaChangeJob
 * https://github.com/apache/doris/issues/1429
 */
public class SchemaChangeJobV2 extends AlterJobV2 {
    private static final Logger LOG = LogManager.getLogger(SchemaChangeJobV2.class);

    // partition id -> (shadow index id -> (shadow tablet id -> origin tablet id))
    @SerializedName(value = "partitionIndexTabletMap")
    private Table<Long, Long, Map<Long, Long>> partitionIndexTabletMap = HashBasedTable.create();
    // partition id -> (shadow index id -> shadow index))
    @SerializedName(value = "partitionIndexMap")
    private Table<Long, Long, MaterializedIndex> partitionIndexMap = HashBasedTable.create();
    // shadow index id -> origin index id
    @SerializedName(value = "indexIdMap")
    private Map<Long, Long> indexIdMap = Maps.newHashMap();
    // partition id -> origin index id
    @SerializedName(value = "partitionOriginIndexIdMap")
    private Map<Long, Long> partitionOriginIndexIdMap = Maps.newHashMap();
    // shadow index id -> shadow index name(__doris_shadow_xxx)
    @SerializedName(value = "indexIdToName")
    private Map<Long, String> indexIdToName = Maps.newHashMap();
    // shadow index id -> index schema
    @SerializedName(value = "indexSchemaMap")
    private Map<Long, List<Column>> indexSchemaMap = Maps.newHashMap();
    // shadow index id -> (shadow index schema version : schema hash)
    @SerializedName(value = "indexSchemaVersionAndHashMap")
    private Map<Long, SchemaVersionAndHash> indexSchemaVersionAndHashMap = Maps.newHashMap();
    // shadow index id -> shadow index short key count
    @SerializedName(value = "indexShortKeyMap")
    private Map<Long, Short> indexShortKeyMap = Maps.newHashMap();

    // bloom filter info
    @SerializedName(value = "hasBfChange")
    private boolean hasBfChange;
    @SerializedName(value = "bfColumns")
    private Set<String> bfColumns = null;
    @SerializedName(value = "bfFpp")
    private double bfFpp = 0;

    // alter index info
    @SerializedName(value = "indexChange")
    private boolean indexChange = false;
    @SerializedName(value = "invertedIndexChange")
    private boolean invertedIndexChange = false;
    @SerializedName(value = "isDropOp")
    private boolean isDropOp = false;
    @SerializedName(value = "indexes")
    private List<Index> indexes = null;
    @SerializedName(value = "alterInvertedIndexes")
    private List<Index> alterInvertedIndexes = null;
    @SerializedName(value = "oriIndexes")
    private List<Index> oriIndexes = null;

    // The schema change job will wait all transactions before this txn id finished, then send the schema change tasks.
    @SerializedName(value = "watershedTxnId")
    protected long watershedTxnId = -1;
    @SerializedName(value = "storageFormat")
    private TStorageFormat storageFormat = TStorageFormat.DEFAULT;

    // save all schema change tasks
    private AgentBatchTask schemaChangeBatchTask = new AgentBatchTask();
    // save failed task after retry three times, tabletId -> agentTask
    private Map<Long, List<AgentTask>> failedAgentTasks = Maps.newHashMap();

    private SchemaChangeJobV2() {
        super(JobType.SCHEMA_CHANGE);
    }

    public SchemaChangeJobV2(String rawSql, long jobId, long dbId, long tableId, String tableName, long timeoutMs) {
        super(rawSql, jobId, JobType.SCHEMA_CHANGE, dbId, tableId, tableName, timeoutMs);
        if (Config.isCloudMode()) {
            ConnectContext context = ConnectContext.get();
            if (context != null) {
                LOG.debug("schema change job add cloud cluster, context not null, cluster: {}",
                        context.getCloudCluster());
                setCloudClusterName(context.getCloudCluster());
            }
            LOG.debug("schema change job add cloud cluster, context {}", context);
        }
    }

    public void addTabletIdMap(long partitionId, long shadowIdxId, long shadowTabletId, long originTabletId) {
        Map<Long, Long> tabletMap = partitionIndexTabletMap.get(partitionId, shadowIdxId);
        if (tabletMap == null) {
            tabletMap = Maps.newHashMap();
            partitionIndexTabletMap.put(partitionId, shadowIdxId, tabletMap);
        }
        tabletMap.put(shadowTabletId, originTabletId);
    }

    public void addPartitionShadowIndex(long partitionId, long shadowIdxId, MaterializedIndex shadowIdx) {
        partitionIndexMap.put(partitionId, shadowIdxId, shadowIdx);
    }

    public void addPartitionOriginIndexIdMap(long partitionId, long originIdxId) {
        partitionOriginIndexIdMap.put(partitionId, originIdxId);
    }

    public void addIndexSchema(long shadowIdxId, long originIdxId,
            String shadowIndexName, int shadowSchemaVersion, int shadowSchemaHash,
            short shadowIdxShortKeyCount, List<Column> shadowIdxSchema) {
        indexIdMap.put(shadowIdxId, originIdxId);
        indexIdToName.put(shadowIdxId, shadowIndexName);
        indexSchemaVersionAndHashMap.put(shadowIdxId, new SchemaVersionAndHash(shadowSchemaVersion, shadowSchemaHash));
        indexShortKeyMap.put(shadowIdxId, shadowIdxShortKeyCount);
        indexSchemaMap.put(shadowIdxId, shadowIdxSchema);
    }

    public void setBloomFilterInfo(boolean hasBfChange, Set<String> bfColumns, double bfFpp) {
        this.hasBfChange = hasBfChange;
        this.bfColumns = bfColumns;
        this.bfFpp = bfFpp;
    }

    public void setAlterIndexInfo(boolean indexChange, List<Index> indexes) {
        this.indexChange = indexChange;
        this.indexes = indexes;
    }

    public void setAlterInvertedIndexInfo(boolean invertedIndexChange,
                    boolean isDropOp, List<Index> alterInvertedIndexes) {
        this.invertedIndexChange = invertedIndexChange;
        this.isDropOp = isDropOp;
        this.alterInvertedIndexes = alterInvertedIndexes;
    }

    public void setOriIndexInfo(List<Index> oriIndexes) {
        this.oriIndexes = oriIndexes;
    }

    public void setStorageFormat(TStorageFormat storageFormat) {
        this.storageFormat = storageFormat;
    }

    /**
     * clear some date structure in this job to save memory
     * these data structures must not used in getInfo method
     */
    private void pruneMeta() {
        partitionIndexTabletMap.clear();
        partitionIndexMap.clear();
        indexSchemaMap.clear();
        indexShortKeyMap.clear();
        partitionOriginIndexIdMap.clear();
    }

    private void createShadowIndexReplica() throws AlterCancelException {
        Database db = Env.getCurrentInternalCatalog()
                .getDbOrException(dbId, s -> new AlterCancelException("Database " + s + " does not exist"));

        if (!checkTableStable(db)) {
            return;
        }

        // 1. create replicas
        AgentBatchTask batchTask = new AgentBatchTask();
        // count total replica num
        int totalReplicaNum = 0;
        for (MaterializedIndex shadowIdx : partitionIndexMap.values()) {
            for (Tablet tablet : shadowIdx.getTablets()) {
                totalReplicaNum += tablet.getReplicas().size();
            }
        }
        MarkedCountDownLatch<Long, Long> countDownLatch = new MarkedCountDownLatch<>(totalReplicaNum);

        OlapTable tbl;
        try {
            tbl = (OlapTable) db.getTableOrMetaException(tableId, TableType.OLAP);
        } catch (MetaNotFoundException e) {
            throw new AlterCancelException(e.getMessage());
        }

        if (invertedIndexChange) {
            // add inverted index for tablet meta
            tbl.writeLockOrAlterCancelException();
            try {
                Preconditions.checkState(tbl.getState() == OlapTableState.SCHEMA_CHANGE);
                // update index
                tbl.setIndexes(indexes);
            } finally {
                tbl.writeUnlock();
            }
            return;
        }

        tbl.readLock();
        try {
            Preconditions.checkState(tbl.getState() == OlapTableState.SCHEMA_CHANGE);
            BinlogConfig binlogConfig = new BinlogConfig(tbl.getBinlogConfig());
            for (long partitionId : partitionIndexMap.rowKeySet()) {
                Partition partition = tbl.getPartition(partitionId);
                if (partition == null) {
                    continue;
                }
                TStorageMedium storageMedium = tbl.getPartitionInfo().getDataProperty(partitionId).getStorageMedium();

                Map<Long, MaterializedIndex> shadowIndexMap = partitionIndexMap.row(partitionId);
                for (Map.Entry<Long, MaterializedIndex> entry : shadowIndexMap.entrySet()) {
                    long shadowIdxId = entry.getKey();
                    MaterializedIndex shadowIdx = entry.getValue();

                    short shadowShortKeyColumnCount = indexShortKeyMap.get(shadowIdxId);
                    List<Column> shadowSchema = indexSchemaMap.get(shadowIdxId);
                    int shadowSchemaHash = indexSchemaVersionAndHashMap.get(shadowIdxId).schemaHash;
                    long originIndexId = indexIdMap.get(shadowIdxId);
                    int originSchemaHash = tbl.getSchemaHashByIndexId(originIndexId);
                    KeysType originKeysType = tbl.getKeysTypeByIndexId(originIndexId);

                    for (Tablet shadowTablet : shadowIdx.getTablets()) {
                        long shadowTabletId = shadowTablet.getId();
                        List<Replica> shadowReplicas = shadowTablet.getReplicas();
                        for (Replica shadowReplica : shadowReplicas) {
                            long backendId = shadowReplica.getBackendId();
                            long shadowReplicaId = shadowReplica.getId();
                            countDownLatch.addMark(backendId, shadowTabletId);
                            CreateReplicaTask createReplicaTask = new CreateReplicaTask(
                                    backendId, dbId, tableId, partitionId, shadowIdxId, shadowTabletId,
                                    shadowReplicaId, shadowShortKeyColumnCount, shadowSchemaHash,
                                    Partition.PARTITION_INIT_VERSION,
                                    originKeysType, TStorageType.COLUMN, storageMedium,
                                    shadowSchema, bfColumns, bfFpp, countDownLatch, indexes,
                                    tbl.isInMemory(),
                                    tbl.getPartitionInfo().getTabletType(partitionId),
                                    null,
                                    tbl.getCompressionType(),
                                    tbl.getEnableUniqueKeyMergeOnWrite(), tbl.getStoragePolicy(),
                                    tbl.disableAutoCompaction(),
                                    tbl.enableSingleReplicaCompaction(),
                                    tbl.skipWriteIndexOnLoad(),
                                    tbl.getCompactionPolicy(),
                                    tbl.getTimeSeriesCompactionGoalSizeMbytes(),
                                    tbl.getTimeSeriesCompactionFileCountThreshold(),
                                    tbl.getTimeSeriesCompactionTimeThresholdSeconds(),
                                    tbl.storeRowColumn(),
                                    tbl.isDynamicSchema(),
                                    binlogConfig);

                            createReplicaTask.setBaseTablet(partitionIndexTabletMap.get(partitionId, shadowIdxId)
                                    .get(shadowTabletId), originSchemaHash);
                            if (this.storageFormat != null) {
                                createReplicaTask.setStorageFormat(this.storageFormat);
                            }

                            batchTask.addTask(createReplicaTask);
                        } // end for rollupReplicas
                    } // end for rollupTablets
                }
            }
        } finally {
            tbl.readUnlock();
        }
        if (!FeConstants.runningUnitTest) {
            // send all tasks and wait them finished
            AgentTaskQueue.addBatchTask(batchTask);
            AgentTaskExecutor.submit(batchTask);
            long timeout = DbUtil.getCreateReplicasTimeoutMs(totalReplicaNum);
            boolean ok = false;
            try {
                ok = countDownLatch.await(timeout, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                LOG.warn("InterruptedException: ", e);
                ok = false;
            }

            if (!ok) {
                // create replicas failed. just cancel the job
                // clear tasks and show the failed replicas to user
                AgentTaskQueue.removeBatchTask(batchTask, TTaskType.CREATE);
                String errMsg = null;
                if (!countDownLatch.getStatus().ok()) {
                    errMsg = countDownLatch.getStatus().getErrorMsg();
                } else {
                    // only show at most 3 results
                    List<String> subList = countDownLatch.getLeftMarks().stream().limit(3)
                            .map(item -> "(backendId = " + item.getKey() + ", tabletId = "  + item.getValue() + ")")
                            .collect(Collectors.toList());
                    errMsg = "Error replicas:" + Joiner.on(", ").join(subList);
                }
                LOG.warn("failed to create replicas for job: {}, {}", jobId, errMsg);
                throw new AlterCancelException("Create replicas failed. Error: " + errMsg);
            }
        }

        // create all replicas success.
        // add all shadow indexes to catalog
        tbl.writeLockOrAlterCancelException();
        try {
            Preconditions.checkState(tbl.getState() == OlapTableState.SCHEMA_CHANGE);
            addShadowIndexToCatalog(tbl);
        } finally {
            tbl.writeUnlock();
        }
    }

    private void commitCloudShadowIndex() throws AlterCancelException {
        if (!Config.isCloudMode()) {
            return;
        }

        List<Long> shadowIdxList =
                indexIdMap.keySet().stream().collect(Collectors.toList());
        try {
            Env.getCurrentInternalCatalog().commitCloudMaterializedIndex(tableId,
                    shadowIdxList);
        } catch (Exception e) {
            LOG.warn("commitCloudMaterializedIndex exception:", e);
            throw new AlterCancelException(e.getMessage());
        }
        LOG.info("commitCloudShadowIndex finished, dbId:{}, tableId:{}, jobId:{}, shadowIdxList:{}",
                dbId, tableId, jobId, shadowIdxList);
    }

    private void dropCloudShadowIndex() {
        if (!Config.isCloudMode()) {
            return;
        }

        List<Long> shadowIdxList = indexIdMap.keySet().stream().collect(Collectors.toList());
        long tryTimes = 1;
        while (true) {
            try {
                Env.getCurrentInternalCatalog().dropCloudMaterializedIndex(tableId, shadowIdxList);
                break;
            } catch (Exception e) {
                LOG.warn("tryTimes:{}, dropCloudShadowIndex exception:", tryTimes, e);
            }
            sleepSeveralSeconds();
            tryTimes++;
        }

        LOG.info("dropCloudShadowIndex finished, dbId:{}, tableId:{}, jobId:{}, shadowIdxList:{}",
                dbId, tableId, jobId, shadowIdxList);
    }

    private void dropCloudOriginIndex() {
        if (!Config.isCloudMode()) {
            return;
        }

        List<Long> originIdxList = indexIdMap.values().stream().collect(Collectors.toList());

        int tryTimes = 1;
        while (true) {
            try {
                Env.getCurrentInternalCatalog().dropCloudMaterializedIndex(tableId, originIdxList);
                break;
            } catch (Exception e) {
                LOG.warn("tryTimes:{}, dropCloudOriginIndex exception:", tryTimes, e);
            }
            sleepSeveralSeconds();
            tryTimes++;
        }

        LOG.info("dropCloudOriginIndex finished, dbId:{}, tableId:{}, jobId:{}, originIdxList:{}",
                dbId, tableId, jobId, originIdxList);
    }

    private void createCloudShadowIndexReplica() throws AlterCancelException {
        Database db = Env.getCurrentInternalCatalog()
                .getDbOrException(dbId, s -> new AlterCancelException("Database " + s + " does not exist"));

        // 1. create replicas

        OlapTable tbl;
        try {
            tbl = (OlapTable) db.getTableOrMetaException(tableId, TableType.OLAP);
        } catch (MetaNotFoundException e) {
            throw new AlterCancelException(e.getMessage());
        }

        if (invertedIndexChange) {
            // add inverted index for tablet meta
            tbl.writeLockOrAlterCancelException();
            try {
                Preconditions.checkState(tbl.getState() == OlapTableState.SCHEMA_CHANGE);
                // update index
                tbl.setIndexes(indexes);
            } finally {
                tbl.writeUnlock();
            }
            return;
        }

        long expiration = (createTimeMs + timeoutMs) / 1000;
        tbl.readLock();
        try {
            Preconditions.checkState(tbl.getState() == OlapTableState.SCHEMA_CHANGE);
            try {
                List<Long> shadowIdxList = indexIdMap.keySet().stream().collect(Collectors.toList());
                Env.getCurrentInternalCatalog().prepareCloudMaterializedIndex(tableId, shadowIdxList,
                        expiration);

                for (long partitionId : partitionIndexMap.rowKeySet()) {
                    Partition partition = tbl.getPartition(partitionId);
                    if (partition == null) {
                        continue;
                    }
                    Map<Long, MaterializedIndex> shadowIndexMap = partitionIndexMap.row(partitionId);
                    for (Map.Entry<Long, MaterializedIndex> entry : shadowIndexMap.entrySet()) {
                        long shadowIdxId = entry.getKey();
                        MaterializedIndex shadowIdx = entry.getValue();

                        short shadowShortKeyColumnCount = indexShortKeyMap.get(shadowIdxId);
                        List<Column> shadowSchema = indexSchemaMap.get(shadowIdxId);
                        int shadowSchemaHash = indexSchemaVersionAndHashMap.get(shadowIdxId).schemaHash;
                        int shadowSchemaVersion = indexSchemaVersionAndHashMap.get(shadowIdxId).schemaVersion;
                        long originIndexId = indexIdMap.get(shadowIdxId);
                        KeysType originKeysType = tbl.getKeysTypeByIndexId(originIndexId);

                        SelectdbCloud.CreateTabletsRequest.Builder requestBuilder =
                                SelectdbCloud.CreateTabletsRequest.newBuilder();
                        for (Tablet shadowTablet : shadowIdx.getTablets()) {
                            OlapFile.TabletMetaPB.Builder builder =
                                    Env.getCurrentInternalCatalog().createCloudTabletMetaBuilder(tableId, shadowIdxId,
                                    partitionId, shadowTablet, tbl.getPartitionInfo().getTabletType(partitionId),
                                    shadowSchemaHash, originKeysType, shadowShortKeyColumnCount, bfColumns,
                                    bfFpp, indexes, shadowSchema, tbl.getDataSortInfo(), tbl.getCompressionType(),
                                    tbl.getStoragePolicy(), tbl.isInMemory(), tbl.isPersistent(), true,
                                    tbl.isDynamicSchema(), tbl.getName(), tbl.getTTLSeconds(),
                                    tbl.getEnableUniqueKeyMergeOnWrite(), tbl.storeRowColumn(), shadowSchemaVersion);
                            requestBuilder.addTabletMetas(builder);
                        } // end for rollupTablets
                        Env.getCurrentInternalCatalog().sendCreateTabletsRpc(requestBuilder);
                    }
                }
            } catch (Exception e) {
                LOG.warn("createCloudShadowIndexReplica Exception:", e);
                throw new AlterCancelException(e.getMessage());
            }

        } finally {
            tbl.readUnlock();
        }

        // create all replicas success.
        // add all shadow indexes to catalog
        tbl.writeLockOrAlterCancelException();
        try {
            Preconditions.checkState(tbl.getState() == OlapTableState.SCHEMA_CHANGE);
            addShadowIndexToCatalog(tbl);
        } finally {
            tbl.writeUnlock();
        }
    }

    /**
     * runPendingJob():
     * 1. Create all replicas of all shadow indexes and wait them finished.
     * 2. After creating done, add the shadow indexes to catalog, user can not see this
     *    shadow index, but internal load process will generate data for these indexes.
     * 3. Get a new transaction id, then set job's state to WAITING_TXN
     */
    @Override
    protected void runPendingJob() throws AlterCancelException {
        Preconditions.checkState(jobState == JobState.PENDING, jobState);
        LOG.info("begin to send create replica tasks. job: {}", jobId);

        if (Config.isNotCloudMode()) {
            Database db = Env.getCurrentInternalCatalog()
                    .getDbOrException(dbId, s -> new AlterCancelException("Database " + s + " does not exist"));
            if (!checkTableStable(db)) {
                return;
            }
            createShadowIndexReplica();
        } else {
            createCloudShadowIndexReplica();
        }

        try {
            this.watershedTxnId = Env.getCurrentGlobalTransactionMgr()
                    .getNextTransactionId(dbId);
        } catch (AnalysisException e) {
            throw new AlterCancelException(e.getMessage());
        }
        this.jobState = JobState.WAITING_TXN;

        // write edit log
        Env.getCurrentEnv().getEditLog().logAlterJob(this);
        LOG.info("transfer schema change job {} state to {}, watershed txn id: {}",
                jobId, this.jobState, watershedTxnId);
    }

    private void addShadowIndexToCatalog(OlapTable tbl) {
        if (invertedIndexChange) {
            return;
        }

        for (long partitionId : partitionIndexMap.rowKeySet()) {
            Partition partition = tbl.getPartition(partitionId);
            if (partition == null) {
                continue;
            }
            Map<Long, MaterializedIndex> shadowIndexMap = partitionIndexMap.row(partitionId);
            for (MaterializedIndex shadowIndex : shadowIndexMap.values()) {
                Preconditions.checkState(shadowIndex.getState() == IndexState.SHADOW, shadowIndex.getState());
                partition.createRollupIndex(shadowIndex);
            }
        }

        for (long shadowIdxId : indexIdMap.keySet()) {
            tbl.setIndexMeta(shadowIdxId, indexIdToName.get(shadowIdxId), indexSchemaMap.get(shadowIdxId),
                    indexSchemaVersionAndHashMap.get(shadowIdxId).schemaVersion,
                    indexSchemaVersionAndHashMap.get(shadowIdxId).schemaHash,
                    indexShortKeyMap.get(shadowIdxId), TStorageType.COLUMN,
                    tbl.getKeysTypeByIndexId(indexIdMap.get(shadowIdxId)),
                    indexChange ? indexes : tbl.getIndexMetaByIndexId(indexIdMap.get(shadowIdxId)).getIndexes());
        }

        tbl.rebuildFullSchema();
    }

    /**
     * runWaitingTxnJob():
     * 1. Wait the transactions before the watershedTxnId to be finished.
     * 2. If all previous transactions finished, send schema change tasks to BE.
     * 3. Change job state to RUNNING.
     */
    @Override
    protected void runWaitingTxnJob() throws AlterCancelException {
        Preconditions.checkState(jobState == JobState.WAITING_TXN, jobState);
        try {
            if (!isPreviousLoadFinished()) {
                LOG.info("wait transactions before {} to be finished, schema change job: {}", watershedTxnId, jobId);
                return;
            }
        } catch (AnalysisException e) {
            throw new AlterCancelException(e.getMessage());
        }

        LOG.info("previous transactions are all finished, begin to send schema change tasks. job: {}", jobId);
        Database db = Env.getCurrentInternalCatalog()
                .getDbOrException(dbId, s -> new AlterCancelException("Database " + s + " does not exist"));

        OlapTable tbl;
        try {
            tbl = (OlapTable) db.getTableOrMetaException(tableId, TableType.OLAP);
        } catch (MetaNotFoundException e) {
            throw new AlterCancelException(e.getMessage());
        }

        tbl.readLock();

        try {
            long expiration = (createTimeMs + timeoutMs) / 1000;
            Map<String, Column> indexColumnMap = Maps.newHashMap();
            for (Map.Entry<Long, List<Column>> entry : indexSchemaMap.entrySet()) {
                for (Column column : entry.getValue()) {
                    indexColumnMap.put(column.getName(), column);
                }
            }

            Preconditions.checkState(tbl.getState() == OlapTableState.SCHEMA_CHANGE);

            for (long partitionId : partitionIndexMap.rowKeySet()) {
                Partition partition = tbl.getPartition(partitionId);
                Preconditions.checkNotNull(partition, partitionId);

                // the schema change task will transform the data before visible
                // version(included).
                long visibleVersion = partition.getVisibleVersion();

                Map<Long, MaterializedIndex> shadowIndexMap = partitionIndexMap.row(partitionId);
                for (Map.Entry<Long, MaterializedIndex> entry : shadowIndexMap.entrySet()) {
                    long shadowIdxId = entry.getKey();
                    MaterializedIndex shadowIdx = entry.getValue();

                    Map<String, Expr> defineExprs = Maps.newHashMap();

                    List<Column> fullSchema = tbl.getBaseSchema(true);
                    DescriptorTable descTable = new DescriptorTable();
                    TupleDescriptor destTupleDesc = descTable.createTupleDescriptor();
                    for (Column column : fullSchema) {
                        SlotDescriptor destSlotDesc = descTable.addSlotDescriptor(destTupleDesc);
                        destSlotDesc.setIsMaterialized(true);
                        destSlotDesc.setColumn(column);
                        destSlotDesc.setIsNullable(column.isAllowNull());

                        if (indexColumnMap.containsKey(SchemaChangeHandler.SHADOW_NAME_PREFIX + column.getName())) {
                            Column newColumn = indexColumnMap
                                    .get(SchemaChangeHandler.SHADOW_NAME_PREFIX + column.getName());
                            if (newColumn.getType() != column.getType()) {
                                try {
                                    SlotRef slot = new SlotRef(destSlotDesc);
                                    slot.setCol(column.getName());
                                    defineExprs.put(column.getName(), slot.castTo(newColumn.getType()));
                                } catch (AnalysisException e) {
                                    throw new AlterCancelException(e.getMessage());
                                }
                            }
                        }

                    }

                    long originIdxId = indexIdMap.get(shadowIdxId);
                    int shadowSchemaHash = indexSchemaVersionAndHashMap.get(shadowIdxId).schemaHash;
                    int originSchemaHash = tbl.getSchemaHashByIndexId(indexIdMap.get(shadowIdxId));
                    List<Column> originSchemaColumns = tbl.getSchemaByIndexId(originIdxId, true);
                    for (Tablet shadowTablet : shadowIdx.getTablets()) {
                        long shadowTabletId = shadowTablet.getId();
                        long originTabletId = partitionIndexTabletMap.get(partitionId, shadowIdxId).get(shadowTabletId);
                        List<Replica> shadowReplicas = shadowTablet.getReplicas();
                        for (Replica shadowReplica : shadowReplicas) {
                            AlterReplicaTask rollupTask = new AlterReplicaTask(shadowReplica.getBackendId(), dbId,
                                    tableId, partitionId, shadowIdxId, originIdxId, shadowTabletId, originTabletId,
                                    shadowReplica.getId(), shadowSchemaHash, originSchemaHash, visibleVersion, jobId,
                                    JobType.SCHEMA_CHANGE, defineExprs, descTable, originSchemaColumns, null,
                                    expiration);
                            schemaChangeBatchTask.addTask(rollupTask);
                        }
                    }
                }
            } // end for partitions
        } finally {
            tbl.readUnlock();
        }

        LOG.debug("schemaChangeBatchTask:{}", schemaChangeBatchTask);
        AgentTaskQueue.addBatchTask(schemaChangeBatchTask);
        AgentTaskExecutor.submit(schemaChangeBatchTask);

        this.jobState = JobState.RUNNING;

        // DO NOT write edit log here, tasks will be send again if FE restart or master changed.
        LOG.info("transfer schema change job {} state to {}", jobId, this.jobState);
    }

    /**
     * runRunningJob()
     * 1. Wait all schema change tasks to be finished.
     * 2. Check the integrity of the newly created shadow indexes.
     * 3. Replace the origin index with shadow index, and set shadow index's state as NORMAL to be visible to user.
     * 4. Set job'state as FINISHED.
     */
    @Override
    protected void runRunningJob() throws AlterCancelException {
        Preconditions.checkState(jobState == JobState.RUNNING, jobState);

        // must check if db or table still exist first.
        // or if table is dropped, the tasks will never be finished,
        // and the job will be in RUNNING state forever.
        Database db = Env.getCurrentInternalCatalog()
                .getDbOrException(dbId, s -> new AlterCancelException("Database " + s + " does not exist"));
        OlapTable tbl;
        try {
            tbl = (OlapTable) db.getTableOrMetaException(tableId, TableType.OLAP);
        } catch (MetaNotFoundException e) {
            throw new AlterCancelException(e.getMessage());
        }

        LOG.debug("jobId:{}, cloudClusterName:{}", jobId, cloudClusterName);
        if (!schemaChangeBatchTask.isFinished()) {
            LOG.info("schema change tasks not finished. job: {}", jobId);
            List<AgentTask> tasks = schemaChangeBatchTask.getUnfinishedTasks(2000);
            if (Config.isCloudMode() && Env.getCurrentSystemInfo()
                    .getCloudClusterIdByName(cloudClusterName) == null) {
                for (AgentTask task : tasks) {
                    task.setFinished(true);
                    AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.ALTER, task.getSignature());
                }
                StringBuilder sb = new StringBuilder("cloud cluster(");
                sb.append(cloudClusterName);
                sb.append(") has been removed, jobId=");
                sb.append(jobId);
                String msg = sb.toString();
                LOG.warn(msg);
                throw new AlterCancelException(msg);
            }
            for (AgentTask task : tasks) {
                LOG.debug("schema change task: {}, {}", task, task.getFailedTimes());
                if (task.getFailedTimes() > 0) {
                    task.setFinished(true);
                    AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.ALTER, task.getSignature());
                    LOG.warn("schema change task failed: " + task.getErrorMsg());
                    if (!failedAgentTasks.containsKey(task.getTabletId())) {
                        failedAgentTasks.put(task.getTabletId(), Lists.newArrayList(task));
                    } else {
                        failedAgentTasks.get(task.getTabletId()).add(task);
                    }
                    int expectSucceedTaskNum = tbl.getPartitionInfo()
                            .getReplicaAllocation(task.getPartitionId()).getTotalReplicaNum();
                    int failedTaskCount = failedAgentTasks.get(task.getTabletId()).size();
                    if (expectSucceedTaskNum - failedTaskCount < expectSucceedTaskNum / 2 + 1) {
                        throw new AlterCancelException("schema change tasks failed on same tablet reach threshold "
                                    + failedAgentTasks.get(task.getTabletId()));
                    }
                }
            }
            return;
        }
        waitWalFinished();
        /*
         * all tasks are finished. check the integrity.
         * we just check whether all new replicas are healthy.
         */
        tbl.writeLockOrAlterCancelException();

        try {
            Preconditions.checkState(tbl.getState() == OlapTableState.SCHEMA_CHANGE);
            TabletInvertedIndex invertedIndex = Env.getCurrentInvertedIndex();
            for (List<AgentTask> tasks : failedAgentTasks.values()) {
                for (AgentTask task : tasks) {
                    invertedIndex.getReplica(task.getTabletId(), task.getBackendId()).setBad(true);
                }
            }
            for (long partitionId : partitionIndexMap.rowKeySet()) {
                Partition partition = tbl.getPartition(partitionId);
                Preconditions.checkNotNull(partition, partitionId);

                long visiableVersion = partition.getVisibleVersion();
                short expectReplicationNum = tbl.getPartitionInfo()
                        .getReplicaAllocation(partition.getId()).getTotalReplicaNum();

                Map<Long, MaterializedIndex> shadowIndexMap = partitionIndexMap.row(partitionId);
                for (Map.Entry<Long, MaterializedIndex> entry : shadowIndexMap.entrySet()) {
                    MaterializedIndex shadowIdx = entry.getValue();

                    for (Tablet shadowTablet : shadowIdx.getTablets()) {
                        List<Replica> replicas = shadowTablet.getReplicas();
                        int healthyReplicaNum = 0;
                        for (Replica replica : replicas) {
                            if (!replica.isBad() && replica.getLastFailedVersion() < 0
                                    && replica.checkVersionCatchUp(visiableVersion, false)) {
                                healthyReplicaNum++;
                            }
                        }

                        if (healthyReplicaNum < expectReplicationNum / 2 + 1) {
                            LOG.warn("shadow tablet {} has few healthy replicas: {}, schema change job: {}",
                                    shadowTablet.getId(), replicas, jobId);
                            throw new AlterCancelException(
                                    "shadow tablet " + shadowTablet.getId() + " has few healthy replicas");
                        }
                    } // end for tablets
                }
            } // end for partitions
            // all partitions are good

            // for cloud schema change
            commitCloudShadowIndex();
            onFinished(tbl);
        } finally {
            tbl.writeUnlock();
        }

        pruneMeta();
        this.jobState = JobState.FINISHED;
        this.finishedTimeMs = System.currentTimeMillis();

        Env.getCurrentEnv().getEditLog().logAlterJob(this);
        LOG.info("schema change job finished: {}", jobId);

        changeTableState(dbId, tableId, OlapTableState.NORMAL);
        LOG.info("set table's state to NORMAL, table id: {}, job id: {}", tableId, jobId);

        // try best to drop origin index
        dropCloudOriginIndex();
    }

    private void waitWalFinished() {
        // wait wal done here
        Env.getCurrentEnv().getGroupCommitManager().setStatus(tableId, SchemaChangeStatus.BLOCK);
        LOG.info("block table {}", tableId);
        List<Long> aliveBeIds = Env.getCurrentSystemInfo().getAllBackendIds(true);
        long expireTime = System.currentTimeMillis() + Config.check_wal_queue_timeout_threshold;
        while (true) {
            LOG.info("wait for wal queue size to be empty");
            boolean walFinished = Env.getCurrentEnv().getGroupCommitManager()
                    .isPreviousWalFinished(tableId, aliveBeIds);
            if (walFinished) {
                LOG.info("all wal is finished");
                break;
            } else if (System.currentTimeMillis() > expireTime) {
                LOG.warn("waitWalFinished time out");
                break;
            } else {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ie) {
                    LOG.info("schema change job sleep wait for wal InterruptedException: ", ie);
                }
            }
        }
        Env.getCurrentEnv().getGroupCommitManager().setStatus(tableId, SchemaChangeStatus.NORMAL);
        LOG.info("release table {}", tableId);
    }

    private void onFinished(OlapTable tbl) {
        if (!invertedIndexChange) {
            // replace the origin index with shadow index, set index state as NORMAL
            for (Partition partition : tbl.getPartitions()) {
                // drop the origin index from partitions
                for (Map.Entry<Long, Long> entry : indexIdMap.entrySet()) {
                    long shadowIdxId = entry.getKey();
                    long originIdxId = entry.getValue();
                    // get index from catalog, not from 'partitionIdToRollupIndex'.
                    // because if this alter job is recovered from edit log, index in 'partitionIndexMap'
                    // is not the same object in catalog. So modification on that index can not reflect to the index
                    // in catalog.
                    MaterializedIndex shadowIdx = partition.getIndex(shadowIdxId);
                    Preconditions.checkNotNull(shadowIdx, shadowIdxId);
                    MaterializedIndex droppedIdx = null;
                    if (originIdxId == partition.getBaseIndex().getId()) {
                        droppedIdx = partition.getBaseIndex();
                    } else {
                        droppedIdx = partition.deleteRollupIndex(originIdxId);
                    }
                    Preconditions.checkNotNull(droppedIdx, originIdxId + " vs. " + shadowIdxId);

                    // set replica state
                    for (Tablet tablet : shadowIdx.getTablets()) {
                        for (Replica replica : tablet.getReplicas()) {
                            replica.setState(ReplicaState.NORMAL);
                        }
                    }

                    partition.visualiseShadowIndex(shadowIdxId, originIdxId == partition.getBaseIndex().getId());

                    // delete origin replicas
                    for (Tablet originTablet : droppedIdx.getTablets()) {
                        Env.getCurrentInvertedIndex().deleteTablet(originTablet.getId());
                    }
                }
            }

            // update index schema info of each index
            for (Map.Entry<Long, Long> entry : indexIdMap.entrySet()) {
                long shadowIdxId = entry.getKey();
                long originIdxId = entry.getValue();
                String shadowIdxName = tbl.getIndexNameById(shadowIdxId);
                String originIdxName = tbl.getIndexNameById(originIdxId);
                int maxColUniqueId = tbl.getIndexMetaByIndexId(originIdxId).getMaxColUniqueId();
                for (Column column : indexSchemaMap.get(shadowIdxId)) {
                    if (column.getUniqueId() > maxColUniqueId) {
                        maxColUniqueId = column.getUniqueId();
                    }
                }
                tbl.getIndexMetaByIndexId(shadowIdxId).setMaxColUniqueId(maxColUniqueId);
                LOG.debug("originIdxId:{}, shadowIdxId:{}, maxColUniqueId:{}, indexSchema:{}",
                        originIdxId, shadowIdxId, maxColUniqueId,  indexSchemaMap.get(shadowIdxId));

                tbl.deleteIndexInfo(originIdxName);
                // the shadow index name is '__doris_shadow_xxx', rename it to origin name 'xxx'
                // this will also remove the prefix of columns
                tbl.renameIndexForSchemaChange(shadowIdxName, originIdxName);
                tbl.renameColumnNamePrefix(shadowIdxId);

                if (originIdxId == tbl.getBaseIndexId()) {
                    // set base index
                    tbl.setBaseIndexId(shadowIdxId);
                }
            }
        }
        // rebuild table's full schema
        tbl.rebuildFullSchema();

        // update bloom filter
        if (hasBfChange) {
            tbl.setBloomFilterInfo(bfColumns, bfFpp);
        }
        // update index
        if (indexChange) {
            tbl.setIndexes(indexes);
        }

        // set storage format of table, only set if format is v2
        if (storageFormat == TStorageFormat.V2) {
            tbl.setStorageFormat(storageFormat);
        }
    }

    /*
     * cancelImpl() can be called any time any place.
     * We need to clean any possible residual of this job.
     */
    @Override
    protected synchronized boolean cancelImpl(String errMsg) {
        if (jobState.isFinalState()) {
            return false;
        }

        cancelInternal();
        pruneMeta();
        this.errMsg = errMsg;
        this.finishedTimeMs = System.currentTimeMillis();
        LOG.info("cancel {} job {}, err: {}", this.type, jobId, errMsg);
        Env.getCurrentEnv().getEditLog().logAlterJob(this);

        changeTableState(dbId, tableId, OlapTableState.NORMAL);
        LOG.info("set table's state to NORMAL when cancel, table id: {}, job id: {}", tableId, jobId);

        // try best to drop shadow index, when job is cancelled
        dropCloudShadowIndex();

        return true;
    }

    private void cancelInternal() {
        // clear tasks if has
        AgentTaskQueue.removeBatchTask(schemaChangeBatchTask, TTaskType.ALTER);
        AgentTaskQueue.removeBatchTask(schemaChangeBatchTask, TTaskType.ALTER_INVERTED_INDEX);
        // remove all shadow indexes, and set state to NORMAL
        TabletInvertedIndex invertedIndex = Env.getCurrentInvertedIndex();
        Database db = Env.getCurrentInternalCatalog().getDbNullable(dbId);
        if (db != null) {
            OlapTable tbl = (OlapTable) db.getTableNullable(tableId);
            if (tbl != null) {
                tbl.writeLock();
                try {
                    for (long partitionId : partitionIndexMap.rowKeySet()) {
                        Partition partition = tbl.getPartition(partitionId);
                        Preconditions.checkNotNull(partition, partitionId);

                        Map<Long, MaterializedIndex> shadowIndexMap = partitionIndexMap.row(partitionId);
                        for (Map.Entry<Long, MaterializedIndex> entry : shadowIndexMap.entrySet()) {
                            MaterializedIndex shadowIdx = entry.getValue();
                            for (Tablet shadowTablet : shadowIdx.getTablets()) {
                                invertedIndex.deleteTablet(shadowTablet.getId());
                            }
                            partition.deleteRollupIndex(shadowIdx.getId());
                        }
                    }
                    for (String shadowIndexName : indexIdToName.values()) {
                        tbl.deleteIndexInfo(shadowIndexName);
                    }
                } finally {
                    tbl.writeUnlock();
                }
            }
        }

        jobState = JobState.CANCELLED;
    }

    // Check whether transactions of the given database which txnId is less than 'watershedTxnId' are finished.
    protected boolean isPreviousLoadFinished() throws AnalysisException {
        return Env.getCurrentGlobalTransactionMgr().isPreviousTransactionsFinished(
                watershedTxnId, dbId, Lists.newArrayList(tableId));
    }

    /**
     * Replay job in PENDING state.
     * Should replay all changes before this job's state transfer to PENDING.
     * These changes should be same as changes in SchemaChangeHandler.createJob()
     */
    private void replayCreateJob(SchemaChangeJobV2 replayedJob) throws MetaNotFoundException {
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(dbId);
        OlapTable olapTable = (OlapTable) db.getTableOrMetaException(tableId, TableType.OLAP);
        olapTable.writeLock();
        try {
            TabletInvertedIndex invertedIndex = Env.getCurrentInvertedIndex();
            for (Cell<Long, Long, MaterializedIndex> cell : partitionIndexMap.cellSet()) {
                long partitionId = cell.getRowKey();
                long shadowIndexId = cell.getColumnKey();
                MaterializedIndex shadowIndex = cell.getValue();

                TStorageMedium medium = olapTable.getPartitionInfo().getDataProperty(partitionId).getStorageMedium();

                for (Tablet shadownTablet : shadowIndex.getTablets()) {
                    TabletMeta shadowTabletMeta = new TabletMeta(dbId, tableId, partitionId, shadowIndexId,
                            indexSchemaVersionAndHashMap.get(shadowIndexId).schemaHash, medium);
                    invertedIndex.addTablet(shadownTablet.getId(), shadowTabletMeta);
                    for (Replica shadowReplica : shadownTablet.getReplicas()) {
                        invertedIndex.addReplica(shadownTablet.getId(), shadowReplica);
                    }
                }
            }

            // set table state
            olapTable.setState(OlapTableState.SCHEMA_CHANGE);
        } finally {
            olapTable.writeUnlock();
        }

        this.watershedTxnId = replayedJob.watershedTxnId;
        jobState = JobState.PENDING;
        LOG.info("replay pending schema change job: {}, table id: {}", jobId, tableId);
    }

    /**
     * Replay job in WAITING_TXN state.
     * Should replay all changes in runPendingJob()
     */
    private void replayPendingJob(SchemaChangeJobV2 replayedJob) throws MetaNotFoundException {
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(dbId);
        OlapTable olapTable = (OlapTable) db.getTableOrMetaException(tableId, TableType.OLAP);
        olapTable.writeLock();
        try {
            addShadowIndexToCatalog(olapTable);
        } finally {
            olapTable.writeUnlock();
        }

        // should still be in WAITING_TXN state, so that the alter tasks will be resend again
        this.jobState = JobState.WAITING_TXN;
        this.watershedTxnId = replayedJob.watershedTxnId;
        LOG.info("replay waiting txn schema change job: {} table id: {}", jobId, tableId);
    }

    /**
     * Replay job in FINISHED state.
     * Should replay all changes in runRunningJob()
     */
    private void replayRunningJob(SchemaChangeJobV2 replayedJob) {
        Database db = Env.getCurrentInternalCatalog().getDbNullable(dbId);
        if (db != null) {
            OlapTable tbl = (OlapTable) db.getTableNullable(tableId);
            if (tbl != null) {
                tbl.writeLock();
                try {
                    onFinished(tbl);
                } finally {
                    tbl.writeUnlock();
                }
            }
        }
        // try best to drop shadow index
        dropCloudOriginIndex();

        pruneMeta();
        jobState = JobState.FINISHED;
        this.finishedTimeMs = replayedJob.finishedTimeMs;
        LOG.info("replay finished schema change job: {} table id: {}", jobId, tableId);
        changeTableState(dbId, tableId, OlapTableState.NORMAL);
        LOG.info("set table's state to NORMAL when replay finished, table id: {}, job id: {}", tableId, jobId);
    }

    /**
     * Replay job in CANCELLED state.
     */
    private void replayCancelled(SchemaChangeJobV2 replayedJob) {
        cancelInternal();
        // try best to drop shadow index
        dropCloudShadowIndex();
        this.jobState = JobState.CANCELLED;
        this.finishedTimeMs = replayedJob.finishedTimeMs;
        this.errMsg = replayedJob.errMsg;
        LOG.info("replay cancelled schema change job: {}", jobId);
        changeTableState(dbId, tableId, OlapTableState.NORMAL);
        LOG.info("set table's state to NORMAL when replay cancelled, table id: {}, job id: {}", tableId, jobId);
    }

    @Override
    public void replay(AlterJobV2 replayedJob) {
        try {
            SchemaChangeJobV2 replayedSchemaChangeJob = (SchemaChangeJobV2) replayedJob;
            switch (replayedJob.jobState) {
                case PENDING:
                    replayCreateJob(replayedSchemaChangeJob);
                    break;
                case WAITING_TXN:
                    replayPendingJob(replayedSchemaChangeJob);
                    break;
                case FINISHED:
                    replayRunningJob(replayedSchemaChangeJob);
                    break;
                case CANCELLED:
                    replayCancelled(replayedSchemaChangeJob);
                    break;
                default:
                    break;
            }
        } catch (MetaNotFoundException e) {
            LOG.warn("[INCONSISTENT META] replay schema change job failed {}", replayedJob.getJobId(), e);
        }
    }

    @Override
    protected void getInfo(List<List<Comparable>> infos) {
        // calc progress first. all index share the same process
        String progress = FeConstants.null_string;
        if (jobState == JobState.RUNNING && schemaChangeBatchTask.getTaskNum() > 0) {
            progress = schemaChangeBatchTask.getFinishedTaskNum() + "/" + schemaChangeBatchTask.getTaskNum();
        }

        // one line for one shadow index
        for (Map.Entry<Long, Long> entry : indexIdMap.entrySet()) {
            long shadowIndexId = entry.getKey();
            List<Comparable> info = Lists.newArrayList();
            info.add(jobId);
            info.add(tableName);
            info.add(TimeUtils.longToTimeStringWithms(createTimeMs));
            info.add(TimeUtils.longToTimeStringWithms(finishedTimeMs));
            // only show the origin index name
            info.add(indexIdToName.get(shadowIndexId).substring(SchemaChangeHandler.SHADOW_NAME_PREFIX.length()));
            info.add(shadowIndexId);
            info.add(entry.getValue());
            info.add(indexSchemaVersionAndHashMap.get(shadowIndexId).toString());
            info.add(watershedTxnId);
            info.add(jobState.name());
            info.add(errMsg);
            info.add(progress);
            info.add(timeoutMs / 1000);
            infos.add(info);
        }
    }

    public List<List<String>> getUnfinishedTasks(int limit) {
        List<List<String>> taskInfos = Lists.newArrayList();
        if (jobState == JobState.RUNNING) {
            List<AgentTask> tasks = schemaChangeBatchTask.getUnfinishedTasks(limit);
            for (AgentTask agentTask : tasks) {
                AlterReplicaTask alterTask = (AlterReplicaTask) agentTask;
                List<String> info = Lists.newArrayList();
                info.add(String.valueOf(alterTask.getBackendId()));
                info.add(String.valueOf(alterTask.getBaseTabletId()));
                info.add(String.valueOf(alterTask.getSignature()));
                taskInfos.add(info);
            }
        }
        return taskInfos;
    }

    private void changeTableState(long dbId, long tableId, OlapTableState olapTableState) {
        try {
            Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(dbId);
            OlapTable olapTable = (OlapTable) db.getTableOrMetaException(tableId, TableType.OLAP);
            olapTable.writeLockOrMetaException();
            try {
                if (olapTable.getState() == olapTableState) {
                    return;
                } else if (olapTable.getState() == OlapTableState.SCHEMA_CHANGE) {
                    olapTable.setState(olapTableState);
                }
            } finally {
                olapTable.writeUnlock();
            }
        } catch (MetaNotFoundException e) {
            LOG.warn("[INCONSISTENT META] changing table status failed after schema change job done", e);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this, AlterJobV2.class);
        Text.writeString(out, json);
    }

    @Override
    public String toJson() {
        return GsonUtils.GSON.toJson(this);
    }
}
