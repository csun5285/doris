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

package org.apache.doris.catalog;

import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.metric.GaugeMetricImpl;
import org.apache.doris.metric.Metric.MetricUnit;
import org.apache.doris.metric.MetricLabel;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.rpc.RpcException;

import com.selectdb.cloud.proto.SelectdbCloud;
import com.selectdb.cloud.rpc.MetaServiceProxy;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;

/*
 * CloudTabletStatMgr is for collecting tablet(replica) statistics from backends.
 * Each FE will collect by itself.
 */
public class CloudTabletStatMgr extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(CloudTabletStatMgr.class);

    private ForkJoinPool taskPool = new ForkJoinPool(Runtime.getRuntime().availableProcessors());

    public CloudTabletStatMgr() {
        super("cloud tablet stat mgr", Config.tablet_stat_update_interval_second * 1000);
    }

    @Override
    protected void runAfterCatalogReady() {
        LOG.info("cloud tablet stat begin");
        long start = System.currentTimeMillis();
        List<SelectdbCloud.GetTabletStatsRequest> reqList =
                new ArrayList<SelectdbCloud.GetTabletStatsRequest>();

        SelectdbCloud.GetTabletStatsRequest.Builder builder =
                SelectdbCloud.GetTabletStatsRequest.newBuilder();

        List<Long> dbIds = Env.getCurrentInternalCatalog().getDbIds();
        for (Long dbId : dbIds) {
            Database db = Env.getCurrentInternalCatalog().getDbNullable(dbId);
            if (db == null) {
                continue;
            }

            List<Table> tableList = db.getTables();
            for (Table table : tableList) {
                if (table.getType() != TableType.OLAP) {
                    continue;
                }

                table.readLock();
                try {
                    OlapTable tbl = (OlapTable) table;
                    for (Partition partition : tbl.getAllPartitions()) {
                        for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                            for (Long tabletId : index.getTabletIdsInOrder()) {
                                Tablet tablet = index.getTablet(tabletId);
                                SelectdbCloud.TabletIndexPB.Builder tabletBuilder =
                                        SelectdbCloud.TabletIndexPB.newBuilder();
                                tabletBuilder.setDbId(dbId);
                                tabletBuilder.setTableId(table.getId());
                                tabletBuilder.setIndexId(index.getId());
                                tabletBuilder.setPartitionId(partition.getId());
                                tabletBuilder.setTabletId(tablet.getId());
                                builder.addTabletIdx(tabletBuilder);

                                if (builder.getTabletIdxCount() >= Config.get_tablet_stat_batch_size) {
                                    reqList.add(builder.build());
                                    builder = SelectdbCloud.GetTabletStatsRequest.newBuilder();
                                }
                            }
                        }
                    } // partitions
                } finally {
                    table.readUnlock();
                }
            } // tables
        } // end for dbs

        if (builder.getTabletIdxCount() > 0) {
            reqList.add(builder.build());
        }

        for (SelectdbCloud.GetTabletStatsRequest req : reqList) {
            SelectdbCloud.GetTabletStatsResponse resp;
            try {
                resp = getTabletStats(req);
            } catch (RpcException e) {
                LOG.info("get tablet stats exception {} ", e);
                continue;
            }

            if (resp.getStatus().getCode() != SelectdbCloud.MetaServiceCode.OK) {
                continue;
            }

            if (LOG.isDebugEnabled()) {
                int i = 0;
                for (SelectdbCloud.TabletIndexPB idx : req.getTabletIdxList()) {
                    LOG.debug("db_id: {} table_id: {} index_id: {} tablet_id: {} size: {}",
                            idx.getDbId(), idx.getTableId(), idx.getIndexId(), idx.getTabletId(),
                            resp.getTabletStats(i++).getDataSize());
                }
            }

            updateTabletStat(resp);
        }

        LOG.info("finished to get tablet stat of all backends. cost: {} ms",
                (System.currentTimeMillis() - start));

        // after update replica in all backends, update index row num
        start = System.currentTimeMillis();
        //List<Long> dbIds = Env.getCurrentInternalCatalog().getDbIds();
        for (Long dbId : dbIds) {
            Database db = Env.getCurrentInternalCatalog().getDbNullable(dbId);
            if (db == null) {
                continue;
            }
            List<Table> tableList = db.getTables();
            for (Table table : tableList) {
                if (table.getType() != TableType.OLAP) {
                    continue;
                }
                OlapTable olapTable = (OlapTable) table;
                if (!table.writeLockIfExist()) {
                    continue;
                }
                try {
                    for (Partition partition : olapTable.getAllPartitions()) {
                        for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                            long indexRowCount = 0L;
                            for (Tablet tablet : index.getTablets()) {
                                long tabletRowCount = 0L;
                                for (Replica replica : tablet.getReplicas()) {
                                    if (replica.getRowCount() > tabletRowCount) {
                                        tabletRowCount = replica.getRowCount();
                                    }
                                }
                                indexRowCount += tabletRowCount;
                            } // end for tablets
                            index.setRowCount(indexRowCount);
                        } // end for indices
                    } // end for partitions
                    LOG.debug("finished to set row num for table: {} in database: {}",
                             table.getName(), db.getFullName());
                } finally {
                    table.writeUnlock();
                }
                reportTableStat(olapTable);
            }
        }
        LOG.info("finished to update index row num of all databases. cost: {} ms",
                (System.currentTimeMillis() - start));
    }

    private void reportTableStat(OlapTable table) {
        Long tableRowCount = 0L;
        Long tableSegmentCount = 0L;
        Long tableRowsetCount = 0L;
        Long tableDataSize = 0L;
        String dbName = table.getDBName();
        String tableName = table.name;
        String metricKey = dbName + "." + table.name;
        table.readLock();
        try {
            for (Partition partition : table.getAllPartitions()) {
                Long partitionRowCount = 0L;
                Long partitionSegmentCount = 0L;
                Long partitionRowsetCount = 0L;
                Long partitionDataSize = 0L;
                for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                    Long indexRowCount = 0L;
                    Long indexSegmentCount = 0L;
                    Long indexRowsetCount = 0L;
                    Long indexDataSize = 0L;
                    for (Tablet tablet : index.getTablets()) {
                        Replica replica = tablet.getReplicas().get(0);
                        indexRowCount += replica.getRowCount();
                        indexSegmentCount += replica.getSegmentCount();
                        indexRowsetCount += replica.getRowsetCount();
                        indexDataSize += replica.getDataSize();
                    } // end for tablets
                    partitionRowCount += indexRowCount;
                    partitionSegmentCount += indexSegmentCount;
                    partitionRowsetCount += indexRowsetCount;
                    partitionDataSize += indexDataSize;
                } // end for indices
                tableRowCount += partitionRowCount;
                tableSegmentCount += partitionSegmentCount;
                tableRowsetCount += partitionRowsetCount;
                tableDataSize += partitionDataSize;
            } // end for partitions
        } finally {
            table.readUnlock();
        }
        MetricRepo.CLOUD_DB_TABLE_DATA_SIZE.computeIfAbsent(metricKey, key -> {
            GaugeMetricImpl<Long> gaugeDataSize = new GaugeMetricImpl<>("table_data_size", MetricUnit.BYTES,
                    "table data size");
            gaugeDataSize.addLabel(new MetricLabel("db_name", dbName));
            gaugeDataSize.addLabel(new MetricLabel("table_name", tableName));
            MetricRepo.DORIS_METRIC_REGISTER.addMetrics(gaugeDataSize);
            return gaugeDataSize;
        }).setValue(tableDataSize);
        MetricRepo.CLOUD_DB_TABLE_ROWSET_COUNT.computeIfAbsent(metricKey, key -> {
            GaugeMetricImpl<Long> gaugeRowsetCount = new GaugeMetricImpl<>("table_rowset_count", MetricUnit.ROWSETS,
                    "table rowset count");
            gaugeRowsetCount.addLabel(new MetricLabel("db_name", dbName));
            gaugeRowsetCount.addLabel(new MetricLabel("table_name", tableName));
            MetricRepo.DORIS_METRIC_REGISTER.addMetrics(gaugeRowsetCount);
            return gaugeRowsetCount;
        }).setValue(tableRowsetCount);
        MetricRepo.CLOUD_DB_TABLE_SEGMENT_COUNT.computeIfAbsent(metricKey, key -> {
            GaugeMetricImpl<Long> gaugeSegmentCount = new GaugeMetricImpl<>("table_segment_count", MetricUnit.NOUNIT,
                    "table segment count");
            gaugeSegmentCount.addLabel(new MetricLabel("db_name", dbName));
            gaugeSegmentCount.addLabel(new MetricLabel("table_name", tableName));
            MetricRepo.DORIS_METRIC_REGISTER.addMetrics(gaugeSegmentCount);
            return gaugeSegmentCount;
        }).setValue(tableSegmentCount);
        MetricRepo.CLOUD_DB_TABLE_ROW_COUNT.computeIfAbsent(metricKey, key -> {
            GaugeMetricImpl<Long> gaugeRowCount = new GaugeMetricImpl<>("table_row_count", MetricUnit.ROWS,
                    "table row count");
            gaugeRowCount.addLabel(new MetricLabel("db_name", dbName));
            gaugeRowCount.addLabel(new MetricLabel("table_name", tableName));
            MetricRepo.DORIS_METRIC_REGISTER.addMetrics(gaugeRowCount);
            return gaugeRowCount;
        }).setValue(tableRowCount);
    }

    private void updateTabletStat(SelectdbCloud.GetTabletStatsResponse result) {
        TabletInvertedIndex invertedIndex = Env.getCurrentInvertedIndex();
        for (SelectdbCloud.TabletStatsPB stat : result.getTabletStatsList()) {
            if (invertedIndex.getTabletMeta(stat.getIdx().getTabletId()) != null) {
                List<Replica> replicas = invertedIndex.getReplicasByTabletId(stat.getIdx().getTabletId());
                if (replicas != null && !replicas.isEmpty() && replicas.get(0) != null) {
                    replicas.get(0).updateCloudStat(stat.getDataSize(), stat.getNumRowsets(),
                            stat.getNumSegments(), stat.getNumRows());
                }
            }
        }
    }

    private SelectdbCloud.GetTabletStatsResponse getTabletStats(SelectdbCloud.GetTabletStatsRequest req)
            throws RpcException {
        SelectdbCloud.GetTabletStatsResponse response;
        try {
            response = MetaServiceProxy.getInstance().getTabletStats(req);
        } catch (RpcException e) {
            LOG.info("get tablet stat get exception {}", e);
            throw e;
        }

        return response;
    }
}
