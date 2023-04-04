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

package com.selectdb.cloud.catalog;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.persist.UpdateCloudReplicaInfo;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class CloudTabletRebalancer extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(CloudTabletRebalancer.class);

    private Map<Long, List<Tablet>> beToTabletsGlobal;
    private Map<String, List<Long>> clusterToBes;
    private List<UpdateCloudReplicaInfo> replicaInfos;

    // partitionId -> indexId -> be -> tablet
    private Map<Long, Map<Long, Map<Long, List<Tablet>>>> partitionToTablets;

    private Random rand = new Random();

    private boolean indexBalanced = true;

    public CloudTabletRebalancer() {
        super("cloud tablet rebalancer", Config.tablet_rebalancer_interval_second * 1000);
    }

    private interface Operator {
        void op(Database db, Table table, Partition partition, MaterializedIndex index, String cluster);
    }

    // 1 build cluster to backends info
    // 2 complete route info
    // 3 Statistics cluster to backend mapping information
    // 4 balance in partitions/index
    // 5 if the partition already balanced, perform global balance
    @Override
    protected void runAfterCatalogReady() {
        LOG.info("cloud tablet rebalance begin");
        beToTabletsGlobal = new HashMap<Long, List<Tablet>>();
        partitionToTablets = new HashMap<Long, Map<Long, Map<Long, List<Tablet>>>>();
        clusterToBes = new HashMap<String, List<Long>>();
        long start = System.currentTimeMillis();

        // build cluster to backend info
        SystemInfoService systemInfoService = Env.getCurrentSystemInfo();
        for (Long beId : systemInfoService.getBackendIds(true)) {
            Backend be = systemInfoService.getBackend(beId);
            clusterToBes.putIfAbsent(be.getCloudClusterId(), new ArrayList<Long>());
            clusterToBes.get(be.getCloudClusterId()).add(beId);
        }
        LOG.info("cluster to backends {}", clusterToBes);

        // complete route info
        replicaInfos = new ArrayList<UpdateCloudReplicaInfo>();
        completeRouteInfo();
        for (UpdateCloudReplicaInfo info : replicaInfos) {
            Env.getCurrentEnv().getEditLog().logUpdateCloudReplica(info);
        }

        // Statistics cluster to be mapping information
        statRouteInfo();

        for (Map.Entry<Long, List<Tablet>> entry : beToTabletsGlobal.entrySet()) {
            LOG.info("before index balance be {} tablet num {}", entry.getKey(), entry.getValue().size());
        }

        // balance in partitions/index
        indexBalanced = true;
        for (Map.Entry<String, List<Long>> entry : clusterToBes.entrySet()) {
            balanceInPartition(entry.getValue(), entry.getKey());
        }

        for (Map.Entry<Long, List<Tablet>> entry : beToTabletsGlobal.entrySet()) {
            LOG.info("after index balance be {} tablet num {}", entry.getKey(), entry.getValue().size());
        }

        // if the partition/index already balanced, perform global balance
        if (indexBalanced) {
            for (Map.Entry<Long, List<Tablet>> entry : beToTabletsGlobal.entrySet()) {
                LOG.info("before global balance be {} tablet num {}", entry.getKey(), entry.getValue().size());
            }

            for (Map.Entry<String, List<Long>> entry : clusterToBes.entrySet()) {
                balanceImpl(entry.getValue(), entry.getKey(), beToTabletsGlobal, true);
            }

            for (Map.Entry<Long, List<Tablet>> entry : beToTabletsGlobal.entrySet()) {
                LOG.info("after global balance be {} tablet num {}", entry.getKey(), entry.getValue().size());
            }
        }

        LOG.info("finished to rebalancer. cost: {} ms", (System.currentTimeMillis() - start));
    }

    private void completeRouteInfo() {
        loopCloudReplica((Database db, Table table, Partition partition, MaterializedIndex index, String cluster) -> {
            boolean assigned = false;
            List<Long> beIds = new ArrayList<Long>();
            List<Long> tabletIds = new ArrayList<Long>();
            for (Tablet tablet : index.getTablets()) {
                for (Replica replica : tablet.getReplicas()) {
                    Map<String, List<Long>> clusterToBackends =
                            ((CloudReplica) replica).getClusterToBackends();
                    if (!clusterToBackends.containsKey(cluster)) {
                        long beId = ((CloudReplica) replica).hashReplicaToBe(cluster);
                        List<Long> bes = new ArrayList<Long>();
                        bes.add(beId);
                        clusterToBackends.put(cluster, bes);

                        assigned = true;
                        beIds.add(beId);
                        tabletIds.add(tablet.getId());
                    } else {
                        beIds.add(clusterToBackends.get(cluster).get(0));
                        tabletIds.add(tablet.getId());
                    }
                }
            }

            if (assigned) {
                UpdateCloudReplicaInfo info = new UpdateCloudReplicaInfo(db.getId(), table.getId(),
                        partition.getId(), index.getId(), cluster, beIds, tabletIds);
                replicaInfos.add(info);
            }
        });
    }

    public void statRouteInfo() {
        loopCloudReplica((Database db, Table table, Partition partition, MaterializedIndex index, String cluster) -> {
            for (Tablet tablet : index.getTablets()) {
                for (Replica replica : tablet.getReplicas()) {
                    Map<String, List<Long>> clusterToBackends =
                            ((CloudReplica) replica).getClusterToBackends();
                    for (List<Long> bes : clusterToBackends.values()) {
                        beToTabletsGlobal.putIfAbsent(bes.get(0), new ArrayList<Tablet>());
                        beToTabletsGlobal.get(bes.get(0)).add(tablet);

                        partitionToTablets.putIfAbsent(partition.getId(),
                                                       new HashMap<Long, Map<Long, List<Tablet>>>());
                        Map<Long, Map<Long, List<Tablet>>> indexToTablets
                                = partitionToTablets.get(partition.getId());
                        indexToTablets.putIfAbsent(index.getId(),
                                                   new HashMap<Long, List<Tablet>>());
                        Map<Long, List<Tablet>> beToTabletsOfIndex
                                = indexToTablets.get(index.getId());
                        beToTabletsOfIndex.putIfAbsent(bes.get(0), new ArrayList<Tablet>());
                        beToTabletsOfIndex.get(bes.get(0)).add(tablet);
                    }
                }
            }
        });
    }

    public void loopCloudReplica(Operator operator) {
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
                OlapTable olapTable = (OlapTable) table;
                if (!table.writeLockIfExist()) {
                    continue;
                }

                try {
                    for (Partition partition : olapTable.getAllPartitions()) {
                        for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                            for (Map.Entry<String, List<Long>> entry : clusterToBes.entrySet()) {
                                String cluster = entry.getKey();
                                operator.op(db, table, partition, index, cluster);
                            }
                        } // end for indices
                    } // end for partitions
                } finally {
                    table.writeUnlock();
                }
            }
        }
    }

    public void balanceInPartition(List<Long> bes, String clusterId) {
        // balance all partition
        for (Map.Entry<Long, Map<Long, Map<Long, List<Tablet>>>> partitionEntry : partitionToTablets.entrySet()) {
            Map<Long, Map<Long, List<Tablet>>> indexToTablets = partitionEntry.getValue();
            // balance all index of a partition
            for (Map.Entry<Long, Map<Long, List<Tablet>>> entry : indexToTablets.entrySet()) {
                // balance a index
                balanceImpl(bes, clusterId, entry.getValue(), false);
            }
        }
    }

    private void balanceImpl(List<Long> bes, String clusterId, Map<Long, List<Tablet>> beToTablets,
            boolean isGlobal) {
        if (bes == null || bes.isEmpty() || beToTablets == null || beToTablets.isEmpty()) {
            return;
        }

        long totalTabletsNum = 0;
        for (Long be : bes) {
            long tabletNum = beToTablets.get(be) == null ? 0 : beToTablets.get(be).size();
            totalTabletsNum += tabletNum;
        }
        long avgNum = totalTabletsNum / bes.size();
        long transferNum = Math.max(Math.round(avgNum * Config.balance_tablet_percent_per_run),
                                    Config.min_balance_tablet_num_per_run);

        for (int i = 0; i < transferNum; i++) {
            long minBe = bes.get(0);
            long maxBe = bes.get(0);

            long minTabletsNum = Long.MAX_VALUE;
            long maxTabletsNum = 0;
            long beNum = bes.size();

            for (Long be : bes) {
                long tabletNum = beToTablets.get(be) == null ? 0 : beToTablets.get(be).size();
                if (tabletNum > maxTabletsNum) {
                    maxBe = be;
                    maxTabletsNum = tabletNum;
                }

                if (tabletNum < minTabletsNum) {
                    minBe = be;
                    minTabletsNum = tabletNum;
                }
            }

            if ((maxTabletsNum < avgNum * (1 + Config.cloud_rebalance_percent_threshold)
                    && minTabletsNum > avgNum * (1 - Config.cloud_rebalance_percent_threshold))
                    || minTabletsNum > maxTabletsNum - Config.cloud_rebalance_number_threshold) {
                return;
            }

            int randomIndex = rand.nextInt(beToTablets.get(maxBe).size());
            Tablet pickedTablet = beToTablets.get(maxBe).get(randomIndex);
            CloudReplica cloudReplica = (CloudReplica) pickedTablet.getReplicas().get(0);

            if (isGlobal) {
                // update clusterToBackens
                long maxBeSize = partitionToTablets.get(cloudReplica.getPartitionId())
                        .get(cloudReplica.getIndexId()).get(maxBe).size();
                long minBeSize = partitionToTablets.get(cloudReplica.getPartitionId())
                        .get(cloudReplica.getIndexId()).get(minBe).size();
                if (minBeSize >= maxBeSize) {
                    return;
                }
                cloudReplica.updateClusterToBe(clusterId, minBe);

                LOG.info("transfer {} from {} to {} cluster {} minNum {} maxNum {} beNum {} totalTabletsNum {}",
                         pickedTablet.getId(), maxBe, minBe, clusterId,
                         minTabletsNum, maxTabletsNum, beNum, totalTabletsNum);

                beToTabletsGlobal.get(maxBe).remove(randomIndex);
                beToTabletsGlobal.putIfAbsent(minBe, new ArrayList<Tablet>());
                beToTabletsGlobal.get(minBe).add(pickedTablet);
            } else {
                indexBalanced = false;

                // update clusterToBackens
                cloudReplica.updateClusterToBe(clusterId, minBe);
                LOG.info("transfer {} from {} to {} cluster {} minNum {} maxNum {} beNum {} totalTabletsNum {} part {}",
                         pickedTablet.getId(), maxBe, minBe, clusterId,
                         minTabletsNum, maxTabletsNum, beNum, totalTabletsNum, cloudReplica.getPartitionId());

                beToTabletsGlobal.get(maxBe).remove(randomIndex);
                beToTabletsGlobal.putIfAbsent(minBe, new ArrayList<Tablet>());
                beToTabletsGlobal.get(minBe).add(pickedTablet);

                beToTablets.get(maxBe).remove(randomIndex);
                beToTablets.putIfAbsent(minBe, new ArrayList<Tablet>());
                beToTablets.get(minBe).add(pickedTablet);
            }

            Database db = Env.getCurrentInternalCatalog().getDbNullable(cloudReplica.getDbId());
            if (db == null) {
                continue;
            }
            OlapTable table = (OlapTable) db.getTableNullable(cloudReplica.getTableId());
            if (table == null) {
                continue;
            }

            table.readLock();

            try {
                if (db.getTableNullable(cloudReplica.getTableId()) == null) {
                    continue;
                }

                UpdateCloudReplicaInfo info = new UpdateCloudReplicaInfo(cloudReplica.getDbId(),
                        cloudReplica.getTableId(), cloudReplica.getPartitionId(), cloudReplica.getIndexId(),
                        pickedTablet.getId(), cloudReplica.getId(), clusterId, minBe);
                Env.getCurrentEnv().getEditLog().logUpdateCloudReplica(info);
            } finally {
                table.readUnlock();
            }
        }
    }
}

