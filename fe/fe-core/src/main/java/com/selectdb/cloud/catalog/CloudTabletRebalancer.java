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

import com.selectdb.cloud.proto.SelectdbCloud;
import com.selectdb.cloud.rpc.MetaServiceProxy;

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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.persist.UpdateCloudReplicaInfo;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.BackendService;
import org.apache.doris.thrift.TCheckPreCacheRequest;
import org.apache.doris.thrift.TCheckPreCacheResponse;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPreCacheAsyncRequest;
import org.apache.doris.thrift.TPreCacheAsyncResponse;
import org.apache.doris.thrift.TStatusCode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;

public class CloudTabletRebalancer extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(CloudTabletRebalancer.class);

    private Map<Long, List<Tablet>> beToTabletsGlobal;

    private Map<Long, List<Tablet>> futureBeToTabletsGlobal;

    private Map<String, List<Long>> clusterToBes;

    private List<UpdateCloudReplicaInfo> replicaInfos;

    // partitionId -> indexId -> be -> tablet
    private Map<Long, Map<Long, Map<Long, List<Tablet>>>> partitionToTablets;

    private Map<Long, Map<Long, Map<Long, List<Tablet>>>> futurePartitionToTablets;

    private Map<Long, Long> beToDecommissionedTime = new HashMap<Long, Long>();

    private Random rand = new Random();

    private boolean indexBalanced = true;

    private LinkedBlockingQueue<Pair<Long, Long>> tabletsMigrateTasks = new LinkedBlockingQueue<Pair<Long, Long>>();

    private Map<Long, InfightTask> tabletToInfightTask = new HashMap<Long, InfightTask>();

    public CloudTabletRebalancer() {
        super("cloud tablet rebalancer", Config.tablet_rebalancer_interval_second * 1000);
    }

    private interface Operator {
        void op(Database db, Table table, Partition partition, MaterializedIndex index, String cluster);
    }

    private class InfightTask {
        public Tablet pickedTablet;
        public long srcBe;
        public long destBe;
        public boolean isGlobal;
        public String clusterId;
        public Map<Long, List<Tablet>> beToTablets;
    }

    // 1 build cluster to backends info
    // 2 complete route info
    // 3 check whether the inflight preheating task has been completed
    // 4 migrate tablet for smooth upgrade
    // 5 statistics be to tablets mapping information
    // 6 partition-level balance
    // 7 if tablets in partition-level already balanced, perform global balance
    // 8 check whether all tablets of decomission node have been migrated
    @Override
    protected void runAfterCatalogReady() {
        LOG.info("cloud tablet rebalance begin");
        beToTabletsGlobal = new HashMap<Long, List<Tablet>>();
        partitionToTablets = new HashMap<Long, Map<Long, Map<Long, List<Tablet>>>>();
        futureBeToTabletsGlobal = new HashMap<Long, List<Tablet>>();
        futurePartitionToTablets = new HashMap<Long, Map<Long, Map<Long, List<Tablet>>>>();

        clusterToBes = new HashMap<String, List<Long>>();
        long start = System.currentTimeMillis();

        // 1 build cluster to backend info
        SystemInfoService systemInfoService = Env.getCurrentSystemInfo();
        for (Long beId : systemInfoService.getBackendIds(false)) {
            Backend be = systemInfoService.getBackend(beId);
            clusterToBes.putIfAbsent(be.getCloudClusterId(), new ArrayList<Long>());
            clusterToBes.get(be.getCloudClusterId()).add(beId);
        }
        LOG.info("cluster to backends {}", clusterToBes);

        // 2 complete route info
        replicaInfos = new ArrayList<UpdateCloudReplicaInfo>();
        completeRouteInfo();
        for (UpdateCloudReplicaInfo info : replicaInfos) {
            Env.getCurrentEnv().getEditLog().logUpdateCloudReplica(info);
        }

        // 3 check whether the inflight preheating task has been completed
        checkInflghtPreCache();

        // 4 migrate tablet for smooth upgrade
        Pair<Long, Long> pair;
        while (!tabletsMigrateTasks.isEmpty()) {
            try {
                pair = tabletsMigrateTasks.take();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            LOG.info("begin tablets migration from be {} to be {}", pair.first, pair.second);
            migrateTablets(pair.first, pair.second);
        }

        // 5 statistics be to tablets mapping information
        statRouteInfo();

        // 6 partition-level balance
        balanceAllPartitions();

        // 7 if tablets in partition-level already balanced, perform global balance
        globalBalance();

        // 8 check whether all tablets of decomission have been migrated
        checkDecommissionState(clusterToBes);

        LOG.info("finished to rebalancer. cost: {} ms", (System.currentTimeMillis() - start));
    }

    public void balanceAllPartitions() {
        for (Map.Entry<Long, List<Tablet>> entry : beToTabletsGlobal.entrySet()) {
            LOG.info("before partition balance be {} tablet num {}", entry.getKey(), entry.getValue().size());
        }

        for (Map.Entry<Long, List<Tablet>> entry : futureBeToTabletsGlobal.entrySet()) {
            LOG.info("before partition balance be {} tablet num(current + pre heating inflight) {}",
                    entry.getKey(), entry.getValue().size());
        }

        // balance in partitions/index
        indexBalanced = true;
        for (Map.Entry<String, List<Long>> entry : clusterToBes.entrySet()) {
            balanceInPartition(entry.getValue(), entry.getKey());
        }

        for (Map.Entry<Long, List<Tablet>> entry : beToTabletsGlobal.entrySet()) {
            LOG.info("after partition balance be {} tablet num {}", entry.getKey(), entry.getValue().size());
        }

        for (Map.Entry<Long, List<Tablet>> entry : futureBeToTabletsGlobal.entrySet()) {
            LOG.info("after partition balance be {} tablet num(current + pre heating inflight) {}",
                    entry.getKey(), entry.getValue().size());
        }
    }

    public void globalBalance() {
        if (indexBalanced) {
            for (Map.Entry<Long, List<Tablet>> entry : beToTabletsGlobal.entrySet()) {
                LOG.info("before global balance be {} tablet num {}", entry.getKey(), entry.getValue().size());
            }

            for (Map.Entry<Long, List<Tablet>> entry : futureBeToTabletsGlobal.entrySet()) {
                LOG.info("before global balance be {} tablet num(current + pre heating inflight) {}",
                        entry.getKey(), entry.getValue().size());
            }

            for (Map.Entry<String, List<Long>> entry : clusterToBes.entrySet()) {
                balanceImpl(entry.getValue(), entry.getKey(), futureBeToTabletsGlobal, true);
            }

            for (Map.Entry<Long, List<Tablet>> entry : beToTabletsGlobal.entrySet()) {
                LOG.info("after global balance be {} tablet num {}", entry.getKey(), entry.getValue().size());
            }

            for (Map.Entry<Long, List<Tablet>> entry : futureBeToTabletsGlobal.entrySet()) {
                LOG.info("after global balance be {} tablet num(current + pre heating inflight) {}",
                        entry.getKey(), entry.getValue().size());
            }
        }
    }

    public void checkInflghtPreCache() {
        Map<Long, List<Long>> beToTabletIds = new HashMap<Long, List<Long>>();

        for (Map.Entry<Long, InfightTask> entry : tabletToInfightTask.entrySet()) {
            beToTabletIds.putIfAbsent(entry.getValue().destBe, new ArrayList<Long>());
            beToTabletIds.get(entry.getValue().destBe).add(entry.getValue().pickedTablet.getId());
        }

        for (Map.Entry<Long, List<Long>> entry : beToTabletIds.entrySet()) {
            LOG.info("before pre cache check dest be {} inflight task num {}", entry.getKey(), entry.getValue().size());
            Backend destBackend = Env.getCurrentSystemInfo().getBackend(entry.getKey());
            if (destBackend == null) {
                for (long tabletId : entry.getValue()) {
                    tabletToInfightTask.remove(tabletId);
                }
                continue;
            }

            Map<Long, Boolean> taskDone = sendCheckPreCacheRpc(entry.getValue(), entry.getKey());

            for (Map.Entry<Long, Boolean> result : taskDone.entrySet()) {
                if (result.getValue()) {
                    InfightTask task = tabletToInfightTask.get(result.getKey());
                    updateClusterToBeMap(task.pickedTablet, task.destBe, task.clusterId);
                    tabletToInfightTask.remove(result.getKey());
                }
            }
        }

        // recalculate inflight beToTablets, just for print the log
        beToTabletIds = new HashMap<Long, List<Long>>();
        for (Map.Entry<Long, InfightTask> entry : tabletToInfightTask.entrySet()) {
            beToTabletIds.putIfAbsent(entry.getValue().destBe, new ArrayList<Long>());
            beToTabletIds.get(entry.getValue().destBe).add(entry.getValue().pickedTablet.getId());
        }

        for (Map.Entry<Long, List<Long>> entry : beToTabletIds.entrySet()) {
            LOG.info("after pre cache check dest be {} inflight task num {}", entry.getKey(), entry.getValue().size());
        }
    }

    public void checkDecommissionState(Map<String, List<Long>> clusterToBes) {
        for (Map.Entry<String, List<Long>> entry : clusterToBes.entrySet()) {
            List<Long> beList = entry.getValue();
            for (long beId : beList) {
                long tabletNum = beToTabletsGlobal.get(beId) == null ? 0 : beToTabletsGlobal.get(beId).size();
                Backend backend = Env.getCurrentSystemInfo().getBackend(beId);
                if (backend.isDecommissioned() && tabletNum == 0 && !backend.isActive()) {
                    if (!beToDecommissionedTime.containsKey(beId)) {
                        LOG.info("prepare to notify meta service be {} decommissioned", backend.getId());
                        SelectdbCloud.AlterClusterRequest.Builder builder =
                                SelectdbCloud.AlterClusterRequest.newBuilder();
                        builder.setCloudUniqueId(Config.cloud_unique_id);
                        builder.setOp(SelectdbCloud.AlterClusterRequest.Operation.NOTIFY_DECOMMISSIONED);

                        SelectdbCloud.ClusterPB.Builder clusterBuilder =
                                SelectdbCloud.ClusterPB.newBuilder();
                        clusterBuilder.setClusterName(backend.getCloudClusterName());
                        clusterBuilder.setClusterId(backend.getCloudClusterId());
                        clusterBuilder.setType(SelectdbCloud.ClusterPB.Type.COMPUTE);

                        SelectdbCloud.NodeInfoPB.Builder nodeBuilder =
                                SelectdbCloud.NodeInfoPB.newBuilder();
                        nodeBuilder.setIp(backend.getHost());
                        nodeBuilder.setHeartbeatPort(backend.getHeartbeatPort());
                        nodeBuilder.setCloudUniqueId(backend.getCloudUniqueId());
                        nodeBuilder.setStatus(SelectdbCloud.NodeStatusPB.NODE_STATUS_DECOMMISSIONED);

                        clusterBuilder.addNodes(nodeBuilder);
                        builder.setCluster(clusterBuilder);

                        SelectdbCloud.AlterClusterResponse response;
                        try {
                            response = MetaServiceProxy.getInstance().alterCluster(builder.build());
                            if (response.getStatus().getCode() != SelectdbCloud.MetaServiceCode.OK) {
                                LOG.warn("notify decommission response: {}", response);
                            }
                            LOG.info("notify decommission response: {} ", response);
                        } catch (RpcException e) {
                            LOG.info("failed to notify decommission {}", e);
                            return;
                        }
                        beToDecommissionedTime.put(beId, System.currentTimeMillis() / 1000);
                    }
                }
            }
        }
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

    public void fillBeToTablets(long be, Partition partition, MaterializedIndex index, Tablet tablet,
            Map<Long, List<Tablet>> globalBeToTablets,
            Map<Long, Map<Long, Map<Long, List<Tablet>>>> partToTablets) {
        globalBeToTablets.putIfAbsent(be, new ArrayList<Tablet>());
        globalBeToTablets.get(be).add(tablet);

        partToTablets.putIfAbsent(partition.getId(),
                                       new HashMap<Long, Map<Long, List<Tablet>>>());
        Map<Long, Map<Long, List<Tablet>>> indexToTablets
                = partToTablets.get(partition.getId());
        indexToTablets.putIfAbsent(index.getId(),
                                   new HashMap<Long, List<Tablet>>());
        Map<Long, List<Tablet>> beToTabletsOfIndex
                = indexToTablets.get(index.getId());
        beToTabletsOfIndex.putIfAbsent(be, new ArrayList<Tablet>());
        beToTabletsOfIndex.get(be).add(tablet);
    }

    public void statRouteInfo() {
        loopCloudReplica((Database db, Table table, Partition partition, MaterializedIndex index, String cluster) -> {
            for (Tablet tablet : index.getTablets()) {
                for (Replica replica : tablet.getReplicas()) {
                    Map<String, List<Long>> clusterToBackends =
                            ((CloudReplica) replica).getClusterToBackends();
                    for (List<Long> bes : clusterToBackends.values()) {
                        fillBeToTablets(bes.get(0), partition, index, tablet, this.beToTabletsGlobal,
                                this.partitionToTablets);

                        if (tabletToInfightTask.containsKey(tablet.getId())) {
                            InfightTask task = tabletToInfightTask.get(tablet.getId());
                            fillBeToTablets(task.destBe, partition, index, tablet, futureBeToTabletsGlobal,
                                            futurePartitionToTablets);
                        } else {
                            fillBeToTablets(bes.get(0), partition, index, tablet, futureBeToTabletsGlobal,
                                            futurePartitionToTablets);
                        }
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
        for (Map.Entry<Long, Map<Long, Map<Long, List<Tablet>>>> partitionEntry : futurePartitionToTablets.entrySet()) {
            Map<Long, Map<Long, List<Tablet>>> indexToTablets = partitionEntry.getValue();
            // balance all index of a partition
            for (Map.Entry<Long, Map<Long, List<Tablet>>> entry : indexToTablets.entrySet()) {
                // balance a index
                balanceImpl(bes, clusterId, entry.getValue(), false);
            }
        }
    }

    private void sendPreHeatingRpc(Tablet pickedTablet, long srcBe, long destBe) {
        BackendService.Client client = null;
        TNetworkAddress address = null;
        Backend srcBackend = Env.getCurrentSystemInfo().getBackend(srcBe);
        Backend destBackend = Env.getCurrentSystemInfo().getBackend(destBe);
        try {
            address = new TNetworkAddress(destBackend.getHost(), destBackend.getBePort());
            client = ClientPool.backendPool.borrowObject(address);
            TPreCacheAsyncRequest req = new TPreCacheAsyncRequest();
            req.setHost(srcBackend.getHost());
            req.setBrpcPort(srcBackend.getBrpcPort());
            List<Long> tablets = new ArrayList<Long>();
            tablets.add(pickedTablet.getId());
            req.setTabletIds(tablets);
            TPreCacheAsyncResponse result = client.preCacheAsync(req);
            if (result.getStatus().getStatusCode() != TStatusCode.OK) {
                LOG.warn("pre cache failed status {} {}", result.getStatus().getStatusCode(),
                        result.getStatus().getErrorMsgs());
            }
        } catch (Exception e) {
            LOG.warn("send pre heating rpc error. backend[{}]", destBackend.getId(), e);
        }
    }

    private Map<Long, Boolean> sendCheckPreCacheRpc(List<Long> tabletIds, long be) {
        BackendService.Client client = null;
        TNetworkAddress address = null;
        Backend destBackend = Env.getCurrentSystemInfo().getBackend(be);
        try {
            address = new TNetworkAddress(destBackend.getHost(), destBackend.getBePort());
            client = ClientPool.backendPool.borrowObject(address);
            TCheckPreCacheRequest req = new TCheckPreCacheRequest();
            req.setTablets(tabletIds);
            TCheckPreCacheResponse result = client.checkPreCache(req);
            if (result.getStatus().getStatusCode() != TStatusCode.OK) {
                LOG.warn("check pre cache status {} {}", result.getStatus().getStatusCode(),
                        result.getStatus().getErrorMsgs());
            } else {
                LOG.info("check pre cache succ status {} {}", result.getStatus().getStatusCode(),
                        result.getStatus().getErrorMsgs());
            }
            return result.getTaskDone();
        } catch (Exception e) {
            LOG.warn("send check pre cache rpc error. backend[{}]", destBackend.getId(), e);
        }
        return null;
    }

    private void updateBeToTablets(Tablet pickedTablet, long srcBe, long destBe, boolean isGlobal,
            String clusterId, Map<Long, List<Tablet>> beToTablets,
            Map<Long, List<Tablet>> globalBeToTablets) {
        if (isGlobal) {
            globalBeToTablets.get(srcBe).remove(pickedTablet);
            globalBeToTablets.putIfAbsent(destBe, new ArrayList<Tablet>());
            globalBeToTablets.get(destBe).add(pickedTablet);
        } else {
            indexBalanced = false;

            globalBeToTablets.get(srcBe).remove(pickedTablet);
            globalBeToTablets.putIfAbsent(destBe, new ArrayList<Tablet>());
            globalBeToTablets.get(destBe).add(pickedTablet);

            beToTablets.get(srcBe).remove(pickedTablet);
            beToTablets.putIfAbsent(destBe, new ArrayList<Tablet>());
            beToTablets.get(destBe).add(pickedTablet);
        }
    }

    private void updateClusterToBeMap(Tablet pickedTablet, long destBe, String clusterId) {
        CloudReplica cloudReplica = (CloudReplica) pickedTablet.getReplicas().get(0);
        cloudReplica.updateClusterToBe(clusterId, destBe);
        Database db = Env.getCurrentInternalCatalog().getDbNullable(cloudReplica.getDbId());
        if (db == null) {
            return;
        }
        OlapTable table = (OlapTable) db.getTableNullable(cloudReplica.getTableId());
        if (table == null) {
            return;
        }

        table.readLock();

        try {
            if (db.getTableNullable(cloudReplica.getTableId()) == null) {
                return;
            }

            UpdateCloudReplicaInfo info = new UpdateCloudReplicaInfo(cloudReplica.getDbId(),
                    cloudReplica.getTableId(), cloudReplica.getPartitionId(), cloudReplica.getIndexId(),
                    pickedTablet.getId(), cloudReplica.getId(), clusterId, destBe);
            Env.getCurrentEnv().getEditLog().logUpdateCloudReplica(info);
        } finally {
            table.readUnlock();
        }
    }

    private void balanceImpl(List<Long> bes, String clusterId, Map<Long, List<Tablet>> beToTablets,
            boolean isGlobal) {
        if (bes == null || bes.isEmpty() || beToTablets == null || beToTablets.isEmpty()) {
            return;
        }

        long totalTabletsNum = 0;
        long beNum = 0;
        for (Long be : bes) {
            long tabletNum = beToTablets.get(be) == null ? 0 : beToTablets.get(be).size();
            Backend backend = Env.getCurrentSystemInfo().getBackend(be);
            if (!backend.isDecommissioned()) {
                beNum++;
            }
            totalTabletsNum += tabletNum;
        }
        long avgNum = totalTabletsNum / beNum;
        long transferNum = Math.max(Math.round(avgNum * Config.balance_tablet_percent_per_run),
                                    Config.min_balance_tablet_num_per_run);

        for (int i = 0; i < transferNum; i++) {
            long destBe = bes.get(0);
            long srcBe = bes.get(0);

            long minTabletsNum = Long.MAX_VALUE;
            long maxTabletsNum = 0;
            boolean srcDecommissioned = false;

            for (Long be : bes) {
                long tabletNum = beToTablets.get(be) == null ? 0 : beToTablets.get(be).size();
                if (tabletNum > maxTabletsNum) {
                    srcBe = be;
                    maxTabletsNum = tabletNum;
                }

                Backend backend = Env.getCurrentSystemInfo().getBackend(be);
                if (tabletNum < minTabletsNum && !backend.isDecommissioned()) {
                    destBe = be;
                    minTabletsNum = tabletNum;
                }
            }

            for (Long be : bes) {
                long tabletNum = beToTablets.get(be) == null ? 0 : beToTablets.get(be).size();
                Backend backend = Env.getCurrentSystemInfo().getBackend(be);
                if (backend.isDecommissioned() && tabletNum > 0) {
                    srcBe = be;
                    srcDecommissioned = true;
                    break;
                }
            }

            if (!srcDecommissioned) {
                if ((maxTabletsNum < avgNum * (1 + Config.cloud_rebalance_percent_threshold)
                        && minTabletsNum > avgNum * (1 - Config.cloud_rebalance_percent_threshold))
                        || minTabletsNum > maxTabletsNum - Config.cloud_rebalance_number_threshold) {
                    return;
                }
            }

            int randomIndex = rand.nextInt(beToTablets.get(srcBe).size());
            Tablet pickedTablet = beToTablets.get(srcBe).get(randomIndex);
            CloudReplica cloudReplica = (CloudReplica) pickedTablet.getReplicas().get(0);

            if (Config.preheating_enabled) {
                if (isGlobal) {
                    long maxBeSize = futurePartitionToTablets.get(cloudReplica.getPartitionId())
                            .get(cloudReplica.getIndexId()).get(srcBe).size();
                    List<Tablet> destBeTablets = futurePartitionToTablets.get(cloudReplica.getPartitionId())
                            .get(cloudReplica.getIndexId()).get(destBe);
                    long minBeSize = destBeTablets == null ? 0 : destBeTablets.size();
                    if (minBeSize >= maxBeSize) {
                        continue;
                    }
                }

                sendPreHeatingRpc(pickedTablet, srcBe, destBe);

                InfightTask task = new InfightTask();
                task.pickedTablet = pickedTablet;
                task.srcBe = srcBe;
                task.destBe = destBe;
                task.isGlobal = isGlobal;
                task.clusterId = clusterId;
                task.beToTablets = beToTablets;
                tabletToInfightTask.put(pickedTablet.getId(), task);

                LOG.info("pre cache {} from {} to {}, cluster {} minNum {} maxNum {} beNum {} tabletsNum {}, part {}",
                         pickedTablet.getId(), srcBe, destBe, clusterId,
                         minTabletsNum, maxTabletsNum, beNum, totalTabletsNum, cloudReplica.getPartitionId());
                updateBeToTablets(pickedTablet, srcBe, destBe, isGlobal, clusterId, beToTablets,
                                  futureBeToTabletsGlobal);
            } else {
                if (isGlobal) {
                    long maxBeSize = partitionToTablets.get(cloudReplica.getPartitionId())
                            .get(cloudReplica.getIndexId()).get(srcBe).size();
                    List<Tablet> destBeTablets = partitionToTablets.get(cloudReplica.getPartitionId())
                            .get(cloudReplica.getIndexId()).get(destBe);
                    long minBeSize = destBeTablets == null ? 0 : destBeTablets.size();
                    if (minBeSize >= maxBeSize) {
                        continue;
                    }
                }

                LOG.info("transfer {} from {} to {}, cluster {} minNum {} maxNum {} beNum {} tabletsNum {}, part {}",
                         pickedTablet.getId(), srcBe, destBe, clusterId,
                         minTabletsNum, maxTabletsNum, beNum, totalTabletsNum, cloudReplica.getPartitionId());
                updateBeToTablets(pickedTablet, srcBe, destBe, isGlobal, clusterId, beToTablets, beToTabletsGlobal);
                updateClusterToBeMap(pickedTablet, destBe, clusterId);
            }
        }
    }

    public void addTabletMigrationTask(Long srcBe, Long dstBe) {
        tabletsMigrateTasks.offer(Pair.of(srcBe, dstBe));
    }

    /* Migrate tablet replicas from srcBe to dstBe
     * replica location info will be updated in both master and follower FEs.
     */
    private void migrateTablets(Long srcBe, Long dstBe) {
        // get tablets
        List<Tablet> tablets = beToTabletsGlobal.get(srcBe);
        SystemInfoService systemInfoService = Env.getCurrentSystemInfo();
        if (tablets.isEmpty()) {
            // srcBe does not have any tablets, set inactive
            Env.getCurrentEnv().getCloudUpgradeMgr().setBeStateInactive(srcBe);
            return;
        }
        for (Tablet tablet : tablets) {
            // get replica
            CloudReplica cloudReplica = (CloudReplica) tablet.getReplicas().get(0);
            String clusterId = systemInfoService.getBackend(srcBe).getCloudClusterId();
            String clusterName = systemInfoService.getBackend(srcBe).getCloudClusterName();
            // update replica location info
            cloudReplica.updateClusterToBe(clusterId, dstBe);
            LOG.info("cloud be migrate tablet {} from srcBe={} to dstBe={}, clusterId={}, clusterName={}",
                    tablet.getId(), srcBe, dstBe, clusterId, clusterName);

            // populate to followers
            Database db = Env.getCurrentInternalCatalog().getDbNullable(cloudReplica.getDbId());
            if (db == null) {
                LOG.error("get null db from replica, tabletId={}, partitionId={}, beId={}",
                        cloudReplica.getTableId(), cloudReplica.getPartitionId(), cloudReplica.getBackendId());
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
                        tablet.getId(), cloudReplica.getId(), clusterId, dstBe);
                Env.getCurrentEnv().getEditLog().logUpdateCloudReplica(info);
            } finally {
                table.readUnlock();
            }
        }
        try {
            Env.getCurrentEnv().getCloudUpgradeMgr().registerWaterShedTxnId(srcBe);
        } catch (AnalysisException e) {
            throw new RuntimeException(e);
        }
    }
}

