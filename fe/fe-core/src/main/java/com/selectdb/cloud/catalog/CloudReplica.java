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

import com.google.common.base.Strings;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.gson.annotations.SerializedName;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.common.io.Text;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.Backend;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CloudReplica extends Replica {
    private static final Logger LOG = LogManager.getLogger(CloudReplica.class);

    // In the future, a replica may be mapped to multiple BEs in a cluster,
    // so this value is be list
    private Map<String, List<Long>> clusterToBackends = new ConcurrentHashMap<String, List<Long>>();
    @SerializedName(value = "dbId")
    private long dbId = -1;
    @SerializedName(value = "tableId")
    private long tableId = -1;
    @SerializedName(value = "partitionId")
    private long partitionId = -1;
    @SerializedName(value = "indexId")
    private long indexId = -1;
    @SerializedName(value = "idx")
    private long idx = -1;

    public CloudReplica() {
    }

    public CloudReplica(long replicaId, List<Long> backendIds, ReplicaState state, long version, int schemaHash,
            long dbId, long tableId, long partitionId, long indexId, long idx) {
        super(replicaId, -1, state, version, schemaHash);
        this.dbId = dbId;
        this.tableId = tableId;
        this.partitionId = partitionId;
        this.indexId = indexId;
        this.idx = idx;
    }

    private boolean isColocated() {
        return Env.getCurrentColocateIndex().isColocateTable(tableId);
    }

    private long getColocatedBeId(String cluster) {
        List<Backend> bes = Env.getCurrentSystemInfo().getBackendsByClusterName(cluster);
        List<Backend> availableBes = new ArrayList<>();
        for (Backend be : bes) {
            if (be.isAlive()) {
                availableBes.add(be);
            }
        }
        if (availableBes == null || availableBes.size() == 0) {
            LOG.warn("failed to get available be, clusterName: {}", cluster);
            return -1;
        }

        // Tablets with the same idx will be hashed to the same BE, which
        // meets the requirements of colocated table.
        long index = idx % availableBes.size();
        long pickedBeId = availableBes.get((int) index).getId();

        return pickedBeId;
    }

    @Override
    public long getBackendId() {
        String cluster = null;
        // Not in a connect session
        ConnectContext context = ConnectContext.get();
        if (context != null) {
            if (!Strings.isNullOrEmpty(context.getSessionVariable().getCloudCluster())) {
                cluster = context.getSessionVariable().getCloudCluster();
                try {
                    Env.getCurrentEnv().checkCloudClusterPriv(cluster);
                } catch (Exception e) {
                    LOG.warn("get cluster by session context exception");
                    return -1;
                }
                LOG.debug("get cluster by session context cluster: {}", cluster);
            } else {
                cluster = context.getCloudCluster();
                LOG.debug("get cluster by context {}", cluster);
            }
        } else {
            LOG.warn("connect context is null in getBackendId");
            return -1;
        }

        // check default cluster valid.
        if (!Strings.isNullOrEmpty(cluster)) {
            boolean exist = Env.getCurrentSystemInfo().getCloudClusterNames().contains(cluster);
            if (!exist) {
                //can't use this default cluster, plz change another
                LOG.warn("cluster: {} is not existed", cluster);
                return -1;
            }
        } else {
            LOG.warn("failed to get available be, clusterName: {}", cluster);
            return -1;
        }

        if (isColocated()) {
            return getColocatedBeId(cluster);
        }

        if (clusterToBackends.containsKey(cluster)) {
            long backendId = clusterToBackends.get(cluster).get(0);
            Backend be = Env.getCurrentSystemInfo().getBackend(backendId);
            if (be != null && be.isQueryAvailable()) {
                LOG.debug("backendId={} ", backendId);
                return backendId;
            }
        }

        return hashReplicaToBe(cluster);
    }

    public long hashReplicaToBe(String cluster) {
        // TODO(luwei) list shoule be sorted
        List<Backend> clusterBes = Env.getCurrentSystemInfo().getBackendsByClusterName(cluster);
        // use alive be to exec sql
        List<Backend> availableBes = new ArrayList<>();
        for (Backend be : clusterBes) {
            if (be.isAlive()) {
                availableBes.add(be);
            }
        }
        if (availableBes == null || availableBes.size() == 0) {
            LOG.warn("failed to get available be, clusterName: {}", cluster);
            return -1;
        }
        LOG.debug("availableBes={}", availableBes);
        long index = -1;
        HashCode hashCode = null;
        if (idx == -1) {
            index = getId() % availableBes.size();
        } else {
            hashCode = Hashing.murmur3_128().hashLong(partitionId);
            int beNum = availableBes.size();
            // (hashCode.asLong() + idx) % beNum may be a negative value, so we
            // need to take the modulus of beNum again to ensure that index is
            // a positive value
            index = ((hashCode.asLong() + idx) % beNum + beNum) % beNum;
        }
        long pickedBeId = availableBes.get((int) index).getId();
        LOG.info("picked beId {}, replicaId {}, partitionId {}, beNum {}, replicaIdx {}, picked Index {}, hashVal {}",
                pickedBeId, getId(), partitionId, availableBes.size(), idx, index,
                hashCode == null ? -1 : hashCode.asLong());

        // save to clusterToBackends map
        List<Long> bes = new ArrayList<Long>();
        bes.add(pickedBeId);
        String clusterId = Env.getCurrentSystemInfo().getCloudClusterIdByName(cluster);
        clusterToBackends.put(clusterId, bes);

        return pickedBeId;
    }

    @Override
    public boolean checkVersionCatchUp(long expectedVersion, boolean ignoreAlter) {
        if (ignoreAlter && getState() == ReplicaState.ALTER
                && getVersion() == Partition.PARTITION_INIT_VERSION) {
            return true;
        }

        if (expectedVersion == Partition.PARTITION_INIT_VERSION) {
            // no data is loaded into this replica, just return true
            return true;
        }

        return true;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        dbId = in.readLong();
        tableId = in.readLong();
        partitionId = in.readLong();
        indexId = in.readLong();
        idx = in.readLong();
        int count = in.readInt();
        for (int i = 0; i < count; ++i) {
            String clusterId = Text.readString(in);
            String realClusterId = Env.getCurrentSystemInfo().getCloudClusterIdByName(clusterId);
            LOG.info("cluster Id {}, real cluster Id {}", clusterId, realClusterId);

            if (!Strings.isNullOrEmpty(realClusterId)) {
                clusterId = realClusterId;
            }

            long beId = in.readLong();
            List<Long> bes = new ArrayList<Long>();
            bes.add(beId);
            clusterToBackends.put(clusterId, bes);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeLong(dbId);
        out.writeLong(tableId);
        out.writeLong(partitionId);
        out.writeLong(indexId);
        out.writeLong(idx);
        out.writeInt(clusterToBackends.size());
        for (Map.Entry<String, List<Long>> entry : clusterToBackends.entrySet()) {
            Text.writeString(out, entry.getKey());
            out.writeLong(entry.getValue().get(0));
        }
    }

    public static CloudReplica read(DataInput in) throws IOException {
        CloudReplica replica = new CloudReplica();
        replica.readFields(in);
        // TODO(luwei): perisist and fillup clusterToBackends to take full advantage of data cache
        return replica;
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public long getPartitionId() {
        return partitionId;
    }

    public long getIndexId() {
        return indexId;
    }

    public long getIdx() {
        return idx;
    }

    public Map<String, List<Long>> getClusterToBackends() {
        return clusterToBackends;
    }

    public void updateClusterToBe(String cluster, long beId) {
        // write lock
        List<Long> bes = new ArrayList<Long>();
        bes.add(beId);
        clusterToBackends.put(cluster, bes);
    }
}
