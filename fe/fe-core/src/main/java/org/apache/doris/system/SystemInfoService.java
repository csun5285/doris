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

package org.apache.doris.system;

import org.apache.doris.analysis.ModifyBackendClause;
import org.apache.doris.analysis.ModifyBackendHostNameClause;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DiskInfo;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.CountingDataOutputStream;
import org.apache.doris.common.util.NetUtils;
import org.apache.doris.ha.FrontendNodeType;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.resource.Tag;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.thrift.TNodeInfo;
import org.apache.doris.thrift.TPaloNodesInfo;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.selectdb.cloud.proto.SelectdbCloud;
import com.selectdb.cloud.proto.SelectdbCloud.ClusterPB;
import com.selectdb.cloud.rpc.MetaServiceProxy;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class SystemInfoService {
    private static final Logger LOG = LogManager.getLogger(SystemInfoService.class);

    public static final String DEFAULT_CLUSTER = "default_cluster";

    public static final String NO_BACKEND_LOAD_AVAILABLE_MSG = "No backend load available.";

    public static final String NO_SCAN_NODE_BACKEND_AVAILABLE_MSG = "There is no scanNode Backend available.";

    public static final String NOT_USING_VALID_CLUSTER_MSG = "Not using valid cloud clusters, "
            + "please use a cluster before issuing any queries";

    private volatile ImmutableMap<Long, Backend> idToBackendRef = ImmutableMap.of();
    private volatile ImmutableMap<Long, AtomicLong> idToReportVersionRef = ImmutableMap.of();
    // TODO(gavin): use {clusterId -> List<BackendId>} instead to reduce risk of inconsistency
    // use exclusive lock to make sure only one thread can change clusterIdToBackend and clusterNameToId
    private ReentrantLock lock = new ReentrantLock();

    // for show cluster and cache user owned cluster
    // mysqlUserName -> List of ClusterPB
    private Map<String, List<ClusterPB>> mysqlUserNameToClusterPB = ImmutableMap.of();
    // clusterId -> List<Backend>
    private Map<String, List<Backend>> clusterIdToBackend = new ConcurrentHashMap<>();
    // clusterName -> clusterId
    private Map<String, String> clusterNameToId = new ConcurrentHashMap<>();

    private volatile ImmutableMap<Long, DiskInfo> pathHashToDishInfoRef = ImmutableMap.of();

    public static class HostInfo implements Comparable<HostInfo> {
        public String host;
        public int port;

        public HostInfo(String host, int port) {
            this.host = host;
            this.port = port;
        }

        public String getHost() {
            return host;
        }

        public int getPort() {
            return port;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public String getIdent() {
            return host;
        }

        @Override
        public int compareTo(@NotNull HostInfo o) {
            int res = host.compareTo(o.getHost());
            if (res == 0) {
                return Integer.compare(port, o.getPort());
            }
            return res;
        }

        public boolean isSame(HostInfo other) {
            if (other.getPort() != port) {
                return false;
            }
            return host.equals(other.getHost());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            HostInfo that = (HostInfo) o;
            return Objects.equals(host, that.getHost())
                    && Objects.equals(port, that.getPort());
        }

        @Override
        public String toString() {
            return "HostInfo{"
                    + "host='" + host + '\''
                    + ", port=" + port
                    + '}';
        }

        public String toHostPortString() {
            return host + ":" + port;
        }
    }

    // sort host backends list by num of backends, descending
    private static final Comparator<List<Backend>> hostBackendsListComparator = new Comparator<List<Backend>>() {
        @Override
        public int compare(List<Backend> list1, List<Backend> list2) {
            if (list1.size() > list2.size()) {
                return -1;
            } else {
                return 1;
            }
        }
    };

    public boolean containClusterName(String clusterName) {
        return clusterNameToId.containsKey(clusterName);
    }

    public List<Backend> getBackendsByClusterName(final String clusterName) {
        String clusterId = clusterNameToId.getOrDefault(clusterName, "");
        if (clusterId.isEmpty()) {
            return new ArrayList<>();
        }
        return clusterIdToBackend.get(clusterId);
    }

    public List<Backend> getBackendsByClusterId(final String clusterId) {
        return clusterIdToBackend.getOrDefault(clusterId, new ArrayList<>());
    }

    public List<String> getCloudClusterIds() {
        return new ArrayList<>(clusterIdToBackend.keySet());
    }

    public String getCloudStatusByName(final String clusterName) {
        String clusterId = clusterNameToId.getOrDefault(clusterName, "");
        if ("".equals(clusterId)) {
            // for rename cluster or dropped cluster
            LOG.info("cant find clusterId by clusteName {}", clusterName);
            return "";
        }
        return getCloudStatusById(clusterId);
    }

    public String getCloudStatusById(final String clusterId) {
        return clusterIdToBackend.getOrDefault(clusterId, new ArrayList<>())
            .stream().map(Backend::getCloudClusterStatus).findFirst().orElse("");
    }

    public void updateClusterNameToId(final String newName,
            final String originalName, final String clusterId) {
        lock.lock();
        clusterNameToId.remove(originalName);
        clusterNameToId.put(newName, clusterId);
        lock.unlock();
    }

    public String getClusterNameByClusterId(final String clusterId) {
        String clusterName = "";
        for (Map.Entry<String, String> entry : clusterNameToId.entrySet()) {
            if (entry.getValue().equals(clusterId)) {
                clusterName = entry.getKey();
                break;
            }
        }
        return clusterName;
    }

    public void dropCluster(final String clusterId, final String clusterName) {
        lock.lock();
        clusterNameToId.remove(clusterName, clusterId);
        clusterIdToBackend.remove(clusterId);
        lock.unlock();
    }

    public List<String> getCloudClusterNames() {
        return new ArrayList<>(clusterNameToId.keySet());
    }

    // use cluster $clusterName
    // return clusterName for userName
    public String addCloudCluster(final String clusterName, final String userName) throws UserException {
        lock.lock();
        if ((Strings.isNullOrEmpty(clusterName) && Strings.isNullOrEmpty(userName))
                || (!Strings.isNullOrEmpty(clusterName) && !Strings.isNullOrEmpty(userName))) {
            // clusterName or userName just only need one.
            lock.unlock();
            LOG.warn("addCloudCluster args err clusterName {}, userName {}", clusterName, userName);
            return "";
        }
        // First time this method is called, build cloud cluster map
        if (clusterNameToId.isEmpty() || clusterIdToBackend.isEmpty()) {
            List<Backend> toAdd = Maps.newHashMap(idToBackendRef)
                    .entrySet().stream().map(i -> i.getValue())
                    .filter(i -> i.getTagMap().containsKey(Tag.CLOUD_CLUSTER_ID))
                    .filter(i -> i.getTagMap().containsKey(Tag.CLOUD_CLUSTER_NAME))
                    .collect(Collectors.toList());
            // The larger bakendId the later it was added, the order matters
            toAdd.sort((x, y) -> (int) (x.getId() - y.getId()));
            updateCloudClusterMap(toAdd, new ArrayList<>());
        }

        String clusterId;
        if (Strings.isNullOrEmpty(userName)) {
            // use clusterName
            LOG.info("try to add a cloud cluster, clusterName={}", clusterName);
            clusterId = clusterNameToId.get(clusterName);
            clusterId = clusterId == null ? "" : clusterId;
            if (clusterIdToBackend.containsKey(clusterId)) { // Cluster already added
                lock.unlock();
                LOG.info("cloud cluster already added, clusterName={}, clusterId={}", clusterName, clusterId);
                return "";
            }
        }
        lock.unlock();
        LOG.info("begin to get cloud cluster from remote, clusterName={}, userName={}", clusterName, userName);

        // Get cloud cluster info from resource manager
        SelectdbCloud.GetClusterResponse response = getCloudCluster(clusterName, "", userName);
        if (!response.hasStatus() || !response.getStatus().hasCode()
                || response.getStatus().getCode() != SelectdbCloud.MetaServiceCode.OK) {
            LOG.warn("get cluster info from meta failed, clusterName={}, incomplete response: {}",
                    clusterName, response);
            throw new UserException("no cluster clusterName: " + clusterName + " or userName: " + userName + " found");
        }

        // Note: get_cluster interface cluster(option -> repeated), so it has at least one cluster.
        if (response.getClusterCount() == 0) {
            LOG.warn("meta service error , return cluster zero, plz check it, "
                            + "cloud_unique_id={}, clusterId={}, response={}",
                    Config.cloud_unique_id, Config.cloud_sql_server_cluster_id, response);
            throw new UserException("get cluster return zero cluster info");
        }

        ClusterPB cpb = response.getCluster(0);
        clusterId = cpb.getClusterId();
        String clusterNameMeta = cpb.getClusterName();

        // Prepare backends
        Map<String, String> newTagMap = Tag.DEFAULT_BACKEND_TAG.toMap();
        newTagMap.put(Tag.CLOUD_CLUSTER_NAME, clusterNameMeta);
        newTagMap.put(Tag.CLOUD_CLUSTER_ID, clusterId);
        List<Backend> backends = new ArrayList<>();
        for (SelectdbCloud.NodeInfoPB node : cpb.getNodesList()) {
            Backend b = new Backend(Env.getCurrentEnv().getNextId(), node.getIp(), node.getHeartbeatPort());
            b.setTagMap(newTagMap);
            backends.add(b);
            LOG.info("new backend to add, clusterName={} clusterId={} backend={}",
                    clusterNameMeta, clusterId, b.toString());
        }

        updateCloudBackends(backends, new ArrayList<>());
        return clusterNameMeta;
    }

    public void updateCloudClusterMap(List<Backend> toAdd, List<Backend> toDel) {
        lock.lock();
        Set<String> clusterNameSet = new HashSet<>();
        for (Backend b : toAdd) {
            String clusterName = b.getCloudClusterName();
            String clusterId = b.getCloudClusterId();
            if (clusterName.isEmpty() || clusterId.isEmpty()) {
                LOG.warn("cloud cluster name or id empty: id={}, name={}", clusterId, clusterName);
                continue;
            }
            clusterNameSet.add(clusterName);
            if (clusterNameSet.size() != 1) {
                LOG.warn("toAdd be list have multi clusterName, please check, Set: {}", clusterNameSet);
            }

            clusterNameToId.put(clusterName, clusterId);
            List<Backend> be = clusterIdToBackend.get(clusterId);
            if (be == null) {
                be = new ArrayList<>();
                clusterIdToBackend.put(clusterId, be);
                MetricRepo.registerClusterMetrics(clusterName, clusterId);
            }
            Set<String> existed = be.stream().map(i -> i.getHost() + ":" + i.getHeartbeatPort())
                    .collect(Collectors.toSet());
            // Deduplicate
            // TODO(gavin): consider vpc
            boolean alreadyExisted = existed.contains(b.getHost() + ":" + b.getHeartbeatPort());
            if (alreadyExisted) {
                LOG.info("BE already existed, clusterName={} clusterId={} backendNum={} backend={}",
                        clusterName, clusterId, be.size(), b);
                continue;
            }
            be.add(b);
            LOG.info("update (add) cloud cluster map, clusterName={} clusterId={} backendNum={} current backend={}",
                    clusterName, clusterId, be.size(), b);
        }

        for (Backend b : toDel) {
            String clusterName = b.getCloudClusterName();
            String clusterId = b.getCloudClusterId();
            // We actually don't care about cluster name here
            if (clusterName.isEmpty() || clusterId.isEmpty()) {
                LOG.warn("cloud cluster name or id empty: id={}, name={}", clusterId, clusterName);
                continue;
            }
            List<Backend> be = clusterIdToBackend.get(clusterId);
            if (be == null) {
                LOG.warn("try to remove a non-existing cluster, clusterId={} clusterName={}",
                        clusterId, clusterName);
                continue;
            }
            Set<Long> d = toDel.stream().map(i -> i.getId()).collect(Collectors.toSet());
            be = be.stream().filter(i -> !d.contains(i.getId())).collect(Collectors.toList());
            // ATTN: clusterId may have zero nodes
            clusterIdToBackend.replace(clusterId, be);
            // such as dropCluster, but no lock
            // ATTN: Empty clusters are treated as dropped clusters.
            if (be.size() == 0) {
                LOG.info("del clusterId {} and clusterName {} due to be nodes eq 0", clusterId, clusterName);
                boolean succ = clusterNameToId.remove(clusterName, clusterId);
                if (!succ) {
                    LOG.warn("impossible, somewhere err, clusterNameToId {}, "
                            + "want remove cluster name {}, cluster id {}", clusterNameToId, clusterName, clusterId);
                }
                clusterIdToBackend.remove(clusterId);
            }
            LOG.info("update (del) cloud cluster map, clusterName={} clusterId={} backendNum={} current backend={}",
                    clusterName, clusterId, be.size(), b);
        }
        lock.unlock();
    }

    // Return the ref of concurrentMap clusterIdToBackend
    // It should be thread-safe to iterate.
    // reference: https://stackoverflow.com/questions/3768554/is-iterating-concurrenthashmap-values-thread-safe
    public Map<String, List<Backend>> getCloudClusterIdToBackend() {
        return clusterIdToBackend;
    }

    public String getCloudClusterIdByName(String clusterName) {
        return clusterNameToId.get(clusterName);
    }

    public ImmutableMap<Long, Backend> getCloudIdToBackend(String clusterName) {
        String clusterId = clusterNameToId.get(clusterName);
        List<Backend> backends = clusterIdToBackend.get(clusterId);
        Map<Long, Backend> idToBackend = Maps.newHashMap();
        for (Backend be : backends) {
            idToBackend.put(be.getId(), be);
        }
        return ImmutableMap.copyOf(idToBackend);
    }

    // Return the ref of concurrentMap clusterNameToId
    // It should be thread-safe to iterate.
    // reference: https://stackoverflow.com/questions/3768554/is-iterating-concurrenthashmap-values-thread-safe
    public Map<String, String> getCloudClusterNameToId() {
        return clusterNameToId;
    }

    public Map<String, List<ClusterPB>> getMysqlUserNameToClusterPb() {
        return mysqlUserNameToClusterPB;
    }

    public void updateMysqlUserNameToClusterPb(Map<String, List<ClusterPB>> m) {
        mysqlUserNameToClusterPB = m;
    }

    public List<Pair<String, Integer>> getCurrentObFrontends() {
        List<Frontend> frontends = Env.getCurrentEnv().getFrontends(FrontendNodeType.OBSERVER);
        List<Pair<String, Integer>> frontendsPair = new ArrayList<>();
        for (Frontend frontend : frontends) {
            frontendsPair.add(Pair.of(frontend.getHost(), frontend.getEditLogPort()));
        }
        return frontendsPair;
    }

    public static TPaloNodesInfo createAliveNodesInfo() {
        TPaloNodesInfo nodesInfo = new TPaloNodesInfo();
        SystemInfoService systemInfoService = Env.getCurrentSystemInfo();
        for (Long id : systemInfoService.getAllBackendIds(true /*need alive*/)) {
            Backend backend = systemInfoService.getBackend(id);
            nodesInfo.addToNodes(new TNodeInfo(backend.getId(), 0, backend.getHost(), backend.getBrpcPort()));
        }
        return nodesInfo;
    }

    // for deploy manager
    public void addBackends(List<HostInfo> hostInfos, boolean isFree)
            throws UserException {
        addBackends(hostInfos, Tag.DEFAULT_BACKEND_TAG.toMap());
    }

    /**
     * @param hostInfos : backend's ip, hostName and port
     * @throws DdlException
     */
    public void addBackends(List<HostInfo> hostInfos, Map<String, String> tagMap) throws UserException {
        for (HostInfo hostInfo : hostInfos) {
            // check is already exist
            if (getBackendWithHeartbeatPort(hostInfo.getHost(), hostInfo.getPort()) != null) {
                String backendIdentifier = hostInfo.getHost() + ":"
                        + hostInfo.getPort();
                throw new DdlException("Same backend already exists[" + backendIdentifier + "]");
            }
        }

        for (HostInfo hostInfo : hostInfos) {
            addBackend(hostInfo.getHost(), hostInfo.getPort(), tagMap);
        }
    }

    public synchronized void updateCloudFrontends(List<Frontend> toAdd,
            List<Frontend> toDel) throws DdlException {
        LOG.debug("updateCloudFrontends toAdd={} toDel={}", toAdd, toDel);
        String masterIp = Env.getCurrentEnv().getMasterHost();
        for (Frontend fe : toAdd) {
            if (masterIp.equals(fe.getHost())) {
                continue;
            }
            Env.getCurrentEnv().addFrontend(FrontendNodeType.OBSERVER,
                    fe.getHost(), fe.getEditLogPort(), fe.getNodeName());
            LOG.info("added cloud frontend={} ", fe);
        }
        for (Frontend fe : toDel) {
            if (masterIp.equals(fe.getHost())) {
                continue;
            }
            Env.getCurrentEnv().dropFrontend(FrontendNodeType.OBSERVER, fe.getHost(), fe.getEditLogPort());
            LOG.info("dropped cloud frontend={} ", fe);
        }
    }

    public synchronized void updateCloudBackends(List<Backend> toAdd, List<Backend> toDel) {
        // Deduplicate and validate
        if (toAdd.size() == 0 && toDel.size() == 0) {
            LOG.info("nothing to do");
            return;
        }
        Set<String> existedBes = idToBackendRef.entrySet().stream().map(i -> i.getValue())
                .map(i -> i.getHost() + ":" + i.getHeartbeatPort())
                .collect(Collectors.toSet());
        LOG.debug("deduplication existedBes={}", existedBes);
        LOG.debug("before deduplication toAdd={} toDel={}", toAdd, toDel);
        toAdd = toAdd.stream().filter(i -> !existedBes.contains(i.getHost() + ":" + i.getHeartbeatPort()))
                .collect(Collectors.toList());
        toDel = toDel.stream().filter(i -> existedBes.contains(i.getHost() + ":" + i.getHeartbeatPort()))
                .collect(Collectors.toList());
        LOG.debug("after deduplication toAdd={} toDel={}", toAdd, toDel);

        Map<String, List<Backend>> existedHostToBeList = idToBackendRef.values().stream().collect(Collectors.groupingBy(
                Backend::getHost));
        for (Backend be : toAdd) {
            Env.getCurrentEnv().getEditLog().logAddBackend(be);
            LOG.info("added cloud backend={} ", be);
            // backends is changed, regenerated tablet number metrics
            MetricRepo.generateBackendsTabletMetrics();

            String host = be.getHost();
            if (existedHostToBeList.keySet().contains(host)) {
                if (be.isSmoothUpgradeDst()) {
                    LOG.info("a new BE process will start on the existed node for smooth upgrading");
                    int beNum = existedHostToBeList.get(host).size();
                    Backend colocatedBe = existedHostToBeList.get(host).get(0);
                    if (beNum != 1) {
                        LOG.warn("find multiple co-located BEs, num: {}, select the 1st {} as migration src", beNum,
                                colocatedBe.getId());
                    }
                    colocatedBe.setSmoothUpgradeSrc(true);
                    handleNewBeOnSameNode(colocatedBe, be);
                } else {
                    LOG.warn("a new BE process will start on the existed node, it should not happend unless testing");
                }
            }
        }
        for (Backend be : toDel) {
            // drop be, set it not alive
            be.setAlive(false);
            be.setLastMissingHeartbeatTime(System.currentTimeMillis());
            Env.getCurrentEnv().getEditLog().logDropBackend(be);
            LOG.info("dropped cloud backend={}, and lastMissingHeartbeatTime={}", be, be.getLastMissingHeartbeatTime());
            // backends is changed, regenerated tablet number metrics
            MetricRepo.generateBackendsTabletMetrics();
        }

        // Update idToBackendRef
        Map<Long, Backend> copiedBackends = Maps.newHashMap(idToBackendRef);
        toAdd.forEach(i -> copiedBackends.put(i.getId(), i));
        toDel.forEach(i -> copiedBackends.remove(i.getId()));
        ImmutableMap<Long, Backend> newIdToBackend = ImmutableMap.copyOf(copiedBackends);
        idToBackendRef = newIdToBackend;

        // Update idToReportVersionRef
        Map<Long, AtomicLong> copiedReportVersions = Maps.newHashMap(idToReportVersionRef);
        toAdd.forEach(i -> copiedReportVersions.put(i.getId(), new AtomicLong(0L)));
        toDel.forEach(i -> copiedReportVersions.remove(i.getId()));
        ImmutableMap<Long, AtomicLong> newIdToReportVersion = ImmutableMap.copyOf(copiedReportVersions);
        idToReportVersionRef = newIdToReportVersion;

        updateCloudClusterMap(toAdd, toDel);
    }

    private void handleNewBeOnSameNode(Backend oldBe, Backend newBe) {
        LOG.info("new BE {} starts on the same node as existing BE {}", newBe.getId(), oldBe.getId());
        Env.getCurrentEnv().getCloudTabletRebalancer().addTabletMigrationTask(oldBe.getId(), newBe.getId());
    }

    // for test
    public void addBackend(Backend backend) {
        Map<Long, Backend> copiedBackends = Maps.newHashMap(idToBackendRef);
        copiedBackends.put(backend.getId(), backend);
        ImmutableMap<Long, Backend> newIdToBackend = ImmutableMap.copyOf(copiedBackends);
        idToBackendRef = newIdToBackend;
    }

    // Final entry of adding backend
    private void addBackend(String host, int heartbeatPort, Map<String, String> tagMap) {
        Backend newBackend = new Backend(Env.getCurrentEnv().getNextId(), host, heartbeatPort);
        // update idToBackend
        Map<Long, Backend> copiedBackends = Maps.newHashMap(idToBackendRef);
        copiedBackends.put(newBackend.getId(), newBackend);
        ImmutableMap<Long, Backend> newIdToBackend = ImmutableMap.copyOf(copiedBackends);
        idToBackendRef = newIdToBackend;

        // set new backend's report version as 0L
        Map<Long, AtomicLong> copiedReportVersions = Maps.newHashMap(idToReportVersionRef);
        copiedReportVersions.put(newBackend.getId(), new AtomicLong(0L));
        ImmutableMap<Long, AtomicLong> newIdToReportVersion = ImmutableMap.copyOf(copiedReportVersions);
        idToReportVersionRef = newIdToReportVersion;

        // set tags
        newBackend.setTagMap(tagMap);

        // log
        Env.getCurrentEnv().getEditLog().logAddBackend(newBackend);
        LOG.info("finished to add {} ", newBackend);

        // backends is changed, regenerated tablet number metrics
        MetricRepo.generateBackendsTabletMetrics();
    }

    public void dropBackends(List<HostInfo> hostInfos) throws DdlException {
        for (HostInfo hostInfo : hostInfos) {
            // check is already exist
            if (getBackendWithHeartbeatPort(hostInfo.getHost(), hostInfo.getPort()) == null) {
                String backendIdentifier = hostInfo.getHost() + ":" + hostInfo.getPort();
                throw new DdlException("backend does not exists[" + backendIdentifier + "]");
            }
        }
        for (HostInfo hostInfo : hostInfos) {
            dropBackend(hostInfo.getHost(), hostInfo.getPort());
        }
    }

    // for decommission
    public void dropBackend(long backendId) throws DdlException {
        Backend backend = getBackend(backendId);
        if (backend == null) {
            throw new DdlException("Backend[" + backendId + "] does not exist");
        }
        dropBackend(backend.getHost(), backend.getHeartbeatPort());
    }

    // final entry of dropping backend
    public void dropBackend(String host, int heartbeatPort) throws DdlException {
        Backend droppedBackend = getBackendWithHeartbeatPort(host, heartbeatPort);
        if (droppedBackend == null) {
            throw new DdlException("backend does not exists[" + host
                    + ":" + heartbeatPort + "]");
        }
        // update idToBackend
        Map<Long, Backend> copiedBackends = Maps.newHashMap(idToBackendRef);
        copiedBackends.remove(droppedBackend.getId());
        ImmutableMap<Long, Backend> newIdToBackend = ImmutableMap.copyOf(copiedBackends);
        idToBackendRef = newIdToBackend;

        // update idToReportVersion
        Map<Long, AtomicLong> copiedReportVersions = Maps.newHashMap(idToReportVersionRef);
        copiedReportVersions.remove(droppedBackend.getId());
        ImmutableMap<Long, AtomicLong> newIdToReportVersion = ImmutableMap.copyOf(copiedReportVersions);
        idToReportVersionRef = newIdToReportVersion;

        // log
        Env.getCurrentEnv().getEditLog().logDropBackend(droppedBackend);
        LOG.info("finished to drop {}", droppedBackend);

        // backends is changed, regenerated tablet number metrics
        MetricRepo.generateBackendsTabletMetrics();
    }

    // only for test
    public void dropAllBackend() {
        // update idToBackend
        idToBackendRef = ImmutableMap.<Long, Backend>of();
        // update idToReportVersion
        idToReportVersionRef = ImmutableMap.<Long, AtomicLong>of();
    }

    public Backend getBackend(long backendId) {
        return idToBackendRef.get(backendId);
    }

    public boolean checkBackendLoadAvailable(long backendId) {
        Backend backend = idToBackendRef.get(backendId);
        if (backend == null || !backend.isLoadAvailable()) {
            return false;
        }
        return true;
    }

    public boolean checkBackendQueryAvailable(long backendId) {
        Backend backend = idToBackendRef.get(backendId);
        if (backend == null || !backend.isQueryAvailable()) {
            return false;
        }
        return true;
    }

    public boolean checkBackendScheduleAvailable(long backendId) {
        Backend backend = idToBackendRef.get(backendId);
        if (backend == null || !backend.isScheduleAvailable()) {
            return false;
        }
        return true;
    }

    public boolean checkBackendAlive(long backendId) {
        Backend backend = idToBackendRef.get(backendId);
        if (backend == null || !backend.isAlive()) {
            return false;
        }
        return true;
    }

    public Backend getBackendWithHeartbeatPort(String host, int heartPort) {
        ImmutableMap<Long, Backend> idToBackend = idToBackendRef;
        for (Backend backend : idToBackend.values()) {
            if (backend.getHost().equals(host) && backend.getHeartbeatPort() == heartPort) {
                return backend;
            }
        }
        return null;
    }

    public Backend getBackendWithBePort(String ip, int bePort) {
        ImmutableMap<Long, Backend> idToBackend = idToBackendRef;
        for (Backend backend : idToBackend.values()) {
            if (backend.getHost().equals(ip) && backend.getBePort() == bePort) {
                return backend;
            }
        }
        return null;
    }

    public Backend getBackendWithHttpPort(String ip, int httpPort) {
        ImmutableMap<Long, Backend> idToBackend = idToBackendRef;
        for (Backend backend : idToBackend.values()) {
            if (backend.getHost().equals(ip) && backend.getHttpPort() == httpPort) {
                return backend;
            }
        }
        return null;
    }

    public List<Long> getAllBackendIds() {
        return getAllBackendIds(false);
    }

    public int getBackendsNumber(boolean needAlive) {
        int beNumber = ConnectContext.get().getSessionVariable().getBeNumberForTest();
        if (beNumber < 0) {
            beNumber = getAllBackendIds(needAlive).size();
        }
        return beNumber;
    }

    public List<Long> getAllBackendIds(boolean needAlive) {
        ImmutableMap<Long, Backend> idToBackend = idToBackendRef;
        List<Long> backendIds = Lists.newArrayList(idToBackend.keySet());
        if (!needAlive) {
            return backendIds;
        } else {
            Iterator<Long> iter = backendIds.iterator();
            while (iter.hasNext()) {
                Backend backend = this.getBackend(iter.next());
                if (backend == null || !backend.isAlive()) {
                    iter.remove();
                }
            }
            return backendIds;
        }
    }

    public List<Long> getDecommissionedBackendIds() {
        ImmutableMap<Long, Backend> idToBackend = idToBackendRef;
        List<Long> backendIds = Lists.newArrayList(idToBackend.keySet());

        Iterator<Long> iter = backendIds.iterator();
        while (iter.hasNext()) {
            Backend backend = this.getBackend(iter.next());
            if (backend == null || !backend.isDecommissioned()) {
                iter.remove();
            }
        }
        return backendIds;
    }

    public List<Backend> getAllBackends() {
        return Lists.newArrayList(idToBackendRef.values());
    }

    public List<Backend> getMixBackends() {
        return idToBackendRef.values().stream().filter(backend -> backend.isMixNode()).collect(Collectors.toList());
    }

    public List<Backend> getCnBackends() {
        return idToBackendRef.values().stream().filter(backend -> backend.isComputeNode()).collect(Collectors.toList());
    }

    class BeComparator implements Comparator<Backend> {
        public int compare(Backend a, Backend b) {
            return (int) (a.getId() - b.getId());
        }
    }

    public List<Long> selectBackendIdsRoundRobinByPolicy(BeSelectionPolicy policy, int number,
            int nextIndex) {
        Preconditions.checkArgument(number >= -1);
        List<Backend> candidates = getCandidates(policy);
        if (number != -1 && candidates.size() < number) {
            LOG.info("Not match policy: {}. candidates num: {}, expected: {}", policy, candidates.size(), number);
            return Lists.newArrayList();
        }

        int realIndex = nextIndex % candidates.size();
        List<Long> partialOrderList = new ArrayList<Long>();
        partialOrderList.addAll(candidates.subList(realIndex, candidates.size())
                .stream().map(b -> b.getId()).collect(Collectors.toList()));
        partialOrderList.addAll(candidates.subList(0, realIndex)
                .stream().map(b -> b.getId()).collect(Collectors.toList()));

        if (number == -1) {
            return partialOrderList;
        } else {
            return partialOrderList.subList(0, number);
        }
    }

    public List<Backend> getCandidates(BeSelectionPolicy policy) {
        List<Backend> candidates = policy.getCandidateBackends(idToBackendRef.values());
        if (candidates.isEmpty()) {
            LOG.info("Not match policy: {}. candidates num: {}", policy, candidates.size());
            return Lists.newArrayList();
        }

        if (!policy.allowOnSameHost) {
            Map<String, List<Backend>> backendMaps = Maps.newHashMap();
            for (Backend backend : candidates) {
                if (backendMaps.containsKey(backend.getHost())) {
                    backendMaps.get(backend.getHost()).add(backend);
                } else {
                    List<Backend> list = Lists.newArrayList();
                    list.add(backend);
                    backendMaps.put(backend.getHost(), list);
                }
            }
            candidates.clear();
            for (List<Backend> list : backendMaps.values()) {
                candidates.add(list.get(0));
            }
        }

        if (candidates.isEmpty()) {
            LOG.info("Not match policy: {}. candidates num: {}", policy, candidates.size());
            return Lists.newArrayList();
        }

        Collections.sort(candidates, new BeComparator());
        return candidates;
    }

    // Select the smallest number of tablets as the starting position of
    // round robin in the BE that match the policy
    public int getStartPosOfRoundRobin(Tag tag, TStorageMedium storageMedium) {
        BeSelectionPolicy.Builder builder = new BeSelectionPolicy.Builder()
                .needScheduleAvailable().needCheckDiskUsage().addTags(Sets.newHashSet(tag))
                .setStorageMedium(storageMedium);
        if (FeConstants.runningUnitTest || Config.allow_replica_on_same_host) {
            builder.allowOnSameHost();
        }

        BeSelectionPolicy policy = builder.build();
        List<Backend> candidates = getCandidates(policy);

        long minBeTabletsNum = Long.MAX_VALUE;
        int minIndex = -1;
        for (int i = 0; i < candidates.size(); ++i) {
            long tabletsNum = Env.getCurrentInvertedIndex()
                    .getTabletIdsByBackendId(candidates.get(i).getId()).size();
            if (tabletsNum < minBeTabletsNum) {
                minBeTabletsNum = tabletsNum;
                minIndex = i;
            }
        }
        return minIndex;
    }

    public Map<Tag, List<Long>> getBeIdRoundRobinForReplicaCreation(
            ReplicaAllocation replicaAlloc, TStorageMedium storageMedium,
            Map<Tag, Integer> nextIndexs) throws DdlException {
        Map<Tag, List<Long>> chosenBackendIds = Maps.newHashMap();
        Map<Tag, Short> allocMap = replicaAlloc.getAllocMap();
        short totalReplicaNum = 0;
        for (Map.Entry<Tag, Short> entry : allocMap.entrySet()) {
            BeSelectionPolicy.Builder builder = new BeSelectionPolicy.Builder()
                    .needScheduleAvailable().needCheckDiskUsage().addTags(Sets.newHashSet(entry.getKey()))
                    .setStorageMedium(storageMedium);
            if (FeConstants.runningUnitTest || Config.allow_replica_on_same_host) {
                builder.allowOnSameHost();
            }

            BeSelectionPolicy policy = builder.build();
            int nextIndex = nextIndexs.get(entry.getKey());
            List<Long> beIds = selectBackendIdsRoundRobinByPolicy(policy, entry.getValue(), nextIndex);
            nextIndexs.put(entry.getKey(), nextIndex + beIds.size());

            if (beIds.isEmpty()) {
                throw new DdlException("Failed to find " + entry.getValue() + " backend(s) for policy: " + policy);
            }
            chosenBackendIds.put(entry.getKey(), beIds);
            totalReplicaNum += beIds.size();
        }
        Preconditions.checkState(totalReplicaNum == replicaAlloc.getTotalReplicaNum());
        return chosenBackendIds;
    }

    /**
     * Select a set of backends for replica creation.
     * The following parameters need to be considered when selecting backends.
     *
     * @param replicaAlloc
     * @param storageMedium
     * @param isStorageMediumSpecified
     * @param isOnlyForCheck set true if only used for check available backend
     * @return return the selected backend ids group by tag.
     * @throws DdlException
     */
    public Map<Tag, List<Long>> selectBackendIdsForReplicaCreation(
            ReplicaAllocation replicaAlloc, TStorageMedium storageMedium, boolean isStorageMediumSpecified,
            boolean isOnlyForCheck)
            throws DdlException {
        Map<Long, Backend> copiedBackends = Maps.newHashMap(idToBackendRef);
        Map<Tag, List<Long>> chosenBackendIds = Maps.newHashMap();
        Map<Tag, Short> allocMap = replicaAlloc.getAllocMap();
        short totalReplicaNum = 0;

        int aliveBackendNum = (int) copiedBackends.values().stream().filter(Backend::isAlive).count();
        if (aliveBackendNum < replicaAlloc.getTotalReplicaNum()) {
            throw new DdlException("replication num should be less than the number of available backends. "
                    + "replication num is " + replicaAlloc.getTotalReplicaNum()
                    + ", available backend num is " + aliveBackendNum);
        } else {
            List<String> failedEntries = Lists.newArrayList();

            for (Map.Entry<Tag, Short> entry : allocMap.entrySet()) {
                BeSelectionPolicy.Builder builder = new BeSelectionPolicy.Builder()
                        .needScheduleAvailable().needCheckDiskUsage().addTags(Sets.newHashSet(entry.getKey()))
                        .setStorageMedium(storageMedium);
                if (FeConstants.runningUnitTest || Config.allow_replica_on_same_host) {
                    builder.allowOnSameHost();
                }

                BeSelectionPolicy policy = builder.build();
                List<Long> beIds = selectBackendIdsByPolicy(policy, entry.getValue());
                // first time empty, retry with different storage medium
                // if only for check, no need to retry different storage medium to get backend
                if (beIds.isEmpty() && storageMedium != null && !isStorageMediumSpecified && !isOnlyForCheck) {
                    storageMedium = (storageMedium == TStorageMedium.HDD) ? TStorageMedium.SSD : TStorageMedium.HDD;
                    policy = builder.setStorageMedium(storageMedium).build();
                    beIds = selectBackendIdsByPolicy(policy, entry.getValue());
                }
                // after retry different storage medium, it's still empty
                if (beIds.isEmpty()) {
                    LOG.error("failed backend(s) for policy:" + policy);
                    String errorReplication = "replication tag: " + entry.getKey()
                            + ", replication num: " + entry.getValue()
                            + ", storage medium: " + storageMedium;
                    failedEntries.add(errorReplication);
                } else {
                    chosenBackendIds.put(entry.getKey(), beIds);
                    totalReplicaNum += beIds.size();
                }
            }

            if (!failedEntries.isEmpty()) {
                String failedMsg = Joiner.on("\n").join(failedEntries);
                throw new DdlException("Failed to find enough backend, please check the replication num,"
                        + "replication tag and storage medium.\n" + "Create failed replications:\n" + failedMsg);
            }
        }

        Preconditions.checkState(totalReplicaNum == replicaAlloc.getTotalReplicaNum());
        return chosenBackendIds;
    }

    /**
     * Select a set of backends by the given policy.
     *
     * @param policy
     * @param number number of backends which need to be selected. -1 means return as many as possible.
     * @return return #number of backend ids,
     *         or empty set if no backends match the policy, or the number of matched backends is less than "number";
     */
    public List<Long> selectBackendIdsByPolicy(BeSelectionPolicy policy, int number) {
        Preconditions.checkArgument(number >= -1);
        List<Backend> candidates = policy.getCandidateBackends(idToBackendRef.values());
        if ((number != -1 && candidates.size() < number) || candidates.isEmpty()) {
            LOG.debug("Not match policy: {}. candidates num: {}, expected: {}", policy, candidates.size(), number);
            return Lists.newArrayList();
        }
        // If only need one Backend, just return a random one.
        if (number == 1) {
            Collections.shuffle(candidates);
            return Lists.newArrayList(candidates.get(0).getId());
        }

        if (policy.allowOnSameHost) {
            Collections.shuffle(candidates);
            if (number == -1) {
                return candidates.stream().map(b -> b.getId()).collect(Collectors.toList());
            } else {
                return candidates.subList(0, number).stream().map(b -> b.getId()).collect(Collectors.toList());
            }
        }

        // for each host, random select one backend.
        Map<String, List<Backend>> backendMaps = Maps.newHashMap();
        for (Backend backend : candidates) {
            if (backendMaps.containsKey(backend.getHost())) {
                backendMaps.get(backend.getHost()).add(backend);
            } else {
                List<Backend> list = Lists.newArrayList();
                list.add(backend);
                backendMaps.put(backend.getHost(), list);
            }
        }
        candidates.clear();
        for (List<Backend> list : backendMaps.values()) {
            Collections.shuffle(list);
            candidates.add(list.get(0));
        }
        if (number != -1 && candidates.size() < number) {
            LOG.debug("Not match policy: {}. candidates num: {}, expected: {}", policy, candidates.size(), number);
            return Lists.newArrayList();
        }
        Collections.shuffle(candidates);
        if (number != -1) {
            return candidates.subList(0, number).stream().map(b -> b.getId()).collect(Collectors.toList());
        } else {
            return candidates.stream().map(b -> b.getId()).collect(Collectors.toList());
        }
    }

    public ImmutableMap<Long, Backend> getIdToBackend() {
        return idToBackendRef;
    }

    public ImmutableMap<Long, Backend> getAllBackendsMap() {
        return idToBackendRef;
    }

    public long getBackendReportVersion(long backendId) {
        AtomicLong atomicLong = null;
        if ((atomicLong = idToReportVersionRef.get(backendId)) == null) {
            return -1L;
        } else {
            return atomicLong.get();
        }
    }

    public void updateBackendReportVersion(long backendId, long newReportVersion, long dbId, long tableId) {
        AtomicLong atomicLong;
        if ((atomicLong = idToReportVersionRef.get(backendId)) != null) {
            Database db = (Database) Env.getCurrentInternalCatalog().getDbNullable(dbId);
            if (db == null) {
                LOG.warn("failed to update backend report version, db {} does not exist", dbId);
                return;
            }
            atomicLong.set(newReportVersion);
            LOG.debug("update backend {} report version: {}, db: {}, table: {}",
                    backendId, newReportVersion, dbId, tableId);
        }
    }

    public long saveBackends(CountingDataOutputStream dos, long checksum) throws IOException {
        ImmutableMap<Long, Backend> idToBackend = idToBackendRef;
        int backendCount = idToBackend.size();
        checksum ^= backendCount;
        dos.writeInt(backendCount);
        for (Map.Entry<Long, Backend> entry : idToBackend.entrySet()) {
            long key = entry.getKey();
            checksum ^= key;
            dos.writeLong(key);
            entry.getValue().write(dos);
        }
        return checksum;
    }

    public long loadBackends(DataInputStream dis, long checksum) throws IOException {
        int count = dis.readInt();
        checksum ^= count;
        for (int i = 0; i < count; i++) {
            long key = dis.readLong();
            checksum ^= key;
            Backend backend = Backend.read(dis);
            replayAddBackend(backend);
        }
        return checksum;
    }

    public void clear() {
        this.idToBackendRef = null;
        this.idToReportVersionRef = null;
    }

    public static HostInfo getHostAndPort(String hostPort)
            throws AnalysisException {
        hostPort = hostPort.replaceAll("\\s+", "");
        if (hostPort.isEmpty()) {
            throw new AnalysisException("Invalid host port: " + hostPort);
        }

        HostInfo hostInfo = NetUtils.resolveHostInfoFromHostPort(hostPort);

        String host = hostInfo.getHost();
        if (Strings.isNullOrEmpty(host)) {
            throw new AnalysisException("Host is null");
        }

        int heartbeatPort = -1;
        try {
            // validate port
            heartbeatPort = hostInfo.getPort();
            if (heartbeatPort <= 0 || heartbeatPort >= 65536) {
                throw new AnalysisException("Port is out of range: " + heartbeatPort);
            }

            return new HostInfo(host, heartbeatPort);
        } catch (Exception e) {
            throw new AnalysisException("Encounter unknown exception: " + e.getMessage());
        }
    }


    public static Pair<String, Integer> validateHostAndPort(String hostPort) throws AnalysisException {
        HostInfo hostInfo = getHostAndPort(hostPort);
        return Pair.of(hostInfo.getHost(), hostInfo.getPort());
    }

    public void replayAddBackend(Backend newBackend) {
        // update idToBackend
        Map<Long, Backend> copiedBackends = Maps.newHashMap(idToBackendRef);
        copiedBackends.put(newBackend.getId(), newBackend);
        ImmutableMap<Long, Backend> newIdToBackend = ImmutableMap.copyOf(copiedBackends);
        idToBackendRef = newIdToBackend;

        // set new backend's report version as 0L
        Map<Long, AtomicLong> copiedReportVersions = Maps.newHashMap(idToReportVersionRef);
        copiedReportVersions.put(newBackend.getId(), new AtomicLong(0L));
        ImmutableMap<Long, AtomicLong> newIdToReportVersion = ImmutableMap.copyOf(copiedReportVersions);
        idToReportVersionRef = newIdToReportVersion;
        if (Config.isCloudMode()) {
            List<Backend> toAdd = new ArrayList<>();
            toAdd.add(newBackend);
            updateCloudClusterMap(toAdd, new ArrayList<>());
        }
    }

    public void replayDropBackend(Backend backend) {
        LOG.debug("replayDropBackend: {}", backend);
        // update idToBackend
        Map<Long, Backend> copiedBackends = Maps.newHashMap(idToBackendRef);
        copiedBackends.remove(backend.getId());
        ImmutableMap<Long, Backend> newIdToBackend = ImmutableMap.copyOf(copiedBackends);
        idToBackendRef = newIdToBackend;

        // update idToReportVersion
        Map<Long, AtomicLong> copiedReportVersions = Maps.newHashMap(idToReportVersionRef);
        copiedReportVersions.remove(backend.getId());
        ImmutableMap<Long, AtomicLong> newIdToReportVersion = ImmutableMap.copyOf(copiedReportVersions);
        idToReportVersionRef = newIdToReportVersion;

        if (Config.isCloudMode()) {
            List<Backend> toDel = new ArrayList<>();
            toDel.add(backend);
            updateCloudClusterMap(new ArrayList<>(), toDel);
        }
    }

    public void updateBackendState(Backend be) {
        long id = be.getId();
        Backend memoryBe = getBackend(id);
        // backend may already be dropped. this may happen when
        // drop and modify operations do not guarantee the order.
        if (memoryBe != null) {
            memoryBe.setHost(be.getHost());
            memoryBe.setBePort(be.getBePort());
            memoryBe.setAlive(be.isAlive());
            memoryBe.setDecommissioned(be.isDecommissioned());
            memoryBe.setHttpPort(be.getHttpPort());
            memoryBe.setBeRpcPort(be.getBeRpcPort());
            memoryBe.setBrpcPort(be.getBrpcPort());
            memoryBe.setLastUpdateMs(be.getLastUpdateMs());
            memoryBe.setLastStartTime(be.getLastStartTime());
            memoryBe.setDisks(be.getDisks());
            memoryBe.setCpuCores(be.getCputCores());
            memoryBe.setPipelineExecutorSize(be.getPipelineExecutorSize());
        }
    }

    private long getAvailableCapacityB() {
        long capacity = 0L;
        for (Backend backend : idToBackendRef.values()) {
            // Here we do not check if backend is alive,
            // We suppose the dead backends will back to alive later.
            if (backend.isDecommissioned()) {
                // Data on decommissioned backend will move to other backends,
                // So we need to minus size of those data.
                capacity -= backend.getDataUsedCapacityB();
            } else {
                capacity += backend.getAvailableCapacityB();
            }
        }
        return capacity;
    }

    public void checkAvailableCapacity() throws DdlException {
        if (getAvailableCapacityB() <= 0L) {
            throw new DdlException("System has no available disk capacity or no available BE nodes");
        }
    }

    /*
     * Try to randomly get a backend id by given host.
     * If not found, return -1
     */
    public long getBackendIdByHost(String host) {
        ImmutableMap<Long, Backend> idToBackend = idToBackendRef;
        List<Backend> selectedBackends = Lists.newArrayList();
        for (Backend backend : idToBackend.values()) {
            if (backend.getHost().equals(host)) {
                selectedBackends.add(backend);
            }
        }

        if (selectedBackends.isEmpty()) {
            return -1L;
        }

        Collections.shuffle(selectedBackends);
        return selectedBackends.get(0).getId();
    }

    /*
     * Check if the specified disks' capacity has reached the limit.
     * bePathsMap is (BE id -> list of path hash)
     * If floodStage is true, it will check with the floodStage threshold.
     *
     * return Status.OK if not reach the limit
     */
    public Status checkExceedDiskCapacityLimit(Multimap<Long, Long> bePathsMap, boolean floodStage) {
        LOG.debug("pathBeMap: {}", bePathsMap);
        ImmutableMap<Long, DiskInfo> pathHashToDiskInfo = pathHashToDishInfoRef;
        for (Long beId : bePathsMap.keySet()) {
            for (Long pathHash : bePathsMap.get(beId)) {
                DiskInfo diskInfo = pathHashToDiskInfo.get(pathHash);
                if (diskInfo != null && diskInfo.exceedLimit(floodStage)) {
                    return new Status(TStatusCode.CANCELLED,
                            "disk " + pathHash + " on backend " + beId + " exceed limit usage");
                }
            }
        }
        return Status.OK;
    }

    // update the path info when disk report
    // there is only one thread can update path info, so no need to worry about concurrency control
    public void updatePathInfo(List<DiskInfo> addedDisks, List<DiskInfo> removedDisks) {
        Map<Long, DiskInfo> copiedPathInfos = Maps.newHashMap(pathHashToDishInfoRef);
        for (DiskInfo diskInfo : addedDisks) {
            copiedPathInfos.put(diskInfo.getPathHash(), diskInfo);
        }
        for (DiskInfo diskInfo : removedDisks) {
            copiedPathInfos.remove(diskInfo.getPathHash());
        }
        ImmutableMap<Long, DiskInfo> newPathInfos = ImmutableMap.copyOf(copiedPathInfos);
        pathHashToDishInfoRef = newPathInfos;
        LOG.debug("update path infos: {}", newPathInfos);
    }

    public void modifyBackendHost(ModifyBackendHostNameClause clause) throws UserException {
        Backend be = getBackendWithHeartbeatPort(clause.getHost(), clause.getPort());
        if (be == null) {
            throw new DdlException("backend does not exists[" + clause.getHost() + ":" + clause.getPort() + "]");
        }
        if (be.getHost().equals(clause.getNewHost())) {
            // no need to modify
            return;
        }
        be.setHost(clause.getNewHost());
        Env.getCurrentEnv().getEditLog().logModifyBackend(be);
    }

    public void modifyBackends(ModifyBackendClause alterClause) throws UserException {
        List<HostInfo> hostInfos = alterClause.getHostInfos();
        List<Backend> backends = Lists.newArrayList();
        for (HostInfo hostInfo : hostInfos) {
            Backend be = getBackendWithHeartbeatPort(hostInfo.getHost(), hostInfo.getPort());
            if (be == null) {
                throw new DdlException(
                        "backend does not exists[" + hostInfo.getHost() + ":" + hostInfo.getPort() + "]");
            }
            backends.add(be);
        }

        for (Backend be : backends) {
            boolean shouldModify = false;
            Map<String, String> tagMap = alterClause.getTagMap();
            if (!tagMap.isEmpty()) {
                be.setTagMap(tagMap);
                shouldModify = true;
            }

            if (alterClause.isQueryDisabled() != null) {
                if (!alterClause.isQueryDisabled().equals(be.isQueryDisabled())) {
                    be.setQueryDisabled(alterClause.isQueryDisabled());
                    shouldModify = true;
                }
            }

            if (alterClause.isLoadDisabled() != null) {
                if (!alterClause.isLoadDisabled().equals(be.isLoadDisabled())) {
                    be.setLoadDisabled(alterClause.isLoadDisabled());
                    shouldModify = true;
                }
            }

            if (shouldModify) {
                Env.getCurrentEnv().getEditLog().logModifyBackend(be);
                LOG.info("finished to modify backend {} ", be);
            }
        }
    }

    public void replayModifyBackend(Backend backend) {
        Backend memBe = getBackend(backend.getId());
        if (Config.isCloudMode()) {
            // for rename cluster
            String originalClusterName = memBe.getCloudClusterName();
            String originalClusterId = memBe.getCloudClusterId();
            String newClusterName = backend.getTagMap().getOrDefault(Tag.CLOUD_CLUSTER_NAME, "");
            if (!originalClusterName.equals(newClusterName)) {
                // rename
                updateClusterNameToId(newClusterName, originalClusterName, originalClusterId);
            }
            LOG.info("cloud mode replay rename cluster, "
                            + "originalClusterName: {}, originalClusterId: {}, newClusterName: {}",
                    originalClusterName, originalClusterId, newClusterName);
        }
        memBe.setTagMap(backend.getTagMap());
        memBe.setQueryDisabled(backend.isQueryDisabled());
        memBe.setLoadDisabled(backend.isLoadDisabled());
        memBe.setHost(backend.getHost());
        LOG.debug("replay modify backend: {}", backend);
    }

    // Check if there is enough suitable BE for replica allocation
    public void checkReplicaAllocation(ReplicaAllocation replicaAlloc) throws DdlException {
        List<Backend> backends = getMixBackends();
        for (Map.Entry<Tag, Short> entry : replicaAlloc.getAllocMap().entrySet()) {
            if (backends.stream().filter(b -> b.getLocationTag().equals(entry.getKey()))
                    .count() < entry.getValue()) {
                throw new DdlException(
                        "Failed to find enough host with tag(" + entry.getKey() + ") in all backends. need: "
                                + entry.getValue());
            }
        }
    }

    public Set<Tag> getTags() {
        List<Backend> bes = getMixBackends();
        Set<Tag> tags = Sets.newHashSet();
        for (Backend be : bes) {
            tags.add(be.getLocationTag());
        }
        return tags;
    }

    public List<Backend> getBackendsByTag(Tag tag) {
        List<Backend> bes = getMixBackends();
        return bes.stream().filter(b -> b.getLocationTag().equals(tag)).collect(Collectors.toList());
    }

    /**
     * Gets cloud cluster from remote with either clusterId or clusterName
     *
     * @param clusterName cluster name
     * @param clusterId cluster id
     * @return
     */
    public SelectdbCloud.GetClusterResponse getCloudCluster(String clusterName, String clusterId, String userName) {
        SelectdbCloud.GetClusterRequest.Builder builder =
                SelectdbCloud.GetClusterRequest.newBuilder();
        builder.setCloudUniqueId(Config.cloud_unique_id)
                .setClusterName(clusterName).setClusterId(clusterId).setMysqlUserName(userName);
        final SelectdbCloud.GetClusterRequest pRequest = builder.build();
        SelectdbCloud.GetClusterResponse response;
        try {
            response = MetaServiceProxy.getInstance().getCluster(pRequest);
            return response;
        } catch (RpcException e) {
            LOG.warn("rpcToMetaGetClusterInfo exception: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public int getMinPipelineExecutorSize() {
        if (idToBackendRef.size() == 0) {
            return 1;
        }
        int minPipelineExecutorSize = Integer.MAX_VALUE;
        for (Backend be : idToBackendRef.values()) {
            int size = be.getPipelineExecutorSize();
            if (size > 0) {
                minPipelineExecutorSize = Math.min(minPipelineExecutorSize, size);
            }
        }
        return minPipelineExecutorSize;
    }

    public long aliveBECount() {
        return idToBackendRef.values().stream().filter(Backend::isAlive).count();
    }
}
