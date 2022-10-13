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
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DiskInfo;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.cluster.Cluster;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.CountingDataOutputStream;
import org.apache.doris.ha.FrontendNodeType;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.resource.Tag;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.system.Backend.BackendState;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TStorageMedium;

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
import org.apache.commons.validator.routines.InetAddressValidator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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

    // last backend id used by round robin for sequential selecting backends for replica creation
    private Map<Tag, Long> lastBackendIdForReplicaCreation = Maps.newConcurrentMap();

    private long lastBackendIdForCreation = -1;
    private long lastBackendIdForOther = -1;

    private volatile ImmutableMap<Long, DiskInfo> pathHashToDishInfoRef = ImmutableMap.of();

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

    public List<Backend> getBackendsByClusterName(final String clusterName) {
        String clusterId = clusterNameToId.getOrDefault(clusterName, "");
        if (clusterId.isEmpty()) {
            return new ArrayList<>();
        }
        return clusterIdToBackend.get(clusterId);
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
        clusterNameToId.remove(clusterName);
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

        // FIXME(gavin): is it safe to send hearbeat to an unadded Backend?
        //               Check be/src/agent/heartbeat_server.cpp for more details.
        // backends = Catalog.getCurrentHeartbeatMgr().checkBeStatus(backends);

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
            clusterIdToBackend.replace(clusterId, be);
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

    // for deploy manager
    public void addBackends(List<Pair<String, Integer>> hostPortPairs, boolean isFree) throws UserException {
        addBackends(hostPortPairs, isFree, "", Tag.DEFAULT_BACKEND_TAG.toMap());
    }

    /**
     * @param hostPortPairs : backend's host and port
     * @param isFree : if true the backend is not owned by any cluster
     * @param destCluster : if not null or empty backend will be added to destCluster
     * @throws DdlException
     */
    public void addBackends(List<Pair<String, Integer>> hostPortPairs, boolean isFree, String destCluster,
            Map<String, String> tagMap) throws UserException {
        for (Pair<String, Integer> pair : hostPortPairs) {
            // check is already exist
            if (getBackendWithHeartbeatPort(pair.first, pair.second) != null) {
                throw new DdlException("Same backend already exists[" + pair.first + ":" + pair.second + "]");
            }
        }

        for (Pair<String, Integer> pair : hostPortPairs) {
            addBackend(pair.first, pair.second, isFree, destCluster, tagMap);
        }
    }

    public synchronized void updateCloudFrontends(List<Frontend> toAdd,
                                                  List<Frontend> toDel) throws DdlException {
        LOG.debug("updateCloudFrontends toAdd={} toDel={}", toAdd, toDel);
        String masterIp = Env.getCurrentEnv().getMasterIp();
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
        // TODO(gavin): consider vpc
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

        for (Backend be : toAdd) {
            setBackendOwner(be, DEFAULT_CLUSTER);
            Env.getCurrentEnv().getEditLog().logAddBackend(be);
            LOG.info("added cloud backend={} ", be);
            // backends is changed, regenerated tablet number metrics
            MetricRepo.generateBackendsTabletMetrics();
        }
        for (Backend be : toDel) {
            Env.getCurrentEnv().getEditLog().logDropBackend(be);
            Cluster cluster = Env.getCurrentEnv().getCluster(be.getOwnerClusterName());
            if (null != cluster) {
                cluster.removeBackend(be.getId());
            }
            LOG.info("dropped cloud backend={} ", be);
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

    // for test
    public void addBackend(Backend backend) {
        Map<Long, Backend> copiedBackends = Maps.newHashMap(idToBackendRef);
        copiedBackends.put(backend.getId(), backend);
        ImmutableMap<Long, Backend> newIdToBackend = ImmutableMap.copyOf(copiedBackends);
        idToBackendRef = newIdToBackend;
    }

    private void setBackendOwner(Backend backend, String clusterName) {
        final Cluster cluster = Env.getCurrentEnv().getCluster(clusterName);
        Preconditions.checkState(cluster != null);
        cluster.addBackend(backend.getId());
        backend.setOwnerClusterName(clusterName);
        backend.setBackendState(BackendState.using);
    }

    // Final entry of adding backend
    private void addBackend(String host, int heartbeatPort, boolean isFree, String destCluster,
            Map<String, String> tagMap) {
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

        if (!Strings.isNullOrEmpty(destCluster)) {
            // add backend to destCluster
            setBackendOwner(newBackend, destCluster);
        } else if (!isFree) {
            // add backend to DEFAULT_CLUSTER
            setBackendOwner(newBackend, DEFAULT_CLUSTER);
        } else {
            // backend is free
        }

        // set tags
        newBackend.setTagMap(tagMap);

        // log
        Env.getCurrentEnv().getEditLog().logAddBackend(newBackend);
        LOG.info("finished to add {} ", newBackend);

        // backends is changed, regenerated tablet number metrics
        MetricRepo.generateBackendsTabletMetrics();
    }

    public void dropBackends(List<Pair<String, Integer>> hostPortPairs) throws DdlException {
        for (Pair<String, Integer> pair : hostPortPairs) {
            // check is already exist
            if (getBackendWithHeartbeatPort(pair.first, pair.second) == null) {
                throw new DdlException("backend does not exists[" + pair.first + ":" + pair.second + "]");
            }
        }

        for (Pair<String, Integer> pair : hostPortPairs) {
            dropBackend(pair.first, pair.second);
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
        if (getBackendWithHeartbeatPort(host, heartbeatPort) == null) {
            throw new DdlException("backend does not exists[" + host + ":" + heartbeatPort + "]");
        }

        Backend droppedBackend = getBackendWithHeartbeatPort(host, heartbeatPort);

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

        // update cluster
        final Cluster cluster = Env.getCurrentEnv().getCluster(droppedBackend.getOwnerClusterName());
        if (null != cluster) {
            cluster.removeBackend(droppedBackend.getId());
        } else {
            LOG.error("Cluster " + droppedBackend.getOwnerClusterName() + " no exist.");
        }
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

    public Backend getBackendWithBePort(String host, int bePort) {
        ImmutableMap<Long, Backend> idToBackend = idToBackendRef;
        for (Backend backend : idToBackend.values()) {
            if (backend.getHost().equals(host) && backend.getBePort() == bePort) {
                return backend;
            }
        }
        return null;
    }

    public Backend getBackendWithHttpPort(String host, int httpPort) {
        ImmutableMap<Long, Backend> idToBackend = idToBackendRef;
        for (Backend backend : idToBackend.values()) {
            if (backend.getHost().equals(host) && backend.getHttpPort() == httpPort) {
                return backend;
            }
        }
        return null;
    }

    public List<Long> getBackendIds(boolean needAlive) {
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

    /**
     * choose backends to create cluster
     *
     * @param clusterName
     * @param instanceNum
     * @return if BE available is less than requested , return null.
     */
    public List<Long> createCluster(String clusterName, int instanceNum) {
        final List<Long> chosenBackendIds = Lists.newArrayList();
        final Map<String, List<Backend>> hostBackendsMap = getHostBackendsMap(true /* need alive*/,
                true /* need free */,
                false /* can not be in decommission*/);

        LOG.info("begin to create cluster {} with instance num: {}", clusterName, instanceNum);
        int availableBackendsCount = 0;
        // list of backends on each host.
        List<List<Backend>> hostBackendsList = Lists.newArrayList();
        for (List<Backend> list : hostBackendsMap.values()) {
            availableBackendsCount += list.size();
            hostBackendsList.add(list);
        }

        if (instanceNum > availableBackendsCount) {
            LOG.warn("not enough available backends. requires :" + instanceNum
                    + ", available:" + availableBackendsCount);
            return null;
        }

        //  sort by number of backend in host
        Collections.sort(hostBackendsList, hostBackendsListComparator);

        // hostIsEmpty is used to mark if host is empty, so avoid
        // iterating hostIsEmpty with numOfHost in every circle.
        boolean[] hostIsEmpty = new boolean[hostBackendsList.size()];
        for (int i = 0; i < hostBackendsList.size(); i++) {
            hostIsEmpty[i] = false;
        }
        //  to select backend in circle
        int numOfHost = hostBackendsList.size();
        for (int i = 0; ; i = ++i % hostBackendsList.size()) {
            if (hostBackendsList.get(i).size() > 0) {
                chosenBackendIds.add(hostBackendsList.get(i).remove(0).getId());
            } else {
                // avoid counting repeatedly
                if (hostIsEmpty[i] == false) {
                    hostIsEmpty[i] = true;
                    numOfHost--;
                }
            }
            if (chosenBackendIds.size() == instanceNum || numOfHost == 0) {
                break;
            }
        }

        if (chosenBackendIds.size() != instanceNum) {
            LOG.warn("not enough available backends. require :" + instanceNum + " get:" + chosenBackendIds.size());
            return null;
        }
        return chosenBackendIds;
    }


    /**
     * remove backends in cluster
     *
     * @throws DdlException
     */
    public void releaseBackends(String clusterName, boolean isReplay) {
        ImmutableMap<Long, Backend> idToBackend = idToBackendRef;
        final List<Long> backendIds = getClusterBackendIds(clusterName);
        final Iterator<Long> iterator = backendIds.iterator();

        while (iterator.hasNext()) {
            final Long id = iterator.next();
            if (!idToBackend.containsKey(id)) {
                LOG.warn("cluster {} contain backend {} that doesn't exist", clusterName, id);
            } else {
                final Backend backend = idToBackend.get(id);
                backend.setBackendState(BackendState.free);
                backend.clearClusterName();
                if (!isReplay) {
                    Env.getCurrentEnv().getEditLog().logBackendStateChange(backend);
                }
            }
        }
    }

    /**
     * select host where has least free backends , be's state become free when decommission finish
     *
     * @param shrinkNum
     * @return
     */
    @Deprecated
    public List<Long> calculateDecommissionBackends(String clusterName, int shrinkNum) {
        LOG.info("calculate decommission backend in cluster: {}. decommission num: {}", clusterName, shrinkNum);

        final List<Long> decomBackendIds = Lists.newArrayList();
        ImmutableMap<Long, Backend> idToBackends = idToBackendRef;
        final List<Long> clusterBackends = getClusterBackendIds(clusterName);
        // host -> backends of this cluster
        final Map<String, List<Backend>> hostBackendsMapInCluster = Maps.newHashMap();

        // put backend in same host in list
        for (Long id : clusterBackends) {
            final Backend backend = idToBackends.get(id);
            if (hostBackendsMapInCluster.containsKey(backend.getHost())) {
                hostBackendsMapInCluster.get(backend.getHost()).add(backend);
            } else {
                List<Backend> list = Lists.newArrayList();
                list.add(backend);
                hostBackendsMapInCluster.put(backend.getHost(), list);
            }
        }

        List<List<Backend>> hostList = Lists.newArrayList(hostBackendsMapInCluster.values());
        Collections.sort(hostList, hostBackendsListComparator);

        // in each cycle, choose one backend from the host which has maximal backends num.
        // break if all hosts are empty or get enough backends.
        while (true) {
            if (hostList.get(0).size() > 0) {
                decomBackendIds.add(hostList.get(0).remove(0).getId());
                if (decomBackendIds.size() == shrinkNum) {
                    // enough
                    break;
                }
                Collections.sort(hostList, hostBackendsListComparator);
            } else {
                // all hosts empty
                break;
            }
        }

        if (decomBackendIds.size() != shrinkNum) {
            LOG.info("failed to get enough backends to shrink in cluster: {}. required: {}, get: {}",
                    clusterName, shrinkNum, decomBackendIds.size());
            return null;
        }

        return decomBackendIds;
    }

    /**
     * to expand backends in cluster.
     * firstly, acquire backends from hosts not in this cluster.
     * if not enough, secondly acquire backends from hosts in this cluster, returns a list of hosts
     * sorted by the descending order of the number of backend in the first two ways,
     * and get backends from the list in cycle.
     *
     * @param clusterName
     * @param expansionNum
     * @return
     */
    public List<Long> calculateExpansionBackends(String clusterName, int expansionNum) {
        LOG.debug("calculate expansion backend in cluster: {}, new instance num: {}", clusterName, expansionNum);

        final List<Long> chosenBackendIds = Lists.newArrayList();
        ImmutableMap<Long, Backend> idToBackends = idToBackendRef;
        // host -> backends
        final Map<String, List<Backend>> hostBackendsMap = getHostBackendsMap(true /* need alive*/,
                true /* need free */,
                false /* can not be in decommission */);
        final List<Long> clusterBackends = getClusterBackendIds(clusterName);

        // hosts not in cluster
        List<List<Backend>> hostsNotInCluster = Lists.newArrayList();
        // hosts in cluster
        List<List<Backend>> hostsInCluster = Lists.newArrayList();

        int availableBackendsCount = 0;

        Set<String> hostsSet = Sets.newHashSet();
        for (Long beId : clusterBackends) {
            hostsSet.add(getBackend(beId).getHost());
        }

        // distinguish backend in or not in cluster
        for (Map.Entry<String, List<Backend>> entry : hostBackendsMap.entrySet()) {
            availableBackendsCount += entry.getValue().size();
            if (hostsSet.contains(entry.getKey())) {
                hostsInCluster.add(entry.getValue());
            } else {
                hostsNotInCluster.add(entry.getValue());
            }
        }

        if (expansionNum > availableBackendsCount) {
            LOG.info("not enough available backends. requires :" + expansionNum
                    + ", available:" + availableBackendsCount);
            return null;
        }

        Collections.sort(hostsNotInCluster, hostBackendsListComparator);
        Collections.sort(hostsInCluster, hostBackendsListComparator);

        // first select backends which belong to the hosts NOT IN this cluster
        if (hostsNotInCluster.size() > 0) {
            // hostIsEmpty is used to mark if host is empty, so
            // avoid iterating hostIsEmpty with numOfHost in every circle
            boolean[] hostIsEmpty = new boolean[hostsNotInCluster.size()];
            for (int i = 0; i < hostsNotInCluster.size(); i++) {
                hostIsEmpty[i] = false;
            }
            int numOfHost = hostsNotInCluster.size();
            for (int i = 0; ; i = ++i % hostsNotInCluster.size()) {
                if (hostsNotInCluster.get(i).size() > 0) {
                    chosenBackendIds.add(hostsNotInCluster.get(i).remove(0).getId());
                } else {
                    // avoid counting repeatedly
                    if (hostIsEmpty[i] == false) {
                        hostIsEmpty[i] = true;
                        numOfHost--;
                    }
                }
                if (chosenBackendIds.size() == expansionNum || numOfHost == 0) {
                    break;
                }
            }
        }

        // if not enough, select backends which belong to the hosts IN this cluster
        if (hostsInCluster.size() > 0 && chosenBackendIds.size() != expansionNum) {
            boolean[] hostIsEmpty = new boolean[hostsInCluster.size()];
            for (int i = 0; i < hostsInCluster.size(); i++) {
                hostIsEmpty[i] = false;
            }
            int numOfHost = hostsInCluster.size();
            for (int i = 0; ; i = ++i % hostsInCluster.size()) {
                if (hostsInCluster.get(i).size() > 0) {
                    chosenBackendIds.add(hostsInCluster.get(i).remove(0).getId());
                } else {
                    if (hostIsEmpty[i] == false) {
                        hostIsEmpty[i] = true;
                        numOfHost--;
                    }
                }
                if (chosenBackendIds.size() == expansionNum || numOfHost == 0) {
                    break;
                }
            }
        }

        if (chosenBackendIds.size() != expansionNum) {
            LOG.info("not enough available backends. requires :" + expansionNum
                    + ", get:" + chosenBackendIds.size());
            return null;
        }

        // set be state and owner
        Iterator<Long> iterator = chosenBackendIds.iterator();
        while (iterator.hasNext()) {
            final Long id = iterator.next();
            final Backend backend = idToBackends.get(id);
            backend.setOwnerClusterName(clusterName);
            backend.setBackendState(BackendState.using);
            Env.getCurrentEnv().getEditLog().logBackendStateChange(backend);
        }
        return chosenBackendIds;
    }

    /**
     * get cluster's backend id list
     *
     * @param name
     * @return
     */
    public List<Backend> getClusterBackends(String name) {
        final Map<Long, Backend> copiedBackends = Maps.newHashMap(idToBackendRef);
        final List<Backend> ret = Lists.newArrayList();

        if (Strings.isNullOrEmpty(name)) {
            return ret;
        }

        for (Backend backend : copiedBackends.values()) {
            if (name.equals(backend.getOwnerClusterName())) {
                ret.add(backend);
            }
        }
        return ret;
    }

    /**
     * get cluster's backend id list
     *
     * @param name
     * @return
     */
    public List<Backend> getClusterBackends(String name, boolean needAlive) {
        final Map<Long, Backend> copiedBackends = Maps.newHashMap(idToBackendRef);
        final List<Backend> ret = new ArrayList<Backend>();

        if (Strings.isNullOrEmpty(name)) {
            return null;
        }

        if (needAlive) {
            for (Backend backend : copiedBackends.values()) {
                if (backend != null && name.equals(backend.getOwnerClusterName())
                        && backend.isAlive()) {
                    ret.add(backend);
                }
            }
        } else {
            for (Backend backend : copiedBackends.values()) {
                if (name.equals(backend.getOwnerClusterName())) {
                    ret.add(backend);
                }
            }
        }

        return ret;
    }

    /**
     * get cluster's backend id list
     *
     * @param clusterName
     * @return
     */
    public List<Long> getClusterBackendIds(String clusterName) {
        if (Strings.isNullOrEmpty(clusterName)) {
            return null;
        }

        ImmutableMap<Long, Backend> idToBackend = idToBackendRef;
        final List<Long> beIds = Lists.newArrayList();

        for (Backend backend : idToBackend.values()) {
            if (clusterName.equals(backend.getOwnerClusterName())) {
                beIds.add(backend.getId());
            }
        }
        return beIds;
    }

    /**
     * get cluster's backend id list
     *
     * @param clusterName
     * @return
     */
    public List<Long> getClusterBackendIds(String clusterName, boolean needAlive) {
        final Map<Long, Backend> copiedBackends = Maps.newHashMap(idToBackendRef);
        final List<Long> ret = new ArrayList<Long>();

        if (Strings.isNullOrEmpty(clusterName)) {
            return null;
        }

        if (needAlive) {
            for (Backend backend : copiedBackends.values()) {
                if (backend != null && clusterName.equals(backend.getOwnerClusterName())
                        && backend.isAlive()) {
                    ret.add(backend.getId());
                }
            }
        } else {
            for (Backend backend : copiedBackends.values()) {
                if (clusterName.equals(backend.getOwnerClusterName())) {
                    ret.add(backend.getId());
                }
            }
        }

        return ret;
    }

    /**
     * return backend list in every host
     *
     * @return
     */
    private Map<String, List<Backend>> getHostBackendsMap(boolean needAlive, boolean needFree,
                                                          boolean canBeDecommission) {
        final Map<Long, Backend> copiedBackends = Maps.newHashMap(idToBackendRef);
        final Map<String, List<Backend>> classMap = Maps.newHashMap();

        // to select backend where state is free
        for (Backend backend : copiedBackends.values()) {
            if ((needAlive && !backend.isAlive()) || (needFree && !backend.isFreeFromCluster())
                    || (!canBeDecommission && backend.isDecommissioned())) {
                continue;
            }
            if (classMap.containsKey(backend.getHost())) {
                final List<Backend> list = classMap.get(backend.getHost());
                list.add(backend);
                classMap.put(backend.getHost(), list);
            } else {
                final List<Backend> list = new ArrayList<Backend>();
                list.add(backend);
                classMap.put(backend.getHost(), list);
            }
        }
        return classMap;
    }


    /**
     * Select a set of backends for replica creation.
     * The following parameters need to be considered when selecting backends.
     *
     * @param replicaAlloc
     * @param clusterName
     * @param storageMedium
     * @return return the selected backend ids group by tag.
     * @throws DdlException
     */
    public Map<Tag, List<Long>> selectBackendIdsForReplicaCreation(
            ReplicaAllocation replicaAlloc, String clusterName, TStorageMedium storageMedium)
            throws DdlException {
        Map<Tag, List<Long>> chosenBackendIds = Maps.newHashMap();
        Map<Tag, Short> allocMap = replicaAlloc.getAllocMap();
        short totalReplicaNum = 0;

        for (Map.Entry<Tag, Short> entry : allocMap.entrySet()) {
            BeSelectionPolicy.Builder builder = new BeSelectionPolicy.Builder().setCluster(clusterName)
                    .needScheduleAvailable().needCheckDiskUsage().addTags(Sets.newHashSet(entry.getKey()))
                    .setStorageMedium(storageMedium);
            if (FeConstants.runningUnitTest || Config.allow_replica_on_same_host) {
                builder.allowOnSameHost();
            }

            BeSelectionPolicy policy = builder.build();
            List<Long> beIds = selectBackendIdsByPolicy(policy, entry.getValue());
            if (beIds.isEmpty()) {
                throw new DdlException("Failed to find " + entry.getValue() + " backends for policy: " + policy);
            }
            chosenBackendIds.put(entry.getKey(), beIds);
            totalReplicaNum += beIds.size();
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
     * or empty set if no backends match the policy, or the number of matched backends is less than "number";
     */
    public List<Long> selectBackendIdsByPolicy(BeSelectionPolicy policy, int number) {
        Preconditions.checkArgument(number >= -1);
        List<Backend> candidates =
                idToBackendRef.values().stream().filter(policy::isMatch).collect(Collectors.toList());
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

    public ImmutableMap<Long, Backend> getBackendsInCluster(String cluster) {
        if (Strings.isNullOrEmpty(cluster)) {
            return idToBackendRef;
        }

        Map<Long, Backend> retMaps = Maps.newHashMap();
        for (Backend backend : idToBackendRef.values().asList()) {
            if (cluster.equals(backend.getOwnerClusterName())) {
                retMaps.put(backend.getId(), backend);
            }
        }
        return ImmutableMap.copyOf(retMaps);
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

    public static Pair<String, Integer> validateHostAndPort(String hostPort) throws AnalysisException {
        hostPort = hostPort.replaceAll("\\s+", "");
        if (hostPort.isEmpty()) {
            throw new AnalysisException("Invalid host port: " + hostPort);
        }

        String[] pair = hostPort.split(":");
        if (pair.length != 2) {
            throw new AnalysisException("Invalid host port: " + hostPort);
        }

        String host = pair[0];
        if (Strings.isNullOrEmpty(host)) {
            throw new AnalysisException("Host is null");
        }

        int heartbeatPort = -1;
        try {
            // validate host
            if (!InetAddressValidator.getInstance().isValid(host)) {
                // maybe this is a hostname
                // if no IP address for the host could be found, 'getByName'
                // will throw
                // UnknownHostException
                InetAddress inetAddress = InetAddress.getByName(host);
                host = inetAddress.getHostAddress();
            }

            // validate port
            heartbeatPort = Integer.parseInt(pair[1]);

            if (heartbeatPort <= 0 || heartbeatPort >= 65536) {
                throw new AnalysisException("Port is out of range: " + heartbeatPort);
            }

            return Pair.of(host, heartbeatPort);
        } catch (UnknownHostException e) {
            throw new AnalysisException("Unknown host: " + e.getMessage());
        } catch (Exception e) {
            throw new AnalysisException("Encounter unknown exception: " + e.getMessage());
        }
    }

    public void replayAddBackend(Backend newBackend) {
        // update idToBackend
        Map<Long, Backend> copiedBackends = Maps.newHashMap(idToBackendRef);
        copiedBackends.put(newBackend.getId(), newBackend);
        idToBackendRef = ImmutableMap.copyOf(copiedBackends);

        // set new backend's report version as 0L
        Map<Long, AtomicLong> copiedReportVersions = Maps.newHashMap(idToReportVersionRef);
        copiedReportVersions.put(newBackend.getId(), new AtomicLong(0L));
        idToReportVersionRef = ImmutableMap.copyOf(copiedReportVersions);

        if (!Config.cloud_unique_id.isEmpty()) {
            List<Backend> toAdd = new ArrayList<>();
            toAdd.add(newBackend);
            updateCloudClusterMap(toAdd, new ArrayList<>());
        }

        // to add be to DEFAULT_CLUSTER
        if (newBackend.getBackendState() == BackendState.using) {
            final Cluster cluster = Env.getCurrentEnv().getCluster(DEFAULT_CLUSTER);
            if (null != cluster) {
                // replay log
                cluster.addBackend(newBackend.getId());
            } else {
                // This happens in loading image when fe is restarted, because loadCluster is after loadBackend,
                // cluster is not created. Be in cluster will be updated in loadCluster.
            }
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

        if (!Config.cloud_unique_id.isEmpty()) {
            List<Backend> toDel = new ArrayList<>();
            toDel.add(backend);
            updateCloudClusterMap(new ArrayList<>(), toDel);
        }

        // update cluster
        final Cluster cluster = Env.getCurrentEnv().getCluster(backend.getOwnerClusterName());
        if (null != cluster) {
            cluster.removeBackend(backend.getId());
        } else {
            LOG.error("Cluster " + backend.getOwnerClusterName() + " no exist.");
        }
    }

    public void updateBackendState(Backend be) {
        long id = be.getId();
        Backend memoryBe = getBackend(id);
        if (memoryBe == null) {
            // backend may already be dropped. this may happen when
            // 1. SystemHandler drop the decommission backend
            // 2. at same time, user try to cancel the decommission of that backend.
            // These two operations do not guarantee the order.
            return;
        }
        memoryBe.setBePort(be.getBePort());
        memoryBe.setAlive(be.isAlive());
        memoryBe.setDecommissioned(be.isDecommissioned());
        memoryBe.setHttpPort(be.getHttpPort());
        memoryBe.setBeRpcPort(be.getBeRpcPort());
        memoryBe.setBrpcPort(be.getBrpcPort());
        memoryBe.setLastUpdateMs(be.getLastUpdateMs());
        memoryBe.setLastStartTime(be.getLastStartTime());
        memoryBe.setDisks(be.getDisks());
        memoryBe.setBackendState(be.getBackendState());
        memoryBe.setOwnerClusterName(be.getOwnerClusterName());
        memoryBe.setDecommissionType(be.getDecommissionType());
    }

    private long getClusterAvailableCapacityB(String clusterName) {
        List<Backend> clusterBackends = getClusterBackends(clusterName);
        long capacity = 0L;
        for (Backend backend : clusterBackends) {
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

    public void checkClusterCapacity(String clusterName) throws DdlException {
        if (getClusterAvailableCapacityB(clusterName) <= 0L) {
            throw new DdlException("Cluster " + clusterName + " has no available capacity");
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

    public Set<String> getClusterNames() {
        ImmutableMap<Long, Backend> idToBackend = idToBackendRef;
        Set<String> clusterNames = Sets.newHashSet();
        for (Backend backend : idToBackend.values()) {
            if (!Strings.isNullOrEmpty(backend.getOwnerClusterName())) {
                clusterNames.add(backend.getOwnerClusterName());
            }
        }
        return clusterNames;
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

    public void modifyBackends(ModifyBackendClause alterClause) throws UserException {
        List<Pair<String, Integer>> hostPortPairs = alterClause.getHostPortPairs();
        List<Backend> backends = Lists.newArrayList();
        for (Pair<String, Integer> pair : hostPortPairs) {
            Backend be = getBackendWithHeartbeatPort(pair.first, pair.second);
            if (be == null) {
                throw new DdlException("backend does not exists[" + pair.first + ":" + pair.second + "]");
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
        memBe.setTagMap(backend.getTagMap());
        memBe.setQueryDisabled(backend.isQueryDisabled());
        memBe.setLoadDisabled(backend.isLoadDisabled());
        LOG.debug("replay modify backend: {}", backend);
    }

    // Check if there is enough suitable BE for replica allocation
    public void checkReplicaAllocation(String cluster, ReplicaAllocation replicaAlloc) throws DdlException {
        List<Backend> backends = getClusterBackends(cluster);
        for (Map.Entry<Tag, Short> entry : replicaAlloc.getAllocMap().entrySet()) {
            if (backends.stream().filter(b -> b.getLocationTag().equals(entry.getKey())).count() < entry.getValue()) {
                throw new DdlException(
                        "Failed to find enough host with tag(" + entry.getKey() + ") in all backends. need: "
                                + entry.getValue());
            }
        }
    }

    public Set<Tag> getTagsByCluster(String clusterName) {
        List<Backend> bes = getClusterBackends(clusterName);
        Set<Tag> tags = Sets.newHashSet();
        for (Backend be : bes) {
            tags.add(be.getLocationTag());
        }
        return tags;
    }

    public List<Backend> getBackendsByTagInCluster(String clusterName, Tag tag) {
        List<Backend> bes = getClusterBackends(clusterName);
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
}
