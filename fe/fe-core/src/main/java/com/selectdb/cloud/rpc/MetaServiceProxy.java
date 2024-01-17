package com.selectdb.cloud.rpc;

import com.selectdb.cloud.proto.SelectdbCloud;

import com.google.common.collect.Maps;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class MetaServiceProxy {
    private static final Logger LOG = LogManager.getLogger(MetaServiceProxy.class);
    // use exclusive lock to make sure only one thread can add or remove client from serviceMap.
    // use concurrent map to allow access serviceMap in multi thread.
    private static Pair<String, Integer> metaServiceHostPort = null;

    static {
        if (Config.isCloudMode()) {
            try {
                metaServiceHostPort = SystemInfoService.validateHostAndPort(Config.meta_service_endpoint);
            } catch (AnalysisException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private ReentrantLock lock = new ReentrantLock();

    private final Map<TNetworkAddress, MetaServiceClient> serviceMap;

    public MetaServiceProxy() {
        this.serviceMap = Maps.newConcurrentMap();
    }

    private static class SingletonHolder {
        private static AtomicInteger count = new AtomicInteger();
        private static MetaServiceProxy[] proxies;

        static {
            if (Config.isCloudMode()) {
                int size = Config.meta_service_connection_pooled
                        ? Config.meta_service_connection_pool_size : 1;
                proxies = new MetaServiceProxy[size];
                for (int i = 0; i < size; ++i) {
                    proxies[i] = new MetaServiceProxy();
                }
            }
        }

        static MetaServiceProxy get() {
            return proxies[Math.abs(count.addAndGet(1) % proxies.length)];
        }
    }

    public static MetaServiceProxy getInstance() {
        return MetaServiceProxy.SingletonHolder.get();
    }

    public SelectdbCloud.GetInstanceResponse getInstance(SelectdbCloud.GetInstanceRequest request)
            throws RpcException {
        if (metaServiceHostPort == null) {
            throw new RpcException("", "cloud mode, please configure cloud_unique_id and meta_service_endpoint");
        }
        TNetworkAddress metaAddress = new TNetworkAddress(metaServiceHostPort.first, metaServiceHostPort.second);
        try {
            final MetaServiceClient client = getProxy(metaAddress);
            return client.getInstance(request);
        } catch (Exception e) {
            throw new RpcException(metaAddress.hostname, e.getMessage(), e);
        }
    }

    public void removeProxy(TNetworkAddress address) {
        LOG.warn("begin to remove proxy: {}", address);
        MetaServiceClient service;
        lock.lock();
        try {
            service = serviceMap.remove(address);
        } finally {
            lock.unlock();
        }

        if (service != null) {
            service.shutdown(false);
        }
    }

    private MetaServiceClient getProxy(TNetworkAddress address) {
        MetaServiceClient service = serviceMap.get(address);
        if (service != null && service.isNormalState()) {
            return service;
        }

        // not exist, create one and return.
        MetaServiceClient removedClient = null;
        lock.lock();
        try {
            service = serviceMap.get(address);
            if (service != null && !service.isNormalState()) {
                // At this point we cannot judge the progress of reconnecting the underlying channel.
                // In the worst case, it may take two minutes. But we can't stand the connection refused
                // for two minutes, so rebuild the channel directly.
                serviceMap.remove(address);
                removedClient = service;
                service = null;
            }
            if (service != null && !service.isConnectionAgeExpired()) {
                serviceMap.remove(address);
                removedClient = service;
                service = null;
            }
            if (service == null) {
                service = new MetaServiceClient(address);
                serviceMap.put(address, service);
            }
            return service;
        } finally {
            lock.unlock();
            if (removedClient != null) {
                removedClient.shutdown(true);
            }
        }
    }

    public Future<SelectdbCloud.GetVersionResponse>
            getVisibleVersionAsync(SelectdbCloud.GetVersionRequest request)
            throws RpcException {
        if (metaServiceHostPort == null) {
            throw new RpcException("", "cloud mode, please configure cloud_unique_id and meta_service_endpoint");
        }
        TNetworkAddress metaAddress = new TNetworkAddress(metaServiceHostPort.first, metaServiceHostPort.second);
        try {
            final MetaServiceClient client = getProxy(metaAddress);
            return client.getVisibleVersionAsync(request);
        } catch (Exception e) {
            throw new RpcException(metaAddress.hostname, e.getMessage(), e);
        }
    }

    public SelectdbCloud.GetVersionResponse
            getVersion(SelectdbCloud.GetVersionRequest request) throws RpcException {
        if (metaServiceHostPort == null) {
            throw new RpcException("", "cloud mode, please configure cloud_unique_id and meta_service_endpoint");
        }
        TNetworkAddress metaAddress = new TNetworkAddress(metaServiceHostPort.first, metaServiceHostPort.second);
        try {
            final MetaServiceClient client = getProxy(metaAddress);
            return client.getVersion(request);
        } catch (Exception e) {
            throw new RpcException(metaAddress.hostname, e.getMessage(), e);
        }
    }

    public SelectdbCloud.CreateTabletsResponse
            createTablets(SelectdbCloud.CreateTabletsRequest request) throws RpcException {
        if (metaServiceHostPort == null) {
            throw new RpcException("", "cloud mode, please configure cloud_unique_id and meta_service_endpoint");
        }
        TNetworkAddress metaAddress = new TNetworkAddress(metaServiceHostPort.first, metaServiceHostPort.second);
        try {
            final MetaServiceClient client = getProxy(metaAddress);
            return client.createTablets(request);
        } catch (Exception e) {
            throw new RpcException(metaAddress.hostname, e.getMessage(), e);
        }
    }

    public SelectdbCloud.UpdateTabletResponse
            updateTablet(SelectdbCloud.UpdateTabletRequest request) throws RpcException {
        TNetworkAddress metaAddress = new TNetworkAddress(metaServiceHostPort.first, metaServiceHostPort.second);
        try {
            final MetaServiceClient client = getProxy(metaAddress);
            return client.updateTablet(request);
        } catch (Exception e) {
            throw new RpcException(metaAddress.hostname, e.getMessage(), e);
        }
    }

    public SelectdbCloud.BeginTxnResponse
            beginTxn(SelectdbCloud.BeginTxnRequest request)
            throws RpcException {
        if (metaServiceHostPort == null) {
            throw new RpcException("", "cloud mode, please configure cloud_unique_id and meta_service_endpoint");
        }
        TNetworkAddress metaAddress = new TNetworkAddress(metaServiceHostPort.first, metaServiceHostPort.second);
        try {
            final MetaServiceClient client = getProxy(metaAddress);
            return client.beginTxn(request);
        } catch (Exception e) {
            throw new RpcException(metaAddress.hostname, e.getMessage(), e);
        }
    }

    public SelectdbCloud.PrecommitTxnResponse
            precommitTxn(SelectdbCloud.PrecommitTxnRequest request)
            throws RpcException {
        if (metaServiceHostPort == null) {
            throw new RpcException("", "cloud mode, please configure cloud_unique_id and meta_service_endpoint");
        }
        TNetworkAddress metaAddress = new TNetworkAddress(metaServiceHostPort.first, metaServiceHostPort.second);
        try {
            final MetaServiceClient client = getProxy(metaAddress);
            return client.precommitTxn(request);
        } catch (Exception e) {
            throw new RpcException(metaAddress.hostname, e.getMessage(), e);
        }
    }

    public SelectdbCloud.CommitTxnResponse
            commitTxn(SelectdbCloud.CommitTxnRequest request)
            throws RpcException {
        if (metaServiceHostPort == null) {
            throw new RpcException("", "cloud mode, please configure cloud_unique_id and meta_service_endpoint");
        }
        TNetworkAddress metaAddress = new TNetworkAddress(metaServiceHostPort.first, metaServiceHostPort.second);
        try {
            final MetaServiceClient client = getProxy(metaAddress);
            return client.commitTxn(request);
        } catch (Exception e) {
            throw new RpcException(metaAddress.hostname, e.getMessage(), e);
        }
    }

    public SelectdbCloud.AbortTxnResponse
            abortTxn(SelectdbCloud.AbortTxnRequest request)
            throws RpcException {
        if (metaServiceHostPort == null) {
            throw new RpcException("", "cloud mode, please configure cloud_unique_id and meta_service_endpoint");
        }
        TNetworkAddress metaAddress = new TNetworkAddress(metaServiceHostPort.first, metaServiceHostPort.second);
        try {
            final MetaServiceClient client = getProxy(metaAddress);
            return client.abortTxn(request);
        } catch (Exception e) {
            throw new RpcException(metaAddress.hostname, e.getMessage(), e);
        }
    }

    public SelectdbCloud.GetTxnResponse
            getTxn(SelectdbCloud.GetTxnRequest request)
            throws RpcException {
        if (metaServiceHostPort == null) {
            throw new RpcException("", "cloud mode, please configure cloud_unique_id and meta_service_endpoint");
        }
        TNetworkAddress metaAddress = new TNetworkAddress(metaServiceHostPort.first, metaServiceHostPort.second);
        try {
            final MetaServiceClient client = getProxy(metaAddress);
            return client.getTxn(request);
        } catch (Exception e) {
            throw new RpcException(metaAddress.hostname, e.getMessage(), e);
        }
    }

    public SelectdbCloud.GetCurrentMaxTxnResponse
            getCurrentMaxTxnId(SelectdbCloud.GetCurrentMaxTxnRequest request)
            throws RpcException {
        if (metaServiceHostPort == null) {
            throw new RpcException("", "cloud mode, please configure cloud_unique_id and meta_service_endpoint");
        }
        TNetworkAddress metaAddress = new TNetworkAddress(metaServiceHostPort.first, metaServiceHostPort.second);
        try {
            final MetaServiceClient client = getProxy(metaAddress);
            return client.getCurrentMaxTxnId(request);
        } catch (Exception e) {
            throw new RpcException(metaAddress.hostname, e.getMessage(), e);
        }
    }

    public SelectdbCloud.CheckTxnConflictResponse
            checkTxnConflict(SelectdbCloud.CheckTxnConflictRequest request)
            throws RpcException {
        if (metaServiceHostPort == null) {
            throw new RpcException("", "cloud mode, please configure cloud_unique_id and meta_service_endpoint");
        }
        TNetworkAddress metaAddress = new TNetworkAddress(metaServiceHostPort.first, metaServiceHostPort.second);
        try {
            final MetaServiceClient client = getProxy(metaAddress);
            return client.checkTxnConflict(request);
        } catch (Exception e) {
            throw new RpcException(metaAddress.hostname, e.getMessage(), e);
        }
    }

    public SelectdbCloud.CleanTxnLabelResponse
            cleanTxnLabel(SelectdbCloud.CleanTxnLabelRequest request)
            throws RpcException {
        if (metaServiceHostPort == null) {
            throw new RpcException("", "cloud mode, please configure cloud_unique_id and meta_service_endpoint");
        }
        TNetworkAddress metaAddress = new TNetworkAddress(metaServiceHostPort.first, metaServiceHostPort.second);
        try {
            final MetaServiceClient client = getProxy(metaAddress);
            return client.cleanTxnLabel(request);
        } catch (Exception e) {
            throw new RpcException(metaAddress.hostname, e.getMessage(), e);
        }
    }

    public SelectdbCloud.GetClusterResponse
            getCluster(SelectdbCloud.GetClusterRequest request) throws RpcException {
        if (metaServiceHostPort == null) {
            throw new RpcException("", "cloud mode, please configure cloud_unique_id and meta_service_endpoint");
        }
        TNetworkAddress metaAddress = new TNetworkAddress(metaServiceHostPort.first, metaServiceHostPort.second);
        try {
            final MetaServiceClient client = getProxy(metaAddress);
            return client.getCluster(request);
        } catch (Exception e) {
            throw new RpcException(metaAddress.hostname, e.getMessage(), e);
        }
    }

    public SelectdbCloud.IndexResponse
            prepareIndex(SelectdbCloud.IndexRequest request) throws RpcException {
        if (metaServiceHostPort == null) {
            throw new RpcException("", "cloud mode, please configure cloud_unique_id and meta_service_endpoint");
        }
        TNetworkAddress metaAddress = new TNetworkAddress(metaServiceHostPort.first, metaServiceHostPort.second);
        try {
            final MetaServiceClient client = getProxy(metaAddress);
            return client.prepareIndex(request);
        } catch (Exception e) {
            throw new RpcException(metaAddress.hostname, e.getMessage(), e);
        }
    }

    public SelectdbCloud.IndexResponse
            commitIndex(SelectdbCloud.IndexRequest request) throws RpcException {
        if (metaServiceHostPort == null) {
            throw new RpcException("", "cloud mode, please configure cloud_unique_id and meta_service_endpoint");
        }
        TNetworkAddress metaAddress = new TNetworkAddress(metaServiceHostPort.first, metaServiceHostPort.second);
        try {
            final MetaServiceClient client = getProxy(metaAddress);
            return client.commitIndex(request);
        } catch (Exception e) {
            throw new RpcException(metaAddress.hostname, e.getMessage(), e);
        }
    }

    public SelectdbCloud.IndexResponse
            dropIndex(SelectdbCloud.IndexRequest request) throws RpcException {
        if (metaServiceHostPort == null) {
            throw new RpcException("", "cloud mode, please configure cloud_unique_id and meta_service_endpoint");
        }
        TNetworkAddress metaAddress = new TNetworkAddress(metaServiceHostPort.first, metaServiceHostPort.second);
        try {
            final MetaServiceClient client = getProxy(metaAddress);
            return client.dropIndex(request);
        } catch (Exception e) {
            throw new RpcException(metaAddress.hostname, e.getMessage(), e);
        }
    }

    public SelectdbCloud.PartitionResponse preparePartition(SelectdbCloud.PartitionRequest request)
            throws RpcException {
        if (metaServiceHostPort == null) {
            throw new RpcException("", "cloud mode, please configure cloud_unique_id and meta_service_endpoint");
        }
        TNetworkAddress metaAddress = new TNetworkAddress(metaServiceHostPort.first, metaServiceHostPort.second);
        try {
            final MetaServiceClient client = getProxy(metaAddress);
            return client.preparePartition(request);
        } catch (Exception e) {
            throw new RpcException(metaAddress.hostname, e.getMessage(), e);
        }
    }

    public SelectdbCloud.PartitionResponse
            commitPartition(SelectdbCloud.PartitionRequest request) throws RpcException {
        if (metaServiceHostPort == null) {
            throw new RpcException("", "cloud mode, please configure cloud_unique_id and meta_service_endpoint");
        }
        TNetworkAddress metaAddress = new TNetworkAddress(metaServiceHostPort.first, metaServiceHostPort.second);
        try {
            final MetaServiceClient client = getProxy(metaAddress);
            return client.commitPartition(request);
        } catch (Exception e) {
            throw new RpcException(metaAddress.hostname, e.getMessage(), e);
        }
    }

    public SelectdbCloud.PartitionResponse
            dropPartition(SelectdbCloud.PartitionRequest request) throws RpcException {
        if (metaServiceHostPort == null) {
            throw new RpcException("", "cloud mode, please configure cloud_unique_id and meta_service_endpoint");
        }
        TNetworkAddress metaAddress = new TNetworkAddress(metaServiceHostPort.first, metaServiceHostPort.second);
        try {
            final MetaServiceClient client = getProxy(metaAddress);
            return client.dropPartition(request);
        } catch (Exception e) {
            throw new RpcException(metaAddress.hostname, e.getMessage(), e);
        }
    }

    public SelectdbCloud.GetTabletStatsResponse
            getTabletStats(SelectdbCloud.GetTabletStatsRequest request) throws RpcException {
        if (metaServiceHostPort == null) {
            throw new RpcException("", "cloud mode, please configure cloud_unique_id and meta_service_endpoint");
        }
        TNetworkAddress metaAddress = new TNetworkAddress(metaServiceHostPort.first, metaServiceHostPort.second);
        try {
            final MetaServiceClient client = getProxy(metaAddress);
            return client.getTabletStats(request);
        } catch (Exception e) {
            throw new RpcException(metaAddress.hostname, e.getMessage(), e);
        }
    }

    public SelectdbCloud.CreateStageResponse createStage(SelectdbCloud.CreateStageRequest request) throws RpcException {
        if (metaServiceHostPort == null) {
            throw new RpcException("", "cloud mode, please configure cloud_unique_id and meta_service_endpoint");
        }
        TNetworkAddress metaAddress = new TNetworkAddress(metaServiceHostPort.first, metaServiceHostPort.second);
        try {
            final MetaServiceClient client = getProxy(metaAddress);
            return client.createStage(request);
        } catch (Exception e) {
            throw new RpcException(metaAddress.hostname, e.getMessage(), e);
        }
    }

    public SelectdbCloud.GetStageResponse getStage(SelectdbCloud.GetStageRequest request) throws RpcException {
        if (metaServiceHostPort == null) {
            throw new RpcException("", "cloud mode, please configure cloud_unique_id and meta_service_endpoint");
        }
        TNetworkAddress metaAddress = new TNetworkAddress(metaServiceHostPort.first, metaServiceHostPort.second);
        try {
            final MetaServiceClient client = getProxy(metaAddress);
            return client.getStage(request);
        } catch (Exception e) {
            throw new RpcException(metaAddress.hostname, e.getMessage(), e);
        }
    }

    public SelectdbCloud.DropStageResponse dropStage(SelectdbCloud.DropStageRequest request) throws RpcException {
        if (metaServiceHostPort == null) {
            throw new RpcException("", "cloud mode, please configure cloud_unique_id and meta_service_endpoint");
        }
        TNetworkAddress metaAddress = new TNetworkAddress(metaServiceHostPort.first, metaServiceHostPort.second);
        try {
            final MetaServiceClient client = getProxy(metaAddress);
            return client.dropStage(request);
        } catch (Exception e) {
            throw new RpcException(metaAddress.hostname, e.getMessage(), e);
        }
    }

    public SelectdbCloud.GetIamResponse getIam(SelectdbCloud.GetIamRequest request) throws RpcException {
        if (metaServiceHostPort == null) {
            throw new RpcException("", "cloud mode, please configure cloud_unique_id and meta_service_endpoint");
        }
        TNetworkAddress metaAddress = new TNetworkAddress(metaServiceHostPort.first, metaServiceHostPort.second);
        try {
            final MetaServiceClient client = getProxy(metaAddress);
            return client.getIam(request);
        } catch (Exception e) {
            throw new RpcException(metaAddress.hostname, e.getMessage(), e);
        }
    }

    public SelectdbCloud.BeginCopyResponse beginCopy(SelectdbCloud.BeginCopyRequest request) throws RpcException {
        if (metaServiceHostPort == null) {
            throw new RpcException("", "cloud mode, please configure cloud_unique_id and meta_service_endpoint");
        }
        TNetworkAddress metaAddress = new TNetworkAddress(metaServiceHostPort.first, metaServiceHostPort.second);
        try {
            final MetaServiceClient client = getProxy(metaAddress);
            return client.beginCopy(request);
        } catch (Exception e) {
            throw new RpcException(metaAddress.hostname, e.getMessage(), e);
        }
    }

    public SelectdbCloud.FinishCopyResponse finishCopy(SelectdbCloud.FinishCopyRequest request) throws RpcException {
        if (metaServiceHostPort == null) {
            throw new RpcException("", "cloud mode, please configure cloud_unique_id and meta_service_endpoint");
        }
        TNetworkAddress metaAddress = new TNetworkAddress(metaServiceHostPort.first, metaServiceHostPort.second);
        try {
            final MetaServiceClient client = getProxy(metaAddress);
            return client.finishCopy(request);
        } catch (Exception e) {
            throw new RpcException(metaAddress.hostname, e.getMessage(), e);
        }
    }

    public SelectdbCloud.GetCopyJobResponse getCopyJob(SelectdbCloud.GetCopyJobRequest request) throws RpcException {
        if (metaServiceHostPort == null) {
            throw new RpcException("", "cloud mode, please configure cloud_unique_id and meta_service_endpoint");
        }
        TNetworkAddress metaAddress = new TNetworkAddress(metaServiceHostPort.first, metaServiceHostPort.second);
        try {
            final MetaServiceClient client = getProxy(metaAddress);
            return client.getCopyJob(request);
        } catch (Exception e) {
            throw new RpcException(metaAddress.hostname, e.getMessage(), e);
        }
    }

    public SelectdbCloud.GetCopyFilesResponse getCopyFiles(SelectdbCloud.GetCopyFilesRequest request)
            throws RpcException {
        if (metaServiceHostPort == null) {
            throw new RpcException("", "cloud mode, please configure cloud_unique_id and meta_service_endpoint");
        }
        TNetworkAddress metaAddress = new TNetworkAddress(metaServiceHostPort.first, metaServiceHostPort.second);
        try {
            final MetaServiceClient client = getProxy(metaAddress);
            return client.getCopyFiles(request);
        } catch (Exception e) {
            throw new RpcException(metaAddress.hostname, e.getMessage(), e);
        }
    }

    public SelectdbCloud.FilterCopyFilesResponse filterCopyFiles(SelectdbCloud.FilterCopyFilesRequest request)
            throws RpcException {
        if (metaServiceHostPort == null) {
            throw new RpcException("", "cloud mode, please configure cloud_unique_id and meta_service_endpoint");
        }
        TNetworkAddress metaAddress = new TNetworkAddress(metaServiceHostPort.first, metaServiceHostPort.second);
        try {
            final MetaServiceClient client = getProxy(metaAddress);
            return client.filterCopyFiles(request);
        } catch (Exception e) {
            throw new RpcException(metaAddress.hostname, e.getMessage(), e);
        }
    }

    public SelectdbCloud.AlterClusterResponse alterCluster(SelectdbCloud.AlterClusterRequest request)
            throws RpcException {
        if (metaServiceHostPort == null) {
            throw new RpcException("", "cloud mode, please configure cloud_unique_id and meta_service_endpoint");
        }
        TNetworkAddress metaAddress = new TNetworkAddress(metaServiceHostPort.first, metaServiceHostPort.second);
        try {
            final MetaServiceClient client = getProxy(metaAddress);
            return client.alterCluster(request);
        } catch (Exception e) {
            throw new RpcException(metaAddress.hostname, e.getMessage(), e);
        }
    }

    public SelectdbCloud.GetDeleteBitmapUpdateLockResponse
            getDeleteBitmapUpdateLock(SelectdbCloud.GetDeleteBitmapUpdateLockRequest request)
            throws RpcException {
        if (metaServiceHostPort == null) {
            throw new RpcException("", "cloud mode, please configure cloud_unique_id and meta_service_endpoint");
        }
        TNetworkAddress metaAddress = new TNetworkAddress(metaServiceHostPort.first, metaServiceHostPort.second);
        try {
            final MetaServiceClient client = getProxy(metaAddress);
            return client.getDeleteBitmapUpdateLock(request);
        } catch (Exception e) {
            throw new RpcException(metaAddress.hostname, e.getMessage(), e);
        }
    }

    public SelectdbCloud.GetRLTaskCommitAttachResponse
            getRLTaskCommitAttach(SelectdbCloud.GetRLTaskCommitAttachRequest request)
            throws RpcException {
        if (metaServiceHostPort == null) {
            throw new RpcException("", "cloud mode, please configure cloud_unique_id and meta_service_endpoint");
        }
        TNetworkAddress metaAddress = new TNetworkAddress(metaServiceHostPort.first, metaServiceHostPort.second);
        try {
            final MetaServiceClient client = getProxy(metaAddress);
            return client.getRLTaskCommitAttach(request);
        } catch (Exception e) {
            throw new RpcException(metaAddress.hostname, e.getMessage(), e);
        }
    }
}
