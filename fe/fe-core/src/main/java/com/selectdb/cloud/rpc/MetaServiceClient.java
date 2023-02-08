package com.selectdb.cloud.rpc;

import com.selectdb.cloud.proto.MetaServiceGrpc;
import com.selectdb.cloud.proto.SelectdbCloud;

import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import org.apache.doris.common.Config;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class MetaServiceClient {
    public static final Logger LOG = LogManager.getLogger(MetaServiceClient.class);

    private static final int MAX_RETRY_NUM = 0;
    private final TNetworkAddress address;
    private final MetaServiceGrpc.MetaServiceFutureStub stub;
    private final MetaServiceGrpc.MetaServiceBlockingStub blockingStub;
    private final ManagedChannel channel;

    public MetaServiceClient(TNetworkAddress address) {
        this.address = address;
        channel = NettyChannelBuilder.forAddress(address.getHostname(), address.getPort())
            .flowControlWindow(Config.grpc_max_message_size_bytes)
            .maxInboundMessageSize(Config.grpc_max_message_size_bytes).enableRetry().maxRetryAttempts(MAX_RETRY_NUM)
            .usePlaintext().build();
        stub = MetaServiceGrpc.newFutureStub(channel);
        blockingStub = MetaServiceGrpc.newBlockingStub(channel);
    }

    public void shutdown() {
        if (!channel.isShutdown()) {
            channel.shutdown();
            try {
                if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                    LOG.warn("Timed out gracefully shutting down connection: {}. ", channel);
                }
            } catch (InterruptedException e) {
                return;
            }
        }

        if (!channel.isTerminated()) {
            channel.shutdownNow();
            try {
                if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                    LOG.warn("Timed out forcefully shutting down connection: {}. ", channel);
                }
            } catch (InterruptedException e) {
                return;
            }
        }

        LOG.warn("shut down backend service client: {}", address);
    }

    public Future<SelectdbCloud.GetVersionResponse>
            getVisibleVersionAsync(SelectdbCloud.GetVersionRequest request) {
        if (!request.hasCloudUniqueId()) {
            SelectdbCloud.GetVersionRequest.Builder builder =
                    SelectdbCloud.GetVersionRequest.newBuilder();
            builder.mergeFrom(request);
            return stub.getVersion(builder.setCloudUniqueId(Config.cloud_unique_id).build());
        }
        return stub.getVersion(request);
    }

    public SelectdbCloud.GetVersionResponse getVersion(SelectdbCloud.GetVersionRequest request) {
        if (!request.hasCloudUniqueId()) {
            SelectdbCloud.GetVersionRequest.Builder builder =
                    SelectdbCloud.GetVersionRequest.newBuilder();
            builder.mergeFrom(request);
            return blockingStub.getVersion(builder.setCloudUniqueId(Config.cloud_unique_id).build());
        }
        return blockingStub.getVersion(request);
    }

    public SelectdbCloud.CreateTabletsResponse createTablets(SelectdbCloud.CreateTabletsRequest request) {
        if (!request.hasCloudUniqueId()) {
            SelectdbCloud.CreateTabletsRequest.Builder builder =
                    SelectdbCloud.CreateTabletsRequest.newBuilder();
            builder.mergeFrom(request);
            return blockingStub.createTablets(builder.setCloudUniqueId(Config.cloud_unique_id).build());
        }
        return blockingStub.createTablets(request);
    }

    public SelectdbCloud.UpdateTabletResponse updateTablet(SelectdbCloud.UpdateTabletRequest request) {
        if (!request.hasCloudUniqueId()) {
            SelectdbCloud.UpdateTabletRequest.Builder builder =
                    SelectdbCloud.UpdateTabletRequest.newBuilder();
            builder.mergeFrom(request);
            return blockingStub.updateTablet(builder.setCloudUniqueId(Config.cloud_unique_id).build());
        }
        return blockingStub.updateTablet(request);
    }

    public SelectdbCloud.BeginTxnResponse
            beginTxn(SelectdbCloud.BeginTxnRequest request) {
        if (!request.hasCloudUniqueId()) {
            SelectdbCloud.BeginTxnRequest.Builder builder =
                    SelectdbCloud.BeginTxnRequest.newBuilder();
            builder.mergeFrom(request);
            return blockingStub.beginTxn(builder.setCloudUniqueId(Config.cloud_unique_id).build());
        }
        return blockingStub.beginTxn(request);
    }

    public SelectdbCloud.PrecommitTxnResponse
            precommitTxn(SelectdbCloud.PrecommitTxnRequest request) {
        if (!request.hasCloudUniqueId()) {
            SelectdbCloud.PrecommitTxnRequest.Builder builder =
                    SelectdbCloud.PrecommitTxnRequest.newBuilder();
            builder.mergeFrom(request);
            return blockingStub.precommitTxn(builder.setCloudUniqueId(Config.cloud_unique_id).build());
        }
        return blockingStub.precommitTxn(request);
    }

    public SelectdbCloud.CommitTxnResponse
            commitTxn(SelectdbCloud.CommitTxnRequest request) {
        if (!request.hasCloudUniqueId()) {
            SelectdbCloud.CommitTxnRequest.Builder builder =
                    SelectdbCloud.CommitTxnRequest.newBuilder();
            builder.mergeFrom(request);
            return blockingStub.commitTxn(builder.setCloudUniqueId(Config.cloud_unique_id).build());
        }
        return blockingStub.commitTxn(request);
    }

    public SelectdbCloud.AbortTxnResponse
            abortTxn(SelectdbCloud.AbortTxnRequest request) {
        if (!request.hasCloudUniqueId()) {
            SelectdbCloud.AbortTxnRequest.Builder builder =
                    SelectdbCloud.AbortTxnRequest.newBuilder();
            builder.mergeFrom(request);
            return blockingStub.abortTxn(builder.setCloudUniqueId(Config.cloud_unique_id).build());
        }
        return blockingStub.abortTxn(request);
    }

    public SelectdbCloud.GetTxnResponse
            getTxn(SelectdbCloud.GetTxnRequest request) {
        return blockingStub.getTxn(request);
    }

    public SelectdbCloud.GetCurrentMaxTxnResponse
            getCurrentMaxTxnId(SelectdbCloud.GetCurrentMaxTxnRequest request) {
        return blockingStub.getCurrentMaxTxnId(request);
    }

    public SelectdbCloud.CheckTxnConflictResponse
            checkTxnConflict(SelectdbCloud.CheckTxnConflictRequest request) {
        return blockingStub.checkTxnConflict(request);
    }

    public SelectdbCloud.GetClusterResponse getCluster(SelectdbCloud.GetClusterRequest request) {
        if (!request.hasCloudUniqueId()) {
            SelectdbCloud.GetClusterRequest.Builder builder =
                    SelectdbCloud.GetClusterRequest.newBuilder();
            builder.mergeFrom(request);
            return blockingStub.getCluster(builder.setCloudUniqueId(Config.cloud_unique_id).build());
        }
        return blockingStub.getCluster(request);
    }

    public SelectdbCloud.IndexResponse
            prepareIndex(SelectdbCloud.IndexRequest request) {
        return blockingStub.prepareIndex(request);
    }

    public SelectdbCloud.IndexResponse
            commitIndex(SelectdbCloud.IndexRequest request) {
        return blockingStub.commitIndex(request);
    }

    public SelectdbCloud.IndexResponse
            dropIndex(SelectdbCloud.IndexRequest request) {
        return blockingStub.dropIndex(request);
    }

    public SelectdbCloud.PartitionResponse
            preparePartition(SelectdbCloud.PartitionRequest request) {
        return blockingStub.preparePartition(request);
    }

    public SelectdbCloud.PartitionResponse
            commitPartition(SelectdbCloud.PartitionRequest request) {
        return blockingStub.commitPartition(request);
    }

    public SelectdbCloud.PartitionResponse
            dropPartition(SelectdbCloud.PartitionRequest request) {
        return blockingStub.dropPartition(request);
    }

    public SelectdbCloud.GetTabletStatsResponse getTabletStats(SelectdbCloud.GetTabletStatsRequest request) {
        if (!request.hasCloudUniqueId()) {
            SelectdbCloud.GetTabletStatsRequest.Builder builder =
                    SelectdbCloud.GetTabletStatsRequest.newBuilder();
            builder.mergeFrom(request);
            return blockingStub.getTabletStats(builder.setCloudUniqueId(Config.cloud_unique_id).build());
        }
        return blockingStub.getTabletStats(request);
    }

    public SelectdbCloud.CreateStageResponse createStage(SelectdbCloud.CreateStageRequest request) {
        return blockingStub.createStage(request);
    }

    public SelectdbCloud.GetStageResponse getStage(SelectdbCloud.GetStageRequest request) {
        return blockingStub.getStage(request);
    }

    public SelectdbCloud.DropStageResponse dropStage(SelectdbCloud.DropStageRequest request) {
        return blockingStub.dropStage(request);
    }

    public SelectdbCloud.GetIamResponse getIam(SelectdbCloud.GetIamRequest request) {
        return blockingStub.getIam(request);
    }

    public SelectdbCloud.BeginCopyResponse beginCopy(SelectdbCloud.BeginCopyRequest request) {
        return blockingStub.beginCopy(request);
    }

    public SelectdbCloud.FinishCopyResponse finishCopy(SelectdbCloud.FinishCopyRequest request) {
        return blockingStub.finishCopy(request);
    }

    public SelectdbCloud.GetCopyJobResponse getCopyJob(SelectdbCloud.GetCopyJobRequest request) {
        return blockingStub.getCopyJob(request);
    }

    public SelectdbCloud.GetCopyFilesResponse getCopyFiles(SelectdbCloud.GetCopyFilesRequest request) {
        return blockingStub.getCopyFiles(request);
    }

    public SelectdbCloud.AlterClusterResponse alterCluster(SelectdbCloud.AlterClusterRequest request) {
        return blockingStub.alterCluster(request);
    }
}
