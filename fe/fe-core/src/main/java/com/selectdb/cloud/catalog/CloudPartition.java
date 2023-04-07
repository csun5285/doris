package com.selectdb.cloud.catalog;

import com.selectdb.cloud.proto.SelectdbCloud;
import com.selectdb.cloud.proto.SelectdbCloud.MetaServiceCode;
import com.selectdb.cloud.rpc.MetaServiceProxy;

import com.google.gson.annotations.SerializedName;
import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.Partition;
import org.apache.doris.rpc.RpcException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Internal representation of partition-related metadata.
 */
// TODO(dx): cache version
public class CloudPartition extends Partition {
    private static final Logger LOG = LogManager.getLogger(CloudPartition.class);

    // not Serialized
    @SerializedName(value = "dbId")
    private long dbId;
    @SerializedName(value = "tableId")
    private long tableId;

    public CloudPartition(long id, String name, MaterializedIndex baseIndex,
                          DistributionInfo distributionInfo, long dbId, long tableId) {
        super(id, name, baseIndex, distributionInfo);
        super.nextVersion = -1;
        this.dbId = dbId;
        this.tableId = tableId;
    }

    public CloudPartition() {
        super();
    }

    public void setDbId(long dbId) {
        this.dbId = dbId;
    }

    public void setTableId(long tableId) {
        this.tableId = tableId;
    }

    protected void setVisibleVersion(long visibleVersion) {
        LOG.debug("setVisibleVersion use CloudPartition {}", super.getName());
        return;
    }

    @Override
    public long getVisibleVersion() {
        LOG.debug("getVisibleVersion use CloudPartition {}", super.getName());
        long version = getVersionFromMeta(System.currentTimeMillis() + 3000);
        if (version != -1) {
            // get version from metaService success, set visible version
            super.setVisibleVersion(version);
        } else {
            version = super.getVisibleVersion();
            LOG.warn("failed to get version from metaService, use previous version, parition={} version={}",
                    getName(), version);
        }
        return version;
    }

    @Override
    public long getNextVersion() {
        // use meta service visibleVersion
        LOG.debug("getNextVersion use CloudPartition {}", super.getName());
        return -1;
    }

    @Override
    public void setNextVersion(long nextVersion) {
        // use meta service visibleVersion
        LOG.debug("setNextVersion use CloudPartition {} Version {}", super.getName(), nextVersion);
        return;
    }

    @Override
    public void updateVersionForRestore(long visibleVersion) {
        LOG.debug("updateVersionForRestore use CloudPartition {} version for restore: visible: {}",
                super.getName(), visibleVersion);
        return;
    }

    @Override
    public void updateVisibleVersion(long visibleVersion) {
        // use meta service visibleVersion
        LOG.debug("updateVisibleVersion use CloudPartition {} version for restore: visible: {}",
                super.getName(), visibleVersion);

        return;
    }

    @Override
    public void updateVisibleVersionAndTime(long visibleVersion, long visibleVersionTime) {
    }

    /**
     * CloudPartition always has data
     */
    @Override
    public boolean hasData() {
        // Every partition starts from version 1, version 1 has no deta
        return getVisibleVersion() > 1;
    }

    /**
     * calc all constant exprs by BE
     * @return failed: -1, success: other
     */
    public long getVersionFromMeta(long timeoutTs) {
        SelectdbCloud.GetVersionRequest.Builder builder = SelectdbCloud.GetVersionRequest.newBuilder();
        builder.setDbId(this.dbId).setTableId(this.tableId).setPartitionId(super.getId());
        final SelectdbCloud.GetVersionRequest pRequest = builder.build();

        try {
            Future<SelectdbCloud.GetVersionResponse> future =
                    MetaServiceProxy.getInstance().getVisibleVersionAsync(pRequest);

            SelectdbCloud.GetVersionResponse pResult = null;
            while (pResult == null) {
                long currentTs = System.currentTimeMillis();
                if (currentTs >= timeoutTs) {
                    throw new TimeoutException("query timeout");
                }
                try {
                    pResult = future.get(timeoutTs - currentTs, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    // continue to get result
                    LOG.warn("future get interrupted Exception", e);
                }
            }

            MetaServiceCode code = null;
            if (pResult.hasStatus() && pResult.getStatus().hasCode()) {
                code = pResult.getStatus().getCode();
            }

            if (code == null) {
                throw new RuntimeException("impossible, code must present");
            }

            if (code == SelectdbCloud.MetaServiceCode.OK) {
                // java8 lambda syntax, Assignment function ref.
                AtomicLong version = new AtomicLong(-1);
                Optional.ofNullable(pResult.getVersion()).ifPresent(version::set);
                LOG.debug("get version {}", version);
                return version.get();
            } else if (code == SelectdbCloud.MetaServiceCode.VERSION_NOT_FOUND) {
                LOG.debug("partition {} has no data", getId());
                return 0;
            }

            LOG.warn("get version from meta error, code : {}, msg: {}", code,
                    Optional.ofNullable(pResult.getStatus().getMsg()).orElse(""));
            return -1;
        } catch (RpcException | ExecutionException | TimeoutException | RuntimeException e) {
            LOG.warn("get version from meta service exception: ", e);
            return -1;
        }
    }

    public static CloudPartition read(DataInput in) throws IOException {
        CloudPartition partition = new CloudPartition();
        partition.readFields(in);
        partition.setDbId(in.readLong());
        partition.setTableId(in.readLong());
        return partition;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeLong(this.dbId);
        out.writeLong(this.tableId);
    }

    public boolean equals(Object obj) {
        if (!super.equals(obj)) {
            return false;
        }

        if (!(obj instanceof CloudPartition)) {
            return false;
        }
        CloudPartition cloudPartition = (CloudPartition) obj;
        return (dbId == cloudPartition.dbId) && (tableId == cloudPartition.tableId);
    }

    public String toString() {
        StringBuilder buffer = new StringBuilder();
        buffer.append(super.toString());
        buffer.append("dbId: ").append(this.dbId).append("; ");
        buffer.append("tableId: ").append(this.tableId).append("; ");
        return buffer.toString();
    }
}
