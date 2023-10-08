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

import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.MaxLiteral;
import org.apache.doris.analysis.PartitionDesc;
import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.analysis.SinglePartitionDesc;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.thrift.TTabletType;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/*
 * Repository of a partition's related infos
 */
public class PartitionInfo implements Writable {
    private static final Logger LOG = LogManager.getLogger(PartitionInfo.class);

    protected PartitionType type;
    // partition columns for list and range partitions
    protected List<Column> partitionColumns = Lists.newArrayList();
    // formal partition id -> partition item
    protected Map<Long, PartitionItem> idToItem = Maps.newHashMap();
    // temp partition id -> partition item
    protected Map<Long, PartitionItem> idToTempItem = Maps.newHashMap();
    // partition id -> data property
    protected Map<Long, DataProperty> idToDataProperty;
    // partition id -> storage policy
    protected Map<Long, String> idToStoragePolicy;
    // partition id -> replication allocation
    protected Map<Long, ReplicaAllocation> idToReplicaAllocation;
    // true if the partition has multi partition columns
    protected boolean isMultiColumnPartition = false;

    protected Map<Long, Boolean> idToInMemory;

    protected Map<Long, Boolean> idToPersistent;

    // partition id -> tablet type
    // Note: currently it's only used for testing, it may change/add more meta field later,
    // so we defer adding meta serialization until memory engine feature is more complete.
    protected Map<Long, TTabletType> idToTabletType;

    public PartitionInfo() {
        this.type = PartitionType.UNPARTITIONED;
        this.idToDataProperty = new HashMap<>();
        this.idToReplicaAllocation = new HashMap<>();
        this.idToInMemory = new HashMap<>();
        this.idToTabletType = new HashMap<>();
        this.idToStoragePolicy = new HashMap<>();
        this.idToPersistent = new HashMap<>();
    }

    public PartitionInfo(PartitionType type) {
        this();
        this.type = type;
    }

    public PartitionInfo(PartitionType type, List<Column> partitionColumns) {
        this(type);
        this.partitionColumns = partitionColumns;
        this.isMultiColumnPartition = partitionColumns.size() > 1;
    }

    public PartitionType getType() {
        return type;
    }

    public List<Column> getPartitionColumns() {
        return partitionColumns;
    }

    public Map<Long, PartitionItem> getIdToItem(boolean isTemp) {
        if (isTemp) {
            return idToTempItem;
        } else {
            return idToItem;
        }
    }

    public PartitionItem getItem(long partitionId) {
        PartitionItem item = idToItem.get(partitionId);
        if (item == null) {
            item = idToTempItem.get(partitionId);
        }
        return item;
    }

    public void setItem(long partitionId, boolean isTemp, PartitionItem item) {
        setItemInternal(partitionId, isTemp, item);
    }

    private void setItemInternal(long partitionId, boolean isTemp, PartitionItem item) {
        if (isTemp) {
            idToTempItem.put(partitionId, item);
        } else {
            idToItem.put(partitionId, item);
        }
    }

    public PartitionItem handleNewSinglePartitionDesc(SinglePartitionDesc desc,
                                                      long partitionId, boolean isTemp) throws DdlException {
        Preconditions.checkArgument(desc.isAnalyzed());
        PartitionItem partitionItem = createAndCheckPartitionItem(desc, isTemp);
        setItemInternal(partitionId, isTemp, partitionItem);

        idToDataProperty.put(partitionId, desc.getPartitionDataProperty());
        idToReplicaAllocation.put(partitionId, desc.getReplicaAlloc());
        idToInMemory.put(partitionId, desc.isInMemory());
        idToPersistent.put(partitionId, desc.isPersistent());
        idToStoragePolicy.put(partitionId, desc.getStoragePolicy());

        return partitionItem;
    }

    public PartitionItem createAndCheckPartitionItem(SinglePartitionDesc desc, boolean isTemp) throws DdlException {
        return null;
    }

    public void unprotectHandleNewSinglePartitionDesc(long partitionId, boolean isTemp, PartitionItem partitionItem,
                                                      DataProperty dataProperty, ReplicaAllocation replicaAlloc,
                                                      boolean isInMemory, boolean isPersistent, boolean isMutable) {
        setItemInternal(partitionId, isTemp, partitionItem);
        idToDataProperty.put(partitionId, dataProperty);
        idToReplicaAllocation.put(partitionId, replicaAlloc);
        idToInMemory.put(partitionId, isInMemory);
        idToPersistent.put(partitionId, isPersistent);
        idToStoragePolicy.put(partitionId, "");
        //TODO
        //idToMutable.put(partitionId, isMutable);
    }

    public List<Map.Entry<Long, PartitionItem>> getPartitionItemEntryList(boolean isTemp, boolean isSorted) {
        Map<Long, PartitionItem> tmpMap = idToItem;
        if (isTemp) {
            tmpMap = idToTempItem;
        }
        List<Map.Entry<Long, PartitionItem>> itemEntryList = Lists.newArrayList(tmpMap.entrySet());
        if (isSorted) {
            Collections.sort(itemEntryList, PartitionItem.ITEM_MAP_ENTRY_COMPARATOR);
        }
        return itemEntryList;
    }

    // get sorted item list, exclude partitions which ids are in 'excludePartitionIds'
    public List<PartitionItem> getItemList(Set<Long> excludePartitionIds, boolean isTemp) {
        Map<Long, PartitionItem> tempMap = idToItem;
        if (isTemp) {
            tempMap = idToTempItem;
        }
        List<PartitionItem> resultList = Lists.newArrayList();
        for (Map.Entry<Long, PartitionItem> entry : tempMap.entrySet()) {
            if (!excludePartitionIds.contains(entry.getKey())) {
                resultList.add(entry.getValue());
            }
        }
        return resultList;
    }

    // return any item intersect with the newItem.
    // return null if no item intersect.
    public PartitionItem getAnyIntersectItem(PartitionItem newItem, boolean isTemp) {
        Map<Long, PartitionItem> tmpMap = idToItem;
        if (isTemp) {
            tmpMap = idToTempItem;
        }
        PartitionItem retItem;
        for (PartitionItem item : tmpMap.values()) {
            retItem = item.getIntersect(newItem);
            if (null != retItem) {
                return retItem;
            }
        }
        return null;
    }

    public void checkPartitionItemListsMatch(List<PartitionItem> list1, List<PartitionItem> list2) throws DdlException {
    }

    public void checkPartitionItemListsConflict(List<PartitionItem> list1,
            List<PartitionItem> list2) throws DdlException {
    }

    public DataProperty getDataProperty(long partitionId) {
        return idToDataProperty.get(partitionId);
    }

    public void setDataProperty(long partitionId, DataProperty newDataProperty) {
        idToDataProperty.put(partitionId, newDataProperty);
    }

    public void refreshTableStoragePolicy(String storagePolicy) {
        idToStoragePolicy.replaceAll((k, v) -> storagePolicy);
        idToDataProperty.entrySet().forEach(entry -> {
            entry.getValue().setStoragePolicy(storagePolicy);
        });
    }

    public String getStoragePolicy(long partitionId) {
        return idToStoragePolicy.getOrDefault(partitionId, "");
    }

    public void setStoragePolicy(long partitionId, String storagePolicy) {
        idToStoragePolicy.put(partitionId, storagePolicy);
    }

    public ReplicaAllocation getReplicaAllocation(long partitionId) {
        if (!idToReplicaAllocation.containsKey(partitionId)) {
            LOG.debug("failed to get replica allocation for partition: {}", partitionId);
            return ReplicaAllocation.DEFAULT_ALLOCATION;
        }
        return idToReplicaAllocation.get(partitionId);
    }

    public void setReplicaAllocation(long partitionId, ReplicaAllocation replicaAlloc) {
        this.idToReplicaAllocation.put(partitionId, replicaAlloc);
    }

    public boolean getIsInMemory(long partitionId) {
        return idToInMemory.get(partitionId);
    }

    public boolean getIsMutable(long partitionId) {
        return idToDataProperty.get(partitionId).isMutable();
    }

    public void setIsMutable(long partitionId, boolean isMutable) {
        idToDataProperty.get(partitionId).setMutable(isMutable);
    }

    public void setIsInMemory(long partitionId, boolean isInMemory) {
        idToInMemory.put(partitionId, isInMemory);
    }

    public boolean getIsPersistent(long partitionId) {
        return idToPersistent.get(partitionId);
    }

    public void setIsPersistent(long partitionId, boolean isPersistent) {
        idToPersistent.put(partitionId, isPersistent);
    }

    public TTabletType getTabletType(long partitionId) {
        if (!idToTabletType.containsKey(partitionId)) {
            return TTabletType.TABLET_TYPE_DISK;
        }
        return idToTabletType.get(partitionId);
    }

    public void setTabletType(long partitionId, TTabletType tabletType) {
        idToTabletType.put(partitionId, tabletType);
    }

    public void dropPartition(long partitionId) {
        idToDataProperty.remove(partitionId);
        idToReplicaAllocation.remove(partitionId);
        idToInMemory.remove(partitionId);
        idToItem.remove(partitionId);
        idToTempItem.remove(partitionId);
        idToPersistent.remove(partitionId);
    }

    public void addPartition(long partitionId, boolean isTemp, PartitionItem item, DataProperty dataProperty,
                             ReplicaAllocation replicaAlloc, boolean isInMemory, boolean isPersistent,
                             boolean isMutable) {
        addPartition(partitionId, dataProperty, replicaAlloc, isInMemory, isPersistent, isMutable);
        setItemInternal(partitionId, isTemp, item);
    }

    public void addPartition(long partitionId, DataProperty dataProperty,
                             ReplicaAllocation replicaAlloc,
                             boolean isInMemory, boolean isPersistent, boolean isMutable) {
        idToDataProperty.put(partitionId, dataProperty);
        idToReplicaAllocation.put(partitionId, replicaAlloc);
        idToInMemory.put(partitionId, isInMemory);
        idToPersistent.put(partitionId, isPersistent);
    }

    public static PartitionInfo read(DataInput in) throws IOException {
        PartitionInfo partitionInfo = new PartitionInfo();
        partitionInfo.readFields(in);
        return partitionInfo;
    }

    public boolean isMultiColumnPartition() {
        return isMultiColumnPartition;
    }

    public String toSql(OlapTable table, List<Long> partitionId) {
        return "";
    }

    public PartitionDesc toPartitionDesc(OlapTable olapTable) throws AnalysisException {
        throw new RuntimeException("Should implement it in derived classes.");
    }

    static List<PartitionValue> toPartitionValue(PartitionKey partitionKey) {
        return partitionKey.getKeys().stream().map(expr -> {
            if (expr == MaxLiteral.MAX_VALUE) {
                return PartitionValue.MAX_VALUE;
            } else if (expr instanceof DateLiteral) {
                return new PartitionValue(expr.getStringValue());
            } else {
                return new PartitionValue(expr.getRealValue().toString());
            }
        }).collect(Collectors.toList());
    }

    public void moveFromTempToFormal(long tempPartitionId) {
        PartitionItem item = idToTempItem.remove(tempPartitionId);
        if (item != null) {
            idToItem.put(tempPartitionId, item);
        }
    }

    public void resetPartitionIdForRestore(long newPartitionId, long oldPartitionId,
            ReplicaAllocation restoreReplicaAlloc, boolean isSinglePartitioned) {
        idToDataProperty.put(newPartitionId, idToDataProperty.remove(oldPartitionId));
        idToReplicaAllocation.remove(oldPartitionId);
        idToReplicaAllocation.put(newPartitionId, restoreReplicaAlloc);
        if (!isSinglePartitioned) {
            idToItem.put(newPartitionId, idToItem.remove(oldPartitionId));
        }
        idToInMemory.put(newPartitionId, idToInMemory.remove(oldPartitionId));
        idToPersistent.put(newPartitionId, idToPersistent.remove(oldPartitionId));
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, type.name());

        Preconditions.checkState(idToDataProperty.size() == idToReplicaAllocation.size());
        Preconditions.checkState(idToInMemory.keySet().equals(idToReplicaAllocation.keySet()));
        Preconditions.checkState(idToPersistent.keySet().equals(idToReplicaAllocation.keySet()));
        out.writeInt(idToDataProperty.size());
        for (Map.Entry<Long, DataProperty> entry : idToDataProperty.entrySet()) {
            out.writeLong(entry.getKey());
            if (entry.getValue().equals(DataProperty.DEFAULT_HDD_DATA_PROPERTY)) {
                out.writeBoolean(true);
            } else {
                out.writeBoolean(false);
                entry.getValue().write(out);
            }

            idToReplicaAllocation.get(entry.getKey()).write(out);
            out.writeBoolean(idToInMemory.get(entry.getKey()));
            out.writeBoolean(idToPersistent.get(entry.getKey()));
        }
    }

    public void readFields(DataInput in) throws IOException {
        type = PartitionType.valueOf(Text.readString(in));

        int counter = in.readInt();
        for (int i = 0; i < counter; i++) {
            long partitionId = in.readLong();
            boolean isDefaultHddDataProperty = in.readBoolean();
            if (isDefaultHddDataProperty) {
                idToDataProperty.put(partitionId, new DataProperty(DataProperty.DEFAULT_HDD_DATA_PROPERTY));
            } else {
                idToDataProperty.put(partitionId, DataProperty.read(in));
            }

            if (Env.getCurrentEnvJournalVersion() < FeMetaVersion.VERSION_105) {
                short replicationNum = in.readShort();
                ReplicaAllocation replicaAlloc = new ReplicaAllocation(replicationNum);
                idToReplicaAllocation.put(partitionId, replicaAlloc);
            } else {
                ReplicaAllocation replicaAlloc = ReplicaAllocation.read(in);
                idToReplicaAllocation.put(partitionId, replicaAlloc);
            }

            idToInMemory.put(partitionId, in.readBoolean());
            idToPersistent.put(partitionId, in.readBoolean());
        }
    }

    @Override
    public String toString() {
        StringBuilder buff = new StringBuilder();
        buff.append("type: ").append(type.typeString).append("; ");

        for (Map.Entry<Long, DataProperty> entry : idToDataProperty.entrySet()) {
            buff.append(entry.getKey()).append(" is HDD: ");
            if (entry.getValue().equals(new DataProperty(TStorageMedium.HDD))) {
                buff.append(true);
            } else {
                buff.append(false);
            }
            buff.append("; ");
            buff.append("data_property: ").append(entry.getValue().toString()).append("; ");
            buff.append("replica number: ").append(idToReplicaAllocation.get(entry.getKey())).append("; ");
            buff.append("in memory: ").append(idToInMemory.get(entry.getKey()));
            buff.append("persistent: ").append(idToPersistent.get(entry.getKey()));
            buff.append("is mutable: ").append(idToDataProperty.get(entry.getKey()).isMutable());
        }

        return buff.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PartitionInfo that = (PartitionInfo) o;
        return isMultiColumnPartition == that.isMultiColumnPartition && type == that.type && Objects.equals(
                partitionColumns, that.partitionColumns) && Objects.equals(idToItem, that.idToItem)
                && Objects.equals(idToTempItem, that.idToTempItem) && Objects.equals(idToDataProperty,
                that.idToDataProperty) && Objects.equals(idToStoragePolicy, that.idToStoragePolicy)
                && Objects.equals(idToReplicaAllocation, that.idToReplicaAllocation) && Objects.equals(
                idToInMemory, that.idToInMemory) && Objects.equals(idToTabletType, that.idToTabletType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, partitionColumns, idToItem, idToTempItem, idToDataProperty, idToStoragePolicy,
                idToReplicaAllocation, isMultiColumnPartition, idToInMemory, idToTabletType);
    }
}
