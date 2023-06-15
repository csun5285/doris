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

#pragma once

#include "olap/olap_common.h"
#include "olap/lru_cache.h"
#include "olap/rowset/rowset.h"
#include "olap/tablet_meta.h"
#include "util/time.h"

namespace doris {

class DeleteBitmapTxnManager {
public:
    DeleteBitmapTxnManager(size_t size_in_bytes)
            : _cache("DeleteBitmap TxnCache", size_in_bytes, LRUCacheType::SIZE, 16) {}

    Status get_tablet_txn_info(TTransactionId transaction_id, int64_t tablet_id,
                               RowsetSharedPtr* rowset, DeleteBitmapPtr* delete_bitmap,
                               RowsetIdUnorderedSet* rowset_ids) {
        std::string key_str = std::to_string(transaction_id) + "/" +
                              std::to_string(tablet_id); // Cache key container
        CacheKey key(key_str);
        Cache::Handle* handle = _cache.lookup(key);

        TabletTxnInfo* val = handle == nullptr
                                     ? nullptr
                                     : reinterpret_cast<TabletTxnInfo*>(_cache.value(handle));
        if (val) {
            *rowset = val->rowset;
            *delete_bitmap = val->delete_bitmap;
            *rowset_ids = val->rowset_ids;
            return Status::OK();
        }
        // TODO(liaoxin) read tmp rowset from fdb
        return Status::NotFound("not found txn info, tablet_id={}, transaction_id={}", tablet_id,
                                transaction_id);
    }

    void set_txn_related_delete_bitmap(TTransactionId transaction_id, int64_t tablet_id,
                                       DeleteBitmapPtr delete_bitmap,
                                       const RowsetIdUnorderedSet& rowset_ids,
                                       RowsetSharedPtr rowset) {
        std::string key_str = std::to_string(transaction_id) + "/" +
                              std::to_string(tablet_id); // Cache key container
        CacheKey key(key_str);
        Cache::Handle* handle = _cache.lookup(key);

        TabletTxnInfo* val = handle == nullptr
                                     ? nullptr
                                     : reinterpret_cast<TabletTxnInfo*>(_cache.value(handle));
        if (val == nullptr) { // Renew if needed, put a new Value to cache
            val = new TabletTxnInfo(rowset, delete_bitmap, rowset_ids);
            static auto deleter = [](const CacheKey&, void* value) {
                delete (Value*)value; // Just delete to reclaim
            };
            size_t charge = sizeof(TabletTxnInfo);
            for (auto& [k, v] : val->delete_bitmap->delete_bitmap) {
                charge += v.getSizeInBytes();
            }
            handle = _cache.insert(key, val, charge, deleter, CachePriority::NORMAL);
        }
    }

private:
    struct TabletTxnInfo {
        RowsetSharedPtr rowset;
        DeleteBitmapPtr delete_bitmap;
        // records rowsets calc in commit txn
        RowsetIdUnorderedSet rowset_ids;
        int64_t creation_time;

        TabletTxnInfo(RowsetSharedPtr rowset) : rowset(rowset), creation_time(UnixSeconds()) {}

        TabletTxnInfo(RowsetSharedPtr rowset, DeleteBitmapPtr delete_bitmap,
                      const RowsetIdUnorderedSet& ids)
                : rowset(rowset),
                  delete_bitmap(delete_bitmap),
                  rowset_ids(ids),
                  creation_time(UnixSeconds()) {}

        TabletTxnInfo() = default;
    };

    ShardedLRUCache _cache;
};

} // namespace doris
