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

#include "olap/tablet_schema_cache.h"

#include <gen_cpp/olap_file.pb.h>

#include "common/sync_point.h"

namespace doris {

void TabletSchemaCache::create_global_schema_cache() {
    DCHECK(_s_instance == nullptr);
    static TabletSchemaCache instance;
    _s_instance = &instance;
    std::thread t(&TabletSchemaCache::_recycle, _s_instance);
    t.detach();
}

TabletSchemaSPtr TabletSchemaCache::insert(int64_t index_id, const TabletSchemaPB& schema) {
    TEST_SYNC_POINT_RETURN_WITH_VALUE("TabletSchemaCache::insert1",
                                      std::make_shared<TabletSchema>());
    DCHECK(_s_instance != nullptr);
    DCHECK(index_id > 0);
    std::lock_guard guard(_mtx);
    auto iter = _cache.find({index_id, schema.schema_version()});
    if (iter == _cache.end()) {
        TabletSchemaSPtr tablet_schema_ptr = std::make_shared<TabletSchema>();
        tablet_schema_ptr->init_from_pb(schema);
        _cache.insert({{index_id, schema.schema_version()}, tablet_schema_ptr});
        return tablet_schema_ptr;
    }
    return iter->second;
}

TabletSchemaSPtr TabletSchemaCache::insert(int64_t index_id, const TabletSchemaSPtr& schema) {
    TEST_SYNC_POINT_RETURN_WITH_VALUE("TabletSchemaCache::insert2",
                                      std::make_shared<TabletSchema>());
    DCHECK(_s_instance != nullptr);
    DCHECK(index_id > 0);
    std::lock_guard guard(_mtx);
    auto [it, _] = _cache.insert({{index_id, schema->schema_version()}, schema});
    return it->second;
}

/**
 * @brief recycle when TabletSchemaSPtr use_count equals 1.
 */
void TabletSchemaCache::_recycle() {
    int64_t tablet_schema_cache_recycle_interval = 86400; // s, one day
    for (;;) {
        std::this_thread::sleep_for(std::chrono::seconds(tablet_schema_cache_recycle_interval));
        std::lock_guard guard(_mtx);
        for (auto iter = _cache.begin(), last = _cache.end(); iter != last;) {
            if (iter->second.unique()) {
                iter = _cache.erase(iter);
            } else {
                ++iter;
            }
        }
    }
}

} // namespace doris
