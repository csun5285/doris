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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Interpreters/Cache/FileCacheFactory.h
// and modified by Doris

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "io/cache/block/block_file_cache.h"
#include "io/cache/block/block_file_cache_settings.h"
namespace doris {
class TUniqueId;

namespace io {

/**
 * Creates a FileCache object for cache_base_path.
 */
class FileCacheFactory {
public:
    static FileCacheFactory& instance();

    Status create_file_cache(const std::string& cache_base_path,
                             FileCacheSettings file_cache_settings);

    size_t get_total_cache_size() const { return _total_cache_size; }

    size_t get_cache_instance_size() const { return _caches.size(); }

    BlockFileCachePtr get_by_path(const Key& key);
    BlockFileCachePtr get_by_path(const std::string& cache_base_path);
    std::vector<BlockFileCache::QueryFileCacheContextHolderPtr> get_query_context_holders(
            const TUniqueId& query_id);

    /**
     * Clears data of all file cache instances
     *
     * @param sync wait until all data cleared
     * @return summary message
     */
    std::string clear_file_caches(bool sync);

    std::vector<std::string> get_base_paths();
    FileCacheFactory() = default;
    FileCacheFactory& operator=(const FileCacheFactory&) = delete;
    FileCacheFactory(const FileCacheFactory&) = delete;

private:
    std::vector<std::unique_ptr<BlockFileCache>> _caches;
    std::unordered_map<std::string, BlockFileCachePtr> _path_to_cache;
    size_t _total_cache_size = 0;
};

} // namespace io
} // namespace doris
