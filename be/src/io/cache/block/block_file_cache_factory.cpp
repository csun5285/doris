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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Interpreters/Cache/FileCacheFactory.cpp
// and modified by Doris

#include "io/cache/block/block_file_cache_factory.h"

#include <glog/logging.h>
#include <sys/statfs.h>

#include <algorithm>
#include <ostream>
#include <utility>

#include "common/config.h"
#include "io/cache/block/block_file_cache.h"
#include "io/cache/block/block_file_cache_settings.h"
#include "io/fs/err_utils.h"
#include "io/fs/local_file_system.h"
#include "vec/common/hash_table/hash_table.h"

namespace doris {
class TUniqueId;

namespace io {

FileCacheFactory& FileCacheFactory::instance() {
    static FileCacheFactory ret;
    return ret;
}

Status FileCacheFactory::create_file_cache(const std::string& cache_base_path,
                                           FileCacheSettings file_cache_settings) {
    auto fs = global_local_filesystem();
    bool exists = false;
    RETURN_IF_ERROR(fs->exists(cache_base_path, &exists));
    if (!exists) {
        RETURN_IF_ERROR(fs->create_directory(cache_base_path));
    } else if (config::clear_file_cache) {
        LOG(INFO) << "[FileCache] clear file cache path: " << cache_base_path;
        RETURN_IF_ERROR(fs->delete_directory(cache_base_path));
        RETURN_IF_ERROR(fs->create_directory(cache_base_path));
    }

    struct statfs stat;
    if (statfs(cache_base_path.c_str(), &stat) < 0) {
        return localfs_error(errno, fmt::format("{} statfs error", cache_base_path));
    }
    size_t disk_total_size =
            static_cast<size_t>(stat.f_blocks) * static_cast<size_t>(stat.f_bsize) *
            (static_cast<double>(config::file_cache_enter_disk_resource_limit_mode_percent) / 100);
    if (disk_total_size < file_cache_settings.total_size) {
        LOG_INFO("The cache {} config size {} is larger than {}% disk size {}, recalc it.",
                 cache_base_path, file_cache_settings.total_size,
                 config::file_cache_enter_disk_resource_limit_mode_percent, disk_total_size);
        file_cache_settings =
                calc_settings(disk_total_size, file_cache_settings.max_query_cache_size);
    }
    auto cache = std::make_unique<BlockFileCache>(cache_base_path, file_cache_settings);
    RETURN_IF_ERROR(cache->initialize());
    _path_to_cache.emplace(cache_base_path, cache.get());
    _caches.push_back(std::move(cache));

    LOG(INFO) << "[FileCache] path: " << cache_base_path
              << " total_size: " << file_cache_settings.total_size;
    _total_cache_size += file_cache_settings.total_size;
    return Status::OK();
}

BlockFileCachePtr FileCacheFactory::get_by_path(const Key& key) {
    return _caches[KeyHash()(key) % _caches.size()].get();
}

BlockFileCachePtr FileCacheFactory::get_by_path(const std::string& cache_base_path) {
    auto iter = _path_to_cache.find(cache_base_path);
    if (iter == _path_to_cache.end()) {
        return nullptr;
    } else {
        return iter->second;
    }
}

std::vector<std::string> FileCacheFactory::get_base_paths() {
    std::vector<std::string> paths;
    for (const auto& pair : _path_to_cache) {
        paths.push_back(pair.first);
    }
    return paths;
}

std::vector<BlockFileCache::QueryFileCacheContextHolderPtr>
FileCacheFactory::get_query_context_holders(const TUniqueId& query_id) {
    std::vector<BlockFileCache::QueryFileCacheContextHolderPtr> holders;
    for (const auto& cache : _caches) {
        holders.push_back(cache->get_query_context_holder(query_id));
    }
    return holders;
}

std::string FileCacheFactory::clear_file_caches(bool sync) {
    std::stringstream ss;
    for (const auto& cache : _caches) {
        ss << (sync ? cache->clear_file_cache_directly() : cache->clear_file_cache_async()) << "\n";
    }
    return ss.str();
}

} // namespace io
} // namespace doris
