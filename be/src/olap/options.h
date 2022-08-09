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

#include <string>
#include <vector>

#include "io/cache/file_cache_settings.h"
#include "olap/olap_define.h"
#include "util/uid_util.h"
namespace doris {

struct StorePath {
    StorePath() : capacity_bytes(-1), storage_medium(TStorageMedium::HDD) {}
    StorePath(const std::string& path_, int64_t capacity_bytes_)
            : path(path_), capacity_bytes(capacity_bytes_), storage_medium(TStorageMedium::HDD) {}
    StorePath(const std::string& path_, int64_t capacity_bytes_,
              TStorageMedium::type storage_medium_)
            : path(path_), capacity_bytes(capacity_bytes_), storage_medium(storage_medium_) {}
    std::string path;
    int64_t capacity_bytes;
    TStorageMedium::type storage_medium;
};

// parse a single root path of storage_root_path
Status parse_root_path(const std::string& root_path, StorePath* path);

Status parse_conf_store_paths(const std::string& config_path, std::vector<StorePath>* path);

inline constexpr size_t GB = 1 * 1024 * 1024 * 1024;
inline constexpr size_t KB = 1024;
struct CachePath {
    io::FileCacheSettings init_settings() const;
    std::string path;
    int64_t capacity_bytes = 0;
};
Status parse_root_path(const std::string& root_path, CachePath* path);
Status parse_conf_cache_paths(const std::string& config_path, std::vector<CachePath>& path);

struct EngineOptions {
    // list paths that tablet will be put into.
    std::vector<StorePath> store_paths;
    // BE's UUID. It will be reset every time BE restarts.
    UniqueId backend_uid {0, 0};
};
} // namespace doris
