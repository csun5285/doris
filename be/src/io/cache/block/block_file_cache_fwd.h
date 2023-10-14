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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Interpreters/Cache/FileCache_fwd.h
// and modified by Doris

#pragma once
#include <memory>

#include "vec/common/uint128.h"

static constexpr size_t GB = 1 * 1024 * 1024 * 1024;
static constexpr size_t KB = 1024;
namespace doris {
namespace io {

using uint128_t = vectorized::UInt128;
using UInt128Hash = vectorized::UInt128Hash;
static constexpr size_t REMOTE_FS_OBJECTS_CACHE_DEFAULT_ELEMENTS = 100 * 1024;
static constexpr size_t FILE_CACHE_MAX_FILE_BLOCK_SIZE = 1 * 1024 * 1024;

struct FileCacheSettings;
// default 1 : 17 : 2
enum FileCacheType {
    INDEX,
    NORMAL,
    DISPOSABLE,
    TTL,
};

struct Key {
    uint128_t key;
    std::string to_string() const;

    Key() = default;
    explicit Key(const uint128_t& key_) : key(key_) {}

    bool operator==(const Key& other) const { return key == other.key; }
};

} // namespace io
} // namespace doris
