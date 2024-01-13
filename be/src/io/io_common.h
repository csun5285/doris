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

#include <gen_cpp/Types_types.h>

namespace doris {

enum class ReaderType {
    READER_QUERY = 0,
    READER_ALTER_TABLE = 1,
    READER_BASE_COMPACTION = 2,
    READER_CUMULATIVE_COMPACTION = 3,
    READER_CHECKSUM = 4,
    READER_COLD_DATA_COMPACTION = 5,
    READER_SEGMENT_COMPACTION = 6,
    READER_FULL_COMPACTION = 7,
    UNKNOWN = 8
};

namespace io {

struct ReadStatistics {
    bool hit_cache = true;
    bool skip_cache = false;
    int64_t bytes_read = 0;
    int64_t bytes_write_into_file_cache = 0;
    int64_t remote_read_timer = 0;
    int64_t local_read_timer = 0;
    int64_t local_write_timer = 0;
};

struct FileCacheStatistics {
    int64_t num_local_io_total = 0;
    int64_t num_remote_io_total = 0;
    int64_t local_io_timer = 0;
    int64_t bytes_read_from_local = 0;
    int64_t bytes_read_from_remote = 0;
    int64_t remote_io_timer = 0;
    int64_t write_cache_io_timer = 0;
    int64_t bytes_write_into_cache = 0;
    int64_t num_skip_cache_io_total = 0;
};

struct AsyncIOStatistics {
    int64_t remote_total_use_timer_ns = 0;
    int64_t remote_task_wait_worker_timer_ns = 0;
    int64_t remote_task_wake_up_timer_ns = 0;
    int64_t remote_task_exec_timer_ns = 0;
    int64_t remote_task_total = 0;
    int64_t remote_wait_for_putting_queue = 0;

    int64_t local_total_use_timer_ns = 0;
    int64_t local_task_wait_worker_timer_ns = 0;
    int64_t local_task_wake_up_timer_ns = 0;
    int64_t local_task_exec_timer_ns = 0;
    int64_t local_task_total = 0;
    int64_t local_wait_for_putting_queue = 0;
};

class IOContext {
public:
    IOContext() = default;

<<<<<<< HEAD
=======
    IOContext(const TUniqueId* query_id, FileCacheStatistics* stats, bool use_disposable_cache,
              bool read_segment_index)
            : query_id(query_id),
              is_disposable(use_disposable_cache),
              read_segment_index(read_segment_index),
              should_stop(false),
              file_cache_stats(stats) {}
>>>>>>> selectdb-doris-2.0.4-b01
    ReaderType reader_type = ReaderType::UNKNOWN;
    const TUniqueId* query_id = nullptr;
    bool is_disposable = false;
    bool read_segment_index = false;
<<<<<<< HEAD
    uint64_t expiration_time = 0;
    bool disable_file_cache = false;
    FileCacheStatistics* file_cache_stats = nullptr; // Ref
    AsyncIOStatistics* async_io_stats = nullptr;     // Ref
=======
    // stop reader when reading, used in some interrupted operations
    bool should_stop = false;
    FileCacheStatistics* file_cache_stats = nullptr;
>>>>>>> selectdb-doris-2.0.4-b01
};

} // namespace io
} // namespace doris
