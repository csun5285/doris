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

#include "cloud/io/cloud_file_cache.h"
#include "cloud/io/file_reader.h"
#include "cloud/io/path.h"
#include "cloud/io/file_system.h"

namespace doris {
namespace io {

class CachedRemoteFileReader final : public FileReader {
public:
    CachedRemoteFileReader(FileReaderSPtr remote_file_reader, metrics_hook);

    ~CachedRemoteFileReader() override;

    Status close() override;

    Status read_at(size_t offset, Slice result, size_t* bytes_read,
                   IOState* state = nullptr) override;

    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read, IOState* state);

    const Path& path() const override { return _remote_file_reader->path(); }

    size_t size() const override { return _remote_file_reader->size(); }

    bool closed() const override { return _remote_file_reader->closed(); }

    FileSystemSPtr fs() const override { return _remote_file_reader->fs(); }

private:
    std::pair<size_t, size_t> _align_size(size_t offset, size_t size) const;

    FileReaderSPtr _remote_file_reader;
    IFileCache::Key _cache_key;
    CloudFileCachePtr _cache;

private:
    struct ReadStatistics {
        bool hit_cache = true;
        bool skip_cache = false;
        int64_t bytes_read = 0;
        int64_t bytes_write_into_file_cache = 0;
        int64_t remote_read_timer = 0;
        int64_t local_read_timer = 0;
        int64_t local_write_timer = 0;
    };
    void _update_state(const ReadStatistics& stats, IOState* state) const;
    metrics_hook _metrics;
};

} // namespace io
} // namespace doris
