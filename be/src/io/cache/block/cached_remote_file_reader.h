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

#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <string>
#include <utility>

#include "common/status.h"
#include "io/cache/block/block_file_cache.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_reader_options.h"
#include "io/fs/file_system.h"
#include "io/fs/path.h"
#include "util/slice.h"

namespace doris {
namespace io {
class IOContext;
struct FileCacheStatistics;

class CachedRemoteFileReader final : public FileReader {
public:
    CachedRemoteFileReader(FileReaderSPtr remote_file_reader,
                           const FileReaderOptions* reader_options);

    ~CachedRemoteFileReader() override;

    Status close() override;

    const Path& path() const override { return _remote_file_reader->path(); }

    size_t size() const override { return _remote_file_reader->size(); }

    bool closed() const override { return _remote_file_reader->closed(); }

    FileSystemSPtr fs() const override { return _remote_file_reader->fs(); }

    FileReader* get_remote_reader() { return _remote_file_reader.get(); }

    static std::pair<size_t, size_t> s_align_size(size_t offset, size_t read_size, size_t length);

protected:
    [[nodiscard]] Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                                      const IOContext* io_ctx) override;

private:
    FileReaderSPtr _remote_file_reader;
    Key _cache_key;
    BlockFileCachePtr _cache;
    MetricsHook _metrics_hook;
    bool _is_doris_table;

    void _update_state(const ReadStatistics& stats, FileCacheStatistics* state) const;

    Status _read_from_cache(size_t offset, Slice result, size_t* bytes_read,
                            const IOContext* io_ctx);
};

} // namespace io
} // namespace doris