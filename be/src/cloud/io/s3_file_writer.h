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

#include <atomic>
#include <memory>

#include "cloud/io/cloud_file_cache.h"
#include "cloud/io/file_reader.h"
#include "cloud/io/file_writer.h"
#include "cloud/io/s3_file_system.h"
#include "cloud/io/s3_file_write_bufferpool.h"
#include "util/wait_group.h"

namespace Aws::Transfer {
class TransferHandle;
}
namespace Aws::S3 {
namespace Model {
class CompletedPart;
}
class S3Client;
} // namespace Aws::S3

namespace doris {
namespace io {

class S3FileSystem;
class TmpFileMgr;
class S3FileWriter final : public FileWriter {
public:
    S3FileWriter(Path path, std::string key, std::string bucket, std::shared_ptr<Aws::S3::S3Client>,
                 std::shared_ptr<S3FileSystem> fs, IOState* state);
    ~S3FileWriter() override;

    Status open() override;

    Status close(bool sync = true) override;

    Status abort() override;

    Status append(const Slice& data) override;

    Status appendv(const Slice* data, size_t data_cnt) override;

    Status write_at(size_t offset, const Slice& data) override;

    Status finalize() override;

    size_t bytes_appended() const override { return _bytes_appended; }

    FileSystemSPtr fs() const override { return _fs; }
    int64_t upload_cost_ms() const { return *_upload_cost_ms; }

    const Path& path() const override { return _path; }

    void mark_index_offset() {
        if (_expiration_time == 0) {
            _index_offset = _bytes_appended;
            // Only the normal data need to change to index data
            if (_pending_buf) {
                _pending_buf->set_index_offset(_index_offset);
            }
        }
    }

private:
    Status _complete();
    // void _upload_to_cache(const Slice& data, S3FileBuffer& buf);
    void _upload_one_part(int64_t part_num, S3FileBuffer& buf);

    FileSegmentsHolderPtr _allocate_file_segments(size_t offset);

    Path _path;
    std::shared_ptr<S3FileSystem> _fs;

    std::string _bucket;
    std::string _key;
    bool _closed = true;
    bool _opened = false;

    std::shared_ptr<int64_t> _upload_cost_ms;

    std::shared_ptr<Aws::S3::S3Client> _client;
    std::string _upload_id;
    size_t _bytes_appended {0};
    size_t _index_offset {0};
    // size_t _index_offset {0};
    // bool _set_index_idx {false};

    // Current Part Num for CompletedPart
    int _cur_part_num = 1;
    std::mutex _completed_lock;
    std::vector<std::shared_ptr<Aws::S3::Model::CompletedPart>> _completed_parts;

    IFileCache::Key _cache_key;
    CloudFileCachePtr _cache;

    WaitGroup _wait;

    std::atomic_bool _failed = false;
    Status _st = Status::OK();
    size_t _bytes_written = 0;

    std::shared_ptr<S3FileBuffer> _pending_buf = nullptr;
    int64_t _expiration_time;
    bool _is_cold_data;
};

} // namespace io
} // namespace doris
