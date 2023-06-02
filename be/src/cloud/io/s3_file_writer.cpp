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

#include "cloud/io/s3_file_writer.h"

#include <aws/core/utils/HashingUtils.h>
#include <aws/core/utils/memory/stl/AWSStreamFwd.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/AbortMultipartUploadRequest.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/CompletedPart.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/UploadPartRequest.h>

#include <atomic>
#include <memory>
#include <utility>

#include "cloud/io/cloud_file_cache.h"
#include "cloud/io/cloud_file_cache_factory.h"
#include "cloud/io/cloud_file_segment.h"
#include "cloud/io/s3_file_system.h"
#include "common/logging.h"
#include "common/status.h"

namespace doris {
namespace io {
using namespace Aws::S3::Model;
using Aws::S3::S3Client;
using namespace ErrorCode;

bvar::Adder<uint64_t> s3_file_writer("s3_file_writer", "total_num");
bvar::Adder<uint64_t> s3_bytes_written("s3_file_writer", "bytes_written");
bvar::Adder<uint64_t> s3_file_created("s3_file_writer", "file_created");
bvar::Adder<uint64_t> s3_file_being_written("s3_file_writer", "file_being_written");

S3FileWriter::S3FileWriter(Path path, std::string key, std::string bucket,
                           std::shared_ptr<S3Client> client, std::shared_ptr<S3FileSystem> fs,
                           IOState* state, bool sse_enabled)
        : _path(std::move(path)),
          _fs(std::move(fs)),
          _bucket(std::move(bucket)),
          _key(std::move(key)),
          _sse_enabled(sse_enabled),
          _client(std::move(client)),
          _expiration_time(state ? state->expiration_time : 0),
          _is_cold_data(state ? state->is_cold_data : true),
          _disable_file_cache(state == nullptr ? false : state->disable_file_cache) {
    s3_file_writer << 1;
    s3_file_being_written << 1;
}

S3FileWriter::~S3FileWriter() {
    if (!_closed) {
        abort();
    }
    s3_file_being_written << -1;
}

Status S3FileWriter::open() {
    VLOG_DEBUG << "S3FileWriter::open, path: " << _path.native();
    if (config::enable_file_cache && !_disable_file_cache) {
        _cache_key = CloudFileCache::hash(_path.filename().native());
        _cache = FileCacheFactory::instance().get_by_path(_cache_key);
    }
    CreateMultipartUploadRequest create_request;
    create_request.WithBucket(_bucket).WithKey(_key);
    create_request.SetContentType("text/plain");
    if (_sse_enabled) {
        create_request.WithServerSideEncryption(Aws::S3::Model::ServerSideEncryption::AES256);
    }

    auto outcome = _client->CreateMultipartUpload(create_request);

    if (outcome.IsSuccess()) {
        _upload_id = outcome.GetResult().GetUploadId();
        _closed = false;
        _opened = true;
        return Status::OK();
    }
    return Status::IOError("failed to create multipart upload(bucket={}, key={}, upload_id={}): {}",
                           _bucket, _path.native(), _upload_id, outcome.GetError().GetMessage());
}

Status S3FileWriter::abort() {
    _failed = true;
    if (_closed || !_opened) {
        return Status::OK();
    }
    VLOG_DEBUG << "S3FileWriter::abort, path: " << _path.native();
    _closed = true;
    _wait.wait();
    AbortMultipartUploadRequest request;
    request.WithBucket(_bucket).WithKey(_key).WithUploadId(_upload_id);
    auto outcome = _client->AbortMultipartUpload(request);
    if (outcome.IsSuccess() ||
        outcome.GetError().GetErrorType() == Aws::S3::S3Errors::NO_SUCH_UPLOAD ||
        outcome.GetError().GetResponseCode() == Aws::Http::HttpResponseCode::NOT_FOUND) {
        LOG(INFO) << "Abort multipart upload successfully"
                  << "bucket=" << _bucket << ", key=" << _path.native()
                  << ", upload_id=" << _upload_id;
        return Status::OK();
    }
    return Status::IOError("failed to abort multipart upload(bucket={}, key={}, upload_id={}): {}",
                           _bucket, _path.native(), _upload_id, outcome.GetError().GetMessage());
}

Status S3FileWriter::close(bool /*sync*/) {
    if (_closed) {
        return Status::OK();
    }
    VLOG_DEBUG << "S3FileWriter::close, path: " << _path.native();
    _closed = true;
    RETURN_IF_ERROR(_complete());

    return Status::OK();
}

Status S3FileWriter::append(const Slice& data) {
    DCHECK(!_closed);
    return appendv(&data, 1);
}

Status S3FileWriter::appendv(const Slice* data, size_t data_cnt) {
    DCHECK(!_closed);
    size_t buffer_size = config::s3_write_buffer_size;
    for (size_t i = 0; i < data_cnt; i++) {
        size_t data_size = data[i].get_size();
        for (size_t pos = 0, data_size_to_append = 0; pos < data_size; pos += data_size_to_append) {
            if (!_pending_buf) {
                auto builder = FileBufferBuilder();
                builder.set_type(BufferType::UPLOAD)
                        .set_upload_callback(
                                [part_num = _cur_part_num, this](UploadFileBuffer& buf) {
                                    _upload_one_part(part_num, buf);
                                })
                        .set_file_offset(_bytes_appended)
                        .set_index_offset(_index_offset)
                        .set_sync_after_complete_task([this, part_num = _cur_part_num](Status s) {
                            if (!s.ok()) [[unlikely]] {
                                VLOG_NOTICE << "failed at key: " << _key << ", load part "
                                            << part_num << ", st " << s;
                                std::unique_lock<std::mutex> _lck {_completed_lock};
                                _failed = true;
                                this->_st = std::move(s);
                            }
                            _wait.done();
                        })
                        .set_is_done([this]() { return _failed.load(); });
                if (!_disable_file_cache) {
                    builder.set_allocate_file_segments_holder(
                            [this, offset = _bytes_appended]() -> FileSegmentsHolderPtr {
                                return _allocate_file_segments(offset);
                            });
                }
                _pending_buf = builder.build();
            }
            // we need to make sure all parts except the last one to be 5MB or more
            // and shouldn't be larger than buf
            data_size_to_append = std::min(data_size - pos, _pending_buf->get_file_offset() +
                                                                    buffer_size - _bytes_appended);

            // if the buffer has memory buf inside, the data would be written into memory first then S3 then file cache
            // it would be written to cache then S3 if the buffer doesn't have memory preserved
            _pending_buf->append_data(Slice {data[i].get_data() + pos, data_size_to_append});

            // if it's the last part, it could be less than 5MB, or it must
            // satisfy that the size is larger than or euqal to 5MB
            // _complete() would handle the first situation
            if (_pending_buf->get_size() == buffer_size) {
                _cur_part_num++;
                _wait.add();
                _pending_buf->submit();
                _pending_buf = nullptr;
            }
            _bytes_appended += data_size_to_append;
        }
    }
    return Status::OK();
}

void S3FileWriter::_upload_one_part(int64_t part_num, UploadFileBuffer& buf) {
    if (buf.is_done()) {
        return;
    }
    UploadPartRequest upload_request;
    upload_request.WithBucket(_bucket).WithKey(_key).WithPartNumber(part_num).WithUploadId(
            _upload_id);

    auto _stream_ptr = buf.get_stream();

    upload_request.SetBody(buf.get_stream());

    Aws::Utils::ByteBuffer part_md5(Aws::Utils::HashingUtils::CalculateMD5(*_stream_ptr));
    upload_request.SetContentMD5(Aws::Utils::HashingUtils::Base64Encode(part_md5));

    upload_request.SetContentLength(buf.get_size());

    auto upload_part_callable = _client->UploadPartCallable(upload_request);

    UploadPartOutcome upload_part_outcome = upload_part_callable.get();
    if (!upload_part_outcome.IsSuccess()) {
        auto s = Status::IOError(
                "failed to upload part (bucket={}, key={}, part_num={}, up_load_id={}): {}",
                _bucket, _path.native(), part_num, _upload_id,
                upload_part_outcome.GetError().GetMessage());
        LOG(WARNING) << s;
        buf.set_val(s);
        return;
    }

    s3_bytes_written << buf.get_size();

    std::shared_ptr<CompletedPart> completed_part = std::make_shared<CompletedPart>();
    completed_part->SetPartNumber(part_num);
    auto etag = upload_part_outcome.GetResult().GetETag();
    // DCHECK(etag.empty());
    completed_part->SetETag(etag);

    std::unique_lock<std::mutex> lck {_completed_lock};
    _completed_parts.emplace_back(completed_part);
    _bytes_written += buf.get_size();
}

FileSegmentsHolderPtr S3FileWriter::_allocate_file_segments(size_t offset) {
    CacheContext ctx;
    ctx.cache_type = _expiration_time == 0 ? CacheType::NORMAL : CacheType::TTL;
    ctx.expiration_time = _expiration_time;
    ctx.is_cold_data = _is_cold_data;
    auto holder = _cache->get_or_set(_cache_key, offset, config::s3_write_buffer_size, ctx);
    return std::make_unique<FileSegmentsHolder>(std::move(holder));
}

Status S3FileWriter::_complete() {
    if (_failed) {
        return _st;
    }
    CompleteMultipartUploadRequest complete_request;
    complete_request.WithBucket(_bucket).WithKey(_key).WithUploadId(_upload_id);

    while (!_wait.wait()) {
        LOG_WARNING("The upload {} {} {} takes a long time", _bucket, _key, _path.native());
    }
    // make sure _completed_parts are ascending order
    std::sort(_completed_parts.begin(), _completed_parts.end(),
              [](auto& p1, auto& p2) { return p1->GetPartNumber() < p2->GetPartNumber(); });
    CompletedMultipartUpload completed_upload;
    for (std::shared_ptr<CompletedPart> part : _completed_parts) {
        completed_upload.AddParts(*part);
    }

    complete_request.WithMultipartUpload(completed_upload);

    auto compute_outcome = _client->CompleteMultipartUpload(complete_request);

    if (!compute_outcome.IsSuccess()) {
        auto s = Status::IOError("failed to create multi part upload (bucket={}, key={}): {}",
                                 _bucket, _path.native(), compute_outcome.GetError().GetMessage());
        LOG(WARNING) << s;
        return s;
    }
    s3_file_created << 1;
    return Status::OK();
}

Status S3FileWriter::write_at(size_t /*offset*/, const Slice& /*data*/) {
    DCHECK(false) << "should never come here";
    return Status::NotSupported("not support");
}

Status S3FileWriter::finalize() {
    DCHECK(!_closed);
    // submit pending buf if it's not nullptr
    // it's the last buf, we can submit it right now
    if (_pending_buf != nullptr) {
        _wait.add();
        _pending_buf->submit();
        _pending_buf = nullptr;
    }
    return Status::OK();
}

} // namespace io
} // namespace doris
