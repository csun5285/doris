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

#include "io/fs/s3_file_writer.h"

#include <aws/core/http/HttpResponse.h>
#include <aws/core/http/URI.h>
#include <aws/core/utils/Array.h>
#include <aws/core/utils/HashingUtils.h>
#include <aws/core/utils/Outcome.h>
#include <aws/core/utils/memory/stl/AWSAllocator.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/S3Errors.h>
#include <aws/s3/model/AbortMultipartUploadRequest.h>
#include <aws/s3/model/AbortMultipartUploadResult.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/CompleteMultipartUploadResult.h>
#include <aws/s3/model/CompletedMultipartUpload.h>
#include <aws/s3/model/CompletedPart.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/CreateMultipartUploadResult.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/UploadPartRequest.h>
#include <aws/s3/model/UploadPartResult.h>
#include <fmt/core.h>
#include <glog/logging.h>

#include <sstream>
#include <string_view>
#include <utility>

#include "common/config.h"
#include "common/status.h"
#include "common/sync_point.h"
#include "io/cache/block/block_file_cache.h"
#include "io/cache/block/block_file_cache_factory.h"
#include "io/fs/file_writer.h"
#include "io/fs/path.h"
#include "io/fs/s3_file_bufferpool.h"
#include "io/fs/s3_file_system.h"
#include "util/defer_op.h"
#include "util/doris_metrics.h"
#include "util/runtime_profile.h"
#include "util/s3_util.h"

namespace Aws {
namespace S3 {
namespace Model {
class DeleteObjectRequest;
} // namespace Model
} // namespace S3
} // namespace Aws

using Aws::S3::Model::AbortMultipartUploadRequest;
using Aws::S3::Model::CompletedPart;
using Aws::S3::Model::CompletedMultipartUpload;
using Aws::S3::Model::CompleteMultipartUploadRequest;
using Aws::S3::Model::CreateMultipartUploadRequest;
using Aws::S3::Model::UploadPartRequest;
using Aws::S3::Model::UploadPartOutcome;

namespace doris {
namespace io {
using namespace Aws::S3::Model;

// Whole s3 file writer ever created by the process
bvar::Adder<uint64_t> s3_file_writer_total("s3_file_writer_total_num");
// Whole bytes persistent on S3
bvar::Adder<uint64_t> s3_bytes_written_total("s3_file_writer_bytes_written");
// Whole files created on S3
bvar::Adder<uint64_t> s3_file_created_total("s3_file_writer_file_created");
// S3 file writer currently exists in process
bvar::Adder<uint64_t> s3_file_being_written("s3_file_writer_file_being_written");
// Whole bytes sent to S3(doesn't mean it would be persistent)
bvar::Adder<uint64_t> s3_bytes_uploaded_total("s3_bytes_uploaded_total");

S3FileWriter::S3FileWriter(std::string key, std::shared_ptr<S3FileSystem> fs,
                           const FileWriterOptions* opts)
        : FileWriter(fmt::format("s3://{}/{}", fs->s3_conf().bucket, key), fs),
          _bucket(fs->s3_conf().bucket),
          _key(std::move(key)),
          _sse_enabled(fs->s3_conf().sse_enabled),
          _client(fs->get_client()),
          _expiration_time(opts ? opts->file_cache_expiration : 0),
          _is_cold_data(opts ? opts->is_cold_data : true),
          _write_file_cache(opts ? opts->write_file_cache : true) {
    s3_file_writer_total << 1;
    s3_file_being_written << 1;

    Aws::Http::SetCompliantRfc3986Encoding(true);

    if (config::enable_file_cache && _write_file_cache) {
        _cache_key = BlockFileCache::hash(_path.filename().native());
        _cache = FileCacheFactory::instance().get_by_path(_cache_key);
    }
}

S3FileWriter::~S3FileWriter() {
    if (!_closed || _failed) {
        // if we don't abort multi part upload, the uploaded part in object
        // store will not automatically reclaim itself, it would cost more money
        _abort();
        _bytes_written = 0;
    }
    s3_bytes_written_total << _bytes_written;
    s3_file_being_written << -1;
}

void S3FileWriter::_wait_until_finish(std::string_view task_name) {
    auto timeout_duration = config::s3_task_check_interval;
    auto msg = fmt::format(
            "{} multipart upload already takes {} seconds, bucket={}, key={}, upload_id={}",
            task_name, timeout_duration, _bucket, _path.native(), _upload_id);
    timespec current_time;
    // We don't need high accuracy here, so we use time(nullptr)
    // since it's the fastest way to get current time(second)
    auto current_time_second = time(nullptr);
    current_time.tv_sec = current_time_second + timeout_duration;
    current_time.tv_nsec = 0;
    // bthread::countdown_event::timed_wait() should use absolute time
    while (0 != _countdown_event.timed_wait(current_time)) {
        uint64_t finish_num = 0;
        {
            std::unique_lock lck {_completed_lock};
            finish_num = _completed_parts.size();
        }
        auto progess = fmt::format(" progress {}/{}", finish_num, _cur_part_num);
        current_time.tv_sec += timeout_duration;
        LOG(WARNING) << msg << progess;
    }
}

Status S3FileWriter::_open() {
    CreateMultipartUploadRequest create_request;
    create_request.WithBucket(_bucket).WithKey(_key);
    create_request.SetContentType("application/octet-stream");
    if (_sse_enabled) {
        create_request.WithServerSideEncryption(Aws::S3::Model::ServerSideEncryption::AES256);
    }

    auto outcome = SYNC_POINT_HOOK_RETURN_VALUE(
            _client->CreateMultipartUploadCallable(create_request).get(),
            "s3_file_writer::create_multi_part_upload", std::cref(create_request).get());
    s3_bvar::s3_multi_part_upload_total << 1;
    SYNC_POINT_CALLBACK("s3_file_writer::_open", &outcome);

    if (outcome.IsSuccess()) {
        _upload_id = outcome.GetResult().GetUploadId();
        return Status::OK();
    }
    return Status::IOError(
            "failed to create multipart upload(bucket={}, key={}, upload_id={}): {}, exception {}, "
            "error code {}",
            _bucket, _path.native(), _upload_id, outcome.GetError().GetMessage(),
            outcome.GetError().GetExceptionName(), outcome.GetError().GetResponseCode());
}

Status S3FileWriter::_abort() {
    _failed = true;
    _closed = true;
    if (_aborted) {
        return Status::OK();
    }
    // we need to reclaim the memory
    if (_pending_buf) {
        _pending_buf = nullptr;
    }
    // upload id is empty means there was no create multi upload
    // but the put object task might already be summitted to threadpool
    if (_upload_id.empty()) {
        LOG(INFO) << "early quit when put object, though the object might already be uploaded "
                     "before calling abort";
        _wait_until_finish("early quit when put object");
        _aborted = true;
        return Status::OK();
    }
    VLOG_DEBUG << "S3FileWriter::abort, path: " << _path.native();
    _wait_until_finish("early quit");
    AbortMultipartUploadRequest request;
    request.WithBucket(_bucket).WithKey(_key).WithUploadId(_upload_id);
    auto outcome = SYNC_POINT_HOOK_RETURN_VALUE(
            _client->AbortMultipartUploadCallable(request).get(),
            "s3_file_writer::abort_multi_part", std::cref(request).get());
    s3_bvar::s3_multi_part_upload_total << 1;
    if (outcome.IsSuccess() ||
        outcome.GetError().GetErrorType() == Aws::S3::S3Errors::NO_SUCH_UPLOAD ||
        outcome.GetError().GetResponseCode() == Aws::Http::HttpResponseCode::NOT_FOUND) {
        LOG(INFO) << "Abort multipart upload successfully"
                  << "bucket=" << _bucket << ", key=" << _path.native()
                  << ", upload_id=" << _upload_id;
        _aborted = true;
        return Status::OK();
    }
    return Status::IOError(
            "failed to abort multipart upload(bucket={}, key={}, upload_id={}): {}, exception {}, "
            "error code {}",
            _bucket, _path.native(), _upload_id, outcome.GetError().GetMessage(),
            outcome.GetError().GetExceptionName(), outcome.GetError().GetResponseCode());
}

Status S3FileWriter::close() {
    if (_closed) {
        _wait_until_finish("close");
        return _st;
    }
    Defer defer {[&]() { _closed = true; }};
    VLOG_DEBUG << "S3FileWriter::close, path: " << _path.native();
    if (_pending_buf != nullptr) {
        if (_upload_id.empty()) {
            auto buf = dynamic_cast<UploadFileBuffer*>(_pending_buf.get());
            DCHECK(buf != nullptr);
            buf->set_upload_to_remote([this](UploadFileBuffer& b) { _put_object(b); });
        }
        _countdown_event.add_count();
        _pending_buf->submit(std::move(_pending_buf));
        _pending_buf = nullptr;
    }
    SYNC_POINT_RETURN_WITH_VALUE("s3_file_writer::close", Status());
    RETURN_IF_ERROR(_complete());

    return Status::OK();
}

Status S3FileWriter::appendv(const Slice* data, size_t data_cnt) {
    DCHECK(!_closed);
    size_t buffer_size = config::s3_write_buffer_size;
    TEST_SYNC_POINT_RETURN_WITH_VALUE("s3_file_writer::appenv", Status());
    for (size_t i = 0; i < data_cnt; i++) {
        Slice slice = data[i];
        while (!slice.empty()) {
            if (_failed) {
                return _st;
            }
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
                            bool ret = false;
                            if (!s.ok()) [[unlikely]] {
                                VLOG_NOTICE << "failed at key: " << _key << ", load part "
                                            << part_num << ", st " << s;
                                std::unique_lock<std::mutex> _lck {_completed_lock};
                                _failed = true;
                                ret = true;
                                _st = std::move(s);
                            }
                            // After the signal, there is a scenario where the previous invocation of _wait_until_finish
                            // returns to the caller, and subsequently, the S3 file writer is destructed.
                            // This means that accessing _failed afterwards would result in a heap use after free vulnerability.
                            _countdown_event.signal();
                            return ret;
                        })
                        .set_is_cancelled([this]() { return _failed.load(); });
                if (_write_file_cache) {
                    // We would load the data into file cache asynchronously which indicates
                    // that this instance of S3FileWriter might have been destructed when we
                    // try to do writing into file cache, so we make the lambda capture the variable
                    // we need by value to extend their lifetime
                    builder.set_allocate_file_segments_holder(
                            [cache = _cache, k = _cache_key, offset = _bytes_appended,
                             t = _expiration_time, cold = _is_cold_data]() -> FileBlocksHolderPtr {
                                CacheContext ctx;
                                ctx.cache_type =
                                        t == 0 ? FileCacheType::NORMAL : FileCacheType::TTL;
                                ctx.expiration_time = t;
                                ctx.is_cold_data = cold;
                                auto holder = cache->get_or_set(k, offset,
                                                                config::s3_write_buffer_size, ctx);
                                return std::make_unique<FileBlocksHolder>(std::move(holder));
                            });
                }
                RETURN_IF_ERROR(builder.build(&_pending_buf));
            }
            // we need to make sure all parts except the last one to be 5MB or more
            // and shouldn't be larger than buf
            auto data_size_to_append = std::min(
                    _pending_buf->get_capacaticy() - _pending_buf->get_size(), slice.get_size());

            // if the buffer has memory buf inside, the data would be written into memory first then S3 then file cache
            // it would be written to cache then S3 if the buffer doesn't have memory preserved
            RETURN_IF_ERROR(
                    _pending_buf->append_data(Slice {slice.get_data(), data_size_to_append}));
            slice.remove_prefix(data_size_to_append);
            TEST_SYNC_POINT_CALLBACK("s3_file_writer::appenv_1", &_pending_buf, _cur_part_num);

            // if it's the last part, it could be less than 5MB, or it must
            // satisfy that the size is larger than or euqal to 5MB
            // _complete() would handle the first situation
            if (_pending_buf->get_size() == buffer_size) {
                if (1 == _cur_part_num) [[unlikely]] {
                    RETURN_IF_ERROR(_open());
                }
                _cur_part_num++;
                _countdown_event.add_count();
                _pending_buf->submit(std::move(_pending_buf));
                _pending_buf = nullptr;
            }
            _bytes_appended += data_size_to_append;
        }
    }
    return Status::OK();
}

void S3FileWriter::_upload_one_part(int64_t part_num, UploadFileBuffer& buf) {
    if (_failed) {
        return;
    }
    UploadPartRequest upload_request;
    upload_request.WithBucket(_bucket).WithKey(_key).WithPartNumber(part_num).WithUploadId(
            _upload_id);

    upload_request.SetBody(buf.get_stream());

    Aws::Utils::ByteBuffer part_md5(Aws::Utils::HashingUtils::CalculateMD5(*buf.get_stream()));
    upload_request.SetContentMD5(Aws::Utils::HashingUtils::Base64Encode(part_md5));

    upload_request.SetContentLength(buf.get_size());

    auto upload_part_outcome = SYNC_POINT_HOOK_RETURN_VALUE(
            _client->UploadPartCallable(upload_request).get(), "s3_file_writer::upload_part",
            std::cref(upload_request).get(), &buf);
    s3_bvar::s3_multi_part_upload_total << 1;
    TEST_SYNC_POINT_CALLBACK("S3FileWriter::_upload_one_part", &upload_part_outcome);
    if (!upload_part_outcome.IsSuccess()) {
        _st = Status::IOError(
                "failed to upload part (bucket={}, key={}, part_num={}, upload_id={}, "
                "exception={}, error code={}): {}",
                _bucket, _path.native(), part_num, _upload_id,
                upload_part_outcome.GetError().GetExceptionName(),
                upload_part_outcome.GetError().GetResponseCode(),
                upload_part_outcome.GetError().GetMessage());
        LOG(WARNING) << _st;
        buf.set_status(_st);
        return;
    }

    std::unique_ptr<CompletedPart> completed_part = std::make_unique<CompletedPart>();

    completed_part->SetPartNumber(part_num);
    const auto& etag = upload_part_outcome.GetResult().GetETag();
    // DCHECK(etag.empty());
    completed_part->SetETag(etag);

    std::unique_lock<std::mutex> lck {_completed_lock};
    _completed_parts.emplace_back(std::move(completed_part));
    _bytes_written += buf.get_size();
    s3_bytes_uploaded_total << buf.get_size();
}

Status S3FileWriter::_complete() {
    if (_failed) {
        _wait_until_finish("early quit");
        return _st;
    }
    // upload id is empty means there was no multipart upload
    if (_upload_id.empty()) {
        _wait_until_finish("finish put object");
        return _st;
    }
    CompleteMultipartUploadRequest complete_request;
    complete_request.WithBucket(_bucket).WithKey(_key).WithUploadId(_upload_id);

    _wait_until_finish("Complete");
    TEST_SYNC_POINT_CALLBACK("S3FileWriter::_complete:1",
                             std::make_pair(&_failed, &_completed_parts));
    if (_failed || _completed_parts.size() != _cur_part_num) {
        _st = Status::IOError("error status {}, complete parts {}, cur part num {}", _st,
                                  _completed_parts.size(), _cur_part_num);
        LOG(WARNING) << _st;
        return _st;
    }
    // make sure _completed_parts are ascending order
    std::sort(_completed_parts.begin(), _completed_parts.end(),
              [](auto& p1, auto& p2) { return p1->GetPartNumber() < p2->GetPartNumber(); });
    TEST_SYNC_POINT_CALLBACK("S3FileWriter::_complete:2", &_completed_parts);
    CompletedMultipartUpload completed_upload;
    for (size_t i = 0; i < _completed_parts.size(); i++) {
        if (_completed_parts[i]->GetPartNumber() != i + 1) [[unlikely]] {
            _st = Status::IOError(
                    "error status {}, part num not continous, expected num {}, actual num {}", _st,
                    i + 1, _completed_parts[i]->GetPartNumber());
            LOG(WARNING) << _st;
            return _st;
        }
        completed_upload.AddParts(*_completed_parts[i]);
    }

    complete_request.WithMultipartUpload(completed_upload);

    TEST_SYNC_POINT_RETURN_WITH_VALUE("S3FileWriter::_complete:3", Status(), this);
    auto compute_outcome = SYNC_POINT_HOOK_RETURN_VALUE(
            _client->CompleteMultipartUploadCallable(complete_request).get(),
            "s3_file_writer::complete_multi_part", std::cref(complete_request).get());
    s3_bvar::s3_multi_part_upload_total << 1;

    if (!compute_outcome.IsSuccess()) {
        _st = Status::IOError(
                "failed to complete multi part upload (bucket={}, key={}, upload_id={}, "
                "exception={}, error code={}): {}",
                _bucket, _path.native(), _upload_id, compute_outcome.GetError().GetExceptionName(),
                compute_outcome.GetError().GetResponseCode(),
                compute_outcome.GetError().GetMessage());
        LOG(WARNING) << _st;
        return _st;
    }
    s3_file_created_total << 1;
    return Status::OK();
}

Status S3FileWriter::finalize() {
    DCHECK(!_closed);
    SYNC_POINT_RETURN_WITH_VALUE("s3_file_writer::finalize", Status());
    // submit pending buf if it's not nullptr
    // it's the last buf, we can submit it right now
    if (_pending_buf != nullptr) {
        // the whole file size is less than one buffer, we can just call PutObject to reduce network RTT
        if (1 == _cur_part_num) {
            auto buf = dynamic_cast<UploadFileBuffer*>(_pending_buf.get());
            DCHECK(buf != nullptr);
            buf->set_upload_to_remote([this](UploadFileBuffer& b) { _put_object(b); });
        }
        _countdown_event.add_count();
        _pending_buf->submit(std::move(_pending_buf));
        _pending_buf = nullptr;
    }
    return Status::OK();
}

void S3FileWriter::_put_object(UploadFileBuffer& buf) {
    DCHECK(!_closed) << "closed " << _closed;
    LOG(INFO) << "enter put object operation for key " << _key << " bucket " << _bucket;
    // the task might already be aborted
    if (_failed) [[unlikely]] {
        return;
    }
    Aws::S3::Model::PutObjectRequest request;
    request.WithBucket(_bucket).WithKey(_key);
    Aws::Utils::ByteBuffer part_md5(Aws::Utils::HashingUtils::CalculateMD5(*buf.get_stream()));
    request.SetContentMD5(Aws::Utils::HashingUtils::Base64Encode(part_md5));
    request.SetBody(buf.get_stream());
    request.SetContentLength(buf.get_size());
    request.SetContentType("application/octet-stream");
    TEST_SYNC_POINT_RETURN_WITH_VOID("S3FileWriter::_put_object", this, &buf);
    auto response = SYNC_POINT_HOOK_RETURN_VALUE(_client->PutObjectCallable(request).get(),
                                                 "s3_file_writer::put_object",
                                                 std::cref(request).get(), &buf);
    s3_bvar::s3_put_total << 1;
    if (!response.IsSuccess()) {
        _st = Status::IOError(
                "failed to put object (bucket={}, key={}, upload_id={}, exception={}, error "
                "code={}): {}",
                _bucket, _path.native(), _upload_id, response.GetError().GetExceptionName(),
                response.GetError().GetResponseCode(), response.GetError().GetMessage());
        LOG(WARNING) << _st;
        buf.set_status(_st);
        return;
    }
    _bytes_written += buf.get_size();
    s3_bytes_uploaded_total << buf.get_size();
    s3_file_created_total << 1;
}

} // namespace io
} // namespace doris
