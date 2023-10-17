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

#include "s3_file_bufferpool.h"

#include <chrono>

#include "common/config.h"
#include "common/logging.h"
#include "common/sync_point.h"
#include "io/cache/block/block_file_segment.h"
#include "io/fs/s3_common.h"
#include "runtime/exec_env.h"
#include "util/defer_op.h"
#include "util/slice.h"

namespace doris {
namespace io {

/**
 * 0. check if the inner memory buffer is empty or not
 * 1. relcaim the memory buffer if it's mot empty
 */
void FileBuffer::on_finish() {
    if (_buffer.empty()) {
        return;
    }
    auto cur_time = time(nullptr);
    CHECK(cur_time - get_allocation_time() < 300)
            << "the buffer is hanged too long, cur " << cur_time << " allocation time "
            << get_allocation_time();
    S3FileBufferPool::GetInstance()->reclaim(Slice {_buffer.get_data(), _capacity});
    _buffer.clear();
}

/**
 * take other buffer's memory space and refresh capacity
 */
void FileBuffer::swap_buffer(Slice& other) {
    _buffer = other;
    _capacity = _buffer.get_size();
    other.clear();
}

FileBuffer::FileBuffer(std::function<FileBlocksHolderPtr()> alloc_holder, size_t offset,
                       OperationState state, bool reserve, void* resource_owner)
        : _alloc_holder(std::move(alloc_holder)),
          _buffer(S3FileBufferPool::GetInstance()->allocate(reserve)),
          _offset(offset),
          _size(0),
          _state(std::move(state)),
          _capacity(_buffer.get_size()),
          _resource_owner(resource_owner),
          _allocation_time(0) {
    if (!_buffer.empty()) {
        _allocation_time = time(nullptr);
        LOG_INFO("allocate slice for {}", _resource_owner);
    }
}

/**
 * 0. check if file cache holder allocated
 * 1. update the cache's type to index cache
 */
void UploadFileBuffer::set_index_offset(size_t offset) {
    _index_offset = offset;
}

/**
 * 0. when there is memory preserved, directly write data to buf
 * 1. write to file cache otherwise, then we'll wait for free buffer and to rob it
 */
Status UploadFileBuffer::append_data(const Slice& data) {
    Defer defer {[&] { _size += data.get_size(); }};
    while (true) {
        // if buf is not empty, it means there is memory preserved for this buf
        if (!_buffer.empty()) {
            std::memcpy((void*)(_buffer.get_data() + _size), data.get_data(), data.get_size());
            break;
        }
        // wait allocate buffer pool
        auto tmp = S3FileBufferPool::GetInstance()->allocate(true);
        if (tmp.empty()) {
            LOG_WARNING("no free buffer for 5 minutes");
            return Status::InternalError("no free buffer for 5 minutes");
        }
        swap_buffer(tmp);
        set_allocation_time(time(nullptr));
        LOG_INFO("allocate slice for {}", _resource_owner);
    }
    return Status::OK();
}

/**
 * submit the on_download() task to executor
 */
void DownloadFileBuffer::submit() {
    // Currently download file buffer is only served for cache prefetching
    // so we just skip executing the download task when file cache is not enabled
    if (!config::enable_file_cache) [[unlikely]] {
        LOG(INFO) << "Skip download file task because file cache is not enabled";
        return;
    }
    ExecEnv::GetInstance()->s3_downloader_download_thread_pool()->submit_func(
            [buf = this->shared_from_this(), this]() {
                // to extend buf's lifetime
                // (void)buf;
                on_download();
            });
}

/**
 * 0. constrcut the stream ptr if the buffer is not empty
 * 1. submit the on_upload() callback to executor
 */
void UploadFileBuffer::submit() {
    if (!_buffer.empty()) [[likely]] {
        _stream_ptr = std::make_shared<StringViewStream>(_buffer.get_data(), _size);
    }
    ExecEnv::GetInstance()->s3_file_writer_upload_thread_pool()->submit_func(
            [buf = this->shared_from_this(), this]() {
                // to extend buf's lifetime
                // (void)buf;
                on_upload();
            });
}

/**
 * write the content of the memory buffer to local file cache
 */
void UploadFileBuffer::upload_to_local_file_cache(bool is_cancelled) {
    if (!config::enable_file_cache || _alloc_holder == nullptr) {
        return;
    }
    if (is_cancelled) {
        return;
    }
    // the data is already written to S3 in this situation
    // so i didn't handle the file cache write error
    auto holder = _alloc_holder();
    size_t pos = 0;
    size_t data_remain_size = _size;
    for (auto& segment : holder->file_segments) {
        if (data_remain_size == 0) {
            break;
        }
        size_t segment_size = segment->range().size();
        size_t append_size = std::min(data_remain_size, segment_size);
        if (segment->state() == FileBlock::State::EMPTY) {
            if (_index_offset != 0 && segment->range().right >= _index_offset) {
                segment->change_cache_type_self(FileCacheType::INDEX);
            }
            segment->get_or_set_downloader();
            // Another thread may have started downloading due to a query
            // Just skip putting to cache from UploadFileBuffer
            if (segment->is_downloader()) {
                Slice s(_buffer.get_data() + pos, append_size);
                Status st = segment->append(s);
                if (st.ok()) {
                    st = segment->finalize_write();
                }
                if (!st.ok()) {
                    {
                        [[maybe_unused]] bool ret = false;
                        TEST_SYNC_POINT_CALLBACK("UploadFileBuffer::upload_to_local_file_cache",
                                                 &ret);
                    }
                    LOG_WARNING("failed to append data to file cache").error(st);
                }
            }
        }
        data_remain_size -= append_size;
        pos += append_size;
    }
}

FileBufferBuilder& FileBufferBuilder::set_type(BufferType type) {
    _type = type;
    return *this;
}
FileBufferBuilder& FileBufferBuilder::set_upload_callback(
        std::function<void(UploadFileBuffer& buf)> cb) {
    _upload_cb = std::move(cb);
    return *this;
}
// set callback to do task sync for the caller
FileBufferBuilder& FileBufferBuilder::set_sync_after_complete_task(std::function<bool(Status)> cb) {
    _sync_after_complete_task = std::move(cb);
    return *this;
}

FileBufferBuilder& FileBufferBuilder::set_allocate_file_segments_holder(
        std::function<FileBlocksHolderPtr()> cb) {
    _alloc_holder_cb = std::move(cb);
    return *this;
}

std::shared_ptr<FileBuffer> FileBufferBuilder::build() {
    OperationState state(_sync_after_complete_task, _is_cancelled);
    if (_type == BufferType::UPLOAD) {
        return std::make_shared<UploadFileBuffer>(std::move(_upload_cb), std::move(state), _offset,
                                                  std::move(_alloc_holder_cb), _index_offset,
                                                  _resource_owner);
    }
    if (_type == BufferType::DOWNLOAD) {
        return std::make_shared<DownloadFileBuffer>(
                std::move(_download), std::move(_write_to_local_file_cache),
                std::move(_write_to_use_buffer), std::move(state), _offset,
                std::move(_alloc_holder_cb));
    }
    // should never come here
    return nullptr;
}

void S3FileBufferPool::init(int32_t s3_write_buffer_whole_size, int32_t s3_write_buffer_size,
                            ThreadPool* thread_pool) {
    // the nums could be one configuration
    size_t buf_num = s3_write_buffer_whole_size / s3_write_buffer_size;
    DCHECK((s3_write_buffer_size >= 5 * 1024 * 1024) &&
           (s3_write_buffer_whole_size > s3_write_buffer_size));
    LOG_INFO("S3 file buffer pool with {} buffers", buf_num);
    _whole_mem_buffer = std::make_unique<char[]>(s3_write_buffer_whole_size);
    for (size_t i = 0; i < buf_num; i++) {
        Slice s {_whole_mem_buffer.get() + i * s3_write_buffer_size,
                 static_cast<size_t>(s3_write_buffer_size)};
        // auto buf = std::make_shared<S3FileBuffer>(s);
        _free_raw_buffers.emplace_back(s);
    }
}

Slice S3FileBufferPool::allocate(bool reserve) {
    Slice buf;
    TEST_SYNC_POINT_RETURN_WITH_VALUE("s3_file_bufferpool::allocate", Slice());
    // if need reserve or no cache then we must ensure return buf with memory preserved
    if (reserve || !config::enable_file_cache) {
        {
            std::unique_lock<std::mutex> lck {_lock};
            _cv.wait_for(lck, std::chrono::seconds(300),
                         [this]() { return !_free_raw_buffers.empty(); });
            if (!_free_raw_buffers.empty()) {
                buf = _free_raw_buffers.front();
                _free_raw_buffers.pop_front();
            }
        }
        return buf;
    }
    // try to get one memory reserved buffer
    {
        std::unique_lock<std::mutex> lck {_lock};
        if (!_free_raw_buffers.empty()) {
            buf = _free_raw_buffers.front();
            _free_raw_buffers.pop_front();
        }
    }
    if (!buf.empty()) {
        return buf;
    }
    // if there is no free buffer and no need to reserve memory, we could return one empty buffer
    buf = Slice();
    // if the buf has no memory reserved, it would try to write the data to file cache first
    // or it would try to rob buffer from other S3FileBuffer
    return buf;
}

/**
 * 0. check if we need to write into cache
 * 1. check if there is free space inside the file cache
 * 2. call the download callback
 * 3. write the downloaded content into user buffer if necessary
 */
void DownloadFileBuffer::on_download() {
    FileBlocksHolderPtr holder = nullptr;
    bool need_to_download_into_cache = false;
    auto s = Status::OK();
    Defer def {[&]() {
        _state.set_val(std::move(s));
        on_finish();
    }};
    if (_alloc_holder != nullptr) {
        holder = _alloc_holder();
        std::for_each(holder->file_segments.begin(), holder->file_segments.end(),
                      [&need_to_download_into_cache](FileBlockSPtr& file_segment) {
                          if (file_segment->state() == FileBlock::State::EMPTY) {
                              file_segment->get_or_set_downloader();
                              if (file_segment->is_downloader()) {
                                  need_to_download_into_cache = true;
                              }
                          }
                      });
        if (!need_to_download_into_cache && !_write_to_use_buffer) [[unlikely]] {
            LOG(INFO) << "Skipping download because that there is no space for catch data.";
        } else {
            Slice tmp {_buffer.get_data(), _capacity};
            s = _download(tmp);
            if (s) {
                _size = tmp.get_size();
                if (_write_to_use_buffer != nullptr) {
                    _write_to_use_buffer({_buffer.get_data(), get_size()}, get_file_offset());
                }
                if (need_to_download_into_cache) {
                    _write_to_local_file_cache(std::move(holder),
                                               Slice {_buffer.get_data(), _size});
                }
            } else {
                LOG(WARNING) << s;
            }
            _state.set_val(std::move(s));
        }
        on_finish();
    } else {
        Slice tmp {_buffer.get_data(), _capacity};
        s = _download(tmp);
        _size = tmp.get_size();
        if (_write_to_use_buffer != nullptr) {
            _write_to_use_buffer({_buffer.get_data(), get_size()}, get_file_offset());
        }
    }
}

} // namespace io
} // namespace doris
