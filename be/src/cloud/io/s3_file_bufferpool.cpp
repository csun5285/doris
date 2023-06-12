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

#include <cstring>

#include "cloud/io/cloud_file_segment.h"
#include "cloud/io/s3_common.h"
#include "common/config.h"
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

FileBuffer::FileBuffer(std::function<FileSegmentsHolderPtr()> alloc_holder, size_t offset,
                       OperationState state, bool reserve)
        : _alloc_holder(std::move(alloc_holder)),
          _buffer(S3FileBufferPool::GetInstance()->allocate(reserve)),
          _offset(offset),
          _size(0),
          _state(std::move(state)),
          _capacity(_buffer.get_size()) {}

/**
 * 0. check if file cache holder allocated
 * 1. update the cache's type to index cache
 */
void UploadFileBuffer::set_index_offset(size_t offset) {
    _index_offset = offset;
    if (_holder) {
        bool change_to_index_cache = false;
        for (auto iter = _holder->file_segments.begin(); iter != _holder->file_segments.end();
             ++iter) {
            if (iter == _cur_file_segment) {
                change_to_index_cache = true;
            }
            if (change_to_index_cache) {
                (*iter)->change_cache_type_self(CacheType::INDEX);
            }
        }
    }
}

/**
 * 0. when there is memory preserved, directly write data to buf
 * 1. write to file cache otherwise, then we'll wait for free buffer and to rob it
 */
void UploadFileBuffer::append_data(const Slice& data) {
    Defer defer {[&] { _size += data.get_size(); }};
    while (true) {
        // if buf is not empty, it means there is memory preserved for this buf
        if (!_buffer.empty()) {
            std::memcpy((void*)(_buffer.get_data() + _size), data.get_data(), data.get_size());
            break;
        }
        // if the buf has no memory reserved, then write to disk first
        if (!_is_cache_allocated && config::enable_file_cache) {
            _holder = _alloc_holder();
            bool cache_is_not_enough = false;
            for (auto& segment : _holder->file_segments) {
                DCHECK(segment->state() == FileSegment::State::SKIP_CACHE ||
                       segment->state() == FileSegment::State::EMPTY);
                if (segment->state() == FileSegment::State::SKIP_CACHE) [[unlikely]] {
                    cache_is_not_enough = true;
                    break;
                }
                if (_index_offset != 0) {
                    segment->change_cache_type_self(CacheType::INDEX);
                }
            }
            // if cache_is_not_enough, cannot use it !
            _cur_file_segment = _holder->file_segments.begin();
            _append_offset = (*_cur_file_segment)->range().left;
            _holder = cache_is_not_enough ? nullptr : std::move(_holder);
            if (_holder) {
                (*_cur_file_segment)->get_or_set_downloader();
            }
            _is_cache_allocated = true;
        }
        if (_holder) [[likely]] {
            size_t data_remain_size = data.get_size();
            size_t pos = 0;
            while (data_remain_size != 0) {
                auto range = (*_cur_file_segment)->range();
                size_t segment_remain_size = range.right - _append_offset + 1;
                size_t append_size = std::min(data_remain_size, segment_remain_size);
                Slice append_data(data.get_data() + pos, append_size);
                (*_cur_file_segment)->append(append_data);
                if (segment_remain_size == append_size) {
                    (*_cur_file_segment)->finalize_write(); // DOWNLOADED
                    if (++_cur_file_segment != _holder->file_segments.end()) {
                        (*_cur_file_segment)->get_or_set_downloader();
                    }
                }
                data_remain_size -= append_size;
                _append_offset += append_size;
                pos += append_size;
            }
            break;
        } else {
            // wait allocate buffer pool
            auto tmp = S3FileBufferPool::GetInstance()->allocate(true);
            swap_buffer(tmp);
        }
    }
}

/**
 * 0. allocate one memory buffer
 * 1. read the content from the cache and then write
 * it into memory buffer
 */
void UploadFileBuffer::read_from_cache() {
    auto tmp = S3FileBufferPool::GetInstance()->allocate(true);
    swap_buffer(tmp);

    DCHECK(_holder != nullptr);
    DCHECK(_capacity >= _size);
    size_t pos = 0;
    for (auto& segment : _holder->file_segments) {
        if (pos == _size) {
            break;
        }
        segment->finalize_write();
        size_t segment_size = segment->range().size();
        Slice s(_buffer.get_data() + pos, segment_size);
        segment->read_at(s, 0);
        pos += segment_size;
    }

    // the real lenght should be the buf.get_size() in this situation(consider it's the last part,
    // size of it could be less than 5MB)
    _stream_ptr = std::make_shared<StringViewStream>(_buffer.get_data(), _size);
}
/**
 * submit the on_download() task to executor
 */
void DownloadFileBuffer::submit() {
    ExecEnv::GetInstance()->buffered_reader_prefetch_thread_pool()->submit_func(
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
    if (_holder && _cur_file_segment != _holder->file_segments.end()) {
        (*_cur_file_segment)->finalize_write();   
    }
    ExecEnv::GetInstance()->buffered_reader_prefetch_thread_pool()->submit_func(
            [buf = this->shared_from_this(), this]() {
                // to extend buf's lifetime
                // (void)buf;
                on_upload();
            });
}

/**
 * write the content of the memory buffer to local file cache
 */
void UploadFileBuffer::upload_to_local_file_cache() {
    if (!config::enable_file_cache || _alloc_holder == nullptr) {
        return;
    }
    if (_holder) {
        return;
    }
    if (_state.is_done()) {
        return;
    }
    // the data is already written to S3 in this situation
    // so i didn't handle the file cache write error
    _holder = _alloc_holder();
    size_t pos = 0;
    size_t data_remain_size = _size;
    for (auto& segment : _holder->file_segments) {
        if (data_remain_size == 0) {
            break;
        }
        DCHECK(segment->state() == FileSegment::State::EMPTY ||
               segment->state() == FileSegment::State::SKIP_CACHE);
        size_t segment_size = segment->range().size();
        size_t append_size = std::min(data_remain_size, segment_size);
        if (segment->state() == FileSegment::State::EMPTY) {
            if (_index_offset != 0 && segment->range().right >= _index_offset) {
                segment->change_cache_type_self(CacheType::INDEX);
            }
            segment->get_or_set_downloader();
            DCHECK(segment->is_downloader());
            Slice s(_buffer.get_data() + pos, append_size);
            segment->append(s);
            segment->finalize_write();
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
FileBufferBuilder& FileBufferBuilder::set_sync_after_complete_task(std::function<void(Status)> cb) {
    _sync_after_complete_task = std::move(cb);
    return *this;
}

FileBufferBuilder& FileBufferBuilder::set_allocate_file_segments_holder(
        std::function<FileSegmentsHolderPtr()> cb) {
    _alloc_holder_cb = std::move(cb);
    return *this;
}

std::shared_ptr<FileBuffer> FileBufferBuilder::build() {
    OperationState state(_sync_after_complete_task, _is_done);
    if (_type == BufferType::UPLOAD) {
        return std::make_shared<UploadFileBuffer>(std::move(_upload_cb), std::move(state), _offset,
                                                  std::move(_alloc_holder_cb), _index_offset);
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

S3FileBufferPool::S3FileBufferPool() {
    // the nums could be one configuration
    size_t buf_num = config::s3_write_buffer_whole_size / config::s3_write_buffer_size;
    DCHECK((config::s3_write_buffer_size >= 5 * 1024 * 1024) &&
           (config::s3_write_buffer_whole_size > config::s3_write_buffer_size));
    LOG_INFO("S3 file buffer pool with {} buffers", buf_num);
    _whole_mem_buffer = std::make_unique<char[]>(config::s3_write_buffer_whole_size);
    for (size_t i = 0; i < buf_num; i++) {
        Slice s {_whole_mem_buffer.get() + i * config::s3_write_buffer_size,
                 static_cast<size_t>(config::s3_write_buffer_size)};
        // auto buf = std::make_shared<S3FileBuffer>(s);
        _free_raw_buffers.emplace_back(s);
    }
}

Slice S3FileBufferPool::allocate(bool reserve) {
    Slice buf;
    // if need reserve or no cache then we must ensure return buf with memory preserved
    if (reserve || !config::enable_file_cache) {
        {
            std::unique_lock<std::mutex> lck {_lock};
            _cv.wait(lck, [this]() { return !_free_raw_buffers.empty(); });
            buf = _free_raw_buffers.front();
            _free_raw_buffers.pop_front();
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
    FileSegmentsHolderPtr holder = nullptr;
    bool need_to_download_into_cache = false;
    auto s = Status::OK();
    Defer def {[&]() {
        _state.set_val(std::move(s));
        on_finish();
    }};
    if (_alloc_holder != nullptr) {
        holder = _alloc_holder();
        std::for_each(holder->file_segments.begin(), holder->file_segments.end(),
                      [&need_to_download_into_cache](FileSegmentSPtr& file_segment) {
                          if (file_segment->state() == FileSegment::State::EMPTY) {
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
