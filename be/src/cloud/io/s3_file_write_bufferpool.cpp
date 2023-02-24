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

#include "s3_file_write_bufferpool.h"

#include <cstring>

#include "cloud/io/cloud_file_cache.h"
#include "cloud/io/cloud_file_segment.h"
#include "cloud/io/s3_common.h"
#include "common/config.h"
#include "runtime/exec_env.h"
#include "util/defer_op.h"

namespace doris {
namespace io {
void S3FileBuffer::on_finished() {
    if (_buf == nullptr) {
        return;
    }
    reset();
    S3FileBufferPool::GetInstance()->reclaim(shared_from_this());
}

void S3FileBuffer::set_index_offset(size_t offset) {
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

// when there is memory preserved, directly write data to buf
// write to file cache otherwise, then we'll wait for free buffer
// and to rob it
void S3FileBuffer::append_data(const Slice& data) {
    Defer defer {[&] { _size += data.get_size(); }};
    while (true) {
        // if buf is not empty, it means there is memory preserved for this buf
        if (_buf != nullptr) {
            memcpy(_buf->data() + _size, data.get_data(), data.get_size());
            break;
        }
        // if the buf has no memory reserved, then write to disk first
        if (!_is_cache_allocated && config::enable_file_cache) {
            _holder = _allocate_file_segments_holder();
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
                segment->get_or_set_downloader();
                DCHECK(segment->is_downloader());
            }
            // if cache_is_not_enough, cannot use it !
            _cur_file_segment = _holder->file_segments.begin();
            _append_offset = (*_cur_file_segment)->range().left;
            _holder = cache_is_not_enough ? nullptr : std::move(_holder);
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
                _cur_file_segment = segment_remain_size == append_size ? ++_cur_file_segment
                                                                       : _cur_file_segment;
                data_remain_size -= append_size;
                _append_offset += append_size;
                pos += append_size;
            }
            break;
        } else {
            // wait allocate buffer pool
            auto tmp = S3FileBufferPool::GetInstance()->allocate(true);
            rob_buffer(tmp);
        }
    }
}

void S3FileBuffer::read_from_cache() {
    auto tmp = S3FileBufferPool::GetInstance()->allocate(true);
    rob_buffer(tmp);

    DCHECK(_holder != nullptr);
    DCHECK(_buf->size() >= _size);
    size_t pos = 0;
    for (auto& segment : _holder->file_segments) {
        if (pos == _size) {
            break;
        }
        segment->reset_range();
        segment->finalize_write();
        size_t segment_size = segment->range().size();
        Slice s(_buf->data() + pos, segment_size);
        segment->read_at(s, 0);
        pos += segment_size;
    }

    // the real lenght should be the buf.get_size() in this situation(consider it's the last part,
    // size of it could be less than 5MB)
    _stream_ptr = std::make_shared<StringViewStream>(_buf->data(), _size);
}

void S3FileBuffer::submit() {
    if (_buf != nullptr) [[likely]] {
        _stream_ptr = std::make_shared<StringViewStream>(_buf->data(), _size);
    }
    // just to write code, to be changed
    ExecEnv::GetInstance()->buffered_reader_prefetch_thread_pool()->submit_func(
            [buf = this->shared_from_this()]() { buf->_on_upload(); });
}

void S3FileBuffer::upload_to_local_file_cache() {
    if (!config::enable_file_cache) {
        return;
    }
    // TODO(yuejing && lightman): how about the indexes cache?
    if (_holder) {
        return;
    }
    if (_is_cancelled()) {
        return;
    }
    // the data is already written to S3 in this situation
    // so i didn't handle the file cache write error
    _holder = _allocate_file_segments_holder();
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
            Slice s(_buf->data() + pos, append_size);
            segment->append(s);
            segment->reset_range();
            segment->finalize_write();
        }
        data_remain_size -= append_size;
        pos += append_size;
    }
}

S3FileBufferPool::S3FileBufferPool() {
    // the nums could be one configuration
    size_t buf_num = config::s3_write_buffer_whole_size / config::s3_write_buffer_size;
    DCHECK((config::s3_write_buffer_size >= 5 * 1024 * 1024) &&
           (config::s3_write_buffer_whole_size > config::s3_write_buffer_size));
    LOG_INFO("S3 file buffer pool with {} buffers", buf_num);
    for (size_t i = 0; i < buf_num; i++) {
        auto buf = std::make_shared<S3FileBuffer>();
        buf->reserve_buffer();
        _free_buffers.emplace_back(std::move(buf));
    }
}

std::shared_ptr<S3FileBuffer> S3FileBufferPool::allocate(bool reserve) {
    std::shared_ptr<S3FileBuffer> buf;
    // if need reserve or no cache then we must ensure return buf with memory preserved
    if (reserve || !config::enable_file_cache) {
        {
            std::unique_lock<std::mutex> lck {_lock};
            _cv.wait(lck, [this]() { return !_free_buffers.empty(); });
            buf = std::move(_free_buffers.front());
            _free_buffers.pop_front();
        }
        return buf;
    }
    // try to get one memory reserved buffer
    {
        std::unique_lock<std::mutex> lck {_lock};
        if (!_free_buffers.empty()) {
            buf = std::move(_free_buffers.front());
            _free_buffers.pop_front();
        }
    }
    if (buf != nullptr) {
        return buf;
    }
    // if there is no free buffer and no need to reserve memory, we could return one empty buffer
    buf = std::make_shared<S3FileBuffer>();
    // if the buf has no memory reserved, it would try to write the data to file cache first
    // or it would try to rob buffer from other S3FileBuffer
    return buf;
}
} // namespace io
} // namespace doris
