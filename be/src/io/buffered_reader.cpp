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

#include "io/buffered_reader.h"

#include <bvar/reducer.h>
#include <bvar/window.h>

#include <algorithm>
#include <sstream>

#include "common/compiler_util.h"
#include "common/config.h"
#include "common/status.h"
#include "olap/olap_define.h"
#include "runtime/exec_env.h"
#include "util/bit_util.h"

namespace doris {

// add bvar to capture the download bytes per second by buffered reader
bvar::Adder<uint64_t> g_bytes_downloaded("buffered_reader", "bytes_downloaded");
bvar::PerSecond<bvar::Adder<uint64_t>> g_bytes_downloaded_per_second("buffered_reader",
                                                                    "bytes_downloaded_per_second",
                                                                    &g_bytes_downloaded, 60);

// there exists occasions where the buffer is already closed but
// some prior tasks are still queued in threadpool, so we have to check wheher
// the buffer is closed each time the condition variable is norified
void PrefetchBuffer::reset_offset(int64_t offset) {
    {
        std::unique_lock lck {_lock};
        _prefetched.wait(lck, [this]() { return _buffer_status != BufferStatus::PENDING; });
        if (_buffer_status == BufferStatus::CLOSED) [[unlikely]] {
            _prefetched.notify_all();
            return;
        }
        _buffer_status = BufferStatus::RESET;
        _offset = offset;
        _prefetched.notify_all();
    }
    ExecEnv::GetInstance()->buffered_reader_prefetch_thread_pool()->submit_func(
            [buffer_ptr = shared_from_this()]() { buffer_ptr->prefetch_buffer(); });
}

// only this function would run concurrently in another thread
void PrefetchBuffer::prefetch_buffer() {
    {
        std::unique_lock lck {_lock};
        _prefetched.wait(lck, [this]() {
            return _buffer_status == BufferStatus::RESET || _buffer_status == BufferStatus::CLOSED;
        });
        // in case buffer is already closed
        if (_buffer_status == BufferStatus::CLOSED) [[unlikely]] {
            _prefetched.notify_all();
            return;
        }
        _buffer_status = BufferStatus::PENDING;
        _prefetched.notify_all();
    }
    _len = 0;
    Status s;
    DCHECK(_reader->size() - _offset >= 0);
    size_t nbytes = std::min({_size, _reader->size() - _offset});
    {
        SCOPED_TIMER(_remote_read_timer);
        s = _reader->readat(_offset, nbytes, &_len, _buf.data());
    }
    g_bytes_downloaded << _len;
    COUNTER_UPDATE(_remote_read_counter, 1);
    std::unique_lock lck {_lock};
    _prefetched.wait(lck, [this]() { return _buffer_status == BufferStatus::PENDING; });
    if (!s.ok()) {
        _prefetch_status = std::move(s);
    }
    COUNTER_UPDATE(_remote_read_bytes, _len);
    _buffer_status = BufferStatus::PREFETCHED;
    _prefetched.notify_all();
    // eof would come up with len == 0, it would be handled by read_buffer
}

Status PrefetchBuffer::read_buffer(int64_t off, uint8_t* out, int64_t buf_len,
                                   int64_t* bytes_read) {
    {
        std::unique_lock lck {_lock};
        // buffer must be prefetched or it's closed
        _prefetched.wait(lck, [this]() {
            return _buffer_status == BufferStatus::PREFETCHED ||
                   _buffer_status == BufferStatus::CLOSED;
        });
        if (BufferStatus::CLOSED == _buffer_status) [[unlikely]] {
            return Status::OK();
        }
    }
    RETURN_IF_ERROR(_prefetch_status);
    // there is only parquet would do not sequence read
    // it would read the end of the file first
    if (!contains(off)) [[unlikely]] {
        reset_offset((off / _size) * _size);
        return read_buffer(off, out, buf_len, bytes_read);
    }
    if (0 == _len || _offset + _len < off) [[unlikely]] {
        return Status::OK();
    }
    // [0]: maximum len trying to read, [1] maximum length buffer can provide, [2] actual len buffer has
    int64_t read_len = std::min({buf_len, _offset + _size - off, _offset + _len - off});
    memcpy(out, _buf.data() + (off - _offset), read_len);
    *bytes_read = read_len;
    if (off + *bytes_read == _offset + _len) {
        reset_offset(_offset + _whole_buffer_size);
    }
    return Status::OK();
}

void PrefetchBuffer::close() {
    std::unique_lock lck {_lock};
    // in case _reader still tries to write to the buf after we close the buffer
    _prefetched.wait(lck, [this]() { return _buffer_status != BufferStatus::PENDING; });
    _buffer_status = BufferStatus::CLOSED;
    _prefetched.notify_all();
}

// buffered reader
BufferedReader::BufferedReader(RuntimeProfile* profile, FileReader* reader, int64_t buffer_size)
        : _profile(profile), _reader(reader) {
    if (buffer_size == -1L) {
        buffer_size = config::remote_storage_read_buffer_mb * 1024 * 1024;
    }
    _whole_pre_buffer_size = buffer_size;
#ifdef BE_TEST
    s_max_pre_buffer_size = config::prefetch_single_buffer_size_mb;
#endif
    int buffer_num = buffer_size > s_max_pre_buffer_size ? buffer_size / s_max_pre_buffer_size : 1;
    // set the _cur_offset of this reader as same as the inner reader's,
    // to make sure the buffer reader will start to read at right position.
    _reader->tell(&_cur_offset);
    for (int i = 0; i < buffer_num; i++) {
        _pre_buffers.emplace_back(std::make_shared<PrefetchBuffer>(
                profile, _cur_offset + i * s_max_pre_buffer_size, s_max_pre_buffer_size,
                _whole_pre_buffer_size, _reader.get()));
    }
}

BufferedReader::~BufferedReader() {
    close();
    _closed = true;
}

Status BufferedReader::open() {
    if (!_reader) {
        return Status::InternalError("Open buffered reader failed, reader is null");
    }

    // the macro ADD_XXX is idempotent.
    // So although each scanner calls the ADD_XXX method, they all use the same counters.
    _read_timer = ADD_TIMER(_profile, "BufferedReaderFileReadTime");
    _remote_read_timer = ADD_TIMER(_profile, "FileRemoteReadTime");
    _read_counter = ADD_COUNTER(_profile, "FileReadCalls", TUnit::UNIT);
    _remote_read_counter = ADD_COUNTER(_profile, "FileRemoteReadCalls", TUnit::UNIT);
    _remote_read_bytes = ADD_COUNTER(_profile, "FileRemoteReadBytes", TUnit::BYTES);
    _remote_read_rate = _profile->add_derived_counter(
            "FileRemoteReadRate", TUnit::BYTES_PER_SECOND,
            std::bind<int64_t>(&RuntimeProfile::units_per_second, _remote_read_bytes, _read_timer),
            "");

    RETURN_IF_ERROR(_reader->open());
    _reader_size = _reader->size();
    // init all buffer and submit task
    resetAllBuffer(_cur_offset);
    return Status::OK();
}

//not support
Status BufferedReader::read_one_message(std::unique_ptr<uint8_t[]>* /*buf*/, int64_t* /*length*/) {
    return Status::NotSupported("Not support");
}

Status BufferedReader::read(uint8_t* buf, int64_t buf_len, int64_t* bytes_read, bool* eof) {
    DCHECK_NE(buf_len, 0);
    if (buf_len < 0 || _cur_offset < 0 || _cur_offset >= size()) [[unlikely]] {
        *eof = (_cur_offset >= _reader_size);
        return Status::OK();
    }
    int64_t actual_bytes_read = 0;
    RETURN_IF_ERROR(readat(_cur_offset, buf_len, &actual_bytes_read, buf));
    *eof = (actual_bytes_read == 0);
    _cur_offset += actual_bytes_read;
    *bytes_read = actual_bytes_read;
    return Status::OK();
}

Status BufferedReader::readat(int64_t position, int64_t nbytes, int64_t* bytes_read, void* out) {
    if (nbytes <= 0 || position < 0 || position >= size()) [[unlikely]] {
        *bytes_read = 0;
        return Status::OK();
    }
    SCOPED_TIMER(_read_timer);
    int actual_bytes_read = 0;
    while (actual_bytes_read < nbytes && position < _reader_size) {
        int64_t read_num = 0;
        auto buffer_pos = get_buffer_pos(position);
        RETURN_IF_ERROR(_pre_buffers[buffer_pos]->read_buffer(
                position, reinterpret_cast<uint8*>(out) + actual_bytes_read,
                nbytes - actual_bytes_read, &read_num));
        actual_bytes_read += read_num;
        position += read_num;
    }
    COUNTER_UPDATE(_read_counter, 1);
    *bytes_read = actual_bytes_read;
    return Status::OK();
}

int64_t BufferedReader::size() {
    return _reader_size;
}

Status BufferedReader::seek(int64_t position) {
    _cur_offset = position;
    return Status::OK();
}

Status BufferedReader::tell(int64_t* position) {
    *position = _cur_offset;
    return Status::OK();
}

void BufferedReader::close() {
    std::for_each(_pre_buffers.begin(), _pre_buffers.end(),
                  [](std::shared_ptr<PrefetchBuffer>& buffer) { buffer->close(); });
    _reader->close();

    if (_read_counter != nullptr) {
        COUNTER_UPDATE(_read_counter, _read_count);
    }
    if (_remote_read_counter != nullptr) {
        COUNTER_UPDATE(_remote_read_counter, _remote_read_count);
    }
    if (_remote_read_bytes != nullptr) {
        COUNTER_UPDATE(_remote_read_bytes, _remote_bytes);
    }
}

bool BufferedReader::closed() {
    return _reader->closed();
}

BufferedFileStreamReader::BufferedFileStreamReader(FileReader* file, uint64_t offset,
                                                   uint64_t length, size_t max_buf_size)
        : _file(file),
          _file_start_offset(offset),
          _file_end_offset(offset + length),
          _max_buf_size(max_buf_size) {}

Status BufferedFileStreamReader::read_bytes(const uint8_t** buf, uint64_t offset,
                                            const size_t bytes_to_read) {
    if (offset < _file_start_offset || offset >= _file_end_offset) {
        return Status::IOError("Out-of-bounds Access");
    }
    int64_t end_offset = offset + bytes_to_read;
    if (_buf_start_offset <= offset && _buf_end_offset >= end_offset) {
        *buf = _buf.get() + offset - _buf_start_offset;
        return Status::OK();
    }
    size_t buf_size = std::max(_max_buf_size, bytes_to_read);
    if (_buf_size < buf_size) {
        std::unique_ptr<uint8_t[]> new_buf(new uint8_t[buf_size]);
        if (offset >= _buf_start_offset && offset < _buf_end_offset) {
            memcpy(new_buf.get(), _buf.get() + offset - _buf_start_offset,
                   _buf_end_offset - offset);
        }
        _buf = std::move(new_buf);
        _buf_size = buf_size;
    } else if (offset > _buf_start_offset && offset < _buf_end_offset) {
        memmove(_buf.get(), _buf.get() + offset - _buf_start_offset, _buf_end_offset - offset);
    }
    if (offset < _buf_start_offset || offset >= _buf_end_offset) {
        _buf_end_offset = offset;
    }
    _buf_start_offset = offset;
    int64_t buf_remaining = _buf_end_offset - _buf_start_offset;
    int64_t to_read = std::min(_buf_size - buf_remaining, _file_end_offset - _buf_end_offset);
    int64_t has_read = 0;
    SCOPED_RAW_TIMER(&_statistics.read_time);
    while (has_read < to_read) {
        int64_t loop_read = 0;
        RETURN_IF_ERROR(_file->readat(_buf_end_offset + has_read, to_read - has_read, &loop_read,
                                      _buf.get() + buf_remaining + has_read));
        _statistics.read_calls++;
        if (loop_read <= 0) {
            break;
        }
        has_read += loop_read;
    }
    if (has_read != to_read) {
        return Status::Corruption("Try to read {} bytes, but received {} bytes", to_read, has_read);
    }
    _statistics.read_bytes += to_read;
    _buf_end_offset += to_read;
    *buf = _buf.get();
    return Status::OK();
}

Status BufferedFileStreamReader::read_bytes(Slice& slice, uint64_t offset) {
    return read_bytes((const uint8_t**)&slice.data, offset, slice.size);
}

} // namespace doris
