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

#include <stdint.h>

#include <memory>

#include "common/config.h"
#include "common/status.h"
#include "io/file_reader.h"
#include "util/runtime_profile.h"
#include "util/threadpool.h"

namespace doris {

class BufferedReader;
struct PrefetchBuffer : std::enable_shared_from_this<PrefetchBuffer> {
    enum class BufferStatus { RESET, PENDING, PREFETCHED, CLOSED };
    PrefetchBuffer() = default;
    PrefetchBuffer(RuntimeProfile* profile, int64_t offset, int64_t buffer_size,
                   int64_t whole_buffer_size, FileReader* reader)
            : _offset(offset),
              _size(buffer_size),
              _whole_buffer_size(whole_buffer_size),
              _reader(reader),
              _buf(buffer_size, '0') {
        _remote_read_timer = ADD_TIMER(profile, "FileRemoteReadTime");
        _remote_read_counter = ADD_COUNTER(profile, "FileRemoteReadCalls", TUnit::UNIT);
        _remote_read_bytes = ADD_COUNTER(profile, "FileRemoteReadBytes", TUnit::BYTES);
        _remote_buffer_reset_conter =
                ADD_COUNTER(profile, "FileRemoteBufferResetTimes", TUnit::UNIT);
    }
    PrefetchBuffer(PrefetchBuffer&& other)
            : _offset(other._offset),
              _size(other._size),
              _whole_buffer_size(other._whole_buffer_size),
              _reader(other._reader),
              _buf(std::move(other._buf)),
              _remote_read_timer(other._remote_read_timer),
              _remote_read_counter(other._remote_read_counter),
              _remote_read_bytes(other._remote_read_bytes) {}
    ~PrefetchBuffer() = default;
    int64_t _offset;
    int64_t _size;
    int64_t _len {0};
    int64_t _whole_buffer_size;
    FileReader* _reader;
    std::string _buf;
    BufferStatus _buffer_status {BufferStatus::RESET};
    std::mutex _lock;
    std::condition_variable _prefetched;
    Status _prefetch_status {Status::OK()};
    // time cost of "_reader", "remote" because "_reader" is always a remote reader
    RuntimeProfile::Counter* _remote_read_timer = nullptr;
    // counter of calling "remote read()"
    RuntimeProfile::Counter* _remote_read_counter = nullptr;
    RuntimeProfile::Counter* _remote_read_bytes = nullptr;
    // counter of reset buffer
    RuntimeProfile::Counter* _remote_buffer_reset_conter = nullptr;
    // @brief: reset the start offset of this buffer to offset
    // @param: the new start offset for this buffer
    void reset_offset(int64_t offset);
    // @brief: start to fetch the content between [_offset, _offset + _size)
    void prefetch_buffer();
    // @brief: used by BufferedReader to read the prefetched data
    // @param[off] read start address
    // @param[buf] buffer to put the actual content
    // @param[buf_len] maximum len trying to read
    // @param[bytes_read] actual bytes read
    Status read_buffer(int64_t off, uint8_t* buf, int64_t buf_len, int64_t* bytes_read);
    // @brief: shut down the buffer until the prior prefetching task is done
    void close();
    // @brief: to detect whether this buffer contains off
    // @param[off] detect offset
    bool inline contains(int64_t off) const { return _offset <= off && off < _offset + _size; }
};

// Buffered Reader
// Add a cache layer between the caller and the file reader to reduce the
// times of calls to the read function to speed up.
// **Attention**: make sure the inner reader is thread safe
class BufferedReader : public FileReader {
public:
    // If the reader need the file size, set it when construct FileReader.
    // There is no other way to set the file size.
    // buffered_reader will acquire reader
    // -1 means using config buffered_reader_buffer_size_bytes
    BufferedReader(RuntimeProfile* profile, FileReader* reader, int64_t = -1L);
    virtual ~BufferedReader();

    virtual Status open() override;

    // Read
    virtual Status read(uint8_t* buf, int64_t buf_len, int64_t* bytes_read, bool* eof) override;
    virtual Status readat(int64_t position, int64_t nbytes, int64_t* bytes_read,
                          void* out) override;
    virtual Status read_one_message(std::unique_ptr<uint8_t[]>* buf, int64_t* length) override;
    virtual int64_t size() override;
    virtual Status seek(int64_t position) override;
    virtual Status tell(int64_t* position) override;
    virtual void close() override;
    virtual bool closed() override;

private:
    size_t get_buffer_pos(int64_t position) const {
        return (position % _whole_pre_buffer_size) / s_max_pre_buffer_size;
    }
    size_t get_buffer_offset(int64_t position) const {
        return (position / s_max_pre_buffer_size) * s_max_pre_buffer_size;
    }
    void resetAllBuffer(size_t position) {
        for (int64_t i = 0; i < _pre_buffers.size(); i++) {
            int64_t cur_pos = position + i * s_max_pre_buffer_size;
            int cur_buf_pos = get_buffer_pos(cur_pos);
            // reset would do all the prefetch work
            _pre_buffers[cur_buf_pos]->reset_offset(get_buffer_offset(cur_pos));
        }
    }

    RuntimeProfile* _profile;
    std::unique_ptr<FileReader> _reader;

    int64_t _read_count = 0;
    int64_t _remote_read_count = 0;
    int64_t _remote_bytes = 0;

    // total time cost in this reader
    RuntimeProfile::Counter* _read_timer = nullptr;
    // time cost of "_reader", "remote" because "_reader" is always a remote reader
    RuntimeProfile::Counter* _remote_read_timer = nullptr;
    // counter of calling read()
    RuntimeProfile::Counter* _read_counter = nullptr;
    // counter of calling "remote read()"
    RuntimeProfile::Counter* _remote_read_counter = nullptr;
    RuntimeProfile::Counter* _remote_read_bytes = nullptr;
    RuntimeProfile::Counter* _remote_read_rate = nullptr;

    int64_t s_max_pre_buffer_size = config::prefetch_single_buffer_size_mb * 1024 * 1024;
    std::vector<std::shared_ptr<PrefetchBuffer>> _pre_buffers;
    int64_t _whole_pre_buffer_size;
    std::atomic_bool _closed {false};
    int64_t _cur_offset {0};
    int64_t _reader_size {0};
};

/**
 * Load all the needed data in underlying buffer, so the caller does not need to prepare the data container.
 */
class BufferedStreamReader {
public:
    struct Statistics {
        int64_t read_time = 0;
        int64_t read_calls = 0;
        int64_t read_bytes = 0;
    };

    /**
     * Return the address of underlying buffer that locates the start of data between [offset, offset + bytes_to_read)
     * @param buf the buffer address to save the start address of data
     * @param offset start offset ot read in stream
     * @param bytes_to_read bytes to read
     */
    virtual Status read_bytes(const uint8_t** buf, uint64_t offset, const size_t bytes_to_read) = 0;
    /**
     * Save the data address to slice.data, and the slice.size is the bytes to read.
     */
    virtual Status read_bytes(Slice& slice, uint64_t offset) = 0;
    Statistics& statistics() { return _statistics; }
    virtual ~BufferedStreamReader() = default;

protected:
    Statistics _statistics;
};

class BufferedFileStreamReader : public BufferedStreamReader {
public:
    BufferedFileStreamReader(FileReader* file, uint64_t offset, uint64_t length,
                             size_t max_buf_size);
    ~BufferedFileStreamReader() override = default;

    Status read_bytes(const uint8_t** buf, uint64_t offset, const size_t bytes_to_read) override;
    Status read_bytes(Slice& slice, uint64_t offset) override;

private:
    std::unique_ptr<uint8_t[]> _buf;
    FileReader* _file;
    uint64_t _file_start_offset;
    uint64_t _file_end_offset;

    uint64_t _buf_start_offset = 0;
    uint64_t _buf_end_offset = 0;
    size_t _buf_size = 0;
    size_t _max_buf_size;
};

} // namespace doris
