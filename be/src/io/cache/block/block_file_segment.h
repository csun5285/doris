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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Interpreters/Cache/FileSegment.h
// and modified by Doris

#pragma once

#include <fmt/format.h>
#include <stddef.h>

#include <atomic>
#include <condition_variable>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <utility>

#include "common/status.h"
#include "io/cache/block/block_file_cache_fwd.h"
#include "io/fs/file_writer.h"
#include "util/lock.h"
#include "util/slice.h"

namespace doris {
namespace io {
class BlockFileCache;
class FileBlock;
class FileReader;

using FileBlockSPtr = std::shared_ptr<FileBlock>;
using FileBlocks = std::list<FileBlockSPtr>;

class FileBlock {
    friend class BlockFileCache;
    friend struct FileBlocksHolder;
    friend class CachedRemoteFileReader;

public:
    using LocalWriterPtr = std::unique_ptr<FileWriter>;
    using LocalReaderPtr = std::weak_ptr<FileReader>;

    enum class State {
        DOWNLOADED,
        /**
         * When file segment is first created and returned to user, it has state EMPTY.
         * EMPTY state can become DOWNLOADING when getOrSetDownaloder is called successfully
         * by any owner of EMPTY state file segment.
         */
        EMPTY,
        /**
         * A newly created file segment never has DOWNLOADING state until call to getOrSetDownloader
         * because each cache user might acquire multiple file segments and reads them one by one,
         * so only user which actually needs to read this segment earlier than others - becomes a downloader.
         */
        DOWNLOADING,
        SKIP_CACHE,
    };

    FileBlock(size_t offset, size_t size, const Key& key, BlockFileCache* cache,
              State download_state, FileCacheType cache_type, uint64_t expiration_time);

    ~FileBlock();

    State state() const;

    static std::string state_to_string(FileBlock::State state);

    /// Represents an interval [left, right] including both boundaries.
    struct Range {
        size_t left;
        size_t right;

        Range(size_t left, size_t right) : left(left), right(right) {}

        [[nodiscard]] size_t size() const { return right - left + 1; }

        [[nodiscard]] std::string to_string() const {
            return fmt::format("[{}, {}]", std::to_string(left), std::to_string(right));
        }
    };

    const Range& range() const { return _segment_range; }

    const Key& key() const { return _file_key; }

    size_t offset() const { return range().left; }

    State wait();

    // append data to cache file
    [[nodiscard]] Status append(Slice data);

    // read data from cache file
    [[nodiscard]] Status read_at(Slice buffer, size_t read_offset);

    // finish write, release the file writer
    [[nodiscard]] Status finalize_write();

    // set downloader if state == EMPTY
    uint64_t get_or_set_downloader();

    uint64_t get_downloader() const;

    void reset_downloader(std::lock_guard<doris::Mutex>& segment_lock);

    bool is_downloader() const;

    bool is_downloaded() const { return _is_downloaded.load(); }

    FileCacheType cache_type() const { return _cache_type; }

    static uint64_t get_caller_id();

    std::string get_info_for_log() const;

    std::string get_path_in_local_cache(bool is_tmp = false) const;

    bool change_cache_type(FileCacheType new_type);

    void change_cache_type_self(FileCacheType new_type);

    void update_expiration_time(uint64_t expiration_time) { _expiration_time = expiration_time; }

    uint64_t expiration_time() const { return _expiration_time; }

    State state_unlock(std::lock_guard<doris::Mutex>&) const;

    FileBlock& operator=(const FileBlock&) = delete;
    FileBlock(const FileBlock&) = delete;

private:
    size_t get_downloaded_size(std::lock_guard<doris::Mutex>& segment_lock) const;
    std::string get_info_for_log_impl(std::lock_guard<doris::Mutex>& segment_lock) const;

    [[nodiscard]] Status set_downloaded(std::lock_guard<doris::Mutex>& segment_lock);
    bool is_downloader_impl(std::lock_guard<doris::Mutex>& segment_lock) const;

    void complete_unlocked(std::lock_guard<doris::Mutex>& segment_lock);

    void reset_downloader_impl(std::lock_guard<doris::Mutex>& segment_lock);

    Range _segment_range;

    State _download_state;

    uint64_t _downloader_id = 0;

    LocalWriterPtr _cache_writer;
    LocalReaderPtr _cache_reader;

    size_t _downloaded_size = 0;

    /// global locking order rule:
    /// 1. cache lock
    /// 2. segment lock

    mutable doris::Mutex _mutex;
    doris::ConditionVariable _cv;

    /// Protects downloaded_size access with actual write into fs.
    /// downloaded_size is not protected by download_mutex in methods which
    /// can never be run in parallel to FileBlock::write() method
    /// as downloaded_size is updated only in FileBlock::write() method.
    /// Such methods are identified by isDownloader() check at their start,
    /// e.g. they are executed strictly by the same thread, sequentially.
    mutable doris::Mutex _download_mutex;

    Key _file_key;
    BlockFileCache* _cache;

    std::atomic<bool> _is_downloaded {false};
    FileCacheType _cache_type;
    int64_t _expiration_time {0};
};
extern std::ostream& operator<<(std::ostream& os, const FileBlock::State& value);

struct FileBlocksHolder {
    explicit FileBlocksHolder(FileBlocks&& file_segments_)
            : file_segments(std::move(file_segments_)) {}

    FileBlocksHolder(FileBlocksHolder&& other) noexcept
            : file_segments(std::move(other.file_segments)) {}

    FileBlocksHolder& operator=(const FileBlocksHolder&) = delete;
    FileBlocksHolder(const FileBlocksHolder&) = delete;

    ~FileBlocksHolder();

    FileBlocks file_segments {};
};

using FileBlocksHolderPtr = std::unique_ptr<FileBlocksHolder>;

} // namespace io
} // namespace doris
