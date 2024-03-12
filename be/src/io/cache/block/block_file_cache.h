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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Interpreters/Cache/FileCache.h
// and modified by Doris

#pragma once

#include <bvar/bvar.h>
#include <gen_cpp/Types_types.h>
#include <stdint.h>

#include <cstddef>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "io/cache/block/block_file_cache_fwd.h"
#include "io/cache/block/block_file_cache_settings.h"
#include "io/cache/block/block_file_segment.h"
#include "io/io_common.h"
#include "util/lock.h"
#include "util/metrics.h"

namespace doris {
namespace io {

template <class Lock>
concept IsXLock = requires {
    std::same_as<Lock, std::lock_guard<doris::Mutex>> ||
            std::same_as<Lock, std::unique_lock<doris::Mutex>>;
};

using FileBlockSPtr = std::shared_ptr<FileBlock>;
using FileBlocks = std::list<FileBlockSPtr>;
struct FileBlocksHolder;

struct CacheContext {
    CacheContext(const IOContext* io_context) {
        if (io_context->read_segment_index) {
            cache_type = FileCacheType::INDEX;
        } else if (io_context->is_disposable) {
            cache_type = FileCacheType::DISPOSABLE;
        } else if (io_context->expiration_time != 0) {
            cache_type = FileCacheType::TTL;
            expiration_time = io_context->expiration_time;
        } else {
            cache_type = FileCacheType::NORMAL;
        }
        query_id = io_context->query_id ? *io_context->query_id : TUniqueId();
    }
    CacheContext() = default;

    bool operator==(const CacheContext& rhs) const {
        return query_id == rhs.query_id && cache_type == rhs.cache_type &&
               expiration_time == rhs.expiration_time && is_cold_data == rhs.is_cold_data;
    }

    TUniqueId query_id;
    FileCacheType cache_type;
    uint64_t expiration_time {0};
    bool is_cold_data {false};
};

/**
 * Local cache for remote filesystem files, represented as a set of non-overlapping non-empty file segments.
 */
class BlockFileCache {
    friend class FileBlock;
    friend struct FileBlocksHolder;

public:
    /// use version 2 when USE_CACHE_VERSION2 = true, while use version 1 if false
    /// version 1.0: cache_base_path / key / offset
    /// version 2.0: cache_base_path / key_prefix / key / offset
    static constexpr bool USE_CACHE_VERSION2 = false;
    static constexpr int KEY_PREFIX_LENGTH = 3;

    struct KeyHash {
        std::size_t operator()(const Key& k) const { return UInt128Hash()(k.key); }
    };

    BlockFileCache(const std::string& cache_base_path, const FileCacheSettings& cache_settings);

    ~BlockFileCache() {
        {
            std::lock_guard lock(_close_mtx);
            _close = true;
        }
        _close_cv.notify_all();
        if (_cache_background_thread.joinable()) {
            _cache_background_thread.join();
        }
        if (_cache_background_load_thread.joinable()) {
            _cache_background_load_thread.join();
        }
    };

    /// Restore cache from local filesystem.
    [[nodiscard]] Status initialize();

    /// Cache capacity in bytes.
    size_t capacity() const { return _total_size; }

    static Key hash(const std::string& path);

    std::string get_path_in_local_cache(const Key& key, uint64_t expiration_time, size_t offset,
                                        FileCacheType type, bool is_tmp = false) const;

    std::string get_path_in_local_cache(const Key& key, uint64_t expiration_time) const;

    const std::string& get_base_path() const { return _cache_base_path; }

    /**
     * Given an `offset` and `size` representing [offset, offset + size) bytes interval,
     * return list of cached non-overlapping non-empty
     * file segments `[segment1, ..., segmentN]` which intersect with given interval.
     *
     * Segments in returned list are ordered in ascending order and represent a full contiguous
     * interval (no holes). Each segment in returned list has state: DOWNLOADED, DOWNLOADING or EMPTY.
     *
     * As long as pointers to returned file segments are hold
     * it is guaranteed that these file segments are not removed from cache.
     */
    FileBlocksHolder get_or_set(const Key& key, size_t offset, size_t size,
                                const CacheContext& context);

    std::map<size_t, FileBlockSPtr> get_blocks_by_key(const Key& key);

    /// For debug.
    std::string dump_structure(const Key& key);

    size_t get_used_cache_size(FileCacheType type) const;

    size_t get_file_blocks_num(FileCacheType type) const;

    void change_cache_type(const Key& key, size_t offset, FileCacheType new_type,
                           std::lock_guard<doris::Mutex>& cache_lock);

    static std::string cache_type_to_string(FileCacheType type);
    static FileCacheType string_to_cache_type(const std::string& str);

    void remove_if_cached(const Key&);
    void modify_expiration_time(const Key&, int64_t new_expiration_time);

    void reset_range(const Key&, size_t offset, size_t old_size, size_t new_size,
                     std::lock_guard<doris::Mutex>& cache_lock);

    std::vector<std::tuple<size_t, size_t, FileCacheType, int64_t>> get_hot_segments_meta(
            const Key& key) const;

    void clear_file_cache_async();
    // use for test
    Status clear_file_cache_directly();

    bool get_lazy_open_success() { return _lazy_open_done; }

    void update_ttl_atime(const Key& file_key);

    BlockFileCache& operator=(const BlockFileCache&) = delete;
    BlockFileCache(const BlockFileCache&) = delete;

protected:
    std::string _cache_base_path;
    size_t _total_size = 0;
    size_t _max_file_block_size = 0;
    size_t _max_query_cache_size = 0;
    // metrics
    std::shared_ptr<bvar::Status<size_t>> _cur_cache_size_metrics;
    std::shared_ptr<bvar::Status<size_t>> _cur_ttl_cache_size_metrics;
    std::shared_ptr<bvar::Status<size_t>> _cur_normal_queue_element_count_metrics;
    std::shared_ptr<bvar::Status<size_t>> _cur_normal_queue_cache_size_metrics;
    std::shared_ptr<bvar::Status<size_t>> _cur_index_queue_element_count_metrics;
    std::shared_ptr<bvar::Status<size_t>> _cur_index_queue_cache_size_metrics;
    std::shared_ptr<bvar::Status<size_t>> _cur_disposable_queue_element_count_metrics;
    std::shared_ptr<bvar::Status<size_t>> _cur_disposable_queue_cache_size_metrics;
    std::array<std::shared_ptr<bvar::Adder<size_t>>, 4> _queue_evict_size_metrics;
    std::shared_ptr<bvar::Adder<size_t>> _total_evict_size_metrics;

    mutable doris::Mutex _mutex;

    bool try_reserve(const Key& key, const CacheContext& context, size_t offset, size_t size,
                     std::lock_guard<doris::Mutex>& cache_lock);

    template <class T, class U>
        requires IsXLock<T> && IsXLock<U>
    void remove(FileBlockSPtr file_block, T& cache_lock, U& segment_lock);

    class LRUQueue {
    public:
        LRUQueue() = default;
        LRUQueue(size_t max_size, size_t max_element_size, int64_t hot_data_interval)
                : max_size(max_size),
                  max_element_size(max_element_size),
                  hot_data_interval(hot_data_interval) {}

        struct HashFileKeyAndOffset {
            std::size_t operator()(const std::pair<Key, size_t>& pair) const {
                return KeyHash()(pair.first) + pair.second;
            }
        };

        struct FileKeyAndOffset {
            Key key;
            size_t offset;
            size_t size;

            FileKeyAndOffset(const Key& key, size_t offset, size_t size)
                    : key(key), offset(offset), size(size) {}
        };

        using Iterator = typename std::list<FileKeyAndOffset>::iterator;

        size_t get_max_size() const { return max_size; }
        size_t get_max_element_size() const { return max_element_size; }

        template <class T>
            requires IsXLock<T>
        size_t get_total_cache_size(T& /* cache_lock */) const {
            return cache_size;
        }

        size_t get_elements_num(std::lock_guard<doris::Mutex>& /* cache_lock */) const {
            return queue.size();
        }

        Iterator add(const Key& key, size_t offset, size_t size,
                     std::lock_guard<doris::Mutex>& cache_lock);

        template <class T>
            requires IsXLock<T>
        void remove(Iterator queue_it, T& cache_lock);

        void move_to_end(Iterator queue_it, std::lock_guard<doris::Mutex>& cache_lock);

        std::string to_string(std::lock_guard<doris::Mutex>& cache_lock) const;

        bool contains(const Key& key, size_t offset,
                      std::lock_guard<doris::Mutex>& cache_lock) const;

        Iterator begin() { return queue.begin(); }

        Iterator end() { return queue.end(); }

        Iterator get(const Key& key, size_t offset,
                     std::lock_guard<doris::Mutex>& /* cache_lock */) const;

        int64_t get_hot_data_interval() const { return hot_data_interval; }

        void clear(std::lock_guard<doris::Mutex>& cache_lock) {
            queue.clear();
            map.clear();
            cache_size = 0;
        }

        size_t max_size;
        size_t max_element_size;
        std::list<FileKeyAndOffset> queue;
        std::unordered_map<std::pair<Key, size_t>, Iterator, HashFileKeyAndOffset> map;
        size_t cache_size = 0;
        int64_t hot_data_interval {0};
    };

    using AccessKeyAndOffset = std::pair<Key, size_t>;
    struct KeyAndOffsetHash {
        std::size_t operator()(const AccessKeyAndOffset& key) const {
            return UInt128Hash()(std::get<0>(key).key) ^ std::hash<uint64_t>()(std::get<1>(key));
        }
    };

    using AccessRecord =
            std::unordered_map<AccessKeyAndOffset, LRUQueue::Iterator, KeyAndOffsetHash>;

    /// Used to track and control the cache access of each query.
    /// Through it, we can realize the processing of different queries by the cache layer.
    struct QueryFileCacheContext {
        LRUQueue lru_queue;
        AccessRecord records;

        QueryFileCacheContext(size_t max_cache_size) : lru_queue(max_cache_size, 0, 0) {}

        void remove(const Key& key, size_t offset, std::lock_guard<doris::Mutex>& cache_lock);

        void reserve(const Key& key, size_t offset, size_t size,
                     std::lock_guard<doris::Mutex>& cache_lock);

        size_t get_max_cache_size() const { return lru_queue.get_max_size(); }

        size_t get_cache_size(std::lock_guard<doris::Mutex>& cache_lock) const {
            return lru_queue.get_total_cache_size(cache_lock);
        }

        LRUQueue& queue() { return lru_queue; }
    };

    using QueryFileCacheContextPtr = std::shared_ptr<QueryFileCacheContext>;
    using QueryFileCacheContextMap = std::unordered_map<TUniqueId, QueryFileCacheContextPtr>;

    QueryFileCacheContextMap _query_map;

    bool _enable_file_cache_query_limit = config::enable_file_cache_query_limit;

    QueryFileCacheContextPtr get_query_context(const TUniqueId& query_id,
                                               std::lock_guard<doris::Mutex>&);

    void remove_query_context(const TUniqueId& query_id);

    QueryFileCacheContextPtr get_or_set_query_context(const TUniqueId& query_id,
                                                      std::lock_guard<doris::Mutex>&);

public:
    /// Save a query context information, and adopt different cache policies
    /// for different queries through the context cache layer.
    struct QueryFileCacheContextHolder {
        QueryFileCacheContextHolder(const TUniqueId& query_id, BlockFileCache* cache,
                                    QueryFileCacheContextPtr context)
                : query_id(query_id), cache(cache), context(context) {}

        QueryFileCacheContextHolder& operator=(const QueryFileCacheContextHolder&) = delete;
        QueryFileCacheContextHolder(const QueryFileCacheContextHolder&) = delete;

        ~QueryFileCacheContextHolder() {
            /// If only the query_map and the current holder hold the context_query,
            /// the query has been completed and the query_context is released.
            if (context) {
                context.reset();
                cache->remove_query_context(query_id);
            }
        }

        const TUniqueId& query_id;
        BlockFileCache* cache = nullptr;
        QueryFileCacheContextPtr context;
    };
    using QueryFileCacheContextHolderPtr = std::unique_ptr<QueryFileCacheContextHolder>;
    QueryFileCacheContextHolderPtr get_query_context_holder(const TUniqueId& query_id);

private:
    struct FileBlockCell {
        FileBlockSPtr file_block;
        FileCacheType cache_type;

        /// Iterator is put here on first reservation attempt, if successful.
        std::optional<LRUQueue::Iterator> queue_iterator;

        mutable int64_t atime {0};
        mutable bool is_deleted {false};
        void update_atime() const {
            atime = std::chrono::duration_cast<std::chrono::seconds>(
                            std::chrono::steady_clock::now().time_since_epoch())
                            .count();
        }

        /// Pointer to file block is always hold by the cache itself.
        /// Apart from pointer in cache, it can be hold by cache users, when they call
        /// getorSet(), but cache users always hold it via FileBlocksHolder.
        bool releasable() const { return file_block.unique(); }

        size_t size() const { return file_block->_segment_range.size(); }

        FileBlockCell(FileBlockSPtr file_block, FileCacheType cache_type,
                      std::lock_guard<doris::Mutex>& cache_lock);

        FileBlockCell(FileBlockCell&& other) noexcept
                : file_block(std::move(other.file_block)),
                  cache_type(other.cache_type),
                  queue_iterator(other.queue_iterator),
                  atime(other.atime) {}

        FileBlockCell& operator=(const FileBlockCell&) = delete;
        FileBlockCell(const FileBlockCell&) = delete;
    };

    using FileBlocksByOffset = std::map<size_t, FileBlockCell>;

    struct HashCachedFileKey {
        std::size_t operator()(const Key& k) const { return KeyHash()(k); }
    };

    using CachedFiles = std::unordered_map<Key, FileBlocksByOffset, HashCachedFileKey>;

    CachedFiles _files;
    size_t _cur_cache_size = 0;
    std::multimap<uint64_t, Key> _time_to_key;
    std::unordered_map<Key, uint64_t, HashCachedFileKey> _key_to_time;

    // The three queues are level queue.
    // It means as level1/level2/level3 queue.
    // but the level2 is maximum.
    // If some datas are importance, we can cache it into index queue
    // If some datas are just use once, we can cache it into disposable queue
    // The size proportion is [1:17:2].
    LRUQueue _index_queue;
    LRUQueue _normal_queue;
    LRUQueue _disposable_queue;

    BlockFileCache::LRUQueue& get_queue(FileCacheType type);
    const BlockFileCache::LRUQueue& get_queue(FileCacheType type) const;

    FileBlocks get_impl(const Key& key, const CacheContext& context, const FileBlock::Range& range,
                        std::lock_guard<doris::Mutex>& cache_lock);

    template <class T>
        requires IsXLock<T>
    FileBlockCell* get_cell(const Key& key, size_t offset, T& cache_lock);

    FileBlockCell* add_cell(const Key& key, const CacheContext& context, size_t offset, size_t size,
                            FileBlock::State state, std::lock_guard<doris::Mutex>& cache_lock);

    void use_cell(const FileBlockCell& cell, FileBlocks* result, bool not_need_move,
                  std::lock_guard<doris::Mutex>& cache_lock);

    bool try_reserve_for_lru(const Key& key, QueryFileCacheContextPtr query_context,
                             const CacheContext& context, size_t offset, size_t size,
                             std::lock_guard<doris::Mutex>& cache_lock);

    // other_cache_types: 除本次申请空间的类型的其他所有类型
    // size: 需要的大小
    // cur_time: 当前访问的绝对时间，单位s
    // return: 淘汰成功与否
    bool try_reserve_from_other_queue_by_hot_interval(std::vector<FileCacheType> other_cache_types,
                                                      size_t size, int64_t cur_time,
                                                      std::lock_guard<doris::Mutex>& cache_lock);

    // other_cache_types: 除本次申请空间的类型的其他所有类型
    // size: 需要的大小
    // return: 淘汰成功与否
    bool try_reserve_from_other_queue_by_size(std::vector<FileCacheType> other_cache_types,
                                              size_t size,
                                              std::lock_guard<doris::Mutex>& cache_lock);

    bool try_reserve_for_lazy_load(size_t size, std::lock_guard<doris::Mutex>& cache_lock);

    std::vector<FileCacheType> get_other_cache_type(FileCacheType cur_cache_type);

    bool try_reserve_from_other_queue(FileCacheType cur_cache_type, size_t offset, int64_t cur_time,
                                      std::lock_guard<doris::Mutex>& cache_lock);

    void load_cache_info_into_memory();

    bool try_reserve_for_ttl(size_t size, std::lock_guard<doris::Mutex>& cache_lock);

    FileBlocks split_range_into_cells(const Key& key, const CacheContext& context, size_t offset,
                                      size_t size, FileBlock::State state,
                                      std::lock_guard<doris::Mutex>& cache_lock);

    std::string dump_structure_unlocked(const Key& key, std::lock_guard<doris::Mutex>& cache_lock);

    void fill_holes_with_empty_file_blocks(FileBlocks& file_blocks, const Key& key,
                                           const CacheContext& context,
                                           const FileBlock::Range& range,
                                           std::lock_guard<doris::Mutex>& cache_lock);

    size_t get_used_cache_size_unlocked(FileCacheType type,
                                        std::lock_guard<doris::Mutex>& cache_lock) const;

    void check_disk_resource_limit(const std::string& path);

    size_t get_file_blocks_num_unlocked(FileCacheType type,
                                        std::lock_guard<doris::Mutex>& cache_lock) const;

    bool need_to_move(FileCacheType cell_type, FileCacheType query_type) const;

    bool remove_if_ttl_file_unlock(const Key& file_key, bool remove_directly,
                                   std::lock_guard<doris::Mutex>&);

    void run_background_operation();

    Status initialize_unlocked(std::lock_guard<doris::Mutex>&);

    void recycle_deleted_blocks();

    bool is_overflow(size_t removed_size, size_t need_size, size_t cur_cache_size);

    struct BatchLoadArgs {
        Key key;
        CacheContext ctx;
        uint64_t offset;
        size_t size;
        std::string key_path;
        std::string offset_path;
        bool is_tmp;
    };

    bool _close {false};
    doris::Mutex _close_mtx;
    doris::ConditionVariable _close_cv;
    std::thread _cache_background_thread;
    std::atomic_bool _lazy_open_done {false};
    std::thread _cache_background_load_thread;
    // disk space or inode is less than the specified value
    std::atomic_bool _disk_resource_limit_mode {false};
    bool _async_clear_file_cache {false};

private:
    static inline std::list<std::pair<AccessKeyAndOffset, std::shared_ptr<FileReader>>>
            s_file_reader_cache;
    static inline std::unordered_map<AccessKeyAndOffset, decltype(s_file_reader_cache.begin()),
                                     KeyAndOffsetHash>
            s_file_name_to_reader;
    static inline doris::Mutex s_file_reader_cache_mtx;
    static inline uint64_t _max_file_reader_cache_size = 65533;

public:
    static std::weak_ptr<FileReader> cache_file_reader(const AccessKeyAndOffset& key,
                                                       std::shared_ptr<FileReader> file_reader);

    static void remove_file_reader(const AccessKeyAndOffset& key);

    // use for test
    static bool contains_file_reader(const AccessKeyAndOffset& key);
    static size_t file_reader_cache_size();
};

using BlockFileCachePtr = BlockFileCache*;

struct KeyHash {
    std::size_t operator()(const Key& k) const { return UInt128Hash()(k.key); }
};

} // namespace io
} // namespace doris
