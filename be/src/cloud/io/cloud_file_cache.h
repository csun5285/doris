#pragma once

#include <bvar/bvar.h>

#include <array>
#include <atomic>
#include <chrono>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <unordered_map>

#include "cloud/io/cloud_file_cache_fwd.h"
#include "cloud/io/cloud_file_cache_settings.h"
#include "cloud/io/cloud_file_segment.h"
#include "cloud/io/file_reader.h"
#include "common/config.h"
#include "util/lock.h"

namespace doris {
namespace io {

struct CacheContext {
    static CacheContext create(IOState* state) {
        CacheContext context;
        if (state->read_segment_index) {
            context.cache_type = CacheType::INDEX;
        } else if (state->is_disposable) {
            context.cache_type = CacheType::DISPOSABLE;
        } else if (state->expiration_time != 0) {
            context.cache_type = CacheType::TTL;
            context.expiration_time = state->expiration_time;
        } else {
            context.cache_type = CacheType::NORMAL;
        }
        context.query_id = state->query_id ? *state->query_id : TUniqueId();
        return context;
    }
    CacheContext() = default;
    TUniqueId query_id;
    CacheType cache_type;
    int64_t expiration_time {0};
    bool is_cold_data {false};
};

/**
 * Local cache for remote filesystem files, represented as a set of non-overlapping file segments.
 */
class CloudFileCache {
    friend class FileSegment;
    friend struct FileSegmentsHolder;

public:
    struct KeyHash {
        std::size_t operator()(const Key& k) const { return UInt128Hash()(k.key); }
    };

    CloudFileCache(const std::string& cache_base_path, const FileCacheSettings& cache_settings);

    ~CloudFileCache() {
        _close = true;
        if (_cache_background_thread.joinable()) {
            _cache_background_thread.join();
        }
    }

    /// Restore cache from local filesystem.
    Status initialize();

    /// Cache capacity in bytes.
    size_t capacity() const { return _total_size; }

    static Key hash(const std::string& path);

    std::string get_path_in_local_cache(const Key& key, int64_t expiration_time, size_t offset,
                                        CacheType type, bool is_tmp = false) const;

    std::string get_path_in_local_cache(const Key& key, int64_t expiration_time) const;

    const std::string& get_base_path() const { return _cache_base_path; }

    /**
     * Given an `offset` and `size` representing [offset, offset + size) bytes interval,
     * return list of cached non-overlapping
     * file segments `[segment1, ..., segmentN]` which intersect with given interval.
     *
     * Segments in returned list are ordered in ascending order and represent a full contiguous
     * interval (no holes). Each segment in returned list has state: DOWNLOADED, DOWNLOADING or EMPTY.
     *
     * As long as pointers to returned file segments are hold
     * it is guaranteed that these file segments are not removed from cache.
     */
    FileSegmentsHolder get_or_set(const Key& key, size_t offset, size_t size,
                                  const CacheContext& context);

    /// For debug.
    std::string dump_structure(const Key& key);

    size_t get_used_cache_size(CacheType type) const;

    size_t get_file_segments_num(CacheType type) const;

    void change_cache_type(const Key& key, size_t offset, CacheType new_type);

    static std::string cache_type_to_string(CacheType type);
    static CacheType string_to_cache_type(const std::string& str);

    void remove_if_cached(const Key&);
    void modify_expiration_time(const Key&, int64_t new_expiration_time);

    void reset_range(const Key&, size_t offset, size_t old_size, size_t new_size);

    std::vector<std::tuple<size_t, size_t, CacheType, int64_t>> get_hot_segments_meta(
            const Key& key) const;

    // when cache change to read-write  from read-only, it need reinitialize
    Status reinitialize();

    CloudFileCache& operator=(const CloudFileCache&) = delete;
    CloudFileCache(const CloudFileCache&) = delete;

private:
    std::string _cache_base_path;
    size_t _total_size = 0;
    size_t _max_file_segment_size = 0;
    size_t _max_query_cache_size = 0;

    bool _is_initialized = false;

    mutable doris::Mutex _mutex;

    // metrics
    std::shared_ptr<bvar::Status<size_t>> _cur_size_metrics;
    std::shared_ptr<bvar::Status<size_t>> _cur_ttl_cache_size_metrics;

    bool try_reserve(const Key& key, const CacheContext& context, size_t offset, size_t size,
                     std::lock_guard<doris::Mutex>& cache_lock);

    void remove(FileSegmentSPtr file_segment, std::lock_guard<doris::Mutex>& cache_lock,
                std::lock_guard<doris::Mutex>& segment_lock);

    struct LRUQueue {
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

        size_t get_total_cache_size(std::lock_guard<doris::Mutex>& /* cache_lock */) const {
            return cache_size;
        }

        size_t get_elements_num(std::lock_guard<doris::Mutex>& /* cache_lock */) const {
            return queue.size();
        }

        Iterator add(const Key& key, size_t offset, size_t size,
                     std::lock_guard<doris::Mutex>& cache_lock);

        void remove(Iterator queue_it, std::lock_guard<doris::Mutex>& cache_lock);

        void move_to_end(Iterator queue_it, std::lock_guard<doris::Mutex>& cache_lock);

        std::string to_string(std::lock_guard<doris::Mutex>& cache_lock) const;

        bool contains(const Key& key, size_t offset,
                      std::lock_guard<doris::Mutex>& cache_lock) const;

        Iterator begin() { return queue.begin(); }

        Iterator end() { return queue.end(); }

        void remove_all(std::lock_guard<doris::Mutex>& cache_lock);

        Iterator get(const Key& key, size_t offset,
                     std::lock_guard<doris::Mutex>& /* cache_lock */) const;

        int64_t get_hot_data_interval() const { return hot_data_interval; }

        size_t max_size;
        size_t max_element_size;
        std::list<FileKeyAndOffset> queue;
        std::unordered_map<std::pair<Key, size_t>, Iterator, HashFileKeyAndOffset> map;
        size_t cache_size {0};
        int64_t hot_data_interval {0};
    };

    struct FileSegmentCell {
        FileSegmentSPtr file_segment;
        CacheType cache_type;

        /// Iterator is put here on first reservation attempt, if successful.
        std::optional<LRUQueue::Iterator> queue_iterator;

        mutable int64_t atime {0};
        void update_atime() const {
            atime = std::chrono::duration_cast<std::chrono::seconds>(
                            std::chrono::steady_clock::now().time_since_epoch())
                            .count();
        }

        /// Pointer to file segment is always hold by the cache itself.
        /// Apart from pointer in cache, it can be hold by cache users, when they call
        /// getorSet(), but cache users always hold it via FileSegmentsHolder.
        bool releasable() const { return file_segment.unique(); }

        size_t size() const { return file_segment->_segment_range.size(); }

        FileSegmentCell(FileSegmentSPtr file_segment, CacheType cache_type,
                        std::lock_guard<doris::Mutex>& cache_lock);

        FileSegmentCell(FileSegmentCell&& other) noexcept
                : file_segment(std::move(other.file_segment)),
                  cache_type(other.cache_type),
                  queue_iterator(other.queue_iterator),
                  atime(other.atime) {}

        FileSegmentCell& operator=(const FileSegmentCell&) = delete;
        FileSegmentCell(const FileSegmentCell&) = delete;
    };

    using AccessKeyAndOffset = std::tuple<Key, size_t>;
    struct KeyAndOffsetHash {
        std::size_t operator()(const AccessKeyAndOffset& key) const {
            return UInt128Hash()(std::get<0>(key).key) ^ std::hash<uint64_t>()(std::get<1>(key));
        }
    };

    using AccessRecord =
            std::unordered_map<AccessKeyAndOffset, LRUQueue::Iterator, KeyAndOffsetHash>;

    /// Used to track and control the cache access of each query.
    /// Through it, we can realize the processing of different queries by the cache layer.
    struct QueryContext {
        LRUQueue lru_queue;
        AccessRecord records;

        QueryContext(size_t max_cache_size) : lru_queue(max_cache_size, 0, 0) {}

        void remove(const Key& key, size_t offset, std::lock_guard<doris::Mutex>& cache_lock);

        void reserve(const Key& key, size_t offset, size_t size,
                     std::lock_guard<doris::Mutex>& cache_lock);

        size_t get_max_cache_size() const { return lru_queue.get_max_size(); }

        size_t get_cache_size(std::lock_guard<doris::Mutex>& cache_lock) const {
            return lru_queue.get_total_cache_size(cache_lock);
        }

        LRUQueue& queue() { return lru_queue; }
    };

    using QueryContextPtr = std::shared_ptr<QueryContext>;
    using QueryContextMap = std::unordered_map<TUniqueId, QueryContextPtr>;

    QueryContextMap _query_map;

    bool _enable_file_cache_query_limit = config::enable_file_cache_query_limit;

    QueryContextPtr get_query_context(const TUniqueId& query_id, std::lock_guard<doris::Mutex>&);

    void remove_query_context(const TUniqueId& query_id);

    QueryContextPtr get_or_set_query_context(const TUniqueId& query_id,
                                             std::lock_guard<doris::Mutex>&);

    Status initialize_unlocked(std::lock_guard<doris::Mutex>&);

    using FileSegmentsByOffset = std::map<size_t, FileSegmentCell>;
    struct HashCachedFileKey {
        std::size_t operator()(const Key& k) const { return KeyHash()(k); }
    };
    using CachedFiles = std::unordered_map<Key, FileSegmentsByOffset, HashCachedFileKey>;

    CachedFiles _files;
    size_t _cur_cache_size = 0;
    std::multimap<int64_t, Key> _time_to_key;
    std::unordered_map<Key, int64_t, HashCachedFileKey> _key_to_time;
    LRUQueue _index_queue;
    LRUQueue _normal_queue;
    LRUQueue _disposable_queue;

    LRUQueue& get_queue(CacheType type);
    const LRUQueue& get_queue(CacheType type) const;

    FileSegments get_impl(const Key& key, const CacheContext& context,
                          const FileSegment::Range& range,
                          std::lock_guard<doris::Mutex>& cache_lock);

    FileSegmentCell* get_cell(const Key& key, size_t offset,
                              std::lock_guard<doris::Mutex>& cache_lock);

    FileSegmentCell* add_cell(const Key& key, const CacheContext& context, size_t offset,
                              size_t size, FileSegment::State state,
                              std::lock_guard<doris::Mutex>& cache_lock);

    void use_cell(const FileSegmentCell& cell, FileSegments& result, bool not_need_move,
                  std::lock_guard<doris::Mutex>& cache_lock);

    bool try_reserve_for_lru(const Key& key, QueryContextPtr query_context,
                             const CacheContext& context, size_t offset, size_t size,
                             std::lock_guard<doris::Mutex>& cache_lock);

    std::vector<CacheType> get_other_cache_type(CacheType cur_cache_type);

    bool try_reserve_from_other_queue(CacheType cur_cache_type, size_t offset, int64_t cur_time,
                                      std::lock_guard<doris::Mutex>& cache_lock);

    CacheType get_dowgrade_cache_type(CacheType cur_cache_type) const;

    bool try_reserve_for_ttl(size_t size, std::lock_guard<doris::Mutex>& cache_lock);

    size_t get_available_cache_size(CacheType type) const;

    Status load_cache_info_into_memory(std::lock_guard<doris::Mutex>& cache_lock);

    FileSegments split_range_into_cells(const Key& key, const CacheContext& context, size_t offset,
                                        size_t size, FileSegment::State state,
                                        std::lock_guard<doris::Mutex>& cache_lock);

    std::string dump_structure_unlocked(const Key& key, std::lock_guard<doris::Mutex>& cache_lock);

    void fill_holes_with_empty_file_segments(FileSegments& file_segments, const Key& key,
                                             const CacheContext& context,
                                             const FileSegment::Range& range,
                                             std::lock_guard<doris::Mutex>& cache_lock);

    size_t get_used_cache_size_unlocked(CacheType type,
                                        std::lock_guard<doris::Mutex>& cache_lock) const;

    size_t get_available_cache_size_unlocked(CacheType type,
                                             std::lock_guard<doris::Mutex>& cache_lock) const;

    size_t get_file_segments_num_unlocked(CacheType type,
                                          std::lock_guard<doris::Mutex>& cache_lock) const;

    bool need_to_move(CacheType cell_type, CacheType query_type) const;

    bool remove_if_ttl_file_unlock(const Key& file_key, bool remove_directly,
                                   std::lock_guard<doris::Mutex>&);

    void run_background_operation();

    std::atomic_bool _close {false};
    std::thread _cache_background_thread;

public:
    /// Save a query context information, and adopt different cache policies
    /// for different queries through the context cache layer.
    struct QueryContextHolder {
        QueryContextHolder(const TUniqueId& query_id, CloudFileCache* cache,
                           QueryContextPtr context)
                : query_id(query_id), cache(cache), context(context) {}

        QueryContextHolder& operator=(const QueryContextHolder&) = delete;
        QueryContextHolder(const QueryContextHolder&) = delete;

        ~QueryContextHolder() {
            /// If only the query_map and the current holder hold the context_query,
            /// the query has been completed and the query_context is released.
            if (context) {
                context.reset();
                cache->remove_query_context(query_id);
            }
        }

        const TUniqueId& query_id;
        CloudFileCache* cache = nullptr;
        QueryContextPtr context;
    };
    using QueryContextHolderPtr = std::unique_ptr<QueryContextHolder>;
    QueryContextHolderPtr get_query_context_holder(const TUniqueId& query_id);

private:
    static inline std::deque<std::shared_ptr<FileReader>> s_file_reader_cache;
    static inline doris::Mutex s_file_reader_cache_mtx;
    static constexpr size_t s_max_file_reader_size = 1024 * 1024;
    static inline std::atomic_bool s_read_only {false};

public:
    static void set_read_only(bool read_only) {
        s_read_only = read_only;
        if (read_only) {
            std::lock_guard lock(s_file_reader_cache_mtx);
            s_file_reader_cache.clear();
        }
    }

    static bool read_only() { return s_read_only; }

    static std::weak_ptr<FileReader> cache_file_reader(std::shared_ptr<FileReader> file_reader) {
        std::weak_ptr<FileReader> wp;
        if (!s_read_only) [[likely]] {
            std::lock_guard lock(s_file_reader_cache_mtx);
            if (s_max_file_reader_size == s_file_reader_cache.size()) {
                s_file_reader_cache.pop_back();
            }
            wp = file_reader;
            s_file_reader_cache.emplace_front(std::move(file_reader));
        }
        return wp;
    }
};

using CloudFileCachePtr = CloudFileCache*;

} // namespace io
} // namespace doris
