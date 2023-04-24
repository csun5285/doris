#pragma once

#include <bvar/bvar.h>

#include <list>
#include <memory>
#include <unordered_map>

#include "cloud/io/cloud_file_cache_fwd.h"
#include "cloud/io/file_reader.h"
#include "common/config.h"

namespace doris {
namespace io {
class FileSegment;
using FileSegmentSPtr = std::shared_ptr<FileSegment>;
using FileSegments = std::list<FileSegmentSPtr>;
struct FileSegmentsHolder;
struct ReadSettings;

// default 1 : 17 : 2
enum CacheType {
    INDEX,
    NORMAL,
    DISPOSABLE,
    TTL,
};
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
class IFileCache {
    friend class FileSegment;
    friend struct FileSegmentsHolder;

public:
    struct Key {
        uint128_t key;
        std::string to_string() const;

        Key() = default;
        explicit Key(const uint128_t& key_) : key(key_) {}

        bool operator==(const Key& other) const { return key == other.key; }
    };

    struct KeyHash {
        std::size_t operator()(const Key& k) const { return UInt128Hash()(k.key); }
    };

    IFileCache(const std::string& cache_base_path, const FileCacheSettings& cache_settings);

    virtual ~IFileCache() = default;

    /// Restore cache from local filesystem.
    virtual Status initialize() = 0;

    /// Cache capacity in bytes.
    size_t capacity() const { return _total_size; }

    static Key hash(const std::string& path);

    std::string get_path_in_local_cache(const Key& key, int64_t expiration_time, size_t offset,
                                        CacheType type) const;

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
    virtual FileSegmentsHolder get_or_set(const Key& key, size_t offset, size_t size,
                                          const CacheContext& context) = 0;

    /// For debug.
    virtual std::string dump_structure(const Key& key) = 0;

    virtual size_t get_used_cache_size(CacheType type) const = 0;

    virtual size_t get_file_segments_num(CacheType type) const = 0;

    virtual void change_cache_type(const Key& key, size_t offset, CacheType new_type) = 0;

    static std::string cache_type_to_string(CacheType type);
    static CacheType string_to_cache_type(const std::string& str);

    virtual void remove_if_cached(const IFileCache::Key&) = 0;
    virtual void modify_expiration_time(const IFileCache::Key&, int64_t new_expiration_time) = 0;

    virtual void reset_range(const IFileCache::Key&, size_t offset, size_t old_size,
                             size_t new_size) = 0;

    IFileCache& operator=(const IFileCache&) = delete;
    IFileCache(const IFileCache&) = delete;

protected:
    std::string _cache_base_path;
    size_t _total_size = 0;
    size_t _max_file_segment_size = 0;
    size_t _max_query_cache_size = 0;

    bool _is_initialized = false;

    mutable std::mutex _mutex;

    // metrics
    std::shared_ptr<bvar::Status<size_t>> _cur_cache_size_metrics;
    std::shared_ptr<bvar::Status<size_t>> _cur_ttl_cache_size_metrics;
    std::shared_ptr<bvar::Status<size_t>> _cur_normal_queue_element_count_metrics;
    std::shared_ptr<bvar::Status<size_t>> _cur_normal_queue_cache_size_metrics;
    std::shared_ptr<bvar::Status<size_t>> _cur_index_queue_element_count_metrics;
    std::shared_ptr<bvar::Status<size_t>> _cur_index_queue_cache_size_metrics;
    std::shared_ptr<bvar::Status<size_t>> _cur_disposable_queue_element_count_metrics;
    std::shared_ptr<bvar::Status<size_t>> _cur_disposable_queue_cache_size_metrics;

    virtual bool try_reserve(const Key& key, const CacheContext& context, size_t offset,
                             size_t size, std::lock_guard<std::mutex>& cache_lock) = 0;

    virtual void remove(FileSegmentSPtr file_segment, std::lock_guard<std::mutex>& cache_lock,
                        std::lock_guard<std::mutex>& segment_lock) = 0;

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

        size_t get_total_cache_size(std::lock_guard<std::mutex>& /* cache_lock */) const {
            return cache_size;
        }

        size_t get_elements_num(std::lock_guard<std::mutex>& /* cache_lock */) const {
            return queue.size();
        }

        Iterator add(const Key& key, size_t offset, size_t size,
                     std::lock_guard<std::mutex>& cache_lock);

        void remove(Iterator queue_it, std::lock_guard<std::mutex>& cache_lock);

        void move_to_end(Iterator queue_it, std::lock_guard<std::mutex>& cache_lock);

        std::string to_string(std::lock_guard<std::mutex>& cache_lock) const;

        bool contains(const Key& key, size_t offset, std::lock_guard<std::mutex>& cache_lock) const;

        Iterator begin() { return queue.begin(); }

        Iterator end() { return queue.end(); }

        void remove_all(std::lock_guard<std::mutex>& cache_lock);

        Iterator get(const IFileCache::Key& key, size_t offset,
                     std::lock_guard<std::mutex>& /* cache_lock */) const;

        int64_t get_hot_data_interval() const { return hot_data_interval; }

        size_t max_size;
        size_t max_element_size;
        std::list<FileKeyAndOffset> queue;
        std::unordered_map<std::pair<Key, size_t>, Iterator, HashFileKeyAndOffset> map;
        size_t cache_size {0};
        int64_t hot_data_interval {0};
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

        void remove(const Key& key, size_t offset, std::lock_guard<std::mutex>& cache_lock);

        void reserve(const Key& key, size_t offset, size_t size,
                     std::lock_guard<std::mutex>& cache_lock);

        size_t get_max_cache_size() const { return lru_queue.get_max_size(); }

        size_t get_cache_size(std::lock_guard<std::mutex>& cache_lock) const {
            return lru_queue.get_total_cache_size(cache_lock);
        }

        LRUQueue& queue() { return lru_queue; }
    };

    using QueryContextPtr = std::shared_ptr<QueryContext>;
    using QueryContextMap = std::unordered_map<TUniqueId, QueryContextPtr>;

    QueryContextMap _query_map;

    bool _enable_file_cache_query_limit = config::enable_file_cache_query_limit;

    QueryContextPtr get_query_context(const TUniqueId& query_id, std::lock_guard<std::mutex>&);

    void remove_query_context(const TUniqueId& query_id);

    QueryContextPtr get_or_set_query_context(const TUniqueId& query_id,
                                             std::lock_guard<std::mutex>&);

public:
    /// Save a query context information, and adopt different cache policies
    /// for different queries through the context cache layer.
    struct QueryContextHolder {
        QueryContextHolder(const TUniqueId& query_id, IFileCache* cache, QueryContextPtr context)
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
        IFileCache* cache = nullptr;
        QueryContextPtr context;
    };
    using QueryContextHolderPtr = std::unique_ptr<QueryContextHolder>;
    QueryContextHolderPtr get_query_context_holder(const TUniqueId& query_id);
};

using CloudFileCachePtr = IFileCache*;

} // namespace io
} // namespace doris
