#pragma once

#include <array>
#include <chrono>
#include <map>
#include <memory>
#include <optional>
#include <thread>
#include <unordered_map>

#include "cloud/io/cloud_file_cache.h"
#include "cloud/io/cloud_file_segment.h"
#include "util/doris_metrics.h"
#include "util/metrics.h"

namespace doris {
namespace io {

/**
 * Local cache for remote filesystem files, represented as a set of non-overlapping non-empty file segments.
 * Implements LRU eviction policy.
 */
class LRUFileCache final : public IFileCache {
public:
    /**
     * cache_base_path: the file cache path
     * cache_settings: the file cache setttings
     */
    LRUFileCache(const std::string& cache_base_path, const FileCacheSettings& cache_settings);
    ~LRUFileCache() override {
        _close = true;
        if (_ttl_gc_thread.joinable()) {
            _ttl_gc_thread.join();
        }
    };

    /**
     * get the files which range contain [offset, offset+size-1]
     */
    FileSegmentsHolder get_or_set(const Key& key, size_t offset, size_t size,
                                  const CacheContext& context) override;

    // init file cache
    Status initialize() override;

    size_t get_used_cache_size(CacheType type) const override;

    size_t get_file_segments_num(CacheType type) const override;

    void remove_if_cached(const IFileCache::Key& file_key) override;

    void modify_expiration_time(const IFileCache::Key&, int64_t new_expiration_time) override;

    void change_cache_type(const Key& key, size_t offset, CacheType new_type) override;

private:
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
                        std::lock_guard<std::mutex>& cache_lock);

        FileSegmentCell(FileSegmentCell&& other) noexcept
                : file_segment(std::move(other.file_segment)),
                  cache_type(other.cache_type),
                  queue_iterator(other.queue_iterator),
                  atime(other.atime) {}

        FileSegmentCell& operator=(const FileSegmentCell&) = delete;
        FileSegmentCell(const FileSegmentCell&) = delete;
    };

    using FileSegmentsByOffset = std::map<size_t, FileSegmentCell>;
    struct HashCachedFileKey {
        std::size_t operator()(const Key& k) const { return KeyHash()(k); }
    };
    using CachedFiles = std::unordered_map<Key, FileSegmentsByOffset, HashCachedFileKey>;

    CachedFiles _files;
    size_t _cur_cache_size = 0;
    std::multimap<int64_t, Key> _time_to_key;
    std::unordered_map<Key, int64_t, HashCachedFileKey> _key_to_time;
    LRUQueue _index_queue {7 * 24 * 60 * 60};
    LRUQueue _normal_queue {24 * 60 * 60};
    LRUQueue _disposable_queue {60 * 60};

    LRUFileCache::LRUQueue& get_queue(CacheType type);
    const LRUFileCache::LRUQueue& get_queue(CacheType type) const;

    FileSegments get_impl(const Key& key, const CacheContext& context,
                          const FileSegment::Range& range, std::lock_guard<std::mutex>& cache_lock);

    FileSegmentCell* get_cell(const Key& key, size_t offset,
                              std::lock_guard<std::mutex>& cache_lock);

    FileSegmentCell* add_cell(const Key& key, const CacheContext& context, size_t offset,
                              size_t size, FileSegment::State state,
                              std::lock_guard<std::mutex>& cache_lock);

    void use_cell(const FileSegmentCell& cell, FileSegments& result, bool not_need_move,
                  std::lock_guard<std::mutex>& cache_lock);

    bool try_reserve(const Key& key, const CacheContext& context, size_t offset, size_t size,
                     std::lock_guard<std::mutex>& cache_lock) override;

    bool try_reserve_for_lru(const Key& key, QueryContextPtr query_context,
                             const CacheContext& context, size_t offset, size_t size,
                             std::lock_guard<std::mutex>& cache_lock);

    std::vector<CacheType> get_other_cache_type(CacheType cur_cache_type);

    bool try_reserve_from_other_queue(CacheType cur_cache_type, size_t offset, int64_t cur_time,
                                      std::lock_guard<std::mutex>& cache_lock);

    CacheType get_dowgrade_cache_type(CacheType cur_cache_type) const;

    bool try_reserve_for_ttl(size_t size, std::lock_guard<std::mutex>& cache_lock);

    void remove(FileSegmentSPtr file_segment, std::lock_guard<std::mutex>& cache_lock,
                std::lock_guard<std::mutex>& segment_lock) override;

    size_t get_available_cache_size(CacheType type) const;

    Status load_cache_info_into_memory(std::lock_guard<std::mutex>& cache_lock);

    FileSegments split_range_into_cells(const Key& key, const CacheContext& context, size_t offset,
                                        size_t size, FileSegment::State state,
                                        std::lock_guard<std::mutex>& cache_lock);

    std::string dump_structure_unlocked(const Key& key, std::lock_guard<std::mutex>& cache_lock);

    void fill_holes_with_empty_file_segments(FileSegments& file_segments, const Key& key,
                                             const CacheContext& context,
                                             const FileSegment::Range& range,
                                             std::lock_guard<std::mutex>& cache_lock);

    size_t get_used_cache_size_unlocked(CacheType type,
                                        std::lock_guard<std::mutex>& cache_lock) const;

    size_t get_available_cache_size_unlocked(CacheType type,
                                             std::lock_guard<std::mutex>& cache_lock) const;

    size_t get_file_segments_num_unlocked(CacheType type,
                                          std::lock_guard<std::mutex>& cache_lock) const;

    bool need_to_move(CacheType cell_type, CacheType query_type) const;

    bool remove_if_ttl_file_unlock(const IFileCache::Key& file_key, bool remove_directly,
                                   std::lock_guard<std::mutex>&);

    void ttl_cache_gc();

    void report_cache_metrcs();

public:
    std::string dump_structure(const Key& key) override;

private:
    std::atomic_bool _close {false};
    std::shared_ptr<MetricEntity> entity;
    uint64_t hot_eviction = 0;
    UIntGauge* _hot_eviction_counter = nullptr;
    static inline int64_t s_hot_eviction_interval_seconds = 10 * 60;
    std::thread _ttl_gc_thread;
    UIntGauge* _ttl_cache_percentage = nullptr;
    UIntGauge* _cur_file_cache_size = nullptr;
};

} // namespace io
} // namespace doris
