#include "cloud/io/cloud_lru_file_cache.h"

#include <filesystem>
#include <memory>
#include <random>
#include <system_error>
#include <utility>

#include "cloud/io/cloud_file_cache.h"
#include "cloud/io/cloud_file_cache_settings.h"
#include "common/status.h"
#include "util/time.h"
#include "vec/common/hex.h"

namespace doris {
namespace io {

LRUFileCache::LRUFileCache(const std::string& cache_base_path,
                           const FileCacheSettings& cache_settings)
        : IFileCache(cache_base_path, cache_settings) {
    _disposable_queue = LRUQueue(cache_settings.disposable_queue_size,
                                 cache_settings.disposable_queue_elements, 60 * 60);
    _index_queue = LRUQueue(cache_settings.index_queue_size, cache_settings.index_queue_elements,
                            7 * 24 * 60 * 60);
    _normal_queue = LRUQueue(cache_settings.query_queue_size, cache_settings.query_queue_elements,
                             24 * 60 * 60);

    LOG(INFO) << fmt::format(
            "file cache path={}, disposable queue size={} elements={}, index queue size={} "
            "elements={}, query queue "
            "size={} elements={}",
            cache_base_path, cache_settings.disposable_queue_size,
            cache_settings.disposable_queue_elements, cache_settings.index_queue_size,
            cache_settings.index_queue_elements, cache_settings.query_queue_size,
            cache_settings.query_queue_elements);
}

Status LRUFileCache::initialize() {
    std::lock_guard cache_lock(_mutex);
    if (!_is_initialized) {
        if (std::filesystem::exists(_cache_base_path)) {
            RETURN_IF_ERROR(load_cache_info_into_memory(cache_lock));
        } else {
            std::error_code ec;
            std::filesystem::create_directories(_cache_base_path, ec);
            if (ec) {
                return Status::IOError("cannot create {}: {}", _cache_base_path,
                                       std::strerror(ec.value()));
            }
        }
    }
    _is_initialized = true;
    _cache_background_thread = std::thread(&LRUFileCache::run_background_operation, this);

    return Status::OK();
}

void LRUFileCache::use_cell(const FileSegmentCell& cell, FileSegments& result, bool move_iter_flag,
                            std::lock_guard<std::mutex>& cache_lock) {
    auto file_segment = cell.file_segment;
    DCHECK(!(file_segment->is_downloaded() &&
             std::filesystem::file_size(
                     get_path_in_local_cache(file_segment->key(), file_segment->expiration_time(),
                                             file_segment->offset(), cell.cache_type)) == 0))
            << "Cannot have zero size downloaded file segments. Current file segment: "
            << file_segment->range().to_string();

    result.push_back(cell.file_segment);
    if (cell.cache_type != CacheType::TTL) {
        auto& queue = get_queue(cell.cache_type);
        DCHECK(cell.queue_iterator) << "impossible";
        /// Move to the end of the queue. The iterator remains valid.
        if (move_iter_flag) {
            queue.move_to_end(*cell.queue_iterator, cache_lock);
        }
    }
    cell.update_atime();
}

LRUFileCache::FileSegmentCell* LRUFileCache::get_cell(const Key& key, size_t offset,
                                                      std::lock_guard<std::mutex>& cache_lock
                                                      [[maybe_unused]]) {
    auto it = _files.find(key);
    if (it == _files.end()) {
        return nullptr;
    }

    auto& offsets = it->second;
    auto cell_it = offsets.find(offset);
    if (cell_it == offsets.end()) {
        return nullptr;
    }

    return &cell_it->second;
}

bool LRUFileCache::need_to_move(CacheType cell_type, CacheType query_type) const {
    return query_type != CacheType::DISPOSABLE || cell_type == CacheType::DISPOSABLE ? true : false;
}

FileSegments LRUFileCache::get_impl(const Key& key, const CacheContext& context,
                                    const FileSegment::Range& range,
                                    std::lock_guard<std::mutex>& cache_lock) {
    /// Given range = [left, right] and non-overlapping ordered set of file segments,
    /// find list [segment1, ..., segmentN] of segments which intersect with given range.
    auto it = _files.find(key);
    if (it == _files.end()) {
        return {};
    }

    auto& file_segments = it->second;
    if (file_segments.empty()) {
        auto iter = _key_to_time.find(key);
        auto key_path = get_path_in_local_cache(key, iter == _key_to_time.end() ? 0 : iter->second);
        _files.erase(key);
        if (iter != _key_to_time.end()) {
            // remove from _time_to_key
            auto _time_to_key_iter = _time_to_key.equal_range(iter->second);
            while (_time_to_key_iter.first != _time_to_key_iter.second) {
                if (_time_to_key_iter.first->second == key) {
                    _time_to_key_iter.first = _time_to_key.erase(_time_to_key_iter.first);
                    break;
                }
                _time_to_key_iter.first++;
            }
            _key_to_time.erase(iter);
        }
        /// Note: it is guaranteed that there is no concurrency with files deletion,
        /// because cache files are deleted only inside IFileCache and under cache lock.
        if (std::filesystem::exists(key_path)) {
            std::error_code ec;
            std::filesystem::remove_all(key_path, ec);
            if (ec) {
                LOG(ERROR) << ec.message();
            }
        }

        return {};
    }

    // change to ttl if the segments aren't ttl
    if (context.cache_type == CacheType::TTL && _key_to_time.find(key) == _key_to_time.end()) {
        for (auto& [_, cell] : file_segments) {
            if (cell.file_segment->change_cache_type(CacheType::TTL)) {
                auto& queue = get_queue(cell.cache_type);
                queue.remove(cell.queue_iterator.value(), cache_lock);
                cell.cache_type = CacheType::TTL;
                cell.file_segment->update_expiration_time(context.expiration_time);
            }
        }
        _key_to_time[key] = context.expiration_time;
        _time_to_key.insert(std::make_pair(context.expiration_time, key));
        std::error_code ec;
        std::filesystem::rename(get_path_in_local_cache(key, 0),
                                get_path_in_local_cache(key, context.expiration_time), ec);
        if (ec) {
            LOG(ERROR) << ec.message();
        } else {
            DCHECK(std::filesystem::exists(get_path_in_local_cache(key, context.expiration_time)));
        }
    }
    if (auto iter = _key_to_time.find(key);
        context.cache_type == CacheType::TTL && context.expiration_time != 0 &&
        iter != _key_to_time.end() && iter->second != context.expiration_time) {
        std::error_code ec;
        std::filesystem::rename(get_path_in_local_cache(key, iter->second),
                                get_path_in_local_cache(key, context.expiration_time), ec);
        if (ec) [[unlikely]] {
            LOG(ERROR) << ec.message();
        } else {
            // remove from _time_to_key
            auto _time_to_key_iter = _time_to_key.equal_range(iter->second);
            while (_time_to_key_iter.first != _time_to_key_iter.second) {
                if (_time_to_key_iter.first->second == key) {
                    _time_to_key_iter.first = _time_to_key.erase(_time_to_key_iter.first);
                    break;
                }
                _time_to_key_iter.first++;
            }
            _time_to_key.insert(std::make_pair(context.expiration_time, key));
            for (auto& [_, cell] : file_segments) {
                cell.file_segment->update_expiration_time(context.expiration_time);
            }
            iter->second = context.expiration_time;
        }
    }

    FileSegments result;
    auto segment_it = file_segments.lower_bound(range.left);
    if (segment_it == file_segments.end()) {
        /// N - last cached segment for given file key, segment{N}.offset < range.left:
        ///   segment{N}                       segment{N}
        /// [________                         [_______]
        ///     [__________]         OR                  [________]
        ///     ^                                        ^
        ///     range.left                               range.left

        const auto& cell = file_segments.rbegin()->second;
        if (cell.file_segment->range().right < range.left) {
            return {};
        }
        use_cell(cell, result, need_to_move(cell.cache_type, context.cache_type), cache_lock);
    } else { /// segment_it <-- segmment{k}
        if (segment_it != file_segments.begin()) {
            const auto& prev_cell = std::prev(segment_it)->second;
            const auto& prev_cell_range = prev_cell.file_segment->range();

            if (range.left <= prev_cell_range.right) {
                ///   segment{k-1}  segment{k}
                ///   [________]   [_____
                ///       [___________
                ///       ^
                ///       range.left

                use_cell(prev_cell, result, need_to_move(prev_cell.cache_type, context.cache_type),
                         cache_lock);
            }
        }

        ///  segment{k} ...       segment{k-1}  segment{k}                      segment{k}
        ///  [______              [______]     [____                        [________
        ///  [_________     OR              [________      OR    [______]   ^
        ///  ^                              ^                           ^   segment{k}.offset
        ///  range.left                     range.left                  range.right

        while (segment_it != file_segments.end()) {
            const auto& cell = segment_it->second;
            if (range.right < cell.file_segment->range().left) {
                break;
            }

            use_cell(cell, result, need_to_move(cell.cache_type, context.cache_type), cache_lock);
            ++segment_it;
        }
    }

    return result;
}

FileSegments LRUFileCache::split_range_into_cells(const Key& key, const CacheContext& context,
                                                  size_t offset, size_t size,
                                                  FileSegment::State state,
                                                  std::lock_guard<std::mutex>& cache_lock) {
    DCHECK(size > 0);

    auto current_pos = offset;
    auto end_pos_non_included = offset + size;

    size_t current_size = 0;
    size_t remaining_size = size;

    FileSegments file_segments;
    while (current_pos < end_pos_non_included) {
        current_size = std::min(remaining_size, _max_file_segment_size);
        remaining_size -= current_size;
        state = try_reserve(key, context, current_pos, current_size, cache_lock)
                        ? state
                        : FileSegment::State::SKIP_CACHE;
        if (UNLIKELY(state == FileSegment::State::SKIP_CACHE)) {
            auto file_segment = std::make_shared<FileSegment>(current_pos, current_size, key, this,
                                                              FileSegment::State::SKIP_CACHE,
                                                              context.cache_type, 0);
            file_segments.push_back(std::move(file_segment));
        } else {
            auto* cell = add_cell(key, context, current_pos, current_size, state, cache_lock);
            if (cell) {
                file_segments.push_back(cell->file_segment);
                if (!context.is_cold_data) {
                    cell->update_atime();
                }
            }
        }

        current_pos += current_size;
    }

    DCHECK(file_segments.empty() || offset + size - 1 == file_segments.back()->range().right);
    return file_segments;
}

void LRUFileCache::fill_holes_with_empty_file_segments(FileSegments& file_segments, const Key& key,
                                                       const CacheContext& context,
                                                       const FileSegment::Range& range,
                                                       std::lock_guard<std::mutex>& cache_lock) {
    /// There are segments [segment1, ..., segmentN]
    /// (non-overlapping, non-empty, ascending-ordered) which (maybe partially)
    /// intersect with given range.

    /// It can have holes:
    /// [____________________]         -- requested range
    ///     [____]  [_]   [_________]  -- intersecting cache [segment1, ..., segmentN]
    ///
    /// For each such hole create a cell with file segment state EMPTY.

    auto it = file_segments.begin();
    auto segment_range = (*it)->range();

    size_t current_pos;
    if (segment_range.left < range.left) {
        ///    [_______     -- requested range
        /// [_______
        /// ^
        /// segment1

        current_pos = segment_range.right + 1;
        ++it;
    } else {
        current_pos = range.left;
    }

    while (current_pos <= range.right && it != file_segments.end()) {
        segment_range = (*it)->range();

        if (current_pos == segment_range.left) {
            current_pos = segment_range.right + 1;
            ++it;
            continue;
        }

        DCHECK(current_pos < segment_range.left);

        auto hole_size = segment_range.left - current_pos;

        file_segments.splice(it, split_range_into_cells(key, context, current_pos, hole_size,
                                                        FileSegment::State::EMPTY, cache_lock));

        current_pos = segment_range.right + 1;
        ++it;
    }

    if (current_pos <= range.right) {
        ///   ________]     -- requested range
        ///   _____]
        ///        ^
        /// segmentN

        auto hole_size = range.right - current_pos + 1;

        file_segments.splice(file_segments.end(),
                             split_range_into_cells(key, context, current_pos, hole_size,
                                                    FileSegment::State::EMPTY, cache_lock));
    }
}

FileSegmentsHolder LRUFileCache::get_or_set(const Key& key, size_t offset, size_t size,
                                            const CacheContext& context) {
    FileSegment::Range range(offset, offset + size - 1);

    std::lock_guard cache_lock(_mutex);

    /// Get all segments which intersect with the given range.
    auto file_segments = get_impl(key, context, range, cache_lock);

    if (file_segments.empty()) {
        file_segments = split_range_into_cells(key, context, offset, size,
                                               FileSegment::State::EMPTY, cache_lock);
    } else {
        fill_holes_with_empty_file_segments(file_segments, key, context, range, cache_lock);
    }

    DCHECK(!file_segments.empty());
    return FileSegmentsHolder(std::move(file_segments));
}

LRUFileCache::FileSegmentCell* LRUFileCache::add_cell(const Key& key, const CacheContext& context,
                                                      size_t offset, size_t size,
                                                      FileSegment::State state,
                                                      std::lock_guard<std::mutex>& cache_lock) {
    /// Create a file segment cell and put it in `files` map by [key][offset].
    if (size == 0) {
        return nullptr; /// Empty files are not cached.
    }
    DCHECK(_files[key].count(offset) == 0)
            << "Cache already exists for key: " << key.to_string() << ", offset: " << offset
            << ", size: " << size
            << ".\nCurrent cache structure: " << dump_structure_unlocked(key, cache_lock);

    auto& offsets = _files[key];
    DCHECK((context.expiration_time == 0 && context.cache_type != CacheType::TTL) ||
           (context.cache_type == CacheType::TTL && context.expiration_time != 0));
    if (offsets.empty()) {
        auto key_path = get_path_in_local_cache(key, context.expiration_time);
        if (!std::filesystem::exists(key_path)) {
            std::error_code ec;
            std::filesystem::create_directories(key_path, ec);
            if (ec) {
                LOG(ERROR) << fmt::format("cannot create {}: {}", key_path,
                                          std::strerror(ec.value()));
                state = FileSegment::State::SKIP_CACHE;
            }
        }
    }
    FileSegmentCell cell(std::make_shared<FileSegment>(offset, size, key, this, state,
                                                       context.cache_type, context.expiration_time),
                         context.cache_type, cache_lock);
    if (context.cache_type != CacheType::TTL) {
        auto& queue = get_queue(context.cache_type);
        cell.queue_iterator = queue.add(key, offset, size, cache_lock);
    } else {
        if (_key_to_time.find(key) == _key_to_time.end()) {
            _key_to_time[key] = context.expiration_time;
            _time_to_key.insert(std::make_pair(context.expiration_time, key));
        }
    }
    _cur_cache_size += size;

    auto [it, inserted] = offsets.insert({offset, std::move(cell)});

    DCHECK(inserted) << "Failed to insert into cache key: " << key.to_string()
                     << ", offset: " << offset << ", size: " << size;

    return &(it->second);
}

LRUFileCache::LRUQueue& LRUFileCache::get_queue(CacheType type) {
    switch (type) {
    case CacheType::INDEX:
        return _index_queue;
    case CacheType::DISPOSABLE:
        return _disposable_queue;
    case CacheType::NORMAL:
        return _normal_queue;
    default:
        DCHECK(false);
    }
    return _normal_queue;
}

const LRUFileCache::LRUQueue& LRUFileCache::get_queue(CacheType type) const {
    switch (type) {
    case CacheType::INDEX:
        return _index_queue;
    case CacheType::DISPOSABLE:
        return _disposable_queue;
    case CacheType::NORMAL:
        return _normal_queue;
    default:
        DCHECK(false);
    }
    return _normal_queue;
}

bool LRUFileCache::try_reserve_for_ttl(size_t size, std::lock_guard<std::mutex>& cache_lock) {
    size_t removed_size = 0;
    auto is_overflow = [&] { return _cur_cache_size + size - removed_size > _total_size; };
    auto remove_file_segment_if = [&](FileSegmentCell* cell) {
        FileSegmentSPtr file_segment = cell->file_segment;
        if (file_segment) {
            std::lock_guard segment_lock(file_segment->_mutex);
            remove(file_segment, cache_lock, segment_lock);
        }
    };
    size_t normal_queue_size = _normal_queue.get_total_cache_size(cache_lock);
    size_t disposable_queue_size = _disposable_queue.get_total_cache_size(cache_lock);
    size_t index_queue_size = _index_queue.get_total_cache_size(cache_lock);
    if (is_overflow() && normal_queue_size == 0 && disposable_queue_size == 0 &&
        index_queue_size == 0) {
        return false;
    }
    std::vector<FileSegmentCell*> trash;
    std::vector<FileSegmentCell*> to_evict;
    auto collect_eliminate_fragments = [&](LRUQueue& queue) {
        for (const auto& [entry_key, entry_offset, entry_size] : queue) {
            if (!is_overflow()) {
                break;
            }
            auto* cell = get_cell(entry_key, entry_offset, cache_lock);

            DCHECK(cell) << "Cache became inconsistent. Key: " << entry_key.to_string()
                         << ", offset: " << entry_offset;

            size_t cell_size = cell->size();
            DCHECK(entry_size == cell_size);

            /// It is guaranteed that cell is not removed from cache as long as
            /// pointer to corresponding file segment is hold by any other thread.

            if (cell->releasable()) {
                auto& file_segment = cell->file_segment;

                std::lock_guard segment_lock(file_segment->_mutex);

                switch (file_segment->_download_state) {
                case FileSegment::State::DOWNLOADED: {
                    /// Cell will actually be removed only if
                    /// we managed to reserve enough space.

                    to_evict.push_back(cell);
                    break;
                }
                default: {
                    trash.push_back(cell);
                    break;
                }
                }
                removed_size += cell_size;
            }
        }
    };
    if (disposable_queue_size != 0) {
        collect_eliminate_fragments(get_queue(CacheType::DISPOSABLE));
    }
    if (normal_queue_size != 0) {
        collect_eliminate_fragments(get_queue(CacheType::NORMAL));
    }
    if (index_queue_size != 0) {
        collect_eliminate_fragments(get_queue(CacheType::INDEX));
    }
    std::for_each(trash.begin(), trash.end(), remove_file_segment_if);
    std::for_each(to_evict.begin(), to_evict.end(), remove_file_segment_if);
    if (is_overflow()) {
        return false;
    }
    return true;
}

// 1. if ttl cache
//     a. evict from disposable/normal/index queue one by one
// 2. if dont reach query limit or dont have query limit
//     a. evict from other queue
//     b. evict from current queue
//         a.1 if the data belong write, then just evict cold data
// 3. if reach query limit
//     a. evict from query queue
//     b. evict from other queue

bool LRUFileCache::try_reserve(const Key& key, const CacheContext& context, size_t offset,
                               size_t size, std::lock_guard<std::mutex>& cache_lock) {
    if (context.cache_type == CacheType::TTL) {
        return try_reserve_for_ttl(size, cache_lock);
    }
    auto query_context =
            _enable_file_cache_query_limit && (context.query_id.hi != 0 || context.query_id.lo != 0)
                    ? get_query_context(context.query_id, cache_lock)
                    : nullptr;
    if (!query_context) {
        return try_reserve_for_lru(key, nullptr, context, offset, size, cache_lock);
    } else if (query_context->get_cache_size(cache_lock) + size <=
               query_context->get_max_cache_size()) {
        return try_reserve_for_lru(key, query_context, context, offset, size, cache_lock);
    }
    int64_t cur_time = std::chrono::duration_cast<std::chrono::seconds>(
                               std::chrono::steady_clock::now().time_since_epoch())
                               .count();
    auto& queue = get_queue(context.cache_type);
    size_t removed_size = 0;
    size_t queue_element_size = queue.get_elements_num(cache_lock);
    size_t queue_size = queue.get_total_cache_size(cache_lock);

    std::vector<IFileCache::LRUQueue::Iterator> ghost;
    std::vector<FileSegmentCell*> trash;
    std::vector<FileSegmentCell*> to_evict;

    size_t max_size = queue.get_max_size();
    size_t max_element_size = queue.get_max_element_size();
    auto is_overflow = [&] {
        return _cur_cache_size + size - removed_size > _total_size ||
               (queue_size + size - removed_size > max_size) ||
               queue_element_size >= max_element_size ||
               (query_context->get_cache_size(cache_lock) + size - removed_size >
                query_context->get_max_cache_size());
    };

    /// Select the cache from the LRU queue held by query for expulsion.
    for (auto iter = query_context->queue().begin(); iter != query_context->queue().end(); iter++) {
        if (!is_overflow()) {
            break;
        }

        auto* cell = get_cell(iter->key, iter->offset, cache_lock);

        if (!cell) {
            /// The cache corresponding to this record may be swapped out by
            /// other queries, so it has become invalid.
            ghost.push_back(iter);
            removed_size += iter->size;
        } else {
            size_t cell_size = cell->size();
            DCHECK(iter->size == cell_size);

            if (cell->releasable()) {
                auto& file_segment = cell->file_segment;
                std::lock_guard segment_lock(file_segment->_mutex);

                switch (file_segment->_download_state) {
                case FileSegment::State::DOWNLOADED: {
                    to_evict.push_back(cell);
                    break;
                }
                default: {
                    trash.push_back(cell);
                    break;
                }
                }
                removed_size += cell_size;
                --queue_element_size;
            }
        }
    }

    auto remove_file_segment_if = [&](FileSegmentCell* cell) {
        FileSegmentSPtr file_segment = cell->file_segment;
        if (file_segment) {
            query_context->remove(file_segment->key(), file_segment->offset(), cache_lock);
            std::lock_guard segment_lock(file_segment->_mutex);
            remove(file_segment, cache_lock, segment_lock);
        }
    };

    for (auto& iter : ghost) {
        query_context->remove(iter->key, iter->offset, cache_lock);
    }

    std::for_each(trash.begin(), trash.end(), remove_file_segment_if);
    std::for_each(to_evict.begin(), to_evict.end(), remove_file_segment_if);

    if (is_overflow() &&
        !try_reserve_from_other_queue(context.cache_type, size, cur_time, cache_lock)) {
        return false;
    }
    query_context->reserve(key, offset, size, cache_lock);
    return true;
}
bool LRUFileCache::remove_if_ttl_file_unlock(const IFileCache::Key& file_key, bool remove_directly,
                                             std::lock_guard<std::mutex>& cache_lock) {
    if (auto iter = _key_to_time.find(file_key);
        _key_to_time.find(file_key) != _key_to_time.end()) {
        if (!remove_directly) {
            for (auto& [_, cell] : _files[file_key]) {
                if (cell.cache_type == CacheType::TTL) {
                    if (cell.file_segment->change_cache_type(CacheType::NORMAL)) {
                        auto& queue = get_queue(CacheType::NORMAL);
                        cell.queue_iterator =
                                queue.add(cell.file_segment->key(), cell.file_segment->offset(),
                                          cell.file_segment->range().size(), cache_lock);
                        cell.file_segment->update_expiration_time(0);
                        cell.cache_type = CacheType::NORMAL;
                    }
                }
            }
            std::error_code ec;
            std::filesystem::rename(get_path_in_local_cache(file_key, iter->second),
                                    get_path_in_local_cache(file_key, 0), ec);
            if (ec) {
                LOG(ERROR) << ec.message();
            }
        } else {
            std::vector<FileSegmentCell*> to_remove;
            for (auto& [_, cell] : _files[file_key]) {
                to_remove.push_back(&cell);
            }
            std::for_each(to_remove.begin(), to_remove.end(), [&](FileSegmentCell* cell) {
                FileSegmentSPtr file_segment = cell->file_segment;
                std::lock_guard segment_lock(file_segment->_mutex);
                remove(file_segment, cache_lock, segment_lock);
            });
        }
        // remove from _time_to_key
        // the param key maybe be passed by _time_to_key, if removed it, cannot use it anymore
        auto _time_to_key_iter = _time_to_key.equal_range(iter->second);
        while (_time_to_key_iter.first != _time_to_key_iter.second) {
            if (_time_to_key_iter.first->second == file_key) {
                _time_to_key_iter.first = _time_to_key.erase(_time_to_key_iter.first);
                break;
            }
            _time_to_key_iter.first++;
        }
        _key_to_time.erase(iter);
        return true;
    }
    return false;
}

void LRUFileCache::remove_if_cached(const IFileCache::Key& file_key) {
    std::lock_guard cache_lock(_mutex);
    bool is_ttl_file = remove_if_ttl_file_unlock(file_key, true, cache_lock);
    if (!is_ttl_file) {
        auto iter = _files.find(file_key);
        std::vector<FileSegmentCell*> to_remove;
        if (iter != _files.end()) {
            for (auto& [_, cell] : iter->second) {
                to_remove.push_back(&cell);
            }
        }
        auto remove_file_segment_if = [&](FileSegmentCell* cell) {
            FileSegmentSPtr file_segment = cell->file_segment;
            if (file_segment) {
                std::lock_guard segment_lock(file_segment->_mutex);
                remove(file_segment, cache_lock, segment_lock);
            }
        };
        std::for_each(to_remove.begin(), to_remove.end(), remove_file_segment_if);
    }
}

std::vector<CacheType> LRUFileCache::get_other_cache_type(CacheType cur_cache_type) {
    switch (cur_cache_type) {
    case CacheType::INDEX:
        return {CacheType::DISPOSABLE, CacheType::NORMAL};
    case CacheType::NORMAL:
        return {CacheType::DISPOSABLE, CacheType::INDEX};
    case CacheType::DISPOSABLE:
        return {CacheType::NORMAL, CacheType::INDEX};
    default:
        return {};
    }
    return {};
}

void LRUFileCache::reset_range(const IFileCache::Key& key, size_t offset, size_t old_size,
                               size_t new_size) {
    std::lock_guard cache_lock(_mutex);
    DCHECK(_files.find(key) != _files.end() &&
           _files.find(key)->second.find(offset) != _files.find(key)->second.end());
    FileSegmentCell* cell = get_cell(key, offset, cache_lock);
    DCHECK(cell != nullptr);
    if (cell->cache_type != CacheType::TTL) {
        auto& queue = get_queue(cell->cache_type);
        DCHECK(queue.contains(key, offset, cache_lock));
        auto iter = queue.get(key, offset, cache_lock);
        iter->size = new_size;
        queue.cache_size -= old_size;
        queue.cache_size += new_size;
    }
    _cur_cache_size -= old_size;
    _cur_cache_size += new_size;
}

bool LRUFileCache::try_reserve_from_other_queue(CacheType cur_cache_type, size_t size,
                                                int64_t cur_time,
                                                std::lock_guard<std::mutex>& cache_lock) {
    auto other_cache_types = get_other_cache_type(cur_cache_type);
    size_t removed_size = 0;
    auto is_overflow = [&] { return _cur_cache_size + size - removed_size > _total_size; };
    std::vector<FileSegmentCell*> to_evict;
    std::vector<FileSegmentCell*> trash;
    for (CacheType cache_type : other_cache_types) {
        auto& queue = get_queue(cache_type);
        for (const auto& [entry_key, entry_offset, entry_size] : queue) {
            if (!is_overflow()) {
                break;
            }
            auto* cell = get_cell(entry_key, entry_offset, cache_lock);
            DCHECK(cell) << "Cache became inconsistent. Key: " << entry_key.to_string()
                         << ", offset: " << entry_offset;

            size_t cell_size = cell->size();
            DCHECK(entry_size == cell_size);

            if (cell->atime == 0 ? true : cell->atime + queue.get_hot_data_interval() < cur_time) {
                break;
            }

            if (cell->releasable()) {
                auto& file_segment = cell->file_segment;

                std::lock_guard segment_lock(file_segment->_mutex);

                switch (file_segment->_download_state) {
                case FileSegment::State::DOWNLOADED: {
                    to_evict.push_back(cell);
                    break;
                }
                default: {
                    trash.push_back(cell);
                    break;
                }
                }

                removed_size += cell_size;
            }
        }
    }
    auto remove_file_segment_if = [&](FileSegmentCell* cell) {
        FileSegmentSPtr file_segment = cell->file_segment;
        if (file_segment) {
            std::lock_guard segment_lock(file_segment->_mutex);
            remove(file_segment, cache_lock, segment_lock);
        }
    };

    std::for_each(trash.begin(), trash.end(), remove_file_segment_if);
    std::for_each(to_evict.begin(), to_evict.end(), remove_file_segment_if);

    if (is_overflow()) {
        return false;
    }

    return true;
}

bool LRUFileCache::try_reserve_for_lru(const Key& key, QueryContextPtr query_context,
                                       const CacheContext& context, size_t offset, size_t size,
                                       std::lock_guard<std::mutex>& cache_lock) {
    int64_t cur_time = std::chrono::duration_cast<std::chrono::seconds>(
                               std::chrono::steady_clock::now().time_since_epoch())
                               .count();
    if (!try_reserve_from_other_queue(context.cache_type, size, cur_time, cache_lock)) {
        auto& queue = get_queue(context.cache_type);
        size_t removed_size = 0;
        size_t queue_element_size = queue.get_elements_num(cache_lock);
        size_t queue_size = queue.get_total_cache_size(cache_lock);

        size_t max_size = queue.get_max_size();
        size_t max_element_size = queue.get_max_element_size();
        auto is_overflow = [&] {
            return _cur_cache_size + size - removed_size > _total_size ||
                   (queue_size + size - removed_size > max_size) ||
                   queue_element_size >= max_element_size;
        };

        std::vector<FileSegmentCell*> to_evict;
        std::vector<FileSegmentCell*> trash;
        for (const auto& [entry_key, entry_offset, entry_size] : queue) {
            if (!is_overflow()) {
                break;
            }
            auto* cell = get_cell(entry_key, entry_offset, cache_lock);

            DCHECK(cell) << "Cache became inconsistent. Key: " << entry_key.to_string()
                         << ", offset: " << entry_offset;

            size_t cell_size = cell->size();
            DCHECK(entry_size == cell_size);

            // If is_cold_data is true and the cell is hot, the cell cannot be evicted.
            if (context.is_cold_data && cell->atime != 0 &&
                cell->atime + queue.get_hot_data_interval() > cur_time) {
                continue;
            }
            if (cell->releasable()) {
                auto& file_segment = cell->file_segment;

                std::lock_guard segment_lock(file_segment->_mutex);

                switch (file_segment->_download_state) {
                case FileSegment::State::DOWNLOADED: {
                    /// Cell will actually be removed only if
                    /// we managed to reserve enough space.

                    to_evict.push_back(cell);
                    break;
                }
                default: {
                    trash.push_back(cell);
                    break;
                }
                }

                removed_size += cell_size;
                --queue_element_size;
            }
        }

        auto remove_file_segment_if = [&](FileSegmentCell* cell) {
            FileSegmentSPtr file_segment = cell->file_segment;
            if (file_segment) {
                std::lock_guard segment_lock(file_segment->_mutex);
                remove(file_segment, cache_lock, segment_lock);
            }
        };

        std::for_each(trash.begin(), trash.end(), remove_file_segment_if);
        std::for_each(to_evict.begin(), to_evict.end(), remove_file_segment_if);

        if (is_overflow()) {
            return false;
        }
    }

    if (query_context) {
        query_context->reserve(key, offset, size, cache_lock);
    }
    return true;
}

void LRUFileCache::remove(FileSegmentSPtr file_segment, std::lock_guard<std::mutex>& cache_lock,
                          std::lock_guard<std::mutex>&) {
    auto key = file_segment->key();
    auto offset = file_segment->offset();
    auto type = file_segment->cache_type();
    auto expiration_time = file_segment->expiration_time();
    auto* cell = get_cell(key, offset, cache_lock);
    DCHECK(cell) << "No cache cell for key: " << key.to_string() << ", offset: " << offset;

    if (cell->queue_iterator) {
        auto& queue = get_queue(file_segment->cache_type());
        queue.remove(*cell->queue_iterator, cache_lock);
    }
    _cur_cache_size -= file_segment->range().size();
    auto& offsets = _files[file_segment->key()];
    offsets.erase(file_segment->offset());

    auto cache_file_path = get_path_in_local_cache(key, expiration_time, offset, type);
    if (std::filesystem::exists(cache_file_path)) {
        std::error_code ec;
        std::filesystem::remove(cache_file_path, ec);
        if (ec) {
            LOG(ERROR) << ec.message();
        }
    }
    if (_is_initialized && offsets.empty()) {
        auto key_path = get_path_in_local_cache(key, expiration_time);
        _files.erase(key);
        std::error_code ec;
        std::filesystem::remove_all(key_path, ec);
        if (ec) {
            LOG(ERROR) << ec.message();
        }
    }
}

Status LRUFileCache::load_cache_info_into_memory(std::lock_guard<std::mutex>& cache_lock) {
    Key key;
    uint64_t offset = 0;
    size_t size = 0;
    std::vector<std::pair<LRUQueue::Iterator, CacheType>> queue_entries;

    // upgrade the cache
    std::filesystem::directory_iterator upgrade_key_it {_cache_base_path};
    for (; upgrade_key_it != std::filesystem::directory_iterator(); ++upgrade_key_it) {
        if (upgrade_key_it->path().filename().native().find('_') == std::string::npos) {
            std::error_code ec;
            std::filesystem::rename(upgrade_key_it->path(), upgrade_key_it->path().native() + "_0",
                                    ec);
            if (ec) {
                return Status::IOError(ec.message());
            }
        }
    }

    /// cache_base_path / key / offset
    std::filesystem::directory_iterator key_it {_cache_base_path};
    for (; key_it != std::filesystem::directory_iterator(); ++key_it) {
        auto key_with_suffix = key_it->path().filename().native();
        auto delim_pos = key_with_suffix.find('_');
        DCHECK(delim_pos != std::string::npos);
        std::string key_str;
        std::string expiration_time_str;
        key_str = key_with_suffix.substr(0, delim_pos);
        expiration_time_str = key_with_suffix.substr(delim_pos + 1);
        key = Key(vectorized::unhex_uint<uint128_t>(key_str.c_str()));
        std::filesystem::directory_iterator offset_it {key_it->path()};
        CacheContext context;
        context.query_id = TUniqueId();
        context.expiration_time = std::stol(expiration_time_str);
        for (; offset_it != std::filesystem::directory_iterator(); ++offset_it) {
            auto offset_with_suffix = offset_it->path().filename().native();
            auto delim_pos = offset_with_suffix.find('_');
            CacheType cache_type = CacheType::NORMAL;
            bool parsed = true;
            try {
                if (delim_pos == std::string::npos) {
                    // same as type "normal"
                    offset = stoull(offset_with_suffix);
                } else {
                    offset = stoull(offset_with_suffix.substr(0, delim_pos));
                    std::string suffix = offset_with_suffix.substr(delim_pos + 1);
                    // not need persistent any more
                    if (suffix == "persistent") [[unlikely]] {
                        std::error_code ec;
                        std::filesystem::remove(offset_it->path(), ec);
                        if (ec) {
                            return Status::IOError(ec.message());
                        }
                        continue;
                    } else {
                        cache_type = string_to_cache_type(suffix);
                    }
                }
            } catch (...) {
                parsed = false;
            }

            if (!parsed) {
                return Status::IOError("Unexpected file: {}", offset_it->path().native());
            }

            size = offset_it->file_size();
            if (size == 0) {
                std::error_code ec;
                std::filesystem::remove(offset_it->path(), ec);
                if (ec) {
                    return Status::IOError(ec.message());
                }
                continue;
            }
            context.cache_type = cache_type;
            if (try_reserve(key, context, offset, size, cache_lock)) {
                auto* cell = add_cell(key, context, offset, size, FileSegment::State::DOWNLOADED,
                                      cache_lock);
                if (cache_type != CacheType::TTL) {
                    queue_entries.emplace_back(*cell->queue_iterator, cache_type);
                }
            } else {
                std::error_code ec;
                std::filesystem::remove(offset_it->path(), ec);
                if (ec) {
                    return Status::IOError(ec.message());
                }
            }
        }
    }

    /// Shuffle cells to have random order in LRUQueue as at startup all cells have the same priority.
    auto rng = std::default_random_engine {
            static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count())};
    std::shuffle(queue_entries.begin(), queue_entries.end(), rng);
    for (const auto& [it, cache_type] : queue_entries) {
        auto& queue = get_queue(cache_type);
        queue.move_to_end(it, cache_lock);
    }
    return Status::OK();
}

size_t LRUFileCache::get_used_cache_size(CacheType cache_type) const {
    std::lock_guard cache_lock(_mutex);
    return get_used_cache_size_unlocked(cache_type, cache_lock);
}

size_t LRUFileCache::get_used_cache_size_unlocked(CacheType cache_type,
                                                  std::lock_guard<std::mutex>& cache_lock) const {
    return get_queue(cache_type).get_total_cache_size(cache_lock);
}

size_t LRUFileCache::get_available_cache_size(CacheType cache_type) const {
    std::lock_guard cache_lock(_mutex);
    return get_available_cache_size_unlocked(cache_type, cache_lock);
}

size_t LRUFileCache::get_available_cache_size_unlocked(
        CacheType cache_type, std::lock_guard<std::mutex>& cache_lock) const {
    return get_queue(cache_type).get_max_element_size() -
           get_used_cache_size_unlocked(cache_type, cache_lock);
}

size_t LRUFileCache::get_file_segments_num(CacheType cache_type) const {
    std::lock_guard cache_lock(_mutex);
    return get_file_segments_num_unlocked(cache_type, cache_lock);
}

size_t LRUFileCache::get_file_segments_num_unlocked(CacheType cache_type,
                                                    std::lock_guard<std::mutex>& cache_lock) const {
    return get_queue(cache_type).get_elements_num(cache_lock);
}

LRUFileCache::FileSegmentCell::FileSegmentCell(FileSegmentSPtr file_segment, CacheType cache_type,
                                               std::lock_guard<std::mutex>&)
        : file_segment(file_segment), cache_type(cache_type) {
    /**
     * Cell can be created with either DOWNLOADED or EMPTY file segment's state.
     * File segment acquires DOWNLOADING state and creates LRUQueue iterator on first
     * successful getOrSetDownaloder call.
     */

    switch (file_segment->_download_state) {
    case FileSegment::State::DOWNLOADED:
    case FileSegment::State::EMPTY:
    case FileSegment::State::SKIP_CACHE: {
        break;
    }
    default:
        DCHECK(false) << "Can create cell with either EMPTY, DOWNLOADED, SKIP_CACHE state, got: "
                      << FileSegment::state_to_string(file_segment->_download_state);
    }
}

IFileCache::LRUQueue::Iterator IFileCache::LRUQueue::add(
        const IFileCache::Key& key, size_t offset, size_t size,
        std::lock_guard<std::mutex>& /* cache_lock */) {
    cache_size += size;
    auto iter = queue.insert(queue.end(), FileKeyAndOffset(key, offset, size));
    map.insert(std::make_pair(std::make_pair(key, offset), iter));
    return iter;
}

void IFileCache::LRUQueue::remove(Iterator queue_it,
                                  std::lock_guard<std::mutex>& /* cache_lock */) {
    cache_size -= queue_it->size;
    map.erase(std::make_pair(queue_it->key, queue_it->offset));
    queue.erase(queue_it);
}

void IFileCache::LRUQueue::remove_all(std::lock_guard<std::mutex>& /* cache_lock */) {
    queue.clear();
    map.clear();
    cache_size = 0;
}

void IFileCache::LRUQueue::move_to_end(Iterator queue_it,
                                       std::lock_guard<std::mutex>& /* cache_lock */) {
    queue.splice(queue.end(), queue, queue_it);
}
bool IFileCache::LRUQueue::contains(const IFileCache::Key& key, size_t offset,
                                    std::lock_guard<std::mutex>& /* cache_lock */) const {
    return map.find(std::make_pair(key, offset)) != map.end();
}

IFileCache::LRUQueue::Iterator IFileCache::LRUQueue::get(
        const IFileCache::Key& key, size_t offset,
        std::lock_guard<std::mutex>& /* cache_lock */) const {
    return map.find(std::make_pair(key, offset))->second;
}

std::string IFileCache::LRUQueue::to_string(std::lock_guard<std::mutex>& /* cache_lock */) const {
    std::string result;
    for (const auto& [key, offset, size] : queue) {
        if (!result.empty()) {
            result += ", ";
        }
        result += fmt::format("{}: [{}, {}]", key.to_string(), offset, offset + size - 1);
    }
    return result;
}

std::string LRUFileCache::dump_structure(const Key& key) {
    std::lock_guard cache_lock(_mutex);
    return dump_structure_unlocked(key, cache_lock);
}

std::string LRUFileCache::dump_structure_unlocked(const Key& key, std::lock_guard<std::mutex>&) {
    std::stringstream result;
    const auto& cells_by_offset = _files[key];

    for (const auto& [_, cell] : cells_by_offset) {
        result << cell.file_segment->get_info_for_log() << " "
               << cache_type_to_string(cell.cache_type) << "\n";
    }

    return result.str();
}

void LRUFileCache::change_cache_type(const Key& key, size_t offset, CacheType new_type) {
    std::lock_guard cache_lock(_mutex);
    if (auto iter = _files.find(key); iter != _files.end()) {
        auto& file_segments = iter->second;
        if (auto cell_it = file_segments.find(offset); cell_it != file_segments.end()) {
            FileSegmentCell& cell = cell_it->second;
            auto& cur_queue = get_queue(cell.cache_type);
            cell.cache_type = new_type;
            DCHECK(cell.queue_iterator.has_value());
            cur_queue.remove(*cell.queue_iterator, cache_lock);
            auto& new_queue = get_queue(new_type);
            cell.queue_iterator =
                    new_queue.add(key, offset, cell.file_segment->range().size(), cache_lock);
        }
    }
}

void LRUFileCache::run_background_operation() {
    int64_t interval_time_seconds = 20;
    while (!_close) {
        std::this_thread::sleep_for(std::chrono::seconds(interval_time_seconds));
        // gc
        int64_t cur_time = UnixSeconds();
        std::lock_guard cache_lock(_mutex);
        while (!_time_to_key.empty()) {
            auto begin = _time_to_key.begin();
            if (cur_time < begin->first) {
                break;
            }
            remove_if_ttl_file_unlock(begin->second, false, cache_lock);
        }

        // report
        _cur_size_metrics->set_value(_cur_cache_size);
        _cur_ttl_cache_size_metrics->set_value(_cur_cache_size -
                                               _index_queue.get_total_cache_size(cache_lock) -
                                               _normal_queue.get_total_cache_size(cache_lock) -
                                               _disposable_queue.get_total_cache_size(cache_lock));
    }
}

void LRUFileCache::modify_expiration_time(const IFileCache::Key& key, int64_t new_expiration_time) {
    std::lock_guard cache_lock(_mutex);
    // 1. If new_expiration_time is equal to zero
    if (new_expiration_time == 0) {
        remove_if_ttl_file_unlock(key, false, cache_lock);
        return;
    }
    // 2. If the key in ttl cache, modify its expiration time.
    if (auto iter = _key_to_time.find(key); iter != _key_to_time.end()) {
        std::error_code ec;
        std::filesystem::rename(get_path_in_local_cache(key, iter->second),
                                get_path_in_local_cache(key, new_expiration_time), ec);
        if (ec) [[unlikely]] {
            LOG(ERROR) << ec.message();
        } else {
            // remove from _time_to_key
            auto _time_to_key_iter = _time_to_key.equal_range(iter->second);
            while (_time_to_key_iter.first != _time_to_key_iter.second) {
                if (_time_to_key_iter.first->second == key) {
                    _time_to_key_iter.first = _time_to_key.erase(_time_to_key_iter.first);
                    break;
                }
                _time_to_key_iter.first++;
            }
            _time_to_key.insert(std::make_pair(new_expiration_time, key));
            iter->second = new_expiration_time;
            for (auto& [_, cell] : _files[key]) {
                cell.file_segment->update_expiration_time(new_expiration_time);
            }
        }
        return;
    }
    // 3. change to ttl if the segments aren't ttl
    if (auto iter = _files.find(key); iter != _files.end()) {
        for (auto& [_, cell] : iter->second) {
            if (cell.file_segment->change_cache_type(CacheType::TTL)) {
                auto& queue = get_queue(cell.cache_type);
                queue.remove(cell.queue_iterator.value(), cache_lock);
                cell.cache_type = CacheType::TTL;
                cell.file_segment->update_expiration_time(new_expiration_time);
            }
        }
        _key_to_time[key] = new_expiration_time;
        _time_to_key.insert(std::make_pair(new_expiration_time, key));
        std::error_code ec;
        std::filesystem::rename(get_path_in_local_cache(key, 0),
                                get_path_in_local_cache(key, new_expiration_time), ec);
        if (ec) {
            LOG(ERROR) << ec.message();
        } else {
            DCHECK(std::filesystem::exists(get_path_in_local_cache(key, new_expiration_time)));
        }
    }
}

} // namespace io
} // namespace doris
