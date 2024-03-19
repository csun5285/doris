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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Interpreters/Cache/FileCache.cpp
// and modified by Doris

#include "io/cache/block/block_file_cache.h"

#include <glog/logging.h>
// IWYU pragma: no_include <bits/chrono.h>
#include <sys/statfs.h>

#include <chrono> // IWYU pragma: keep
#include <filesystem>
#include <memory>
#include <ranges>
#include <string>
#include <system_error>
#include <utility>

#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "common/sync_point.h"
#include "io/cache/block/block_file_cache_fwd.h"
#include "io/cache/block/block_file_cache_settings.h"
#include "io/fs/local_file_system.h"
#include "util/defer_op.h"
#include "util/time.h"
#include "vec/common/hex.h"
#include "vec/common/sip_hash.h"

namespace fs = std::filesystem;

namespace doris {
namespace io {

BlockFileCache::BlockFileCache(const std::string& cache_base_path,
                               const FileCacheSettings& cache_settings)
        : _cache_base_path(cache_base_path),
          _total_size(cache_settings.total_size),
          _max_file_block_size(cache_settings.max_file_block_size),
          _max_query_cache_size(cache_settings.max_query_cache_size) {
    _cur_cache_size_metrics = std::make_shared<bvar::Status<size_t>>(_cache_base_path.c_str(),
                                                                     "file_cache_cache_size", 0);
    _cur_ttl_cache_size_metrics = std::make_shared<bvar::Status<size_t>>(
            _cache_base_path.c_str(), "file_cache_ttl_cache_size", 0);
    _cur_normal_queue_element_count_metrics = std::make_shared<bvar::Status<size_t>>(
            _cache_base_path.c_str(), "file_cache_normal_queue_element_count", 0);
    _cur_normal_queue_cache_size_metrics = std::make_shared<bvar::Status<size_t>>(
            _cache_base_path.c_str(), "file_cache_normal_queue_cache_size", 0);
    _cur_index_queue_element_count_metrics = std::make_shared<bvar::Status<size_t>>(
            _cache_base_path.c_str(), "file_cache_index_queue_element_count", 0);
    _cur_index_queue_cache_size_metrics = std::make_shared<bvar::Status<size_t>>(
            _cache_base_path.c_str(), "file_cache_index_queue_cache_size", 0);
    _cur_disposable_queue_element_count_metrics = std::make_shared<bvar::Status<size_t>>(
            _cache_base_path.c_str(), "file_cache_disposable_queue_element_count", 0);
    _cur_disposable_queue_cache_size_metrics = std::make_shared<bvar::Status<size_t>>(
            _cache_base_path.c_str(), "file_cache_disposable_queue_cache_size", 0);
    _queue_evict_size_metrics[0] = std::make_shared<bvar::Adder<size_t>>(
            _cache_base_path.c_str(), "file_cache_index_queue_evict_size");
    _queue_evict_size_metrics[1] = std::make_shared<bvar::Adder<size_t>>(
            _cache_base_path.c_str(), "file_cache_normal_queue_evict_size");
    _queue_evict_size_metrics[2] = std::make_shared<bvar::Adder<size_t>>(
            _cache_base_path.c_str(), "file_cache_disposable_queue_evict_size");
    _queue_evict_size_metrics[3] = std::make_shared<bvar::Adder<size_t>>(
            _cache_base_path.c_str(), "file_cache_ttl_cache_evict_size");
    _total_evict_size_metrics = std::make_shared<bvar::Adder<size_t>>(
            _cache_base_path.c_str(), "file_cache_total_evict_size");

    _disposable_queue = LRUQueue(cache_settings.disposable_queue_size,
                                 cache_settings.disposable_queue_elements, 60 * 60);
    _index_queue = LRUQueue(cache_settings.index_queue_size, cache_settings.index_queue_elements,
                            7 * 24 * 60 * 60);
    _normal_queue = LRUQueue(cache_settings.query_queue_size, cache_settings.query_queue_elements,
                             24 * 60 * 60);

    LOG(INFO) << fmt::format(
            "File Cache Config: file cache path={}, disposable queue size={} elements={}, index "
            "queue size={} "
            "elements={}, query queue "
            "size={} elements={}",
            cache_base_path, cache_settings.disposable_queue_size,
            cache_settings.disposable_queue_elements, cache_settings.index_queue_size,
            cache_settings.index_queue_elements, cache_settings.query_queue_size,
            cache_settings.query_queue_elements);
}

std::string Key::to_string() const {
    return vectorized::get_hex_uint_lowercase(key);
}

Key BlockFileCache::hash(const std::string& path) {
    uint128_t key;
    sip_hash128(path.data(), path.size(), reinterpret_cast<char*>(&key));
    return Key(key);
}

std::string BlockFileCache::cache_type_to_string(FileCacheType type) {
    switch (type) {
    case FileCacheType::INDEX:
        return "_idx";
    case FileCacheType::DISPOSABLE:
        return "_disposable";
    case FileCacheType::NORMAL:
        return "";
    case FileCacheType::TTL:
        return "_ttl";
    }
    return "";
}

FileCacheType BlockFileCache::string_to_cache_type(const std::string& str) {
    switch (str[0]) {
    case 'i':
        return FileCacheType::INDEX;
    case 'd':
        return FileCacheType::DISPOSABLE;
    case 't':
        return FileCacheType::TTL;
    default:
        DCHECK(false);
    }
    return FileCacheType::DISPOSABLE;
}

std::string BlockFileCache::get_path_in_local_cache(const Key& key, uint64_t expiration_time,
                                                    size_t offset, FileCacheType type,
                                                    bool is_tmp) const {
    return get_path_in_local_cache(key, expiration_time) /
           (std::to_string(offset) + (is_tmp ? "_tmp" : cache_type_to_string(type)));
}

std::string BlockFileCache::get_path_in_local_cache(const Key& key,
                                                    uint64_t expiration_time) const {
    auto key_str = key.to_string();
    try {
        return Path(_cache_base_path) / (key_str + "_" + std::to_string(expiration_time));
    } catch (std::filesystem::filesystem_error& e) {
        LOG(WARNING) << "fail to get_path_in_local_cache=" << e.what();
        return "";
    }
}

BlockFileCache::QueryFileCacheContextHolderPtr BlockFileCache::get_query_context_holder(
        const TUniqueId& query_id) {
    std::lock_guard cache_lock(_mutex);

    if (!_enable_file_cache_query_limit) {
        return {};
    }

    /// if enable_filesystem_query_cache_limit is true,
    /// we create context query for current query.
    auto context = get_or_set_query_context(query_id, cache_lock);
    return std::make_unique<QueryFileCacheContextHolder>(query_id, this, context);
}

BlockFileCache::QueryFileCacheContextPtr BlockFileCache::get_query_context(
        const TUniqueId& query_id, std::lock_guard<doris::Mutex>& cache_lock) {
    auto query_iter = _query_map.find(query_id);
    return (query_iter == _query_map.end()) ? nullptr : query_iter->second;
}

void BlockFileCache::remove_query_context(const TUniqueId& query_id) {
    std::lock_guard cache_lock(_mutex);
    const auto& query_iter = _query_map.find(query_id);

    if (query_iter != _query_map.end() && query_iter->second.unique()) {
        _query_map.erase(query_iter);
    }
}

BlockFileCache::QueryFileCacheContextPtr BlockFileCache::get_or_set_query_context(
        const TUniqueId& query_id, std::lock_guard<doris::Mutex>& cache_lock) {
    if (query_id.lo == 0 && query_id.hi == 0) {
        return nullptr;
    }

    auto context = get_query_context(query_id, cache_lock);
    if (context) {
        return context;
    }

    auto query_context = std::make_shared<QueryFileCacheContext>(_max_query_cache_size);
    auto query_iter = _query_map.emplace(query_id, query_context).first;
    return query_iter->second;
}

void BlockFileCache::QueryFileCacheContext::remove(const Key& key, size_t offset,
                                                   std::lock_guard<doris::Mutex>& cache_lock) {
    auto pair = std::make_pair(key, offset);
    auto record = records.find(pair);
    DCHECK(record != records.end());
    auto iter = record->second;
    records.erase(pair);
    lru_queue.remove(iter, cache_lock);
}

void BlockFileCache::QueryFileCacheContext::reserve(const Key& key, size_t offset, size_t size,
                                                    std::lock_guard<doris::Mutex>& cache_lock) {
    auto pair = std::make_pair(key, offset);
    if (records.find(pair) == records.end()) {
        auto queue_iter = lru_queue.add(key, offset, size, cache_lock);
        records.insert({pair, queue_iter});
    }
}

Status BlockFileCache::initialize() {
    std::lock_guard cache_lock(_mutex);
    return initialize_unlocked(cache_lock);
}

Status BlockFileCache::initialize_unlocked(std::lock_guard<doris::Mutex>& cache_lock) {
    auto& local_fs = io::global_local_filesystem();
    auto st = local_fs->create_directory(_cache_base_path, true);
    if (st.is<ErrorCode::ALREADY_EXIST>()) {
        _cache_background_load_thread = std::thread([&]() {
            _lazy_open_done = false;
            load_cache_info_into_memory();
            _lazy_open_done = true;
            std::lock_guard cache_lock(_mutex);
            LOG(INFO) << fmt::format(
                    "Cur File Cache: file cache path={}, disposable queue size={} elements={}, "
                    "index queue size={} "
                    "elements={}, query queue "
                    "size={} elements={}",
                    _cache_base_path, _disposable_queue.get_total_cache_size(cache_lock),
                    _disposable_queue.get_elements_num(cache_lock),
                    _index_queue.get_total_cache_size(cache_lock),
                    _index_queue.get_elements_num(cache_lock),
                    _normal_queue.get_total_cache_size(cache_lock),
                    _normal_queue.get_elements_num(cache_lock));
        });
    } else if (!st.ok()) {
        return st;
    } else { // create OK
        _lazy_open_done = true;
    }
    _cache_background_thread = std::thread(&BlockFileCache::run_background_operation, this);
    return Status::OK();
}

void BlockFileCache::use_cell(const FileBlockCell& cell, FileBlocks* result, bool move_iter_flag,
                              std::lock_guard<doris::Mutex>& cache_lock) {
    if (result) {
        auto file_block = cell.file_block;
        DCHECK(!(file_block->is_downloaded() &&
                 fs::file_size(
                         get_path_in_local_cache(file_block->key(), file_block->expiration_time(),
                                                 file_block->offset(), cell.cache_type)) == 0))
                << "Cannot have zero size downloaded file segments. Current file segment: "
                << file_block->range().to_string();

        result->push_back(std::move(file_block));
    }

    if (cell.cache_type != FileCacheType::TTL) {
        auto& queue = get_queue(cell.cache_type);
        DCHECK(cell.queue_iterator) << "impossible";
        /// Move to the end of the queue. The iterator remains valid.
        if (move_iter_flag) {
            queue.move_to_end(*cell.queue_iterator, cache_lock);
        }
    }
    cell.update_atime();
    cell.is_deleted = false;
}

Status BlockFileCache::clear_file_cache_directly() {
    using namespace std::chrono;
    auto start_time = steady_clock::time_point();
    std::lock_guard cache_lock(_mutex);
    LOG_INFO("Start clear file cache directly").tag("path", _cache_base_path);
    RETURN_IF_ERROR(global_local_filesystem()->delete_directory(_cache_base_path));
    RETURN_IF_ERROR(global_local_filesystem()->create_directory(_cache_base_path));
    _files.clear();
    _cur_cache_size = 0;
    _cur_ttl_size = 0;
    _time_to_key.clear();
    _key_to_time.clear();
    _index_queue.clear(cache_lock);
    _normal_queue.clear(cache_lock);
    _disposable_queue.clear(cache_lock);
    auto use_time = duration_cast<milliseconds>(steady_clock::time_point() - start_time);
    LOG_INFO("End clear file cache directly")
            .tag("path", _async_clear_file_cache)
            .tag("use_time", static_cast<int64_t>(use_time.count()));
    return Status::OK();
}

void BlockFileCache::clear_file_cache_async() {
    {
        std::lock_guard cache_lock(_mutex);
        if (!_async_clear_file_cache) {
            for (auto& [_, offset_to_cell] : _files) {
                for (auto& [_, cell] : offset_to_cell) {
                    if (cell.releasable()) {
                        cell.is_deleted = true;
                    }
                }
            }
            _async_clear_file_cache = true;
        }
    }
}

void BlockFileCache::recycle_deleted_blocks() {
    using namespace std::chrono;
    static int remove_batch = 100;
    TEST_SYNC_POINT_CALLBACK("BlockFileCache::set_remove_batch", &remove_batch);
    TEST_SYNC_POINT_CALLBACK("BlockFileCache::recycle_deleted_blocks");
    std::unique_lock cache_lock(_mutex);
    auto remove_file_block = [&cache_lock, this](FileBlockCell* cell) {
        std::lock_guard segment_lock(cell->file_block->_mutex);
        remove(cell->file_block, cache_lock, segment_lock);
    };
    int i = 0;
    std::condition_variable cond;
    auto start_time = steady_clock::time_point();
    if (_async_clear_file_cache) {
        LOG_INFO("Start clear file cache async").tag("path", _cache_base_path);
        auto remove_file_block = [&cache_lock, this](FileBlockCell* cell) {
            std::lock_guard segment_lock(cell->file_block->_mutex);
            remove(cell->file_block, cache_lock, segment_lock);
        };
        static int remove_batch = 100;
        TEST_SYNC_POINT_CALLBACK("BlockFileCache::set_remove_batch", &remove_batch);
        int i = 0;
        std::condition_variable cond;
        auto iter_queue = [&](LRUQueue& queue) {
            bool end = false;
            while (queue.get_total_cache_size(cache_lock) != 0 && !end) {
                std::vector<FileBlockCell*> cells;
                for (const auto& [entry_key, entry_offset, _] : queue) {
                    if (i == remove_batch) {
                        i = 0;
                        break;
                    }
                    auto* cell = get_cell(entry_key, entry_offset, cache_lock);
                    if (!cell->is_deleted) {
                        end = true;
                        break;
                    } else if (cell->releasable()) {
                        i++;
                        cells.push_back(cell);
                    }
                }
                std::ranges::for_each(cells, remove_file_block);
                // just for sleep
                cond.wait_for(cache_lock, std::chrono::microseconds(100));
            }
        };
        iter_queue(get_queue(FileCacheType::DISPOSABLE));
        iter_queue(get_queue(FileCacheType::NORMAL));
        iter_queue(get_queue(FileCacheType::INDEX));
    }
    if (_async_clear_file_cache || config::file_cache_ttl_valid_check_interval_second != 0) {
        std::vector<Key> ttl_keys;
        ttl_keys.reserve(_key_to_time.size());
        for (auto& [key, _] : _key_to_time) {
            ttl_keys.push_back(key);
        }
        for (Key& key : ttl_keys) {
            if (i >= remove_batch) {
                // just for sleep
                cond.wait_for(cache_lock, std::chrono::microseconds(100));
                i = 0;
            }
            if (auto iter = _files.find(key); iter != _files.end()) {
                std::vector<FileBlockCell*> cells;
                cells.reserve(iter->second.size());
                for (auto& [_, cell] : iter->second) {
                    cell.is_deleted =
                            cell.is_deleted
                                    ? true
                                    : (config::file_cache_ttl_valid_check_interval_second == 0
                                               ? false
                                               : std::chrono::duration_cast<std::chrono::seconds>(
                                                         std::chrono::steady_clock::now()
                                                                 .time_since_epoch())
                                                                         .count() -
                                                                 cell.atime >
                                                         config::file_cache_ttl_valid_check_interval_second);
                    if (!cell.is_deleted) {
                        continue;
                    } else if (cell.releasable()) {
                        cells.emplace_back(&cell);
                        i++;
                    }
                }
                std::ranges::for_each(cells, remove_file_block);
            }
        }
        if (_async_clear_file_cache) {
            _async_clear_file_cache = false;
            auto use_time = duration_cast<milliseconds>(steady_clock::time_point() - start_time);
            LOG_INFO("End clear file cache async")
                    .tag("path", _async_clear_file_cache)
                    .tag("use_time", static_cast<int64_t>(use_time.count()));
        }
    }
}

template <class T>
    requires IsXLock<T>
BlockFileCache::FileBlockCell* BlockFileCache::get_cell(const Key& key, size_t offset,
                                                        T& /* cache_lock */) {
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

bool BlockFileCache::need_to_move(FileCacheType cell_type, FileCacheType query_type) const {
    return query_type != FileCacheType::DISPOSABLE && cell_type != FileCacheType::DISPOSABLE;
}

FileBlocks BlockFileCache::get_impl(const Key& key, const CacheContext& context,
                                    const FileBlock::Range& range,
                                    std::lock_guard<doris::Mutex>& cache_lock) {
    /// Given range = [left, right] and non-overlapping ordered set of file segments,
    /// find list [segment1, ..., segmentN] of segments which intersect with given range.
    auto it = _files.find(key);
    if (it == _files.end()) {
        if (_lazy_open_done) {
            return {};
        }
        // async load, can't find key, need to check exist.
        auto key_path = get_path_in_local_cache(key, context.expiration_time);
        std::error_code ec;
        if (bool is_exist = std::filesystem::exists(key_path, ec); !is_exist && !ec) {
            // cache miss
            return {};
        } else if (ec) [[unlikely]] {
            LOG(WARNING) << "filesystem error, failed to exists file, file=" << key_path
                         << " error=" << ec.message();
            return {};
        }

        CacheContext context_original;
        context_original.query_id = TUniqueId();
        context_original.expiration_time = context.expiration_time;

        // load key into mem
        std::error_code dec;
        std::filesystem::directory_iterator check_it(key_path, dec);
        if (dec) [[unlikely]] {
            LOG(ERROR) << "fail to directory_iterator=" << std::strerror(dec.value());
            return {};
        }
        for (; check_it != std::filesystem::directory_iterator(); ++check_it) {
            uint64_t offset = 0;
            std::string offset_with_suffix = check_it->path().filename().native();
            auto delim_pos1 = offset_with_suffix.find('_');
            FileCacheType cache_type = FileCacheType::NORMAL;
            bool parsed = true;
            bool is_tmp = false;
            try {
                if (delim_pos1 == std::string::npos) {
                    // same as type "normal"
                    offset = stoull(offset_with_suffix);
                } else {
                    offset = stoull(offset_with_suffix.substr(0, delim_pos1));
                    std::string suffix = offset_with_suffix.substr(delim_pos1 + 1);
                    if (suffix == "tmp") [[unlikely]] {
                        is_tmp = true;
                    } else {
                        cache_type = string_to_cache_type(suffix);
                    }
                }
            } catch (...) {
                parsed = false;
            }

            if (!parsed) [[unlikely]] {
                LOG(WARNING) << "parse offset err, path=" << offset_with_suffix;
                continue;
            }
            TEST_SYNC_POINT_CALLBACK("BlockFileCache::REMOVE_FILE_1", &offset_with_suffix);
            std::error_code ec;
            size_t size = check_it->file_size(ec);
            if (ec) {
                LOG(WARNING) << "failed to file_size: error=" << ec.message();
                continue;
            }
            if (size == 0 && !is_tmp) [[unlikely]] {
                std::filesystem::remove(check_it->path(), ec);
                if (ec) [[unlikely]] {
                    LOG(WARNING) << "filesystem error, failed to remove file, file="
                                 << check_it->path() << " error=" << ec.message();
                }
                continue;
            }
            if (_files.count(key) == 0 || _files[key].count(offset) == 0) {
                // if the file is tmp, it means it is the old file and it should be removed
                if (is_tmp) {
                    std::error_code ec;
                    std::filesystem::remove(check_it->path(), ec);
                    if (ec) {
                        LOG(WARNING) << fmt::format("cannot remove {}: {}",
                                                    check_it->path().native(), ec.message());
                    }
                } else {
                    context_original.cache_type = cache_type;
                    add_cell(key, context_original, offset, size, FileBlock::State::DOWNLOADED,
                             cache_lock);
                }
            }
        }

        it = _files.find(key);
        if (it == _files.end()) [[unlikely]] {
            return {};
        }
    }

    auto& file_blocks = it->second;
    if (file_blocks.empty()) {
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
        /// because cache files are deleted only inside BlockFileCache and under cache lock.
        std::error_code ec;
        if (bool is_exist = std::filesystem::exists(key_path, ec); is_exist && !ec) {
            std::filesystem::remove_all(key_path, ec);
            if (ec) [[unlikely]] {
                LOG(WARNING) << "filesystem error, failed to remove_all file, file=" << key_path
                             << " error=" << ec.message();
            }
        } else if (ec) [[unlikely]] {
            LOG(WARNING) << "filesystem error, failed to exists file, file=" << key_path
                         << " error=" << ec.message();
        }
        return {};
    }

    // change to ttl if the segments aren't ttl
    if (context.cache_type == FileCacheType::TTL && _key_to_time.find(key) == _key_to_time.end()) {
        for (auto& [_, cell] : file_blocks) {
            if (cell.file_block->change_cache_type(FileCacheType::TTL)) {
                auto& queue = get_queue(cell.cache_type);
                queue.remove(cell.queue_iterator.value(), cache_lock);
                cell.queue_iterator.reset();
                cell.cache_type = FileCacheType::TTL;
                cell.file_block->update_expiration_time(context.expiration_time);
            }
        }
        _key_to_time[key] = context.expiration_time;
        _time_to_key.insert(std::make_pair(context.expiration_time, key));
        Status st = global_local_filesystem()->rename(
                get_path_in_local_cache(key, 0),
                get_path_in_local_cache(key, context.expiration_time));
        if (!st.ok()) {
            LOG_WARNING("").error(st);
        } else {
            DCHECK(std::filesystem::exists(get_path_in_local_cache(key, context.expiration_time)));
        }
    }
    if (auto iter = _key_to_time.find(key);
        (context.cache_type == FileCacheType::NORMAL || context.cache_type == FileCacheType::TTL) &&
        iter != _key_to_time.end() && iter->second != context.expiration_time) {
        Status st = global_local_filesystem()->rename(
                get_path_in_local_cache(key, iter->second),
                get_path_in_local_cache(key, context.expiration_time));
        if (!st.ok()) [[unlikely]] {
            LOG(WARNING) << "filesystem error, failed to rename file, from file="
                         << get_path_in_local_cache(key, iter->second)
                         << " to file=" << get_path_in_local_cache(key, context.expiration_time)
                         << " error=" << st;
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
            for (auto& [_, cell] : file_blocks) {
                cell.file_block->update_expiration_time(context.expiration_time);
            }
            if (context.expiration_time == 0) {
                for (auto& [_, cell] : file_blocks) {
                    if (cell.file_block->change_cache_type(FileCacheType::NORMAL)) {
                        auto& queue = get_queue(FileCacheType::NORMAL);
                        cell.queue_iterator =
                                queue.add(cell.file_block->key(), cell.file_block->offset(),
                                          cell.file_block->range().size(), cache_lock);
                        cell.cache_type = FileCacheType::NORMAL;
                    }
                }
                _key_to_time.erase(iter);
            } else {
                _time_to_key.insert(std::make_pair(context.expiration_time, key));
                iter->second = context.expiration_time;
            }
        }
    }

    FileBlocks result;
    auto segment_it = file_blocks.lower_bound(range.left);
    if (segment_it == file_blocks.end()) {
        /// N - last cached segment for given file key, segment{N}.offset < range.left:
        ///   segment{N}                       segment{N}
        /// [________                         [_______]
        ///     [__________]         OR                  [________]
        ///     ^                                        ^
        ///     range.left                               range.left

        const auto& cell = file_blocks.rbegin()->second;
        if (cell.file_block->range().right < range.left) {
            return {};
        }

        use_cell(cell, &result, need_to_move(cell.cache_type, context.cache_type), cache_lock);
    } else { /// segment_it <-- segmment{k}
        if (segment_it != file_blocks.begin()) {
            const auto& prev_cell = std::prev(segment_it)->second;
            const auto& prev_cell_range = prev_cell.file_block->range();

            if (range.left <= prev_cell_range.right) {
                ///   segment{k-1}  segment{k}
                ///   [________]   [_____
                ///       [___________
                ///       ^
                ///       range.left

                use_cell(prev_cell, &result, need_to_move(prev_cell.cache_type, context.cache_type),
                         cache_lock);
            }
        }

        ///  segment{k} ...       segment{k-1}  segment{k}                      segment{k}
        ///  [______              [______]     [____                        [________
        ///  [_________     OR              [________      OR    [______]   ^
        ///  ^                              ^                           ^   segment{k}.offset
        ///  range.left                     range.left                  range.right

        while (segment_it != file_blocks.end()) {
            const auto& cell = segment_it->second;
            if (range.right < cell.file_block->range().left) {
                break;
            }

            use_cell(cell, &result, need_to_move(cell.cache_type, context.cache_type), cache_lock);
            ++segment_it;
        }
    }

    return result;
}

FileBlocks BlockFileCache::split_range_into_cells(const Key& key, const CacheContext& context,
                                                  size_t offset, size_t size,
                                                  FileBlock::State state,
                                                  std::lock_guard<doris::Mutex>& cache_lock) {
    DCHECK(size > 0);

    auto current_pos = offset;
    auto end_pos_non_included = offset + size;

    size_t current_size = 0;
    size_t remaining_size = size;

    FileBlocks file_blocks;
    while (current_pos < end_pos_non_included) {
        current_size = std::min(remaining_size, _max_file_block_size);
        remaining_size -= current_size;
        state = try_reserve(key, context, current_pos, current_size, cache_lock)
                        ? state
                        : FileBlock::State::SKIP_CACHE;
        if (UNLIKELY(state == FileBlock::State::SKIP_CACHE)) {
            auto file_block = std::make_shared<FileBlock>(
                    current_pos, current_size, key, this, FileBlock::State::SKIP_CACHE,
                    context.cache_type, context.expiration_time);
            file_blocks.push_back(std::move(file_block));
        } else {
            auto* cell = add_cell(key, context, current_pos, current_size, state, cache_lock);
            if (cell) {
                file_blocks.push_back(cell->file_block);
                if (!context.is_cold_data) {
                    cell->update_atime();
                }
            } else {
                auto file_block = std::make_shared<FileBlock>(
                        current_pos, current_size, key, this, FileBlock::State::SKIP_CACHE,
                        context.cache_type, context.expiration_time);
                file_blocks.push_back(std::move(file_block));
            }
        }

        current_pos += current_size;
    }

    DCHECK(file_blocks.empty() || offset + size - 1 == file_blocks.back()->range().right);
    return file_blocks;
}

void BlockFileCache::fill_holes_with_empty_file_blocks(FileBlocks& file_blocks, const Key& key,
                                                       const CacheContext& context,
                                                       const FileBlock::Range& range,
                                                       std::lock_guard<doris::Mutex>& cache_lock) {
    /// There are segments [segment1, ..., segmentN]
    /// (non-overlapping, non-empty, ascending-ordered) which (maybe partially)
    /// intersect with given range.

    /// It can have holes:
    /// [____________________]         -- requested range
    ///     [____]  [_]   [_________]  -- intersecting cache [segment1, ..., segmentN]
    ///
    /// For each such hole create a cell with file segment state EMPTY.

    auto it = file_blocks.begin();
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

    while (current_pos <= range.right && it != file_blocks.end()) {
        segment_range = (*it)->range();

        if (current_pos == segment_range.left) {
            current_pos = segment_range.right + 1;
            ++it;
            continue;
        }

        DCHECK(current_pos < segment_range.left);

        auto hole_size = segment_range.left - current_pos;

        file_blocks.splice(it, split_range_into_cells(key, context, current_pos, hole_size,
                                                      FileBlock::State::EMPTY, cache_lock));

        current_pos = segment_range.right + 1;
        ++it;
    }

    if (current_pos <= range.right) {
        ///   ________]     -- requested range
        ///   _____]
        ///        ^
        /// segmentN

        auto hole_size = range.right - current_pos + 1;

        file_blocks.splice(file_blocks.end(),
                           split_range_into_cells(key, context, current_pos, hole_size,
                                                  FileBlock::State::EMPTY, cache_lock));
    }
}

std::map<size_t, FileBlockSPtr> BlockFileCache::get_blocks_by_key(const Key& key) {
    std::map<size_t, FileBlockSPtr> offset_to_block;
    std::lock_guard cache_lock(_mutex);
    if (_files.count(key) > 0) {
        for (auto& [offset, cell] : _files[key]) {
            if (cell.file_block->is_downloaded()) {
                offset_to_block.emplace(offset, cell.file_block);
                use_cell(cell, nullptr, need_to_move(cell.cache_type, FileCacheType::DISPOSABLE),
                         cache_lock);
            }
        }
    }
    return offset_to_block;
}

FileBlocksHolder BlockFileCache::get_or_set(const Key& key, size_t offset, size_t size,
                                            const CacheContext& context) {
    FileBlock::Range range(offset, offset + size - 1);

    std::lock_guard cache_lock(_mutex);
    /// Get all segments which intersect with the given range.
    auto file_blocks = get_impl(key, context, range, cache_lock);

    if (file_blocks.empty()) {
        file_blocks = split_range_into_cells(key, context, offset, size, FileBlock::State::EMPTY,
                                             cache_lock);
    } else {
        fill_holes_with_empty_file_blocks(file_blocks, key, context, range, cache_lock);
    }

    DCHECK(!file_blocks.empty());
    return FileBlocksHolder(std::move(file_blocks));
}

BlockFileCache::FileBlockCell* BlockFileCache::add_cell(const Key& key, const CacheContext& context,
                                                        size_t offset, size_t size,
                                                        FileBlock::State state,
                                                        std::lock_guard<doris::Mutex>& cache_lock) {
    /// Create a file segment cell and put it in `files` map by [key][offset].
    if (size == 0) {
        return nullptr; /// Empty files are not cached.
    }

    DCHECK_EQ(_files[key].count(offset), 0)
            << "Cache already exists for key: " << key.to_string() << ", offset: " << offset
            << ", size: " << size
            << ".\nCurrent cache structure: " << dump_structure_unlocked(key, cache_lock);

    auto& offsets = _files[key];
    DCHECK((context.expiration_time == 0 && context.cache_type != FileCacheType::TTL) ||
           (context.cache_type == FileCacheType::TTL && context.expiration_time != 0))
            << fmt::format("expiration time {}, cache type {}", context.expiration_time,
                           context.cache_type);
    auto key_path = get_path_in_local_cache(key, context.expiration_time);
    if (offsets.empty()) {
        Status st = global_local_filesystem()->create_directory(key_path);
        if (!st.ok()) [[unlikely]] {
            LOG_WARNING("Cannot create").tag("dir", key_path).error(st);
            state = FileBlock::State::SKIP_CACHE;
        }
    }

    if (state == FileBlock::State::SKIP_CACHE) {
        return nullptr;
    }

    DCHECK([&]() -> bool {
        bool res;
        Status st = global_local_filesystem()->exists(key_path, &res);
        return st.ok() && res;
    }());

    FileBlockCell cell(std::make_shared<FileBlock>(offset, size, key, this, state,
                                                   context.cache_type, context.expiration_time),
                       context.cache_type, cache_lock);

    if (context.cache_type != FileCacheType::TTL) {
        auto& queue = get_queue(context.cache_type);
        cell.queue_iterator = queue.add(key, offset, size, cache_lock);
    } else {
        if (_key_to_time.find(key) == _key_to_time.end()) {
            _key_to_time[key] = context.expiration_time;
            _time_to_key.insert(std::make_pair(context.expiration_time, key));
        }
        _cur_ttl_size += cell.size();
    }
    auto [it, inserted] = offsets.insert({offset, std::move(cell)});
    _cur_cache_size += size;

    DCHECK(inserted) << "Failed to insert into cache key: " << key.to_string()
                     << ", offset: " << offset << ", size: " << size;

    return &(it->second);
}

BlockFileCache::LRUQueue& BlockFileCache::get_queue(FileCacheType type) {
    switch (type) {
    case FileCacheType::INDEX:
        return _index_queue;
    case FileCacheType::DISPOSABLE:
        return _disposable_queue;
    case FileCacheType::NORMAL:
        return _normal_queue;
    default:
        DCHECK(false);
    }
    return _normal_queue;
}

const BlockFileCache::LRUQueue& BlockFileCache::get_queue(FileCacheType type) const {
    switch (type) {
    case FileCacheType::INDEX:
        return _index_queue;
    case FileCacheType::DISPOSABLE:
        return _disposable_queue;
    case FileCacheType::NORMAL:
        return _normal_queue;
    default:
        DCHECK(false);
    }
    return _normal_queue;
}

bool BlockFileCache::try_reserve_for_ttl(size_t size, std::lock_guard<doris::Mutex>& cache_lock) {
    size_t removed_size = 0;
    size_t cur_cache_size = _cur_cache_size;
    auto limit = config::max_ttl_cache_ratio * _total_size;
    if ((_cur_ttl_size + size) * 100 > limit) {
        return false;
    }
    auto remove_file_block_if = [&](FileBlockCell* cell) {
        FileBlockSPtr file_block = cell->file_block;
        if (file_block) {
            std::lock_guard segment_lock(file_block->_mutex);
            remove(file_block, cache_lock, segment_lock);
        }
    };
    size_t normal_queue_size = _normal_queue.get_total_cache_size(cache_lock);
    size_t disposable_queue_size = _disposable_queue.get_total_cache_size(cache_lock);
    size_t index_queue_size = _index_queue.get_total_cache_size(cache_lock);
    if (is_overflow(removed_size, size, cur_cache_size) && normal_queue_size == 0 &&
        disposable_queue_size == 0 && index_queue_size == 0) {
        return false;
    }
    std::vector<FileBlockCell*> to_evict;
    auto collect_eliminate_fragments = [&](LRUQueue& queue) {
        for (const auto& [entry_key, entry_offset, entry_size] : queue) {
            if (!is_overflow(removed_size, size, cur_cache_size)) {
                break;
            }
            auto* cell = get_cell(entry_key, entry_offset, cache_lock);

            DCHECK(cell) << "Cache became inconsistent. Key: " << entry_key.to_string()
                         << ", offset: " << entry_offset;

            size_t cell_size = cell->size();
            DCHECK(entry_size == cell_size);

            /// It is guaranteed that cell is not removed from cache as long as
            /// pointer to corresponding file segment is hold by any other thread.
            if (!cell->releasable()) {
                continue;
            }

            auto& file_block = cell->file_block;

            std::lock_guard segment_lock(file_block->_mutex);
            DCHECK(file_block->_download_state == FileBlock::State::DOWNLOADED);
            to_evict.push_back(cell);
            removed_size += cell_size;
        }
    };
    if (disposable_queue_size != 0) {
        collect_eliminate_fragments(get_queue(FileCacheType::DISPOSABLE));
    }
    if (normal_queue_size != 0) {
        collect_eliminate_fragments(get_queue(FileCacheType::NORMAL));
    }
    if (index_queue_size != 0) {
        collect_eliminate_fragments(get_queue(FileCacheType::INDEX));
    }
    std::for_each(to_evict.begin(), to_evict.end(), remove_file_block_if);
    if (is_overflow(removed_size, size, cur_cache_size)) {
        return false;
    }
    return true;
}

// 1. if async load file cache not finish
//     a. evict from lru queue
// 2. if ttl cache
//     a. evict from disposable/normal/index queue one by one
// 3. if dont reach query limit or dont have query limit
//     a. evict from other queue
//     b. evict from current queue
//         a.1 if the data belong write, then just evict cold data
// 4. if reach query limit
//     a. evict from query queue
//     b. evict from other queue
bool BlockFileCache::try_reserve(const Key& key, const CacheContext& context, size_t offset,
                                 size_t size, std::lock_guard<doris::Mutex>& cache_lock) {
    if (!_lazy_open_done) {
        return try_reserve_for_lazy_load(size, cache_lock);
    }
    // use this strategy in scenarios where there is insufficient disk capacity or insufficient number of inodes remaining
    // directly eliminate 5 times the size of the space
    if (_disk_resource_limit_mode) {
        size = 5 * size;
    }

    if (context.cache_type == FileCacheType::TTL) {
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
    size_t ghost_remove_size = 0;
    size_t queue_size = queue.get_total_cache_size(cache_lock);
    size_t cur_cache_size = _cur_cache_size;
    size_t query_context_cache_size = query_context->get_cache_size(cache_lock);

    std::vector<BlockFileCache::LRUQueue::Iterator> ghost;
    std::vector<FileBlockCell*> to_evict;

    size_t max_size = queue.get_max_size();
    auto is_overflow = [&] {
        return _disk_resource_limit_mode ? removed_size < size
                                         : cur_cache_size + size - removed_size > _total_size ||
                                                   (queue_size + size - removed_size > max_size) ||
                                                   (query_context_cache_size + size -
                                                            (removed_size + ghost_remove_size) >
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
            ghost_remove_size += iter->size;
        } else {
            size_t cell_size = cell->size();
            DCHECK(iter->size == cell_size);

            if (cell->releasable()) {
                auto& file_block = cell->file_block;
                std::lock_guard segment_lock(file_block->_mutex);
                DCHECK(file_block->_download_state == FileBlock::State::DOWNLOADED);
                to_evict.push_back(cell);
                removed_size += cell_size;
            }
        }
    }

    auto remove_file_block_if = [&](FileBlockCell* cell) {
        FileBlockSPtr file_block = cell->file_block;
        if (file_block) {
            query_context->remove(file_block->key(), file_block->offset(), cache_lock);
            std::lock_guard segment_lock(file_block->_mutex);
            remove(file_block, cache_lock, segment_lock);
        }
    };

    for (auto& iter : ghost) {
        query_context->remove(iter->key, iter->offset, cache_lock);
    }

    std::for_each(to_evict.begin(), to_evict.end(), remove_file_block_if);

    if (is_overflow() &&
        !try_reserve_from_other_queue(context.cache_type, size, cur_time, cache_lock)) {
        return false;
    }
    query_context->reserve(key, offset, size, cache_lock);
    return true;
}

bool BlockFileCache::remove_if_ttl_file_unlock(const Key& file_key, bool remove_directly,
                                               std::lock_guard<doris::Mutex>& cache_lock) {
    if (auto iter = _key_to_time.find(file_key);
        _key_to_time.find(file_key) != _key_to_time.end()) {
        if (!remove_directly) {
            for (auto& [_, cell] : _files[file_key]) {
                if (cell.cache_type == FileCacheType::TTL) {
                    if (cell.file_block->change_cache_type(FileCacheType::NORMAL)) {
                        auto& queue = get_queue(FileCacheType::NORMAL);
                        cell.queue_iterator =
                                queue.add(cell.file_block->key(), cell.file_block->offset(),
                                          cell.file_block->range().size(), cache_lock);
                        cell.file_block->update_expiration_time(0);
                        cell.cache_type = FileCacheType::NORMAL;
                    }
                }
            }
            Status st = global_local_filesystem()->rename(
                    get_path_in_local_cache(file_key, iter->second),
                    get_path_in_local_cache(file_key, 0));
            if (!st.ok()) {
                LOG_WARNING("").error(st);
            }
        } else {
            std::vector<FileBlockCell*> to_remove;
            for (auto& [_, cell] : _files[file_key]) {
                if (cell.releasable()) {
                    to_remove.push_back(&cell);
                } else {
                    cell.is_deleted = true;
                }
            }
            std::for_each(to_remove.begin(), to_remove.end(), [&](FileBlockCell* cell) {
                FileBlockSPtr file_block = cell->file_block;
                std::lock_guard segment_lock(file_block->_mutex);
                remove(file_block, cache_lock, segment_lock);
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

void BlockFileCache::remove_if_cached(const Key& file_key) {
    std::lock_guard cache_lock(_mutex);
    bool is_ttl_file = remove_if_ttl_file_unlock(file_key, true, cache_lock);
    if (!is_ttl_file) {
        auto iter = _files.find(file_key);
        std::vector<FileBlockCell*> to_remove;
        if (iter != _files.end()) {
            for (auto& [_, cell] : iter->second) {
                if (cell.releasable()) {
                    to_remove.push_back(&cell);
                }
            }
        }
        auto remove_file_block_if = [&](FileBlockCell* cell) {
            if (FileBlockSPtr file_block = cell->file_block; file_block) {
                std::lock_guard segment_lock(file_block->_mutex);
                remove(file_block, cache_lock, segment_lock);
            }
        };
        std::for_each(to_remove.begin(), to_remove.end(), remove_file_block_if);
    }
}

std::vector<FileCacheType> BlockFileCache::get_other_cache_type(FileCacheType cur_cache_type) {
    switch (cur_cache_type) {
    case FileCacheType::INDEX:
        return {FileCacheType::DISPOSABLE, FileCacheType::NORMAL};
    case FileCacheType::NORMAL:
        return {FileCacheType::DISPOSABLE, FileCacheType::INDEX};
    default:
        return {};
    }
    return {};
}

void BlockFileCache::reset_range(const Key& key, size_t offset, size_t old_size, size_t new_size,
                                 std::lock_guard<doris::Mutex>& cache_lock) {
    DCHECK(_files.find(key) != _files.end() &&
           _files.find(key)->second.find(offset) != _files.find(key)->second.end());
    FileBlockCell* cell = get_cell(key, offset, cache_lock);
    DCHECK(cell != nullptr);
    if (cell->cache_type != FileCacheType::TTL) {
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

bool BlockFileCache::try_reserve_from_other_queue_by_hot_interval(
        std::vector<FileCacheType> other_cache_types, size_t size, int64_t cur_time,
        std::lock_guard<doris::Mutex>& cache_lock) {
    size_t removed_size = 0;
    size_t cur_cache_size = _cur_cache_size;
    std::vector<FileBlockCell*> to_evict;
    for (FileCacheType cache_type : other_cache_types) {
        auto& queue = get_queue(cache_type);
        for (const auto& [entry_key, entry_offset, entry_size] : queue) {
            if (!is_overflow(removed_size, size, cur_cache_size)) {
                break;
            }
            auto* cell = get_cell(entry_key, entry_offset, cache_lock);
            DCHECK(cell) << "Cache became inconsistent. Key: " << entry_key.to_string()
                         << ", offset: " << entry_offset;

            size_t cell_size = cell->size();
            DCHECK(entry_size == cell_size);

            if (cell->atime == 0 ? true : cell->atime + queue.get_hot_data_interval() > cur_time) {
                break;
            }

            if (cell->releasable()) {
                auto& file_block = cell->file_block;

                std::lock_guard segment_lock(file_block->_mutex);
                DCHECK(file_block->_download_state == FileBlock::State::DOWNLOADED);
                to_evict.push_back(cell);
                removed_size += cell_size;
            }
        }
    }
    auto remove_file_block_if = [&](FileBlockCell* cell) {
        FileBlockSPtr file_block = cell->file_block;
        if (file_block) {
            std::lock_guard segment_lock(file_block->_mutex);
            remove(file_block, cache_lock, segment_lock);
        }
    };

    std::for_each(to_evict.begin(), to_evict.end(), remove_file_block_if);

    if (is_overflow(removed_size, size, cur_cache_size)) {
        return false;
    }

    return true;
}

bool BlockFileCache::is_overflow(size_t removed_size, size_t need_size, size_t cur_cache_size) {
    return _disk_resource_limit_mode ? removed_size < need_size
                                     : cur_cache_size + need_size - removed_size > _total_size;
}

bool BlockFileCache::try_reserve_from_other_queue_by_size(
        std::vector<FileCacheType> other_cache_types, size_t size,
        std::lock_guard<doris::Mutex>& cache_lock) {
    size_t removed_size = 0;
    size_t cur_cache_size = _cur_cache_size;
    std::vector<FileBlockCell*> to_evict;
    for (FileCacheType cache_type : other_cache_types) {
        auto& queue = get_queue(cache_type);
        for (const auto& [entry_key, entry_offset, entry_size] : queue) {
            if (!is_overflow(removed_size, size, cur_cache_size)) {
                break;
            }
            auto* cell = get_cell(entry_key, entry_offset, cache_lock);
            DCHECK(cell) << "Cache became inconsistent. Key: " << entry_key.to_string()
                         << ", offset: " << entry_offset;

            size_t cell_size = cell->size();
            DCHECK(entry_size == cell_size);

            if (cell->releasable()) {
                auto& file_block = cell->file_block;

                std::lock_guard segment_lock(file_block->_mutex);
                DCHECK(file_block->_download_state == FileBlock::State::DOWNLOADED);
                to_evict.push_back(cell);
                removed_size += cell_size;
            }
        }
    }
    auto remove_file_block_if = [&](FileBlockCell* cell) {
        FileBlockSPtr file_block = cell->file_block;
        if (file_block) {
            std::lock_guard segment_lock(file_block->_mutex);
            remove(file_block, cache_lock, segment_lock);
        }
    };

    std::for_each(to_evict.begin(), to_evict.end(), remove_file_block_if);

    if (is_overflow(removed_size, size, cur_cache_size)) {
        return false;
    }
    return true;
}

bool BlockFileCache::try_reserve_from_other_queue(FileCacheType cur_cache_type, size_t size,
                                                  int64_t cur_time,
                                                  std::lock_guard<doris::Mutex>& cache_lock) {
    // disposable queue cannot reserve other queues
    if (cur_cache_type == FileCacheType::DISPOSABLE) {
        return false;
    }
    auto other_cache_types = get_other_cache_type(cur_cache_type);
    bool reserve_success = try_reserve_from_other_queue_by_hot_interval(other_cache_types, size,
                                                                        cur_time, cache_lock);
    if (reserve_success || !config::file_cache_enable_evict_from_other_queue_by_size) {
        return reserve_success;
    }
    auto& cur_queue = get_queue(cur_cache_type);
    size_t cur_queue_size = cur_queue.get_total_cache_size(cache_lock);
    size_t cur_queue_max_size = cur_queue.get_max_size();
    // Hit the soft limit by self, cannot remove from other queues
    if (_cur_cache_size + size > _total_size && cur_queue_size + size > cur_queue_max_size) {
        return false;
    }
    return try_reserve_from_other_queue_by_size(other_cache_types, size, cache_lock);
}

bool BlockFileCache::try_reserve_for_lru(const Key& key, QueryFileCacheContextPtr query_context,
                                         const CacheContext& context, size_t offset, size_t size,
                                         std::lock_guard<doris::Mutex>& cache_lock) {
    int64_t cur_time = std::chrono::duration_cast<std::chrono::seconds>(
                               std::chrono::steady_clock::now().time_since_epoch())
                               .count();
    if (!try_reserve_from_other_queue(context.cache_type, size, cur_time, cache_lock)) {
        auto& queue = get_queue(context.cache_type);
        size_t removed_size = 0;
        size_t cur_cache_size = _cur_cache_size;

        std::vector<FileBlockCell*> to_evict;
        for (const auto& [entry_key, entry_offset, entry_size] : queue) {
            if (!is_overflow(removed_size, size, cur_cache_size)) {
                break;
            }
            auto* cell = get_cell(entry_key, entry_offset, cache_lock);

            DCHECK(cell) << "Cache became inconsistent. Key: " << entry_key.to_string()
                         << ", offset: " << entry_offset;

            size_t cell_size = cell->size();
            DCHECK(entry_size == cell_size);

            if (cell->releasable()) {
                auto& file_block = cell->file_block;

                std::lock_guard segment_lock(file_block->_mutex);
                DCHECK(file_block->_download_state == FileBlock::State::DOWNLOADED);
                to_evict.push_back(cell);
                removed_size += cell_size;
            }
        }

        auto remove_file_block_if = [&](FileBlockCell* cell) {
            FileBlockSPtr file_block = cell->file_block;
            if (file_block) {
                std::lock_guard segment_lock(file_block->_mutex);
                remove(file_block, cache_lock, segment_lock);
            }
        };

        std::for_each(to_evict.begin(), to_evict.end(), remove_file_block_if);
        if (is_overflow(removed_size, size, cur_cache_size)) {
            return false;
        }
    }

    if (query_context) {
        query_context->reserve(key, offset, size, cache_lock);
    }
    return true;
}

template <class T, class U>
    requires IsXLock<T> && IsXLock<U>
void BlockFileCache::remove(FileBlockSPtr file_block, T& cache_lock, U& segment_lock) {
    auto key = file_block->key();
    auto offset = file_block->offset();
    auto type = file_block->cache_type();
    auto expiration_time = file_block->expiration_time();
    auto* cell = get_cell(key, offset, cache_lock);
    // It will be removed concurrently
    DCHECK(cell);
    auto state = cell->file_block->state_unlock(segment_lock);

    if (cell->queue_iterator) {
        auto& queue = get_queue(file_block->cache_type());
        queue.remove(*cell->queue_iterator, cache_lock);
    }
    *_queue_evict_size_metrics[file_block->cache_type()] << file_block->range().size();
    *_total_evict_size_metrics << file_block->range().size();
    _cur_cache_size -= file_block->range().size();
    if (FileCacheType::TTL == type) {
        _cur_ttl_size -= file_block->range().size();
    }
    auto& offsets = _files[file_block->key()];
    offsets.erase(file_block->offset());

    auto cache_file_path = get_path_in_local_cache(key, expiration_time, offset, type,
                                                   state == FileBlock::State::EMPTY);
    std::error_code ec;
    if (bool is_exist = std::filesystem::exists(cache_file_path, ec); is_exist && !ec) {
        std::filesystem::remove(cache_file_path, ec);
        if (ec) {
            LOG(WARNING) << "filesystem error, failed to remove file, file=" << cache_file_path
                         << " error=" << ec.message();
        }
    } else if (ec) {
        LOG(WARNING) << "filesystem error, failed to exists file, file=" << cache_file_path
                     << " error=" << ec.message();
    }
    if (offsets.empty()) {
        auto key_path = get_path_in_local_cache(key, expiration_time);
        _files.erase(key);
        std::filesystem::remove_all(key_path, ec);
        if (ec) {
            LOG(ERROR) << ec.message();
        }
    }
}

void BlockFileCache::load_cache_info_into_memory() {
    Key key;
    uint64_t offset = 0;
    size_t size = 0;

    int scan_length = 10000;
    std::vector<BatchLoadArgs> batch_load_buffer;
    batch_load_buffer.reserve(scan_length);

    std::vector<std::string> need_to_check_if_empty_dir;
    std::error_code ec;
    /// cache_base_path / key / offset
    std::filesystem::directory_iterator key_it(_cache_base_path, ec);
    if (ec) [[unlikely]] {
        LOG(ERROR) << ec.message();
    }
    auto add_cell_batch_func = [&]() {
        std::lock_guard cache_lock(_mutex);
        std::for_each(
                batch_load_buffer.begin(), batch_load_buffer.end(), [&](const BatchLoadArgs& args) {
                    // in async load mode, a cell may be added twice.
                    if (_files.count(args.key) == 0 || _files[args.key].count(args.offset) == 0) {
                        // if the file is tmp, it means it is the old file and it should be removed
                        if (args.is_tmp) {
                            std::filesystem::remove(args.offset_path, ec);
                            if (ec) {
                                LOG(WARNING) << fmt::format("cannot remove {}: {}",
                                                            args.offset_path, ec.message());
                            }
                        } else {
                            add_cell(args.key, args.ctx, args.offset, args.size,
                                     FileBlock::State::DOWNLOADED, cache_lock);
                        }
                    }
                });
        batch_load_buffer.clear();
    };
    TEST_SYNC_POINT_CALLBACK("CloudFileCache::TmpFile1");
    for (; key_it != std::filesystem::directory_iterator(); ++key_it) {
        auto key_with_suffix = key_it->path().filename().native();
        auto delim_pos = key_with_suffix.find('_');
        DCHECK(delim_pos != std::string::npos);
        std::string key_str;
        std::string expiration_time_str;
        key_str = key_with_suffix.substr(0, delim_pos);
        expiration_time_str = key_with_suffix.substr(delim_pos + 1);
        key = Key(vectorized::unhex_uint<uint128_t>(key_str.c_str()));
        std::error_code ec;
        std::filesystem::directory_iterator offset_it(key_it->path(), ec);
        if (ec) [[unlikely]] {
            LOG(WARNING) << "filesystem error, failed to remove file, file=" << key_it->path()
                         << " error=" << ec.message();
            continue;
        }
        CacheContext context;
        context.query_id = TUniqueId();
        context.expiration_time = std::stoul(expiration_time_str);
        for (; offset_it != std::filesystem::directory_iterator(); ++offset_it) {
            std::string offset_with_suffix = offset_it->path().filename().native();
            auto delim_pos1 = offset_with_suffix.find('_');
            FileCacheType cache_type = FileCacheType::NORMAL;
            bool parsed = true;
            bool is_tmp = false;
            try {
                if (delim_pos1 == std::string::npos) {
                    // same as type "normal"
                    offset = stoull(offset_with_suffix);
                } else {
                    offset = stoull(offset_with_suffix.substr(0, delim_pos1));
                    std::string suffix = offset_with_suffix.substr(delim_pos1 + 1);
                    // not need persistent anymore
                    // if suffix is equals to "tmp", it should be removed too.
                    if (suffix == "tmp") [[unlikely]] {
                        is_tmp = true;
                    } else {
                        cache_type = string_to_cache_type(suffix);
                    }
                }
            } catch (...) {
                parsed = false;
            }

            if (!parsed) {
                LOG(WARNING) << "parse offset err, path=" << offset_it->path().native();
                continue;
            }
            TEST_SYNC_POINT_CALLBACK("BlockFileCache::REMOVE_FILE_2", &offset_with_suffix);
            size = offset_it->file_size(ec);
            if (ec) {
                LOG(WARNING) << "failed to file_size: file_name=" << offset_with_suffix
                             << "error=" << ec.message();
                continue;
            }
            if (size == 0 && !is_tmp) {
                std::filesystem::remove(offset_it->path(), ec);
                if (ec) {
                    LOG(WARNING) << "failed to remove: error=" << ec.message();
                }
                continue;
            }
            context.cache_type = cache_type;
            BatchLoadArgs args;
            args.ctx = context;
            args.key = key;
            args.key_path = key_it->path();
            args.offset_path = offset_it->path();
            args.size = size;
            args.offset = offset;
            args.is_tmp = is_tmp;
            batch_load_buffer.push_back(std::move(args));

            // add lock
            if (batch_load_buffer.size() >= scan_length) {
                add_cell_batch_func();
                std::this_thread::sleep_for(std::chrono::microseconds(10));
            }
        }
    }
    if (batch_load_buffer.size() != 0) {
        add_cell_batch_func();
    }
    TEST_SYNC_POINT_CALLBACK("CloudFileCache::TmpFile2");
}

size_t BlockFileCache::get_used_cache_size(FileCacheType cache_type) const {
    std::lock_guard cache_lock(_mutex);
    return get_used_cache_size_unlocked(cache_type, cache_lock);
}

size_t BlockFileCache::get_used_cache_size_unlocked(
        FileCacheType cache_type, std::lock_guard<doris::Mutex>& cache_lock) const {
    return get_queue(cache_type).get_total_cache_size(cache_lock);
}

size_t BlockFileCache::get_file_blocks_num(FileCacheType cache_type) const {
    std::lock_guard cache_lock(_mutex);
    return get_file_blocks_num_unlocked(cache_type, cache_lock);
}

size_t BlockFileCache::get_file_blocks_num_unlocked(
        FileCacheType cache_type, std::lock_guard<doris::Mutex>& cache_lock) const {
    return get_queue(cache_type).get_elements_num(cache_lock);
}

BlockFileCache::FileBlockCell::FileBlockCell(FileBlockSPtr file_block, FileCacheType cache_type,
                                             std::lock_guard<doris::Mutex>& cache_lock)
        : file_block(file_block), cache_type(cache_type) {
    /**
     * Cell can be created with either DOWNLOADED or EMPTY file segment's state.
     * File segment acquires DOWNLOADING state and creates LRUQueue iterator on first
     * successful getOrSetDownaloder call.
     */

    switch (file_block->_download_state) {
    case FileBlock::State::DOWNLOADED:
    case FileBlock::State::EMPTY:
    case FileBlock::State::SKIP_CACHE: {
        break;
    }
    default:
        DCHECK(false) << "Can create cell with either EMPTY, DOWNLOADED, SKIP_CACHE state, got: "
                      << FileBlock::state_to_string(file_block->_download_state);
    }
    if (cache_type == FileCacheType::TTL) {
        update_atime();
    }
}

BlockFileCache::LRUQueue::Iterator BlockFileCache::LRUQueue::add(
        const Key& key, size_t offset, size_t size,
        std::lock_guard<doris::Mutex>& /* cache_lock */) {
    cache_size += size;
    auto iter = queue.insert(queue.end(), FileKeyAndOffset(key, offset, size));
    map.insert(std::make_pair(std::make_pair(key, offset), iter));
    return iter;
}

template <class T>
    requires IsXLock<T>
void BlockFileCache::LRUQueue::remove(Iterator queue_it, T& /* cache_lock */) {
    cache_size -= queue_it->size;
    map.erase(std::make_pair(queue_it->key, queue_it->offset));
    queue.erase(queue_it);
}

void BlockFileCache::LRUQueue::move_to_end(Iterator queue_it,
                                           std::lock_guard<doris::Mutex>& /* cache_lock */) {
    queue.splice(queue.end(), queue, queue_it);
}
bool BlockFileCache::LRUQueue::contains(const Key& key, size_t offset,
                                        std::lock_guard<doris::Mutex>& /* cache_lock */) const {
    return map.find(std::make_pair(key, offset)) != map.end();
}

BlockFileCache::LRUQueue::Iterator BlockFileCache::LRUQueue::get(
        const Key& key, size_t offset, std::lock_guard<doris::Mutex>& /* cache_lock */) const {
    return map.find(std::make_pair(key, offset))->second;
}

std::string BlockFileCache::LRUQueue::to_string(
        std::lock_guard<doris::Mutex>& /* cache_lock */) const {
    std::string result;
    for (const auto& [key, offset, size] : queue) {
        if (!result.empty()) {
            result += ", ";
        }
        result += fmt::format("{}: [{}, {}]", key.to_string(), offset, offset + size - 1);
    }
    return result;
}

std::string BlockFileCache::dump_structure(const Key& key) {
    std::lock_guard cache_lock(_mutex);
    return dump_structure_unlocked(key, cache_lock);
}

std::string BlockFileCache::dump_structure_unlocked(const Key& key,
                                                    std::lock_guard<doris::Mutex>&) {
    std::stringstream result;
    const auto& cells_by_offset = _files[key];

    for (const auto& [_, cell] : cells_by_offset) {
        result << cell.file_block->get_info_for_log() << " "
               << cache_type_to_string(cell.cache_type) << "\n";
    }

    return result.str();
}

void BlockFileCache::change_cache_type(const Key& key, size_t offset, FileCacheType new_type,
                                       std::lock_guard<doris::Mutex>& cache_lock) {
    if (auto iter = _files.find(key); iter != _files.end()) {
        auto& file_blocks = iter->second;
        if (auto cell_it = file_blocks.find(offset); cell_it != file_blocks.end()) {
            FileBlockCell& cell = cell_it->second;
            auto& cur_queue = get_queue(cell.cache_type);
            cell.cache_type = new_type;
            DCHECK(cell.queue_iterator.has_value());
            cur_queue.remove(*cell.queue_iterator, cache_lock);
            auto& new_queue = get_queue(new_type);
            cell.queue_iterator =
                    new_queue.add(key, offset, cell.file_block->range().size(), cache_lock);
        }
    }
}

// @brief: get a path's disk capacity used percent, inode remain percent
// @param: path
// @param: percent.first disk used percent, percent.second inode used percent
int disk_used_percentage(const std::string& path, std::pair<int, int>* percent) {
    struct statfs stat;
    int ret = statfs(path.c_str(), &stat);
    TEST_SYNC_POINT_CALLBACK("BlockFileCache::set_stat_ret", &ret);
    TEST_SYNC_POINT_CALLBACK("BlockFileCache::set_stat", &stat);
    if (ret != 0) {
        return ret;
    }
    // https://github.com/coreutils/coreutils/blob/master/src/df.c#L1195
    // v->used = stat.f_blocks - stat.f_bfree
    // nonroot_total = stat.f_blocks - stat.f_bfree + stat.f_bavail
    uintmax_t u100 = (stat.f_blocks - stat.f_bfree) * 100;
    uintmax_t nonroot_total = stat.f_blocks - stat.f_bfree + stat.f_bavail;
    int capacity_percentage = u100 / nonroot_total + (u100 % nonroot_total != 0);

    unsigned long long inode_free = stat.f_ffree;
    unsigned long long inode_total = stat.f_files;
    int inode_percentage = (inode_free * 1.0 / inode_total) * 100;
    percent->first = capacity_percentage;
    percent->second = 100 - inode_percentage;
    return 0;
}

void BlockFileCache::check_disk_resource_limit(const std::string& path) {
    std::pair<int, int> percent;
    int ret = disk_used_percentage(path, &percent);
    if (ret != 0) {
        LOG_ERROR("").tag("file cache path", path).tag("error", strerror(errno));
        return;
    }
    auto [capacity_percentage, inode_percentage] = percent;
    auto inode_is_insufficient = [](const int& inode_percentage) {
        return inode_percentage >= config::file_cache_enter_disk_resource_limit_mode_percent;
    };
    DCHECK(capacity_percentage >= 0 && capacity_percentage <= 100);
    DCHECK(inode_percentage >= 0 && inode_percentage <= 100);
    // ATTN: due to that can be change, so if its invalid, set it to default value
    if (config::file_cache_enter_disk_resource_limit_mode_percent <=
        config::file_cache_exit_disk_resource_limit_mode_percent) {
        LOG_WARNING("config error, set to default value")
                .tag("enter", config::file_cache_enter_disk_resource_limit_mode_percent)
                .tag("exit", config::file_cache_exit_disk_resource_limit_mode_percent);
        config::file_cache_enter_disk_resource_limit_mode_percent = 90;
        config::file_cache_exit_disk_resource_limit_mode_percent = 80;
    }
    if (capacity_percentage >= config::file_cache_enter_disk_resource_limit_mode_percent ||
        inode_is_insufficient(inode_percentage)) {
        _disk_resource_limit_mode = true;
    } else if (_disk_resource_limit_mode &&
               (capacity_percentage < config::file_cache_exit_disk_resource_limit_mode_percent) &&
               (inode_percentage < config::file_cache_exit_disk_resource_limit_mode_percent)) {
        _disk_resource_limit_mode = false;
    }
    if (_disk_resource_limit_mode) {
        LOG_WARNING("file cache background thread")
                .tag("space percent", capacity_percentage)
                .tag("inode percent", inode_percentage)
                .tag("is inode insufficient", inode_is_insufficient(inode_percentage))
                .tag("mode", "run in resource limit");
    }
}

void BlockFileCache::run_background_operation() {
    int64_t interval_time_seconds = 20;
    while (!_close) {
        TEST_SYNC_POINT_CALLBACK("BlockFileCache::set_sleep_time", &interval_time_seconds);
        check_disk_resource_limit(_cache_base_path);
        {
            std::unique_lock close_lock(_close_mtx);
            _close_cv.wait_for(close_lock, std::chrono::seconds(interval_time_seconds));
            if (_close) break;
        }
        recycle_deleted_blocks();
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
        _cur_cache_size_metrics->set_value(_cur_cache_size);
        _cur_ttl_cache_size_metrics->set_value(_cur_cache_size -
                                               _index_queue.get_total_cache_size(cache_lock) -
                                               _normal_queue.get_total_cache_size(cache_lock) -
                                               _disposable_queue.get_total_cache_size(cache_lock));
        _cur_normal_queue_cache_size_metrics->set_value(
                _normal_queue.get_total_cache_size(cache_lock));
        _cur_normal_queue_element_count_metrics->set_value(
                _normal_queue.get_elements_num(cache_lock));
        _cur_index_queue_cache_size_metrics->set_value(
                _index_queue.get_total_cache_size(cache_lock));
        _cur_index_queue_element_count_metrics->set_value(
                _index_queue.get_elements_num(cache_lock));
        _cur_disposable_queue_cache_size_metrics->set_value(
                _disposable_queue.get_total_cache_size(cache_lock));
        _cur_disposable_queue_element_count_metrics->set_value(
                _disposable_queue.get_elements_num(cache_lock));
    }
}

void BlockFileCache::modify_expiration_time(const Key& key, int64_t new_expiration_time) {
    std::lock_guard cache_lock(_mutex);
    // 1. If new_expiration_time is equal to zero
    if (new_expiration_time == 0) {
        remove_if_ttl_file_unlock(key, false, cache_lock);
        return;
    }
    // 2. If the key in ttl cache, modify its expiration time.
    if (auto iter = _key_to_time.find(key); iter != _key_to_time.end()) {
        Status st = global_local_filesystem()->rename(
                get_path_in_local_cache(key, iter->second),
                get_path_in_local_cache(key, new_expiration_time));
        if (!st.ok()) [[unlikely]] {
            LOG(WARNING) << "filesystem error, failed to rename file, from file="
                         << get_path_in_local_cache(key, iter->second)
                         << " to file=" << get_path_in_local_cache(key, new_expiration_time)
                         << " error=" << st;
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
                cell.file_block->update_expiration_time(new_expiration_time);
            }
        }
        return;
    }
    // 3. change to ttl if the segments aren't ttl
    if (auto iter = _files.find(key); iter != _files.end()) {
        for (auto& [_, cell] : iter->second) {
            if (cell.file_block->change_cache_type(FileCacheType::TTL)) {
                auto& queue = get_queue(cell.cache_type);
                queue.remove(cell.queue_iterator.value(), cache_lock);
                cell.queue_iterator.reset();
                cell.cache_type = FileCacheType::TTL;
                cell.file_block->update_expiration_time(new_expiration_time);
            }
        }
        _key_to_time[key] = new_expiration_time;
        _time_to_key.insert(std::make_pair(new_expiration_time, key));
        Status st = global_local_filesystem()->rename(
                get_path_in_local_cache(key, 0), get_path_in_local_cache(key, new_expiration_time));
        if (!st.ok()) {
            LOG_WARNING("").error(st);
        } else {
            DCHECK(std::filesystem::exists(get_path_in_local_cache(key, new_expiration_time)));
        }
    }
}

std::vector<std::tuple<size_t, size_t, FileCacheType, int64_t>>
BlockFileCache::get_hot_segments_meta(const Key& key) const {
    int64_t cur_time = std::chrono::duration_cast<std::chrono::seconds>(
                               std::chrono::steady_clock::now().time_since_epoch())
                               .count();
    std::lock_guard cache_lock(_mutex);
    std::vector<std::tuple<size_t, size_t, FileCacheType, int64_t>> segments_meta;
    if (auto iter = _files.find(key); iter != _files.end()) {
        for (auto& pair : _files.find(key)->second) {
            const FileBlockCell* cell = &pair.second;
            if (cell->cache_type != FileCacheType::DISPOSABLE) {
                if (cell->cache_type == FileCacheType::TTL ||
                    (cell->atime != 0 &&
                     cur_time - cell->atime <
                             get_queue(cell->cache_type).get_hot_data_interval())) {
                    segments_meta.emplace_back(pair.first, cell->size(), cell->cache_type,
                                               cell->file_block->_expiration_time);
                }
            }
        }
    }
    return segments_meta;
}

std::weak_ptr<FileReader> BlockFileCache::cache_file_reader(
        const AccessKeyAndOffset& key, std::shared_ptr<FileReader> file_reader) {
    std::weak_ptr<FileReader> wp;
    if (config::file_cache_max_file_reader_cache_size != 0) [[likely]] {
        std::lock_guard lock(s_file_reader_cache_mtx);
        if (s_file_name_to_reader.count(key) > 0) {
            wp = s_file_name_to_reader[key]->second;
        } else {
            if (config::file_cache_max_file_reader_cache_size == s_file_reader_cache.size()) {
                s_file_name_to_reader.erase(s_file_reader_cache.back().first);
                s_file_reader_cache.pop_back();
            }
            wp = file_reader;
            s_file_reader_cache.emplace_front(key, std::move(file_reader));
            s_file_name_to_reader.insert(std::make_pair(key, s_file_reader_cache.begin()));
        }
    }
    return wp;
}

void BlockFileCache::remove_file_reader(const AccessKeyAndOffset& key) {
    std::lock_guard lock(s_file_reader_cache_mtx);
    if (auto iter = s_file_name_to_reader.find(key); iter != s_file_name_to_reader.end()) {
        s_file_reader_cache.erase(iter->second);
        s_file_name_to_reader.erase(key);
    }
}

bool BlockFileCache::contains_file_reader(const AccessKeyAndOffset& key) {
    std::lock_guard lock(s_file_reader_cache_mtx);
    return s_file_name_to_reader.find(key) != s_file_name_to_reader.end();
}

size_t BlockFileCache::file_reader_cache_size() {
    std::lock_guard lock(s_file_reader_cache_mtx);
    return s_file_name_to_reader.size();
}

bool BlockFileCache::try_reserve_for_lazy_load(size_t size,
                                               std::lock_guard<doris::Mutex>& cache_lock) {
    size_t removed_size = 0;
    auto remove_file_block_if = [&](FileBlockCell* cell) {
        if (FileBlockSPtr file_block = cell->file_block; file_block) {
            std::lock_guard segment_lock(file_block->_mutex);
            remove(file_block, cache_lock, segment_lock);
        }
    };
    size_t normal_queue_size = _normal_queue.get_total_cache_size(cache_lock);
    size_t disposable_queue_size = _disposable_queue.get_total_cache_size(cache_lock);
    size_t index_queue_size = _index_queue.get_total_cache_size(cache_lock);

    std::vector<FileBlockCell*> to_evict;
    auto collect_eliminate_fragments = [&](LRUQueue& queue) {
        for (const auto& [entry_key, entry_offset, entry_size] : queue) {
            if (!_disk_resource_limit_mode || removed_size >= size) {
                break;
            }
            auto* cell = get_cell(entry_key, entry_offset, cache_lock);

            DCHECK(cell) << "Cache became inconsistent. Key: " << entry_key.to_string()
                         << ", offset: " << entry_offset;

            size_t cell_size = cell->size();
            DCHECK(entry_size == cell_size);

            /// It is guaranteed that cell is not removed from cache as long as
            /// pointer to corresponding file segment is hold by any other thread.

            if (!cell->releasable()) {
                continue;
            }
            auto& file_block = cell->file_block;

            std::lock_guard segment_lock(file_block->_mutex);
            DCHECK(file_block->_download_state == FileBlock::State::DOWNLOADED);
            to_evict.push_back(cell);
            removed_size += cell_size;
        }
    };
    if (disposable_queue_size != 0) {
        collect_eliminate_fragments(get_queue(FileCacheType::DISPOSABLE));
    }
    if (normal_queue_size != 0) {
        collect_eliminate_fragments(get_queue(FileCacheType::NORMAL));
    }
    if (index_queue_size != 0) {
        collect_eliminate_fragments(get_queue(FileCacheType::INDEX));
    }
    std::for_each(to_evict.begin(), to_evict.end(), remove_file_block_if);

    return !_disk_resource_limit_mode || removed_size >= size;
}

void BlockFileCache::update_ttl_atime(const Key& file_key) {
    std::lock_guard lock(_mutex);
    if (auto iter = _files.find(file_key); iter != _files.end()) {
        for (auto& [_, cell] : iter->second) {
            cell.update_atime();
        }
    };
}

} // namespace io
} // namespace doris
