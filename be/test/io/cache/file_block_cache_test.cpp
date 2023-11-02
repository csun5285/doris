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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Interpreters/tests/gtest_lru_file_cache.cpp
// and modified by Doris

#include <gen_cpp/Types_types.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <stddef.h>

// IWYU pragma: no_include <bits/chrono.h>
#include <sys/statfs.h>

#include <chrono> // IWYU pragma: keep
#include <condition_variable>
#include <filesystem>
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "common/config.h"
#include "common/sync_point.h"
#include "gtest/gtest_pred_impl.h"
#include "io/cache/block/block_file_cache.h"
#include "io/cache/block/block_file_cache_settings.h"
#include "io/cache/block/block_file_segment.h"
#include "io/fs/path.h"
#include "olap/options.h"
#include "util/slice.h"
#include "util/time.h"

namespace doris::io {

extern int disk_used_percentage(const std::string& path, std::pair<int, int>* percent);

namespace fs = std::filesystem;

fs::path caches_dir = fs::current_path() / "lru_cache_test";
std::string cache_base_path = caches_dir / "cache1" / "";

void assert_range([[maybe_unused]] size_t assert_n, io::FileBlockSPtr file_block,
                  const io::FileBlock::Range& expected_range, io::FileBlock::State expected_state) {
    auto range = file_block->range();

    ASSERT_EQ(range.left, expected_range.left);
    ASSERT_EQ(range.right, expected_range.right);
    ASSERT_EQ(file_block->state(), expected_state);
}

std::vector<io::FileBlockSPtr> fromHolder(const io::FileBlocksHolder& holder) {
    return std::vector<io::FileBlockSPtr>(holder.file_segments.begin(), holder.file_segments.end());
}

std::string getFileBlockPath(const std::string& base_path, const io::Key& key, size_t offset) {
    auto key_str = key.to_string();
    if constexpr (io::BlockFileCache::USE_CACHE_VERSION2) {
        return fs::path(base_path) / key_str.substr(0, 3) / key_str / std::to_string(offset);
    } else {
        return fs::path(base_path) / key_str / std::to_string(offset);
    }
}

void download(io::FileBlockSPtr file_block, size_t size = 0) {
    const auto& key = file_block->key();
    if (size == 0) {
        size = file_block->range().size();
    }

    auto key_str = key.to_string();
    auto subdir = io::BlockFileCache::USE_CACHE_VERSION2
                          ? fs::path(cache_base_path) / key_str.substr(0, 3) /
                                    (key_str + "_" + std::to_string(file_block->expiration_time()))
                          : fs::path(cache_base_path) /
                                    (key_str + "_" + std::to_string(file_block->expiration_time()));
    ASSERT_TRUE(fs::exists(subdir));

    std::string data(size, '0');
    Slice result(data.data(), size);
    ASSERT_TRUE(file_block->append(result).ok());
    ASSERT_TRUE(file_block->finalize_write().ok());
}

void complete(const io::FileBlocksHolder& holder) {
    for (const auto& file_block : holder.file_segments) {
        ASSERT_TRUE(file_block->get_or_set_downloader() == io::FileBlock::get_caller_id());
        download(file_block);
    }
}

TEST(BlockFileCache, init) {
    config::file_cache_enter_disk_resource_limit_mode_percent = 99;
    std::string string = std::string(R"(
        [
        {
            "path" : "/mnt/ssd01/clickbench/hot/be/file_cache",
            "total_size" : 193273528320,
            "query_limit" : 38654705664
        },
        {
            "path" : "/mnt/ssd01/clickbench/hot/be/file_cache",
            "total_size" : 193273528320,
            "query_limit" : 38654705664
        }
        ]
        )");
    config::enable_file_cache_query_limit = true;
    std::vector<CachePath> cache_paths;
    EXPECT_TRUE(parse_conf_cache_paths(string, cache_paths));
    EXPECT_EQ(cache_paths.size(), 2);
    for (const auto& cache_path : cache_paths) {
        io::FileCacheSettings settings = cache_path.init_settings();
        EXPECT_EQ(settings.total_size, 193273528320);
        EXPECT_EQ(settings.max_query_cache_size, 38654705664);
    }

    // err normal
    std::string err_string = std::string(R"(
        [
        {
            "path" : "/mnt/ssd01/clickbench/hot/be/file_cache",
            "total_size" : "193273528320",
            "query_limit" : 38654705664
        }
        ]
        )");
    cache_paths.clear();
    EXPECT_FALSE(parse_conf_cache_paths(err_string, cache_paths));

    // err query_limit
    err_string = std::string(R"(
        [
        {
            "path" : "/mnt/ssd01/clickbench/hot/be/file_cache",
            "normal" : 193273528320,
            "persistent" : 193273528320,
            "query_limit" : "38654705664"
        }
        ]
        )");
    cache_paths.clear();
    EXPECT_FALSE(parse_conf_cache_paths(err_string, cache_paths));
}

void test_file_cache(io::FileCacheType cache_type) {
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 1;

    TUniqueId other_query_id;
    other_query_id.hi = 2;
    other_query_id.lo = 2;

    io::FileCacheSettings settings;
    switch (cache_type) {
    case io::FileCacheType::INDEX:
        settings.index_queue_elements = 5;
        settings.index_queue_size = 30;
        break;
    case io::FileCacheType::NORMAL:
        settings.query_queue_size = 30;
        settings.query_queue_elements = 5;
        break;
    case io::FileCacheType::DISPOSABLE:
        settings.disposable_queue_size = 30;
        settings.disposable_queue_elements = 5;
        break;
    default:
        break;
    }
    settings.total_size = 30;
    settings.max_file_block_size = 30;
    settings.max_query_cache_size = 30;
    io::CacheContext context, other_context;
    context.cache_type = other_context.cache_type = cache_type;
    context.query_id = query_id;
    other_context.query_id = other_query_id;
    auto key = io::BlockFileCache::hash("key1");
    {
        io::BlockFileCache cache(cache_base_path, settings);
        ASSERT_TRUE(cache.initialize());
        while (true) {
            if (cache.get_lazy_open_success()) {
                break;
            };
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        {
            auto holder = cache.get_or_set(key, 0, 10, context); /// Add range [0, 9]
            auto segments = fromHolder(holder);
            /// Range was not present in cache. It should be added in cache as one while file segment.
            ASSERT_EQ(segments.size(), 1);
            assert_range(1, segments[0], io::FileBlock::Range(0, 9), io::FileBlock::State::EMPTY);
            ASSERT_TRUE(segments[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
            assert_range(2, segments[0], io::FileBlock::Range(0, 9),
                         io::FileBlock::State::DOWNLOADING);
            download(segments[0]);
            assert_range(3, segments[0], io::FileBlock::Range(0, 9),
                         io::FileBlock::State::DOWNLOADED);
        }
        /// Current cache:    [__________]
        ///                   ^          ^
        ///                   0          9
        ASSERT_EQ(cache.get_file_blocks_num(cache_type), 1);
        ASSERT_EQ(cache.get_used_cache_size(cache_type), 10);
        {
            /// Want range [5, 14], but [0, 9] already in cache, so only [10, 14] will be put in cache.
            auto holder = cache.get_or_set(key, 5, 10, context);
            auto segments = fromHolder(holder);
            ASSERT_EQ(segments.size(), 2);

            assert_range(4, segments[0], io::FileBlock::Range(0, 9),
                         io::FileBlock::State::DOWNLOADED);
            assert_range(5, segments[1], io::FileBlock::Range(10, 14), io::FileBlock::State::EMPTY);

            ASSERT_TRUE(segments[1]->get_or_set_downloader() == io::FileBlock::get_caller_id());
            download(segments[1]);
            assert_range(6, segments[1], io::FileBlock::Range(10, 14),
                         io::FileBlock::State::DOWNLOADED);
        }

        /// Current cache:    [__________][_____]
        ///                   ^          ^^     ^
        ///                   0          910    14
        ASSERT_EQ(cache.get_file_blocks_num(cache_type), 2);
        ASSERT_EQ(cache.get_used_cache_size(cache_type), 15);

        {
            auto holder = cache.get_or_set(key, 9, 1, context); /// Get [9, 9]
            auto segments = fromHolder(holder);
            ASSERT_EQ(segments.size(), 1);
            assert_range(7, segments[0], io::FileBlock::Range(0, 9),
                         io::FileBlock::State::DOWNLOADED);
        }
        {
            auto holder = cache.get_or_set(key, 9, 2, context); /// Get [9, 10]
            auto segments = fromHolder(holder);
            ASSERT_EQ(segments.size(), 2);
            assert_range(8, segments[0], io::FileBlock::Range(0, 9),
                         io::FileBlock::State::DOWNLOADED);
            assert_range(9, segments[1], io::FileBlock::Range(10, 14),
                         io::FileBlock::State::DOWNLOADED);
        }
        {
            auto holder = cache.get_or_set(key, 10, 1, context); /// Get [10, 10]
            auto segments = fromHolder(holder);
            ASSERT_EQ(segments.size(), 1);
            assert_range(10, segments[0], io::FileBlock::Range(10, 14),
                         io::FileBlock::State::DOWNLOADED);
        }
        complete(cache.get_or_set(key, 17, 4, context)); /// Get [17, 20]
        complete(cache.get_or_set(key, 24, 3, context)); /// Get [24, 26]

        /// Current cache:    [__________][_____]   [____]    [___]
        ///                   ^          ^^     ^   ^    ^    ^   ^
        ///                   0          910    14  17   20   24  26
        ///
        ASSERT_EQ(cache.get_file_blocks_num(cache_type), 4);
        ASSERT_EQ(cache.get_used_cache_size(cache_type), 22);
        {
            auto holder = cache.get_or_set(key, 0, 31, context); /// Get [0, 25]
            auto segments = fromHolder(holder);
            ASSERT_EQ(segments.size(), 7);
            assert_range(11, segments[0], io::FileBlock::Range(0, 9),
                         io::FileBlock::State::DOWNLOADED);
            assert_range(12, segments[1], io::FileBlock::Range(10, 14),
                         io::FileBlock::State::DOWNLOADED);
            /// Missing [15, 16] should be added in cache.
            assert_range(13, segments[2], io::FileBlock::Range(15, 16),
                         io::FileBlock::State::EMPTY);
            ASSERT_TRUE(segments[2]->get_or_set_downloader() == io::FileBlock::get_caller_id());
            download(segments[2]);

            assert_range(14, segments[3], io::FileBlock::Range(17, 20),
                         io::FileBlock::State::DOWNLOADED);

            assert_range(15, segments[4], io::FileBlock::Range(21, 23),
                         io::FileBlock::State::EMPTY);

            assert_range(16, segments[5], io::FileBlock::Range(24, 26),
                         io::FileBlock::State::DOWNLOADED);

            assert_range(16, segments[6], io::FileBlock::Range(27, 30),
                         io::FileBlock::State::SKIP_CACHE);
            /// Current cache:    [__________][_____][   ][____________]
            ///                   ^                       ^            ^
            ///                   0                        20          26
            ///

            /// Range [27, 30] must be evicted in previous getOrSet [0, 25].
            /// Let's not invalidate pointers to returned segments from range [0, 25] and
            /// as max elements size is reached, next attempt to put something in cache should fail.
            /// This will also check that [27, 27] was indeed evicted.

            auto holder1 = cache.get_or_set(key, 27, 4, context);
            auto segments_1 = fromHolder(holder1); /// Get [27, 30]
            ASSERT_EQ(segments_1.size(), 1);
            assert_range(17, segments_1[0], io::FileBlock::Range(27, 30),
                         io::FileBlock::State::SKIP_CACHE);
        }
        /// Current cache:    [__________][_____][_][____]    [___]
        ///                   ^          ^^     ^   ^    ^    ^   ^
        ///                   0          910    14  17   20   24  26
        ///
        {
            auto holder = cache.get_or_set(key, 12, 10, context); /// Get [12, 21]
            auto segments = fromHolder(holder);
            ASSERT_EQ(segments.size(), 4);

            assert_range(18, segments[0], io::FileBlock::Range(10, 14),
                         io::FileBlock::State::DOWNLOADED);
            assert_range(19, segments[1], io::FileBlock::Range(15, 16),
                         io::FileBlock::State::DOWNLOADED);
            assert_range(20, segments[2], io::FileBlock::Range(17, 20),
                         io::FileBlock::State::DOWNLOADED);
            assert_range(21, segments[3], io::FileBlock::Range(21, 21),
                         io::FileBlock::State::EMPTY);

            ASSERT_TRUE(segments[3]->get_or_set_downloader() == io::FileBlock::get_caller_id());
            download(segments[3]);
            ASSERT_TRUE(segments[3]->state() == io::FileBlock::State::DOWNLOADED);
        }
        /// Current cache:    [__________][_____][_][____][_]    [___]
        ///                   ^          ^^     ^   ^    ^       ^   ^
        ///                   0          910    14  17   20      24  26

        ASSERT_EQ(cache.get_file_blocks_num(cache_type), 6);
        {
            auto holder = cache.get_or_set(key, 23, 5, context); /// Get [23, 28]
            auto segments = fromHolder(holder);
            ASSERT_EQ(segments.size(), 3);

            assert_range(22, segments[0], io::FileBlock::Range(23, 23),
                         io::FileBlock::State::EMPTY);
            assert_range(23, segments[1], io::FileBlock::Range(24, 26),
                         io::FileBlock::State::DOWNLOADED);
            assert_range(24, segments[2], io::FileBlock::Range(27, 27),
                         io::FileBlock::State::EMPTY);

            ASSERT_TRUE(segments[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
            ASSERT_TRUE(segments[2]->get_or_set_downloader() == io::FileBlock::get_caller_id());
            download(segments[0]);
            download(segments[2]);
        }
        /// Current cache:    [__________][_____][_][____][_]  [_][___][_]
        ///                   ^          ^^     ^   ^    ^        ^   ^
        ///                   0          910    14  17   20       24  26

        ASSERT_EQ(cache.get_file_blocks_num(cache_type), 8);
        {
            auto holder5 = cache.get_or_set(key, 2, 3, context); /// Get [2, 4]
            auto s5 = fromHolder(holder5);
            ASSERT_EQ(s5.size(), 1);
            assert_range(25, s5[0], io::FileBlock::Range(0, 9), io::FileBlock::State::DOWNLOADED);

            auto holder1 = cache.get_or_set(key, 30, 2, context); /// Get [30, 31]
            auto s1 = fromHolder(holder1);
            ASSERT_EQ(s1.size(), 1);
            assert_range(26, s1[0], io::FileBlock::Range(30, 31), io::FileBlock::State::EMPTY);

            ASSERT_TRUE(s1[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
            download(s1[0]);

            /// Current cache:    [__________][_____][_][____][_]  [_][___][_]    [__]
            ///                   ^          ^^     ^   ^    ^        ^   ^  ^    ^  ^
            ///                   0          910    14  17   20       24  26 27   30 31

            auto holder2 = cache.get_or_set(key, 23, 1, context); /// Get [23, 23]
            auto s2 = fromHolder(holder2);
            ASSERT_EQ(s2.size(), 1);

            auto holder3 = cache.get_or_set(key, 24, 3, context); /// Get [24, 26]
            auto s3 = fromHolder(holder3);
            ASSERT_EQ(s3.size(), 1);

            auto holder4 = cache.get_or_set(key, 27, 1, context); /// Get [27, 27]
            auto s4 = fromHolder(holder4);
            ASSERT_EQ(s4.size(), 1);

            /// All cache is now unreleasable because pointers are still hold
            auto holder6 = cache.get_or_set(key, 0, 40, context);
            auto f = fromHolder(holder6);
            ASSERT_EQ(f.size(), 12);

            assert_range(29, f[9], io::FileBlock::Range(28, 29), io::FileBlock::State::SKIP_CACHE);
            assert_range(30, f[11], io::FileBlock::Range(32, 39), io::FileBlock::State::SKIP_CACHE);
        }
        {
            auto holder = cache.get_or_set(key, 2, 3, context); /// Get [2, 4]
            auto segments = fromHolder(holder);
            ASSERT_EQ(segments.size(), 1);
            assert_range(31, segments[0], io::FileBlock::Range(0, 9),
                         io::FileBlock::State::DOWNLOADED);
        }
        /// Current cache:    [__________][_____][_][____][_]  [_][___][_]    [__]
        ///                   ^          ^^     ^   ^    ^        ^   ^  ^    ^  ^
        ///                   0          910    14  17   20       24  26 27   30 31

        {
            auto holder = cache.get_or_set(key, 25, 5, context); /// Get [25, 29]
            auto segments = fromHolder(holder);
            ASSERT_EQ(segments.size(), 3);

            assert_range(32, segments[0], io::FileBlock::Range(24, 26),
                         io::FileBlock::State::DOWNLOADED);
            assert_range(33, segments[1], io::FileBlock::Range(27, 27),
                         io::FileBlock::State::DOWNLOADED);

            assert_range(34, segments[2], io::FileBlock::Range(28, 29),
                         io::FileBlock::State::EMPTY);
            ASSERT_TRUE(segments[2]->get_or_set_downloader() == io::FileBlock::get_caller_id());
            ASSERT_TRUE(segments[2]->state() == io::FileBlock::State::DOWNLOADING);

            bool lets_start_download = false;
            std::mutex mutex;
            std::condition_variable cv;

            std::thread other_1([&] {
                auto holder_2 =
                        cache.get_or_set(key, 25, 5, other_context); /// Get [25, 29] once again.
                auto segments_2 = fromHolder(holder_2);
                ASSERT_EQ(segments.size(), 3);

                assert_range(35, segments_2[0], io::FileBlock::Range(24, 26),
                             io::FileBlock::State::DOWNLOADED);
                assert_range(36, segments_2[1], io::FileBlock::Range(27, 27),
                             io::FileBlock::State::DOWNLOADED);
                assert_range(37, segments_2[2], io::FileBlock::Range(28, 29),
                             io::FileBlock::State::DOWNLOADING);

                ASSERT_TRUE(segments[2]->get_or_set_downloader() != io::FileBlock::get_caller_id());
                ASSERT_TRUE(segments[2]->state() == io::FileBlock::State::DOWNLOADING);

                {
                    std::lock_guard lock(mutex);
                    lets_start_download = true;
                }
                cv.notify_one();

                while (segments_2[2]->wait() == io::FileBlock::State::DOWNLOADING) {
                }
                ASSERT_TRUE(segments_2[2]->state() == io::FileBlock::State::DOWNLOADED);
            });

            {
                std::unique_lock lock(mutex);
                cv.wait(lock, [&] { return lets_start_download; });
            }

            download(segments[2]);
            ASSERT_TRUE(segments[2]->state() == io::FileBlock::State::DOWNLOADED);

            other_1.join();
        }
        ASSERT_EQ(cache.get_file_blocks_num(cache_type), 5);
        /// Current cache:    [__________] [___][_][__][__]
        ///                   ^          ^ ^   ^  ^    ^  ^
        ///                   0          9 24  26 27   30 31

        {
            /// Now let's check the similar case but getting ERROR state after segment->wait(), when
            /// state is changed not manually via segment->complete(state) but from destructor of holder
            /// and notify_all() is also called from destructor of holder.

            std::optional<io::FileBlocksHolder> holder;
            holder.emplace(cache.get_or_set(key, 3, 23, context)); /// Get [3, 25]

            auto segments = fromHolder(*holder);
            ASSERT_EQ(segments.size(), 3);

            assert_range(38, segments[0], io::FileBlock::Range(0, 9),
                         io::FileBlock::State::DOWNLOADED);

            assert_range(39, segments[1], io::FileBlock::Range(10, 23),
                         io::FileBlock::State::EMPTY);
            ASSERT_TRUE(segments[1]->get_or_set_downloader() == io::FileBlock::get_caller_id());
            ASSERT_TRUE(segments[1]->state() == io::FileBlock::State::DOWNLOADING);
            assert_range(38, segments[2], io::FileBlock::Range(24, 26),
                         io::FileBlock::State::DOWNLOADED);

            bool lets_start_download = false;
            std::mutex mutex;
            std::condition_variable cv;

            std::thread other_1([&] {
                auto holder_2 =
                        cache.get_or_set(key, 3, 23, other_context); /// Get [3, 25] once again
                auto segments_2 = fromHolder(*holder);
                ASSERT_EQ(segments_2.size(), 3);

                assert_range(41, segments_2[0], io::FileBlock::Range(0, 9),
                             io::FileBlock::State::DOWNLOADED);
                assert_range(42, segments_2[1], io::FileBlock::Range(10, 23),
                             io::FileBlock::State::DOWNLOADING);
                assert_range(43, segments_2[2], io::FileBlock::Range(24, 26),
                             io::FileBlock::State::DOWNLOADED);

                ASSERT_TRUE(segments_2[1]->get_downloader() != io::FileBlock::get_caller_id());
                ASSERT_TRUE(segments_2[1]->state() == io::FileBlock::State::DOWNLOADING);

                {
                    std::lock_guard lock(mutex);
                    lets_start_download = true;
                }
                cv.notify_one();

                while (segments_2[1]->wait() == io::FileBlock::State::DOWNLOADING) {
                }
                ASSERT_TRUE(segments_2[1]->state() == io::FileBlock::State::EMPTY);
                ASSERT_TRUE(segments_2[1]->get_or_set_downloader() ==
                            io::FileBlock::get_caller_id());
                download(segments_2[1]);
            });

            {
                std::unique_lock lock(mutex);
                cv.wait(lock, [&] { return lets_start_download; });
            }

            holder.reset();
            other_1.join();
            ASSERT_TRUE(segments[1]->state() == io::FileBlock::State::DOWNLOADED);
        }
    }
    /// Current cache:    [__________][___][___][_][__]
    ///                   ^          ^      ^    ^  ^ ^
    ///                   0          9      24  26 27  29
    {
        /// Test LRUCache::restore().

        io::BlockFileCache cache2(cache_base_path, settings);
        ASSERT_TRUE(cache2.initialize().ok());
        while(true) {
            if (cache2.get_lazy_open_success()){
                break;
            };
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        auto holder1 = cache2.get_or_set(key, 2, 28, context); /// Get [2, 29]

        auto segments1 = fromHolder(holder1);
        ASSERT_EQ(segments1.size(), 5);

        assert_range(44, segments1[0], io::FileBlock::Range(0, 9),
                     io::FileBlock::State::DOWNLOADED);
        assert_range(45, segments1[1], io::FileBlock::Range(10, 23),
                     io::FileBlock::State::DOWNLOADED);
        assert_range(45, segments1[2], io::FileBlock::Range(24, 26),
                     io::FileBlock::State::DOWNLOADED);
        assert_range(46, segments1[3], io::FileBlock::Range(27, 27),
                     io::FileBlock::State::DOWNLOADED);
        assert_range(47, segments1[4], io::FileBlock::Range(28, 29),
                     io::FileBlock::State::DOWNLOADED);
    }

    {
        /// Test max file segment size
        auto settings2 = settings;
        settings2.index_queue_elements = 5;
        settings2.index_queue_size = 30;
        settings2.disposable_queue_size = 0;
        settings2.disposable_queue_elements = 0;
        settings2.query_queue_size = 0;
        settings2.query_queue_elements = 0;
        settings2.max_file_block_size = 10;
        io::BlockFileCache cache2(caches_dir / "cache2", settings2);
        ASSERT_TRUE(cache2.initialize().ok());
        while(true) {
            if (cache2.get_lazy_open_success()){
                break;
            };
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        auto holder1 = cache2.get_or_set(key, 0, 25, context); /// Get [0, 24]
        auto segments1 = fromHolder(holder1);

        ASSERT_EQ(segments1.size(), 3);
        assert_range(48, segments1[0], io::FileBlock::Range(0, 9), io::FileBlock::State::EMPTY);
        assert_range(49, segments1[1], io::FileBlock::Range(10, 19), io::FileBlock::State::EMPTY);
        assert_range(50, segments1[2], io::FileBlock::Range(20, 24), io::FileBlock::State::EMPTY);
    }
}

TEST(BlockFileCache, normal) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
    test_file_cache(io::FileCacheType::DISPOSABLE);
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }

    fs::create_directories(cache_base_path);
    test_file_cache(io::FileCacheType::INDEX);
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }

    fs::create_directories(cache_base_path);
    test_file_cache(io::FileCacheType::NORMAL);
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

TEST(BlockFileCache, resize) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
    test_file_cache(io::FileCacheType::INDEX);
    /// Current cache:    [__________][___][___][_][__]
    ///                   ^          ^      ^    ^  ^ ^
    ///                   0          9      24  26 27  29
    io::FileCacheSettings settings;
    settings.index_queue_elements = 5;
    settings.index_queue_size = 10;
    settings.disposable_queue_size = 0;
    settings.disposable_queue_elements = 0;
    settings.query_queue_size = 0;
    settings.query_queue_elements = 0;
    settings.max_file_block_size = 100;
    io::BlockFileCache cache(cache_base_path, settings);
    ASSERT_TRUE(cache.initialize());
    while (true) {
        if (cache.get_lazy_open_success()) {
            break;
        };
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

TEST(BlockFileCache, query_limit_heap_use_after_free) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    doris::config::enable_file_cache_query_limit = true;
    fs::create_directories(cache_base_path);
    io::FileCacheSettings settings;
    settings.index_queue_elements = 0;
    settings.index_queue_size = 0;
    settings.disposable_queue_size = 0;
    settings.disposable_queue_elements = 0;
    settings.query_queue_size = 15;
    settings.query_queue_elements = 5;
    settings.max_file_block_size = 10;
    settings.max_query_cache_size = 15;
    settings.total_size = 15;
    io::BlockFileCache cache(cache_base_path, settings);
    ASSERT_TRUE(cache.initialize());
    while (true) {
        if (cache.get_lazy_open_success()) {
            break;
        };
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    io::CacheContext context;
    context.cache_type = io::FileCacheType::NORMAL;
    auto key = io::BlockFileCache::hash("key1");
    {
        auto holder = cache.get_or_set(key, 0, 9, context); /// Add range [0, 8]
        auto blocks = fromHolder(holder);
        ASSERT_GE(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(0, 8), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        assert_range(2, blocks[0], io::FileBlock::Range(0, 8), io::FileBlock::State::DOWNLOADING);
        download(blocks[0]);
    }
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 1;
    context.query_id = query_id;
    auto query_context_holder = cache.get_query_context_holder(query_id);
    {
        auto holder = cache.get_or_set(key, 9, 1, context); /// Add range [9, 9]
        auto blocks = fromHolder(holder);
        ASSERT_GE(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(9, 9), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        assert_range(2, blocks[0], io::FileBlock::Range(9, 9), io::FileBlock::State::DOWNLOADING);
        download(blocks[0]);
    }
    {
        auto holder = cache.get_or_set(key, 10, 5, context); /// Add range [10, 14]
        auto blocks = fromHolder(holder);
        ASSERT_GE(blocks.size(), 1);
        assert_range(3, blocks[0], io::FileBlock::Range(10, 14), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        assert_range(4, blocks[0], io::FileBlock::Range(10, 14), io::FileBlock::State::DOWNLOADING);
        download(blocks[0]);
    }
    {
        auto holder = cache.get_or_set(key, 0, 9, context); /// Add range [0, 8]
        auto blocks = fromHolder(holder);
        ASSERT_GE(blocks.size(), 1);
        assert_range(5, blocks[0], io::FileBlock::Range(0, 8), io::FileBlock::State::DOWNLOADED);
    }
    {
        auto holder = cache.get_or_set(key, 15, 1, context); /// Add range [15, 15]
        auto blocks = fromHolder(holder);
        ASSERT_GE(blocks.size(), 1);
        assert_range(6, blocks[0], io::FileBlock::Range(15, 15), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        assert_range(7, blocks[0], io::FileBlock::Range(15, 15), io::FileBlock::State::DOWNLOADING);
        download(blocks[0]);
    }
    {
        auto holder = cache.get_or_set(key, 16, 9, context); /// Add range [16, 24]
        auto blocks = fromHolder(holder);
        ASSERT_GE(blocks.size(), 1);
        assert_range(8, blocks[0], io::FileBlock::Range(16, 24), io::FileBlock::State::SKIP_CACHE);
    }
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

TEST(BlockFileCache, query_limit_dcheck) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    doris::config::enable_file_cache_query_limit = true;
    fs::create_directories(cache_base_path);
    io::FileCacheSettings settings;
    settings.index_queue_elements = 0;
    settings.index_queue_size = 0;
    settings.disposable_queue_size = 0;
    settings.disposable_queue_elements = 0;
    settings.query_queue_size = 15;
    settings.query_queue_elements = 5;
    settings.max_file_block_size = 10;
    settings.max_query_cache_size = 15;
    settings.total_size = 15;
    io::BlockFileCache cache(cache_base_path, settings);
    ASSERT_TRUE(cache.initialize());
    while (true) {
        if (cache.get_lazy_open_success()) {
            break;
        };
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    io::CacheContext context;
    context.cache_type = io::FileCacheType::NORMAL;
    auto key = io::BlockFileCache::hash("key1");
    {
        auto holder = cache.get_or_set(key, 0, 9, context); /// Add range [0, 8]
        auto blocks = fromHolder(holder);
        ASSERT_GE(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(0, 8), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        assert_range(2, blocks[0], io::FileBlock::Range(0, 8), io::FileBlock::State::DOWNLOADING);
        download(blocks[0]);
    }
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 1;
    context.query_id = query_id;
    auto query_context_holder = cache.get_query_context_holder(query_id);
    {
        auto holder = cache.get_or_set(key, 9, 1, context); /// Add range [9, 9]
        auto blocks = fromHolder(holder);
        ASSERT_GE(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(9, 9), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        assert_range(2, blocks[0], io::FileBlock::Range(9, 9), io::FileBlock::State::DOWNLOADING);
        download(blocks[0]);
    }
    {
        auto holder = cache.get_or_set(key, 10, 5, context); /// Add range [10, 14]
        auto blocks = fromHolder(holder);
        ASSERT_GE(blocks.size(), 1);
        assert_range(3, blocks[0], io::FileBlock::Range(10, 14), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        assert_range(4, blocks[0], io::FileBlock::Range(10, 14), io::FileBlock::State::DOWNLOADING);
        download(blocks[0]);
    }
    {
        auto holder = cache.get_or_set(key, 0, 9, context); /// Add range [0, 8]
        auto blocks = fromHolder(holder);
        ASSERT_GE(blocks.size(), 1);
        assert_range(5, blocks[0], io::FileBlock::Range(0, 8), io::FileBlock::State::DOWNLOADED);
    }
    {
        auto holder = cache.get_or_set(key, 15, 1, context); /// Add range [15, 15]
        auto blocks = fromHolder(holder);
        ASSERT_GE(blocks.size(), 1);
        assert_range(6, blocks[0], io::FileBlock::Range(15, 15), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        assert_range(7, blocks[0], io::FileBlock::Range(15, 15), io::FileBlock::State::DOWNLOADING);
        download(blocks[0]);
    }
    // double add
    {
        auto holder = cache.get_or_set(key, 9, 1, context); /// Add range [9, 9]
        auto blocks = fromHolder(holder);
        ASSERT_GE(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(9, 9), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        assert_range(2, blocks[0], io::FileBlock::Range(9, 9), io::FileBlock::State::DOWNLOADING);
        download(blocks[0]);
    }
    {
        auto holder = cache.get_or_set(key, 30, 5, context); /// Add range [30, 34]
        auto blocks = fromHolder(holder);
        ASSERT_GE(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(30, 34), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        assert_range(2, blocks[0], io::FileBlock::Range(30, 34), io::FileBlock::State::DOWNLOADING);
        download(blocks[0]);
    }
    {
        auto holder = cache.get_or_set(key, 40, 5, context); /// Add range [40, 44]
        auto blocks = fromHolder(holder);
        ASSERT_GE(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(40, 44), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        assert_range(2, blocks[0], io::FileBlock::Range(40, 44), io::FileBlock::State::DOWNLOADING);
        download(blocks[0]);
    }
    {
        auto holder = cache.get_or_set(key, 50, 5, context); /// Add range [50, 54]
        auto blocks = fromHolder(holder);
        ASSERT_GE(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(50, 54), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        assert_range(2, blocks[0], io::FileBlock::Range(50, 54), io::FileBlock::State::DOWNLOADING);
        download(blocks[0]);
    }
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

TEST(BlockFileCache, reset_range) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    doris::config::enable_file_cache_query_limit = true;
    fs::create_directories(cache_base_path);
    io::FileCacheSettings settings;
    settings.index_queue_elements = 0;
    settings.index_queue_size = 0;
    settings.disposable_queue_size = 0;
    settings.disposable_queue_elements = 0;
    settings.query_queue_size = 15;
    settings.query_queue_elements = 5;
    settings.max_file_block_size = 10;
    settings.max_query_cache_size = 15;
    settings.total_size = 15;
    io::BlockFileCache cache(cache_base_path, settings);
    ASSERT_TRUE(cache.initialize());
    while (true) {
        if (cache.get_lazy_open_success()) {
            break;
        };
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    io::CacheContext context;
    context.cache_type = io::FileCacheType::NORMAL;
    auto key = io::BlockFileCache::hash("key1");
    {
        auto holder = cache.get_or_set(key, 0, 9, context); /// Add range [0, 8]
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(0, 8), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        assert_range(2, blocks[0], io::FileBlock::Range(0, 8), io::FileBlock::State::DOWNLOADING);
        download(blocks[0], 6);
    }
    {
        auto holder = cache.get_or_set(key, 0, 9, context); /// Add range [0, 8]
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 2);
        assert_range(1, blocks[0], io::FileBlock::Range(0, 5), io::FileBlock::State::DOWNLOADED);
        assert_range(2, blocks[1], io::FileBlock::Range(6, 8), io::FileBlock::State::EMPTY);
    }
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

TEST(BlockFileCache, change_cache_type) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    doris::config::enable_file_cache_query_limit = true;
    fs::create_directories(cache_base_path);
    io::FileCacheSettings settings;
    settings.index_queue_elements = 5;
    settings.index_queue_size = 15;
    settings.disposable_queue_size = 0;
    settings.disposable_queue_elements = 0;
    settings.query_queue_size = 15;
    settings.query_queue_elements = 5;
    settings.max_file_block_size = 10;
    settings.max_query_cache_size = 15;
    settings.total_size = 30;
    io::BlockFileCache cache(cache_base_path, settings);
    ASSERT_TRUE(cache.initialize());
    while (true) {
        if (cache.get_lazy_open_success()) {
            break;
        };
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    io::CacheContext context;
    context.cache_type = io::FileCacheType::NORMAL;
    auto key = io::BlockFileCache::hash("key1");
    {
        auto holder = cache.get_or_set(key, 0, 9, context); /// Add range [0, 8]
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(0, 8), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        assert_range(2, blocks[0], io::FileBlock::Range(0, 8), io::FileBlock::State::DOWNLOADING);
        size_t size = blocks[0]->range().size();
        std::string data(size, '0');
        Slice result(data.data(), size);
        ASSERT_TRUE(blocks[0]->append(result).ok());
        blocks[0]->change_cache_type_self(io::FileCacheType::INDEX);
        ASSERT_TRUE(blocks[0]->finalize_write().ok());
        auto key_str = key.to_string();
        auto subdir = fs::path(cache_base_path) /
                      (key_str + "_" + std::to_string(blocks[0]->expiration_time()));
        ASSERT_TRUE(fs::exists(subdir / "0_idx"));
    }
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

TEST(BlockFileCache, fd_cache_remove) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    doris::config::enable_file_cache_query_limit = true;
    fs::create_directories(cache_base_path);
    io::FileCacheSettings settings;
    settings.index_queue_elements = 0;
    settings.index_queue_size = 0;
    settings.disposable_queue_size = 0;
    settings.disposable_queue_elements = 0;
    settings.query_queue_size = 15;
    settings.query_queue_elements = 5;
    settings.max_file_block_size = 10;
    settings.max_query_cache_size = 15;
    settings.total_size = 15;
    io::BlockFileCache cache(cache_base_path, settings);
    ASSERT_TRUE(cache.initialize());
    while (true) {
        if (cache.get_lazy_open_success()) {
            break;
        };
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    io::CacheContext context;
    context.cache_type = io::FileCacheType::NORMAL;
    auto key = io::BlockFileCache::hash("key1");
    {
        auto holder = cache.get_or_set(key, 0, 9, context); /// Add range [0, 8]
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(0, 8), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        assert_range(2, blocks[0], io::FileBlock::Range(0, 8), io::FileBlock::State::DOWNLOADING);
        download(blocks[0]);
        std::unique_ptr<char[]> buffer = std::make_unique<char[]>(9);
        ASSERT_TRUE(blocks[0]->read_at(Slice(buffer.get(), 9), 0).ok());
        EXPECT_TRUE(io::BlockFileCache::contains_file_reader(std::make_pair(key, 0)));
    }
    {
        auto holder = cache.get_or_set(key, 9, 1, context); /// Add range [9, 9]
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(9, 9), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        assert_range(2, blocks[0], io::FileBlock::Range(9, 9), io::FileBlock::State::DOWNLOADING);
        download(blocks[0]);
        std::unique_ptr<char[]> buffer = std::make_unique<char[]>(1);
        ASSERT_TRUE(blocks[0]->read_at(Slice(buffer.get(), 1), 0).ok());
        EXPECT_TRUE(io::BlockFileCache::contains_file_reader(std::make_pair(key, 9)));
    }
    {
        auto holder = cache.get_or_set(key, 10, 5, context); /// Add range [10, 14]
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);
        assert_range(3, blocks[0], io::FileBlock::Range(10, 14), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        assert_range(4, blocks[0], io::FileBlock::Range(10, 14), io::FileBlock::State::DOWNLOADING);
        download(blocks[0]);
        std::unique_ptr<char[]> buffer = std::make_unique<char[]>(5);
        ASSERT_TRUE(blocks[0]->read_at(Slice(buffer.get(), 5), 0).ok());
        EXPECT_TRUE(io::BlockFileCache::contains_file_reader(std::make_pair(key, 10)));
    }
    {
        auto holder = cache.get_or_set(key, 15, 10, context); /// Add range [15, 24]
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);
        assert_range(3, blocks[0], io::FileBlock::Range(15, 24), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        assert_range(4, blocks[0], io::FileBlock::Range(15, 24), io::FileBlock::State::DOWNLOADING);
        download(blocks[0]);
        std::unique_ptr<char[]> buffer = std::make_unique<char[]>(10);
        ASSERT_TRUE(blocks[0]->read_at(Slice(buffer.get(), 10), 0).ok());
        EXPECT_TRUE(io::BlockFileCache::contains_file_reader(std::make_pair(key, 15)));
    }
    EXPECT_FALSE(io::BlockFileCache::contains_file_reader(std::make_pair(key, 0)));
    EXPECT_EQ(io::BlockFileCache::file_reader_cache_size(), 2);
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

TEST(BlockFileCache, fd_cache_evict) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    doris::config::enable_file_cache_query_limit = true;
    fs::create_directories(cache_base_path);
    io::FileCacheSettings settings;
    settings.index_queue_elements = 0;
    settings.index_queue_size = 0;
    settings.disposable_queue_size = 0;
    settings.disposable_queue_elements = 0;
    settings.query_queue_size = 15;
    settings.query_queue_elements = 5;
    settings.max_file_block_size = 10;
    settings.max_query_cache_size = 15;
    settings.total_size = 15;
    io::BlockFileCache cache(cache_base_path, settings);
    ASSERT_TRUE(cache.initialize());
    while (true) {
        if (cache.get_lazy_open_success()) {
            break;
        };
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    io::CacheContext context;
    context.cache_type = io::FileCacheType::NORMAL;
    auto key = io::BlockFileCache::hash("key1");
    std::string remove_file_name;
    config::file_cache_max_file_reader_cache_size = 2;
    {
        auto holder = cache.get_or_set(key, 0, 9, context); /// Add range [0, 8]
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(0, 8), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        assert_range(2, blocks[0], io::FileBlock::Range(0, 8), io::FileBlock::State::DOWNLOADING);
        download(blocks[0]);
        std::unique_ptr<char[]> buffer = std::make_unique<char[]>(9);
        ASSERT_TRUE(blocks[0]->read_at(Slice(buffer.get(), 9), 0).ok());
        remove_file_name = blocks[0]->get_path_in_local_cache();
        EXPECT_TRUE(io::BlockFileCache::contains_file_reader(std::make_pair(key, 0)));
    }
    {
        auto holder = cache.get_or_set(key, 9, 1, context); /// Add range [9, 9]
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(9, 9), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        assert_range(2, blocks[0], io::FileBlock::Range(9, 9), io::FileBlock::State::DOWNLOADING);
        download(blocks[0]);
        std::unique_ptr<char[]> buffer = std::make_unique<char[]>(1);
        ASSERT_TRUE(blocks[0]->read_at(Slice(buffer.get(), 1), 0).ok());
        EXPECT_TRUE(io::BlockFileCache::contains_file_reader(std::make_pair(key, 9)));
    }
    {
        auto holder = cache.get_or_set(key, 10, 5, context); /// Add range [10, 14]
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);
        assert_range(3, blocks[0], io::FileBlock::Range(10, 14), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        assert_range(4, blocks[0], io::FileBlock::Range(10, 14), io::FileBlock::State::DOWNLOADING);
        download(blocks[0]);
        std::unique_ptr<char[]> buffer = std::make_unique<char[]>(5);
        ASSERT_TRUE(blocks[0]->read_at(Slice(buffer.get(), 5), 0).ok());
        EXPECT_TRUE(io::BlockFileCache::contains_file_reader(std::make_pair(key, 10)));
    }
    EXPECT_FALSE(io::BlockFileCache::contains_file_reader(std::make_pair(key, 0)));
    EXPECT_EQ(io::BlockFileCache::file_reader_cache_size(), 2);
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

template <std::size_t N>
int get_disk_info(const char* const (&argv)[N], int* percent) {
    assert((N > 0) && (argv[N - 1] == nullptr));
    std::vector<std::string> rets;
    int pipefds[2];
    if (::pipe(pipefds) == -1) {
        std::cerr << "Error creating pipe" << std::endl;
        return -1;
    }
    pid_t pid = ::vfork();
    if (pid == -1) {
        std::cerr << "Error forking process" << std::endl;
        return -1;
    } else if (pid == 0) {
        ::close(pipefds[0]);
        ::dup2(pipefds[1], STDOUT_FILENO);
        ::execvp("df", const_cast<char* const*>(argv));
        std::cerr << "Error executing command" << std::endl;
        _Exit(-1);
    } else {
        waitpid(pid, nullptr, 0);
        close(pipefds[1]);
        char buffer[PATH_MAX];
        ssize_t nbytes;
        while ((nbytes = read(pipefds[0], buffer, PATH_MAX)) > 0) {
            buffer[nbytes - 1] = '\0';
            rets.push_back(buffer);
        }
        ::close(pipefds[0]);
    }

    // df return
    // 已用%
    // 73%
    // str save 73
    std::string str = rets[0].substr(rets[0].rfind('\n'), rets[0].rfind('%') - rets[0].rfind('\n'));
    *percent = std::stoi(str);
    return 0;
}

TEST(BlockFileCache, disk_used_percentage_test) {
    std::string dir = "/dev";
    std::pair<int, int> percent;
    disk_used_percentage(dir, &percent);
    int disk_used, inode_used;
    auto ret = get_disk_info({ (char*)"df", (char*)"--output=pcent", (char*)"/dev", (char*)nullptr }, &disk_used);
    ASSERT_TRUE(!ret);
    ret = get_disk_info({ (char*)"df", (char*)"--output=ipcent", (char*)"/dev", (char*)nullptr }, &inode_used);
    ASSERT_TRUE(!ret);
    ASSERT_EQ(percent.first, disk_used);
    ASSERT_EQ(percent.second, inode_used);
}

void test_file_cache_run_in_resource_limit(io::FileCacheType cache_type) {
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 1;

    TUniqueId other_query_id;
    other_query_id.hi = 2;
    other_query_id.lo = 2;

    io::FileCacheSettings settings;
    switch (cache_type) {
        case io::FileCacheType::INDEX:
            settings.index_queue_elements = 5;
            settings.index_queue_size = 60;
            break;
        case io::FileCacheType::NORMAL:
            settings.query_queue_size = 30;
            settings.query_queue_elements = 5;
            break;
        case io::FileCacheType::DISPOSABLE:
            settings.disposable_queue_size = 30;
            settings.disposable_queue_elements = 5;
            break;
        default:
            break;
    }
    settings.total_size = 100;
    settings.max_file_block_size = 30;
    settings.max_query_cache_size = 30;
    io::CacheContext context, other_context, index_context;
    context.cache_type = other_context.cache_type = cache_type;
    context.query_id = query_id;
    other_context.query_id = other_query_id;
    auto key_1 = io::BlockFileCache::hash("key1");
    index_context.cache_type = io::FileCacheType::INDEX;
    {
        io::BlockFileCache cache(cache_base_path, settings);
        cache._index_queue.hot_data_interval = 0;
        ASSERT_TRUE(cache.initialize());
        while (true) {
            if (cache.get_lazy_open_success()) {
                break;
            };
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        {
            auto key_index = io::BlockFileCache::hash("key_index");
            auto holder_index = cache.get_or_set(key_index, 0, 50, index_context); // Add index range [0, 49]
            auto segments_index = fromHolder(holder_index);
            segments_index[0]->get_or_set_downloader();
            download(segments_index[0]);
            assert_range(0, segments_index[0], io::FileBlock::Range(0, 29),
                         io::FileBlock::State::DOWNLOADED);
            assert_range(1, segments_index[1], io::FileBlock::Range(30, 49),
                         io::FileBlock::State::EMPTY);
            segments_index[1]->get_or_set_downloader();
            download(segments_index[1]);
            assert_range(2, segments_index[1], io::FileBlock::Range(30, 49),
                         io::FileBlock::State::DOWNLOADED);

            auto holder_index_1 = cache.get_or_set(key_index, 50, 10, index_context); // Add index range [50, 59]
            auto segments_index_1 = fromHolder(holder_index_1);
            segments_index_1[0]->get_or_set_downloader();
            download(segments_index_1[0]);
            assert_range(0, segments_index_1[0], io::FileBlock::Range(50, 59),
                         io::FileBlock::State::DOWNLOADED);
        }
        ASSERT_EQ(cache.get_file_blocks_num(io::FileCacheType::INDEX), 3);
        ASSERT_EQ(cache.get_used_cache_size(io::FileCacheType::INDEX), 60);
        {
            cache._disk_resource_limit_mode = true;
            auto holder = cache.get_or_set(key_1, 0, 10, context); /// Add range [0, 9]
            auto segments = fromHolder(holder);
            /// Range was not present in cache. It should be added in cache as one while file segment.
            ASSERT_EQ(segments.size(), 1);
            assert_range(3, segments[0], io::FileBlock::Range(0, 9),
                         io::FileBlock::State::EMPTY);
            ASSERT_TRUE(segments[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
            assert_range(4, segments[0], io::FileBlock::Range(0, 9),
                         io::FileBlock::State::DOWNLOADING);
            download(segments[0]);
            assert_range(5, segments[0], io::FileBlock::Range(0, 9),
                         io::FileBlock::State::DOWNLOADED);

            // ATTN: in disk limit mode, will remove 5*size
            /// Current index cache:    [__________][_______][_____]
            ///                         ^          ^^       ^^     ^
            ///                         0         29 30    4950    59
            // get size 10, in disk limit mode, will remove size 5 * 10 by other lru queue
            // so will remove index queue range 0~49
            ASSERT_EQ(cache.get_file_blocks_num(io::FileCacheType::INDEX), 1);
            ASSERT_EQ(cache.get_used_cache_size(io::FileCacheType::INDEX), 10);
        }
    }
}

// run in disk space or disk inode not enough mode
TEST(BlockFileCache, run_in_resource_limit_mode) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
    {
        test_file_cache_run_in_resource_limit(io::FileCacheType::NORMAL);

        if (fs::exists(cache_base_path)) {
            fs::remove_all(cache_base_path);
        }
        fs::create_directories(cache_base_path);
        test_file_cache(io::FileCacheType::NORMAL);
    }
}

TEST(BlockFileCache, fix_tmp_file) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
    test_file_cache(io::FileCacheType::NORMAL);
    auto sp = SyncPoint::get_instance();
    Defer defer {[sp] { sp->clear_all_call_backs(); }};
    sp->enable_processing();
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 1;
    io::FileCacheSettings settings;
    settings.query_queue_size = 30;
    settings.query_queue_elements = 5;
    settings.total_size = 30;
    settings.max_file_block_size = 30;
    settings.max_query_cache_size = 30;
    io::CacheContext context, other_context;
    context.cache_type = other_context.cache_type = io::FileCacheType::NORMAL;
    context.query_id = query_id;
    auto key = io::BlockFileCache::hash("key1");
    std::atomic_bool flag1 {false}, flag2 {false};
    sp->set_call_back("CloudFileCache::TmpFile1", [&](auto&&) {
        while (!flag1) {
        }
    });
    sp->set_call_back("CloudFileCache::TmpFile2", [&](auto&&) { flag2 = true; });
    io::BlockFileCache cache(cache_base_path, settings);
    ASSERT_TRUE(cache.initialize());
    auto holder = cache.get_or_set(key, 100, 1, context); /// Add range [9, 9]
    auto segments = fromHolder(holder);
    ASSERT_EQ(segments.size(), 1);
    assert_range(1, segments[0], io::FileBlock::Range(100, 100), io::FileBlock::State::EMPTY);
    ASSERT_TRUE(segments[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
    assert_range(2, segments[0], io::FileBlock::Range(100, 100), io::FileBlock::State::DOWNLOADING);
    auto key_str = key.to_string();
    auto subdir = fs::path(cache_base_path) /
                  (key_str + "_" + std::to_string(segments[0]->expiration_time()));
    ASSERT_TRUE(fs::exists(subdir));
    size_t size = segments[0]->range().size();
    std::string data(size, '0');
    Slice result(data.data(), size);
    ASSERT_TRUE(segments[0]->append(result));
    flag1 = true;
    while (!flag2) {
    }
    ASSERT_TRUE(segments[0]->finalize_write());
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

TEST(BlockFileCache, test_lazy_load) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
    test_file_cache(io::FileCacheType::NORMAL);
    auto sp = SyncPoint::get_instance();
    Defer defer {[sp] { sp->clear_all_call_backs(); }};
    sp->enable_processing();
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 1;
    io::FileCacheSettings settings;
    settings.query_queue_size = 30;
    settings.query_queue_elements = 5;
    settings.total_size = 30;
    settings.max_file_block_size = 30;
    settings.max_query_cache_size = 30;
    io::CacheContext context, other_context;
    context.cache_type = other_context.cache_type = io::FileCacheType::NORMAL;
    context.query_id = query_id;
    auto key = io::BlockFileCache::hash("key1");
    std::atomic_bool flag1 {false};
    sp->set_call_back("CloudFileCache::TmpFile2", [&](auto&&) {  while (!flag1) {
        } });
    io::BlockFileCache cache(cache_base_path, settings);
    ASSERT_TRUE(cache.initialize());
    std::this_thread::sleep_for(std::chrono::milliseconds(10)); // wait to load disk
    auto holder = cache.get_or_set(key, 100, 1, context); /// Add range [9, 9]
    auto segments = fromHolder(holder);
    ASSERT_EQ(segments.size(), 1);
    assert_range(1, segments[0], io::FileBlock::Range(100, 100), io::FileBlock::State::EMPTY);
    ASSERT_TRUE(segments[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
    assert_range(2, segments[0], io::FileBlock::Range(100, 100), io::FileBlock::State::DOWNLOADING);
    auto key_str = key.to_string();
    auto subdir = fs::path(cache_base_path) /
                  (key_str + "_" + std::to_string(segments[0]->expiration_time()));
    ASSERT_TRUE(fs::exists(subdir));
    size_t size = segments[0]->range().size();
    std::string data(size, '0');
    Slice result(data.data(), size);
    ASSERT_TRUE(segments[0]->append(result));
    ASSERT_TRUE(segments[0]->finalize_write());
    flag1 = true;
    while (true) {
        if (cache.get_lazy_open_success()) {
            break;
        };
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_EQ(cache.get_file_blocks_num(io::FileCacheType::NORMAL), 6);
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

TEST(BlockFileCache, test_lazy_load_with_limit) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
    test_file_cache(io::FileCacheType::NORMAL);
    auto sp = SyncPoint::get_instance();
    Defer defer {[sp] { sp->clear_all_call_backs(); }};
    sp->enable_processing();
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 1;
    io::FileCacheSettings settings;
    settings.query_queue_size = 30;
    settings.query_queue_elements = 5;
    settings.total_size = 30;
    settings.max_file_block_size = 30;
    settings.max_query_cache_size = 30;
    io::CacheContext context, other_context;
    context.cache_type = other_context.cache_type = io::FileCacheType::NORMAL;
    context.query_id = query_id;
    auto key = io::BlockFileCache::hash("key1");
    std::atomic_bool flag1 {false};
    sp->set_call_back("CloudFileCache::TmpFile2", [&](auto&&) {  while (!flag1) {
        } });
    io::BlockFileCache cache(cache_base_path, settings);
    ASSERT_TRUE(cache.initialize());
    std::this_thread::sleep_for(std::chrono::milliseconds(10)); // wait to load disk
    cache._disk_resource_limit_mode = true;
    auto holder = cache.get_or_set(key, 100, 1, context); /// Add range [9, 9]
    auto segments = fromHolder(holder);
    ASSERT_EQ(segments.size(), 1);
    assert_range(1, segments[0], io::FileBlock::Range(100, 100), io::FileBlock::State::EMPTY);
    ASSERT_TRUE(segments[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
    assert_range(2, segments[0], io::FileBlock::Range(100, 100), io::FileBlock::State::DOWNLOADING);
    auto key_str = key.to_string();
    auto subdir = fs::path(cache_base_path) /
                  (key_str + "_" + std::to_string(segments[0]->expiration_time()));
    ASSERT_TRUE(fs::exists(subdir));
    size_t size = segments[0]->range().size();
    std::string data(size, '0');
    Slice result(data.data(), size);
    ASSERT_TRUE(segments[0]->append(result));
    ASSERT_TRUE(segments[0]->finalize_write());
    flag1 = true;
    while (true) {
        if (cache.get_lazy_open_success()) {
            break;
        };
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_EQ(cache.get_file_blocks_num(io::FileCacheType::NORMAL), 5);
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

TEST(BlockFileCache, ttl_normal) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
    test_file_cache(io::FileCacheType::NORMAL);
    auto sp = SyncPoint::get_instance();
    Defer defer {[sp] { sp->clear_call_back("BlockFileCache::set_sleep_time"); }};
    sp->set_call_back("BlockFileCache::set_sleep_time",
                      [](auto&& args) { *try_any_cast<int64_t*>(args[0]) = 1; });
    sp->enable_processing();
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 1;
    io::FileCacheSettings settings;
    settings.query_queue_size = 30;
    settings.query_queue_elements = 5;
    settings.total_size = 30;
    settings.max_file_block_size = 30;
    settings.max_query_cache_size = 30;
    io::CacheContext context;
    context.cache_type = io::FileCacheType::TTL;
    context.query_id = query_id;
    int64_t cur_time = UnixSeconds();
    context.expiration_time = cur_time + 120;
    int64_t modify_time = cur_time + 5;
    auto key2 = io::BlockFileCache::hash("key2");
    io::BlockFileCache cache(cache_base_path, settings);
    ASSERT_TRUE(cache.initialize());
    while (true) {
        if (cache.get_lazy_open_success()) {
            break;
        };
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    {
        auto holder = cache.get_or_set(key2, 50, 10, context); /// Add range [50, 59]
        auto segments = fromHolder(holder);
        ASSERT_EQ(segments.size(), 1);
        assert_range(1, segments[0], io::FileBlock::Range(50, 59), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(segments[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        download(segments[0]);
        assert_range(1, segments[0], io::FileBlock::Range(50, 59),
                     io::FileBlock::State::DOWNLOADED);
        EXPECT_EQ(segments[0]->cache_type(), io::FileCacheType::TTL);
    }
    {
        cache.modify_expiration_time(key2, modify_time);
        context.expiration_time = modify_time;
        auto holder = cache.get_or_set(key2, 50, 10, context); /// Add range [50, 59]
        auto segments = fromHolder(holder);
        EXPECT_EQ(segments[0]->expiration_time(), modify_time);
    }
    std::this_thread::sleep_for(std::chrono::seconds(10));
    {
        context.cache_type = io::FileCacheType::INDEX;
        context.expiration_time = 0;
        auto holder = cache.get_or_set(key2, 50, 10, context); /// Add range [50, 59]
        auto segments = fromHolder(holder);
        ASSERT_EQ(segments.size(), 1);
        assert_range(1, segments[0], io::FileBlock::Range(50, 59),
                     io::FileBlock::State::DOWNLOADED);
        EXPECT_EQ(segments[0]->cache_type(), io::FileCacheType::NORMAL);
        EXPECT_EQ(segments[0]->expiration_time(), 0);
        std::string buffer(10, '1');
        EXPECT_TRUE(segments[0]->read_at(Slice(buffer.data(), 10), 0).ok());
        EXPECT_EQ(buffer, std::string(10, '0'));
    }
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

TEST(BlockFileCache, ttl_change_to_normal) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
    test_file_cache(io::FileCacheType::NORMAL);
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 1;
    io::FileCacheSettings settings;
    settings.query_queue_size = 30;
    settings.query_queue_elements = 5;
    settings.total_size = 30;
    settings.max_file_block_size = 30;
    settings.max_query_cache_size = 30;
    io::CacheContext context;
    context.cache_type = io::FileCacheType::TTL;
    context.query_id = query_id;
    int64_t cur_time = UnixSeconds();
    context.expiration_time = cur_time + 180;
    auto key2 = io::BlockFileCache::hash("key2");
    io::BlockFileCache cache(cache_base_path, settings);
    ASSERT_TRUE(cache.initialize());
    while (true) {
        if (cache.get_lazy_open_success()) {
            break;
        };
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    {
        auto holder = cache.get_or_set(key2, 50, 10, context); /// Add range [50, 59]
        auto segments = fromHolder(holder);
        ASSERT_EQ(segments.size(), 1);
        assert_range(1, segments[0], io::FileBlock::Range(50, 59), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(segments[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        download(segments[0]);
        assert_range(1, segments[0], io::FileBlock::Range(50, 59),
                     io::FileBlock::State::DOWNLOADED);
        EXPECT_EQ(segments[0]->cache_type(), io::FileCacheType::TTL);
    }
    {
        context.cache_type = io::FileCacheType::NORMAL;
        context.expiration_time = 0;
        auto holder = cache.get_or_set(key2, 50, 10, context); /// Add range [50, 59]
        EXPECT_TRUE(fs::exists(cache.get_path_in_local_cache(key2, 0, 50, io::FileCacheType::NORMAL)));
        auto segments = fromHolder(holder);
        ASSERT_EQ(segments.size(), 1);
        assert_range(1, segments[0], io::FileBlock::Range(50, 59),
                     io::FileBlock::State::DOWNLOADED);
        EXPECT_EQ(segments[0]->cache_type(), io::FileCacheType::NORMAL);
        EXPECT_EQ(segments[0]->expiration_time(), 0);
        std::string buffer(10, '1');
        EXPECT_TRUE(segments[0]->read_at(Slice(buffer.data(), 10), 0).ok());
        EXPECT_EQ(buffer, std::string(10, '0'));
    }
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

TEST(BlockFileCache, ttl_change_expiration_time) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
    test_file_cache(io::FileCacheType::NORMAL);
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 1;
    io::FileCacheSettings settings;
    settings.query_queue_size = 30;
    settings.query_queue_elements = 5;
    settings.total_size = 30;
    settings.max_file_block_size = 30;
    settings.max_query_cache_size = 30;
    io::CacheContext context;
    context.cache_type = io::FileCacheType::TTL;
    context.query_id = query_id;
    int64_t cur_time = UnixSeconds();
    context.expiration_time = cur_time + 180;
    int64_t change_time = cur_time + 120;
    auto key2 = io::BlockFileCache::hash("key2");
    io::BlockFileCache cache(cache_base_path, settings);
    ASSERT_TRUE(cache.initialize());
    while (true) {
        if (cache.get_lazy_open_success()) {
            break;
        };
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    {
        auto holder = cache.get_or_set(key2, 50, 10, context); /// Add range [50, 59]
        auto segments = fromHolder(holder);
        ASSERT_EQ(segments.size(), 1);
        assert_range(1, segments[0], io::FileBlock::Range(50, 59), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(segments[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        download(segments[0]);
        assert_range(1, segments[0], io::FileBlock::Range(50, 59),
                     io::FileBlock::State::DOWNLOADED);
        EXPECT_EQ(segments[0]->cache_type(), io::FileCacheType::TTL);
    }
    {
        context.cache_type = io::FileCacheType::TTL;
        context.expiration_time = change_time;
        auto holder = cache.get_or_set(key2, 50, 10, context); /// Add range [50, 59]
        EXPECT_TRUE(fs::exists(cache.get_path_in_local_cache(key2, change_time, 50, io::FileCacheType::TTL)));
        auto segments = fromHolder(holder);
        ASSERT_EQ(segments.size(), 1);
        assert_range(1, segments[0], io::FileBlock::Range(50, 59),
                     io::FileBlock::State::DOWNLOADED);
        EXPECT_EQ(segments[0]->cache_type(), io::FileCacheType::TTL);
        EXPECT_EQ(segments[0]->expiration_time(), change_time);
        std::string buffer(10, '1');
        EXPECT_TRUE(segments[0]->read_at(Slice(buffer.data(), 10), 0).ok());
        EXPECT_EQ(buffer, std::string(10, '0'));
    }
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

TEST(BlockFileCache, ttl_reverse) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
    test_file_cache(io::FileCacheType::NORMAL);
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 1;
    io::FileCacheSettings settings;
    settings.query_queue_size = 30;
    settings.query_queue_elements = 5;
    settings.total_size = 30;
    settings.max_file_block_size = 5;
    settings.max_query_cache_size = 30;
    io::CacheContext context;
    context.cache_type = io::FileCacheType::TTL;
    context.query_id = query_id;
    int64_t cur_time = UnixSeconds();
    context.expiration_time = cur_time + 180;
    auto key2 = io::BlockFileCache::hash("key2");
    io::BlockFileCache cache(cache_base_path, settings);
    ASSERT_TRUE(cache.initialize());
    while (true) {
        if (cache.get_lazy_open_success()) {
            break;
        };
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    {
        auto holder = cache.get_or_set(key2, 0, 30, context); /// Add range [0, 29]
        auto segments = fromHolder(holder);
        for (auto& block : segments) {
            ASSERT_TRUE(block->get_or_set_downloader() == io::FileBlock::get_caller_id());
            download(block);
        }
        EXPECT_EQ(segments.size() , 6);
    }
    {
        auto holder = cache.get_or_set(key2, 50, 5, context); /// Add range [50, 54]
        auto segments = fromHolder(holder);
        assert_range(1, segments[0], io::FileBlock::Range(50, 54), io::FileBlock::State::SKIP_CACHE);
    }
    {
        context.cache_type = io::FileCacheType::NORMAL;
        auto holder = cache.get_or_set(key2, 50, 5, context); /// Add range [50, 54]
        auto segments = fromHolder(holder);
        assert_range(1, segments[0], io::FileBlock::Range(50, 54), io::FileBlock::State::SKIP_CACHE);
    }
    
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

TEST(BlockFileCache, io_error) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
    config::file_cache_max_file_reader_cache_size = 0;
    test_file_cache(io::FileCacheType::NORMAL);
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 1;
    io::FileCacheSettings settings;
    settings.query_queue_size = 30;
    settings.query_queue_elements = 5;
    settings.total_size = 30;
    settings.max_file_block_size = 30;
    settings.max_query_cache_size = 30;
    io::CacheContext context, other_context;
    context.cache_type = other_context.cache_type = io::FileCacheType::NORMAL;
    context.query_id = query_id;
    auto key = io::BlockFileCache::hash("key1");
    io::BlockFileCache cache(cache_base_path, settings);
    ASSERT_TRUE(cache.initialize());
    while (true) {
        if (cache.get_lazy_open_success()) {
            break;
        };
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    auto sp = SyncPoint::get_instance();
    sp->enable_processing();
    {
        Defer defer {[sp] { sp->clear_all_call_backs(); }};
        sp->set_call_back("LocalFileSystem::open_file_impl", [&](auto&& values) {
            std::pair<Status, bool>* pairs = try_any_cast<std::pair<Status, bool>*>(values.back());
            pairs->second = true;
        });
        auto holder = cache.get_or_set(key, 0, 10, context); /// Add range [0, 9]
        auto segments = fromHolder(holder);
        ASSERT_EQ(segments.size(), 1);
        assert_range(1, segments[0], io::FileBlock::Range(0, 9), io::FileBlock::State::DOWNLOADED);
        auto buffer = std::make_unique<char[]>(10);
        EXPECT_FALSE(segments[0]->read_at(Slice(buffer.get(), 10), 0).ok());
    }
    {
        Defer defer {[sp] { sp->clear_all_call_backs(); }};
        sp->set_call_back("LocalFileReader::read_at_impl", [&](auto&& values) {
            std::pair<Status, bool>* pairs = try_any_cast<std::pair<Status, bool>*>(values.back());
            pairs->second = true;
        });
        auto holder = cache.get_or_set(key, 0, 10, context); /// Add range [0, 9]
        auto segments = fromHolder(holder);
        ASSERT_EQ(segments.size(), 1);
        assert_range(1, segments[0], io::FileBlock::Range(0, 9), io::FileBlock::State::DOWNLOADED);
        auto buffer = std::make_unique<char[]>(10);
        EXPECT_FALSE(segments[0]->read_at(Slice(buffer.get(), 10), 0).ok());
    }
    {
        Defer defer {[sp] { sp->clear_all_call_backs(); }};
        sp->set_call_back("LocalFileSystem::create_file_impl", [&](auto&& values) {
            std::pair<Status, bool>* pairs = try_any_cast<std::pair<Status, bool>*>(values.back());
            pairs->second = true;
        });
        auto holder = cache.get_or_set(key, 50, 10, context); /// Add range [50, 59]
        auto segments = fromHolder(holder);
        ASSERT_EQ(segments.size(), 1);
        assert_range(1, segments[0], io::FileBlock::Range(50, 59), io::FileBlock::State::EMPTY);
        std::string data(10, '0');
        Slice result(data.data(), 10);
        EXPECT_FALSE(segments[0]->append(result).ok());
    }
    {
        Defer defer {[sp] { sp->clear_all_call_backs(); }};
        sp->set_call_back("LocalFileWriter::appendv", [&](auto&& values) {
            std::pair<Status, bool>* pairs = try_any_cast<std::pair<Status, bool>*>(values.back());
            pairs->second = true;
        });
        auto holder = cache.get_or_set(key, 50, 10, context); /// Add range [50, 59]
        auto segments = fromHolder(holder);
        ASSERT_EQ(segments.size(), 1);
        assert_range(1, segments[0], io::FileBlock::Range(50, 59), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(segments[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        std::string data(10, '0');
        Slice result(data.data(), 10);
        EXPECT_FALSE(segments[0]->append(result).ok());
    }
    {
        Defer defer {[sp] { sp->clear_all_call_backs(); }};
        sp->set_call_back("LocalFileWriter::close", [&](auto&& values) {
            std::pair<Status, bool>* pairs = try_any_cast<std::pair<Status, bool>*>(values.back());
            pairs->second = true;
        });
        auto holder = cache.get_or_set(key, 50, 10, context); /// Add range [50, 59]
        auto segments = fromHolder(holder);
        ASSERT_EQ(segments.size(), 1);
        assert_range(1, segments[0], io::FileBlock::Range(50, 59), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(segments[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        std::string data(10, '0');
        Slice result(data.data(), 10);
        EXPECT_TRUE(segments[0]->append(result).ok());
        EXPECT_FALSE(segments[0]->finalize_write().ok());
    }
    {
        Defer defer {[sp] { sp->clear_all_call_backs(); }};
        sp->set_call_back("FileBlock::rename_error", [](auto&& args) {
            *try_any_cast<Status*>(args[0]) = Status::IOError("inject io error");
        });
        auto holder = cache.get_or_set(key, 50, 10, context); /// Add range [50, 59]
        auto segments = fromHolder(holder);
        ASSERT_EQ(segments.size(), 1);
        assert_range(1, segments[0], io::FileBlock::Range(50, 59), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(segments[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        std::string data(10, '0');
        Slice result(data.data(), 10);
        EXPECT_TRUE(segments[0]->append(result).ok());
        EXPECT_FALSE(segments[0]->finalize_write().ok());
    }
    {
        auto holder = cache.get_or_set(key, 50, 10, context); /// Add range [50, 59]
        auto segments = fromHolder(holder);
        ASSERT_EQ(segments.size(), 1);
        assert_range(1, segments[0], io::FileBlock::Range(50, 59), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(segments[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        download(segments[0]);
        assert_range(1, segments[0], io::FileBlock::Range(50, 59),
                     io::FileBlock::State::DOWNLOADED);
    }
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

TEST(BlockFileCache, remove_directly_when_normal_change_to_ttl) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 1;
    io::FileCacheSettings settings;
    settings.query_queue_size = 30;
    settings.query_queue_elements = 5;
    settings.index_queue_size = 30;
    settings.index_queue_elements = 5;
    settings.disposable_queue_size = 30;
    settings.disposable_queue_elements = 5;
    settings.total_size = 90;
    settings.max_file_block_size = 30;
    settings.max_query_cache_size = 30;
    io::CacheContext context;
    context.query_id = query_id;
    auto key1 = io::BlockFileCache::hash("key1");
    io::BlockFileCache cache(cache_base_path, settings);
    ASSERT_TRUE(cache.initialize());
    while (true) {
        if (cache.get_lazy_open_success()) {
            break;
        };
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    context.expiration_time = 0;
    context.cache_type = io::FileCacheType::NORMAL;
    {
        auto holder = cache.get_or_set(key1, 0, 5, context);
        auto segments = fromHolder(holder);
        ASSERT_EQ(segments.size(), 1);
        assert_range(1, segments[0], io::FileBlock::Range(0, 4), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(segments[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        download(segments[0]);
        assert_range(1, segments[0], io::FileBlock::Range(0, 4),
                        io::FileBlock::State::DOWNLOADED);
    }

    context.cache_type = io::FileCacheType::TTL;
    context.expiration_time = UnixSeconds() + 3600;
    {
        auto holder = cache.get_or_set(key1, 0, 5, context);
        auto segments = fromHolder(holder);
        ASSERT_EQ(segments.size(), 1);
        assert_range(1, segments[0], io::FileBlock::Range(0, 4),
                        io::FileBlock::State::DOWNLOADED);
    }
    cache.remove_if_cached(key1);

    EXPECT_EQ(cache._cur_cache_size, 0);
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

TEST(BlockFileCache, recyle_cache_async) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 1;
    io::FileCacheSettings settings;
    settings.query_queue_size = 30;
    settings.query_queue_elements = 5;
    settings.index_queue_size = 30;
    settings.index_queue_elements = 5;
    settings.disposable_queue_size = 30;
    settings.disposable_queue_elements = 5;
    settings.total_size = 90;
    settings.max_file_block_size = 30;
    settings.max_query_cache_size = 30;
    io::CacheContext context;
    context.query_id = query_id;
    auto key = io::BlockFileCache::hash("key1");
    io::BlockFileCache cache(cache_base_path, settings);
    auto sp = SyncPoint::get_instance();
    Defer defer {[sp] { 
        sp->clear_call_back("BlockFileCache::set_remove_batch");
        sp->clear_call_back("BlockFileCache::recycle_deleted_blocks");
        sp->clear_call_back("BlockFileCache::set_sleep_time");
    }};
    sp->set_call_back("BlockFileCache::set_sleep_time",
                      [](auto&& args) { *try_any_cast<int64_t*>(args[0]) = 1; });
    sp->set_call_back("BlockFileCache::set_remove_batch",
                      [](auto&& args) { *try_any_cast<int*>(args[0]) = 2; });
    sp->set_call_back("BlockFileCache::recycle_deleted_blocks", [&](auto&&) {
        context.cache_type = io::FileCacheType::NORMAL;
        cache.get_or_set(key, 0, 5, context);
    });
    sp->enable_processing();
    ASSERT_TRUE(cache.initialize());
    while (true) {
        if (cache.get_lazy_open_success()) {
            break;
        };
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    for (int64_t offset = 0; offset < 60; offset += 5) {
        context.cache_type = static_cast<io::FileCacheType>((offset / 5) % 3);
        auto holder = cache.get_or_set(key, offset, 5, context);
        auto segments = fromHolder(holder);
        ASSERT_EQ(segments.size(), 1);
        assert_range(1, segments[0], io::FileBlock::Range(offset, offset + 4), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(segments[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        download(segments[0]);
        assert_range(1, segments[0], io::FileBlock::Range(offset, offset + 4),
                     io::FileBlock::State::DOWNLOADED);
    }
    cache.clear_file_cache_async();
    while (cache._async_clear_file_cache) ;
    EXPECT_EQ(cache._cur_cache_size, 5);
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

TEST(BlockFileCache, recyle_cache_async_ttl) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 1;
    io::FileCacheSettings settings;
    settings.query_queue_size = 30;
    settings.query_queue_elements = 5;
    settings.index_queue_size = 30;
    settings.index_queue_elements = 5;
    settings.disposable_queue_size = 30;
    settings.disposable_queue_elements = 5;
    settings.total_size = 90;
    settings.max_file_block_size = 30;
    settings.max_query_cache_size = 30;
    io::CacheContext context;
    context.query_id = query_id;
    auto key = io::BlockFileCache::hash("key1");
    io::BlockFileCache cache(cache_base_path, settings);
    context.cache_type = io::FileCacheType::TTL;
    context.expiration_time = UnixSeconds() + 3600;
    auto sp = SyncPoint::get_instance();
    Defer defer {[sp] { 
        sp->clear_call_back("BlockFileCache::set_remove_batch");
        sp->clear_call_back("BlockFileCache::recycle_deleted_blocks");
        sp->clear_call_back("BlockFileCache::set_sleep_time");
    }};
    sp->set_call_back("BlockFileCache::set_sleep_time",
                      [](auto&& args) { *try_any_cast<int64_t*>(args[0]) = 1; });
    sp->set_call_back("BlockFileCache::set_remove_batch",
                      [](auto&& args) { *try_any_cast<int*>(args[0]) = 2; });
    sp->set_call_back("BlockFileCache::recycle_deleted_blocks", [&](auto&&) {
        context.cache_type = io::FileCacheType::TTL;
        cache.get_or_set(key, 0, 5, context);
    });
    sp->enable_processing();
    ASSERT_TRUE(cache.initialize());
    while (true) {
        if (cache.get_lazy_open_success()) {
            break;
        };
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    for (int64_t offset = 0; offset < 60; offset += 5) {
        auto holder = cache.get_or_set(key, offset, 5, context);
        auto segments = fromHolder(holder);
        ASSERT_EQ(segments.size(), 1);
        assert_range(1, segments[0], io::FileBlock::Range(offset, offset + 4), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(segments[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        download(segments[0]);
        assert_range(1, segments[0], io::FileBlock::Range(offset, offset + 4),
                     io::FileBlock::State::DOWNLOADED);
    }
    cache.get_or_set(key, 0, 5, context);
    cache.clear_file_cache_async();
    while (cache._async_clear_file_cache) ;
    EXPECT_EQ(cache._cur_cache_size, 5);
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

} // namespace doris::io
