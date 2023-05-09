#include "cloud/io/cloud_file_cache.h"

#include <gen_cpp/Types_types.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <thread>

#include "cloud/io/cloud_file_cache.h"
#include "cloud/io/cloud_file_cache_fwd.h"
#include "cloud/io/cloud_file_cache_settings.h"
#include "cloud/io/cloud_file_segment.h"
#include "cloud/io/file_reader.h"
#include "cloud/io/tmp_file_mgr.h"
#include "common/config.h"
#include "olap/options.h"
#include "util/slice.h"

namespace doris::cloud {

namespace fs = std::filesystem;

fs::path caches_dir = fs::current_path() / "lru_cache_test";
std::string cache_base_path = caches_dir / "cache1" / "";

void assert_range([[maybe_unused]] size_t assert_n, io::FileSegmentSPtr file_segment,
                  const io::FileSegment::Range& expected_range,
                  io::FileSegment::State expected_state) {
    auto range = file_segment->range();

    ASSERT_EQ(range.left, expected_range.left);
    ASSERT_EQ(range.right, expected_range.right);
    ASSERT_EQ(file_segment->state(), expected_state);
}

std::vector<io::FileSegmentSPtr> fromHolder(const io::FileSegmentsHolder& holder) {
    return std::vector<io::FileSegmentSPtr>(holder.file_segments.begin(),
                                            holder.file_segments.end());
}

std::string getFileSegmentPath(const std::string& base_path, const io::Key& key, size_t offset) {
    auto key_str = key.to_string();
    return fs::path(base_path) / key_str / std::to_string(offset);
}

void download(io::FileSegmentSPtr file_segment, size_t size = 0) {
    const auto& key = file_segment->key();
    if (size == 0) {
        size = file_segment->range().size();
    }

    auto key_str = key.to_string();
    auto subdir = fs::path(cache_base_path) /
                  (key_str + "_" + std::to_string(file_segment->expiration_time()));
    ASSERT_TRUE(fs::exists(subdir));

    std::string data(size, '0');
    Slice result(data.data(), size);
    file_segment->append(result);
    file_segment->finalize_write();
}

void complete(const io::FileSegmentsHolder& holder) {
    for (const auto& file_segment : holder.file_segments) {
        ASSERT_TRUE(file_segment->get_or_set_downloader() == io::FileSegment::get_caller_id());
        download(file_segment);
    }
}

TEST(LRUFileCache, init) {
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

TEST(TmpFileCache, init) {
    config::tmp_file_dirs = (R"(
        [
        {
            "path" : "/mnt/ssd01/clickbench/hot/be/tmp",
            "max_cache_bytes" : "193273528320",
            "max_upload_bytes" : 193273528320
        }
        ]
        )");
    EXPECT_FALSE(io::TmpFileMgr::create_tmp_file_mgrs());

    config::tmp_file_dirs = (R"(
        [
        {
            "path" : "/mnt/ssd01/clickbench/hot/be/tmp",
            "max_cache_bytes" : 193273528320,
            "max_upload_bytes" : "193273528320"
        }
        ]
        )");
    EXPECT_FALSE(io::TmpFileMgr::create_tmp_file_mgrs());

    config::tmp_file_dirs = (R"(
        [
        {
            "path" : "/mnt/ssd01/clickbench/hot/be/tmp",
            "max_cache_bytes" : 193273528320,
            "max_upload_bytes" : 193273528320
        }
        ]
        )");
    EXPECT_TRUE(io::TmpFileMgr::create_tmp_file_mgrs());
}

void test_file_cache(io::CacheType cache_type) {
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 1;

    TUniqueId other_query_id;
    other_query_id.hi = 2;
    other_query_id.lo = 2;

    io::FileCacheSettings settings;
    switch (cache_type) {
    case io::CacheType::INDEX:
        settings.index_queue_elements = 5;
        settings.index_queue_size = 30;
        break;
    case io::CacheType::NORMAL:
        settings.query_queue_size = 30;
        settings.query_queue_elements = 5;
        break;
    case io::CacheType::DISPOSABLE:
        settings.disposable_queue_size = 30;
        settings.disposable_queue_elements = 5;
        break;
    default:
        break;
    }
    settings.total_size = 30;
    settings.max_file_segment_size = 30;
    settings.max_query_cache_size = 30;
    io::CacheContext context, other_context;
    context.cache_type = other_context.cache_type = cache_type;
    context.query_id = query_id;
    other_context.query_id = other_query_id;
    auto key = io::CloudFileCache::hash("key1");
    {
        io::CloudFileCache cache(cache_base_path, settings);
        ASSERT_TRUE(cache.initialize());
        {
            auto holder = cache.get_or_set(key, 0, 10, context); /// Add range [0, 9]
            auto segments = fromHolder(holder);
            /// Range was not present in cache. It should be added in cache as one while file segment.
            ASSERT_EQ(segments.size(), 1);
            assert_range(1, segments[0], io::FileSegment::Range(0, 9),
                         io::FileSegment::State::EMPTY);
            ASSERT_TRUE(segments[0]->get_or_set_downloader() == io::FileSegment::get_caller_id());
            assert_range(2, segments[0], io::FileSegment::Range(0, 9),
                         io::FileSegment::State::DOWNLOADING);
            download(segments[0]);
            assert_range(3, segments[0], io::FileSegment::Range(0, 9),
                         io::FileSegment::State::DOWNLOADED);
        }
        /// Current cache:    [__________]
        ///                   ^          ^
        ///                   0          9
        ASSERT_EQ(cache.get_file_segments_num(cache_type), 1);
        ASSERT_EQ(cache.get_used_cache_size(cache_type), 10);
        {
            /// Want range [5, 14], but [0, 9] already in cache, so only [10, 14] will be put in cache.
            auto holder = cache.get_or_set(key, 5, 10, context);
            auto segments = fromHolder(holder);
            ASSERT_EQ(segments.size(), 2);

            assert_range(4, segments[0], io::FileSegment::Range(0, 9),
                         io::FileSegment::State::DOWNLOADED);
            assert_range(5, segments[1], io::FileSegment::Range(10, 14),
                         io::FileSegment::State::EMPTY);

            ASSERT_TRUE(segments[1]->get_or_set_downloader() == io::FileSegment::get_caller_id());
            download(segments[1]);
            assert_range(6, segments[1], io::FileSegment::Range(10, 14),
                         io::FileSegment::State::DOWNLOADED);
        }

        /// Current cache:    [__________][_____]
        ///                   ^          ^^     ^
        ///                   0          910    14
        ASSERT_EQ(cache.get_file_segments_num(cache_type), 2);
        ASSERT_EQ(cache.get_used_cache_size(cache_type), 15);

        {
            auto holder = cache.get_or_set(key, 9, 1, context); /// Get [9, 9]
            auto segments = fromHolder(holder);
            ASSERT_EQ(segments.size(), 1);
            assert_range(7, segments[0], io::FileSegment::Range(0, 9),
                         io::FileSegment::State::DOWNLOADED);
        }
        {
            auto holder = cache.get_or_set(key, 9, 2, context); /// Get [9, 10]
            auto segments = fromHolder(holder);
            ASSERT_EQ(segments.size(), 2);
            assert_range(8, segments[0], io::FileSegment::Range(0, 9),
                         io::FileSegment::State::DOWNLOADED);
            assert_range(9, segments[1], io::FileSegment::Range(10, 14),
                         io::FileSegment::State::DOWNLOADED);
        }
        {
            auto holder = cache.get_or_set(key, 10, 1, context); /// Get [10, 10]
            auto segments = fromHolder(holder);
            ASSERT_EQ(segments.size(), 1);
            assert_range(10, segments[0], io::FileSegment::Range(10, 14),
                         io::FileSegment::State::DOWNLOADED);
        }
        complete(cache.get_or_set(key, 17, 4, context)); /// Get [17, 20]
        complete(cache.get_or_set(key, 24, 3, context)); /// Get [24, 26]

        /// Current cache:    [__________][_____]   [____]    [___]
        ///                   ^          ^^     ^   ^    ^    ^   ^
        ///                   0          910    14  17   20   24  26
        ///
        ASSERT_EQ(cache.get_file_segments_num(cache_type), 4);
        ASSERT_EQ(cache.get_used_cache_size(cache_type), 22);
        {
            auto holder = cache.get_or_set(key, 0, 31, context); /// Get [0, 25]
            auto segments = fromHolder(holder);
            ASSERT_EQ(segments.size(), 7);
            assert_range(11, segments[0], io::FileSegment::Range(0, 9),
                         io::FileSegment::State::DOWNLOADED);
            assert_range(12, segments[1], io::FileSegment::Range(10, 14),
                         io::FileSegment::State::DOWNLOADED);
            /// Missing [15, 16] should be added in cache.
            assert_range(13, segments[2], io::FileSegment::Range(15, 16),
                         io::FileSegment::State::EMPTY);
            ASSERT_TRUE(segments[2]->get_or_set_downloader() == io::FileSegment::get_caller_id());
            download(segments[2]);

            assert_range(14, segments[3], io::FileSegment::Range(17, 20),
                         io::FileSegment::State::DOWNLOADED);

            assert_range(15, segments[4], io::FileSegment::Range(21, 23),
                         io::FileSegment::State::EMPTY);

            assert_range(16, segments[5], io::FileSegment::Range(24, 26),
                         io::FileSegment::State::DOWNLOADED);

            assert_range(16, segments[6], io::FileSegment::Range(27, 30),
                         io::FileSegment::State::SKIP_CACHE);
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
            assert_range(17, segments_1[0], io::FileSegment::Range(27, 30),
                         io::FileSegment::State::SKIP_CACHE);
        }
        /// Current cache:    [__________][_____][_][____]    [___]
        ///                   ^          ^^     ^   ^    ^    ^   ^
        ///                   0          910    14  17   20   24  26
        ///
        {
            auto holder = cache.get_or_set(key, 12, 10, context); /// Get [12, 21]
            auto segments = fromHolder(holder);
            ASSERT_EQ(segments.size(), 4);

            assert_range(18, segments[0], io::FileSegment::Range(10, 14),
                         io::FileSegment::State::DOWNLOADED);
            assert_range(19, segments[1], io::FileSegment::Range(15, 16),
                         io::FileSegment::State::DOWNLOADED);
            assert_range(20, segments[2], io::FileSegment::Range(17, 20),
                         io::FileSegment::State::DOWNLOADED);

            assert_range(21, segments[3], io::FileSegment::Range(21, 21),
                         io::FileSegment::State::EMPTY);

            ASSERT_TRUE(segments[3]->get_or_set_downloader() == io::FileSegment::get_caller_id());
            download(segments[3]);
            ASSERT_TRUE(segments[3]->state() == io::FileSegment::State::DOWNLOADED);
        }
        /// Current cache:    [__________][_____][_][____][_]    [___]
        ///                   ^          ^^     ^   ^    ^       ^   ^
        ///                   0          910    14  17   20      24  26

        ASSERT_EQ(cache.get_file_segments_num(cache_type), 6);
        {
            auto holder = cache.get_or_set(key, 23, 5, context); /// Get [23, 28]
            auto segments = fromHolder(holder);
            ASSERT_EQ(segments.size(), 3);

            assert_range(22, segments[0], io::FileSegment::Range(23, 23),
                         io::FileSegment::State::EMPTY);
            assert_range(23, segments[1], io::FileSegment::Range(24, 26),
                         io::FileSegment::State::DOWNLOADED);
            assert_range(24, segments[2], io::FileSegment::Range(27, 27),
                         io::FileSegment::State::EMPTY);

            ASSERT_TRUE(segments[0]->get_or_set_downloader() == io::FileSegment::get_caller_id());
            ASSERT_TRUE(segments[2]->get_or_set_downloader() == io::FileSegment::get_caller_id());
            download(segments[0]);
            download(segments[2]);
        }
        /// Current cache:    [__________][_____][_][____][_]  [_][___][_]
        ///                   ^          ^^     ^   ^    ^        ^   ^
        ///                   0          910    14  17   20       24  26

        ASSERT_EQ(cache.get_file_segments_num(cache_type), 8);
        {
            auto holder5 = cache.get_or_set(key, 2, 3, context); /// Get [2, 4]
            auto s5 = fromHolder(holder5);
            ASSERT_EQ(s5.size(), 1);
            assert_range(25, s5[0], io::FileSegment::Range(0, 9),
                         io::FileSegment::State::DOWNLOADED);

            auto holder1 = cache.get_or_set(key, 30, 2, context); /// Get [30, 31]
            auto s1 = fromHolder(holder1);
            ASSERT_EQ(s1.size(), 1);
            assert_range(26, s1[0], io::FileSegment::Range(30, 31), io::FileSegment::State::EMPTY);

            ASSERT_TRUE(s1[0]->get_or_set_downloader() == io::FileSegment::get_caller_id());
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

            assert_range(29, f[9], io::FileSegment::Range(28, 29),
                         io::FileSegment::State::SKIP_CACHE);
            assert_range(30, f[11], io::FileSegment::Range(32, 39),
                         io::FileSegment::State::SKIP_CACHE);
        }
        {
            auto holder = cache.get_or_set(key, 2, 3, context); /// Get [2, 4]
            auto segments = fromHolder(holder);
            ASSERT_EQ(segments.size(), 1);
            assert_range(31, segments[0], io::FileSegment::Range(0, 9),
                         io::FileSegment::State::DOWNLOADED);
        }
        /// Current cache:    [__________][_____][_][____][_]  [_][___][_]    [__]
        ///                   ^          ^^     ^   ^    ^        ^   ^  ^    ^  ^
        ///                   0          910    14  17   20       24  26 27   30 31

        {
            auto holder = cache.get_or_set(key, 25, 5, context); /// Get [25, 29]
            auto segments = fromHolder(holder);
            ASSERT_EQ(segments.size(), 3);

            assert_range(32, segments[0], io::FileSegment::Range(24, 26),
                         io::FileSegment::State::DOWNLOADED);
            assert_range(33, segments[1], io::FileSegment::Range(27, 27),
                         io::FileSegment::State::DOWNLOADED);

            assert_range(34, segments[2], io::FileSegment::Range(28, 29),
                         io::FileSegment::State::EMPTY);
            ASSERT_TRUE(segments[2]->get_or_set_downloader() == io::FileSegment::get_caller_id());
            ASSERT_TRUE(segments[2]->state() == io::FileSegment::State::DOWNLOADING);

            bool lets_start_download = false;
            std::mutex mutex;
            std::condition_variable cv;

            std::thread other_1([&] {
                auto holder_2 =
                        cache.get_or_set(key, 25, 5, other_context); /// Get [25, 29] once again.
                auto segments_2 = fromHolder(holder_2);
                ASSERT_EQ(segments.size(), 3);

                assert_range(35, segments_2[0], io::FileSegment::Range(24, 26),
                             io::FileSegment::State::DOWNLOADED);
                assert_range(36, segments_2[1], io::FileSegment::Range(27, 27),
                             io::FileSegment::State::DOWNLOADED);
                assert_range(37, segments_2[2], io::FileSegment::Range(28, 29),
                             io::FileSegment::State::DOWNLOADING);

                ASSERT_TRUE(segments[2]->get_or_set_downloader() !=
                            io::FileSegment::get_caller_id());
                ASSERT_TRUE(segments[2]->state() == io::FileSegment::State::DOWNLOADING);

                {
                    std::lock_guard lock(mutex);
                    lets_start_download = true;
                }
                cv.notify_one();

                while (segments_2[2]->wait() == io::FileSegment::State::DOWNLOADING) {
                }
                ASSERT_TRUE(segments_2[2]->state() == io::FileSegment::State::DOWNLOADED);
            });

            {
                std::unique_lock lock(mutex);
                cv.wait(lock, [&] { return lets_start_download; });
            }

            download(segments[2]);
            ASSERT_TRUE(segments[2]->state() == io::FileSegment::State::DOWNLOADED);

            other_1.join();
        }
        ASSERT_EQ(cache.get_file_segments_num(cache_type), 5);
        /// Current cache:    [__________] [___][_][__][__]
        ///                   ^          ^ ^   ^  ^    ^  ^
        ///                   0          9 24  26 27   30 31

        {
            /// Now let's check the similar case but getting ERROR state after segment->wait(), when
            /// state is changed not manually via segment->complete(state) but from destructor of holder
            /// and notify_all() is also called from destructor of holder.

            std::optional<io::FileSegmentsHolder> holder;
            holder.emplace(cache.get_or_set(key, 3, 23, context)); /// Get [3, 25]

            auto segments = fromHolder(*holder);
            ASSERT_EQ(segments.size(), 3);

            assert_range(38, segments[0], io::FileSegment::Range(0, 9),
                         io::FileSegment::State::DOWNLOADED);

            assert_range(39, segments[1], io::FileSegment::Range(10, 23),
                         io::FileSegment::State::EMPTY);
            ASSERT_TRUE(segments[1]->get_or_set_downloader() == io::FileSegment::get_caller_id());
            ASSERT_TRUE(segments[1]->state() == io::FileSegment::State::DOWNLOADING);
            assert_range(38, segments[2], io::FileSegment::Range(24, 26),
                         io::FileSegment::State::DOWNLOADED);


            bool lets_start_download = false;
            std::mutex mutex;
            std::condition_variable cv;

            std::thread other_1([&] {
                auto holder_2 =
                        cache.get_or_set(key, 3, 23, other_context); /// Get [3, 25] once again
                auto segments_2 = fromHolder(*holder);
                ASSERT_EQ(segments_2.size(), 3);

                assert_range(41, segments_2[0], io::FileSegment::Range(0, 9),
                             io::FileSegment::State::DOWNLOADED);
                assert_range(42, segments_2[1], io::FileSegment::Range(10, 23),
                             io::FileSegment::State::DOWNLOADING);
                assert_range(43, segments_2[2], io::FileSegment::Range(24, 26),
                             io::FileSegment::State::DOWNLOADED);

                ASSERT_TRUE(segments_2[1]->get_downloader() != io::FileSegment::get_caller_id());
                ASSERT_TRUE(segments_2[1]->state() == io::FileSegment::State::DOWNLOADING);

                {
                    std::lock_guard lock(mutex);
                    lets_start_download = true;
                }
                cv.notify_one();

                while (segments_2[1]->wait() == io::FileSegment::State::DOWNLOADING) {
                }
                ASSERT_TRUE(segments_2[1]->state() == io::FileSegment::State::EMPTY);
                ASSERT_TRUE(segments_2[1]->get_or_set_downloader() ==
                            io::FileSegment::get_caller_id());
                download(segments_2[1]);
            });

            {
                std::unique_lock lock(mutex);
                cv.wait(lock, [&] { return lets_start_download; });
            }

            holder.reset();
            other_1.join();
            ASSERT_TRUE(segments[1]->state() == io::FileSegment::State::DOWNLOADED);
        }
    }
    /// Current cache:    [__________][___][___][_][__]
    ///                   ^          ^      ^    ^  ^ ^
    ///                   0          9      24  26 27  29
    {
        /// Test LRUCache::restore().

        io::CloudFileCache cache2(cache_base_path, settings);
        cache2.initialize();
        auto holder1 = cache2.get_or_set(key, 2, 28, context); /// Get [2, 29]

        auto segments1 = fromHolder(holder1);
        ASSERT_EQ(segments1.size(), 5);

        assert_range(44, segments1[0], io::FileSegment::Range(0, 9),
                     io::FileSegment::State::DOWNLOADED);
        assert_range(45, segments1[1], io::FileSegment::Range(10, 23),
                     io::FileSegment::State::DOWNLOADED);
        assert_range(45, segments1[2], io::FileSegment::Range(24, 26),
                     io::FileSegment::State::DOWNLOADED);
        assert_range(46, segments1[3], io::FileSegment::Range(27, 27),
                     io::FileSegment::State::DOWNLOADED);
        assert_range(47, segments1[4], io::FileSegment::Range(28, 29),
                     io::FileSegment::State::DOWNLOADED);
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
        settings2.max_file_segment_size = 10;
        io::CloudFileCache cache2(caches_dir / "cache2", settings2);

        auto holder1 = cache2.get_or_set(key, 0, 25, context); /// Get [0, 24]
        auto segments1 = fromHolder(holder1);

        ASSERT_EQ(segments1.size(), 3);
        assert_range(48, segments1[0], io::FileSegment::Range(0, 9), io::FileSegment::State::EMPTY);
        assert_range(49, segments1[1], io::FileSegment::Range(10, 19),
                     io::FileSegment::State::EMPTY);
        assert_range(50, segments1[2], io::FileSegment::Range(20, 24),
                     io::FileSegment::State::EMPTY);
    }
}

TEST(LRUFileCache, normal) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
    test_file_cache(io::CacheType::DISPOSABLE);
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }

    fs::create_directories(cache_base_path);
    test_file_cache(io::CacheType::INDEX);
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }

    fs::create_directories(cache_base_path);
    test_file_cache(io::CacheType::NORMAL);
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

TEST(LRUFileCache, resize) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
    test_file_cache(io::CacheType::INDEX);
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
    settings.max_file_segment_size = 100;
    io::CloudFileCache cache(cache_base_path, settings);
    ASSERT_TRUE(cache.initialize());
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

TEST(LRUFileCache, query_limit_heap_use_after_free) {
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
    settings.max_file_segment_size = 10;
    settings.max_query_cache_size = 15;
    settings.total_size = 15;
    io::CloudFileCache cache(cache_base_path, settings);
    ASSERT_TRUE(cache.initialize());
    io::CacheContext context;
    context.cache_type = io::CacheType::NORMAL;
    auto key = io::CloudFileCache::hash("key1");
    {
        auto holder = cache.get_or_set(key, 0, 9, context); /// Add range [0, 8]
        auto segments = fromHolder(holder);
        ASSERT_EQ(segments.size(), 1);
        assert_range(1, segments[0], io::FileSegment::Range(0, 8), io::FileSegment::State::EMPTY);
        ASSERT_TRUE(segments[0]->get_or_set_downloader() == io::FileSegment::get_caller_id());
        assert_range(2, segments[0], io::FileSegment::Range(0, 8),
                     io::FileSegment::State::DOWNLOADING);
        download(segments[0]);
    }
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 1;
    context.query_id = query_id;
    auto query_context_holder = cache.get_query_context_holder(query_id);
    {
        auto holder = cache.get_or_set(key, 9, 1, context); /// Add range [9, 9]
        auto segments = fromHolder(holder);
        ASSERT_EQ(segments.size(), 1);
        assert_range(1, segments[0], io::FileSegment::Range(9, 9), io::FileSegment::State::EMPTY);
        ASSERT_TRUE(segments[0]->get_or_set_downloader() == io::FileSegment::get_caller_id());
        assert_range(2, segments[0], io::FileSegment::Range(9, 9),
                     io::FileSegment::State::DOWNLOADING);
        download(segments[0]);
    }
    {
        auto holder = cache.get_or_set(key, 10, 5, context); /// Add range [10, 14]
        auto segments = fromHolder(holder);
        ASSERT_EQ(segments.size(), 1);
        assert_range(3, segments[0], io::FileSegment::Range(10, 14), io::FileSegment::State::EMPTY);
        ASSERT_TRUE(segments[0]->get_or_set_downloader() == io::FileSegment::get_caller_id());
        assert_range(4, segments[0], io::FileSegment::Range(10, 14),
                     io::FileSegment::State::DOWNLOADING);
        download(segments[0]);
    }
    {
        auto holder = cache.get_or_set(key, 0, 9, context); /// Add range [0, 8]
        auto segments = fromHolder(holder);
        ASSERT_EQ(segments.size(), 1);
        assert_range(5, segments[0], io::FileSegment::Range(0, 8),
                     io::FileSegment::State::DOWNLOADED);
    }
    {
        auto holder = cache.get_or_set(key, 15, 1, context); /// Add range [15, 15]
        auto segments = fromHolder(holder);
        ASSERT_EQ(segments.size(), 1);
        assert_range(6, segments[0], io::FileSegment::Range(15, 15), io::FileSegment::State::EMPTY);
        ASSERT_TRUE(segments[0]->get_or_set_downloader() == io::FileSegment::get_caller_id());
        assert_range(7, segments[0], io::FileSegment::Range(15, 15),
                     io::FileSegment::State::DOWNLOADING);
        download(segments[0]);
    }
    {
        auto holder = cache.get_or_set(key, 16, 9, context); /// Add range [16, 24]
        auto segments = fromHolder(holder);
        ASSERT_EQ(segments.size(), 1);
        assert_range(8, segments[0], io::FileSegment::Range(16, 24),
                     io::FileSegment::State::SKIP_CACHE);
    }
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

TEST(LRUFileCache, query_limit_dcheck) {
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
    settings.max_file_segment_size = 10;
    settings.max_query_cache_size = 15;
    settings.total_size = 15;
    io::CloudFileCache cache(cache_base_path, settings);
    ASSERT_TRUE(cache.initialize());
    io::CacheContext context;
    context.cache_type = io::CacheType::NORMAL;
    auto key = io::CloudFileCache::hash("key1");
    {
        auto holder = cache.get_or_set(key, 0, 9, context); /// Add range [0, 8]
        auto segments = fromHolder(holder);
        ASSERT_EQ(segments.size(), 1);
        assert_range(1, segments[0], io::FileSegment::Range(0, 8), io::FileSegment::State::EMPTY);
        ASSERT_TRUE(segments[0]->get_or_set_downloader() == io::FileSegment::get_caller_id());
        assert_range(2, segments[0], io::FileSegment::Range(0, 8),
                     io::FileSegment::State::DOWNLOADING);
        download(segments[0]);
    }
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 1;
    context.query_id = query_id;
    auto query_context_holder = cache.get_query_context_holder(query_id);
    {
        auto holder = cache.get_or_set(key, 9, 1, context); /// Add range [9, 9]
        auto segments = fromHolder(holder);
        ASSERT_EQ(segments.size(), 1);
        assert_range(1, segments[0], io::FileSegment::Range(9, 9), io::FileSegment::State::EMPTY);
        ASSERT_TRUE(segments[0]->get_or_set_downloader() == io::FileSegment::get_caller_id());
        assert_range(2, segments[0], io::FileSegment::Range(9, 9),
                     io::FileSegment::State::DOWNLOADING);
        download(segments[0]);
    }
    {
        auto holder = cache.get_or_set(key, 10, 5, context); /// Add range [10, 14]
        auto segments = fromHolder(holder);
        ASSERT_EQ(segments.size(), 1);
        assert_range(3, segments[0], io::FileSegment::Range(10, 14), io::FileSegment::State::EMPTY);
        ASSERT_TRUE(segments[0]->get_or_set_downloader() == io::FileSegment::get_caller_id());
        assert_range(4, segments[0], io::FileSegment::Range(10, 14),
                     io::FileSegment::State::DOWNLOADING);
        download(segments[0]);
    }
    {
        auto holder = cache.get_or_set(key, 0, 9, context); /// Add range [0, 8]
        auto segments = fromHolder(holder);
        ASSERT_EQ(segments.size(), 1);
        assert_range(5, segments[0], io::FileSegment::Range(0, 8),
                     io::FileSegment::State::DOWNLOADED);
    }
    {
        auto holder = cache.get_or_set(key, 15, 1, context); /// Add range [15, 15]
        auto segments = fromHolder(holder);
        ASSERT_EQ(segments.size(), 1);
        assert_range(6, segments[0], io::FileSegment::Range(15, 15), io::FileSegment::State::EMPTY);
        ASSERT_TRUE(segments[0]->get_or_set_downloader() == io::FileSegment::get_caller_id());
        assert_range(7, segments[0], io::FileSegment::Range(15, 15),
                     io::FileSegment::State::DOWNLOADING);
        download(segments[0]);
    }
    // double add
    {
        auto holder = cache.get_or_set(key, 9, 1, context); /// Add range [9, 9]
        auto segments = fromHolder(holder);
        ASSERT_EQ(segments.size(), 1);
        assert_range(1, segments[0], io::FileSegment::Range(9, 9), io::FileSegment::State::EMPTY);
        ASSERT_TRUE(segments[0]->get_or_set_downloader() == io::FileSegment::get_caller_id());
        assert_range(2, segments[0], io::FileSegment::Range(9, 9),
                     io::FileSegment::State::DOWNLOADING);
        download(segments[0]);
    }
    {
        auto holder = cache.get_or_set(key, 30, 5, context); /// Add range [30, 34]
        auto segments = fromHolder(holder);
        ASSERT_EQ(segments.size(), 1);
        assert_range(1, segments[0], io::FileSegment::Range(30, 34), io::FileSegment::State::EMPTY);
        ASSERT_TRUE(segments[0]->get_or_set_downloader() == io::FileSegment::get_caller_id());
        assert_range(2, segments[0], io::FileSegment::Range(30, 34),
                     io::FileSegment::State::DOWNLOADING);
        download(segments[0]);
    }
    {
        auto holder = cache.get_or_set(key, 40, 5, context); /// Add range [40, 44]
        auto segments = fromHolder(holder);
        ASSERT_EQ(segments.size(), 1);
        assert_range(1, segments[0], io::FileSegment::Range(40, 44), io::FileSegment::State::EMPTY);
        ASSERT_TRUE(segments[0]->get_or_set_downloader() == io::FileSegment::get_caller_id());
        assert_range(2, segments[0], io::FileSegment::Range(40, 44),
                     io::FileSegment::State::DOWNLOADING);
        download(segments[0]);
    }
    {
        auto holder = cache.get_or_set(key, 50, 5, context); /// Add range [50, 54]
        auto segments = fromHolder(holder);
        ASSERT_EQ(segments.size(), 1);
        assert_range(1, segments[0], io::FileSegment::Range(50, 54), io::FileSegment::State::EMPTY);
        ASSERT_TRUE(segments[0]->get_or_set_downloader() == io::FileSegment::get_caller_id());
        assert_range(2, segments[0], io::FileSegment::Range(50, 54),
                     io::FileSegment::State::DOWNLOADING);
        download(segments[0]);
    }
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

TEST(LRUFileCache, reset_range) {
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
    settings.max_file_segment_size = 10;
    settings.max_query_cache_size = 15;
    settings.total_size = 15;
    io::CloudFileCache cache(cache_base_path, settings);
    ASSERT_TRUE(cache.initialize());
    io::CacheContext context;
    context.cache_type = io::CacheType::NORMAL;
    auto key = io::CloudFileCache::hash("key1");
    {
        auto holder = cache.get_or_set(key, 0, 9, context); /// Add range [0, 8]
        auto segments = fromHolder(holder);
        ASSERT_EQ(segments.size(), 1);
        assert_range(1, segments[0], io::FileSegment::Range(0, 8), io::FileSegment::State::EMPTY);
        ASSERT_TRUE(segments[0]->get_or_set_downloader() == io::FileSegment::get_caller_id());
        assert_range(2, segments[0], io::FileSegment::Range(0, 8),
                     io::FileSegment::State::DOWNLOADING);
        download(segments[0], 6);
    }
    {
        auto holder = cache.get_or_set(key, 0, 9, context); /// Add range [0, 8]
        auto segments = fromHolder(holder);
        ASSERT_EQ(segments.size(), 2);
        assert_range(1, segments[0], io::FileSegment::Range(0, 5),
                     io::FileSegment::State::DOWNLOADED);
        assert_range(2, segments[1], io::FileSegment::Range(6, 8), io::FileSegment::State::EMPTY);
    }
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

TEST(LRUFileCache, change_cache_type) {
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
    settings.max_file_segment_size = 10;
    settings.max_query_cache_size = 15;
    settings.total_size = 30;
    io::CloudFileCache cache(cache_base_path, settings);
    ASSERT_TRUE(cache.initialize());
    io::CacheContext context;
    context.cache_type = io::CacheType::NORMAL;
    auto key = io::CloudFileCache::hash("key1");
    {
        auto holder = cache.get_or_set(key, 0, 9, context); /// Add range [0, 8]
        auto segments = fromHolder(holder);
        ASSERT_EQ(segments.size(), 1);
        assert_range(1, segments[0], io::FileSegment::Range(0, 8), io::FileSegment::State::EMPTY);
        ASSERT_TRUE(segments[0]->get_or_set_downloader() == io::FileSegment::get_caller_id());
        assert_range(2, segments[0], io::FileSegment::Range(0, 8),
                     io::FileSegment::State::DOWNLOADING);
        size_t size = segments[0]->range().size();
        std::string data(size, '0');
        Slice result(data.data(), size);
        segments[0]->append(result);
        ASSERT_TRUE(segments[0]->change_cache_type_self(io::CacheType::INDEX));
        segments[0]->finalize_write();
        auto key_str = key.to_string();
        auto subdir = fs::path(cache_base_path) /
                      (key_str + "_" + std::to_string(segments[0]->expiration_time()));
        ASSERT_TRUE(fs::exists(subdir / "0_idx"));
    }
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

} // namespace doris::cloud
