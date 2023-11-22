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
#include <aws/s3/S3ServiceClientModel.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/GetObjectResult.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <future>
#include <ostream>
#include <regex>
#include <string_view>
#include <thread>
#include <vector>
#include <ranges>

#include "common/sync_point.h"
#include "io/cache/block/block_file_cache_downloader.h"
#include "io/cache/block/block_file_cache.h"
#include "io/cache/block/block_file_cache_factory.h"
#include "io/cache/block/block_file_cache_fwd.h"
#include "io/cache/block/block_file_cache_settings.h"
#include "io/cache/block/block_file_segment.h"
#include "io/fs/local_file_reader.h"
#include "io/fs/local_file_system.h"
#include "io/fs/s3_file_bufferpool.h"
#include "io/fs/s3_file_system.h"
#include "runtime/exec_env.h"
#include "util/s3_util.h"
#include "util/slice.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/unique_rowset_id_generator.h"
#include "olap/tablet.h"
#include "cloud/olap/storage_engine.h"

namespace fs = std::filesystem;

namespace doris::io {
std::shared_ptr<io::S3FileSystem> downloader_s3_fs = nullptr;
using FileBlocksHolderPtr = std::unique_ptr<io::FileBlocksHolder>;

extern void download_file(std::shared_ptr<Aws::S3::S3Client> client, std::string key_name,
                          size_t offset, size_t size, std::string bucket,
                          std::function<FileBlocksHolderPtr(size_t, size_t)> alloc_holder = nullptr,
                          std::function<void(Status)> download_callback = nullptr,
                          Slice s = Slice());
extern std::string cache_base_path;

static UniqueRowsetIdGenerator id_generator({0, 1});

static RowsetMetaSharedPtr create_rowset_meta(Version version, RowsetId id) {
    auto rs_meta = std::make_shared<RowsetMeta>();
    rs_meta->set_rowset_id(id);
    rs_meta->set_rowset_type(BETA_ROWSET);
    rs_meta->_rowset_meta_pb.set_start_version(version.first);
    rs_meta->_rowset_meta_pb.set_end_version(version.second);
    rs_meta->set_fs(downloader_s3_fs);
    return rs_meta;
}

RowsetSharedPtr create_rowset(Version version, RowsetId id) {
    auto rs_meta = create_rowset_meta(version, id);
    RowsetSharedPtr rowset;
    RowsetFactory::create_rowset(nullptr, "", std::move(rs_meta), &rowset);
    return rowset;
}

fs::path tmp_file_1 = fs::current_path() / "tmp_file";

class MockDownloadS3Client {
public:
    auto GetObject(const Aws::S3::Model::GetObjectRequest& req, Slice* s) {
        LOG_INFO("enter mocked get object function");
        auto key = req.GetKey();
        auto local_fs = io::global_local_filesystem();
        io::FileReaderSPtr reader;
        if (auto st = local_fs->open_file(key, &reader); !st.ok()) {
            return Aws::S3::Model::GetObjectOutcome(
                    Aws::Client::AWSError<Aws::S3::S3Errors>(Aws::S3::S3Errors::UNKNOWN, false));
        }
        auto range = req.GetRange();
        std::regex pattern(R"(bytes=(\d+)-(\d+))");
        std::smatch matches;
        int64_t start = 0;
        int64_t end = 0;
        if (std::regex_search(range, matches, pattern)) {
            if (matches.size() == 3) {
                std::string value1 = matches[1].str();
                start = std::stoi(value1);
                std::string value2 = matches[2].str();
                end = std::stoi(value2);
            }
        } else {
            return Aws::S3::Model::GetObjectOutcome(
                    Aws::Client::AWSError<Aws::S3::S3Errors>(Aws::S3::S3Errors::UNKNOWN, false));
        }
        if (start > end) {
            return Aws::S3::Model::GetObjectOutcome(
                    Aws::Client::AWSError<Aws::S3::S3Errors>(Aws::S3::S3Errors::UNKNOWN, false));
        }
        size_t bytes_read = 0;
        size_t len = end - start + 1;
        Slice slice(s->get_data(), len);
        if (auto st = (*reader).read_at(start, slice, &bytes_read); !st.ok() || bytes_read != len) {
            return Aws::S3::Model::GetObjectOutcome(
                    Aws::Client::AWSError<Aws::S3::S3Errors>(Aws::S3::S3Errors::UNKNOWN, false));
        }
        Aws::S3::Model::GetObjectResult result;
        result.SetContentLength(bytes_read);
        // result.
        return Aws::S3::Model::GetObjectOutcome(std::move(result));
    }
};

static std::shared_ptr<MockDownloadS3Client> download_client = nullptr;

struct MockCallback {
    std::string point_name;
    std::function<void(std::vector<std::any>&&)> callback;
};

static auto test_downloader_mock_callbacks = std::array {
        MockCallback {"io::_download_part",
                      [](auto&& outcome) {
                          LOG_INFO("enter fake download file");
                          const auto& req = try_any_cast<const Aws::S3::Model::GetObjectRequest&>(
                                  outcome.at(0));
                          auto s = try_any_cast<Slice*>(outcome.at(1));
                          auto pair = try_any_cast_ret<Aws::S3::Model::GetObjectOutcome>(outcome);
                          pair->second = true;
                          pair->first = download_client->GetObject(req, s);
                      }},
        MockCallback {"s3_client_factory::create", [](auto&& outcome) {
                          LOG_INFO("return fake s3 client");
                          auto pair = try_any_cast_ret<std::shared_ptr<Aws::S3::S3Client>>(outcome);
                          pair->second = true;
                      }}};

class S3DownloaderTest : public testing::Test {
public:
    static void SetUpTestSuite() {
        auto sp = SyncPoint::get_instance();
        sp->enable_processing();
        config::file_cache_enter_disk_resource_limit_mode_percent = 99;
        config::enable_file_cache = true;
        std::for_each(test_downloader_mock_callbacks.begin(), test_downloader_mock_callbacks.end(),
                      [sp](const MockCallback& mockcallback) {
                          sp->set_call_back(mockcallback.point_name, mockcallback.callback);
                      });
        auto cur_path = std::filesystem::current_path();
        config::tmp_file_dirs =
                R"([{"path":")" + cur_path.native() +
                R"(/ut_dir/s3_file_system_test/cache","max_cache_bytes":21474836480,"max_upload_bytes":10737418240}])";
        S3Conf s3_conf;
        s3_conf.ak = config::test_s3_ak;
        s3_conf.sk = config::test_s3_sk;
        s3_conf.endpoint = config::test_s3_endpoint;
        s3_conf.region = config::test_s3_region;
        s3_conf.bucket = config::test_s3_bucket;
        s3_conf.prefix = "s3_file_system_test";
        downloader_s3_fs =
                std::make_shared<io::S3FileSystem>(std::move(s3_conf), "s3_file_system_test");
        LOG_INFO("try to connect fs");
        ASSERT_EQ(Status::OK(), downloader_s3_fs->connect());

        std::unique_ptr<ThreadPool> _pool;
        ThreadPoolBuilder("s3_upload_file_thread_pool")
                .set_min_threads(5)
                .set_max_threads(10)
                .build(&_pool);
        ExecEnv::GetInstance()->_s3_downloader_download_thread_pool = std::move(_pool);
        doris::io::S3FileBufferPool* s3_buffer_pool = doris::io::S3FileBufferPool::GetInstance();
        if (s3_buffer_pool->_whole_mem_buffer != nullptr) {
            return;
        }
        s3_buffer_pool->init(doris::config::s3_write_buffer_whole_size,
                             doris::config::s3_write_buffer_size,
                             ExecEnv::GetInstance()->_s3_downloader_download_thread_pool.get());
        bool exists {false};
        ASSERT_TRUE(global_local_filesystem()->exists(tmp_file_1, &exists).ok());
        if (!exists) {
            FileWriterPtr writer;
            ASSERT_TRUE(global_local_filesystem()->create_file(tmp_file_1, &writer).ok());
            for (int i = 0 ; i < 10; i++) {
                std::string data(1 * 1024 * 1024, '0' + i);
                ASSERT_TRUE(writer->append(Slice(data.data(), data.size())).ok());
            }
            std::string data(1, '0');
            ASSERT_TRUE(writer->append(Slice(data.data(), data.size())).ok());
            ASSERT_TRUE(writer->finalize().ok());
            ASSERT_TRUE(writer->close().ok());
        }
    }

    static void TearDownTestSuite() {
        auto sp = SyncPoint::get_instance();
        std::for_each(test_downloader_mock_callbacks.begin(), test_downloader_mock_callbacks.end(),
                      [sp](const MockCallback& mockcallback) {
                          sp->clear_call_back(mockcallback.point_name);
                      });
        sp->disable_processing();
        config::file_cache_enter_disk_resource_limit_mode_percent = 90;
        config::enable_file_cache = false;
    }
};

TEST_F(S3DownloaderTest, normal) {
    download_client = std::make_shared<MockDownloadS3Client>();
    auto cur_path = std::filesystem::current_path();
    // upload normal file
    auto local_fs = io::global_local_filesystem();
    io::FileWriterPtr local_file;
    auto file_path = cur_path / "be/test/io/test_data/s3_downloader_test/normal";
    auto st = local_fs->create_file(file_path, &local_file);
    ASSERT_TRUE(st.ok());
    std::string_view content = "some bytes, damn it";
    st = local_file->append({content.data(), content.size()});
    ASSERT_TRUE(st.ok());
    st = local_file->close();
    EXPECT_TRUE(st.ok());
    char buf[100];
    std::promise<Status> pro;
    io::download_file(
            downloader_s3_fs->get_client(), file_path, 0, content.size(),
            downloader_s3_fs->s3_conf().bucket, nullptr,
            [&](Status s) { pro.set_value(std::move(s)); }, Slice {buf, 100});
    auto f = pro.get_future();
    ASSERT_TRUE(f.get());
    std::string_view result {buf, content.size()};
    ASSERT_EQ(result, content);
}

TEST_F(S3DownloaderTest, error) {
    download_client = std::make_shared<MockDownloadS3Client>();
    auto cur_path = std::filesystem::current_path();
    // upload normal file
    auto local_fs = io::global_local_filesystem();
    io::FileWriterPtr local_file;
    auto file_path = cur_path / "be/test/io/test_data/s3_downloader_test/error";
    auto st = local_fs->create_file(file_path, &local_file);
    ASSERT_TRUE(st.ok());
    std::string_view content = "some bytes, damn it";
    st = local_file->append({content.data(), content.size()});
    ASSERT_TRUE(st.ok());
    st = local_file->close();
    EXPECT_TRUE(st.ok());
    auto sp = SyncPoint::get_instance();
    Defer defer {[sp] { sp->clear_call_back("io::_download_part::error"); }};
    sp->set_call_back("io::_download_part::error", [](auto&& outcome) {
        Aws::S3::Model::GetObjectOutcome* ret =
                try_any_cast<Aws::S3::Model::GetObjectOutcome*>(outcome.back());
        *ret = Aws::S3::Model::GetObjectOutcome(
                Aws::Client::AWSError<Aws::S3::S3Errors>(Aws::S3::S3Errors::UNKNOWN, false));
    });
    char buf[100];
    std::promise<Status> pro;
    io::download_file(
            downloader_s3_fs->get_client(), file_path, 0, content.size(),
            downloader_s3_fs->s3_conf().bucket, nullptr,
            [&](Status s) { pro.set_value(std::move(s)); }, Slice {buf, 100});
    auto f = pro.get_future();
    auto s = f.get();
    ASSERT_FALSE(s.ok()) << s;
}

TEST_F(S3DownloaderTest, DISABLED_multipart) {
    download_client = std::make_shared<MockDownloadS3Client>();
    auto cur_path = std::filesystem::current_path();
    // upload normal file
    auto local_fs = io::global_local_filesystem();
    io::FileWriterPtr local_file;
    auto file_path = cur_path / "be/test/io/test_data/s3_downloader_test/normal";
    auto st = local_fs->create_file(file_path, &local_file);
    ASSERT_TRUE(st.ok());
    std::string_view content = "some bytes, damn it";
    st = local_file->append({content.data(), content.size()});
    ASSERT_TRUE(st.ok());
    st = local_file->close();
    EXPECT_TRUE(st.ok());
    char buf[100];
    std::promise<Status> pro;
    io::download_file(
            downloader_s3_fs->get_client(), file_path, 0, content.size() / 2,
            downloader_s3_fs->s3_conf().bucket, nullptr,
            [&](Status s) { pro.set_value(std::move(s)); }, Slice {buf, 100});
    auto f = pro.get_future();
    ASSERT_TRUE(f.get());
    std::promise<Status> another_pro;
    io::download_file(
            downloader_s3_fs->get_client(), file_path, content.size() / 2,
            content.size() - (content.size() / 2), downloader_s3_fs->s3_conf().bucket, nullptr,
            [&](Status s) { another_pro.set_value(std::move(s)); }, Slice {buf, 100});
    auto another_f = another_pro.get_future();
    ASSERT_TRUE(another_f.get());
    std::string_view result {buf, content.size()};
    ASSERT_EQ(result, content);
}

TEST_F(S3DownloaderTest, block_file_cache_downloader_test_no_cache) {
    config::enable_file_cache = false;
    FileCacheSegmentS3Downloader::create_preheating_s3_downloader();
    std::vector<FileCacheSegmentMeta> metas;
    DownloadTask task1(std::move(metas));
    FileCacheSegmentDownloader::instance()->submit_download_task(std::move(task1));
    S3FileMeta file_meta;
    DownloadTask task2(std::move(file_meta));
    FileCacheSegmentDownloader::instance()->submit_download_task(std::move(task2));
    config::enable_file_cache = true;
}

TEST_F(S3DownloaderTest, block_file_cache_downloader_test_1) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 1;
    io::FileCacheSettings settings;
    settings.query_queue_size = 15728640;
    settings.query_queue_elements = 15;
    settings.total_size = 15728640;
    settings.max_file_block_size = 1048576;
    settings.max_query_cache_size = 0;
    io::CacheContext context;
    context.cache_type = io::FileCacheType::NORMAL;
    auto sp = SyncPoint::get_instance();
    sp->set_call_back("BlockFileCache::remove_prefix", [](auto&& args) { 
        *try_any_cast<std::string*>(args[0]) = tmp_file_1;
    });
    Defer defer {[sp] { sp->clear_call_back("BlockFileCache::remove_prefix"); }};
    ASSERT_TRUE(FileCacheFactory::instance().create_file_cache(cache_base_path, settings).ok());
    FileCacheSegmentS3Downloader::create_preheating_s3_downloader();
    S3FileMeta meta;
    meta.path = tmp_file_1;
    meta.file_size = 10 * 1024 * 1024 + 1;
    CountDownLatch latch(1);
    meta.file_system = downloader_s3_fs;
    meta.download_callback = [&](Status s) { 
        ASSERT_TRUE(s.ok());
        latch.count_down(1);
    };
    FileCacheSegmentDownloader::instance()->submit_download_task(std::move(meta));
    latch.wait();
    {
        auto key = io::BlockFileCache::hash("tmp_file");
        auto cache = FileCacheFactory::instance().get_by_path(key);
        auto holders = cache->get_or_set(key, 0, 10 * 1024 * 1024 + 1, context);
        int i = 0;
        std::ranges::for_each(holders.file_segments, [&](FileBlockSPtr& block) {
            std::string buffer;
            buffer.resize(i == 10 ? 1 : 1024*1024);
            ASSERT_TRUE(block->read_at(Slice(buffer.data(), buffer.size()), 0).ok());
            EXPECT_EQ(std::string(i == 10 ? 1 : 1024*1024, '0' + (i % 10)), buffer);
            i++;
        });
    }
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    FileCacheFactory::instance()._caches.clear();
    FileCacheFactory::instance()._path_to_cache.clear();
    FileCacheFactory::instance()._total_cache_size = 0;
}

TEST_F(S3DownloaderTest, block_file_cache_downloader_test_2) {
    StorageEngine storage_engine({});
    auto sp = SyncPoint::get_instance();
    Defer defer {[sp] {
        sp->clear_call_back("CloudTabletMgr::get_tablet_2");
        sp->clear_call_back("BlockFileCache::mock_key");
        sp->clear_call_back("FileCacheSegmentS3Downloader::download_file_cache_segment");
    }};
    sp->enable_processing();
    RowsetId rowset_id = id_generator.next_id();
    sp->set_call_back("BlockFileCache::mock_key", [](auto&& args) { 
        *try_any_cast<std::string*>(args[0]) = tmp_file_1;
    });
    sp->set_call_back("CloudTabletMgr::get_tablet_2", [&](auto&& args) {
        auto tablet_id = try_any_cast<int64_t>(args[0]);
        auto tablet = try_any_cast<TabletSharedPtr*>(args[1]);
        auto tablet_meta = std::make_shared<TabletMeta>();
        tablet_meta->_tablet_id = tablet_id;
        *tablet = std::make_shared<Tablet>(std::move(tablet_meta), nullptr);
        RowsetSharedPtr rowset = create_rowset(Version(0, 1), rowset_id);
        (*tablet)->cloud_add_rowsets({rowset}, false);
        auto pair = try_any_cast<std::pair<Status, bool>*>(args.back());
        pair->second = true;
    });
    bool flag1 = false;
    bool flag2 = false;
    sp->set_call_back("FileCacheSegmentS3Downloader::download_file_cache_segment", [&](auto&&) {
        while (!flag1) {
        }
        flag2 = true;
    });
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 1;
    io::FileCacheSettings settings;
    settings.query_queue_size = 15728640;
    settings.query_queue_elements = 15;
    settings.total_size = 15728640;
    settings.max_file_block_size = 1048576;
    settings.max_query_cache_size = 0;
    io::CacheContext context;
    context.cache_type = io::FileCacheType::NORMAL;
    ASSERT_TRUE(FileCacheFactory::instance().create_file_cache(cache_base_path, settings).ok());
    FileCacheSegmentS3Downloader::create_preheating_s3_downloader();
    FileCacheSegmentMeta meta;
    meta.set_tablet_id(1000);
    meta.set_rowset_id(rowset_id.to_string());
    meta.set_segment_id(0);
    meta.set_file_name("tmp_file");
    meta.set_offset(0);
    meta.set_size(10 * 1024 * 1024 + 1);
    meta.set_cache_type(::doris::FileCacheType::NORMAL);
    meta.set_expiration_time(0);
    std::vector<FileCacheSegmentMeta> metas;
    metas.push_back(meta);
    FileCacheSegmentDownloader::instance()->submit_download_task(metas);
    std::vector<int64_t> tablets {1000};
    std::map<int64_t, bool> done;
    FileCacheSegmentDownloader::instance()->check_download_task(tablets, &done);
    EXPECT_FALSE(done[1000]);
    flag1 = true;
    while (!flag2) {}
    {
        auto key = io::BlockFileCache::hash("tmp_file");
        auto cache = FileCacheFactory::instance().get_by_path(key);
        auto holders = cache->get_or_set(key, 0, 10 * 1024 * 1024 + 1, context);
        int i = 0;
        std::ranges::for_each(holders.file_segments, [&](FileBlockSPtr& block) {
            std::string buffer;
            buffer.resize(i == 10 ? 1 : 1024*1024);
            ASSERT_TRUE(block->read_at(Slice(buffer.data(), buffer.size()), 0).ok());
            EXPECT_EQ(std::string(i == 10 ? 1 : 1024*1024, '0' + (i % 10)), buffer);
            i++;
        });
    }
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    FileCacheFactory::instance()._caches.clear();
    FileCacheFactory::instance()._path_to_cache.clear();
    FileCacheFactory::instance()._total_cache_size = 0;
}


} // namespace doris::io
