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
#include <vector>

#include "common/sync_point.h"
#include "io/cache/block/block_file_segment.h"
#include "io/fs/local_file_reader.h"
#include "io/fs/local_file_system.h"
#include "io/fs/s3_file_bufferpool.h"
#include "io/fs/s3_file_system.h"
#include "runtime/exec_env.h"
#include "util/s3_util.h"
#include "util/slice.h"

namespace doris {
std::shared_ptr<io::S3FileSystem> downloader_s3_fs = nullptr;
using FileBlocksHolderPtr = std::unique_ptr<io::FileBlocksHolder>;
namespace io {
extern void download_file(std::shared_ptr<Aws::S3::S3Client> client, std::string key_name,
                          size_t offset, size_t size, std::string bucket,
                          std::function<FileBlocksHolderPtr(size_t, size_t)> alloc_holder = nullptr,
                          std::function<void(Status)> download_callback = nullptr,
                          Slice s = Slice());
}

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
    }

    static void TearDownTestSuite() {
        auto sp = SyncPoint::get_instance();
        std::for_each(test_downloader_mock_callbacks.begin(), test_downloader_mock_callbacks.end(),
                      [sp](const MockCallback& mockcallback) {
                          sp->clear_call_back(mockcallback.point_name);
                      });
        sp->disable_processing();
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

} // namespace doris
