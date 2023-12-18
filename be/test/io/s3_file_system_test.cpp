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

#include "io/fs/s3_file_system.h"

#include <aws/s3/S3ServiceClientModel.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/HeadObjectResult.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/ListObjectsV2Result.h>
#include <gtest/gtest.h>

#include <any>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <functional>
#include <memory>
#include <mutex>
#include <regex>
#include <string>
#include <system_error>
#include <thread>

#include "common/status.h"
#include "common/sync_point.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_reader_options.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "io/fs/s3_file_reader.h"
#include "io/fs/s3_file_writer.h"
#include "runtime/exec_env.h"
#include "util/defer_op.h"
#include "util/s3_util.h"
#include "util/slice.h"
#include "util/threadpool.h"
namespace doris {

static std::shared_ptr<io::S3FileSystem> s3_fs {nullptr};
static bool head_object_return_error = false;

// This MockS3ReadClient is only responsible for handling normal situations,
// while error injection is left to other macros to resolve
class MockS3FileSystemClient {
public:
    MockS3FileSystemClient(std::string name, size_t file_size) : _file_name(std::move(name)) {
        _content.resize(file_size);
    }
    ~MockS3FileSystemClient() = default;

    auto HeadObject(const Aws::S3::Model::HeadObjectRequest& req) {
        LOG_INFO("the path is {}, file name {}", req.GetKey(), _file_name);
        if (head_object_return_error) {
            auto err = Aws::Client::AWSError<Aws::S3::S3Errors>(Aws::S3::S3Errors::UNKNOWN, false);
            return Aws::S3::Model::HeadObjectOutcome(std::move(err));
        }
        if (_file_name != req.GetKey()) {
            auto err = Aws::Client::AWSError<Aws::S3::S3Errors>(Aws::S3::S3Errors::UNKNOWN, false);
            err.SetResponseCode(Aws::Http::HttpResponseCode::NOT_FOUND);
            return Aws::S3::Model::HeadObjectOutcome(std::move(err));
        }
        auto result = Aws::S3::Model::HeadObjectResult();
        result.SetContentLength(_content.size());
        return Aws::S3::Model::HeadObjectOutcome(std::move(result));
    }

    auto ListObjectV2(const Aws::S3::Model::ListObjectsV2Request& req) {
        return Aws::S3::Model::ListObjectsV2Outcome();
    }

private:
    std::string _file_name;
    std::string _content;
};

static std::shared_ptr<MockS3FileSystemClient> mock_client = nullptr;

struct MockCallback {
    std::string point_name;
    std::function<void(std::vector<std::any>&&)> callback;
};

static bool return_nullptr_s3_client = false;

static auto test_mock_callbacks = std::array {
        MockCallback {"s3_file_system::head_object",
                      [](auto&& outcome) {
                          const auto& req = try_any_cast<const Aws::S3::Model::HeadObjectRequest&>(
                                  outcome.at(0));
                          auto pair = try_any_cast_ret<Aws::S3::Model::HeadObjectOutcome>(outcome);
                          pair->second = true;
                          pair->first = mock_client->HeadObject(req);
                      }},
        MockCallback {"s3_file_system::list_object",
                      [](auto&& outcome) {
                          const auto& req =
                                  try_any_cast<const Aws::S3::Model::ListObjectsV2Request&>(
                                          outcome.at(0));
                          auto pair =
                                  try_any_cast_ret<Aws::S3::Model::ListObjectsV2Outcome>(outcome);
                          pair->second = true;
                          pair->first = mock_client->ListObjectV2(req);
                      }},
        MockCallback {"s3_client_factory::create", [](auto&& outcome) {
                          auto pair = try_any_cast_ret<std::shared_ptr<Aws::S3::S3Client>>(outcome);
                          if (return_nullptr_s3_client) {
                              pair->first = nullptr;
                          }
                          pair->second = true;
                      }}};

static S3Conf s3_conf;
class S3FileSystemTest : public testing::Test {
public:
    static void SetUpTestSuite() {
        auto sp = SyncPoint::get_instance();
        sp->enable_processing();
        config::file_cache_enter_disk_resource_limit_mode_percent = 99;
        std::for_each(test_mock_callbacks.begin(), test_mock_callbacks.end(),
                      [sp](const MockCallback& mockcallback) {
                          sp->set_call_back(mockcallback.point_name, mockcallback.callback);
                      });
        std::string cur_path = std::filesystem::current_path();
        s3_conf.ak = "fake_ak";
        s3_conf.sk = "fake_sk";
        s3_conf.endpoint = "fake_s3_endpoint";
        s3_conf.region = "fake_s3_region";
        s3_conf.bucket = "fake_s3_bucket";
        s3_conf.prefix = "s3_file_system_test";
        std::cout << "s3 conf: " << s3_conf.to_string() << std::endl;
    }

    static void TearDownTestSuite() {
        auto sp = SyncPoint::get_instance();
        std::for_each(test_mock_callbacks.begin(), test_mock_callbacks.end(),
                      [sp](const MockCallback& mockcallback) {
                          sp->clear_call_back(mockcallback.point_name);
                      });
        sp->disable_processing();
    }
};

TEST_F(S3FileSystemTest, connect) {
    mock_client = std::make_shared<MockS3FileSystemClient>("connect", 100);
    Defer defer {[&]() {
        mock_client = nullptr;
        return_nullptr_s3_client = false;
    }};
    {
        auto s3_conf_temp = s3_conf;
        s3_fs = std::make_shared<io::S3FileSystem>(std::move(s3_conf_temp),
                                                   "s3_file_system_connect_normal");
        auto st = s3_fs->connect();
        ASSERT_TRUE(st.ok()) << st;
    }
    {
        return_nullptr_s3_client = true;
        auto s3_conf_temp = s3_conf;
        s3_fs = std::make_shared<io::S3FileSystem>(std::move(s3_conf_temp),
                                                   "s3_file_system_connect_normal");
        auto st = s3_fs->connect();
        ASSERT_FALSE(st.ok()) << st;
    }
}

TEST_F(S3FileSystemTest, create_file) {
    mock_client = std::make_shared<MockS3FileSystemClient>("create_file", 100);
    Defer defer {[&]() {
        mock_client = nullptr;
        return_nullptr_s3_client = false;
    }};
    auto s3_conf_temp = s3_conf;
    s3_fs = std::make_shared<io::S3FileSystem>(std::move(s3_conf_temp),
                                               "s3_file_system_create_file");
    auto st = s3_fs->connect();
    ASSERT_TRUE(st.ok()) << st;
    io::FileWriterPtr writer;
    st = s3_fs->create_file("create_file", &writer);
    ASSERT_TRUE(st.ok()) << st;
}

TEST_F(S3FileSystemTest, open_file) {
    mock_client = std::make_shared<MockS3FileSystemClient>("s3_file_system_test/open_file", 100);
    Defer defer {[&]() {
        mock_client = nullptr;
        return_nullptr_s3_client = false;
    }};
    {
        auto s3_conf_temp = s3_conf;
        s3_fs = std::make_shared<io::S3FileSystem>(std::move(s3_conf_temp),
                                                   "s3_file_system_open_file");
        auto st = s3_fs->connect();
        ASSERT_TRUE(st.ok()) << st;
        io::FileReaderSPtr reader;
        io::FileReaderOptions options(io::FileCachePolicy::NO_CACHE, io::NoCachePathPolicy());
        options.file_size = 1;
        st = s3_fs->open_file("open_file", &reader);
        ASSERT_TRUE(st.ok()) << st;
    }
    {
        head_object_return_error = true;
        Defer de {[&]() { head_object_return_error = false; }};
        auto s3_conf_temp = s3_conf;
        s3_fs = std::make_shared<io::S3FileSystem>(std::move(s3_conf_temp),
                                                   "s3_file_system_open_file");
        auto st = s3_fs->connect();
        ASSERT_TRUE(st.ok()) << st;
        io::FileReaderSPtr reader;
        st = s3_fs->open_file("open_file", &reader);
        ASSERT_FALSE(st.ok()) << st;
    }
    {
        io::FileReaderOptions options(io::FileCachePolicy::NO_CACHE, io::NoCachePathPolicy());
        io::FileReaderSPtr reader;
        auto s3_conf_temp = s3_conf;
        s3_fs = std::make_shared<io::S3FileSystem>(std::move(s3_conf_temp),
                                                   "s3_file_system_open_file");
        auto st = s3_fs->connect();
        ASSERT_TRUE(st.ok()) << st;
        st = s3_fs->open_file("open_file", &reader, &options);
        ASSERT_TRUE(st.ok()) << st;
    }
    {
        head_object_return_error = true;
        Defer de {[&]() { head_object_return_error = false; }};
        io::FileReaderOptions options(io::FileCachePolicy::NO_CACHE, io::NoCachePathPolicy());
        io::FileReaderSPtr reader;
        auto s3_conf_temp = s3_conf;
        s3_fs = std::make_shared<io::S3FileSystem>(std::move(s3_conf_temp),
                                                   "s3_file_system_open_file");
        auto st = s3_fs->connect();
        ASSERT_TRUE(st.ok()) << st;
        st = s3_fs->open_file("open_file", &reader, &options);
        ASSERT_FALSE(st.ok()) << st;
    }
    {
        io::FileDescription fd;
        fd.path = "open_file";
        io::FileReaderOptions options(io::FileCachePolicy::NO_CACHE, io::NoCachePathPolicy());
        io::FileReaderSPtr reader;
        auto s3_conf_temp = s3_conf;
        s3_fs = std::make_shared<io::S3FileSystem>(std::move(s3_conf_temp),
                                                   "s3_file_system_open_file");
        auto st = s3_fs->connect();
        ASSERT_TRUE(st.ok()) << st;
        st = s3_fs->open_file(fd, options, &reader);
        ASSERT_TRUE(st.ok()) << st;
    }
    {
        head_object_return_error = true;
        Defer de {[&]() { head_object_return_error = false; }};
        io::FileDescription fd;
        fd.path = "open_file";
        io::FileReaderOptions options(io::FileCachePolicy::NO_CACHE, io::NoCachePathPolicy());
        io::FileReaderSPtr reader;
        auto s3_conf_temp = s3_conf;
        s3_fs = std::make_shared<io::S3FileSystem>(std::move(s3_conf_temp),
                                                   "s3_file_system_open_file");
        auto st = s3_fs->connect();
        ASSERT_TRUE(st.ok()) << st;
        st = s3_fs->open_file(fd, options, &reader);
        ASSERT_FALSE(st.ok()) << st;
    }
}

TEST_F(S3FileSystemTest, exists) {
    mock_client = std::make_shared<MockS3FileSystemClient>("s3_file_system_test/exists", 100);
    Defer defer {[&]() { head_object_return_error = false; }};
    {
        auto s3_conf_temp = s3_conf;
        s3_fs = std::make_shared<io::S3FileSystem>(std::move(s3_conf_temp),
                                                   "s3_file_system_exists");
        auto st = s3_fs->connect();
        ASSERT_TRUE(st.ok()) << st;
        bool exist = false;
        auto s = s3_fs->exists("exists", &exist);
        ASSERT_TRUE(s.ok()) << s;
        ASSERT_TRUE(exist);
    }
    {
        auto s3_conf_temp = s3_conf;
        s3_fs = std::make_shared<io::S3FileSystem>(std::move(s3_conf_temp),
                                                   "s3_file_system_exists");
        auto st = s3_fs->connect();
        ASSERT_TRUE(st.ok()) << st;
        bool exist = false;
        auto s = s3_fs->exists("not_exists", &exist);
        ASSERT_TRUE(s.ok()) << s;
        ASSERT_TRUE(!exist);
    }
    {
        auto s3_conf_temp = s3_conf;
        s3_fs = std::make_shared<io::S3FileSystem>(std::move(s3_conf_temp),
                                                   "s3_file_system_exists");
        auto st = s3_fs->connect();
        ASSERT_TRUE(st.ok()) << st;
        head_object_return_error = true;
        bool exist = false;
        auto s = s3_fs->exists("not_exists", &exist);
        ASSERT_FALSE(s.ok()) << s;
    }
}

TEST_F(S3FileSystemTest, file_size) {
    Defer defer {[&]() { head_object_return_error = false; }};
    mock_client = std::make_shared<MockS3FileSystemClient>(
            "s3_file_system_test/s3_file_system_file_size", 100);
    {
        auto s3_conf_temp = s3_conf;
        s3_fs = std::make_shared<io::S3FileSystem>(std::move(s3_conf_temp),
                                                   "s3_file_system_file_size");
        auto st = s3_fs->connect();
        ASSERT_TRUE(st.ok()) << st;
        int64_t size = false;
        auto s = s3_fs->file_size("s3_file_system_file_size", &size);
        ASSERT_TRUE(s.ok()) << s;
        ASSERT_TRUE(size == 100);
    }
    {
        auto s3_conf_temp = s3_conf;
        s3_fs = std::make_shared<io::S3FileSystem>(std::move(s3_conf_temp),
                                                   "s3_file_system_file_size");
        auto st = s3_fs->connect();
        ASSERT_TRUE(st.ok()) << st;
        head_object_return_error = true;
        int64_t size = false;
        auto s = s3_fs->file_size("s3_file_system_file_size", &size);
        ASSERT_FALSE(s.ok()) << s;
    }
}

} // namespace doris