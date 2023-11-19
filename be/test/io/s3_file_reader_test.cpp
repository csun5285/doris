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

#include "io/fs/s3_file_reader.h"

#include <aws/s3/S3ServiceClientModel.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/GetObjectResult.h>
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
#include "io/fs/file_reader_writer_fwd.h"
#include "io/fs/local_file_system.h"
#include "io/fs/s3_file_system.h"
#include "io/fs/s3_file_writer.h"
#include "io/io_common.h"
#include "runtime/exec_env.h"
#include "util/slice.h"
#include "util/threadpool.h"
namespace doris {

static std::shared_ptr<io::S3FileSystem> s3_fs {nullptr};

static bool change_return_range = false;
// This MockS3ReadClient is only responsible for handling normal situations,
// while error injection is left to other macros to resolve
class MockS3ReadClient {
public:
    MockS3ReadClient(std::string name, size_t file_size) : _file_name(std::move(name)) {
        _content.resize(file_size);
    }
    ~MockS3ReadClient() = default;

    auto GetObject(const Aws::S3::Model::GetObjectRequest& req, Slice* s) {
        LOG_INFO("enter mocked get object function");
        auto key = req.GetKey();
        if (key != _file_name) {
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
        size_t len = end - start + 1;
        size_t bytes_read = len;
        Slice slice(s->get_data(), len);
        std::memcpy(s->data, _content.data() + start, len);
        Aws::S3::Model::GetObjectResult result;
        result.SetContentLength(bytes_read);
        if (change_return_range) {
            result.SetContentLength(0);
        }
        return Aws::S3::Model::GetObjectOutcome(std::move(result));
    }

private:
    std::string _file_name;
    std::string _content;
};

static std::shared_ptr<MockS3ReadClient> mock_client = nullptr;

struct MockCallback {
    std::string point_name;
    std::function<void(std::vector<std::any>&&)> callback;
};

static auto test_mock_callbacks = std::array {
        MockCallback {"s3_file_reader::get_object",
                      [](auto&& outcome) {
                          const auto& req = try_any_cast<const Aws::S3::Model::GetObjectRequest&>(
                                  outcome.at(0));
                          Slice* result = try_any_cast<Slice*>(outcome.at(1));
                          auto pair = try_any_cast_ret<Aws::S3::Model::GetObjectOutcome>(outcome);
                          pair->second = true;
                          pair->first = mock_client->GetObject(req, result);
                      }},
        MockCallback {"s3_client_factory::create", [](auto&& outcome) {
                          auto pair = try_any_cast_ret<std::shared_ptr<Aws::S3::S3Client>>(outcome);
                          pair->second = true;
                      }}};

class S3FileReaderTest : public testing::Test {
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
        S3Conf s3_conf;
        s3_conf.ak = "fake_ak";
        s3_conf.sk = "fake_sk";
        s3_conf.endpoint = "fake_s3_endpoint";
        s3_conf.region = "fake_s3_region";
        s3_conf.bucket = "fake_s3_bucket";
        s3_conf.prefix = "s3_file_reader_test";
        std::cout << "s3 conf: " << s3_conf.to_string() << std::endl;
        s3_fs = std::make_shared<io::S3FileSystem>(std::move(s3_conf), "s3_file_reader_test");
        auto st = s3_fs->connect();
        ASSERT_TRUE(st.ok()) << st;
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

TEST_F(S3FileReaderTest, normal) {
    mock_client = std::make_shared<MockS3ReadClient>("normal", 100);
    Defer defer {[&]() { mock_client = nullptr; }};
    std::shared_ptr<io::S3FileReader> reader =
            std::make_shared<io::S3FileReader>(0, "normal", s3_fs);
    std::unique_ptr<char[]> buf = std::make_unique<char[]>(100);
    Slice result {buf.get(), 100};
    size_t bytes_read = 0;
    auto st = reader->read_at(0, result, &bytes_read);
    ASSERT_TRUE(st.ok()) << st.to_string();
}

TEST_F(S3FileReaderTest, error) {
    mock_client = std::make_shared<MockS3ReadClient>("exist", 100);
    Defer defer {[&]() { mock_client = nullptr; }};
    {
        // offset > file size
        std::shared_ptr<io::S3FileReader> reader =
                std::make_shared<io::S3FileReader>(0, "not_exist", s3_fs);
        std::unique_ptr<char[]> buf = std::make_unique<char[]>(100);
        Slice result {buf.get(), 100};
        size_t bytes_read = 0;
        auto st = reader->read_at(10, result, &bytes_read);
        ASSERT_FALSE(st.ok());
    }
    {
        // return range no match
        change_return_range = true;
        Defer defer {[&]() { change_return_range = false; }};
        std::shared_ptr<io::S3FileReader> reader =
                std::make_shared<io::S3FileReader>(100, "exist", s3_fs);
        std::unique_ptr<char[]> buf = std::make_unique<char[]>(100);
        Slice result {buf.get(), 100};
        size_t bytes_read = 0;
        auto st = reader->read_at(0, result, &bytes_read);
        ASSERT_FALSE(st.ok());
    }
    {
        // no s3 client
        auto fake_s3_fs = s3_fs;
        fake_s3_fs->_client = nullptr;
        std::shared_ptr<io::S3FileReader> reader =
                std::make_shared<io::S3FileReader>(100, "exist", fake_s3_fs);
        std::unique_ptr<char[]> buf = std::make_unique<char[]>(100);
        Slice result {buf.get(), 100};
        size_t bytes_read = 0;
        auto st = reader->read_at(0, result, &bytes_read);
        ASSERT_FALSE(st.ok());
    }
}

} // namespace doris