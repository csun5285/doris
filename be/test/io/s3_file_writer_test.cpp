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

#include <aws/s3/S3Client.h>
#include <aws/s3/model/CompletedPart.h>
#include <aws/s3/model/UploadPartRequest.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <memory>
#include <thread>

#include "common/sync_point.h"
#include "io/cache/block/block_file_cache.h"
#include "io/cache/block/block_file_cache_factory.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "io/fs/s3_file_bufferpool.h"
#include "io/fs/s3_file_system.h"
#include "io/fs/s3_file_writer.h"
#include "io/io_common.h"
#include "runtime/exec_env.h"
#include "util/slice.h"
#include "util/threadpool.h"
namespace doris {

static std::shared_ptr<io::S3FileSystem> s3_fs {nullptr};

class S3FileWriterTest : public testing::Test {
public:
    static void SetUpTestSuite() {
        std::string cur_path = std::filesystem::current_path();
        S3Conf s3_conf;
        s3_conf.ak = config::test_s3_ak;
        s3_conf.sk = config::test_s3_sk;
        s3_conf.endpoint = config::test_s3_endpoint;
        s3_conf.region = config::test_s3_region;
        s3_conf.bucket = config::test_s3_bucket;
        s3_conf.prefix = "s3_file_writer_test";
        s3_fs = std::make_shared<io::S3FileSystem>(std::move(s3_conf), "s3_file_writer_test");
        std::cout << "s3 conf: " << s3_conf.to_string() << std::endl;
        ASSERT_EQ(Status::OK(), s3_fs->connect());

        std::unique_ptr<ThreadPool> _pool;
        ThreadPoolBuilder("s3_upload_file_thread_pool")
                .set_min_threads(5)
                .set_max_threads(10)
                .build(&_pool);
        ExecEnv::GetInstance()->_s3_file_writer_upload_thread_pool = std::move(_pool);
        doris::io::S3FileBufferPool* s3_buffer_pool = doris::io::S3FileBufferPool::GetInstance();
        s3_buffer_pool->init(doris::config::s3_write_buffer_whole_size,
                             doris::config::s3_write_buffer_size,
                             ExecEnv::GetInstance()->_s3_file_writer_upload_thread_pool.get());
    }

    static void TearDownTestSuite() {}
};

TEST_F(S3FileWriterTest, multi_part_io_error) {
    doris::io::FileWriterOptions state;
    auto fs = io::global_local_filesystem();
    {
        auto sp = SyncPoint::get_instance();
        Defer defer {[&]() { sp->clear_all_call_backs(); }};
        int largerThan5MB = 0;
        sp->set_call_back("S3FileWriter::_upload_one_part", [&largerThan5MB](auto&& outcome) {
            // Deliberately make one upload one part task fail to test if s3 file writer could
            // handle io error
            if (largerThan5MB > 0) {
                LOG(INFO) << "set upload one part to error";
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
                auto ptr = try_any_cast<
                        Aws::Utils::Outcome<Aws::S3::Model::UploadPartResult, Aws::S3::S3Error>*>(
                        outcome.back());
                *ptr = Aws::Utils::Outcome<Aws::S3::Model::UploadPartResult, Aws::S3::S3Error>(
                        Aws::Client::AWSError<Aws::S3::S3Errors>());
            }
            largerThan5MB++;
        });
        auto client = s3_fs->get_client();
        io::FileReaderSPtr local_file_reader;

        ASSERT_TRUE(
                fs->open_file("./be/test/olap/test_data/all_types_100000.txt", &local_file_reader)
                        .ok());

        constexpr int buf_size = 8192;

        io::FileWriterPtr s3_file_writer;
        ASSERT_EQ(Status::OK(), s3_fs->create_file("multi_part_io_error", &s3_file_writer, &state));

        char buf[buf_size];
        doris::Slice slice(buf, buf_size);
        size_t offset = 0;
        size_t bytes_read = 0;
        auto file_size = local_file_reader->size();
        while (offset < file_size) {
            ASSERT_TRUE(local_file_reader->read_at(offset, slice, &bytes_read).ok());
            ASSERT_EQ(Status::OK(), s3_file_writer->append(Slice(buf, bytes_read)));
            offset += bytes_read;
        }
        ASSERT_EQ(s3_file_writer->bytes_appended(), file_size);
        ASSERT_TRUE(s3_file_writer->finalize().ok());
        // The second part would fail uploading itself to s3
        // so the result of close should be not ok
        ASSERT_TRUE(!s3_file_writer->close().ok());
        bool exits = false;
        auto s = s3_fs->exists("multi_part_io_error", &exits);
        LOG(INFO) << "status is " << s;
        ASSERT_TRUE(!exits);
    }
}

TEST_F(S3FileWriterTest, put_object_io_error) {
    doris::io::FileWriterOptions state;
    auto fs = io::global_local_filesystem();
    {
        auto sp = SyncPoint::get_instance();
        Defer defer {[&]() { sp->clear_all_call_backs(); }};
        sp->set_call_back("S3FileWriter::_put_object", [](auto&& outcome) {
            // Deliberately make put object task fail to test if s3 file writer could
            // handle io error
            LOG(INFO) << "set put object to error";
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            io::S3FileWriter* writer = try_any_cast<io::S3FileWriter*>(outcome.at(0));
            io::UploadFileBuffer* buf = try_any_cast<io::UploadFileBuffer*>(outcome.at(1));
            writer->_st = Status::IOError(
                    "failed to put object (bucket={}, key={}, upload_id={}, exception=inject "
                    "error): "
                    "inject error",
                    writer->_bucket, writer->_path.native(), writer->_upload_id);
            buf->set_val(writer->_st);
            bool* pred = try_any_cast<bool*>(outcome.back());
            *pred = true;
        });
        auto client = s3_fs->get_client();
        io::FileReaderSPtr local_file_reader;

        ASSERT_TRUE(
                fs->open_file("./be/test/olap/test_data/all_types_100000.txt", &local_file_reader)
                        .ok());

        constexpr int buf_size = 8192;

        io::FileWriterPtr s3_file_writer;
        ASSERT_EQ(Status::OK(), s3_fs->create_file("put_object_io_error", &s3_file_writer, &state));

        char buf[buf_size];
        Slice slice(buf, buf_size);
        size_t offset = 0;
        size_t bytes_read = 0;
        // Only upload 4MB to trigger put object operation
        auto file_size = 4 * 1024 * 1024;
        while (offset < file_size) {
            ASSERT_TRUE(local_file_reader->read_at(offset, slice, &bytes_read).ok());
            ASSERT_EQ(Status::OK(), s3_file_writer->append(Slice(buf, bytes_read)));
            offset += bytes_read;
        }
        ASSERT_EQ(s3_file_writer->bytes_appended(), file_size);
        ASSERT_TRUE(s3_file_writer->finalize().ok());
        // The object might be timeout but still succeed in loading
        ASSERT_TRUE(!s3_file_writer->close().ok());
    }
}

TEST_F(S3FileWriterTest, appendv_random_quit) {
    doris::io::FileWriterOptions state;
    auto fs = io::global_local_filesystem();
    {
        io::FileReaderSPtr local_file_reader;

        ASSERT_TRUE(
                fs->open_file("./be/test/olap/test_data/all_types_100000.txt", &local_file_reader)
                        .ok());

        constexpr int buf_size = 8192;
        size_t quit_time = rand() % local_file_reader->size();
        auto sp = SyncPoint::get_instance();
        sp->set_call_back("s3_file_writer::appenv", [&quit_time](auto&& st) {
            if (quit_time == 0) {
                std::pair<Status, bool>* pair = try_any_cast<std::pair<Status, bool>*>(st.back());
                pair->second = true;
                pair->first = Status::InternalError("error");
                return;
            }
            quit_time--;
        });
        Defer defer {[&]() { sp->clear_all_call_backs(); }};

        io::FileWriterPtr s3_file_writer;
        ASSERT_EQ(Status::OK(), s3_fs->create_file("appendv_random_quit", &s3_file_writer, &state));

        char buf[buf_size];
        Slice slice(buf, buf_size);
        size_t offset = 0;
        size_t bytes_read = 0;
        auto file_size = local_file_reader->size();
        while (offset < file_size) {
            ASSERT_TRUE(local_file_reader->read_at(offset, slice, &bytes_read).ok());
            auto st = s3_file_writer->append(Slice(buf, bytes_read));
            if (quit_time == 0) {
                ASSERT_TRUE(!st.ok());
            } else {
                ASSERT_EQ(Status::OK(), st);
            }
            offset += bytes_read;
        }
        bool exits = false;
        s3_fs->exists("appendv_random_quit", &exits);
        ASSERT_TRUE(!exits);
    }
}

TEST_F(S3FileWriterTest, multi_part_open_error) {
    doris::io::FileWriterOptions state;
    auto fs = io::global_local_filesystem();
    {
        io::FileReaderSPtr local_file_reader;

        ASSERT_TRUE(
                fs->open_file("./be/test/olap/test_data/all_types_100000.txt", &local_file_reader)
                        .ok());

        constexpr int buf_size = 5 * 1024 * 1024;
        auto sp = SyncPoint::get_instance();
        sp->set_call_back("s3_file_writer::_open", [](auto&& outcome) {
            auto open_outcome =
                    try_any_cast<Aws::S3::Model::CreateMultipartUploadOutcome*>(outcome.back());
            *open_outcome = Aws::Utils::Outcome<Aws::S3::Model::CreateMultipartUploadResult,
                                                Aws::S3::S3Error>(
                    Aws::Client::AWSError<Aws::S3::S3Errors>());
        });
        Defer defer {[&]() { sp->clear_all_call_backs(); }};

        io::FileWriterPtr s3_file_writer;
        ASSERT_EQ(Status::OK(),
                  s3_fs->create_file("multi_part_open_error", &s3_file_writer, &state));

        auto buf = std::make_unique<char[]>(buf_size);
        Slice slice(buf.get(), buf_size);
        size_t offset = 0;
        size_t bytes_read = 0;
        ASSERT_TRUE(local_file_reader->read_at(offset, slice, &bytes_read).ok());
        // Directly write 5MB would cause one create multi part upload request
        // and it would be rejectd one error
        auto st = s3_file_writer->append(Slice(buf.get(), bytes_read));
        ASSERT_TRUE(!st.ok());
        bool exits = false;
        s3_fs->exists("multi_part_open_error", &exits);
        ASSERT_TRUE(!exits);
    }
}

TEST_F(S3FileWriterTest, write_into_cache_io_error) {
    std::filesystem::path caches_dir =
            std::filesystem::current_path() / "s3_file_writer_cache_test";
    std::string cache_base_path = caches_dir / "cache1" / "";
    Defer fs_clear {[&]() {
        if (std::filesystem::exists(cache_base_path)) {
            std::filesystem::remove_all(cache_base_path);
        }
    }};
    io::FileCacheSettings settings;
    settings.query_queue_size = 10 * 1024 * 1024;
    settings.query_queue_elements = 100;
    settings.total_size = 10 * 1024 * 1024;
    settings.max_file_block_size = 1 * 1024 * 1024;
    settings.max_query_cache_size = 30;
    io::FileCacheFactory::instance()._caches.clear();
    io::FileCacheFactory::instance()._path_to_cache.clear();
    io::FileCacheFactory::instance()._total_cache_size = 0;
    auto cache = std::make_unique<io::BlockFileCache>(cache_base_path, settings);
    ASSERT_TRUE(cache->initialize());
    while (true) {
        if (cache->get_lazy_open_success()) {
            break;
        };
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    io::FileCacheFactory::instance()._caches.emplace_back(std::move(cache));
    doris::io::FileWriterOptions state;
    auto fs = io::global_local_filesystem();
    {
        io::FileReaderSPtr local_file_reader;

        ASSERT_TRUE(
                fs->open_file("./be/test/olap/test_data/all_types_100000.txt", &local_file_reader)
                        .ok());

        constexpr int buf_size = 8192;
        auto sp = SyncPoint::get_instance();
        config::enable_file_cache = true;
        // Make append to cache return one error to test if it would exit
        sp->set_call_back("file_block::append", [](auto&& values) {
            LOG(INFO) << "file segment append";
            std::pair<Status, bool>* pairs = try_any_cast<std::pair<Status, bool>*>(values.back());
            pairs->second = true;
            pairs->first = Status::IOError("failed to append to cache file segments");
        });
        sp->set_call_back("S3FileWriter::_complete:3", [](auto&& values) {
            LOG(INFO) << "don't send s3 complete request";
            std::pair<Status, bool>* pairs = try_any_cast<std::pair<Status, bool>*>(values.back());
            pairs->second = true;
        });
        sp->set_call_back("UploadFileBuffer::upload_to_local_file_cache", [](auto&& values) {
            LOG(INFO) << "Check if upload failed due to injected error";
            bool ret = *try_any_cast<bool*>(values.back());
            ASSERT_FALSE(ret);
        });
        Defer defer {[&]() {
            sp->clear_all_call_backs();
            config::enable_file_cache = false;
        }};

        io::FileWriterPtr s3_file_writer;
        ASSERT_EQ(Status::OK(),
                  s3_fs->create_file("write_into_cache_io_error", &s3_file_writer, &state));

        char buf[buf_size];
        Slice slice(buf, buf_size);
        size_t offset = 0;
        size_t bytes_read = 0;
        auto file_size = local_file_reader->size();
        LOG(INFO) << "file size is " << file_size;
        while (offset < file_size) {
            ASSERT_TRUE(local_file_reader->read_at(offset, slice, &bytes_read).ok());
            ASSERT_EQ(Status::OK(), s3_file_writer->append(Slice(buf, bytes_read)));
            offset += bytes_read;
        }
        bool exits = false;
        s3_fs->exists("write_into_cache_io_error", &exits);
        ASSERT_TRUE(!exits);
    }
}

TEST_F(S3FileWriterTest, normal) {
    doris::io::FileWriterOptions state;
    auto fs = io::global_local_filesystem();
    {
        io::FileReaderSPtr local_file_reader;

        ASSERT_TRUE(
                fs->open_file("./be/test/olap/test_data/all_types_100000.txt", &local_file_reader)
                        .ok());

        constexpr int buf_size = 8192;

        io::FileWriterPtr s3_file_writer;
        ASSERT_EQ(Status::OK(), s3_fs->create_file("normal", &s3_file_writer, &state));

        char buf[buf_size];
        Slice slice(buf, buf_size);
        size_t offset = 0;
        size_t bytes_read = 0;
        auto file_size = local_file_reader->size();
        while (offset < file_size) {
            ASSERT_TRUE(local_file_reader->read_at(offset, slice, &bytes_read).ok());
            ASSERT_EQ(Status::OK(), s3_file_writer->append(Slice(buf, bytes_read)));
            offset += bytes_read;
        }
        ASSERT_EQ(s3_file_writer->bytes_appended(), file_size);
        ASSERT_TRUE(s3_file_writer->finalize().ok());
        ASSERT_EQ(Status::OK(), s3_file_writer->close());
        int64_t s3_file_size = 0;
        ASSERT_EQ(Status::OK(), s3_fs->file_size("normal", &s3_file_size));
        ASSERT_EQ(s3_file_size, file_size);
    }
}

TEST_F(S3FileWriterTest, smallFile) {
    doris::io::FileWriterOptions state;
    auto fs = io::global_local_filesystem();
    {
        io::FileReaderSPtr local_file_reader;

        ASSERT_TRUE(fs->open_file("./be/test/olap/test_data/all_types_1000.txt", &local_file_reader)
                            .ok());

        constexpr int buf_size = 8192;

        io::FileWriterPtr s3_file_writer;
        ASSERT_EQ(Status::OK(), s3_fs->create_file("small", &s3_file_writer, &state));

        char buf[buf_size];
        Slice slice(buf, buf_size);
        size_t offset = 0;
        size_t bytes_read = 0;
        auto file_size = local_file_reader->size();
        while (offset < file_size) {
            ASSERT_TRUE(local_file_reader->read_at(offset, slice, &bytes_read).ok());
            ASSERT_EQ(Status::OK(), s3_file_writer->append(Slice(buf, bytes_read)));
            offset += bytes_read;
        }
        ASSERT_EQ(s3_file_writer->bytes_appended(), file_size);
        ASSERT_TRUE(s3_file_writer->finalize().ok());
        ASSERT_EQ(Status::OK(), s3_file_writer->close());
        int64_t s3_file_size = 0;
        ASSERT_EQ(Status::OK(), s3_fs->file_size("small", &s3_file_size));
        ASSERT_EQ(s3_file_size, file_size);
    }
}

TEST_F(S3FileWriterTest, close_error) {
    doris::io::FileWriterOptions state;
    auto fs = io::global_local_filesystem();
    {
        io::FileReaderSPtr local_file_reader;

        ASSERT_TRUE(fs->open_file("./be/test/olap/test_data/all_types_1000.txt", &local_file_reader)
                            .ok());

        auto sp = SyncPoint::get_instance();
        sp->set_call_back("s3_file_writer::close", [](auto&& values) {
            std::pair<Status, bool>* pairs = try_any_cast<std::pair<Status, bool>*>(values.back());
            pairs->second = true;
            pairs->first = Status::InternalError("failed to close s3 file writer");
            LOG(INFO) << "return error when closing s3 file writer";
        });
        io::FileWriterPtr s3_file_writer;
        ASSERT_TRUE(s3_fs->create_file("close_error", &s3_file_writer, &state).ok());
        Defer defer {[&]() { sp->clear_all_call_backs(); }};
        ASSERT_TRUE(!s3_file_writer->close().ok());
        bool exits = false;
        s3_fs->exists("close_error", &exits);
        ASSERT_TRUE(!exits);
    }
}

TEST_F(S3FileWriterTest, finalize_error) {
    doris::io::FileWriterOptions state;
    auto fs = io::global_local_filesystem();
    {
        io::FileReaderSPtr local_file_reader;

        ASSERT_TRUE(fs->open_file("./be/test/olap/test_data/all_types_1000.txt", &local_file_reader)
                            .ok());

        io::FileWriterPtr s3_file_writer;
        ASSERT_EQ(Status::OK(), s3_fs->create_file("finalize_error", &s3_file_writer, &state));

        auto sp = SyncPoint::get_instance();
        sp->set_call_back("s3_file_writer::finalize", [](auto&& values) {
            std::pair<Status, bool>* pairs = try_any_cast<std::pair<Status, bool>*>(values.back());
            pairs->second = true;
            pairs->first = Status::InternalError("failed to finalize s3 file writer");
            LOG(INFO) << "return error when finalizing s3 file writer";
        });
        Defer defer {[&]() { sp->clear_all_call_backs(); }};

        constexpr int buf_size = 8192;

        char buf[buf_size];
        Slice slice(buf, buf_size);
        size_t offset = 0;
        size_t bytes_read = 0;
        auto file_size = local_file_reader->size();
        while (offset < file_size) {
            ASSERT_TRUE(local_file_reader->read_at(offset, slice, &bytes_read).ok());
            ASSERT_EQ(Status::OK(), s3_file_writer->append(Slice(buf, bytes_read)));
            offset += bytes_read;
        }
        ASSERT_EQ(s3_file_writer->bytes_appended(), file_size);
        ASSERT_TRUE(!s3_file_writer->finalize().ok());
        bool exits = false;
        s3_fs->exists("finalize_error", &exits);
        ASSERT_TRUE(!exits);
    }
}

TEST_F(S3FileWriterTest, multi_part_complete_error_2) {
    doris::io::FileWriterOptions state;
    auto fs = io::global_local_filesystem();
    {
        auto sp = SyncPoint::get_instance();
        Defer defer {[&]() { sp->clear_all_call_backs(); }};
        sp->set_call_back("S3FileWriter::_complete:2", [](auto&& outcome) {
            // Deliberately make one upload one part task fail to test if s3 file writer could
            // handle io error
            std::vector<std::unique_ptr<Aws::S3::Model::CompletedPart>>* parts =
                    try_any_cast<std::vector<std::unique_ptr<Aws::S3::Model::CompletedPart>>*>(
                            outcome.back());
            size_t size = parts->size();
            parts->back()->SetPartNumber(size + 2);
        });
        auto client = s3_fs->get_client();
        io::FileReaderSPtr local_file_reader;

        ASSERT_TRUE(
                fs->open_file("./be/test/olap/test_data/all_types_100000.txt", &local_file_reader)
                        .ok());

        constexpr int buf_size = 8192;

        io::FileWriterPtr s3_file_writer;
        ASSERT_EQ(Status::OK(), s3_fs->create_file("multi_part_io_error", &s3_file_writer, &state));

        char buf[buf_size];
        Slice slice(buf, buf_size);
        size_t offset = 0;
        size_t bytes_read = 0;
        auto file_size = local_file_reader->size();
        while (offset < file_size) {
            ASSERT_TRUE(local_file_reader->read_at(offset, slice, &bytes_read).ok());
            ASSERT_EQ(Status::OK(), s3_file_writer->append(Slice(buf, bytes_read)));
            offset += bytes_read;
        }
        ASSERT_EQ(s3_file_writer->bytes_appended(), file_size);
        ASSERT_TRUE(s3_file_writer->finalize().ok());
        // The second part would fail uploading itself to s3
        // so the result of close should be not ok
        auto st = s3_file_writer->close();
        ASSERT_TRUE(!st.ok());
        std::cout << st << std::endl;
    }
}

TEST_F(S3FileWriterTest, multi_part_complete_error_1) {
    doris::io::FileWriterOptions state;
    auto fs = io::global_local_filesystem();
    {
        auto sp = SyncPoint::get_instance();
        Defer defer {[&]() { sp->clear_all_call_backs(); }};
        sp->set_call_back("S3FileWriter::_complete:1", [](auto&& outcome) {
            // Deliberately make one upload one part task fail to test if s3 file writer could
            // handle io error
            const std::pair<std::atomic_bool*,
                            std::vector<std::unique_ptr<Aws::S3::Model::CompletedPart>>*>& points =
                    try_any_cast<const std::pair<
                            std::atomic_bool*,
                            std::vector<std::unique_ptr<Aws::S3::Model::CompletedPart>>*>&>(
                            outcome.back());
            (*points.first) = false;
            points.second->pop_back();
        });
        auto client = s3_fs->get_client();
        io::FileReaderSPtr local_file_reader;

        ASSERT_TRUE(
                fs->open_file("./be/test/olap/test_data/all_types_100000.txt", &local_file_reader)
                        .ok());

        constexpr int buf_size = 8192;

        io::FileWriterPtr s3_file_writer;
        ASSERT_EQ(Status::OK(), s3_fs->create_file("multi_part_io_error", &s3_file_writer, &state));

        char buf[buf_size];
        Slice slice(buf, buf_size);
        size_t offset = 0;
        size_t bytes_read = 0;
        auto file_size = local_file_reader->size();
        while (offset < file_size) {
            ASSERT_TRUE(local_file_reader->read_at(offset, slice, &bytes_read).ok());
            ASSERT_EQ(Status::OK(), s3_file_writer->append(Slice(buf, bytes_read)));
            offset += bytes_read;
        }
        ASSERT_EQ(s3_file_writer->bytes_appended(), file_size);
        ASSERT_TRUE(s3_file_writer->finalize().ok());
        // The second part would fail uploading itself to s3
        // so the result of close should be not ok
        auto st = s3_file_writer->close();
        ASSERT_TRUE(!st.ok());
        std::cout << st << std::endl;
    }
}

TEST_F(S3FileWriterTest, multi_part_complete_error_3) {
    doris::io::FileWriterOptions state;
    auto fs = io::global_local_filesystem();
    {
        auto sp = SyncPoint::get_instance();
        Defer defer {[&]() { sp->clear_all_call_backs(); }};
        sp->set_call_back("S3FileWriter::_complete:3", [](auto&& outcome) {
            std::pair<Status, bool>* pair = try_any_cast<std::pair<Status, bool>*>(outcome.back());
            io::S3FileWriter* writer = try_any_cast<io::S3FileWriter*>(outcome.at(0));
            pair->second = true;
            pair->first = Status::IOError("inject error");
            writer->_st = Status::IOError(
                    "failed to complete multi part upload (bucket={}, key={}, upload_id={}, "
                    "exception=inject_error): inject_error",
                    writer->_bucket, writer->_path.native(), writer->_upload_id);
        });
        auto client = s3_fs->get_client();
        io::FileReaderSPtr local_file_reader;

        ASSERT_TRUE(
                fs->open_file("./be/test/olap/test_data/all_types_100000.txt", &local_file_reader)
                        .ok());

        constexpr int buf_size = 8192;

        io::FileWriterPtr s3_file_writer;
        ASSERT_EQ(Status::OK(), s3_fs->create_file("multi_part_io_error", &s3_file_writer, &state));

        char buf[buf_size];
        Slice slice(buf, buf_size);
        size_t offset = 0;
        size_t bytes_read = 0;
        auto file_size = local_file_reader->size();
        while (offset < file_size) {
            ASSERT_TRUE(local_file_reader->read_at(offset, slice, &bytes_read).ok());
            ASSERT_EQ(Status::OK(), s3_file_writer->append(Slice(buf, bytes_read)));
            offset += bytes_read;
        }
        ASSERT_EQ(s3_file_writer->bytes_appended(), file_size);
        ASSERT_TRUE(s3_file_writer->finalize().ok());
        // The second part would fail uploading itself to s3
        // so the result of close should be not ok
        auto st = s3_file_writer->close();
        ASSERT_TRUE(!st.ok());
        std::cout << st << std::endl;
    }
}

} // namespace doris