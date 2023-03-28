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

#include <gtest/gtest.h>

#include "common/config.h"
#include "common/status.h"
#include "cloud/io/tmp_file_mgr.h"
#include "cloud/io/file_reader.h"
#include "cloud/io/file_writer.h"
#include "cloud/io/local_file_system.h"
#include "cloud/io/s3_file_system.h"
#include "util/s3_util.h"
namespace doris {

static std::shared_ptr<io::S3FileSystem> s3_fs{nullptr};

class S3FileWriterTest : public testing::Test {
public:
    static void SetUpTestSuite() {
        std::string cur_path = std::filesystem::current_path();
        config::tmp_file_dirs = R"([{"path":")" + cur_path +
                R"(/ut_dir/s3_file_writer_test","max_cache_bytes":0,"max_upload_bytes":10737418240}])";
        io::TmpFileMgr::create_tmp_file_mgrs();
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
        ThreadPoolBuilder("BufferedReaderPrefetchThreadPool")
                .set_min_threads(5)
                .set_max_threads(10)
                .build(&_pool);
        ExecEnv::GetInstance()->_buffered_reader_prefetch_thread_pool = std::move(_pool);
    }

    static void TearDownTestSuite() { }
};

TEST_F(S3FileWriterTest, normal) {
    io::IOState state;
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
        size_t s3_file_size = 0;
        ASSERT_EQ(Status::OK(), s3_fs->file_size("normal", &s3_file_size));
        ASSERT_EQ(s3_file_size, file_size);
    }
    // tmp file in s3_file_writer should be deleted
    bool exists = true;
    fs->exists(
            io::Path(io::TmpFileMgr::instance()->get_tmp_file_dir()) / "s3_file_writer_test_normal",
            &exists);
    ASSERT_FALSE(exists);
}

TEST_F(S3FileWriterTest, abort) {
    io::IOState state;
    auto fs = io::global_local_filesystem();
    {
        io::FileWriterPtr s3_file_writer;
        ASSERT_EQ(Status::OK(), s3_fs->create_file("abort1", &s3_file_writer, &state));
        s3_file_writer->abort();
    }
    bool exists = true;
    fs->exists(
            io::Path(io::TmpFileMgr::instance()->get_tmp_file_dir()) / "s3_file_writer_test_abort1",
            &exists);
    ASSERT_FALSE(exists);
    {
        io::FileWriterPtr s3_file_writer;
        ASSERT_EQ(Status::OK(), s3_fs->create_file("abort2", &s3_file_writer, &state));
        ASSERT_TRUE(s3_file_writer->finalize().ok());
        s3_file_writer->abort();
    }
    exists = true;
    fs->exists(
            io::Path(io::TmpFileMgr::instance()->get_tmp_file_dir()) / "s3_file_writer_test_abort2",
            &exists);
    ASSERT_FALSE(exists);
    // Sleep to ensure S3FileWriter::_handle async Cancel finished before _executor released.
    sleep(1);
}

} // namespace doris
