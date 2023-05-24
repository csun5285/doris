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

#include "cloud/io/s3_downloader.h"

#include <gtest/gtest.h>

#include <future>
#include <string_view>

#include "cloud/io/cloud_file_segment.h"
#include "cloud/io/local_file_system.h"
#include "cloud/io/s3_file_system.h"
#include "cloud/io/tmp_file_mgr.h"
#include "runtime/exec_env.h"
#include "util/s3_util.h"
#include "util/slice.h"

namespace doris {
std::shared_ptr<io::S3FileSystem> downloader_s3_fs = nullptr;
using FileSegmentsHolderPtr = std::unique_ptr<io::FileSegmentsHolder>;
namespace io {
extern void download_file(
        std::shared_ptr<S3Client> client, std::string key_name, size_t offset, size_t size,
        std::string bucket,
        std::function<FileSegmentsHolderPtr(size_t, size_t)> alloc_holder = nullptr,
        std::function<void(Status)> download_callback = nullptr, Slice s = Slice());
}

class S3DownloaderTest : public testing::Test {
public:
    static void SetUpTestSuite() {
        auto cur_path = std::filesystem::current_path();
        config::tmp_file_dirs =
                R"([{"path":")" + cur_path.native() +
                R"(/ut_dir/s3_file_system_test/cache","max_cache_bytes":21474836480,"max_upload_bytes":10737418240}])";
        io::TmpFileMgr::create_tmp_file_mgrs();
        S3Conf s3_conf;
        s3_conf.ak = config::test_s3_ak;
        s3_conf.sk = config::test_s3_sk;
        s3_conf.endpoint = config::test_s3_endpoint;
        s3_conf.region = config::test_s3_region;
        s3_conf.bucket = config::test_s3_bucket;
        s3_conf.prefix = "s3_file_system_test";
        downloader_s3_fs =
                std::make_shared<io::S3FileSystem>(std::move(s3_conf), "s3_file_system_test");
        ASSERT_EQ(Status::OK(), downloader_s3_fs->connect());

        std::unique_ptr<ThreadPool> _pool;
        ThreadPoolBuilder("BufferedReaderPrefetchThreadPool")
                .set_min_threads(5)
                .set_max_threads(10)
                .build(&_pool);
        ExecEnv::GetInstance()->_buffered_reader_prefetch_thread_pool = std::move(_pool);
    }

    static void TearDownTestSuite() {}
};

TEST_F(S3DownloaderTest, normal) {
    auto cur_path = std::filesystem::current_path();
    // upload normal file
    auto local_fs = io::global_local_filesystem();
    io::FileWriterPtr local_file;
    auto st = local_fs->create_file(cur_path / "be/test/io/test_data/s3_downloader_test/normal",
                                    &local_file);
    ASSERT_TRUE(st.ok());
    std::string_view content = "some bytes, damn it";
    st = local_file->append({content.data(), content.size()});
    ASSERT_TRUE(st.ok());
    st = local_file->close(false);
    EXPECT_TRUE(st.ok());
    st = downloader_s3_fs->upload(local_file->path(), "normal");
    ASSERT_TRUE(st.ok());
    char buf[100];
    std::promise<Status> pro;
    io::download_file(
            downloader_s3_fs->get_client(), downloader_s3_fs->get_key("normal"), 0, content.size(),
            downloader_s3_fs->s3_conf().bucket, nullptr,
            [&](Status s) { pro.set_value(std::move(s)); }, Slice {buf, 100});
    auto f = pro.get_future();
    ASSERT_TRUE(f.get());
    std::string_view result {buf, content.size()};
    ASSERT_EQ(result, content);
}

TEST_F(S3DownloaderTest, multipart) {
    auto cur_path = std::filesystem::current_path();
    // upload normal file
    auto local_fs = io::global_local_filesystem();
    io::FileWriterPtr local_file;
    auto st = local_fs->create_file(cur_path / "be/test/io/test_data/s3_downloader_test/normal",
                                    &local_file);
    ASSERT_TRUE(st.ok());
    std::string_view content = "some bytes, damn it";
    st = local_file->append({content.data(), content.size()});
    ASSERT_TRUE(st.ok());
    st = local_file->close(false);
    EXPECT_TRUE(st.ok());
    st = downloader_s3_fs->upload(local_file->path(), "normal");
    ASSERT_TRUE(st.ok());
    char buf[100];
    std::promise<Status> pro;
    io::download_file(
            downloader_s3_fs->get_client(), downloader_s3_fs->get_key("normal"), 0,
            content.size() / 2, downloader_s3_fs->s3_conf().bucket, nullptr,
            [&](Status s) { pro.set_value(std::move(s)); }, Slice {buf, 100});
    auto f = pro.get_future();
    ASSERT_TRUE(f.get());
    std::promise<Status> another_pro;
    io::download_file(
            downloader_s3_fs->get_client(), downloader_s3_fs->get_key("normal"), content.size() / 2,
            content.size() - (content.size() / 2), downloader_s3_fs->s3_conf().bucket, nullptr,
            [&](Status s) { another_pro.set_value(std::move(s)); }, Slice {buf, 100});
    auto another_f = pro.get_future();
    ASSERT_TRUE(another_f.get());
    std::string_view result {buf, content.size()};
    ASSERT_EQ(result, content);
}

} // namespace doris
