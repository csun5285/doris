#include "cloud/io/s3_file_system.h"

#include <gtest/gtest.h>

#include "cloud/io/tmp_file_mgr.h"
#include "cloud/io/file_reader.h"
#include "cloud/io/local_file_system.h"
#include "util/s3_util.h"

namespace doris {
std::shared_ptr<io::S3FileSystem> s3_fs = nullptr;

class S3FileSystemTest : public testing::Test {
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
        s3_fs = io::S3FileSystem::create(std::move(s3_conf), "s3_file_system_test");
        ASSERT_EQ(Status::OK(), s3_fs->connect());
    }

    static void TearDownTestSuite() { s3_fs.reset(); }
};

TEST_F(S3FileSystemTest, upload) {
    auto cur_path = std::filesystem::current_path();
    // upload non-existent file
    auto st = s3_fs->upload(cur_path / "be/test/io/test_data/s3_file_system_test/no_such_file",
                            "no_such_file");
    ASSERT_FALSE(st.ok());
    // upload normal file
    auto local_fs = io::global_local_filesystem();
    io::FileWriterPtr local_file;
    st = local_fs->create_file(cur_path / "be/test/io/test_data/s3_file_system_test/normal",
                               &local_file);
    ASSERT_TRUE(st.ok());
    std::string_view content = "some bytes";
    st = local_file->append({content.data(), content.size()});
    ASSERT_TRUE(st.ok());
    st = local_file->close(false);
    EXPECT_TRUE(st.ok());
    st = s3_fs->upload(local_file->path(), "normal");
    ASSERT_TRUE(st.ok());
    io::FileReaderSPtr s3_file;
    st = s3_fs->open_file("normal", &s3_file);
    ASSERT_TRUE(st.ok());
    char buf[100];
    size_t bytes_read = 0;
    st = s3_file->read_at(0, {buf, sizeof(buf)}, &bytes_read);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(bytes_read, content.size());
    std::string_view content1(buf, bytes_read);
    ASSERT_EQ(content, content1);
}

} // namespace doris
