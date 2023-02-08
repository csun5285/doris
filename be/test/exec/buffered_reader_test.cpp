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

#include "io/buffered_reader.h"

#include <gtest/gtest.h>

#include <memory>

#include "io/local_file_reader.h"
#include "runtime/exec_env.h"
#include "util/stopwatch.hpp"

namespace doris {
class BufferedReaderTest : public testing::Test {
public:
    BufferedReaderTest() {
        std::unique_ptr<ThreadPool> _pool;
        ThreadPoolBuilder("BufferedReaderPrefetchThreadPool")
                .set_min_threads(5)
                .set_max_threads(10)
                .build(&_pool);
        ExecEnv::GetInstance()->_buffered_reader_prefetch_thread_pool = std::move(_pool);
    }

protected:
    virtual void SetUp() {}
    virtual void TearDown() {}
};

class SyncLocalFileReader : public FileReader {
public:
    SyncLocalFileReader(LocalFileReader* reader) : _reader(reader) {}
    ~SyncLocalFileReader() override = default;

    Status open() override { return _reader->open(); }

    // Read content to 'buf', 'buf_len' is the max size of this buffer.
    // Return ok when read success, and 'buf_len' is set to size of read content
    // If reach to end of file, the eof is set to true. meanwhile 'buf_len'
    // is set to zero.
    Status read(uint8_t* buf, int64_t buf_len, int64_t* bytes_read, bool* eof) override {
        std::unique_lock lck {_lock};
        return _reader->read(buf, buf_len, bytes_read, eof);
    }
    Status readat(int64_t position, int64_t nbytes, int64_t* bytes_read, void* out) override {
        std::unique_lock lck {_lock};
        return _reader->readat(position, nbytes, bytes_read, out);
    }
    Status read_one_message(std::unique_ptr<uint8_t[]>* buf, int64_t* length) override {
        std::unique_lock lck {_lock};
        return _reader->read_one_message(buf, length);
    }
    int64_t size() override {
        return _reader->size();
    }
    Status seek(int64_t position) override {
        return _reader->seek(position);
    }
    Status tell(int64_t* position) override {
        return _reader->tell(position);
    }
    void close() override {
        return _reader->close();
    }
    bool closed() override {
        return _reader->closed();
    }

private:
    std::unique_ptr<LocalFileReader> _reader;
    std::mutex _lock;
};

TEST_F(BufferedReaderTest, normal_use) {
    RuntimeProfile profile("test");
    // buffered_reader_test_file 950 bytes
    auto file_reader = new LocalFileReader(
            "./be/test/exec/test_data/buffered_reader/buffered_reader_test_file", 0);
    auto sync_local_reader = new SyncLocalFileReader(file_reader);
    config::prefetch_single_buffer_size_mb = 200;
    BufferedReader reader(&profile, sync_local_reader, 1024);
    auto st = reader.open();
    EXPECT_TRUE(st.ok());
    uint8_t buf[1024];
    MonotonicStopWatch watch;
    watch.start();
    int64_t read_length = 0;
    st = reader.readat(0, 1024, &read_length, buf);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(950, read_length);
    LOG(INFO) << "read bytes " << read_length << " using time " << watch.elapsed_time();
}

TEST_F(BufferedReaderTest, test_validity) {
    RuntimeProfile profile("test");
    // buffered_reader_test_file.txt 45 bytes
    auto file_reader = new LocalFileReader(
            "./be/test/exec/test_data/buffered_reader/buffered_reader_test_file.txt", 0);
    auto sync_local_reader = new SyncLocalFileReader(file_reader);
    config::prefetch_single_buffer_size_mb = 10;
    BufferedReader reader(&profile, sync_local_reader, 64);
    auto st = reader.open();
    EXPECT_TRUE(st.ok());
    uint8_t buf[10];
    bool eof = false;
    int64_t buf_len = 10;
    int64_t read_length = 0;

    st = reader.read(buf, buf_len, &read_length, &eof);
    EXPECT_TRUE(st.ok());
    EXPECT_STREQ("bdfhjlnprt", std::string((char*)buf, read_length).c_str());
    EXPECT_FALSE(eof);

    st = reader.read(buf, buf_len, &read_length, &eof);
    EXPECT_TRUE(st.ok());
    EXPECT_STREQ("vxzAbCdEfG", std::string((char*)buf, read_length).c_str());
    EXPECT_FALSE(eof);

    st = reader.read(buf, buf_len, &read_length, &eof);
    EXPECT_TRUE(st.ok());
    EXPECT_STREQ("hIj\n\nMnOpQ", std::string((char*)buf, read_length).c_str());
    EXPECT_FALSE(eof);

    st = reader.read(buf, buf_len, &read_length, &eof);
    EXPECT_TRUE(st.ok());
    EXPECT_STREQ("rStUvWxYz\n", std::string((char*)buf, read_length).c_str());
    EXPECT_FALSE(eof);

    st = reader.read(buf, buf_len, &read_length, &eof);
    EXPECT_TRUE(st.ok());
    EXPECT_STREQ("IjKl", std::string((char*)buf, 4).c_str());
    EXPECT_FALSE(eof);

    st = reader.read(buf, buf_len, &read_length, &eof);
    EXPECT_TRUE(st.ok());
    EXPECT_TRUE(eof);
}

TEST_F(BufferedReaderTest, test_seek) {
    RuntimeProfile profile("test");
    // buffered_reader_test_file.txt 45 bytes
    auto file_reader = new LocalFileReader(
            "./be/test/exec/test_data/buffered_reader/buffered_reader_test_file.txt", 0);
    auto sync_local_reader = new SyncLocalFileReader(file_reader);
    config::prefetch_single_buffer_size_mb = 10;
    BufferedReader reader(&profile, sync_local_reader, 64);
    auto st = reader.open();
    EXPECT_TRUE(st.ok());
    uint8_t buf[10];
    bool eof = false;
    size_t buf_len = 10;
    int64_t read_length = 0;

    // Seek to the end of the file
    st = reader.seek(45);
    EXPECT_TRUE(st.ok());
    st = reader.read(buf, buf_len, &read_length, &eof);
    EXPECT_TRUE(st.ok());
    EXPECT_TRUE(eof);

    // Seek to the beginning of the file
    st = reader.seek(0);
    EXPECT_TRUE(st.ok());
    st = reader.read(buf, buf_len, &read_length, &eof);
    EXPECT_TRUE(st.ok());
    EXPECT_STREQ("bdfhjlnprt", std::string((char*)buf, read_length).c_str());
    EXPECT_FALSE(eof);

    // Seek to a wrong position
    st = reader.seek(-1);
    EXPECT_TRUE(st.ok());
    st = reader.read(buf, buf_len, &read_length, &eof);
    EXPECT_TRUE(st.ok());
    EXPECT_STREQ("bdfhjlnprt", std::string((char*)buf, read_length).c_str());
    EXPECT_FALSE(eof);

    // Seek to a wrong position
    st = reader.seek(-1000);
    EXPECT_TRUE(st.ok());
    st = reader.read(buf, buf_len, &read_length, &eof);
    EXPECT_TRUE(st.ok());
    EXPECT_STREQ("bdfhjlnprt", std::string((char*)buf, read_length).c_str());
    EXPECT_FALSE(eof);

    // Seek to a wrong position
    st = reader.seek(1000);
    EXPECT_TRUE(st.ok());
    st = reader.read(buf, buf_len, &read_length, &eof);
    EXPECT_TRUE(st.ok());
    EXPECT_TRUE(eof);
}

TEST_F(BufferedReaderTest, test_miss) {
    RuntimeProfile profile("test");
    // buffered_reader_test_file.txt 45 bytes
    auto file_reader = new LocalFileReader(
            "./be/test/exec/test_data/buffered_reader/buffered_reader_test_file.txt", 0);
    auto sync_local_reader = new SyncLocalFileReader(file_reader);
    config::prefetch_single_buffer_size_mb = 10;
    BufferedReader reader(&profile, sync_local_reader, 64);
    auto st = reader.open();
    EXPECT_TRUE(st.ok());
    uint8_t buf[128];
    int64_t bytes_read;

    st = reader.readat(20, 10, &bytes_read, buf);
    EXPECT_TRUE(st.ok());
    EXPECT_STREQ("hIj\n\nMnOpQ", std::string((char*)buf, (size_t)bytes_read).c_str());
    EXPECT_EQ(10, bytes_read);

    st = reader.readat(0, 5, &bytes_read, buf);
    EXPECT_TRUE(st.ok());
    EXPECT_STREQ("bdfhj", std::string((char*)buf, (size_t)bytes_read).c_str());
    EXPECT_EQ(5, bytes_read);

    st = reader.readat(5, 10, &bytes_read, buf);
    EXPECT_TRUE(st.ok());
    EXPECT_STREQ("lnprtvxzAb", std::string((char*)buf, (size_t)bytes_read).c_str());
    EXPECT_EQ(10, bytes_read);

    // if requested length is larger than the capacity of buffer, do not
    // need to copy the character into local buffer.
    st = reader.readat(0, 128, &bytes_read, buf);
    EXPECT_TRUE(st.ok());
    EXPECT_STREQ("bdfhjlnprt", std::string((char*)buf, 10).c_str());
    EXPECT_EQ(45, bytes_read);
}

} // end namespace doris
