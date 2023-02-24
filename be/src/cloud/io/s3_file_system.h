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

#pragma once

#include <mutex>

#include "cloud/io/file_reader.h"
#include "cloud/io/remote_file_system.h"
#include "util/s3_util.h"

namespace Aws::S3 {
class S3Client;
} // namespace Aws::S3
namespace Aws::Utils::Threading {
class PooledThreadExecutor;
} // namespace Aws::Utils::Threading
namespace Aws::Transfer {
class TransferManager;
} // namespace Aws::Transfer

namespace doris {
namespace io {

// This class is thread-safe.(Except `set_xxx` method)
class S3FileSystem final : public RemoteFileSystem {
public:
    static std::shared_ptr<S3FileSystem> create(S3Conf s3_conf, ResourceId resource_id);

    ~S3FileSystem() override;

    Status create_file(const Path& path, FileWriterPtr* writer, IOState*) override;

    Status open_file(const Path& path, FileReaderSPtr* reader) override;

    Status open_file_impl(const Path& path, metrics_hook, FileReaderSPtr* reader, size_t file_size = 0);

    Status open_file(const Path& path, metrics_hook, FileReaderSPtr* reader,
                     size_t file_size = 0) override;

    Status delete_file(const Path& path) override;

    Status create_directory(const Path& path) override;

    // Delete all objects start with path.
    Status delete_directory(const Path& path) override;

    Status link_file(const Path& src, const Path& dest) override;

    Status exists(const Path& path, bool* res) const override;

    Status file_size(const Path& path, size_t* file_size) const override;

    Status file_size_impl(const Path& path, size_t* file_size) const;

    Status list(const Path& path, std::vector<Path>* files) override;

    Status upload(const Path& local_path, const Path& dest_path) override;

    Status batch_upload(const std::vector<Path>& local_paths,
                        const std::vector<Path>& dest_paths) override;

    Status connect() override;

    std::shared_ptr<Aws::S3::S3Client> get_client() const {
        std::lock_guard lock(_client_mu);
        return _client;
    };

    std::shared_ptr<Aws::Transfer::TransferManager> get_transfer_manager();

    // Guarded by external lock.
    void set_ak(const std::string& ak) { _s3_conf.ak = ak; }

    // Guarded by external lock.
    void set_sk(const std::string& sk) { _s3_conf.sk = sk; }

    const S3Conf& s3_conf() { return _s3_conf; }

    std::string generate_presigned_url(const Path& path, int64_t expiration_secs,
                                       bool is_public_endpoint) const;

    std::string get_key(const Path& path) const;

    // TODO(chengyuxuan): fix it, private
    S3FileSystem(S3Conf&& s3_conf, ResourceId&& resource_id);

private:
    S3Conf _s3_conf;

    // FIXME(cyx): We can use std::atomic<std::shared_ptr> since c++20.
    std::shared_ptr<Aws::S3::S3Client> _client;
    std::shared_ptr<Aws::Transfer::TransferManager> _transfer_manager;
    mutable std::mutex _client_mu;

    friend class S3FileWriter;
    std::shared_ptr<Aws::Utils::Threading::PooledThreadExecutor> _executor;
};

} // namespace io
} // namespace doris
