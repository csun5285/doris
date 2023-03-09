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

#include "cloud/io/s3_file_system.h"

#include <aws/core/utils/threading/Executor.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/CopyObjectRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/DeleteObjectsRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/transfer/TransferManager.h>

#include <filesystem>
#include <fstream>
#include <memory>

#include "cloud/io/cached_remote_file_reader.h"
#include "cloud/io/local_file_system.h"
#include "cloud/io/remote_file_system.h"
#include "cloud/io/s3_file_reader.h"
#include "cloud/io/s3_file_writer.h"
#include "common/config.h"
#include "common/status.h"
#include "gutil/strings/stringpiece.h"
#include "olap/olap_common.h"
#include "util/async_io.h"
#include "util/s3_util.h"
#include "util/string_util.h"

namespace doris {
namespace io {

#ifndef CHECK_S3_CLIENT
#define CHECK_S3_CLIENT(client)                               \
    if (!client) {                                            \
        return Status::InternalError("init s3 client error"); \
    }
#endif

const std::string OSS_PRIVATE_ENDPOINT_SUFFIX = "-internal.aliyuncs.com";

std::shared_ptr<S3FileSystem> S3FileSystem::create(S3Conf s3_conf, ResourceId resource_id) {
    return std::make_shared<S3FileSystem>(std::move(s3_conf), std::move(resource_id));
}

S3FileSystem::S3FileSystem(S3Conf&& s3_conf, ResourceId&& resource_id)
        : RemoteFileSystem(
                  fmt::format("{}/{}/{}", s3_conf.endpoint, s3_conf.bucket, s3_conf.prefix),
                  std::move(resource_id), FileSystemType::S3),
          _s3_conf(std::move(s3_conf)) {
    if (_s3_conf.prefix.size() > 0 && _s3_conf.prefix[0] == '/') {
        _s3_conf.prefix = _s3_conf.prefix.substr(1);
    }
    _executor = Aws::MakeShared<Aws::Utils::Threading::PooledThreadExecutor>(
            resource_id.c_str(), config::s3_transfer_executor_pool_size);
}

S3FileSystem::~S3FileSystem() = default;

Status S3FileSystem::connect() {
    auto client = ClientFactory::instance().create(_s3_conf);
    if (!client) {
        return Status::InternalError("failed to init s3 client with {}", _s3_conf.to_string());
    }
    std::shared_ptr<Aws::Transfer::TransferManager> transfer_manager;
    {
        std::lock_guard lock(_client_mu);
        _client.swap(client);
        _transfer_manager.swap(transfer_manager);
    }
    return Status::OK();
}

std::shared_ptr<Aws::Transfer::TransferManager> S3FileSystem::get_transfer_manager() {
    std::lock_guard lock(_client_mu);
    if (_transfer_manager == nullptr) {
        if (_client == nullptr) {
            return nullptr;
        }
        Aws::Transfer::TransferManagerConfiguration transfer_config(_executor.get());
        transfer_config.s3Client = _client;
        transfer_config.transferBufferMaxHeapSize = config::s3_transfer_buffer_size_mb << 20;
        transfer_config.transferStatusUpdatedCallback =
                [](const Aws::Transfer::TransferManager*,
                   const std::shared_ptr<const Aws::Transfer::TransferHandle>& handle) {
                    handle->Callback();
                };
        if (sse_enabled()) {
            transfer_config.putObjectTemplate.WithServerSideEncryption(Aws::S3::Model::ServerSideEncryption::AES256);
            transfer_config.createMultipartUploadTemplate.WithServerSideEncryption(Aws::S3::Model::ServerSideEncryption::AES256);
        }
        _transfer_manager = Aws::Transfer::TransferManager::Create(transfer_config);
    }
    return _transfer_manager;
}

void S3FileSystem::reset_transfer_manager() {
    std::shared_ptr<Aws::Transfer::TransferManager> transfer_manager;
    {
        std::lock_guard lock(_client_mu);
        _transfer_manager.swap(transfer_manager);
    }
}

Status S3FileSystem::upload(const Path& local_path, const Path& dest_path) {
    auto client = get_client();
    CHECK_S3_CLIENT(client);

    Aws::Transfer::TransferManagerConfiguration transfer_config(_executor.get());
    transfer_config.s3Client = client;
    auto transfer_manager = Aws::Transfer::TransferManager::Create(transfer_config);

    auto start = std::chrono::steady_clock::now();

    auto key = get_key(dest_path);
    auto handle = transfer_manager->UploadFile(local_path.native(), _s3_conf.bucket, key,
                                               "text/plain", Aws::Map<Aws::String, Aws::String>());
    handle->WaitUntilFinished();

    auto duration = std::chrono::duration<float>(std::chrono::steady_clock::now() - start);

    if (handle->GetStatus() != Aws::Transfer::TransferStatus::COMPLETED) {
        return Status::IOError("failed to upload(endpoint={}, bucket={}, key={}): {}",
                               _s3_conf.endpoint, _s3_conf.bucket, key,
                               handle->GetLastError().GetMessage());
    }

    auto file_size = std::filesystem::file_size(local_path);
    LOG(INFO) << "Upload " << local_path.native() << " to s3, endpoint=" << _s3_conf.endpoint
              << ", bucket=" << _s3_conf.bucket << ", key=" << key
              << ", duration=" << duration.count() << ", capacity=" << file_size
              << ", tp=" << (file_size) / duration.count();

    return Status::OK();
}

Status S3FileSystem::batch_upload(const std::vector<Path>& local_paths,
                                  const std::vector<Path>& dest_paths) {
    auto client = get_client();
    CHECK_S3_CLIENT(client);

    if (local_paths.size() != dest_paths.size()) {
        return Status::InvalidArgument("local_paths.size() != dest_paths.size()");
    }

    Aws::Transfer::TransferManagerConfiguration transfer_config(_executor.get());
    transfer_config.s3Client = client;
    auto transfer_manager = Aws::Transfer::TransferManager::Create(transfer_config);

    std::vector<std::shared_ptr<Aws::Transfer::TransferHandle>> handles;
    for (int i = 0; i < local_paths.size(); ++i) {
        auto key = get_key(dest_paths[i]);
        LOG(INFO) << "Start to upload " << local_paths[i].native()
                  << " to s3, endpoint=" << _s3_conf.endpoint << ", bucket=" << _s3_conf.bucket
                  << ", key=" << key;
        auto handle =
                transfer_manager->UploadFile(local_paths[i].native(), _s3_conf.bucket, key,
                                             "text/plain", Aws::Map<Aws::String, Aws::String>());
        handles.push_back(std::move(handle));
    }
    for (auto& handle : handles) {
        handle->WaitUntilFinished();
        if (handle->GetStatus() != Aws::Transfer::TransferStatus::COMPLETED) {
            // TODO(cyx): Maybe we can cancel remaining handles.
            return Status::IOError("failed to upload(endpoint={}, bucket={}, key={}): {}",
                                   _s3_conf.endpoint, _s3_conf.bucket, handle->GetKey(),
                                   handle->GetLastError().GetMessage());
        }
    }
    return Status::OK();
}

Status S3FileSystem::create_file(const Path& path, FileWriterPtr* writer, IOState* io_state) {
    auto key = get_key(path);
    auto fs_path = Path(_s3_conf.endpoint) / _s3_conf.bucket / key;
    *writer = std::make_unique<S3FileWriter>(
            std::move(fs_path), std::move(key), _s3_conf.bucket, _client,
            std::static_pointer_cast<S3FileSystem>(shared_from_this()), io_state, sse_enabled());
    return (*writer)->open();
}

Status S3FileSystem::open_file(const Path& path, FileReaderSPtr* reader) {
    if (bthread_self() == 0) {
        return open_file_impl(path, nullptr, reader);
    }
    Status s;
    auto task = [&] { s = open_file_impl(path, nullptr, reader); };
    AsyncIO::run_task(task, io::FileSystemType::S3);
    return s;
}

Status S3FileSystem::open_file_impl(const Path& path, metrics_hook metrics, FileReaderSPtr* reader,
                                    size_t fsize) {
    if (fsize == 0) [[unlikely]] {
        RETURN_IF_ERROR(file_size(path, &fsize));
    }
    auto key = get_key(path);
    auto fs_path = Path(_s3_conf.endpoint) / _s3_conf.bucket / key;
    *reader = std::make_shared<S3FileReader>(
            std::move(fs_path), fsize, std::move(key), _s3_conf.bucket,
            std::static_pointer_cast<S3FileSystem>(shared_from_this()));
    if (config::enable_file_cache) {
        if (config::enable_file_cache) {
        *reader = std::make_shared<CachedRemoteFileReader>(std::move(*reader), std::move(metrics));
    }
    }
    return Status::OK();
}

Status S3FileSystem::open_file(const Path& path, metrics_hook metrics, FileReaderSPtr* reader,
                               size_t file_size) {
    if (bthread_self() == 0) {
        return open_file_impl(path, metrics, reader, file_size);
    }
    Status s;
    auto task = [&] { s = open_file_impl(path, metrics, reader, file_size); };
    AsyncIO::run_task(task, io::FileSystemType::S3);
    return s;
}

Status S3FileSystem::delete_file(const Path& path) {
    auto client = get_client();
    CHECK_S3_CLIENT(client);

    Aws::S3::Model::DeleteObjectRequest request;
    auto key = get_key(path);
    request.WithBucket(_s3_conf.bucket).WithKey(key);

    auto outcome = client->DeleteObject(request);
    if (outcome.IsSuccess() ||
        outcome.GetError().GetResponseCode() == Aws::Http::HttpResponseCode::NOT_FOUND) {
        return Status::OK();
    }
    return Status::IOError("failed to delete object(endpoint={}, bucket={}, key={}): {}",
                           _s3_conf.endpoint, _s3_conf.bucket, key,
                           outcome.GetError().GetMessage());
}

Status S3FileSystem::create_directory(const Path& path) {
    return Status::OK();
}

Status S3FileSystem::delete_directory(const Path& path) {
    auto client = get_client();
    CHECK_S3_CLIENT(client);

    Aws::S3::Model::ListObjectsV2Request request;
    auto prefix = get_key(path);
    if (!prefix.empty() && prefix.back() != '/') {
        prefix.push_back('/');
    }
    request.WithBucket(_s3_conf.bucket).WithPrefix(prefix);

    Aws::S3::Model::DeleteObjectsRequest delete_request;
    delete_request.SetBucket(_s3_conf.bucket);
    bool is_trucated = false;
    do {
        auto outcome = client->ListObjectsV2(request);
        if (!outcome.IsSuccess()) {
            return Status::IOError("failed to list objects(endpoint={}, bucket={}, prefix={}): {}",
                                   _s3_conf.endpoint, _s3_conf.bucket, prefix,
                                   outcome.GetError().GetMessage());
        }
        const auto& result = outcome.GetResult();
        Aws::Vector<Aws::S3::Model::ObjectIdentifier> objects;
        objects.reserve(result.GetContents().size());
        for (const auto& obj : result.GetContents()) {
            objects.emplace_back().SetKey(obj.GetKey());
        }
        if (!objects.empty()) {
            Aws::S3::Model::Delete del;
            del.WithObjects(std::move(objects)).SetQuiet(true);
            delete_request.SetDelete(std::move(del));
            auto delete_outcome = client->DeleteObjects(delete_request);
            if (!delete_outcome.IsSuccess()) {
                return Status::IOError(
                        "failed to delete objects(endpoint={}, bucket={}, prefix={}): {}",
                        _s3_conf.endpoint, _s3_conf.bucket, prefix,
                        delete_outcome.GetError().GetMessage());
            }
            if (!delete_outcome.GetResult().GetErrors().empty()) {
                const auto& e = delete_outcome.GetResult().GetErrors().front();
                return Status::IOError("fail to delete object(endpoint={}, bucket={}, key={}): {}",
                                       _s3_conf.endpoint, _s3_conf.bucket, e.GetKey(),
                                       e.GetMessage());
            }
            VLOG_TRACE << "delete " << objects.size()
                       << " s3 objects, endpoint: " << _s3_conf.endpoint
                       << ", bucket: " << _s3_conf.bucket << ", prefix: " << _s3_conf.prefix;
        }
        is_trucated = result.GetIsTruncated();
        request.SetContinuationToken(result.GetNextContinuationToken());
    } while (is_trucated);
    return Status::OK();
}

Status S3FileSystem::link_file(const Path& src, const Path& dest) {
    return Status::NotSupported("not support");
}

Status S3FileSystem::exists(const Path& path, bool* res) const {
    auto client = get_client();
    CHECK_S3_CLIENT(client);

    Aws::S3::Model::HeadObjectRequest request;
    auto key = get_key(path);
    request.WithBucket(_s3_conf.bucket).WithKey(key);

    auto outcome = client->HeadObject(request);
    if (outcome.IsSuccess()) {
        *res = true;
    } else if (outcome.GetError().GetResponseCode() == Aws::Http::HttpResponseCode::NOT_FOUND) {
        *res = false;
    } else {
        return Status::IOError("failed to get object head(endpoint={}, bucket={}, key={}): {}",
                               _s3_conf.endpoint, _s3_conf.bucket, key,
                               outcome.GetError().GetMessage());
    }
    return Status::OK();
}

Status S3FileSystem::file_size(const Path& path, size_t* file_size) const {
    if (bthread_self() == 0) {
        return file_size_impl(path, file_size);
    }
    Status s;
    auto task = [&] { s = file_size_impl(path, file_size); };
    AsyncIO::run_task(task, io::FileSystemType::S3);
    return s;
}

Status S3FileSystem::file_size_impl(const Path& path, size_t* file_size) const {
    auto client = get_client();
    CHECK_S3_CLIENT(client);

    Aws::S3::Model::HeadObjectRequest request;
    auto key = get_key(path);
    request.WithBucket(_s3_conf.bucket).WithKey(key);

    auto outcome = client->HeadObject(request);
    if (outcome.IsSuccess()) {
        *file_size = outcome.GetResult().GetContentLength();
    } else {
        return Status::IOError("failed to get object size(endpoint={}, bucket={}, key={}): {}",
                               _s3_conf.endpoint, _s3_conf.bucket, key,
                               outcome.GetError().GetMessage());
    }
    return Status::OK();
}

Status S3FileSystem::list(const Path& path, std::vector<Path>* files) {
    return Status::NotSupported("not support");
}

std::string S3FileSystem::get_key(const Path& path) const {
    StringPiece str(path.native());
    if (str.starts_with(_root_path.native())) {
        return fmt::format("{}/{}", _s3_conf.prefix, str.data() + _root_path.native().size());
    }
    // We consider it as a relative path.
    return fmt::format("{}/{}", _s3_conf.prefix, path.native());
}

// oss has public endpoint and private endpoint, is_public_endpoint determines
// whether to return a public endpoint.
std::string S3FileSystem::generate_presigned_url(const Path& path, int64_t expiration_secs,
                                                 bool is_public_endpoint) const {
    auto key = get_key(path);
    std::shared_ptr<Aws::S3::S3Client> client;
    if (is_public_endpoint && ends_with(_s3_conf.endpoint, OSS_PRIVATE_ENDPOINT_SUFFIX)) {
        S3Conf new_s3_conf = _s3_conf;
        new_s3_conf.endpoint.erase(new_s3_conf.endpoint.size() - OSS_PRIVATE_ENDPOINT_SUFFIX.size(),
                                   9);
        client = ClientFactory::instance().create(new_s3_conf);
    } else {
        client = get_client();
    }
    DCHECK(client != nullptr);
    return client->GeneratePresignedUrl(_s3_conf.bucket, key, Aws::Http::HttpMethod::HTTP_GET,
                                        expiration_secs);
}

} // namespace io
} // namespace doris
