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

#include <aws/core/client/AWSError.h>
#include <aws/core/http/HttpResponse.h>
#include <aws/core/utils/Outcome.h>
#include <aws/core/utils/memory/stl/AWSAllocator.h>
#include <aws/core/utils/memory/stl/AWSMap.h>
#include <aws/core/utils/memory/stl/AWSStreamFwd.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <aws/core/utils/memory/stl/AWSVector.h>
#include <aws/core/utils/threading/Executor.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/S3Errors.h>
#include <aws/s3/model/CopyObjectRequest.h>
#include <aws/s3/model/CopyObjectResult.h>
#include <aws/s3/model/Delete.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/DeleteObjectResult.h>
#include <aws/s3/model/DeleteObjectsRequest.h>
#include <aws/s3/model/DeleteObjectsResult.h>
#include <aws/s3/model/Error.h>
#include <aws/s3/model/GetBucketVersioningRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/GetObjectResult.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/HeadObjectResult.h>
#include <aws/s3/model/ListObjectVersionsRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/ListObjectsV2Result.h>
#include <aws/s3/model/Object.h>
#include <aws/s3/model/ObjectIdentifier.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/PutObjectResult.h>
#include <aws/transfer/TransferHandle.h>
#include <aws/transfer/TransferManager.h>
#include <fmt/format.h>
#include <stddef.h>

#include <algorithm>

#include "common/sync_point.h"
#include "io/fs/file_reader_options.h"

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include "common/compiler_util.h" // IWYU pragma: keep
// IWYU pragma: no_include <bits/chrono.h>
#include <chrono> // IWYU pragma: keep
#include <filesystem>
#include <fstream> // IWYU pragma: keep
#include <memory>
#include <sstream>

#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "io/fs/file_system.h"
#include "io/fs/file_writer.h"
#include "io/fs/remote_file_system.h"
#include "io/fs/s3_file_reader.h"
#include "io/fs/s3_file_writer.h"
#include "service/backend_options.h"
#include "util/s3_uri.h"
#include "util/s3_util.h"

namespace doris {
namespace io {

bvar::Adder<uint64_t> s3_file_size_counter("s3_file_system", "file_size");

#ifndef CHECK_S3_CLIENT
#define CHECK_S3_CLIENT(client)                         \
    if (!client) {                                      \
        return Status::IOError("init s3 client error"); \
    }
#endif

constexpr std::string_view OSS_PRIVATE_ENDPOINT_SUFFIX = "-internal.aliyuncs.com";
constexpr int LEN_OF_OSS_PRIVATE_SUFFIX = 9; // length of "-internal"
#ifndef CHECK_S3_PATH
#define CHECK_S3_PATH(uri, path) \
    S3URI uri(path.string());    \
    RETURN_IF_ERROR(uri.parse());
#endif

#ifndef GET_KEY
#define GET_KEY(key, path) \
    std::string key;       \
    RETURN_IF_ERROR(get_key(path, &key));
#endif

Status S3FileSystem::create(S3Conf s3_conf, std::string id, std::shared_ptr<S3FileSystem>* fs) {
    (*fs).reset(new S3FileSystem(std::move(s3_conf), std::move(id)));
    return (*fs)->connect();
}

S3FileSystem::S3FileSystem(S3Conf&& s3_conf, std::string&& id)
        : RemoteFileSystem(s3_conf.prefix, std::move(id), FileSystemType::S3),
          _s3_conf(std::move(s3_conf)) {
    // remove the first and last '/'
    if (!_s3_conf.prefix.empty()) {
        if (_s3_conf.prefix[0] == '/') {
            _s3_conf.prefix = _s3_conf.prefix.substr(1);
        }
        if (_s3_conf.prefix.back() == '/') {
            _s3_conf.prefix.pop_back();
        }
    }
    _executor = Aws::MakeShared<Aws::Utils::Threading::PooledThreadExecutor>(
            id.c_str(), config::s3_transfer_executor_pool_size);
}

S3FileSystem::~S3FileSystem() = default;

std::shared_ptr<Aws::Transfer::TransferManager> S3FileSystem::get_transfer_manager() {
    std::lock_guard lock(_client_mu);
    if (_transfer_manager == nullptr) {
        if (_client == nullptr) {
            return nullptr;
        }
        Aws::Transfer::TransferManagerConfiguration transfer_config(_executor.get());
        transfer_config.s3Client = _client;
        transfer_config.transferBufferMaxHeapSize = config::s3_transfer_buffer_size_mb << 20;
        if (_s3_conf.sse_enabled) {
            transfer_config.putObjectTemplate.WithServerSideEncryption(
                    Aws::S3::Model::ServerSideEncryption::AES256);
            transfer_config.createMultipartUploadTemplate.WithServerSideEncryption(
                    Aws::S3::Model::ServerSideEncryption::AES256);
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

Status S3FileSystem::connect_impl() {
    auto client = S3ClientFactory::instance().create(_s3_conf);
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

Status S3FileSystem::create_file_impl(const Path& file, FileWriterPtr* writer,
                                      const FileWriterOptions* opts) {
    GET_KEY(key, file);
    *writer = std::make_unique<S3FileWriter>(
            key, std::static_pointer_cast<S3FileSystem>(shared_from_this()), opts);
    return Status::OK();
}

Status S3FileSystem::open_file_internal(const Path& file, FileReaderSPtr* reader,
                                        const FileReaderOptions* opts) {
    int64_t fsize = opts ? opts->file_size : -1;
    if (fsize < 0) {
        RETURN_IF_ERROR(file_size_impl(file, &fsize));
    }
    GET_KEY(key, file);
    *reader = std::make_shared<S3FileReader>(
            fsize, std::move(key), std::static_pointer_cast<S3FileSystem>(shared_from_this()));
    return Status::OK();
}

Status S3FileSystem::create_directory_impl(const Path& dir, bool failed_if_exists) {
    return Status::OK();
}

Status S3FileSystem::delete_file_impl(const Path& file) {
    auto client = get_client();
    CHECK_S3_CLIENT(client);

    Aws::S3::Model::DeleteObjectRequest request;
    GET_KEY(key, file);
    request.WithBucket(_s3_conf.bucket).WithKey(key);

    auto outcome = client->DeleteObject(request);
    s3_bvar::s3_delete_total << 1;
    if (outcome.IsSuccess() ||
        outcome.GetError().GetResponseCode() == Aws::Http::HttpResponseCode::NOT_FOUND) {
        return Status::OK();
    }
    return Status::IOError("failed to delete file {}: {}", file.native(), error_msg(key, outcome));
}

Status S3FileSystem::delete_directory_impl(const Path& dir) {
    auto client = get_client();
    CHECK_S3_CLIENT(client);

    Aws::S3::Model::ListObjectsV2Request request;
    GET_KEY(prefix, dir);
    if (!prefix.empty() && prefix.back() != '/') {
        prefix.push_back('/');
    }
    request.WithBucket(_s3_conf.bucket).WithPrefix(prefix);

    Aws::S3::Model::DeleteObjectsRequest delete_request;
    delete_request.SetBucket(_s3_conf.bucket);
    bool is_trucated = false;
    do {
        auto outcome = client->ListObjectsV2(request);
        s3_bvar::s3_list_total << 1;
        if (!outcome.IsSuccess()) {
            return Status::IOError("failed to list objects when delete dir {}: {}", dir.native(),
                                   error_msg(prefix, outcome));
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
            s3_bvar::s3_delete_total << 1;
            if (!delete_outcome.IsSuccess()) {
                return Status::IOError("failed to delete dir {}: {}", dir.native(),
                                       error_msg(prefix, delete_outcome));
            }
            if (!delete_outcome.GetResult().GetErrors().empty()) {
                const auto& e = delete_outcome.GetResult().GetErrors().front();
                return Status::IOError("fail to delete object: {}",
                                       error_msg(e.GetKey(), e.GetMessage()));
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

Status S3FileSystem::batch_delete_impl(const std::vector<Path>& remote_files) {
    auto client = get_client();
    CHECK_S3_CLIENT(client);

    // `DeleteObjectsRequest` can only contain 1000 keys at most.
    constexpr size_t max_delete_batch = 1000;
    auto path_iter = remote_files.begin();

    Aws::S3::Model::DeleteObjectsRequest delete_request;
    delete_request.SetBucket(_s3_conf.bucket);
    do {
        Aws::S3::Model::Delete del;
        Aws::Vector<Aws::S3::Model::ObjectIdentifier> objects;
        auto path_begin = path_iter;
        for (; path_iter != remote_files.end() && (path_iter - path_begin < max_delete_batch);
             ++path_iter) {
            GET_KEY(key, *path_iter);
            objects.emplace_back().SetKey(key);
        }
        if (objects.empty()) {
            return Status::OK();
        }
        del.WithObjects(std::move(objects)).SetQuiet(true);
        delete_request.SetDelete(std::move(del));
        auto delete_outcome = client->DeleteObjects(delete_request);
        s3_bvar::s3_delete_total << 1;
        if (UNLIKELY(!delete_outcome.IsSuccess())) {
            return Status::IOError(
                    "failed to delete objects: {}",
                    error_msg(delete_request.GetDelete().GetObjects().front().GetKey(),
                              delete_outcome));
        }
        if (UNLIKELY(!delete_outcome.GetResult().GetErrors().empty())) {
            const auto& e = delete_outcome.GetResult().GetErrors().front();
            return Status::IOError("failed to delete objects: {}",
                                   error_msg(e.GetKey(), delete_outcome));
        }
    } while (path_iter != remote_files.end());

    return Status::OK();
}

Status S3FileSystem::exists_impl(const Path& path, bool* res) const {
    auto client = get_client();
    CHECK_S3_CLIENT(client);
    GET_KEY(key, path);

    Aws::S3::Model::HeadObjectRequest request;
    request.WithBucket(_s3_conf.bucket).WithKey(key);

    auto outcome = SYNC_POINT_HOOK_RETURN_VALUE(
            client->HeadObject(request), "s3_file_system::head_object", std::ref(request).get());
    s3_bvar::s3_head_total << 1;
    if (outcome.IsSuccess()) {
        *res = true;
    } else if (outcome.GetError().GetResponseCode() == Aws::Http::HttpResponseCode::NOT_FOUND) {
        *res = false;
    } else {
        return Status::IOError("failed to check exists {}: {}", path.native(),
                               error_msg(key, outcome));
    }
    return Status::OK();
}

Status S3FileSystem::file_size_impl(const Path& file, int64_t* file_size) const {
    auto client = get_client();
    CHECK_S3_CLIENT(client);

    Aws::S3::Model::HeadObjectRequest request;
    GET_KEY(key, file);
    request.WithBucket(_s3_conf.bucket).WithKey(key);

    auto outcome = SYNC_POINT_HOOK_RETURN_VALUE(
            client->HeadObject(request), "s3_file_system::head_object", std::ref(request).get());
    s3_bvar::s3_head_total << 1;
    if (outcome.IsSuccess()) {
        *file_size = outcome.GetResult().GetContentLength();
    } else {
        return Status::IOError("failed to get file size {}, {}", file.native(),
                               error_msg(key, outcome));
    }
    s3_file_size_counter << 1;
    return Status::OK();
}

Status S3FileSystem::list_impl(const Path& dir, bool only_file, std::vector<FileInfo>* files,
                               bool* exists) {
    // For object storage, this path is always not exist.
    // So we ignore this property and set exists to true.
    *exists = true;
    auto client = get_client();
    CHECK_S3_CLIENT(client);
    GET_KEY(prefix, dir);
    if (!prefix.empty() && prefix.back() != '/') {
        prefix.push_back('/');
    }

    Aws::S3::Model::ListObjectsV2Request request;
    request.WithBucket(_s3_conf.bucket).WithPrefix(prefix);
    bool is_trucated = false;
    do {
        auto outcome = SYNC_POINT_HOOK_RETURN_VALUE(client->ListObjectsV2(request),
                                                    "s3_file_system::list_object",
                                                    std::ref(request).get());
        s3_bvar::s3_list_total << 1;
        if (!outcome.IsSuccess()) {
            return Status::IOError("failed to list {}: {}", dir.native(),
                                   error_msg(prefix, outcome));
        }
        for (const auto& obj : outcome.GetResult().GetContents()) {
            std::string key = obj.GetKey();
            bool is_dir = (key.at(key.size() - 1) == '/');
            if (only_file && is_dir) {
                continue;
            }
            FileInfo file_info;
            // note: if full path is s3://bucket/path/to/file.txt
            // obj.GetKey() will be /path/to/file.txt
            file_info.file_name = obj.GetKey().substr(prefix.size());
            file_info.file_size = obj.GetSize();
            file_info.is_file = !is_dir;
            files->push_back(std::move(file_info));
        }
        is_trucated = outcome.GetResult().GetIsTruncated();
        request.SetContinuationToken(outcome.GetResult().GetNextContinuationToken());
    } while (is_trucated);
    return Status::OK();
}

Status S3FileSystem::rename_impl(const Path& orig_name, const Path& new_name) {
    RETURN_IF_ERROR(copy(orig_name, new_name));
    return delete_file_impl(orig_name);
}

Status S3FileSystem::rename_dir_impl(const Path& orig_name, const Path& new_name) {
    RETURN_IF_ERROR(copy_dir(orig_name, new_name));
    return delete_directory_impl(orig_name);
}

Status S3FileSystem::upload_impl(const Path& local_file, const Path& remote_file) {
    auto transfer_manager = get_transfer_manager();

    auto start = std::chrono::steady_clock::now();

    GET_KEY(key, remote_file);
    auto handle = transfer_manager->UploadFile(local_file.native(), _s3_conf.bucket, key,
                                               "text/plain", Aws::Map<Aws::String, Aws::String>());
    handle->WaitUntilFinished();

    auto duration = std::chrono::duration<float>(std::chrono::steady_clock::now() - start);

    if (handle->GetStatus() != Aws::Transfer::TransferStatus::COMPLETED) {
        return Status::IOError("failed to upload {}: {}", remote_file.native(),
                               error_msg(key, handle->GetLastError().GetMessage()));
    }

    auto file_size = std::filesystem::file_size(local_file);
    LOG(INFO) << "Upload " << local_file.native() << " to s3, endpoint=" << _s3_conf.endpoint
              << ", bucket=" << _s3_conf.bucket << ", key=" << key
              << ", duration=" << duration.count() << ", capacity=" << file_size
              << ", tp=" << (file_size) / duration.count();

    return Status::OK();
}

Status S3FileSystem::batch_upload_impl(const std::vector<Path>& local_files,
                                       const std::vector<Path>& remote_files) {
    if (local_files.size() != remote_files.size()) {
        return Status::InvalidArgument("local_files.size({}) != remote_files.size({})",
                                       local_files.size(), remote_files.size());
    }

    auto transfer_manager = get_transfer_manager();

    std::vector<std::shared_ptr<Aws::Transfer::TransferHandle>> handles;
    for (int i = 0; i < local_files.size(); ++i) {
        GET_KEY(key, remote_files[i]);
        LOG(INFO) << "Start to upload " << local_files[i].native()
                  << " to s3, endpoint=" << _s3_conf.endpoint << ", bucket=" << _s3_conf.bucket
                  << ", key=" << key;
        auto handle =
                transfer_manager->UploadFile(local_files[i].native(), _s3_conf.bucket, key,
                                             "text/plain", Aws::Map<Aws::String, Aws::String>());
        handles.push_back(std::move(handle));
    }
    for (auto& handle : handles) {
        handle->WaitUntilFinished();
        if (handle->GetStatus() != Aws::Transfer::TransferStatus::COMPLETED) {
            // TODO(cyx): Maybe we can cancel remaining handles.
            return Status::IOError(
                    "failed to upload: {}",
                    error_msg(handle->GetKey(), handle->GetLastError().GetMessage()));
        }
    }
    return Status::OK();
}

Status S3FileSystem::direct_upload_impl(const Path& remote_file, const std::string& content) {
    CHECK_S3_CLIENT(_client);
    Aws::S3::Model::PutObjectRequest request;
    GET_KEY(key, remote_file);
    request.WithBucket(_s3_conf.bucket).WithKey(key);
    const std::shared_ptr<Aws::IOStream> input_data =
            Aws::MakeShared<Aws::StringStream>("upload_directly");
    *input_data << content.c_str();
    if (input_data->good()) {
        request.SetBody(input_data);
    }
    if (!input_data->good()) {
        return Status::IOError("failed to direct upload {}: failed to read from string",
                               remote_file.native());
    }
    Aws::S3::Model::PutObjectOutcome response = _client->PutObject(request);
    s3_bvar::s3_put_total << 1;
    if (response.IsSuccess()) {
        return Status::OK();
    } else {
        return Status::IOError("failed to direct upload {}: {}", remote_file.native(),
                               error_msg(key, response));
    }
}

Status S3FileSystem::upload_with_checksum_impl(const Path& local_file, const Path& remote_file,
                                               const std::string& checksum) {
    return upload_impl(local_file, remote_file.string() + "." + checksum);
}

Status S3FileSystem::download_impl(const Path& remote_file, const Path& local_file) {
    auto client = get_client();
    CHECK_S3_CLIENT(client);
    GET_KEY(key, remote_file);
    Aws::S3::Model::GetObjectRequest request;
    request.WithBucket(_s3_conf.bucket).WithKey(key);
    Aws::S3::Model::GetObjectOutcome response = _client->GetObject(request);
    s3_bvar::s3_get_total << 1;
    if (response.IsSuccess()) {
        Aws::OFStream local_file_s;
        local_file_s.open(local_file, std::ios::out | std::ios::binary);
        if (local_file_s.good()) {
            local_file_s << response.GetResult().GetBody().rdbuf();
        }
        if (!local_file_s.good()) {
            return Status::IOError("failed to download {}: failed to write file: {}",
                                   remote_file.native(), local_file.native());
        }
    } else {
        return Status::IOError("failed to download {}: {}", remote_file.native(),
                               error_msg(key, response));
    }
    return Status::OK();
}

Status S3FileSystem::direct_download_impl(const Path& remote, std::string* content) {
    CHECK_S3_CLIENT(_client);
    Aws::S3::Model::GetObjectRequest request;
    GET_KEY(key, remote);
    request.WithBucket(_s3_conf.bucket).WithKey(key);
    Aws::S3::Model::GetObjectOutcome response = _client->GetObject(request);
    s3_bvar::s3_get_total << 1;
    if (response.IsSuccess()) {
        std::stringstream ss;
        ss << response.GetResult().GetBody().rdbuf();
        *content = ss.str();
    } else {
        return Status::IOError("failed to direct download {}: {}", remote.native(),
                               error_msg(key, response));
    }
    return Status::OK();
}

Status S3FileSystem::copy(const Path& src, const Path& dst) {
    CHECK_S3_CLIENT(_client);
    Aws::S3::Model::CopyObjectRequest request;
    GET_KEY(src_key, src);
    GET_KEY(dst_key, dst);
    request.WithCopySource(_s3_conf.bucket + "/" + src_key)
            .WithKey(dst_key)
            .WithBucket(_s3_conf.bucket);
    Aws::S3::Model::CopyObjectOutcome response = _client->CopyObject(request);
    s3_bvar::s3_copy_object_total << 1;
    if (response.IsSuccess()) {
        return Status::OK();
    } else {
        return Status::IOError("failed to copy from {} to {}: {}", src.native(), dst.native(),
                               error_msg(src_key, response));
    }
}

Status S3FileSystem::copy_dir(const Path& src, const Path& dst) {
    std::vector<FileInfo> files;
    bool exists = false;
    RETURN_IF_ERROR(list_impl(src, true, &files, &exists));
    if (!exists) {
        return Status::IOError("path not found: {}", src.native());
    }
    if (files.empty()) {
        LOG(WARNING) << "Nothing need to copy: " << src << " -> " << dst;
        return Status::OK();
    }
    for (auto& file : files) {
        RETURN_IF_ERROR(copy(src / file.file_name, dst / file.file_name));
    }
    return Status::OK();
}

Status S3FileSystem::get_key(const Path& path, std::string* key) const {
    CHECK_S3_PATH(uri, path);
    *key = uri.get_key();
    return Status::OK();
}

template <typename AwsOutcome>
std::string S3FileSystem::error_msg(const std::string& key, const AwsOutcome& outcome) const {
    return fmt::format("(endpoint: {}, bucket: {}, key:{}, {}), {}, error code {}",
                       _s3_conf.endpoint, _s3_conf.bucket, key,
                       outcome.GetError().GetExceptionName(), outcome.GetError().GetMessage(),
                       outcome.GetError().GetResponseCode());
}

std::string S3FileSystem::error_msg(const std::string& key, const std::string& err) const {
    return fmt::format("(endpoint: {}, bucket: {}, key:{}), {}", _s3_conf.endpoint, _s3_conf.bucket,
                       key, err);
}

// oss has public endpoint and private endpoint, is_public_endpoint determines
// whether to return a public endpoint.
std::string S3FileSystem::generate_presigned_url(const Path& path, int64_t expiration_secs,
                                                 bool is_public_endpoint) const {
    // FIXME(Xiaoccer): only support relative path
    std::string key = fmt::format("{}/{}", _s3_conf.prefix, path.native());
    std::shared_ptr<Aws::S3::S3Client> client;
    if (is_public_endpoint && _s3_conf.endpoint.ends_with(OSS_PRIVATE_ENDPOINT_SUFFIX)) {
        S3Conf new_s3_conf = _s3_conf;
        new_s3_conf.endpoint.erase(new_s3_conf.endpoint.size() - OSS_PRIVATE_ENDPOINT_SUFFIX.size(),
                                   LEN_OF_OSS_PRIVATE_SUFFIX);
        client = S3ClientFactory::instance().create(new_s3_conf);
    } else {
        client = get_client();
    }
    DCHECK(client != nullptr);
    return client->GeneratePresignedUrl(_s3_conf.bucket, key, Aws::Http::HttpMethod::HTTP_GET,
                                        expiration_secs);
}

// All exception logs start with "Versioning Err":
Status S3FileSystem::check_bucket_versioning() const {
    auto s3_client = get_client();
    CHECK_S3_CLIENT(s3_client);

    auto ip = BackendOptions::get_localhost();
    if (ip.empty() || ip == "127.0.0.1") {
        return Status::InternalError("Versioning Err: fail to get correct host ip, ip=" + ip);
    }
    const std::string check_key = _s3_conf.prefix + "/error_log/" + ip + "_check_versioning.txt";
    const std::string check_value = "check bucket versioning";
    const std::string bucket = _s3_conf.bucket;

    auto log_with_s3_info = [&]() {
        std::stringstream ss;
        ss << " s3info: endpoint: " << _s3_conf.endpoint << " bucket:" << _s3_conf.bucket
           << " key: " << check_key;
        return ss.str();
    };

    Aws::S3::Model::GetBucketVersioningRequest request;
    request.SetBucket(bucket);
    auto outcome = s3_client->GetBucketVersioning(request);
    s3_bvar::s3_get_bucket_version_total << 1;

    if (outcome.IsSuccess()) {
        auto versioning_configuration = outcome.GetResult().GetStatus();
        if (versioning_configuration == Aws::S3::Model::BucketVersioningStatus::Enabled) {
            LOG(INFO) << "Bucket versioning is enabled";
        } else {
            return Status::InternalError("Versioning Err: Bucket versioning is not enabled" +
                                         log_with_s3_info());
        }
    } else {
        return Status::InternalError("Versioning Err: Error get bucket versioning configuration: " +
                                     outcome.GetError().GetMessage() + log_with_s3_info());
    }

    // Todo: cos does not support the ListObjectVersions yet
    if (_s3_conf.provider == selectdb::ObjectStoreInfoPB::COS) {
        return Status::OK();
    }

    // Test whether the versioning behavior is correct with the following process:
    // put obj-> delete obj-> list&check obj version -> get obj with version & check content -> put obj again

    // put obj
    Aws::S3::Model::PutObjectRequest upload_request;
    upload_request.WithBucket(bucket).WithKey(check_key);
    auto input_data = Aws::MakeShared<Aws::StringStream>("PutObjectInputStream");
    *input_data << check_value;
    upload_request.SetBody(input_data);
    auto upload_response = s3_client->PutObject(upload_request);
    s3_bvar::s3_put_total << 1;
    if (!upload_response.IsSuccess()) {
        return Status::InternalError("Versioning Err: Error uploading check object : " +
                                     upload_response.GetError().GetMessage() + log_with_s3_info());
    }
    std::string check_version_id = upload_response.GetResult().GetVersionId();
    VLOG_DEBUG << "put verison_id:" << upload_response.GetResult().GetVersionId();

    // delete obj
    Aws::S3::Model::DeleteObjectRequest delete_request;
    delete_request.WithBucket(bucket).WithKey(check_key);
    auto delete_response = s3_client->DeleteObject(delete_request);
    s3_bvar::s3_delete_total << 1;
    if (!delete_response.IsSuccess()) {
        return Status::InternalError("Versioning Err: Error deleting check object : " +
                                     delete_response.GetError().GetMessage() + log_with_s3_info());
    } else {
        VLOG_DEBUG << "delete version_id: " << delete_response.GetResult().GetVersionId();
    }

    // list&check version
    Aws::S3::Model::ListObjectVersionsRequest list_request;
    // get last two version
    list_request.WithBucket(bucket).WithPrefix(check_key).WithMaxKeys(2);
    auto list_response = s3_client->ListObjectVersions(list_request);
    s3_bvar::s3_list_object_versions_total << 1;
    if (!list_response.IsSuccess()) {
        return Status::InternalError("Versioning Err: Error listing object versions: " +
                                     list_response.GetError().GetMessage() + log_with_s3_info());
    } else {
        const auto& object_version_list = list_response.GetResult().GetVersions();
        bool hit_check_version_id = false;
        for (const auto& object_version : object_version_list) {
            VLOG_DEBUG << "object version_id: " << object_version.GetVersionId();
            if (object_version.GetVersionId() == check_version_id) {
                hit_check_version_id = true;
            }
        }
        const auto& delete_marker_list = list_response.GetResult().GetDeleteMarkers();
        for (const auto& delete_marker : delete_marker_list) {
            VLOG_DEBUG << "object deleteMarker version_id: " << delete_marker.GetVersionId();
        }
        if (!hit_check_version_id) {
            return Status::InternalError("Versioning Err: do not contain check version obj." +
                                         log_with_s3_info());
        }
    }

    // get obj with version & check content
    Aws::S3::Model::GetObjectRequest get_request;
    get_request.WithBucket(bucket).WithKey(check_key).WithVersionId(check_version_id);
    auto get_response = s3_client->GetObject(get_request);
    s3_bvar::s3_get_total << 1;
    if (!get_response.IsSuccess()) {
        return Status::InternalError("Versioning Err: Error getting check object with version: " +
                                     get_response.GetError().GetMessage() + log_with_s3_info());
    }

    auto res_stream = std::stringstream {};
    res_stream << get_response.GetResult().GetBody().rdbuf();
    std::string res_string = res_stream.str();

    if (res_string != check_value) {
        return Status::InternalError(
                "Versioning Err: check object value does not match original value." +
                log_with_s3_info());
    }

    // put obj again
    Aws::S3::Model::PutObjectRequest reupload_request;
    reupload_request.WithBucket(bucket).WithKey(check_key);
    auto reupload_input_data = Aws::MakeShared<Aws::StringStream>("PutObjectInputStream");
    *reupload_input_data << check_value;
    reupload_request.SetBody(reupload_input_data);
    auto reupload_response = s3_client->PutObject(reupload_request);
    s3_bvar::s3_put_total << 1;
    if (!reupload_response.IsSuccess()) {
        return Status::InternalError("Versioning Err: Error re-uploading check object: " +
                                     reupload_response.GetError().GetMessage() +
                                     log_with_s3_info());
    }
    return Status::OK();
}

} // namespace io
} // namespace doris
