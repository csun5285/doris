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


#include <memory>

#include "cloud/io/file_system.h"
#include "cloud/io/local_file_system.h"
#include "cloud/io/local_file_reader.h"
#include "cloud/io/local_file_writer.h"
#include "util/async_io.h"
namespace doris {
namespace io {

std::shared_ptr<LocalFileSystem> LocalFileSystem::create(Path path, ResourceId resource_id) {
    std::shared_ptr<LocalFileSystem> fs(new LocalFileSystem(std::move(path), std::move(resource_id)));
    return fs;
}

LocalFileSystem::LocalFileSystem(Path&& root_path, ResourceId&& resource_id)
        : FileSystem(std::move(root_path), std::move(resource_id), FileSystemType::LOCAL) {}

LocalFileSystem::~LocalFileSystem() = default;

Path LocalFileSystem::absolute_path(const Path& path) const {
    if (path.is_absolute()) {
        return path;
    }
    return _root_path / path;
}

Status LocalFileSystem::create_file(const Path& path, FileWriterPtr* writer, IOState*) {
    auto fs_path = absolute_path(path);
    *writer = std::make_unique<LocalFileWriter>(
            std::move(fs_path), std::static_pointer_cast<LocalFileSystem>(shared_from_this()));
    return (*writer)->open();
}

Status LocalFileSystem::open_file(const Path& path, FileReaderSPtr* reader) {
    if (bthread_self() == 0) {
        return open_file_impl(path, reader);
    }
    Status s;
    auto task = [&] { s = open_file_impl(path, reader); };
    AsyncIO::run_task(task, io::FileSystemType::LOCAL);
    return s;
}

Status LocalFileSystem::open_file_impl(const Path& path, FileReaderSPtr* reader) {
    auto fs_path = absolute_path(path);
    size_t fsize = 0;
    RETURN_IF_ERROR(file_size(fs_path, &fsize));
    int fd = -1;
    RETRY_ON_EINTR(fd, open(fs_path.c_str(), O_RDONLY));
    if (fd < 0) {
        return Status::IOError("cannot open {}: {}", fs_path.native(), std::strerror(errno));
    }
    *reader = std::make_shared<LocalFileReader>(
            std::move(fs_path), fsize, fd,
            std::static_pointer_cast<LocalFileSystem>(shared_from_this()));
    return Status::OK();
}

Status LocalFileSystem::delete_file(const Path& path) {
    auto fs_path = absolute_path(path);
    if (!std::filesystem::exists(fs_path)) {
        return Status::OK();
    }
    if (!std::filesystem::is_regular_file(fs_path)) {
        return Status::IOError("{} is not a file", fs_path.native());
    }
    std::error_code ec;
    std::filesystem::remove(fs_path, ec);
    if (ec) {
        return Status::IOError("cannot delete {}: {}", fs_path.native(), std::strerror(ec.value()));
    }
    return Status::OK();
}

Status LocalFileSystem::create_directory(const Path& path) {
    auto fs_path = absolute_path(path);
    if (std::filesystem::exists(fs_path)) {
        return Status::IOError("{} exists", fs_path.native());
    }
    std::error_code ec;
    std::filesystem::create_directories(fs_path, ec);
    if (ec) {
        return Status::IOError("cannot create {}: {}", fs_path.native(), std::strerror(ec.value()));
    }
    return Status::OK();
}

Status LocalFileSystem::delete_directory(const Path& path) {
    auto fs_path = absolute_path(path);
    if (!std::filesystem::exists(fs_path)) {
        return Status::OK();
    }
    if (!std::filesystem::is_directory(fs_path)) {
        return Status::IOError("{} is not a directory", fs_path.native());
    }
    std::error_code ec;
    std::filesystem::remove_all(fs_path, ec);
    if (ec) {
        return Status::IOError("cannot delete {}: {}", fs_path.native(), std::strerror(ec.value()));
    }
    return Status::OK();
}

Status LocalFileSystem::link_file(const Path& src, const Path& dest) {
    if (::link(src.c_str(), dest.c_str()) != 0) {
        return Status::IOError("fail to create hard link: {}. from {} to {}", std::strerror(errno),
                               src.native(), dest.native());
    }
    return Status::OK();
}

Status LocalFileSystem::exists(const Path& path, bool* res) const {
    auto fs_path = absolute_path(path);
    *res = std::filesystem::exists(fs_path);
    return Status::OK();
}

Status LocalFileSystem::file_size(const Path& path, size_t* file_size) const {
    if (bthread_self() == 0) {
        return file_size_impl(path, file_size);
    }

    Status s;
    auto task = [&] { s = file_size_impl(path, file_size); };
    AsyncIO::run_task(task, io::FileSystemType::LOCAL);
    return s;
}

Status LocalFileSystem::file_size_impl(const Path& path, size_t* file_size) const {
    auto fs_path = absolute_path(path);
    std::error_code ec;
    *file_size = std::filesystem::file_size(fs_path, ec);
    if (ec) {
        return Status::IOError("cannot get file size {}: {}", fs_path.native(),
                               std::strerror(ec.value()));
    }
    return Status::OK();
}

Status LocalFileSystem::list(const Path& path, std::vector<Path>* files) {
    files->clear();
    auto fs_path = absolute_path(path);
    std::error_code ec;
    for (const auto& entry : std::filesystem::directory_iterator(fs_path, ec)) {
        files->push_back(entry.path().filename());
    }
    if (ec) {
        return Status::IOError("cannot list {}: {}", fs_path.native(), std::strerror(ec.value()));
    }
    return Status::OK();
}

static FileSystemSPtr local_fs = LocalFileSystem::create("");

const FileSystemSPtr& global_local_filesystem() {
    return local_fs;
}

} // namespace io
} // namespace doris
