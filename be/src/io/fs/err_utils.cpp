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

#include "io/fs/err_utils.h"

// IWYU pragma: no_include <bthread/errno.h>
#include <aws/s3/S3Errors.h>
#include <errno.h> // IWYU pragma: keep
#include <fmt/format.h>
#include <string.h>

#include <sstream>

#include "io/fs/hdfs.h"

namespace doris {
using namespace ErrorCode;

namespace io {

std::string errno_to_str() {
    char buf[1024];
    return fmt::format("({}), {}", errno, strerror_r(errno, buf, 1024));
}

std::string errcode_to_str(const std::error_code& ec) {
    return fmt::format("({}), {}", ec.value(), ec.message());
}

std::string hdfs_error() {
    std::stringstream ss;
    char buf[1024];
    ss << "(" << errno << "), " << strerror_r(errno, buf, 1024) << ")";
#ifdef USE_HADOOP_HDFS
    char* root_cause = hdfsGetLastExceptionRootCause();
    if (root_cause != nullptr) {
        ss << ", reason: " << root_cause;
    }
#else
    ss << ", reason: " << hdfsGetLastError();
#endif
    return ss.str();
}

std::string glob_err_to_str(int code) {
    std::string msg;
    // https://sites.uclouvain.be/SystInfo/usr/include/glob.h.html
    switch (code) {
    case 1:
        msg = "Ran out of memory";
        break;
    case 2:
        msg = "read error";
        break;
    case 3:
        msg = "No matches found";
        break;
    default:
        msg = "unknown";
        break;
    }
    return fmt::format("({}), {}", code, msg);
}

Status localfs_error(const std::error_code& ec, std::string_view msg) {
    if (ec == std::errc::io_error) {
        return Status::IOError(msg);
    } else if (ec == std::errc::no_such_file_or_directory) {
        return Status::NotFound(msg);
    } else if (ec == std::errc::file_exists) {
        return Status::AlreadyExist(msg);
    } else if (ec == std::errc::no_space_on_device) {
        return Status::Error<DISK_REACH_CAPACITY_LIMIT>(msg);
    } else if (ec == std::errc::permission_denied) {
        return Status::Error<PERMISSION_DENIED>(msg);
    } else {
        return Status::InternalError("{}: {}", msg, ec.message());
    }
}

Status localfs_error(int posix_errno, std::string_view msg) {
    switch (posix_errno) {
    case EIO:
        return Status::IOError(msg);
    case ENOENT:
        return Status::NotFound(msg);
    case EEXIST:
        return Status::AlreadyExist(msg);
    case ENOSPC:
        return Status::Error<DISK_REACH_CAPACITY_LIMIT>(msg);
    case EACCES:
        return Status::Error<PERMISSION_DENIED>(msg);
    default:
        return Status::InternalError("{}: {}", msg, std::strerror(posix_errno));
    }
}

Status s3fs_error(const Aws::S3::S3Error& err, std::string_view msg) {
    using namespace Aws::Http;
    switch (err.GetResponseCode()) {
    case HttpResponseCode::NOT_FOUND:
        return Status::NotFound("{}: {} {}, request_id={}", msg, err.GetExceptionName(),
                                err.GetMessage(), err.GetRequestId());
    case HttpResponseCode::FORBIDDEN:
        return Status::Error<PERMISSION_DENIED>("{}: {} {}, request_id={}", msg,
                                                err.GetExceptionName(), err.GetMessage(),
                                                err.GetRequestId());
    default:
        return Status::InternalError("{}: {} {} code={}, request_id={}", msg,
                                     err.GetExceptionName(), err.GetMessage(),
                                     err.GetResponseCode(), err.GetRequestId());
    }
}

} // namespace io
} // namespace doris
