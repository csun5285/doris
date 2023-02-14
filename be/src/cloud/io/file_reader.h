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

#include <gen_cpp/Types_types.h>

#include <memory>

#include "cloud/io/path.h"
#include "common/status.h"
#include "gutil/macros.h"
#include "olap/olap_common.h"
#include "util/slice.h"
namespace doris {

namespace io {

class FileSystem;

struct IOState {
    IOState(const TUniqueId* query_id, OlapReaderStatistics* stats, bool is_presistent,
            bool use_disposable_cache, bool read_segmeng_index)
            : query_id(query_id),
              stats(stats),
              is_persistent(is_presistent),
              use_disposable_cache(use_disposable_cache),
              read_segmeng_index(read_segmeng_index) {}
    const TUniqueId* query_id = nullptr;
    OlapReaderStatistics* stats = nullptr;
    bool is_persistent = false;
    bool use_disposable_cache = false;
    bool read_segmeng_index = false;
    bool read_from_tmp_file = false;
};
class FileReader {
public:
    FileReader() = default;
    virtual ~FileReader() = default;

    DISALLOW_COPY_AND_ASSIGN(FileReader);

    virtual Status close() = 0;

    virtual Status read_at(size_t offset, Slice result, size_t* bytes_read,
                           IOState* state = nullptr) = 0;

    virtual const Path& path() const = 0;

    virtual size_t size() const = 0;

    virtual bool closed() const = 0;

    virtual std::shared_ptr<FileSystem> fs() const = 0;
};

using FileReaderSPtr = std::shared_ptr<FileReader>;

} // namespace io
} // namespace doris
