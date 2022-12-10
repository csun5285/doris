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

#include "cloud/io/file_writer.h"
#include "cloud/io/local_file_system.h"

namespace doris {
namespace io {

class LocalFileWriter final : public FileWriter {
public:
    LocalFileWriter(Path path, std::shared_ptr<LocalFileSystem> fs);
    ~LocalFileWriter() override;

    Status open() override;

    Status close() override;

    Status abort() override;

    Status append(const Slice& data) override;

    Status appendv(const Slice* data, size_t data_cnt) override;

    Status write_at(size_t offset, const Slice& data) override;

    Status finalize() override;

    size_t bytes_appended() const override { return _bytes_appended; }

    FileSystemSPtr fs() const override { return _fs; }

private:
    Status _close(bool sync);

private:
    int _fd = -1; // owned
    std::shared_ptr<LocalFileSystem> _fs;

    size_t _bytes_appended = 0;
    bool _dirty = false;
    bool _closed = true;
};

} // namespace io
} // namespace doris
