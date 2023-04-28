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

#include "olap/wal_writer.h"

#include "olap/storage_engine.h"
#include "olap/utils.h"
#include "util/crc32c.h"

namespace doris {

WalWriter::WalWriter(const std::string& file_name) : _file_name(file_name) {
}

WalWriter::~WalWriter() {}

Status WalWriter::init() {
    Status st = _file_handler.open_with_mode(_file_name, O_CREAT | O_EXCL | O_WRONLY,
                                             S_IRUSR | S_IWUSR);
    if (UNLIKELY(!st.ok())) {
        LOG(WARNING) << "fail to open wal file: '" << _file_name << "', error: " << st.to_string();
    }
    return st;
}

Status WalWriter::finalize() {
    Status st = _file_handler.close();
    if (UNLIKELY(!st.ok())) {
        LOG(WARNING) << "fail to close wal file: '" << _file_name << "', error: " << st.to_string();
    }
    return st;
}

Status WalWriter::append_blocks(const PBlockArray& blocks) {
    size_t total_size = 0;
    for (const auto& block : blocks) {
        total_size += LENGTH_SIZE + block.ByteSizeLong() + CHECKSUM_SIZE;
    }
    // uint8_t row_binary[total_size];
    // std::shared_ptr<void> binary(malloc(total_size), free);
    std::string binary(total_size, '\0');
    char* row_binary = binary.data();
    // memset(row_binary, 0, total_size);
    size_t offset = 0;
    for (const auto& block : blocks) {
        unsigned long row_length = block.GetCachedSize();
        memcpy(row_binary + offset, &row_length, LENGTH_SIZE);
        offset += LENGTH_SIZE;
        memcpy(row_binary + offset, block.SerializeAsString().data(), row_length);
        offset += row_length;
        uint32_t checksum = crc32c::Value(block.SerializeAsString().data(), row_length);
        memcpy(row_binary + offset, &checksum, CHECKSUM_SIZE);
        offset += CHECKSUM_SIZE;
    }
    // write rows
    RETURN_IF_ERROR(_file_handler.write(row_binary, total_size));
    return Status::OK();
}

} // namespace doris
