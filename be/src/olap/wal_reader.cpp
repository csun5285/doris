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

#include "olap/wal_reader.h"

#include "common/status.h"
#include "olap/storage_engine.h"
#include "olap/wal_writer.h"
#include "util/crc32c.h"

namespace doris {

WalReader::WalReader(const std::string& file_name) : _file_name(file_name), _offset(0) {}

WalReader::~WalReader() {}

Status WalReader::init() {
    Status st = _file_handler.open(_file_name, O_RDONLY);
    if (UNLIKELY(!st.ok())) {
        LOG(WARNING) << "fail to open wal file: '" << _file_name << "', error: " << st.to_string();
    }
    return st;
}

Status WalReader::finalize() {
    Status st = _file_handler.close();
    if (UNLIKELY(!st.ok())) {
        LOG(WARNING) << "fail to close wal file: '" << _file_name << "', error: " << st.to_string();
    }
    return st;
}

Status WalReader::read_block(PBlock& block) {
    if (_offset >= _file_handler.length()) {
        return Status::EndOfFile("end of wal file");
    }
    // read row length
    uint8_t row_len_buf[WalWriter::LENGTH_SIZE];
    RETURN_IF_ERROR(_file_handler.pread(row_len_buf, WalWriter::LENGTH_SIZE, _offset));
    _offset += WalWriter::LENGTH_SIZE;
    size_t block_len;
    memcpy(&block_len, row_len_buf, WalWriter::LENGTH_SIZE);
    // read block
    std::string block_buf;
    block_buf.resize(block_len);
    RETURN_IF_ERROR(_file_handler.pread(block_buf.data(), block_len, _offset));
    _offset += block_len;
    RETURN_IF_ERROR(_deserialize(block, block_buf));
    // checksum
    uint8_t checksum_len_buf[WalWriter::CHECKSUM_SIZE];
    RETURN_IF_ERROR(_file_handler.pread(checksum_len_buf, WalWriter::CHECKSUM_SIZE, _offset));
    _offset += WalWriter::CHECKSUM_SIZE;
    uint32_t checksum;
    memcpy(&checksum, checksum_len_buf, WalWriter::CHECKSUM_SIZE);
    RETURN_IF_ERROR(_check_checksum(block_buf.data(), block_len, checksum));
    return Status::OK();
}

Status WalReader::_deserialize(PBlock& block, std::string& buf) {
    if (UNLIKELY(!block.ParseFromString(buf))) {
        return Status::InternalError("failed to deserialize row");
    }
    return Status::OK();
}

Status WalReader::_check_checksum(const char* binary, size_t size, uint32_t checksum) {
    uint32_t computed_checksum = crc32c::Value(binary, size);
    if (LIKELY(computed_checksum == checksum)) {
        return Status::OK();
    }
    return Status::InternalError("checksum failed for wal=" + _file_name +
                                 ", computed checksum=" + std::to_string(computed_checksum) +
                                 ", expected=" + std::to_string(checksum));
}

} // namespace doris
