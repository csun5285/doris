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

#include "common/status.h"
#include "runtime/types.h"

namespace doris::vectorized {

class Block;
// This a reader interface for all file readers.
// A GenericReader is responsible for reading a file and return
// a set of blocks with specified schema,
class GenericReader {
public:
    virtual Status get_next_block(Block* block, size_t* read_rows, bool* eof) = 0;
    virtual std::unordered_map<std::string, TypeDescriptor> get_name_to_type() {
        std::unordered_map<std::string, TypeDescriptor> map;
        return map;
    }
    virtual Status get_columns(std::unordered_map<std::string, TypeDescriptor>* name_to_type,
                               std::unordered_set<std::string>* missing_cols) {
        return Status::NotSupported("get_columns is not implemented");
    }

    virtual Status get_parsered_schema(std::vector<std::string>* col_names,
                                       std::vector<TypeDescriptor>* col_types) {
        return Status::NotSupported("get_parser_schema is not implemented for this reader.");
    }
    virtual ~GenericReader() = default;
};

} // namespace doris::vectorized
