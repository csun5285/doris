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

#include "exprs/table_function/explode_split.h"
#include "gutil/strings/stringpiece.h"
#include "runtime/string_value.h"
#include "vec/columns/column.h"

namespace doris::vectorized {

class VExplodeSplitTableFunction : public ExplodeSplitTableFunction {
public:
    VExplodeSplitTableFunction();
    virtual ~VExplodeSplitTableFunction() = default;

    virtual Status open() override;
    virtual Status process_init(vectorized::Block* block) override;
    virtual Status process_row(size_t row_idx) override;
    virtual Status process_close() override;
    virtual Status get_value(void** output) override;
    virtual Status get_value_length(int64_t* length) override;

private:
    std::vector<std::string_view> _backup;

    ColumnPtr _text_column;
    const uint8_t* _test_null_map = nullptr;
    const ColumnString* _real_text_column = nullptr;

    StringRef _delimiter = {};
};

} // namespace doris::vectorized
