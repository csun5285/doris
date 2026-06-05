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

#include <cstdint>
#include <vector>

#include "common/status.h"
#include "core/column/column.h"
#include "core/column/column_array.h"
#include "core/column/column_nullable.h"
#include "storage/segment/storage_view.h"
#include "storage/tablet/tablet_schema.h"

namespace doris::test_helpers {

// Stage an ARRAY IColumn to the (offsets, item_data, item_nullmap) tuple that
// IndexColumnWriter::add_array_values consumes. Mirrors what the previous
// OlapColumnDataConvertorArray produced for tests that exercise inverted-index
// array writes directly.
struct ArrayStaged {
    StorageView item_view;
    std::vector<uint64_t> offsets;
    const uint8_t* outer_nullmap = nullptr;
    size_t num_rows = 0;

    const void* item_data() const { return item_view.data; }
    const uint8_t* item_nullmap() const { return item_view.nullmap; }
    const uint8_t* offsets_ptr() const {
        return reinterpret_cast<const uint8_t*>(offsets.data());
    }
};

inline Status stage_array(const TabletColumn& array_col, const IColumn& column,
                           ArrayStaged* out) {
    const auto* nullable = check_and_get_column<ColumnNullable>(&column);
    const IColumn& nested = nullable ? nullable->get_nested_column() : column;
    const auto* col_array = check_and_get_column<ColumnArray>(&nested);
    if (col_array == nullptr) {
        return Status::InternalError("stage_array: expected ColumnArray, got {}",
                                     nested.get_name());
    }
    if (nullable) {
        out->outer_nullmap = nullable->get_null_map_data().data();
    }

    out->num_rows = col_array->size();
    out->offsets.resize(out->num_rows + 1);
    for (size_t i = 0; i <= out->num_rows; ++i) {
        out->offsets[i] = col_array->offset_at(static_cast<ssize_t>(i));
    }
    const size_t elem_count = col_array->offset_at(static_cast<ssize_t>(out->num_rows));
    if (elem_count > 0) {
        RETURN_IF_ERROR(col_array->get_data_ptr()->storage_view(array_col.get_sub_column(0), 0,
                                                                  elem_count, &out->item_view));
    }
    return Status::OK();
}

// Stage a scalar IColumn to (storage_data, storage_nullmap) — wraps
// IColumn::storage_view for tests that previously consumed OlapBlockDataConvertor's
// IOlapColumnDataAccessor->get_data() / get_nullmap() pair.
struct ScalarStaged {
    StorageView view;
    const uint8_t* outer_nullmap = nullptr;

    const uint8_t* storage_data() const { return view.data; }
    const uint8_t* storage_nullmap() const {
        return outer_nullmap ? outer_nullmap : view.nullmap;
    }
};

inline Status stage_scalar(const TabletColumn& col, const IColumn& column, ScalarStaged* out) {
    if (const auto* nullable = check_and_get_column<ColumnNullable>(&column)) {
        out->outer_nullmap = nullable->get_null_map_data().data();
    }
    return column.storage_view(col, 0, column.size(), &out->view);
}

} // namespace doris::test_helpers
