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
#include "core/block/column_with_type_and_name.h"
#include "core/column/column_array.h"
#include "core/column/column_nullable.h"
#include "storage/iterator/olap_data_convertor.h"
#include "storage/tablet/tablet_schema.h"

namespace doris::segment_v2 {

// What the array index writers need from an ARRAY column: the item cells, the
// item null map, and the per-row element counts.
//
// Same shape as IndexBuilder::_add_array. add_array_values only ever reads
// offset differences, so the offsets here start at 0 for each batch.
struct ArrayIndexInput {
    ColumnEncoding item_encoder;
    std::vector<uint64_t> offsets;
    const void* item_data = nullptr;
    const uint8_t* item_nullmap = nullptr;
    const uint8_t* outer_nullmap = nullptr;

    Status build(const TabletColumn& array_column, const ColumnWithTypeAndName& typed_column,
                 size_t num_rows) {
        item_data = nullptr;
        item_nullmap = nullptr;
        outer_nullmap = nullptr;
        item_encoder.encoder = create_column_data_convertor(array_column.get_sub_column(0));

        const IColumn* nested = typed_column.column.get();
        if (const auto* nullable = check_and_get_column<ColumnNullable>(nested)) {
            outer_nullmap = nullable->get_null_map_data().data();
            nested = &nullable->get_nested_column();
        }
        const auto* col_array = check_and_get_column<ColumnArray>(nested);
        if (col_array == nullptr) {
            return Status::InternalError("expected ColumnArray, got {}", nested->get_name());
        }

        const size_t start_offset = col_array->offset_at(0);
        offsets.clear();
        offsets.reserve(num_rows + 1);
        for (size_t i = 0; i <= num_rows; ++i) {
            offsets.push_back(col_array->offset_at(static_cast<ssize_t>(i)) - start_offset);
        }
        if (offsets.back() > 0) {
            RETURN_IF_ERROR(
                    item_encoder.encode(*col_array->get_data_ptr(), start_offset, offsets.back()));
            item_data = item_encoder.scratch.data;
            item_nullmap = item_encoder.scratch.nullmap;
        }
        return Status::OK();
    }

    const uint8_t* offsets_ptr() const { return reinterpret_cast<const uint8_t*>(offsets.data()); }
};

} // namespace doris::segment_v2
