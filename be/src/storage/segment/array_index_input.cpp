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

#include "storage/segment/array_index_input.h"

#include "common/cast_set.h"
#include "common/exception.h"
#include "core/column/column_nullable.h"
#include "storage/index/index_writer.h"

namespace doris::segment_v2 {

namespace {

// The storage cells of n array elements from `first` on.
const uint8_t* index_values(FieldType type, const IColumn& column, size_t first, size_t n,
                            const uint8_t* null_map, StorageBuffers& buffers, size_t char_length) {
    if (n == 0) {
        // Empty arrays still advance document IDs. The raw BKD interface requires
        // a non-null value pointer even when none of these rows has an element.
        buffers.bytes.resize(field_type_size(type));
        return reinterpret_cast<const uint8_t*>(buffers.bytes.data());
    }
    // Load passes unpadded execution values; BUILD INDEX passes already padded items.
    if (type == FieldType::OLAP_FIELD_TYPE_CHAR && char_length != 0) {
        return StorageLayout<FieldType::OLAP_FIELD_TYPE_CHAR>::column_to_storage(
                column, first, n, char_length, buffers, null_map);
    }
    if (field_is_slice_type(type)) {
        return storage_layout_detail::Bytes::column_to_storage(column, first, n, buffers, null_map);
    }
    switch (type) {
#define CASE(FT)        \
    case FieldType::FT: \
        return StorageLayout<FieldType::FT>::column_to_storage(column, first, n, buffers, null_map);
        DORIS_APPLY_FOR_FIXED_WIDTH_STORAGE_LAYOUT_TYPES(CASE)
#undef CASE
    default:
        throw Exception(ErrorCode::INTERNAL_ERROR, "No array index storage layout for type {}",
                        static_cast<int>(type));
    }
}

} // namespace

size_t rebase_offsets(const IColumn::Offsets64& offsets, size_t row_pos, size_t num_rows,
                      uint64_t base, ColumnOffset64* out) {
    // Row i starts at offsets[i - 1]; offsets[-1] is 0 by the array's padding.
    const auto start_of = [&](size_t row) {
        return offsets[cast_set<ssize_t, size_t, false>(row) - 1];
    };
    const uint64_t first = start_of(row_pos);
    auto& data = out->get_data();
    data.clear();
    data.reserve(num_rows + 1);
    for (size_t i = 0; i <= num_rows; ++i) {
        data.push_back(start_of(row_pos + i) - first + base);
    }
    return data.back() - base;
}

Status feed_array_index(IndexColumnWriter* writer, FieldType item_type, const IColumn& items,
                        size_t first_item, const ColumnOffset64& offsets,
                        const uint8_t* outer_nullmap, StorageBuffers& buffers, size_t char_length) {
    RETURN_IF_CATCH_EXCEPTION({
        DCHECK_GE(offsets.size(), 1);
        const size_t num_rows = offsets.size() - 1;
        const auto* row_offsets = offsets.get_data().data();
        const size_t num_items = row_offsets[num_rows] - row_offsets[0];
        const uint8_t* null_map = nullptr;
        const IColumn& nested = peel_nullable(items, first_item, &null_map);
        const uint8_t* values = index_values(item_type, nested, first_item, num_items, null_map,
                                             buffers, char_length);
        RETURN_IF_ERROR(writer->add_array_values(field_type_size(item_type), values, null_map,
                                                 row_offsets, num_rows));
        if (outer_nullmap != nullptr) {
            RETURN_IF_ERROR(writer->add_array_nulls(outer_nullmap, num_rows));
        }
        return Status::OK();
    });
}

} // namespace doris::segment_v2
