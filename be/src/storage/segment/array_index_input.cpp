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
#include "core/column/column_array.h"
#include "storage/index/index_writer.h"

namespace doris::segment_v2 {

size_t rebase_offsets(const IColumn::Offsets64& offsets, size_t row_pos, size_t num_rows,
                      uint64_t base, std::vector<uint64_t>* out) {
    // Row i starts at offsets[i - 1]; offsets[-1] is 0 by the array's padding.
    const auto start_of = [&](size_t row) {
        return offsets[cast_set<ssize_t, size_t, false>(row) - 1];
    };
    const uint64_t first = start_of(row_pos);
    out->clear();
    out->reserve(num_rows + 1);
    for (size_t i = 0; i <= num_rows; ++i) {
        out->push_back(start_of(row_pos + i) - first + base);
    }
    return out->back() - base;
}

Status stage_array_index_input(const ColumnArray& array, size_t row_pos, size_t num_rows,
                               uint64_t base, ColumnEncoding* item_encoding, ArrayIndexInput* out) {
    out->element_cnt = rebase_offsets(array.get_offsets(), row_pos, num_rows, base, &out->offsets);
    out->item_data = nullptr;
    out->item_nullmap = nullptr;
    if (item_encoding == nullptr || out->element_cnt == 0) {
        return Status::OK();
    }
    const size_t first_item = array.offset_at(cast_set<ssize_t, size_t, false>(row_pos));
    RETURN_IF_ERROR(item_encoding->encode(*array.get_data_ptr(), first_item, out->element_cnt));
    out->item_data = item_encoding->scratch.data;
    out->item_nullmap = item_encoding->scratch.nullmap;
    return Status::OK();
}

Status feed_array_index(IndexColumnWriter* writer, size_t item_cell_size,
                        const ArrayIndexInput& input, size_t num_rows,
                        const uint8_t* outer_nullmap) {
    // A batch without elements has no first cell. The writers only read cells
    // inside a row's offsets, but some assert a non-null base pointer, so hand
    // them one that is never dereferenced.
    static constexpr uint8_t kNoCells[1] = {0};
    const void* cells = input.item_data != nullptr ? input.item_data : kNoCells;
    RETURN_IF_ERROR(writer->add_array_values(item_cell_size, cells, input.item_nullmap,
                                             input.offsets_ptr(), num_rows));
    if (outer_nullmap != nullptr) {
        RETURN_IF_ERROR(writer->add_array_nulls(outer_nullmap, num_rows));
    }
    return Status::OK();
}

} // namespace doris::segment_v2
