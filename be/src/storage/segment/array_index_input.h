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

#include <cstddef>
#include <cstdint>
#include <vector>

#include "common/status.h"
#include "core/column/column.h"
#include "storage/iterator/column_storage_scratch.h"

namespace doris {
class ColumnArray;

namespace segment_v2 {
class IndexColumnWriter;

// One batch of ARRAY rows in the shape the array index writers read: num_rows + 1
// offsets rebased to the segment, the item cells and the item null map. Staged
// by stage_array_index_input() and fed by feed_array_index(), so that
// ArrayColumnWriter and IndexBuilder produce the same shape and feed it by the
// same rule.
struct ArrayIndexInput {
    // offsets[i + 1] - offsets[i] is row i's element count; offsets[0] is the
    // base the batch was rebased to.
    std::vector<uint64_t> offsets;
    size_t element_cnt = 0;
    // The item cells and their null map, from the item encoder's scratch. Null
    // when the batch has no elements.
    const void* item_data = nullptr;
    const uint8_t* item_nullmap = nullptr;

    const uint8_t* offsets_ptr() const { return reinterpret_cast<const uint8_t*>(offsets.data()); }
};

// Rewrites the block-absolute offsets of rows [row_pos, row_pos + num_rows) so
// the batch starts at `base`, num_rows + 1 entries into `out`, and returns the
// batch's element count. Disk offsets restart at 0 per segment, so a writer
// passes the element count it has written so far; a consumer that only reads
// element counts passes 0.
size_t rebase_offsets(const IColumn::Offsets64& offsets, size_t row_pos, size_t num_rows,
                      uint64_t base, std::vector<uint64_t>* out);

// Stages rows [row_pos, row_pos + num_rows) of `array`, already peeled of its
// Nullable wrapper: offsets rebased to `base`, items encoded through
// `item_encoding` -- the encoder of the array's item column -- when the batch
// has any. A null `item_encoding` stages the offsets only.
Status stage_array_index_input(const ColumnArray& array, size_t row_pos, size_t num_rows,
                               uint64_t base, ColumnEncoding* item_encoding, ArrayIndexInput* out);

// Feeds one staged batch to an index writer. add_array_values() walks every
// row, an empty batch included, because that is what advances the writer's row
// counter; add_array_nulls() then marks the rows the array-level null map says
// are NULL, so it is skipped when there is none.
Status feed_array_index(IndexColumnWriter* writer, size_t item_cell_size,
                        const ArrayIndexInput& input, size_t num_rows,
                        const uint8_t* outer_nullmap);

} // namespace segment_v2
} // namespace doris
