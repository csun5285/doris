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
#include <string>
#include <vector>

#include "common/status.h"
#include "core/column/column.h"
#include "util/slice.h"

namespace doris {

class KeyCoder;

// StorageView — the result of an IColumn::storage_view() call.
//
// Carries the storage-format byte view of a [row_pos, row_pos+num_rows) slice,
// plus any owned scratch buffers the column needed to materialize it. Two
// physical shapes share one struct (the consumer knows which by FieldType):
//
//   - Fixed-width rows: `data` points at a contiguous byte array, row i sits at
//     `data + i * row_size`. Used for numeric / IP / date / decimal / repack
//     types. `row_size` is the storage-format row width.
//
//   - Slice array: `data` points at a `Slice[num_rows]`, row i sits at
//     `((const Slice*)data)[i]`. Used for string-family and object-family
//     columns. `row_size == sizeof(Slice)`.
//
// `nullmap` is pre-offset to the slice's first row (caller does not add
// row_pos). Null entries always still occupy a slot in `data`; null slices
// hold `{nullptr, 0}`.
//
// The owned buffers (`repack_buf` / `slice_buf` / `object_serialize_buf` /
// `padded_string`) are reused across calls — the caller keeps one StorageView
// alive across appends, and each `IColumn::storage_view()` invocation refills
// only the buffer it uses for the current column type.
struct StorageView {
    const uint8_t* nullmap = nullptr;
    const uint8_t* data = nullptr;
    size_t row_size = 0;
    size_t num_rows = 0;
    // True when `data` points at a Slice[num_rows] array (string / object
    // family); false for fixed-width rows. Set by every producer so consumers
    // never have to infer the shape from row_size (16-byte fixed types would
    // collide with sizeof(Slice)).
    bool is_slices = false;

    // Owned scratch buffers. Pointers in `data` / Slice contents may reference
    // these — keep the StorageView alive while consuming `data`.
    std::vector<uint8_t> repack_buf;
    std::vector<Slice> slice_buf;
    std::vector<char> object_serialize_buf;
    ColumnPtr padded_string;
};

namespace segment_v2 {

// KeyEncodingTarget: a (KeyCoder, StorageView) pair that carries everything
// needed to encode one column's keys from already-staged storage bytes.
// Callers populate the two pointers — typically from ScalarColumnWriter::view()
// (after append) and a coder cached from the schema — and pass a vector of
// these to SegmentWriter::build_key_index().
struct KeyEncodingTarget {
    const KeyCoder* coder = nullptr;
    const StorageView* view = nullptr;
};

// Free helpers that consume a StorageView for cross-row work (key encoding,
// null checking). Use from any caller (BlockAggregator / IndexBuilder /
// SegmentWriter / etc.) without owning extra state.
bool storage_view_is_null_at(const StorageView& view, size_t row_offset);

// Encode the row at `row_offset` (relative to the view's staged slice) into
// `buf` using the schema's KeyCoder. Null rows write a single
// KEY_NULL_FIRST_MARKER byte.
Status storage_view_encode_full_key_ascending(const KeyCoder* coder, const StorageView& view,
                                               size_t row_offset, std::string* buf);
Status storage_view_encode_short_key_ascending(const KeyCoder* coder, const StorageView& view,
                                                size_t row_offset, std::string* buf,
                                                size_t index_size);

} // namespace segment_v2

} // namespace doris
