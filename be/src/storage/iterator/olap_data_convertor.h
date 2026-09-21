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

#include "common/status.h"
#include "storage/iterator/column_storage_scratch.h"

namespace doris {

class IColumn;
class TabletColumn;

// Build the encoder for one column. Every holder that needs storage-format
// bytes owns its own encoder: the column writer, IndexBuilder, and the block
// transform stages that encode keys before any writer exists. The encoders
// themselves are private to the factory: a caller picks one by TabletColumn,
// never by name.
ColumnDataConvertorUPtr create_column_data_convertor(const TabletColumn& column);
ColumnDataConvertorUPtr create_agg_state_data_convertor(const TabletColumn& column);

// Turns one column from its compute-layer layout, where a value is a
// `PrimitiveTypeTraits<PrimitiveType>::CppType`, into an array of storage cells,
// where a value is a `CppTypeTraits<FieldType>::CppType`. The two coincide for
// most types -- those encode with zero copies, handing back a pointer into the
// source column -- and the ones that differ are what the subclasses are for.
//
// Immutable: everything an encode() needs arrives as an argument, and
// everything it produces goes into the caller's scratch, so one encoder can
// serve any number of columns of its type at once.
//
// The result stays valid until the next encode() into the same scratch.
class ColumnDataConvertor {
public:
    ColumnDataConvertor() = default;
    virtual ~ColumnDataConvertor() = default;
    ColumnDataConvertor(const ColumnDataConvertor&) = delete;
    ColumnDataConvertor& operator=(const ColumnDataConvertor&) = delete;
    ColumnDataConvertor(ColumnDataConvertor&&) = delete;
    ColumnDataConvertor& operator=(ColumnDataConvertor&&) = delete;

    // Encode rows [row_pos, row_pos + num_rows) into storage format: empties
    // the scratch, peels a Nullable column into scratch.nullmap and the column
    // underneath, then hands that column to encode_nested().
    Status encode(const IColumn& column, size_t row_pos, size_t num_rows,
                  ColumnStorageScratch& scratch) const;

protected:
    // `nested` is the column with its Nullable wrapper peeled off. Its null
    // bits, offset to row_pos, are already in scratch.nullmap (null when the
    // column is not nullable); the rest of the scratch is empty.
    virtual Status encode_nested(const IColumn& nested, size_t row_pos, size_t num_rows,
                                 ColumnStorageScratch& scratch) const = 0;
};

} // namespace doris
