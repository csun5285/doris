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

#include "common/status.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column_array.h"
#include "core/column/column_nullable.h"
#include "storage/iterator/olap_data_convertor.h"
#include "storage/segment/array_index_input.h"
#include "storage/tablet/tablet_schema.h"

namespace doris::segment_v2 {

// Stages one ARRAY column for a test the way ArrayColumnWriter and IndexBuilder
// do: peel the Nullable wrapper, build the item encoder from the schema, and run
// the production stager. The tests then feed the index writers themselves.
struct ArrayIndexFixture {
    ColumnEncoding item_encoding;
    ArrayIndexInput staged;
    const void* item_data = nullptr;
    const uint8_t* item_nullmap = nullptr;
    const uint8_t* outer_nullmap = nullptr;

    Status build(const TabletColumn& array_column, const ColumnWithTypeAndName& typed_column,
                 size_t num_rows) {
        item_encoding.encoder = create_column_data_convertor(array_column.get_sub_column(0));
        const IColumn& nested = peel_nullable(*typed_column.column, 0, &outer_nullmap);
        const auto* col_array = check_and_get_column<ColumnArray>(&nested);
        if (col_array == nullptr) {
            return Status::InternalError("expected ColumnArray, got {}", nested.get_name());
        }
        RETURN_IF_ERROR(
                stage_array_index_input(*col_array, 0, num_rows, 0, &item_encoding, &staged));
        item_data = staged.item_data;
        item_nullmap = staged.item_nullmap;
        return Status::OK();
    }

    const uint8_t* offsets_ptr() const { return staged.offsets_ptr(); }
};

} // namespace doris::segment_v2
