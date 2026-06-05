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

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <string>

#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/data_type/data_type_jsonb.h"
#include "core/data_type_serde/data_type_serde.h"
#include "core/string_ref.h"
#include "gtest/gtest_pred_impl.h"
#include "storage/segment/storage_view.h"
#include "storage/tablet/tablet_schema.h"
#include "util/slice.h"

namespace doris {

// Validates that a JSONB column built via serde stages cleanly through the
// segment-storage path. JSONB binary validation moved upstream to the FE cast
// / stream load sink, so storage no longer rejects bad bytes — we exercise
// the post-serde happy path here.
TEST(JsonbValueConvertorTest, JsonbValueValid) {
    auto input = ColumnString::create();
    auto dataTypeJsonb = std::make_shared<DataTypeJsonb>();
    auto serde = dataTypeJsonb->get_serde();
    DataTypeSerDe::FormatOptions options;

    std::vector<std::string> jsons = {
            R"({"key1": "value1"})", R"({"key2": 12345})", R"({"key3": true})",
            R"({"key4": [1, 2, 3]})", R"({"key5": {"subkey": "subvalue"}})"};
    for (const auto& s : jsons) {
        Slice sl(s.data(), s.length());
        ASSERT_TRUE(serde->deserialize_one_cell_from_json(*input, sl, options).ok());
    }
    ASSERT_EQ(input->size(), jsons.size());

    // Stage as JSONB through IColumn::storage_view (same path as
    // ScalarColumnWriter for FieldType::OLAP_FIELD_TYPE_JSONB).
    TabletColumn jsonb_column;
    jsonb_column.set_type(FieldType::OLAP_FIELD_TYPE_JSONB);
    StorageView view;
    ASSERT_TRUE(input->storage_view(jsonb_column, /*row_pos=*/0,
                                     /*num_rows=*/input->size(), &view)
                        .ok());
    EXPECT_EQ(view.num_rows, jsons.size());
    EXPECT_NE(view.data, nullptr);

    // Nullable variant: a null row sprinkled in.
    auto nullable_col = ColumnNullable::create(ColumnString::create(), ColumnUInt8::create());
    auto nullable_dataTypeJsonb = make_nullable(std::make_shared<DataTypeJsonb>());
    auto serde1 = nullable_dataTypeJsonb->get_serde();

    Slice sl0(jsons[0].data(), jsons[0].length());
    ASSERT_TRUE(serde1->deserialize_one_cell_from_json(*nullable_col, sl0, options).ok());
    nullable_col->insert_default(); // null
    Slice sl1(jsons[1].data(), jsons[1].length());
    ASSERT_TRUE(serde1->deserialize_one_cell_from_json(*nullable_col, sl1, options).ok());
    Slice slice_null = "NULL";
    ASSERT_TRUE(serde1->deserialize_one_cell_from_json(*nullable_col, slice_null, options).ok());
    Slice sl2(jsons[2].data(), jsons[2].length());
    ASSERT_TRUE(serde1->deserialize_one_cell_from_json(*nullable_col, sl2, options).ok());

    StorageView view2;
    ASSERT_TRUE(nullable_col->storage_view(jsonb_column, 0, nullable_col->size(), &view2).ok());
    EXPECT_EQ(view2.num_rows, nullable_col->size());
    EXPECT_TRUE(segment_v2::storage_view_is_null_at(view2, 1)); // the inserted default
    EXPECT_TRUE(segment_v2::storage_view_is_null_at(view2, 3)); // the parsed-as-null "NULL"
}

} // namespace doris
