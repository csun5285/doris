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
#include <stddef.h>

#include <string>

#include "core/column/column.h"
#include "core/column/column_string.h"
#include "core/string_ref.h"
#include "gtest/gtest_pred_impl.h"
#include "storage/iterator/olap_data_convertor.h"
#include "util/slice.h"

namespace doris {

namespace {

const std::string kStr = "Allemande"; // NOLINT(runtime/string)

// The storage cells CharDataConvertor produced, read back off the scratch.
const Slice* cells_of(const ColumnStorageScratch& scratch) {
    return reinterpret_cast<const Slice*>(scratch.data);
}

} // namespace

// A column whose rows are all exactly the declared width -- what reading a
// segment back gives, since CHAR is stored padded. Nothing may be copied: the
// cells point straight into the source column.
TEST(CharTypePaddingTest, AlreadyPaddedRowsAreNotCopied) {
    auto input = ColumnString::create();
    constexpr size_t kRows = 10;
    for (size_t i = 0; i < kRows; i++) {
        input->insert_data(kStr.data(), kStr.length());
    }

    CharDataConvertor convertor(kStr.length());
    ColumnStorageScratch scratch;
    ASSERT_TRUE(convertor.encode(*input, 0, kRows, scratch).ok());

    const Slice* cells = cells_of(scratch);
    for (size_t i = 0; i < kRows; i++) {
        EXPECT_EQ(cells[i].size, kStr.length());
        EXPECT_EQ(cells[i].data, input->get_data_at(i).data) << "row " << i << " was copied";
    }
    EXPECT_TRUE(scratch.bytes.empty());
}

// Short rows are padded out to the declared width with zeroes, which is the
// on-disk layout the reader's strnlen() undoes.
TEST(CharTypePaddingTest, ShortRowsArePaddedWithZeroes) {
    auto input = ColumnString::create();
    const size_t rows = kStr.length();
    for (size_t i = 0; i < rows; i++) {
        input->insert_data(kStr.data(), kStr.length() - i);
    }

    CharDataConvertor convertor(kStr.length());
    ColumnStorageScratch scratch;
    ASSERT_TRUE(convertor.encode(*input, 0, rows, scratch).ok());

    const Slice* cells = cells_of(scratch);
    for (size_t i = 0; i < rows; i++) {
        ASSERT_EQ(cells[i].size, kStr.length()) << "row " << i;
        const std::string cell(cells[i].data, cells[i].size);
        const size_t kept = kStr.length() - i;
        EXPECT_EQ(cell.substr(0, kept), kStr.substr(0, kept));
        for (size_t j = kept; j < kStr.length(); j++) {
            EXPECT_EQ(cell[j], '\0') << "row " << i << " byte " << j;
        }
    }
}

} // namespace doris
