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

#include "storage/iterator/vgeneric_iterators.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <array>
#include <memory>
#include <vector>

#include "core/block/column_with_type_and_name.h"
#include "core/column/column.h"
#include "core/data_type/data_type.h"
#include "core/field.h"
#include "gtest/gtest_pred_impl.h"
#include "storage/dense_read_schema.h"
#include "storage/olap_common.h"
#include "storage/segment/column_reader.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {
using namespace ErrorCode;

class VGenericIteratorsTest : public testing::Test {
public:
    VGenericIteratorsTest() {}
    virtual ~VGenericIteratorsTest() {}
};

static DenseReadSchemaSPtr create_schema(const std::vector<TabletColumnId>& tablet_column_ids = {
                                                 0, 1, 2}) {
    TabletSchema tablet_schema;
    auto c1 = std::make_shared<TabletColumn>(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                                             FieldType::OLAP_FIELD_TYPE_SMALLINT, true);
    c1->set_is_key(true);
    c1->set_unique_id(10);
    c1->set_name("c1");
    tablet_schema.append_column(*c1);
    // c2: int
    auto c2 = std::make_shared<TabletColumn>(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                                             FieldType::OLAP_FIELD_TYPE_INT, true);
    c2->set_is_key(true);
    c2->set_unique_id(11);
    c2->set_name("c2");
    tablet_schema.append_column(*c2);
    // c3: big int
    TabletColumn c3(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_SUM,
                    FieldType::OLAP_FIELD_TYPE_BIGINT, true);
    c3.set_unique_id(12);
    c3.set_name("c3");
    tablet_schema.append_column(c3);

    auto result = DenseReadSchema::create(tablet_schema, tablet_column_ids);
    CHECK(result.has_value()) << result.error().to_string();
    return result.value();
}

static void create_block(const DenseReadSchema& schema, Block& block) {
    block = schema.create_block();
}

TEST(VGenericIteratorsTest, AutoIncrement) {
    auto schema = create_schema();
    auto iter = new_auto_increment_iterator(schema, 10);

    StorageReadOptions opts;
    auto st = iter->init(opts);
    EXPECT_TRUE(st.ok());

    Block block;
    create_block(*schema, block);

    auto ret = iter->next_batch(&block);
    EXPECT_TRUE(ret.ok());
    EXPECT_EQ(block.rows(), 10);

    auto c0 = block.get_by_position(0).column;
    auto c1 = block.get_by_position(1).column;
    auto c2 = block.get_by_position(2).column;

    int row_count = 0;
    size_t rows = block.rows();
    for (size_t i = 0; i < rows; ++i) {
        EXPECT_EQ(row_count, (*c0)[i].get<TYPE_SMALLINT>());
        EXPECT_EQ(row_count + 1, (*c1)[i].get<TYPE_INT>());
        EXPECT_EQ(row_count + 2, (*c2)[i].get<TYPE_BIGINT>());
        row_count++;
    }
}

TEST(VGenericIteratorsTest, Union) {
    auto schema = create_schema();
    auto output_schema = schema;
    std::vector<RowwiseIteratorUPtr> inputs;

    inputs.push_back(new_auto_increment_iterator(schema, 100));
    inputs.push_back(new_auto_increment_iterator(schema, 200));
    inputs.push_back(new_auto_increment_iterator(schema, 300));

    auto iter = new_union_iterator(std::move(inputs), output_schema);
    StorageReadOptions opts;
    auto st = iter->init(opts);
    EXPECT_TRUE(st.ok());

    Block block;
    create_block(*schema, block);

    do {
        st = iter->next_batch(&block);
    } while (st.ok());

    EXPECT_TRUE(st.is<END_OF_FILE>());
    EXPECT_EQ(block.rows(), 600);

    auto c0 = block.get_by_position(0).column;
    auto c1 = block.get_by_position(1).column;
    auto c2 = block.get_by_position(2).column;

    size_t row_count = 0;
    for (int i = 0; i < block.rows(); ++i) {
        size_t base_value = row_count;
        if (row_count >= 300) {
            base_value -= 300;
        } else if (i >= 100) {
            base_value -= 100;
        }

        EXPECT_EQ(base_value, (*c0)[i].get<TYPE_SMALLINT>());
        EXPECT_EQ(base_value + 1, (*c1)[i].get<TYPE_INT>());
        EXPECT_EQ(base_value + 2, (*c2)[i].get<TYPE_BIGINT>());
        row_count++;
    }
}

TEST(VGenericIteratorsTest, ProjectionUsesStableFieldIdentity) {
    auto segment_schema = create_schema();
    auto scan_schema = create_schema({2, 0});
    auto projection_result =
            new_projection_iterator(new_auto_increment_iterator(segment_schema, 10), scan_schema);
    ASSERT_TRUE(projection_result.has_value()) << projection_result.error().to_string();
    auto iter = std::move(projection_result).value();

    StorageReadOptions opts;
    ASSERT_TRUE(iter->init(opts).ok());
    Block block = scan_schema->create_block();
    ASSERT_TRUE(iter->next_batch(&block).ok());
    ASSERT_EQ(10, block.rows());

    const auto& value_column = block.get_by_position(0).column;
    const auto& key_column = block.get_by_position(1).column;
    for (size_t row = 0; row < block.rows(); ++row) {
        EXPECT_EQ(row + 2, (*value_column)[row].get<TYPE_BIGINT>());
        EXPECT_EQ(row, (*key_column)[row].get<TYPE_SMALLINT>());
    }
}

class ReorderedKeyIterator final : public RowwiseIterator {
public:
    ReorderedKeyIterator(DenseReadSchemaSPtr schema,
                         std::vector<std::array<int64_t, 3>> physical_rows)
            : _schema(std::move(schema)), _physical_rows(std::move(physical_rows)) {}

    Status init(const StorageReadOptions& opts) override { return Status::OK(); }

    Status next_batch(Block* block) override {
        if (_finished) {
            return Status::EndOfFile("End of ReorderedKeyIterator");
        }
        auto columns_guard = block->mutate_columns_scoped();
        auto& columns = columns_guard.mutable_columns();
        for (const auto& [k1, k2, value] : _physical_rows) {
            const auto dense_row = std::array<int64_t, 3> {k2, k1, value};
            for (size_t position = 0; position < dense_row.size(); ++position) {
                auto& column = *columns[position];
                if (position == 0) {
                    const auto cell = static_cast<int32_t>(dense_row[position]);
                    column.insert_data(reinterpret_cast<const char*>(&cell), sizeof(cell));
                } else if (position == 1) {
                    const auto cell = static_cast<int16_t>(dense_row[position]);
                    column.insert_data(reinterpret_cast<const char*>(&cell), sizeof(cell));
                } else {
                    const auto cell = dense_row[position];
                    column.insert_data(reinterpret_cast<const char*>(&cell), sizeof(cell));
                }
            }
        }
        _finished = true;
        return Status::OK();
    }

    const DenseReadSchema& schema() const override { return *_schema; }

private:
    DenseReadSchemaSPtr _schema;
    std::vector<std::array<int64_t, 3>> _physical_rows;
    bool _finished = false;
};

TEST(VGenericIteratorsTest, MergeUsesPhysicalKeyOrderForReorderedLayout) {
    auto schema = create_schema({1, 0, 2});
    std::vector<RowwiseIteratorUPtr> inputs;
    inputs.push_back(std::make_unique<ReorderedKeyIterator>(
            schema, std::vector<std::array<int64_t, 3>> {{0, 10, 1}, {1, 0, 2}}));
    inputs.push_back(std::make_unique<ReorderedKeyIterator>(
            schema, std::vector<std::array<int64_t, 3>> {{0, 20, 3}, {1, 5, 4}}));

    auto iter = new_merge_iterator(std::move(inputs), -1, false, false, nullptr, schema);
    StorageReadOptions opts;
    ASSERT_TRUE(iter->init(opts).ok());
    Block block = schema->create_block();
    Status status;
    do {
        status = iter->next_batch(&block);
    } while (status.ok());

    EXPECT_TRUE(status.is<END_OF_FILE>());
    ASSERT_EQ(4, block.rows());
    const auto& k2 = block.get_by_position(0).column;
    const auto& k1 = block.get_by_position(1).column;
    EXPECT_EQ(0, (*k1)[0].get<TYPE_SMALLINT>());
    EXPECT_EQ(10, (*k2)[0].get<TYPE_INT>());
    EXPECT_EQ(0, (*k1)[1].get<TYPE_SMALLINT>());
    EXPECT_EQ(20, (*k2)[1].get<TYPE_INT>());
    EXPECT_EQ(1, (*k1)[2].get<TYPE_SMALLINT>());
    EXPECT_EQ(0, (*k2)[2].get<TYPE_INT>());
    EXPECT_EQ(1, (*k1)[3].get<TYPE_SMALLINT>());
    EXPECT_EQ(5, (*k2)[3].get<TYPE_INT>());
}

TEST(VGenericIteratorsTest, MergeAgg) {
    EXPECT_TRUE(1);
    auto schema = create_schema();
    auto output_schema = schema;
    std::vector<RowwiseIteratorUPtr> inputs;

    inputs.push_back(new_auto_increment_iterator(schema, 100));
    inputs.push_back(new_auto_increment_iterator(schema, 200));
    inputs.push_back(new_auto_increment_iterator(schema, 300));

    auto iter = new_merge_iterator(std::move(inputs), -1, false, false, nullptr, output_schema);
    StorageReadOptions opts;
    auto st = iter->init(opts);
    EXPECT_TRUE(st.ok());

    Block block;
    std::vector<bool> row_is_same;
    BlockWithSameBit block_with_same_bit {.block = &block, .same_bit = row_is_same};
    create_block(*schema, block);

    do {
        st = iter->next_batch(&block_with_same_bit);
    } while (st.ok());

    EXPECT_TRUE(st.is<END_OF_FILE>());
    EXPECT_EQ(block.rows(), 600);

    auto c0 = block.get_by_position(0).column;
    auto c1 = block.get_by_position(1).column;
    auto c2 = block.get_by_position(2).column;

    size_t row_count = 0;
    for (size_t i = 0; i < block.rows(); ++i) {
        size_t base_value = row_count;
        // 100 * 3, 200 * 2, 300
        if (row_count < 300) {
            base_value = row_count / 3;
        } else if (row_count < 500) {
            base_value = (row_count - 300) / 2 + 100;
        } else {
            base_value = row_count - 300;
        }

        EXPECT_EQ(base_value, (*c0)[i].get<TYPE_SMALLINT>());
        EXPECT_EQ(base_value + 1, (*c1)[i].get<TYPE_INT>());
        EXPECT_EQ(base_value + 2, (*c2)[i].get<TYPE_BIGINT>());
        row_count++;
    }
}

TEST(VGenericIteratorsTest, MergeUnique) {
    EXPECT_TRUE(1);
    auto schema = create_schema();
    auto output_schema = schema;
    std::vector<RowwiseIteratorUPtr> inputs;

    inputs.push_back(new_auto_increment_iterator(schema, 100));
    inputs.push_back(new_auto_increment_iterator(schema, 200));
    inputs.push_back(new_auto_increment_iterator(schema, 300));

    auto iter = new_merge_iterator(std::move(inputs), -1, true, false, nullptr, output_schema);
    StorageReadOptions opts;
    auto st = iter->init(opts);
    EXPECT_TRUE(st.ok());

    Block block;
    std::vector<bool> row_is_same;
    BlockWithSameBit block_with_same_bit {.block = &block, .same_bit = row_is_same};
    create_block(*schema, block);

    do {
        st = iter->next_batch(&block_with_same_bit);
    } while (st.ok());

    EXPECT_TRUE(st.is<END_OF_FILE>());
    EXPECT_EQ(block.rows(), 300);

    auto c0 = block.get_by_position(0).column;
    auto c1 = block.get_by_position(1).column;
    auto c2 = block.get_by_position(2).column;

    size_t row_count = 0;
    for (size_t i = 0; i < block.rows(); ++i) {
        size_t base_value = row_count;

        EXPECT_EQ(base_value, (*c0)[i].get<TYPE_SMALLINT>());
        EXPECT_EQ(base_value + 1, (*c1)[i].get<TYPE_INT>());
        EXPECT_EQ(base_value + 2, (*c2)[i].get<TYPE_BIGINT>());
        row_count++;
    }
}

// only used for Seq Column UT
class SeqColumnUtIterator : public RowwiseIterator {
public:
    // Will generate num_rows rows in total
    SeqColumnUtIterator(DenseReadSchemaSPtr schema, size_t num_rows, size_t rows_returned,
                        size_t seq_col_idx, size_t seq_col_rows_returned)
            : _schema(std::move(schema)),
              _num_rows(num_rows),
              _rows_returned(rows_returned),
              _seq_col_idx(seq_col_idx),
              _seq_col_rows_returned(seq_col_rows_returned) {}
    ~SeqColumnUtIterator() override {}

    // NOTE: Currently, this function will ignore StorageReadOptions
    Status init(const StorageReadOptions& opts) override { return Status::OK(); }

    Status next_batch(Block* block) override {
        int row_idx = 0;
        while (_rows_returned < _num_rows) {
            for (int j = 0; j < _schema->num_columns(); ++j) {
                ColumnWithTypeAndName& vc = block->get_by_position(j);
                IColumn& vi = (IColumn&)(*vc.column);

                char data[16] = {};
                size_t data_len = 0;
                const auto* col_schema = _schema->column(j);
                switch (col_schema->type()) {
                case FieldType::OLAP_FIELD_TYPE_SMALLINT:
                    *(int16_t*)data = j == _seq_col_idx ? _seq_col_rows_returned : 1;
                    data_len = sizeof(int16_t);
                    break;
                case FieldType::OLAP_FIELD_TYPE_INT:
                    *(int32_t*)data = j == _seq_col_idx ? _seq_col_rows_returned : 1;
                    data_len = sizeof(int32_t);
                    break;
                case FieldType::OLAP_FIELD_TYPE_BIGINT:
                    *(int64_t*)data = j == _seq_col_idx ? _seq_col_rows_returned : 1;
                    data_len = sizeof(int64_t);
                    break;
                case FieldType::OLAP_FIELD_TYPE_FLOAT:
                    *(float*)data = j == _seq_col_idx ? _seq_col_rows_returned : 1;
                    data_len = sizeof(float);
                    break;
                case FieldType::OLAP_FIELD_TYPE_DOUBLE:
                    *(double*)data = j == _seq_col_idx ? _seq_col_rows_returned : 1;
                    data_len = sizeof(double);
                    break;
                default:
                    break;
                }

                vi.insert_data(data, data_len);
            }

            ++_rows_returned;
            _seq_col_rows_returned++;
            row_idx++;
        }

        if (row_idx > 0) return Status::OK();
        return Status::EndOfFile("End of VAutoIncrementIterator");
    }

    const DenseReadSchema& schema() const override { return *_schema; }

    DenseReadSchemaSPtr _schema;
    size_t _num_rows;
    size_t _rows_returned;
    int _seq_col_idx = -1;
    int _seq_col_rows_returned = -1;
};

TEST(VGenericIteratorsTest, MergeWithSeqColumn) {
    EXPECT_TRUE(1);
    auto schema = create_schema();
    auto output_schema = schema;
    std::vector<RowwiseIteratorUPtr> inputs;

    int seq_column_id = 2;
    int seg_iter_num = 10;
    int num_rows = 1;
    int rows_begin = 0;
    // The same key in each file will only keep one with the largest seq id
    // keep the key columns all the same, but seq column value different
    // input seg file in Ascending,  expect output seq column in Descending
    for (int i = 0; i < seg_iter_num; i++) {
        int seq_id_in_every_file = i;
        inputs.push_back(std::make_unique<SeqColumnUtIterator>(
                schema, num_rows, rows_begin, seq_column_id, seq_id_in_every_file));
    }

    auto iter = new_merge_iterator(std::move(inputs), seq_column_id, true, false, nullptr,
                                   output_schema);
    StorageReadOptions opts;
    auto st = iter->init(opts);
    EXPECT_TRUE(st.ok());

    Block block;
    std::vector<bool> row_is_same;
    BlockWithSameBit block_with_same_bit {.block = &block, .same_bit = row_is_same};
    create_block(*schema, block);

    do {
        st = iter->next_batch(&block_with_same_bit);
    } while (st.ok());

    EXPECT_TRUE(st.is<END_OF_FILE>());
    EXPECT_EQ(block.rows(), 1);

    auto col0 = block.get_by_position(0).column;
    auto col1 = block.get_by_position(1).column;
    auto seq_col = block.get_by_position(seq_column_id).column;
    size_t actual_value = (*seq_col)[0].get<TYPE_BIGINT>();
    EXPECT_EQ(seg_iter_num - 1, actual_value);
}

// Mirror of MergeWithSeqColumn but with small_seq_first=true.
// Same key across all segments, seq values are 0..seg_iter_num-1; the merge
// iterator should keep exactly one row whose seq value is the smallest (0).
TEST(VGenericIteratorsTest, MergeWithSeqColumnSmallSeqFirst) {
    auto schema = create_schema();
    auto output_schema = schema;
    std::vector<RowwiseIteratorUPtr> inputs;

    int seq_column_id = 2;
    int seg_iter_num = 10;
    int num_rows = 1;
    int rows_begin = 0;
    for (int i = 0; i < seg_iter_num; i++) {
        int seq_id_in_every_file = i;
        inputs.push_back(std::make_unique<SeqColumnUtIterator>(
                schema, num_rows, rows_begin, seq_column_id, seq_id_in_every_file));
    }

    // small_seq_first = true => smaller seq value sorts first / wins on tie.
    auto iter = new_merge_iterator(std::move(inputs), seq_column_id, /*is_unique=*/true,
                                   /*is_reverse=*/false, /*merged_rows=*/nullptr, output_schema,
                                   /*small_seq_first=*/true);
    StorageReadOptions opts;
    auto st = iter->init(opts);
    EXPECT_TRUE(st.ok());

    Block block;
    std::vector<bool> row_is_same;
    BlockWithSameBit block_with_same_bit {.block = &block, .same_bit = row_is_same};
    create_block(*schema, block);

    do {
        st = iter->next_batch(&block_with_same_bit);
    } while (st.ok());

    EXPECT_TRUE(st.is<END_OF_FILE>());
    EXPECT_EQ(block.rows(), 1);

    auto seq_col = block.get_by_position(seq_column_id).column;
    size_t actual_value = (*seq_col)[0].get<TYPE_BIGINT>();
    EXPECT_EQ(0, actual_value);
}

} // namespace doris
