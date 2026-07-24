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

#if defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wkeyword-macro"
#endif
#define private public
#define protected public
#include "storage/iterator/block_reader.h"
#include "storage/iterator/vertical_block_reader.h"
#undef private
#undef protected
#if defined(__clang__)
#pragma clang diagnostic pop
#endif

#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "core/assert_cast.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "storage/binlog.h"
#include "storage/dense_read_schema.h"
#include "storage/iterator/binlog_block_reader_utils.h"
#include "storage/iterator/vertical_merge_iterator.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {
namespace {

TabletColumn make_column(int32_t unique_id, std::string name, FieldType type, bool is_key,
                         bool is_nullable) {
    TabletColumn column;
    column.set_unique_id(unique_id);
    column.set_name(std::move(name));
    column.set_type(type);
    column.set_is_key(is_key);
    column.set_is_nullable(is_nullable);
    column.set_length(type == FieldType::OLAP_FIELD_TYPE_BIGINT ? sizeof(int64_t)
                                                                : sizeof(int32_t));
    column.set_index_length(column.length());
    column.set_aggregation_method(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE);
    return column;
}

TabletSchemaSPtr make_binlog_schema() {
    auto schema = std::make_shared<TabletSchema>();
    schema->append_column(make_column(1, "k", FieldType::OLAP_FIELD_TYPE_INT, true, false));
    schema->append_column(make_column(2, "v", FieldType::OLAP_FIELD_TYPE_INT, false, false));
    schema->append_column(make_column(3, binlog::build_before_column_name("v"),
                                      FieldType::OLAP_FIELD_TYPE_INT, false, true));
    schema->append_column(make_column(4, BINLOG_OP_COL,
                                      FieldType::OLAP_FIELD_TYPE_BIGINT, false, false));
    return schema;
}

TabletSchemaSPtr make_unique_sequence_schema() {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(UNIQUE_KEYS);
    schema_pb.set_num_short_key_columns(1);
    schema_pb.set_num_rows_per_row_block(1024);
    schema_pb.set_compress_kind(COMPRESS_NONE);
    schema_pb.set_next_column_unique_id(6);
    schema_pb.set_sequence_col_idx(3);

    auto add_column = [&](int32_t unique_id, const std::string& name, const std::string& type,
                          bool is_key, int32_t length) {
        auto* column = schema_pb.add_column();
        column->set_unique_id(unique_id);
        column->set_name(name);
        column->set_type(type);
        column->set_is_key(is_key);
        column->set_is_nullable(false);
        column->set_length(length);
        column->set_index_length(length);
        column->set_is_bf_column(false);
        column->set_aggregation("NONE");
    };
    add_column(1, "k", "INT", true, sizeof(int32_t));
    add_column(2, "v1", "INT", false, sizeof(int32_t));
    add_column(3, "v2", "INT", false, sizeof(int32_t));
    add_column(4, SEQUENCE_COL, "INT", false, sizeof(int32_t));
    add_column(5, DELETE_SIGN, "TINYINT", false, sizeof(int8_t));

    auto schema = std::make_shared<TabletSchema>();
    schema->init_from_pb(schema_pb);
    return schema;
}

class SingleSequenceRowIterator final : public RowwiseIterator {
public:
    SingleSequenceRowIterator(DenseReadSchemaSPtr schema, int32_t key, int32_t sequence)
            : _schema(std::move(schema)), _key(key), _sequence(sequence) {}

    Status init(const StorageReadOptions&) override { return Status::OK(); }

    Status next_batch(Block* block) override {
        if (_returned) {
            return Status::EndOfFile("single sequence row returned");
        }
        auto columns_guard = block->mutate_columns_scoped();
        auto& columns = columns_guard.mutable_columns();
        columns[0]->insert_data(reinterpret_cast<const char*>(&_key), sizeof(_key));
        columns[1]->insert_data(reinterpret_cast<const char*>(&_sequence), sizeof(_sequence));
        int8_t delete_sign = 0;
        columns[2]->insert_data(reinterpret_cast<const char*>(&delete_sign), sizeof(delete_sign));
        _returned = true;
        return Status::OK();
    }

    const DenseReadSchema& schema() const override { return *_schema; }

private:
    DenseReadSchemaSPtr _schema;
    int32_t _key;
    int32_t _sequence;
    bool _returned = false;
};

void append_source_row(Block& block, int32_t key, int32_t value, int32_t before_value,
                       bool before_is_null) {
    auto columns_guard = block.mutate_columns_scoped();
    auto& columns = columns_guard.mutable_columns();
    assert_cast<ColumnInt32&>(*columns[0]).insert_value(key);
    assert_cast<ColumnInt32&>(*columns[1]).insert_value(value);
    auto& before_column = assert_cast<ColumnNullable&>(*columns[2]);
    assert_cast<ColumnInt32&>(before_column.get_nested_column()).insert_value(before_value);
    before_column.get_null_map_data().push_back(before_is_null);
    assert_cast<ColumnInt64&>(*columns[3]).insert_value(ROW_BINLOG_UPDATE);
}

void configure_reader(BlockReader& reader, const TabletSchemaSPtr& tablet_schema,
                      TBinlogScanType::type scan_type) {
    reader._tablet_schema = tablet_schema;
    auto read_schema = DenseReadSchema::create(*tablet_schema, {0, 1, 2, 3});
    ASSERT_TRUE(read_schema.has_value()) << read_schema.error().to_string();
    reader._read_schema = read_schema.value();
    TabletReader::ReaderParams params;
    params.binlog_scan_type = scan_type;
    ASSERT_TRUE(reader._init_read_positions(params).ok());
}

int32_t int32_value(const IColumn& column, size_t row) {
    return assert_cast<const ColumnInt32&>(column).get_data()[row];
}

int64_t int64_value(const IColumn& column, size_t row) {
    return assert_cast<const ColumnInt64&>(column).get_data()[row];
}

} // namespace

TEST(BlockReaderBinlogCopyTest, DetailUpdateBeforeUnwrapsNullableBeforeImage) {
    auto tablet_schema = make_binlog_schema();
    BlockReader reader;
    configure_reader(reader, tablet_schema, TBinlogScanType::DETAIL);
    ASSERT_EQ(reader._before_source_positions[1], 2);
    ASSERT_EQ(reader._before_copy_modes[1],
              BlockReader::BinlogColumnCopyMode::UNWRAP_NULLABLE_SOURCE);

    Block source = reader._read_schema->create_block();
    append_source_row(source, 11, 22, 7, false);
    Block target = reader._read_schema->create_block();
    {
        auto target_guard = target.mutate_columns_scoped();
        ASSERT_TRUE(reader._append_change_row(target_guard.mutable_columns(), source, 0,
                                              binlog::STREAM_CHANGE_UPDATE_BEFORE, true)
                            .ok());
    }

    ASSERT_EQ(target.rows(), 1);
    EXPECT_EQ(int32_value(*target.get_by_position(0).column, 0), 11);
    EXPECT_EQ(int32_value(*target.get_by_position(1).column, 0), 7);
    EXPECT_EQ(int64_value(*target.get_by_position(3).column, 0),
              binlog::STREAM_CHANGE_UPDATE_BEFORE);
}

TEST(BlockReaderBinlogCopyTest, MinDeltaUpdateBeforeUsesSameUnwrapPlan) {
    auto tablet_schema = make_binlog_schema();
    BlockReader reader;
    configure_reader(reader, tablet_schema, TBinlogScanType::MIN_DELTA);
    Block source = reader._read_schema->create_block();
    append_source_row(source, 11, 22, 7, false);
    reader._stored_data_columns = std::move(source).mutate_columns();
    Block target = reader._read_schema->create_block();

    {
        auto target_guard = target.mutate_columns_scoped();
        ASSERT_TRUE(reader._append_min_delta_update_before(target_guard.mutable_columns(),
                                                           /*group_size=*/1)
                            .ok());
    }

    ASSERT_EQ(target.rows(), 1);
    EXPECT_EQ(int32_value(*target.get_by_position(0).column, 0), 11);
    EXPECT_EQ(int32_value(*target.get_by_position(1).column, 0), 7);
    EXPECT_EQ(int64_value(*target.get_by_position(3).column, 0),
              binlog::STREAM_CHANGE_UPDATE_BEFORE);
}

TEST(BlockReaderBinlogCopyTest, NullableBeforeNullFailsForNonNullableOutput) {
    auto tablet_schema = make_binlog_schema();
    BlockReader reader;
    configure_reader(reader, tablet_schema, TBinlogScanType::DETAIL);
    Block source = reader._read_schema->create_block();
    append_source_row(source, 11, 22, 0, true);
    Block target = reader._read_schema->create_block();

    Status status;
    {
        auto target_guard = target.mutate_columns_scoped();
        status = reader._append_change_row(target_guard.mutable_columns(), source, 0,
                                           binlog::STREAM_CHANGE_UPDATE_BEFORE, true);
    }
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("is NULL for non-nullable output field v"),
              std::string::npos);
    EXPECT_EQ(target.rows(), 0);
}

TEST(BlockReaderBinlogCopyTest, WrapsNonNullableSourceForNullableOutput) {
    const auto target_type = make_nullable(std::make_shared<DataTypeInt32>());
    const auto source_type = std::make_shared<DataTypeInt32>();
    auto copy_mode = BlockReader::_plan_binlog_column_copy(target_type, source_type);
    ASSERT_TRUE(copy_mode.has_value()) << copy_mode.error().to_string();
    ASSERT_EQ(copy_mode.value(), BlockReader::BinlogColumnCopyMode::WRAP_NON_NULL_SOURCE);

    auto tablet_schema = make_binlog_schema();
    BlockReader reader;
    configure_reader(reader, tablet_schema, TBinlogScanType::DETAIL);
    auto target = target_type->create_column();
    auto source = source_type->create_column();
    assert_cast<ColumnInt32&>(*source).insert_value(9);
    ASSERT_TRUE(reader._append_binlog_column(*target, *source, 0, 2, 1, copy_mode.value()).ok());

    const auto& nullable_target = assert_cast<const ColumnNullable&>(*target);
    ASSERT_EQ(nullable_target.size(), 1);
    EXPECT_FALSE(nullable_target.is_null_at(0));
    EXPECT_EQ(int32_value(nullable_target.get_nested_column(), 0), 9);
}

TEST(BlockReaderDenseLayoutTest, RejectsSameWidthBlockWithDifferentLayout) {
    auto tablet_schema = make_binlog_schema();
    BlockReader reader;
    configure_reader(reader, tablet_schema, TBinlogScanType::DETAIL);
    Block block = reader._read_schema->create_block();
    block.get_by_position(0).name = "wrong_key";
    bool eof = false;

    const auto status = reader.next_block_with_aggregation(&block, &eof);

    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("does not match dense read schema"), std::string::npos);
}

TEST(VerticalBlockReaderDenseLayoutTest, RejectsSameWidthBlockWithDifferentLayout) {
    auto tablet_schema = make_binlog_schema();
    auto read_schema = DenseReadSchema::create(*tablet_schema, {0, 1, 2, 3});
    ASSERT_TRUE(read_schema.has_value()) << read_schema.error().to_string();
    VerticalBlockReader reader(nullptr);
    reader._read_schema = read_schema.value();
    Block block = reader._read_schema->create_block();
    block.get_by_position(0).name = "wrong_key";
    bool eof = false;

    const auto status = reader.next_block_with_aggregation(&block, &eof);

    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("does not match dense read schema"), std::string::npos);
}

TEST(VerticalBlockReaderDenseLayoutTest, UsesDensePositionForNonAdjacentSequenceColumn) {
    auto tablet_schema = make_unique_sequence_schema();
    auto read_schema = DenseReadSchema::create(*tablet_schema, {0, 3, 4});
    ASSERT_TRUE(read_schema.has_value()) << read_schema.error().to_string();
    ASSERT_EQ(read_schema.value()->sequence_col_idx(), 1);
    ASSERT_EQ(tablet_schema->sequence_col_idx(), 3);

    std::vector<RowwiseIteratorUPtr> inputs;
    inputs.push_back(std::make_unique<SingleSequenceRowIterator>(read_schema.value(), 7, 10));
    inputs.push_back(std::make_unique<SingleSequenceRowIterator>(read_schema.value(), 7, 20));

    RowSourcesBuffer row_sources(1, "/tmp", ReaderType::READER_BASE_COMPACTION);
    VerticalBlockReader reader(&row_sources);
    reader._tablet_schema = tablet_schema;
    reader._read_schema = read_schema.value();

    TabletReader::ReaderParams params;
    params.reader_type = ReaderType::READER_BASE_COMPACTION;
    params.is_key_column_group = true;
    params.segment_iters_ptr = &inputs;
    params.batch_size = 16;
    ASSERT_TRUE(reader._init_collect_iter(params, nullptr).ok());

    Block output = read_schema.value()->create_block();
    const auto status = reader._vcollect_iter->next_batch(&output);
    EXPECT_TRUE(status.is<ErrorCode::END_OF_FILE>()) << status;
    ASSERT_EQ(output.rows(), 1);
    EXPECT_EQ(output.get_by_position(0).column->get_int(0), 7);
    EXPECT_EQ(output.get_by_position(1).column->get_int(0), 20);
}

} // namespace doris
