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

#include "storage/dense_read_schema.h"

#include <gtest/gtest.h>

#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/consts.h"
#include "core/column/column_nothing.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_struct.h"
#include "storage/binlog.h"
#include "storage/tablet/tablet_schema.h"
#include "util/json/path_in_data.h"

namespace doris {
namespace {

TabletColumn create_int_column(int32_t unique_id, std::string name, bool is_key,
                               bool is_nullable = false) {
    TabletColumn column;
    column.set_unique_id(unique_id);
    column.set_name(std::move(name));
    column.set_type(FieldType::OLAP_FIELD_TYPE_INT);
    column.set_is_key(is_key);
    column.set_is_nullable(is_nullable);
    column.set_length(sizeof(int32_t));
    column.set_index_length(sizeof(int32_t));
    column.set_aggregation_method(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE);
    return column;
}

void init_tablet_schema(TabletSchema& tablet_schema) {
    tablet_schema.append_column(create_int_column(10, "k0", true));
    tablet_schema.append_column(create_int_column(11, "k1", true));
    tablet_schema.append_column(create_int_column(12, "v0", false));
}

TEST(DenseReadSchemaTest, PreservesDenseOrderAndBlockTypes) {
    TabletSchema tablet_schema;
    init_tablet_schema(tablet_schema);
    std::unordered_set<TabletColumnId> nullable_columns {0};

    auto result = DenseReadSchema::create(tablet_schema, {2, 0}, &nullable_columns);
    ASSERT_TRUE(result.has_value()) << result.error().to_string();
    DenseReadSchemaSPtr read_schema = result.value();

    ASSERT_EQ(2, read_schema->size());
    ASSERT_EQ(1, read_schema->key_positions().size());
    EXPECT_EQ(1, read_schema->key_positions()[0].value());
    EXPECT_EQ((std::vector<TabletColumnId> {2, 0}), read_schema->tablet_column_ids());

    EXPECT_EQ(2, read_schema->field(ReadPosition(0)).tablet_cid);
    EXPECT_EQ("v0", read_schema->field(ReadPosition(0)).block_name);
    EXPECT_EQ("INT", read_schema->field(ReadPosition(0)).block_type->get_name());
    EXPECT_EQ(&tablet_schema.column(2), read_schema->field(ReadPosition(0)).storage_column.get());

    EXPECT_EQ(0, read_schema->field(ReadPosition(1)).tablet_cid);
    EXPECT_EQ("k0", read_schema->field(ReadPosition(1)).block_name);
    EXPECT_EQ("Nullable(INT)", read_schema->field(ReadPosition(1)).block_type->get_name());

    auto value_position = read_schema->position_of_tablet_cid(2);
    ASSERT_TRUE(value_position.has_value());
    EXPECT_EQ(0, value_position->value());
    auto key_position = read_schema->position_of_tablet_cid(0);
    ASSERT_TRUE(key_position.has_value());
    EXPECT_EQ(1, key_position->value());
    EXPECT_FALSE(read_schema->position_of_tablet_cid(1).has_value());
    EXPECT_FALSE(read_schema->position_of_tablet_cid(100).has_value());

    Block block = read_schema->create_block();
    ASSERT_EQ(2, block.columns());
    EXPECT_EQ("v0", block.get_by_position(0).name);
    EXPECT_EQ("INT", block.get_by_position(0).type->get_name());
    EXPECT_EQ("k0", block.get_by_position(1).name);
    EXPECT_EQ("Nullable(INT)", block.get_by_position(1).type->get_name());
    EXPECT_TRUE(read_schema->validate_block(block).ok());
}

TEST(DenseReadSchemaTest, KeepsPhysicalKeyOrderForArbitraryDenseLayout) {
    TabletSchema tablet_schema;
    init_tablet_schema(tablet_schema);
    auto result = DenseReadSchema::create(tablet_schema, {1, 2, 0});
    ASSERT_TRUE(result.has_value()) << result.error().to_string();

    EXPECT_EQ((std::vector<ReadPosition> {ReadPosition(2), ReadPosition(0)}),
              result.value()->key_positions());
}

TEST(DenseReadSchemaTest, RejectsInvalidTabletColumnLayouts) {
    TabletSchema tablet_schema;
    init_tablet_schema(tablet_schema);

    auto duplicate = DenseReadSchema::create(tablet_schema, {0, 0});
    ASSERT_FALSE(duplicate.has_value());
    EXPECT_TRUE(duplicate.error().is<ErrorCode::INVALID_ARGUMENT>());

    auto outside_schema = DenseReadSchema::create(tablet_schema, {3});
    ASSERT_FALSE(outside_schema.has_value());
    EXPECT_TRUE(outside_schema.error().is<ErrorCode::INVALID_ARGUMENT>());
}

TEST(DenseReadSchemaTest, ValidatesExactBlockLayout) {
    TabletSchema tablet_schema;
    init_tablet_schema(tablet_schema);
    auto result = DenseReadSchema::create(tablet_schema, {0, 2});
    ASSERT_TRUE(result.has_value()) << result.error().to_string();
    DenseReadSchemaSPtr read_schema = result.value();

    Block wrong_order = tablet_schema.create_block({2, 0});
    EXPECT_FALSE(read_schema->validate_block(wrong_order).ok());

    Block missing_column = tablet_schema.create_block({0});
    EXPECT_FALSE(read_schema->validate_block(missing_column).ok());

    Block renamed = read_schema->create_block();
    renamed.get_by_position(0).name = "renamed";
    EXPECT_FALSE(read_schema->validate_block(renamed).ok());

    Block fresh = read_schema->create_block();
    EXPECT_EQ("k0", fresh.get_by_position(0).name);
    EXPECT_TRUE(read_schema->validate_block(fresh).ok());
}

TEST(DenseReadSchemaTest, PreservesVirtualColumnPrototype) {
    TabletSchema tablet_schema;
    std::string virtual_name = BeConsts::VIRTUAL_COLUMN_PREFIX + "score";
    tablet_schema.append_column(create_int_column(20, virtual_name, false));

    auto result = DenseReadSchema::create(tablet_schema, {0});
    ASSERT_TRUE(result.has_value()) << result.error().to_string();
    DenseReadSchemaSPtr read_schema = result.value();

    Block block = read_schema->create_block();
    ASSERT_NE(nullptr,
              check_and_get_column<const ColumnNothing>(block.get_by_position(0).column.get()));
    EXPECT_TRUE(read_schema->validate_block(block).ok());

    Block materialized = tablet_schema.create_block_by_cids({0});
    EXPECT_TRUE(read_schema->validate_block(materialized).ok());
}

TEST(DenseReadSchemaTest, ValidatesPrunedStructFieldIdentity) {
    TabletColumn struct_column;
    struct_column.set_unique_id(30);
    struct_column.set_name("payload");
    struct_column.set_type(FieldType::OLAP_FIELD_TYPE_STRUCT);
    struct_column.set_is_nullable(false);
    struct_column.set_aggregation_method(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE);
    TabletColumn field_a = create_int_column(31, "a", false);
    TabletColumn field_c = create_int_column(32, "c", false);
    struct_column.add_sub_column(field_a);
    struct_column.add_sub_column(field_c);

    TabletSchema tablet_schema;
    tablet_schema.append_column(struct_column);
    auto result = DenseReadSchema::create(tablet_schema, {0});
    ASSERT_TRUE(result.has_value()) << result.error().to_string();
    DenseReadSchemaSPtr read_schema = result.value();
    EXPECT_TRUE(read_schema->validate_block(read_schema->create_block()).ok());

    auto reordered_type = std::make_shared<DataTypeStruct>(
            DataTypes {std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeInt32>()},
            Strings {"c", "a"});
    Block reordered;
    reordered.insert({reordered_type->create_column(), reordered_type, "payload"});
    EXPECT_FALSE(read_schema->validate_block(reordered).ok());
}

TEST(DenseReadSchemaTest, AppendsAuxiliaryColumnsWithoutChangingPrefix) {
    TabletSchema tablet_schema;
    init_tablet_schema(tablet_schema);
    std::unordered_set<TabletColumnId> nullable_columns {0};
    auto scan_result = DenseReadSchema::create(tablet_schema, {2, 0}, &nullable_columns);
    ASSERT_TRUE(scan_result.has_value()) << scan_result.error().to_string();
    auto scan_schema = scan_result.value();

    auto segment_result =
            scan_schema->append_tablet_columns(tablet_schema, {1, 2}, &nullable_columns);
    ASSERT_TRUE(segment_result.has_value()) << segment_result.error().to_string();
    auto segment_schema = segment_result.value();

    ASSERT_EQ(3, segment_schema->size());
    EXPECT_EQ((std::vector<TabletColumnId> {2, 0, 1}), segment_schema->tablet_column_ids());
    EXPECT_EQ("INT", segment_schema->field(ReadPosition(0)).block_type->get_name());
    EXPECT_EQ("Nullable(INT)", segment_schema->field(ReadPosition(1)).block_type->get_name());
    EXPECT_EQ(2, segment_schema->position_of_tablet_cid(1)->value());

    auto projection_result = segment_schema->projection_positions(*scan_schema);
    ASSERT_TRUE(projection_result.has_value()) << projection_result.error().to_string();
    EXPECT_EQ((std::vector<ReadPosition> {ReadPosition(0), ReadPosition(1)}),
              projection_result.value());

    auto prefix_result = segment_schema->prefix(scan_schema->size());
    ASSERT_TRUE(prefix_result.has_value()) << prefix_result.error().to_string();
    EXPECT_TRUE(prefix_result.value()->validate_block(scan_schema->create_block()).ok());
}

TEST(DenseReadSchemaTest, ExistingNullablePrefixWinsOverAuxiliaryStorageLayout) {
    TabletSchema tablet_schema;
    init_tablet_schema(tablet_schema);
    std::unordered_set<TabletColumnId> nullable_columns {0};
    auto scan_result = DenseReadSchema::create(tablet_schema, {0}, &nullable_columns);
    ASSERT_TRUE(scan_result.has_value()) << scan_result.error().to_string();

    // Delete predicates may request k0 again as a BE-only auxiliary field. The scan prefix is
    // already nullable-aligned with the FE slot, while the physical TabletColumn is non-nullable.
    // Re-appending the same tablet CID must keep the prefix layout authoritative.
    auto segment_result = scan_result.value()->append_tablet_columns(tablet_schema, {0, 2});
    ASSERT_TRUE(segment_result.has_value()) << segment_result.error().to_string();

    const auto& segment_schema = segment_result.value();
    EXPECT_EQ((std::vector<TabletColumnId> {0, 2}), segment_schema->tablet_column_ids());
    EXPECT_EQ("Nullable(INT)", segment_schema->field(ReadPosition(0)).block_type->get_name());
    EXPECT_EQ("INT", segment_schema->field(ReadPosition(1)).block_type->get_name());
    auto prefix_result = segment_schema->prefix(1);
    ASSERT_TRUE(prefix_result.has_value()) << prefix_result.error().to_string();
    EXPECT_TRUE(prefix_result.value()->validate_block(scan_result.value()->create_block()).ok());
}

TEST(DenseReadSchemaTest, ProjectsByPhysicalIdentityInsteadOfTabletOrdinal) {
    TabletSchema source_tablet_schema;
    init_tablet_schema(source_tablet_schema);
    auto source_result = DenseReadSchema::create(source_tablet_schema, {0, 1, 2});
    ASSERT_TRUE(source_result.has_value()) << source_result.error().to_string();

    TabletSchema target_tablet_schema;
    target_tablet_schema.append_column(create_int_column(12, "v0", false));
    target_tablet_schema.append_column(create_int_column(10, "k0", true));
    auto target_result = DenseReadSchema::create(target_tablet_schema, {0, 1});
    ASSERT_TRUE(target_result.has_value()) << target_result.error().to_string();

    auto projection_result = source_result.value()->projection_positions(*target_result.value());
    ASSERT_TRUE(projection_result.has_value()) << projection_result.error().to_string();
    EXPECT_EQ((std::vector<ReadPosition> {ReadPosition(2), ReadPosition(0)}),
              projection_result.value());
}

TEST(DenseReadSchemaTest, DistinguishesVariantPathsWithSameParentUid) {
    TabletColumn field_a = create_int_column(-1, "payload.a", false);
    field_a.set_parent_unique_id(100);
    field_a.set_path_info(PathInData("payload.a"));
    TabletColumn field_b = create_int_column(-1, "payload.b", false);
    field_b.set_parent_unique_id(100);
    field_b.set_path_info(PathInData("payload.b"));

    TabletSchema tablet_schema;
    tablet_schema.append_column(field_a);
    tablet_schema.append_column(field_b);
    auto result = DenseReadSchema::create(tablet_schema, {0, 1});
    ASSERT_TRUE(result.has_value()) << result.error().to_string();

    const auto& identity_a = result.value()->field(ReadPosition(0)).identity;
    const auto& identity_b = result.value()->field(ReadPosition(1)).identity;
    EXPECT_EQ(DenseFieldIdentityKind::VARIANT_PATH, identity_a.kind);
    EXPECT_EQ(100, identity_a.unique_id);
    EXPECT_NE(identity_a, identity_b);
    EXPECT_EQ(0, result.value()->position_of_identity(identity_a)->value());
    EXPECT_EQ(1, result.value()->position_of_identity(identity_b)->value());
}

TEST(DenseReadSchemaTest, SpecialColumnAtPositionZeroIsPresent) {
    TabletSchema tablet_schema;
    tablet_schema.append_column(create_int_column(20, VERSION_COL, false));
    auto result = DenseReadSchema::create(tablet_schema, {0});
    ASSERT_TRUE(result.has_value()) << result.error().to_string();

    EXPECT_EQ(0, result.value()->version_col_idx());
    EXPECT_EQ(-1, result.value()->lsn_col_idx());
    EXPECT_EQ(0, result.value()->column_id(0));
    EXPECT_EQ(0, result.value()->column_index(0));
    EXPECT_EQ(0, result.value()->tablet_column_id(ReadPosition(0)));
}

} // namespace
} // namespace doris
