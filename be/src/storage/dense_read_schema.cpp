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

#include <glog/logging.h>

#include <utility>

#include "core/block/column_with_type_and_name.h"

namespace doris {

DenseReadSchema::DenseReadSchema(std::vector<DenseReadField> fields,
                                 std::vector<TabletColumnId> tablet_column_ids,
                                 std::vector<int32_t> tablet_cid_to_position,
                                 std::vector<ReadPosition> key_positions, Block block_template)
        : _fields(std::move(fields)),
          _tablet_column_ids(std::move(tablet_column_ids)),
          _tablet_cid_to_position(std::move(tablet_cid_to_position)),
          _key_positions(std::move(key_positions)),
          _block_template(std::move(block_template)) {}

Result<DenseReadSchemaSPtr> DenseReadSchema::create(
        const TabletSchema& tablet_schema, const std::vector<TabletColumnId>& tablet_column_ids,
        const std::unordered_set<TabletColumnId>* tablet_columns_need_convert_null) {
    std::vector<int32_t> tablet_cid_to_position(tablet_schema.num_columns(), -1);
    std::vector<ReadPosition> key_positions;
    for (size_t position = 0; position < tablet_column_ids.size(); ++position) {
        const TabletColumnId tablet_cid = tablet_column_ids[position];
        if (tablet_cid >= tablet_schema.num_columns()) {
            return ResultError(Status::InvalidArgument(
                    "tablet column id {} at read position {} is outside schema with {} columns",
                    tablet_cid, position, tablet_schema.num_columns()));
        }
        if (tablet_cid_to_position[tablet_cid] >= 0) {
            return ResultError(Status::InvalidArgument(
                    "duplicate tablet column id {} at read position {}", tablet_cid, position));
        }
        tablet_cid_to_position[tablet_cid] = static_cast<int32_t>(position);
        if (tablet_schema.column(tablet_cid).is_key()) {
            key_positions.emplace_back(static_cast<uint32_t>(position));
        }
    }

    Block block_template =
            tablet_schema.create_block(tablet_column_ids, tablet_columns_need_convert_null);
    DCHECK_EQ(block_template.columns(), tablet_column_ids.size());

    std::vector<DenseReadField> fields;
    fields.reserve(tablet_column_ids.size());
    for (size_t position = 0; position < tablet_column_ids.size(); ++position) {
        const TabletColumnId tablet_cid = tablet_column_ids[position];
        const auto& block_column = block_template.get_by_position(position);
        fields.push_back({.tablet_cid = tablet_cid,
                          .storage_column = tablet_schema.columns()[tablet_cid],
                          .block_type = block_column.type,
                          .block_name = block_column.name});
    }

    return DenseReadSchemaSPtr(new DenseReadSchema(
            std::move(fields), tablet_column_ids, std::move(tablet_cid_to_position),
            std::move(key_positions), std::move(block_template)));
}

const DenseReadField& DenseReadSchema::field(ReadPosition position) const {
    const size_t index = position.value();
    DCHECK_LT(index, _fields.size());
    return _fields[index];
}

std::optional<ReadPosition> DenseReadSchema::position_of_tablet_cid(
        TabletColumnId tablet_cid) const {
    if (tablet_cid >= _tablet_cid_to_position.size()) {
        return std::nullopt;
    }
    const int32_t position = _tablet_cid_to_position[tablet_cid];
    if (position < 0) {
        return std::nullopt;
    }
    return ReadPosition(static_cast<uint32_t>(position));
}

Block DenseReadSchema::create_block() const {
    return _block_template.clone_empty();
}

Status DenseReadSchema::validate_block(const Block& block) const {
    if (block.columns() != _block_template.columns()) {
        return Status::InvalidArgument("block has {} columns but dense read schema requires {}",
                                       block.columns(), _block_template.columns());
    }

    for (size_t position = 0; position < _fields.size(); ++position) {
        const auto& expected = _block_template.get_by_position(position);
        const auto& actual = block.get_by_position(position);
        const bool same_type = actual.type != nullptr && expected.type != nullptr &&
                               actual.type->get_name() == expected.type->get_name();
        const bool same_column = actual.column && expected.column &&
                                 actual.column->get_name() == expected.column->get_name();
        if (actual.name != expected.name || !same_type || !same_column) {
            return Status::InvalidArgument(
                    "block column at read position {} (tablet column id {}) does not match dense "
                    "read schema, expected={}, actual={}",
                    position, _fields[position].tablet_cid, expected.dump_structure(),
                    actual.dump_structure());
        }
    }
    return Status::OK();
}

} // namespace doris
