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

#include <algorithm>
#include <cstdint>
#include <limits>
#include <sstream>
#include <utility>

#include "common/consts.h"
#include "core/block/column_with_type_and_name.h"
#include "storage/binlog.h"

namespace doris {
namespace {

bool has_same_block_layout(const ColumnWithTypeAndName& lhs, const ColumnWithTypeAndName& rhs) {
    return lhs.type != nullptr && rhs.type != nullptr && lhs.type->equals(*rhs.type) &&
           lhs.type->get_name() == rhs.type->get_name() && lhs.column && rhs.column &&
           lhs.column->get_name() == rhs.column->get_name();
}

} // namespace

DenseFieldIdentity DenseFieldIdentity::from_column(const TabletColumn& column,
                                                   std::string_view block_name) {
    if (column.is_extracted_column()) {
        return {.kind = DenseFieldIdentityKind::VARIANT_PATH,
                .unique_id = column.parent_unique_id(),
                .path_or_name = column.path_info_ptr()->get_path()};
    }
    if (column.unique_id() >= 0) {
        return {.kind = DenseFieldIdentityKind::STORAGE_COLUMN,
                .unique_id = column.unique_id(),
                .path_or_name = {}};
    }
    return {.kind = DenseFieldIdentityKind::SYNTHETIC,
            .unique_id = -1,
            .path_or_name = std::string(block_name)};
}

std::string DenseFieldIdentity::debug_string() const {
    std::ostringstream out;
    switch (kind) {
    case DenseFieldIdentityKind::STORAGE_COLUMN:
        out << "uid=" << unique_id;
        break;
    case DenseFieldIdentityKind::VARIANT_PATH:
        out << "parent_uid=" << unique_id << ",path=" << path_or_name;
        break;
    case DenseFieldIdentityKind::SYNTHETIC:
        out << "synthetic=" << path_or_name;
        break;
    }
    return out.str();
}

size_t DenseFieldIdentityHash::operator()(const DenseFieldIdentity& identity) const {
    size_t seed = std::hash<uint8_t> {}(static_cast<uint8_t>(identity.kind));
    seed ^= std::hash<int32_t> {}(identity.unique_id) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
    seed ^= std::hash<std::string> {}(identity.path_or_name) + 0x9e3779b9 + (seed << 6) +
            (seed >> 2);
    return seed;
}

DenseReadSchema::DenseReadSchema(std::vector<DenseReadField> fields,
                                 std::vector<TabletColumnId> tablet_column_ids,
                                 std::vector<int32_t> tablet_cid_to_position,
                                 std::vector<ReadPosition> key_positions, Block block_template)
        : _fields(std::move(fields)),
          _tablet_column_ids(std::move(tablet_column_ids)),
          _tablet_cid_to_position(std::move(tablet_cid_to_position)),
          _key_positions(std::move(key_positions)),
          _block_template(std::move(block_template)) {
    _positions.reserve(_fields.size());
    _column_ids.reserve(_fields.size());
    _column_id_to_index.reserve(_fields.size());
    _columns.reserve(_fields.size());
    for (size_t position = 0; position < _fields.size(); ++position) {
        const auto local_position = static_cast<uint32_t>(position);
        _positions.emplace_back(local_position);
        _column_ids.push_back(local_position);
        _column_id_to_index.push_back(static_cast<int>(position));
        _columns.push_back(_fields[position].storage_column);
        _identity_to_position.emplace(_fields[position].identity, ReadPosition(local_position));

        const auto& name = _fields[position].block_name;
        if (name == DELETE_SIGN) {
            _delete_sign_idx = static_cast<int32_t>(position);
        }
        if (_fields[position].is_sequence_column) {
            _sequence_col_idx = static_cast<int32_t>(position);
        }
        if (name == BeConsts::ROWID_COL || name.starts_with(BeConsts::GLOBAL_ROWID_COL)) {
            _rowid_col_idx = static_cast<int32_t>(position);
        }
        if (name == VERSION_COL) {
            _version_col_idx = static_cast<int32_t>(position);
        }
        if (name == BINLOG_LSN_COL) {
            _lsn_col_idx = static_cast<int32_t>(position);
        }
        if (name == BINLOG_TSO_COL) {
            _tso_col_idx = static_cast<int32_t>(position);
        }
        if (name == COMMIT_TSO_COL) {
            _commit_tso_col_idx = static_cast<int32_t>(position);
        }
    }
}

Result<DenseReadSchemaSPtr> DenseReadSchema::create(
        const TabletSchema& tablet_schema, const std::vector<TabletColumnId>& tablet_column_ids,
        const std::unordered_set<TabletColumnId>* tablet_columns_need_convert_null) {
    for (size_t position = 0; position < tablet_column_ids.size(); ++position) {
        const TabletColumnId tablet_cid = tablet_column_ids[position];
        if (tablet_cid >= tablet_schema.num_columns()) {
            return ResultError(Status::InvalidArgument(
                    "tablet column id {} at read position {} is outside schema with {} columns",
                    tablet_cid, position, tablet_schema.num_columns()));
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
                          .block_name = block_column.name,
                          .identity = DenseFieldIdentity::from_column(
                                  tablet_schema.column(tablet_cid), block_column.name),
                          .is_sequence_column =
                                  tablet_schema.has_sequence_col() &&
                                  std::cmp_equal(tablet_schema.sequence_col_idx(), tablet_cid)});
    }

    return _create_from_fields(std::move(fields), std::move(block_template));
}

Result<DenseReadSchemaSPtr> DenseReadSchema::_create_from_fields(std::vector<DenseReadField> fields,
                                                                 Block block_template) {
    if (fields.size() != block_template.columns()) {
        return ResultError(Status::InvalidArgument(
                "dense schema has {} fields but block template has {} columns", fields.size(),
                block_template.columns()));
    }

    uint32_t max_tablet_cid = 0;
    std::unordered_set<TabletColumnId> tablet_cids;
    std::unordered_set<DenseFieldIdentity, DenseFieldIdentityHash> identities;
    std::vector<TabletColumnId> tablet_column_ids;
    std::vector<std::pair<TabletColumnId, ReadPosition>> key_positions_by_tablet_cid;
    tablet_column_ids.reserve(fields.size());
    key_positions_by_tablet_cid.reserve(fields.size());

    for (size_t position = 0; position < fields.size(); ++position) {
        const auto& field = fields[position];
        if (!tablet_cids.emplace(field.tablet_cid).second) {
            return ResultError(
                    Status::InvalidArgument("duplicate tablet column id {} at dense position {}",
                                            field.tablet_cid, position));
        }
        if (!identities.emplace(field.identity).second) {
            return ResultError(
                    Status::InvalidArgument("duplicate field identity {} at dense position {}",
                                            field.identity.debug_string(), position));
        }
        const auto& block_column = block_template.get_by_position(position);
        if (field.block_name != block_column.name || field.block_type == nullptr ||
            !field.block_type->equals(*block_column.type) ||
            field.block_type->get_name() != block_column.type->get_name()) {
            return ResultError(Status::InvalidArgument(
                    "field {} does not match its block template column at dense position {}",
                    field.identity.debug_string(), position));
        }
        max_tablet_cid = std::max(max_tablet_cid, field.tablet_cid);
        tablet_column_ids.push_back(field.tablet_cid);
        if (field.storage_column->is_key()) {
            key_positions_by_tablet_cid.emplace_back(field.tablet_cid,
                                                     ReadPosition(static_cast<uint32_t>(position)));
        }
    }

    std::ranges::sort(key_positions_by_tablet_cid,
                      [](const auto& lhs, const auto& rhs) { return lhs.first < rhs.first; });
    std::vector<ReadPosition> key_positions;
    key_positions.reserve(key_positions_by_tablet_cid.size());
    for (const auto& entry : key_positions_by_tablet_cid) {
        key_positions.push_back(entry.second);
    }

    std::vector<int32_t> tablet_cid_to_position;
    if (!fields.empty()) {
        tablet_cid_to_position.assign(static_cast<size_t>(max_tablet_cid) + 1, -1);
        for (size_t position = 0; position < fields.size(); ++position) {
            tablet_cid_to_position[fields[position].tablet_cid] = static_cast<int32_t>(position);
        }
    }

    return DenseReadSchemaSPtr(new DenseReadSchema(
            std::move(fields), std::move(tablet_column_ids), std::move(tablet_cid_to_position),
            std::move(key_positions), std::move(block_template)));
}

const DenseReadField& DenseReadSchema::field(ReadPosition position) const {
    const size_t index = position.value();
    DCHECK_LT(index, _fields.size());
    return _fields[index];
}

TabletColumnId DenseReadSchema::tablet_column_id(ReadPosition position) const {
    return field(position).tablet_cid;
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

std::optional<ReadPosition> DenseReadSchema::position_of_identity(
        const DenseFieldIdentity& identity) const {
    auto it = _identity_to_position.find(identity);
    if (it == _identity_to_position.end()) {
        return std::nullopt;
    }
    return it->second;
}

Result<std::vector<ReadPosition>> DenseReadSchema::projection_positions(
        const DenseReadSchema& target_schema) const {
    std::vector<ReadPosition> source_positions;
    source_positions.reserve(target_schema.size());
    for (size_t target_position = 0; target_position < target_schema.size(); ++target_position) {
        const auto& target_field =
                target_schema.field(ReadPosition(static_cast<uint32_t>(target_position)));
        auto source_position = position_of_identity(target_field.identity);
        if (!source_position.has_value()) {
            return ResultError(Status::InvalidArgument(
                    "projection target field {} at position {} is absent from source schema",
                    target_field.identity.debug_string(), target_position));
        }
        const auto& source_block_column = _block_template.get_by_position(source_position->value());
        const auto& target_block_column =
                target_schema._block_template.get_by_position(target_position);
        if (!has_same_block_layout(source_block_column, target_block_column)) {
            return ResultError(Status::InvalidArgument(
                    "projection field {} has incompatible source and target block layouts",
                    target_field.identity.debug_string()));
        }
        source_positions.push_back(*source_position);
    }
    return source_positions;
}

Result<DenseReadSchemaSPtr> DenseReadSchema::append_missing_fields(
        const DenseReadSchema& auxiliary_schema) const {
    std::vector<DenseReadField> fields = _fields;
    Block block_template = _block_template.clone_empty();
    for (size_t position = 0; position < auxiliary_schema.size(); ++position) {
        const auto& auxiliary_field =
                auxiliary_schema.field(ReadPosition(static_cast<uint32_t>(position)));
        auto existing_position = position_of_identity(auxiliary_field.identity);
        if (existing_position.has_value()) {
            const auto& existing = _block_template.get_by_position(existing_position->value());
            const auto& auxiliary = auxiliary_schema._block_template.get_by_position(position);
            if (!has_same_block_layout(existing, auxiliary)) {
                return ResultError(Status::InvalidArgument(
                        "auxiliary field {} conflicts with the dense schema prefix",
                        auxiliary_field.identity.debug_string()));
            }
            continue;
        }
        fields.push_back(auxiliary_field);
        const auto& auxiliary = auxiliary_schema._block_template.get_by_position(position);
        block_template.insert({auxiliary.column->clone_empty(), auxiliary.type, auxiliary.name});
    }
    return _create_from_fields(std::move(fields), std::move(block_template));
}

Result<DenseReadSchemaSPtr> DenseReadSchema::append_tablet_columns(
        const TabletSchema& tablet_schema,
        const std::vector<TabletColumnId>& auxiliary_tablet_column_ids,
        const std::unordered_set<TabletColumnId>* tablet_columns_need_convert_null) const {
    std::vector<TabletColumnId> missing_tablet_column_ids;
    missing_tablet_column_ids.reserve(auxiliary_tablet_column_ids.size());
    for (TabletColumnId tablet_cid : auxiliary_tablet_column_ids) {
        if (!position_of_tablet_cid(tablet_cid).has_value()) {
            missing_tablet_column_ids.push_back(tablet_cid);
        }
    }

    auto auxiliary_schema =
            create(tablet_schema, missing_tablet_column_ids, tablet_columns_need_convert_null);
    if (!auxiliary_schema.has_value()) {
        return ResultError(std::move(auxiliary_schema).error());
    }
    return append_missing_fields(*auxiliary_schema.value());
}

Result<DenseReadSchemaSPtr> DenseReadSchema::prefix(size_t prefix_size) const {
    if (prefix_size > size()) {
        return ResultError(Status::InvalidArgument("prefix size {} exceeds dense schema size {}",
                                                   prefix_size, size()));
    }
    std::vector<DenseReadField> fields(_fields.begin(), _fields.begin() + prefix_size);
    Block block_template;
    for (size_t position = 0; position < prefix_size; ++position) {
        const auto& source = _block_template.get_by_position(position);
        block_template.insert({source.column->clone_empty(), source.type, source.name});
    }
    return _create_from_fields(std::move(fields), std::move(block_template));
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
                               actual.type->equals(*expected.type) &&
                               actual.type->get_name() == expected.type->get_name();
        // Column implementations are allowed to change while a batch is evaluated. For
        // example, a virtual field starts as ColumnNothing and is replaced by its materialized
        // column before returning. The dense contract fixes position, name, and DataType.
        if (actual.name != expected.name || !same_type || !actual.column) {
            return Status::InvalidArgument(
                    "block column at read position {} (tablet column id {}) does not match dense "
                    "read schema, expected={}, actual={}",
                    position, _fields[position].tablet_cid, expected.dump_structure(),
                    actual.dump_structure());
        }
    }
    return Status::OK();
}

int DenseReadSchema::column_index(ColumnId position) const {
    DCHECK_LT(position, _column_id_to_index.size());
    return _column_id_to_index[position];
}

const TabletColumn* DenseReadSchema::column(ColumnId position) const {
    DCHECK_LT(position, _columns.size());
    return _columns[position].get();
}

} // namespace doris
