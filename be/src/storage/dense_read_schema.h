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

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/status.h"
#include "core/block/block.h"
#include "storage/olap_common.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {

using TabletColumnId = ColumnId;

class ReadPosition final {
public:
    explicit constexpr ReadPosition(uint32_t value) : _value(value) {}

    constexpr uint32_t value() const { return _value; }
    bool operator==(const ReadPosition&) const = default;
    bool operator<(const ReadPosition& other) const { return _value < other._value; }

private:
    uint32_t _value;
};

enum class DenseFieldIdentityKind : uint8_t {
    STORAGE_COLUMN,
    VARIANT_PATH,
    SYNTHETIC,
};

// Stable identity of a field across dense layouts. Tablet column ordinals are
// deliberately excluded because they are only meaningful in one TabletSchema.
struct DenseFieldIdentity {
    DenseFieldIdentityKind kind = DenseFieldIdentityKind::SYNTHETIC;
    int32_t unique_id = -1;
    std::string path_or_name;

    static DenseFieldIdentity from_column(const TabletColumn& column, std::string_view block_name);

    bool operator==(const DenseFieldIdentity&) const = default;
    std::string debug_string() const;
};

struct DenseFieldIdentityHash {
    size_t operator()(const DenseFieldIdentity& identity) const;
};

struct DenseReadField {
    TabletColumnId tablet_cid;
    TabletColumnPtr storage_column;
    DataTypePtr block_type;
    std::string block_name;
    DenseFieldIdentity identity;
    bool is_sequence_column = false;
};

class DenseReadSchema;
using DenseReadSchemaSPtr = std::shared_ptr<const DenseReadSchema>;

// Describes the exact, dense layout of a storage-read Block. Unlike the legacy
// Schema, a ReadPosition is always the position of a field in the Block and is
// never a TabletSchema column ordinal.
class DenseReadSchema final {
public:
    static Result<DenseReadSchemaSPtr> create(
            const TabletSchema& tablet_schema, const std::vector<TabletColumnId>& tablet_column_ids,
            const std::unordered_set<TabletColumnId>* tablet_columns_need_convert_null = nullptr);

    size_t size() const { return _fields.size(); }
    const std::vector<ReadPosition>& positions() const { return _positions; }

    const DenseReadField& field(ReadPosition position) const;
    const std::vector<ReadPosition>& key_positions() const { return _key_positions; }
    const std::vector<TabletColumnId>& tablet_column_ids() const { return _tablet_column_ids; }
    TabletColumnId tablet_column_id(ReadPosition position) const;
    std::optional<ReadPosition> position_of_tablet_cid(TabletColumnId tablet_cid) const;
    std::optional<ReadPosition> position_of_identity(const DenseFieldIdentity& identity) const;

    // Return source positions, in target order, for an identity-based projection
    // from this schema to target_schema.
    Result<std::vector<ReadPosition>> projection_positions(
            const DenseReadSchema& target_schema) const;

    // Preserve this schema as an exact prefix and append fields from auxiliary_schema
    // whose stable identity is not already present.
    Result<DenseReadSchemaSPtr> append_missing_fields(
            const DenseReadSchema& auxiliary_schema) const;
    Result<DenseReadSchemaSPtr> append_tablet_columns(
            const TabletSchema& tablet_schema,
            const std::vector<TabletColumnId>& auxiliary_tablet_column_ids,
            const std::unordered_set<TabletColumnId>* tablet_columns_need_convert_null =
                    nullptr) const;
    Result<DenseReadSchemaSPtr> prefix(size_t prefix_size) const;

    Block create_block() const;
    Status validate_block(const Block& block) const;

    // Local-position compatibility surface for iterator code. Every ColumnId
    // accepted or returned by these methods is a dense Block position.
    size_t num_columns() const { return size(); }
    size_t num_column_ids() const { return size(); }
    size_t num_key_columns() const { return _key_positions.size(); }
    const std::vector<ColumnId>& column_ids() const { return _column_ids; }
    ColumnId column_id(size_t index) const { return _column_ids[index]; }
    int column_index(ColumnId position) const;
    const std::vector<int>& column_id_to_index() const { return _column_id_to_index; }
    const std::vector<TabletColumnPtr>& columns() const { return _columns; }
    const TabletColumn* column(ColumnId position) const;
    int32_t delete_sign_idx() const { return _delete_sign_idx; }
    bool has_sequence_col() const { return _sequence_col_idx >= 0; }
    int32_t sequence_col_idx() const { return _sequence_col_idx; }
    int32_t rowid_col_idx() const { return _rowid_col_idx; }
    int32_t version_col_idx() const { return _version_col_idx; }
    int32_t lsn_col_idx() const { return _lsn_col_idx; }
    int32_t tso_col_idx() const { return _tso_col_idx; }
    int32_t commit_tso_col_idx() const { return _commit_tso_col_idx; }

private:
    static Result<DenseReadSchemaSPtr> _create_from_fields(std::vector<DenseReadField> fields,
                                                           Block block_template);

    DenseReadSchema(std::vector<DenseReadField> fields,
                    std::vector<TabletColumnId> tablet_column_ids,
                    std::vector<int32_t> tablet_cid_to_position,
                    std::vector<ReadPosition> key_positions, Block block_template);

    std::vector<DenseReadField> _fields;
    std::vector<ReadPosition> _positions;
    std::vector<TabletColumnId> _tablet_column_ids;
    std::vector<int32_t> _tablet_cid_to_position;
    std::vector<ReadPosition> _key_positions;
    std::unordered_map<DenseFieldIdentity, ReadPosition, DenseFieldIdentityHash>
            _identity_to_position;
    std::vector<ColumnId> _column_ids;
    std::vector<int> _column_id_to_index;
    std::vector<TabletColumnPtr> _columns;
    Block _block_template;
    int32_t _delete_sign_idx = -1;
    int32_t _sequence_col_idx = -1;
    int32_t _rowid_col_idx = -1;
    int32_t _version_col_idx = -1;
    int32_t _lsn_col_idx = -1;
    int32_t _tso_col_idx = -1;
    int32_t _commit_tso_col_idx = -1;
};

} // namespace doris
