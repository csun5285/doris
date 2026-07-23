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
#include <memory>
#include <optional>
#include <string>
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

private:
    uint32_t _value;
};

struct DenseReadField {
    TabletColumnId tablet_cid;
    std::shared_ptr<const TabletColumn> storage_column;
    DataTypePtr block_type;
    std::string block_name;
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

    const DenseReadField& field(ReadPosition position) const;
    const std::vector<ReadPosition>& key_positions() const { return _key_positions; }
    const std::vector<TabletColumnId>& tablet_column_ids() const { return _tablet_column_ids; }
    std::optional<ReadPosition> position_of_tablet_cid(TabletColumnId tablet_cid) const;

    Block create_block() const;
    Status validate_block(const Block& block) const;

private:
    DenseReadSchema(std::vector<DenseReadField> fields,
                    std::vector<TabletColumnId> tablet_column_ids,
                    std::vector<int32_t> tablet_cid_to_position,
                    std::vector<ReadPosition> key_positions, Block block_template);

    std::vector<DenseReadField> _fields;
    std::vector<TabletColumnId> _tablet_column_ids;
    std::vector<int32_t> _tablet_cid_to_position;
    std::vector<ReadPosition> _key_positions;
    Block _block_template;
};

} // namespace doris
