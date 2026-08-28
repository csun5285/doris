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

#include "storage/key/row_key_encoder.h"

#include <algorithm>
#include <cassert>

#include "common/cast_set.h"
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/consts.h"
#include "common/logging.h"
#include "core/block/block.h"
#include "storage/iterator/olap_data_convertor.h"
#include "storage/key_coder.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {

void RowKeyEncoder::_init_key_column(KeyColumn& key, const TabletSchema& schema, uint32_t cid) {
    const auto& column = schema.column(cid);
    key.cid = cid;
    // Until set_block_layout() says otherwise, a block is in schema order: a
    // column id is its own position.
    key.position = cid;
    key.coder = get_key_coder(column.type());
    key.encoding.encoder = create_column_data_convertor(column);
}

// Which columns each view ends up holding:
//
//                       _sort_key           _primary_key
//   non-mow             key columns         (none)
//   mow                 key columns         key columns
//   mow + cluster keys  cluster key cols    key columns
//
// The primary key index is built on the schema key columns whatever the segment sorts by, so every
// mow table gets that view, not just the ones with cluster keys. The sort-key view follows the
// segment's own order, which is the only column set that differs between the two.
RowKeyEncoder::RowKeyEncoder(const TabletSchema& schema, bool mow)
        : _num_short_key_columns(schema.num_short_key_columns()) {
    const auto add_sort_key = [&](uint32_t cid) {
        auto& key = _sort_key.emplace_back();
        _init_key_column(key, schema, cid);
        // Only this view truncates, under KeyWidth::ShortKeyPrefix.
        key.index_size = cast_set<uint16_t>(schema.column(cid).index_length());
    };
    const auto add_schema_keys = [&](auto&& add) {
        for (uint32_t cid = 0; cid < schema.num_key_columns(); ++cid) {
            add(cid);
        }
    };

    if (!mow) {
        // A non-mow segment sorts by its key columns and builds no primary key view.
        add_schema_keys(add_sort_key);
        return;
    }

    add_schema_keys(
            [&](uint32_t cid) { _init_key_column(_primary_key.emplace_back(), schema, cid); });
    // encode the sequence id into the primary key index
    if (schema.has_sequence_col()) {
        const auto cid = cast_set<uint32_t>(schema.sequence_col_idx());
        _init_key_column(_seq, schema, cid);
        _seq.length = static_cast<size_t>(schema.column(cid).length());
    }

    if (schema.cluster_key_uids().empty()) {
        add_schema_keys(add_sort_key);
        return;
    }
    for (auto uid : schema.cluster_key_uids()) {
        const auto cluster_key_cid = schema.field_index(uid);
        DCHECK_GE(cluster_key_cid, 0)
                << "cluster key column with unique_id=" << uid << " is not in the schema";
        add_sort_key(cast_set<uint32_t>(cluster_key_cid));
    }
}

Status RowKeyEncoder::set_block_layout(std::span<const uint32_t> block_cids) {
    // Only a handful of columns to place, so scan the layout for each rather
    // than building a reverse table; this runs once per writer init.
    const auto place = [&](KeyColumn& key) {
        key.position = kNoColumn;
        for (size_t position = 0; position < block_cids.size(); ++position) {
            if (block_cids[position] == key.cid) {
                key.position = cast_set<uint32_t>(position);
                return;
            }
        }
    };
    // Check every key column once here rather than per row: a key column the
    // layout is missing would silently encode whatever column sits there.
    const auto place_all = [&](std::span<KeyColumn> view, const char* name) -> Status {
        for (auto& key : view) {
            place(key);
            if (key.position == kNoColumn) {
                return Status::InternalError("{} column {} is not in this block layout", name,
                                             key.cid);
            }
        }
        return Status::OK();
    };
    RETURN_IF_ERROR(place_all(_sort_key, "sort key"));
    RETURN_IF_ERROR(place_all(_primary_key, "primary key"));
    // The sequence column may legitimately be absent, so it only gets placed.
    if (_seq.cid != kNoColumn) {
        place(_seq);
    }
    return Status::OK();
}

bool RowKeyEncoder::layout_has_seq_column() const {
    return _seq.cid != kNoColumn && _seq.position != kNoColumn;
}

Status RowKeyEncoder::_encode_row(KeyColumn& key, const Block& block, size_t row,
                                  const uint8_t** field) {
    DCHECK_LT(key.position, block.columns())
            << "column " << key.cid << " is not in the block handed to this encoder";
    // One row at a time: what the caller hands over is what gets read, so a
    // block rewritten between two calls cannot leave stale bytes behind.
    RETURN_IF_ERROR(key.encoding.encode(*block.get_by_position(key.position).column, row, 1));
    // A null row has no value bytes -- the caller writes a null marker instead.
    *field = key.encoding.scratch.is_null_at(0) ? nullptr : key.encoding.scratch.data;
    return Status::OK();
}

Status RowKeyEncoder::encode_sort_key(const Block& block, size_t row, KeyWidth width,
                                      std::string* encoded_key) {
    // The short key index only covers the leading columns; everything else takes the whole view.
    const size_t num_columns =
            width == KeyWidth::ShortKeyPrefix ? _num_short_key_columns : _sort_key.size();
    DCHECK_LE(num_columns, _sort_key.size());
    return _encode(_sort_key, num_columns, block, row, width, encoded_key);
}

Status RowKeyEncoder::encode_primary_key(const Block& block, size_t row, bool with_seq_col,
                                         std::string* out, size_t* pk_prefix_len) {
    RETURN_IF_ERROR(_encode(_primary_key, _primary_key.size(), block, row, KeyWidth::Full, out));
    if (pk_prefix_len != nullptr) {
        *pk_prefix_len = out->size();
    }
    if (with_seq_col) {
        RETURN_IF_ERROR(encode_seq_value(block, row, out));
    }
    return Status::OK();
}

Status RowKeyEncoder::_encode(std::span<KeyColumn> key_columns, size_t num_columns,
                              const Block& block, size_t row, KeyWidth width,
                              std::string* encoded_key) {
    encoded_key->clear();
    for (size_t i = 0; i < num_columns; ++i) {
        KeyColumn& key = key_columns[i];
        const uint8_t* field = nullptr;
        RETURN_IF_ERROR(_encode_row(key, block, row, &field));
        if (UNLIKELY(!field)) {
            // A NULL value contributes its marker and no bytes at all.
            encoded_key->push_back(KeyConsts::KEY_NULL_FIRST_MARKER);
            continue;
        }
        encoded_key->push_back(KeyConsts::KEY_NORMAL_MARKER);
        DCHECK(key.coder != nullptr);
        if (width == KeyWidth::ShortKeyPrefix) {
            key.coder->encode_ascending(field, key.index_size, encoded_key);
        } else {
            key.coder->full_encode_ascending(field, encoded_key);
        }
    }
    return Status::OK();
}

Status RowKeyEncoder::encode_seq_value(const Block& block, size_t row, std::string* out) {
    if (!layout_has_seq_column()) {
        return Status::InternalError("the sequence column is not in this block layout");
    }
    return encode_seq_value(*block.get_by_position(_seq.position).column, row, out);
}

Status RowKeyEncoder::encode_seq_value(const IColumn& seq_column, size_t row, std::string* out) {
    if (_seq.cid == kNoColumn) {
        return Status::InternalError("this table has no sequence column to encode");
    }
    RETURN_IF_ERROR(_seq.encoding.encode(seq_column, row, 1));
    const uint8_t* field =
            _seq.encoding.scratch.is_null_at(0) ? nullptr : _seq.encoding.scratch.data;
    // So the primary key index can still use it, encode a null seq column as
    // the smallest value of its length.
    if (UNLIKELY(!field)) {
        out->push_back(KeyConsts::KEY_NULL_FIRST_MARKER);
        out->append(_seq.length, KeyConsts::KEY_MINIMAL_MARKER);
        return Status::OK();
    }
    out->push_back(KeyConsts::KEY_NORMAL_MARKER);
    _seq.coder->full_encode_ascending(field, out);
    return Status::OK();
}

void RowKeyEncoder::append_rowid_suffix(std::string* encoded_keys, uint32_t rowid) const {
    encoded_keys->push_back(KeyConsts::KEY_NORMAL_MARKER);
    // A rowid is always a uint32, so the coder is known here rather than looked up.
    KeyCoderTraits<FieldType::OLAP_FIELD_TYPE_UNSIGNED_INT>::full_encode_ascending(&rowid,
                                                                                   encoded_keys);
}

} // namespace doris
