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

#include <cstddef>
#include <cstdint>
#include <limits>
#include <span>
#include <string>
#include <vector>

#include "common/status.h"
#include "storage/iterator/olap_data_convertor.h"

namespace doris {

class Block;
class KeyCoder;
class TabletColumn;
class TabletSchema;

// Encodes rows into the sortable binary key format shared by the short key
// index and the primary key index. Each column is encoded as a one byte
// marker (KeyConsts) followed by the KeyCoder encoded value, a null column
// is encoded as KEY_NULL_FIRST_MARKER without value bytes.
// The caller hands over the whole block a row lives in, straight from the
// compute layer; which columns each key view needs, where they sit in that
// block and how to convert them to storage format are all this encoder's own
// business. Conversion happens per row, so a caller may hand over a different
// block from one call to the next -- an aggregator rewriting its block between
// rows, say -- and never read stale bytes.
class RowKeyEncoder {
public:
    RowKeyEncoder(const TabletSchema& schema, bool mow);

    // Part of init: describe the blocks this encoder will be handed, where
    // `block_cids[position]` is the schema column id at that block position. A
    // block in schema order is the default and needs no call; one column group
    // of a vertical compaction, or the narrow block of a partial update, is the
    // case that does. Fails when a key column this encoder reads is not in the
    // layout, which would otherwise encode whatever column sits there instead.
    // Call before the first encode, and again whenever the layout changes --
    // vertical compaction re-inits the same writer once per column group.
    Status set_block_layout(std::span<const uint32_t> block_cids);

    // How much of each column's value goes into the key.
    enum class KeyWidth {
        // The whole value: segment min/max keys, the primary key index.
        Full,
        // Each column truncated to its index_length, over the leading
        // num_short_key_columns columns only: the short key index.
        ShortKeyPrefix,
    };

    // Encode `row`'s sort key columns -- whatever the segment is sorted by.
    // Returns a Status because converting the row can fail -- a VARCHAR key
    // over `string_type_length_soft_limit_bytes`, say -- and a key that
    // silently encoded as NULL instead would corrupt the index.
    Status encode_sort_key(const Block& block, size_t row, KeyWidth width,
                           std::string* encoded_key);

    // Encode `row`'s primary key, the key stored in and probed against the primary key index.
    // Every mow table builds this view; a table with cluster keys is the case where it differs
    // from the sort key, which follows the segment's own order.
    // `with_seq_col` appends the sequence suffix -- whether a row carries one is the caller's
    // question (schema-level for the writers, per-load or per-row for partial updates), the same
    // vocabulary as BaseTablet::lookup_row_key. `pk_prefix_len`, when asked for, is the length of
    // the key without the suffix: the row cache is keyed by that prefix.
    Status encode_primary_key(const Block& block, size_t row, bool with_seq_col, std::string* out,
                              size_t* pk_prefix_len = nullptr);

    // What a sequence value looks like in the primary key index: a marker plus
    // the KeyCoder encoding, or the null marker plus enough minimal-value
    // filler to keep the length, so a null sequence sorts first. This is the
    // one definition of that format, which is why it is also how a caller
    // encodes a value on its own to byte-compare against a suffix read back
    // out of the index. Appends, so `out` may already hold a key. The column
    // is taken from the block by layout, or handed over directly for a value
    // synthesized outside any block (a flexible partial update's default).
    Status encode_seq_value(const Block& block, size_t row, std::string* out);
    Status encode_seq_value(const IColumn& seq_column, size_t row, std::string* out);

    // Append the encoded row id to `encoded_keys`, only used by mow tables
    // with cluster keys.
    void append_rowid_suffix(std::string* encoded_keys, uint32_t rowid) const;

    // True when the blocks this encoder is handed carry the sequence column.
    // A caller that also tracks whether a row really sets a sequence value --
    // a partial update whose block holds the column but leaves it empty --
    // needs its own flag on top of this one.
    bool layout_has_seq_column() const;

private:
    // A schema column id this encoder does not read, and the position of a
    // column the block layout does not hold.
    static constexpr uint32_t kNoColumn = std::numeric_limits<uint32_t>::max();

    // One column of one key view: everything encoding it needs, so the hot
    // loop reads one contiguous record per column and looks nothing up.
    struct KeyColumn {
        uint32_t cid = kNoColumn;
        // Where the column sits in the blocks handed over, kNoColumn when the
        // layout does not hold it. Starts out as the identity: schema order.
        uint32_t position = kNoColumn;
        const KeyCoder* coder = nullptr;
        // What KeyWidth::ShortKeyPrefix truncates this column to. The primary
        // key view never truncates, so it leaves this at zero.
        uint16_t index_size = 0;
        // This view's own converter and buffers. A column in both views gets
        // one per view, which costs a scratch and buys the views not sharing
        // any state.
        ColumnEncoding encoding;
    };

    // Convert one key column at `row`. `field` comes back null for a NULL row.
    Status _encode_row(KeyColumn& key, const Block& block, size_t row, const uint8_t** field);
    // The shared shape of every key: per column a marker byte, then the coder's
    // bytes, or the null marker on its own for a NULL value.
    Status _encode(std::span<KeyColumn> key_columns, size_t num_columns, const Block& block,
                   size_t row, KeyWidth width, std::string* encoded_key);

    // Fill in everything encoding column `cid` needs, so encoding only encodes.
    void _init_key_column(KeyColumn& key, const TabletSchema& schema, uint32_t cid);

    // The sort-key view: whatever the segment sorts by. Cluster key columns
    // for mow tables with cluster keys, primary key columns otherwise. Used by
    // encode_sort_key(), at either width.
    std::vector<KeyColumn> _sort_key;
    // The primary-key view, built for every mow table. It coincides with the
    // sort-key view unless the table has cluster keys, where the segment sorts
    // by those while its primary key index stays on the schema key columns.
    std::vector<KeyColumn> _primary_key;
    // The sequence suffix of the primary key index, mow only. `cid` stays
    // kNoColumn when this table appends no sequence suffix.
    struct SeqColumn : KeyColumn {
        // How many filler bytes a null sequence value encodes to.
        size_t length = 0;
    };
    SeqColumn _seq;
    size_t _num_short_key_columns = 0;
};

} // namespace doris
