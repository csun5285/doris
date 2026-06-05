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

#include "storage/segment/historical_row_retriever.h"

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "common/consts.h"
#include "common/logging.h" // LOG
#include "common/status.h"
#include "core/block/block.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "core/string_ref.h"
#include "runtime/exec_env.h"
#include "service/point_query_executor.h"
#include "storage/binlog.h"
#include "storage/data_dir.h"
#include "storage/key_coder.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_reader_context.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/segment/segment.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet.h"
#include "storage/tablet/tablet_meta.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {

namespace segment_v2 {

using namespace ErrorCode;

namespace {

void insert_value_to_nullable_column(IColumn* dst_column, const IColumn& src_column, size_t pos) {
    auto* nullable_column = assert_cast<ColumnNullable*>(dst_column);
    if (src_column.is_nullable()) {
        nullable_column->insert_from(src_column, pos);
        return;
    }

    nullable_column->get_nested_column().insert_from(src_column, pos);
    nullable_column->get_null_map_data().push_back(0);
}

} // namespace

Status PrimaryKeyModelRowRetriever::init(const HistoricalRowRetrieverContext& context) {
    _context = context;
    return Status::OK();
}

Status PrimaryKeyModelRowRetriever::retrieve_historical_row(const Int8* delete_sign_column_data,
                                                            size_t row_pos, size_t num_rows) {
    auto* tablet = static_cast<Tablet*>(_context.tablet.get());
    auto& tablet_schema = _context.tablet_schema;

    DCHECK(_context.partial_update_info);

    std::vector<RowsetSharedPtr> specified_rowsets;
    {
        std::shared_lock rlock(_context.tablet->get_header_lock());
        specified_rowsets = _mow_context->rowset_ptrs;
    }
    std::vector<std::unique_ptr<SegmentCacheHandle>> segment_caches(specified_rowsets.size());

    for (size_t block_pos = row_pos; block_pos < row_pos + num_rows; block_pos++) {
        // After converting to olap column, [0, num_rows) in the result column is corresponding to
        // [row_pos, row_pos + num_rows) in the original block
        size_t delta_pos = block_pos - row_pos;
        std::string key = _full_encode_keys(_key_targets, delta_pos);

        _maybe_invalid_row_cache(key);
        if (_has_seq_target) {
            _encode_seq_column(&_seq_target, delta_pos, &key);
        }

        // mark key with delete sign as deleted.
        bool have_delete_sign =
                (delete_sign_column_data != nullptr && delete_sign_column_data[block_pos] != 0);

        RowLocation loc;
        // save rowset shared ptr so this rowset wouldn't delete
        RowsetSharedPtr rowset;
        auto st = tablet->lookup_row_key(key, tablet->tablet_schema().get(), _has_seq_target,
                                         specified_rowsets, &loc, _mow_context->max_version,
                                         segment_caches, &rowset);
        if (st.is<KEY_NOT_FOUND>()) {
            // it's an insert row
            _has_default_or_nullable = true;
            _use_default_or_null_flag.emplace_back(true);
            _operators.emplace_back(have_delete_sign ? ROW_BINLOG_DELETE : ROW_BINLOG_APPEND);
            continue;
        }
        if (!st.ok() && !st.is<KEY_ALREADY_EXISTS>()) {
            LOG(WARNING) << "failed to lookup row key, error: " << st;
            return st;
        }

        CHECK(_context.rowset_writer_ctx != nullptr);
        bool write_before =
                _context.rowset_writer_ctx->write_binlog_opt().write_binlog_config().write_before;
        // 1. if the delete sign is marked, it means that the value columns of the row will not
        //    be read. So we don't need to read the missing values from the previous rows.
        // 2. the one exception is when there are sequence columns in the table, we need to read
        //    the sequence columns, otherwise it may cause the merge-on-read based compaction
        //    policy to produce incorrect results.
        // 3. if row binlog needs BEFORE image, delete rows must still read historical values so
        //    __BEFORE__* columns can be populated.
        if (have_delete_sign && !tablet_schema->has_sequence_col() && !write_before) {
            _has_default_or_nullable = true;
            _use_default_or_null_flag.emplace_back(true);
            _operators.emplace_back(ROW_BINLOG_DELETE);
        } else {
            // partial update should not contain invisible columns
            _use_default_or_null_flag.emplace_back(false);
            _rsid_to_rowset.emplace(rowset->rowset_id(), rowset);
            // currently we think row_pos must be zero, so we won't consider row_pos > 0
            DCHECK(row_pos == 0);
            _rssid_to_rid.prepare_to_read(loc, delta_pos);
            _operators.emplace_back(have_delete_sign ? ROW_BINLOG_DELETE : ROW_BINLOG_UPDATE);
        }
    }

    CHECK_EQ(_use_default_or_null_flag.size(), num_rows);

    return Status::OK();
}

Status PrimaryKeyModelRowRetriever::build_after_block(Block* block, size_t row_pos,
                                                      size_t num_rows) {
    DCHECK_EQ(_use_default_or_null_flag.size(), num_rows);
    if (config::is_cloud_mode()) {
        return Status::NotSupported("fill_missing_columns");
    }
    if (_context.partial_update_info == nullptr) {
        return Status::InternalError("partial update info is null");
    }
    return _rssid_to_rid.fill_missing_columns(
            _context, _rsid_to_rowset, *_context.tablet_schema, *block, _use_default_or_null_flag,
            _has_default_or_nullable, cast_set<uint32_t>(row_pos), block);
}

Status PrimaryKeyModelRowRetriever::build_before_block(Block* before_block,
                                                       const std::vector<uint32_t>& value_cids,
                                                       size_t /*row_pos*/, size_t num_rows) {
    if (config::is_cloud_mode()) {
        // TODO(plat1ko): cloud mode
        return Status::NotSupported("fill_before_columns");
    }

    auto& tablet_schema = _context.tablet_schema;

    if (num_rows == 0 || value_cids.empty()) {
        return Status::OK();
    }

    // Create block to hold historical values for value columns.
    Block old_value_block = tablet_schema->create_block_by_cids(value_cids);
    CHECK_EQ(value_cids.size(), old_value_block.columns());

    // key: logical row index in current batch; value: index in old_value_block
    std::map<uint32_t, uint32_t> read_index;
    RETURN_IF_ERROR(_rssid_to_rid.read_columns_by_plan(*tablet_schema, value_cids, _rsid_to_rowset,
                                                       old_value_block, &read_index, false,
                                                       nullptr));

    {
        auto mutable_before_columns_guard = before_block->mutate_columns_scoped();
        auto& mutable_before_columns = mutable_before_columns_guard.mutable_columns();
        // Fill each row in before_block.
        for (uint32_t idx = 0; idx < num_rows; ++idx) {
            auto it = read_index.find(idx);
            if (it == read_index.end()) {
                // No historical row, fill BEFORE with NULL.
                for (size_t i = 0; i < value_cids.size(); ++i) {
                    auto* nullable_column =
                            assert_cast<ColumnNullable*>(mutable_before_columns[i].get());
                    nullable_column->insert_many_defaults(1);
                }
                continue;
            }

            uint32_t pos_in_old_block = it->second;
            for (size_t i = 0; i < value_cids.size(); ++i) {
                insert_value_to_nullable_column(mutable_before_columns[i].get(),
                                                *old_value_block.get_by_position(i).column,
                                                pos_in_old_block);
            }
        }
    }
    return Status::OK();
}

std::string PrimaryKeyModelRowRetriever::_full_encode_keys(
        const std::vector<KeyEncodingTarget>& key_targets, size_t pos) {
    std::string encoded_keys;
    for (const auto& t : key_targets) {
        DCHECK(t.coder != nullptr && t.view != nullptr);
        auto st = storage_view_encode_full_key_ascending(t.coder, *t.view, pos, &encoded_keys);
        DCHECK(st.ok()) << "storage_view_encode_full_key_ascending failed: " << st;
    }
    return encoded_keys;
}

void PrimaryKeyModelRowRetriever::_encode_seq_column(const KeyEncodingTarget* seq_target,
                                                     size_t pos, std::string* encoded_keys) {
    // To facilitate the use of the primary key index, encode the seq column
    // to the minimum value of the corresponding length when the seq column
    // is null
    if (UNLIKELY(storage_view_is_null_at(*seq_target->view, pos))) {
        auto& tablet_schema = _context.tablet_schema;
        encoded_keys->push_back(KeyConsts::KEY_NULL_FIRST_MARKER);
        size_t seq_col_length = tablet_schema->column(tablet_schema->sequence_col_idx()).length();
        encoded_keys->append(seq_col_length, KeyConsts::KEY_MINIMAL_MARKER);
        return;
    }
    auto st = storage_view_encode_full_key_ascending(seq_target->coder, *seq_target->view, pos,
                                                      encoded_keys);
    DCHECK(st.ok()) << "storage_view_encode_full_key_ascending failed: " << st;
}

void PrimaryKeyModelRowRetriever::_maybe_invalid_row_cache(const std::string& key) {
    // Just invalid row cache for simplicity, since the rowset is not visible at present.
    // If we update/insert cache, if load failed rowset will not be visible but cached data
    // will be visible, and lead to inconsistency.
    if (!config::disable_storage_row_cache &&
        _context.tablet_schema->has_row_store_for_all_columns() &&
        _context.write_type == DataWriteType::TYPE_DIRECT) {
        // invalidate cache
        RowCache::instance()->erase({static_cast<int64_t>(_context.tablet->tablet_id()), key});
    }
}

} // namespace segment_v2
} // namespace doris
