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

#include "storage/segment/row_binlog_segment_writer.h"

#include "cloud/config.h"
#include "common/cast_set.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "storage/binlog.h"
#include "storage/olap_utils.h"
#include "storage/rowset/rowset_writer_context.h" // RowsetWriterContext

namespace doris {
namespace segment_v2 {

RowBinlogSourceDataWriter::RowBinlogSourceDataWriter(const SegmentWriteBinlogOptions& opt)
        : _opt(opt) {}

RowBinlogSegmentWriter::RowBinlogSegmentWriter(
        io::FileWriter* file_writer, uint32_t segment_id, TabletSchemaSPtr tablet_schema,
        BaseTabletSPtr tablet, DataDir* data_dir, const SegmentWriterOptions& opts,
        const segment_v2::SegmentWriteBinlogOptions& row_binlog_opts)
        : SegmentWriter(file_writer, segment_id, tablet_schema, tablet, data_dir, opts, nullptr),
          _binlog_opts(row_binlog_opts) {
    if (_opts.write_type == DataWriteType::TYPE_DIRECT) {
        _source_data_writer = std::make_unique<RowBinlogSourceDataWriter>(row_binlog_opts);
        _lsn_ids = const_cast<SegmentWriteBinlogOptions&>(row_binlog_opts).get_seg_lsn(_segment_id);
        const_cast<SegmentWriteBinlogOptions&>(row_binlog_opts).remove_seg(segment_id);
    }
}

RowBinlogSourceDataWriter::~RowBinlogSourceDataWriter() {
    this->clear();
}

Status RowBinlogSegmentWriter::init() {
    RETURN_IF_ERROR(SegmentWriter::init());

    if (_opts.write_type != DataWriteType::TYPE_DIRECT) {
        return Status::OK();
    }

    RETURN_IF_ERROR(_source_data_writer->init());
    _write_before = _source_data_writer->need_before();

    const TabletSchemaSPtr& source_schema = _binlog_opts.source.tablet_schema;
    if (UNLIKELY(source_schema == nullptr)) {
        return Status::InternalError("binlog<row> writer missing source_tablet_schema");
    }

    int lsn_col_id = _tablet_schema->field_index(std::string(kRowBinlogLsnColName));
    CHECK(lsn_col_id >= 0) << "binlog<row> schema missing __DORIS_BINLOG_LSN__";
    _binlog_col_start_id = static_cast<uint32_t>(lsn_col_id);
    _normal_col_start_id = lsn_col_id == 0 ? BINLOG_COLNUM : 0;

    uint32_t normal_col_num = cast_set<uint32_t>(source_schema->num_visible_columns());
    _before_col_start_id = _normal_col_start_id + normal_col_num;

    if (!_write_before && _tablet_schema->num_columns() > normal_col_num + BINLOG_COLNUM) {
        // Compatibility path
        _fill_empty_before_value = true;
        _write_before = true;
    }

    HistoricalRowRetrieverContext historical_row_retriever_context = {
            .tablet = _tablet,
            .tablet_schema = source_schema,
            .rowset_writer_ctx = _opts.rowset_ctx,
            .partial_update_info = _binlog_opts.source.partial_update_info,
            .is_transient_rowset_writer = _binlog_opts.source.is_transient_rowset_writer,
            .write_type = _binlog_opts.source.source_write_type};
    if (_tablet->enable_unique_key_merge_on_write()) {
        _historical_data_writer = std::make_unique<PrimaryKeyModelRowRetriever>();
        RETURN_IF_ERROR(_historical_data_writer->init(historical_row_retriever_context));
    } else if (_tablet->keys_type() == KeysType::AGG_KEYS) {
        // todo
    }
    return Status::OK();
}

Status RowBinlogSegmentWriter::append_block(const Block* block, size_t row_pos, size_t num_rows) {
    if (config::is_cloud_mode()) {
        // TODO(cjh): cloud mode
        return Status::NotSupported("append binlog");
    }

    if (_opts.write_type != DataWriteType::TYPE_DIRECT) {
        // append block directly because binlog data is completed
        RETURN_IF_ERROR(_append_direct_block(block, row_pos, num_rows));
        return Status::OK();
    }

    const TabletSchemaSPtr& source_schema = _binlog_opts.source.tablet_schema;
    if (UNLIKELY(source_schema == nullptr)) {
        return Status::InternalError("binlog<row> writer missing source_tablet_schema");
    }

    bool is_partial_update = _binlog_opts.source.partial_update_info &&
                             _binlog_opts.source.partial_update_info->is_partial_update() &&
                             _binlog_opts.source.source_write_type == DataWriteType::TYPE_DIRECT &&
                             !_binlog_opts.source.is_transient_rowset_writer;
    std::vector<uint32_t> partial_cids =
            is_partial_update ? _binlog_opts.source.partial_update_info->update_cids
                              : std::vector<uint32_t>();
    if (is_partial_update) {
        if (block->columns() <= source_schema->num_key_columns() ||
            block->columns() >= source_schema->num_columns()) {
            return Status::InternalError(fmt::format(
                    "illegal partial update block columns: {}, num key columns: {}, total "
                    "schema columns: {}",
                    block->columns(), _tablet_schema->num_key_columns(),
                    _tablet_schema->num_columns()));
        }

        // binlog don't need invisible column
        auto erase_invisible_col = std::remove_if(
                partial_cids.begin(), partial_cids.end(),
                [&](uint32_t cid) { return cid >= source_schema->num_visible_columns(); });
        partial_cids.erase(erase_invisible_col, partial_cids.end());
    }

    // get delete_sign_column from source block if has
    const Int8* delete_sign_column_data = nullptr;
    int32_t delete_sign_column_id = source_schema->delete_sign_idx();
    int32_t seq_col_id = source_schema->sequence_col_idx();
    if (is_partial_update) {
        delete_sign_column_id = -1;
        seq_col_id = -1;
        int32_t pos = 0;
        for (auto& cid : _binlog_opts.source.partial_update_info->update_cids) {
            if (cid == source_schema->delete_sign_idx()) {
                delete_sign_column_id = pos;
            } else if (cid == source_schema->sequence_col_idx()) {
                seq_col_id = pos;
            }
            pos++;
        }
    }
    if (delete_sign_column_id != -1) {
        const ColumnWithTypeAndName& delete_sign_column =
                block->get_by_position(delete_sign_column_id);

        auto& delete_sign_col = reinterpret_cast<const ColumnInt8&>(*(delete_sign_column.column));
        if (delete_sign_col.size() >= row_pos + num_rows) {
            delete_sign_column_data = delete_sign_col.get_data().data();
        }
    }

    // use full_block to save entrie row data
    Block full_block = source_schema->create_block();

    RETURN_IF_ERROR(_source_data_writer->prepare_by_source_block(block, row_pos, num_rows,
                                                                 partial_cids, &full_block));

    if (seq_col_id != -1) {
        RETURN_IF_ERROR(_source_data_writer->prepare_seq_column(block->get_by_position(seq_col_id),
                                                                source_schema->sequence_col_idx(),
                                                                row_pos, num_rows));
    }

    size_t max_normal_col_id = _normal_col_start_id + source_schema->num_visible_columns();
    RETURN_IF_ERROR(_source_data_writer->fill_normal_columns(_column_writers, _normal_col_start_id,
                                                             max_normal_col_id, partial_cids));

    // We read historical rows only when we really need them:
    // 1. partial update: build the full AFTER row.
    // 2. write_before: fill __BEFORE__* columns.
    // Otherwise we do not compare with old rows here, so row binlog op only
    // keeps the simple meaning: append for non-delete rows, delete for delete rows.
    if (is_partial_update || _write_before) {
        auto* pk_retriever =
                dynamic_cast<PrimaryKeyModelRowRetriever*>(_historical_data_writer.get());
        DCHECK(pk_retriever != nullptr);
        RETURN_IF_ERROR(pk_retriever->prepare_lookup_plan_from_source_columns(
                _source_data_writer->source_key_targets(), _source_data_writer->seq_target(),
                _binlog_opts.source.mow_context));
        RETURN_IF_ERROR(_historical_data_writer->retrieve_historical_row(delete_sign_column_data,
                                                                         row_pos, num_rows));
    }

    if (is_partial_update) {
        std::vector<uint32_t> row_binlog_missing_column_ids;
        _source_data_writer->filter_source_ids(
                _binlog_opts.source.partial_update_info->missing_cids,
                row_binlog_missing_column_ids);

        // build AFTER block (fill missing columns in full_block)
        RETURN_IF_ERROR(_historical_data_writer->build_after_block(&full_block, row_pos, num_rows));

        // write AFTER missing columns from full_block to segment via the
        // ColumnWriter::append(IColumn&) path.
        for (auto cid : row_binlog_missing_column_ids) {
            auto dest_cid = _normal_col_start_id + cid;
            const auto& col = full_block.get_by_position(cid).column;
            RETURN_IF_ERROR(_column_writers[dest_cid]->append(*col, row_pos, num_rows));
        }
    }

    // Snapshot writer views for source key columns; the source PK writers were
    // append()-ed in fill_normal_columns above. _fill_binlog_columns will fill
    // the remaining binlog-prefix slots into the leading positions of
    // _key_targets. build_key_index then consumes _key_targets.
    DCHECK(!_tablet_schema->has_sequence_col());
    _key_targets.assign(_tablet_schema->num_key_columns(), KeyEncodingTarget {});
    for (size_t i = _normal_col_start_id; i < _tablet_schema->num_key_columns(); i++) {
        auto* scalar_writer = assert_cast<ScalarColumnWriter*>(_column_writers[i].get());
        _key_targets[i] = {get_key_coder(_tablet_schema->column(i).type()),
                           &scalar_writer->view()};
    }

    std::vector<int64_t> no_operators = std::vector<int64_t> {};
    std::vector<int64_t>& operators =
            _historical_data_writer ? _historical_data_writer->get_operators() : no_operators;
    if (operators.empty()) {
        // haven't search historical row, only delete or append
        for (size_t block_pos = row_pos; block_pos < row_pos + num_rows; block_pos++) {
            bool have_delete_sign =
                    (delete_sign_column_data != nullptr && delete_sign_column_data[block_pos] != 0);
            if (have_delete_sign) {
                operators.emplace_back(ROW_BINLOG_DELETE);
            } else {
                operators.emplace_back(ROW_BINLOG_APPEND);
            }
        }
    }

    // The prefix block must outlive build_key_index below: for passthrough
    // types (LARGEINT lsn / BIGINT op,ts) the writer's staged view aliases the
    // block's column memory, and _key_targets point at those views.
    Block binlog_prefix_block;
    RETURN_IF_ERROR(_fill_binlog_columns(binlog_prefix_block, num_rows, operators));

    // row-binlog key don't need seq column
    RETURN_IF_ERROR(build_key_index(_key_targets, /*seq_target=*/nullptr,
                                    /*cluster_key_targets=*/{}, num_rows));

    if (_write_before) {
        RETURN_IF_ERROR(_fill_before_columns(num_rows));
    }

    _num_rows_written += num_rows;
    _source_data_writer->clear();
    if (_historical_data_writer) {
        _historical_data_writer->clear();
    }
    return Status::OK();
}

Status RowBinlogSegmentWriter::_append_direct_block(const Block* block, size_t row_pos,
                                                    size_t num_rows) {
    // ColumnWriter::append(IColumn&) feeds bytes through the writer chain; for
    // key columns we snapshot the writer's just-staged view into a per-batch
    // KeyEncodingTarget so build_key_index can encode keys without consulting
    // ColumnWriter internals.
    std::vector<KeyEncodingTarget> key_targets;
    for (size_t id = 0; id < _column_writers.size(); ++id) {
        auto cid = _column_ids[id];
        const auto& col = block->get_by_position(id).column;
        RETURN_IF_ERROR(_column_writers[id]->append(*col, row_pos, num_rows));
        if (_has_key && cid < _tablet_schema->num_key_columns()) {
            auto* scalar_writer = assert_cast<ScalarColumnWriter*>(_column_writers[id].get());
            key_targets.push_back(
                    {get_key_coder(_tablet_schema->column(cid).type()), &scalar_writer->view()});
        }
    }

    RETURN_IF_ERROR(build_key_index(key_targets, /*seq_target=*/nullptr,
                                    /*cluster_key_targets=*/{}, num_rows));

    _num_rows_written += num_rows;
    return Status::OK();
}

Status RowBinlogSegmentWriter::_fill_binlog_columns(Block& binlog_prefix_block, size_t num_rows,
                                                    const std::vector<int64_t>& op_types) {
    std::vector<uint32_t> binlog_cids = {_binlog_col_start_id, _binlog_col_start_id + 1,
                                         _binlog_col_start_id + 2};
    binlog_prefix_block = _tablet_schema->create_block_by_cids(binlog_cids);
    {
        auto binlog_prefix_columns_guard = binlog_prefix_block.mutate_columns_scoped();
        auto& binlog_prefix_columns = binlog_prefix_columns_guard.mutable_columns();
        // we can't get correct lsn number before commit, because we can't get the version before commit,
        // but we can fill auto-inc lsn to ensure the order first, then fill version when read single rowset.
        IColumn* lsn_col_ptr = binlog_prefix_columns[0].get();
        CHECK(_lsn_ids->size() >= num_rows) << _lsn_ids->size() << " vs " << num_rows;
        for (int i = 0; i < num_rows; i++) {
            assert_cast<ColumnInt128*>(lsn_col_ptr)
                    ->insert_value(static_cast<int128_t>(_lsn_ids->at(i)));
        }

        // wrong op only happens when partial-update, it will be fixed by delete bitmap when publish
        const FieldType op_col_type = _tablet_schema->column(binlog_cids[1]).type();
        IColumn* op_col_ptr = binlog_prefix_columns[1].get();
        auto* op_nullable_column = check_and_get_column<ColumnNullable>(op_col_ptr);
        IColumn* op_nested_column = op_nullable_column != nullptr
                                            ? &op_nullable_column->get_nested_column()
                                            : op_col_ptr;

        CHECK(op_types.size() >= num_rows) << op_types.size() << " vs " << num_rows;
        CHECK(op_col_type == FieldType::OLAP_FIELD_TYPE_BIGINT)
                << "row binlog op column type must be BIGINT, actual="
                << static_cast<int>(op_col_type);
        auto* op_int64_column = assert_cast<ColumnInt64*>(op_nested_column);
        for (int i = 0; i < num_rows; i++) {
            op_int64_column->insert_value(op_types[i]);
        }

        // we can't get correct timestamp when commit
        IColumn* ts_col_ptr = binlog_prefix_columns[2].get();
        auto timestamp = UnixMillis();
        auto* ts_nullable_column = check_and_get_column<ColumnNullable>(ts_col_ptr);
        if (ts_nullable_column != nullptr) {
            assert_cast<ColumnInt64*>(&ts_nullable_column->get_nested_column())
                    ->insert_many_vals(timestamp, num_rows);
        } else {
            assert_cast<ColumnInt64*>(ts_col_ptr)->insert_many_vals(timestamp, num_rows);
        }

        // finally update null map
        for (int i = 0; i < num_rows; i++) {
            //lsn_column->get_null_map_data().emplace_back(0);
            if (op_nullable_column != nullptr) {
                op_nullable_column->get_null_map_data().emplace_back(0);
            }
            if (ts_nullable_column != nullptr) {
                ts_nullable_column->get_null_map_data().emplace_back(0);
            }
        }
    }

    // LOG(INFO) << binlog_prefix_block.dump_data(0, num_rows);

    size_t col_pos_in_block = 0;
    for (auto& cid : binlog_cids) {
        const auto& col = binlog_prefix_block.get_by_position(col_pos_in_block++).column;
        RETURN_IF_ERROR(_column_writers[cid]->append(*col, 0, num_rows));
        if (cid < _tablet_schema->num_key_columns()) {
            auto* scalar_writer = assert_cast<ScalarColumnWriter*>(_column_writers[cid].get());
            _key_targets[cid] = {get_key_coder(_tablet_schema->column(cid).type()),
                                  &scalar_writer->view()};
        }
    }

    return Status::OK();
}

Status RowBinlogSegmentWriter::_fill_before_columns(size_t num_rows) {
    const TabletSchemaSPtr& source_schema = _binlog_opts.source.tablet_schema;
    if (UNLIKELY(source_schema == nullptr)) {
        return Status::InternalError("row binlog writer missing source_tablet_schema");
    }
    size_t value_column_num = source_schema->num_visible_value_columns();
    if (value_column_num == 0) {
        // No BEFORE columns in row binlog schema.
        return Status::OK();
    }

    uint32_t before_start_id = _before_col_start_id;
    uint32_t before_end_id = _before_col_start_id + cast_set<uint32_t>(value_column_num);

    std::vector<uint32_t> before_cids;
    for (uint32_t cid = before_start_id; cid < before_end_id; cid++) {
        before_cids.emplace_back(cid);
    }

    Block before_block = _tablet_schema->create_block_by_cids(before_cids);

    // Compatibility path: only fill empty BEFORE values.
    if (_fill_empty_before_value) {
        auto before_mutable_columns_guard = before_block.mutate_columns_scoped();
        for (auto& before_mutable_column : before_mutable_columns_guard.mutable_columns()) {
            auto* before_nullable_column =
                    reinterpret_cast<ColumnNullable*>(before_mutable_column.get());
            before_nullable_column->insert_many_defaults(num_rows);
        }
    } else {
        DCHECK(_historical_data_writer != nullptr);

        std::vector<uint32_t> value_cids;
        uint32_t value_start = cast_set<uint32_t>(source_schema->num_key_columns());
        uint32_t value_end = cast_set<uint32_t>(source_schema->num_visible_columns());
        for (uint32_t cid = value_start; cid < value_end; ++cid) {
            value_cids.emplace_back(cid);
        }

        DCHECK_EQ(before_cids.size(), value_cids.size());
        RETURN_IF_ERROR(_historical_data_writer->build_before_block(&before_block, value_cids, 0,
                                                                    num_rows));
    }

    size_t col_pos_in_block = 0;
    for (auto& cid : before_cids) {
        const auto& col = before_block.get_by_position(col_pos_in_block++).column;
        RETURN_IF_ERROR(_column_writers[cid]->append(*col, 0, num_rows));
    }

    return Status::OK();
}

Status RowBinlogSourceDataWriter::init() {
    if (UNLIKELY(_opt.source.tablet_schema == nullptr)) {
        return Status::InternalError("row binlog writer missing source_tablet_schema");
    }
    const auto& schema = *_opt.source.tablet_schema;
    for (uint32_t i = 0; i < schema.num_visible_columns(); i++) {
        _normal_column_ids.emplace_back(i);
    }
    // One slot per source-schema column. view/coder/pin are lazily filled on
    // first use, so we only pay KeyCoder lookup for cids that are actually
    // staged.
    _per_cid.resize(schema.num_columns());
    return Status::OK();
}

Status RowBinlogSourceDataWriter::prepare_by_source_block(
        const Block* block, size_t row_pos, size_t num_rows,
        std::vector<uint32_t>& partial_source_cids, Block* full_block) {
    size_t col_pos_in_block = 0;
    TabletSchemaSPtr tablet_schema = _opt.source.tablet_schema;
    const auto& including_cids =
            partial_source_cids.empty() ? _normal_column_ids : partial_source_cids;
    for (auto& cid : including_cids) {
        const ColumnWithTypeAndName& col = block->get_by_position(col_pos_in_block++);
        auto& slot = _per_cid[cid];
        RETURN_IF_ERROR(col.column->storage_view(tablet_schema->column(cid), row_pos, num_rows,
                                                  &slot.view));
        if (slot.coder == nullptr) {
            slot.coder = get_key_coder(tablet_schema->column(cid).type());
        }
        slot.pin = col.column;

        if (cid < tablet_schema->num_key_columns()) {
            _key_targets.push_back({slot.coder, &slot.view});
        }
        full_block->replace_by_position(cid, col.column);
    }
    _num_rows = num_rows;

    return Status::OK();
}

Status RowBinlogSourceDataWriter::prepare_seq_column(const ColumnWithTypeAndName& col,
                                                     int32_t seq_col_id_in_schema, size_t row_pos,
                                                     size_t num_rows) {
    const auto& tablet_schema = *_opt.source.tablet_schema;
    auto& slot = _per_cid[seq_col_id_in_schema];
    RETURN_IF_ERROR(col.column->storage_view(tablet_schema.column(seq_col_id_in_schema), row_pos,
                                              num_rows, &slot.view));
    if (slot.coder == nullptr) {
        slot.coder = get_key_coder(tablet_schema.column(seq_col_id_in_schema).type());
    }
    slot.pin = col.column;
    _seq_target = {slot.coder, &slot.view};
    _has_seq_target = true;
    return Status::OK();
}

Status RowBinlogSourceDataWriter::fill_normal_columns(
        std::vector<std::unique_ptr<ColumnWriter>>& column_writers, size_t start, size_t end,
        std::vector<uint32_t>& partial_source_cids) {
    DCHECK_EQ(end - start, _normal_column_ids.size());

    const auto& including_cids =
            partial_source_cids.empty() ? _normal_column_ids : partial_source_cids;
    for (size_t cid : including_cids) {
        DCHECK(column_writers[start + cid]->get_column()->type() ==
               _opt.source.tablet_schema->columns()[cid]->type())
                << cid;
        // The source column was pinned in _per_cid[cid].pin during prepare_*;
        // hand it to ColumnWriter::append(IColumn&) which drives the
        // storage-byte conversion internally.
        RETURN_IF_ERROR(
                column_writers[start + cid]->append(*_per_cid[cid].pin, 0, _num_rows));
    }

    return Status::OK();
}

void RowBinlogSourceDataWriter::clear() {
    _num_rows = 0;
    _key_targets.clear();
    _has_seq_target = false;
    for (auto& slot : _per_cid) {
        slot.pin.reset();
    }
}

} // namespace segment_v2
} // namespace doris
