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

#include "storage/segment/vertical_segment_writer.h"

#include <crc32c/crc32c.h>
#include <gen_cpp/olap_file.pb.h>
#include <gen_cpp/segment_v2.pb.h>
#include <parallel_hashmap/phmap.h>

#include <algorithm>
#include <cassert>
#include <memory>
#include <ostream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "cloud/config.h"
#include "common/cast_set.h"
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "common/logging.h" // LOG
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_number.h" // IWYU pragma: keep
#include "core/types.h"
#include "exec/common/variant_util.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "runtime/memory/mem_tracker.h"
#include "service/point_query_executor.h"
#include "storage/data_dir.h"
#include "storage/index/index_file_writer.h"
#include "storage/index/inverted/inverted_index_desc.h"
#include "storage/index/inverted/inverted_index_fs_directory.h"
#include "storage/index/primary_key_index.h"
#include "storage/index/short_key_index.h"
#include "storage/key_coder.h"
#include "storage/olap_common.h"
#include "storage/partial_update_info.h"
#include "storage/row_cursor.h" // RowCursor // IWYU pragma: keep
#include "storage/rowset/rowset_fwd.h"
#include "storage/rowset/rowset_writer_context.h" // RowsetWriterContext
#include "storage/rowset/segment_creator.h"
#include "storage/segment/column_writer.h" // ColumnWriter
#include "storage/segment/storage_view.h"
#include "storage/segment/encoding_info.h"
#include "storage/segment/external_col_meta_util.h"
#include "storage/segment/historical_row_retriever.h"
#include "storage/segment/page_io.h"
#include "storage/segment/page_pointer.h"
#include "storage/segment/segment_loader.h"
#include "storage/segment/variant/variant_ext_meta_writer.h"
#include "storage/tablet/base_tablet.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/utils.h"
#include "util/coding.h"
#include "util/debug_points.h"
#include "util/faststring.h"
#include "util/json/path_in_data.h"
#include "util/jsonb/serialize.h"
namespace doris::segment_v2 {

using namespace ErrorCode;
using namespace KeyConsts;

static constexpr const char* k_segment_magic = "D0R1";
static constexpr uint32_t k_segment_magic_length = 4;

inline std::string vertical_segment_writer_mem_tracker_name(uint32_t segment_id) {
    return "VerticalSegmentWriter:Segment-" + std::to_string(segment_id);
}

static ColumnBitmap* get_mutable_skip_bitmap_column(Block* block, size_t skip_bitmap_col_idx) {
    auto skip_bitmap_column =
            IColumn::mutate(std::move(block->get_by_position(skip_bitmap_col_idx).column));
    auto* skip_bitmap_column_ptr = assert_cast<ColumnBitmap*>(skip_bitmap_column.get());
    block->replace_by_position(skip_bitmap_col_idx, std::move(skip_bitmap_column));
    return skip_bitmap_column_ptr;
}

VerticalSegmentWriter::VerticalSegmentWriter(io::FileWriter* file_writer, uint32_t segment_id,
                                             TabletSchemaSPtr tablet_schema, BaseTabletSPtr tablet,
                                             DataDir* data_dir,
                                             const VerticalSegmentWriterOptions& opts,
                                             IndexFileWriter* index_file_writer)
        : _segment_id(segment_id),
          _tablet_schema(std::move(tablet_schema)),
          _tablet(std::move(tablet)),
          _data_dir(data_dir),
          _opts(opts),
          _file_writer(file_writer),
          _index_file_writer(index_file_writer),
          _mem_tracker(std::make_unique<MemTracker>(
                  vertical_segment_writer_mem_tracker_name(segment_id))),
          _mow_context(std::move(opts.mow_ctx)),
          _block_aggregator(*this) {
    CHECK_NOTNULL(file_writer);
    _num_sort_key_columns = _tablet_schema->num_key_columns();
    _num_short_key_columns = _tablet_schema->num_short_key_columns();
    if (!_is_mow_with_cluster_key()) {
        DCHECK(_num_sort_key_columns >= _num_short_key_columns)
                << ", table_id=" << _tablet_schema->table_id()
                << ", num_key_columns=" << _num_sort_key_columns
                << ", num_short_key_columns=" << _num_short_key_columns
                << ", cluster_key_columns=" << _tablet_schema->cluster_key_uids().size();
    }
    for (size_t cid = 0; cid < _num_sort_key_columns; ++cid) {
        const auto& column = _tablet_schema->column(cid);
        _key_coders.push_back(get_key_coder(column.type()));
        _key_index_size.push_back(cast_set<uint16_t>(column.index_length()));
    }
    // encode the sequence id into the primary key index
    if (_is_mow()) {
        if (_tablet_schema->has_sequence_col()) {
            const auto& column = _tablet_schema->column(_tablet_schema->sequence_col_idx());
            _seq_coder = get_key_coder(column.type());
        }
        // encode the rowid into the primary key index
        if (_is_mow_with_cluster_key()) {
            _rowid_coder = get_key_coder(FieldType::OLAP_FIELD_TYPE_UNSIGNED_INT);
            // primary keys
            _primary_key_coders.swap(_key_coders);
            // cluster keys
            _key_coders.clear();
            _key_index_size.clear();
            _num_sort_key_columns = _tablet_schema->cluster_key_uids().size();
            for (auto cid : _tablet_schema->cluster_key_uids()) {
                const auto& column = _tablet_schema->column_by_uid(cid);
                _key_coders.push_back(get_key_coder(column.type()));
                _key_index_size.push_back(cast_set<uint16_t>(column.index_length()));
            }
        }
    }
}

VerticalSegmentWriter::~VerticalSegmentWriter() {
    _mem_tracker->release(_mem_tracker->consumption());
}

void VerticalSegmentWriter::_init_column_meta(ColumnMetaPB* meta, uint32_t column_id,
                                              const TabletColumn& column,
                                              const ColumnWriterOptions& opts) {
    meta->set_column_id(column_id);
    meta->set_type(int(column.type()));
    meta->set_length(cast_set<int32_t>(column.length()));
    meta->set_encoding(EncodingInfo::resolve_default_encoding(opts.storage_format, column));
    meta->set_compression(_opts.compression_type);
    meta->set_is_nullable(column.is_nullable());
    meta->set_default_value(column.default_value());
    meta->set_precision(column.precision());
    meta->set_frac(column.frac());
    if (column.has_path_info()) {
        column.path_info_ptr()->to_protobuf(meta->mutable_column_path_info(),
                                            column.parent_unique_id());
    }
    meta->set_unique_id(column.unique_id());
    for (uint32_t i = 0; i < column.get_subtype_count(); ++i) {
        _init_column_meta(meta->add_children_columns(), column_id, column.get_sub_column(i), opts);
    }
    if (column.is_variant_type()) {
        meta->set_variant_max_subcolumns_count(column.variant_max_subcolumns_count());
        meta->set_variant_enable_doc_mode(column.variant_enable_doc_mode());
    }
    meta->set_result_is_nullable(column.get_result_is_nullable());
    meta->set_function_name(column.get_aggregation_name());
    meta->set_be_exec_version(column.get_be_exec_version());
}

Status VerticalSegmentWriter::_create_column_writer(uint32_t cid, const TabletColumn& column,
                                                    const TabletSchemaSPtr& tablet_schema) {
    ColumnWriterOptions opts;
    opts.meta = _footer.add_columns();
    opts.storage_format = tablet_schema->storage_format();

    _init_column_meta(opts.meta, cid, column, opts);

    // now we create zone map for key columns in AGG_KEYS or all column in UNIQUE_KEYS or DUP_KEYS
    // except for columns whose type don't support zone map.
    opts.need_zone_map = column.is_key() || tablet_schema->keys_type() != KeysType::AGG_KEYS;
    opts.need_bloom_filter = column.is_bf_column();
    if (opts.need_bloom_filter) {
        opts.bf_options.fpp =
                tablet_schema->has_bf_fpp() ? tablet_schema->bloom_filter_fpp() : 0.05;
    }
    auto* tablet_index = tablet_schema->get_ngram_bf_index(column.unique_id());
    if (tablet_index) {
        opts.need_bloom_filter = true;
        opts.is_ngram_bf_index = true;
        //narrow convert from int32_t to uint8_t and uint16_t which is dangerous
        auto gram_size = tablet_index->get_gram_size();
        auto gram_bf_size = tablet_index->get_gram_bf_size();
        if (gram_size > 256 || gram_size < 1) {
            return Status::NotSupported("Do not support ngram bloom filter for ngram_size: ",
                                        gram_size);
        }
        if (gram_bf_size > 65535 || gram_bf_size < 64) {
            return Status::NotSupported("Do not support ngram bloom filter for bf_size: ",
                                        gram_bf_size);
        }
        opts.gram_size = cast_set<uint8_t>(gram_size);
        opts.gram_bf_size = cast_set<uint16_t>(gram_bf_size);
    }

    bool skip_inverted_index = false;
    if (_opts.rowset_ctx != nullptr) {
        // skip write inverted index for index compaction column
        skip_inverted_index =
                _opts.rowset_ctx->columns_to_do_index_compaction.contains(column.unique_id());
    }
    // skip write inverted index on load if skip_write_index_on_load is true
    if (_opts.write_type == DataWriteType::TYPE_DIRECT &&
        tablet_schema->skip_write_index_on_load()) {
        skip_inverted_index = true;
    }
    if (!skip_inverted_index) {
        auto inverted_indexs = tablet_schema->inverted_indexs(column);
        if (!inverted_indexs.empty()) {
            opts.inverted_indexes = inverted_indexs;
            opts.need_inverted_index = true;
            DCHECK(_index_file_writer != nullptr);
        }
    }
    opts.index_file_writer = _index_file_writer;

    if (const auto& index = tablet_schema->ann_index(column); index != nullptr) {
        opts.ann_index = index;
        opts.need_ann_index = true;
        DCHECK(_index_file_writer != nullptr);
        opts.index_file_writer = _index_file_writer;
    }

#define DISABLE_INDEX_IF_FIELD_TYPE(TYPE)                     \
    if (column.type() == FieldType::OLAP_FIELD_TYPE_##TYPE) { \
        opts.need_zone_map = false;                           \
        opts.need_bloom_filter = false;                       \
    }

    DISABLE_INDEX_IF_FIELD_TYPE(STRUCT)
    DISABLE_INDEX_IF_FIELD_TYPE(ARRAY)
    DISABLE_INDEX_IF_FIELD_TYPE(JSONB)
    DISABLE_INDEX_IF_FIELD_TYPE(AGG_STATE)
    DISABLE_INDEX_IF_FIELD_TYPE(MAP)
    DISABLE_INDEX_IF_FIELD_TYPE(BITMAP)
    DISABLE_INDEX_IF_FIELD_TYPE(HLL)
    DISABLE_INDEX_IF_FIELD_TYPE(QUANTILE_STATE)
    DISABLE_INDEX_IF_FIELD_TYPE(VARIANT)

#undef DISABLE_INDEX_IF_FIELD_TYPE

#undef CHECK_FIELD_TYPE

    int64_t storage_page_size = _tablet_schema->storage_page_size();
    // storage_page_size must be between 4KB and 10MB.
    if (storage_page_size >= 4096 && storage_page_size <= 10485760) {
        opts.data_page_size = storage_page_size;
    }
    opts.dict_page_size = _tablet_schema->storage_dict_page_size();
    DBUG_EXECUTE_IF("VerticalSegmentWriter._create_column_writer.storage_page_size", {
        auto table_id = DebugPoints::instance()->get_debug_param_or_default<int64_t>(
                "VerticalSegmentWriter._create_column_writer.storage_page_size", "table_id",
                INT_MIN);
        auto target_data_page_size = DebugPoints::instance()->get_debug_param_or_default<int64_t>(
                "VerticalSegmentWriter._create_column_writer.storage_page_size",
                "storage_page_size", INT_MIN);
        if (table_id == INT_MIN || target_data_page_size == INT_MIN) {
            return Status::Error<ErrorCode::INTERNAL_ERROR>(
                    "Debug point parameters missing: either 'table_id' or 'storage_page_size' not "
                    "set.");
        }
        if (table_id == _tablet_schema->table_id() &&
            opts.data_page_size != target_data_page_size) {
            return Status::Error<ErrorCode::INTERNAL_ERROR>(
                    "Mismatch in 'storage_page_size': expected size does not match the current "
                    "data page size. "
                    "Expected: " +
                    std::to_string(target_data_page_size) +
                    ", Actual: " + std::to_string(opts.data_page_size) + ".");
        }
    })
    if (column.is_row_store_column()) {
        // smaller page size for row store column; encoding is already set to PLAIN /
        // PLAIN_V2 by _init_column_meta via resolve_default_encoding().
        auto page_size = _tablet_schema->row_store_page_size();
        opts.data_page_size =
                (page_size > 0) ? page_size : segment_v2::ROW_STORE_PAGE_SIZE_DEFAULT_VALUE;
    }

    opts.rowset_ctx = _opts.rowset_ctx;
    opts.file_writer = _file_writer;
    opts.compression_type = _opts.compression_type;
    opts.footer = &_footer;
    opts.input_rs_readers = _opts.rowset_ctx->input_rs_readers;

    std::unique_ptr<ColumnWriter> writer;
    RETURN_IF_ERROR(ColumnWriter::create(opts, &column, _file_writer, &writer));
    RETURN_IF_ERROR(writer->init());
    _column_writers[cid] = std::move(writer);
    return Status::OK();
};

Status VerticalSegmentWriter::init() {
    DCHECK(_column_writers.empty());
    if (_opts.compression_type == UNKNOWN_COMPRESSION) {
        _opts.compression_type = _tablet_schema->compression_type();
    }
    _column_writers.resize(_tablet_schema->num_columns());
    // we don't need the short key index for unique key merge on write table.
    if (_is_mow()) {
        size_t seq_col_length = 0;
        if (_tablet_schema->has_sequence_col()) {
            seq_col_length =
                    _tablet_schema->column(_tablet_schema->sequence_col_idx()).length() + 1;
        }
        size_t rowid_length = 0;
        if (_is_mow_with_cluster_key()) {
            rowid_length = PrimaryKeyIndexReader::ROW_ID_LENGTH;
            _short_key_index_builder.reset(
                    new ShortKeyIndexBuilder(_segment_id, _opts.num_rows_per_block));
        }
        _primary_key_index_builder.reset(
                new PrimaryKeyIndexBuilder(_file_writer, seq_col_length, rowid_length));
        RETURN_IF_ERROR(_primary_key_index_builder->init());
    } else {
        _short_key_index_builder.reset(
                new ShortKeyIndexBuilder(_segment_id, _opts.num_rows_per_block));
    }
    return Status::OK();
}

void VerticalSegmentWriter::_maybe_invalid_row_cache(const std::string& key) const {
    // Just invalid row cache for simplicity, since the rowset is not visible at present.
    // If we update/insert cache, if load failed rowset will not be visible but cached data
    // will be visible, and lead to inconsistency.
    if (!config::disable_storage_row_cache && _tablet_schema->has_row_store_for_all_columns() &&
        _opts.write_type == DataWriteType::TYPE_DIRECT) {
        // invalidate cache
        RowCache::instance()->erase({_opts.rowset_ctx->tablet_id, key});
    }
}

Status VerticalSegmentWriter::_append_row_store_column(const Block& block, size_t row_pos,
                                                       size_t num_rows, uint32_t cid) {
    DCHECK(_tablet_schema->column(cid).is_row_store_column());
    if (num_rows == 0) {
        return Status::OK();
    }
    DCHECK_LE(row_pos + num_rows, block.rows());

    auto serdes = create_data_type_serdes(block.get_data_types());
    std::unordered_set<int32_t> row_store_cids_set(_tablet_schema->row_columns_uids().begin(),
                                                   _tablet_schema->row_columns_uids().end());
    size_t end_pos = row_pos + num_rows;
    size_t batch_rows = _opts.num_rows_per_block;
    static constexpr size_t kRowStoreBatchBytes = 4 * 1024 * 1024;
    DCHECK_GT(batch_rows, 0);
    for (size_t pos = row_pos; pos < end_pos;) {
        size_t max_rows = std::min(batch_rows, end_pos - pos);
        auto row_column = ColumnString::create();
        auto* row_store_column = row_column.get();
        size_t rows = JsonbSerializeUtil::block_to_jsonb(
                *_tablet_schema, block, *row_store_column,
                cast_set<int>(_tablet_schema->num_columns()), serdes, row_store_cids_set, pos,
                max_rows, kRowStoreBatchBytes);
        DCHECK_GT(rows, 0);

        RETURN_IF_ERROR(_column_writers[cid]->append(*row_column, 0, rows));
        pos += rows;
    }
    return Status::OK();
}

Status VerticalSegmentWriter::_probe_key_for_mow(
        std::string key, std::size_t segment_pos, bool have_input_seq_column, bool have_delete_sign,
        const std::vector<RowsetSharedPtr>& specified_rowsets,
        std::vector<std::unique_ptr<SegmentCacheHandle>>& segment_caches,
        bool& has_default_or_nullable, std::vector<bool>& use_default_or_null_flag,
        const std::function<void(const RowLocation& loc)>& found_cb,
        const std::function<Status()>& not_found_cb, PartialUpdateStats& stats) {
    RowLocation loc;
    // save rowset shared ptr so this rowset wouldn't delete
    RowsetSharedPtr rowset;
    auto st = _tablet->lookup_row_key(key, _tablet_schema.get(), have_input_seq_column,
                                      specified_rowsets, &loc, _mow_context->max_version,
                                      segment_caches, &rowset);
    if (st.is<KEY_NOT_FOUND>()) {
        if (!have_delete_sign) {
            RETURN_IF_ERROR(not_found_cb());
        }
        ++stats.num_rows_new_added;
        has_default_or_nullable = true;
        use_default_or_null_flag.emplace_back(true);
        return Status::OK();
    }
    if (!st.ok() && !st.is<KEY_ALREADY_EXISTS>()) {
        LOG(WARNING) << "failed to lookup row key, error: " << st;
        return st;
    }

    // 1. if the delete sign is marked, it means that the value columns of the row will not
    //    be read. So we don't need to read the missing values from the previous rows.
    // 2. the one exception is when there are sequence columns in the table, we need to read
    //    the sequence columns, otherwise it may cause the merge-on-read based compaction
    //    policy to produce incorrect results

    // 3. In flexible partial update, we may delete the existing rows before if there exists
    //    insert after delete in one load. In this case, the insert should also be treated
    //    as newly inserted rows, note that the sequence column value is filled in
    //    BlockAggregator::aggregate_for_insert_after_delete() if this row doesn't specify the sequence column
    if (st.is<KEY_ALREADY_EXISTS>() || (have_delete_sign && !_tablet_schema->has_sequence_col()) ||
        (_opts.rowset_ctx->partial_update_info->is_flexible_partial_update() &&
         _mow_context->delete_bitmap->contains(
                 {loc.rowset_id, loc.segment_id, DeleteBitmap::TEMP_VERSION_COMMON}, loc.row_id))) {
        has_default_or_nullable = true;
        use_default_or_null_flag.emplace_back(true);
    } else {
        // partial update should not contain invisible columns
        use_default_or_null_flag.emplace_back(false);
        _rsid_to_rowset.emplace(rowset->rowset_id(), rowset);
        found_cb(loc);
    }

    if (st.is<KEY_ALREADY_EXISTS>()) {
        // although we need to mark delete current row, we still need to read missing columns
        // for this row, we need to ensure that each column is aligned
        _mow_context->delete_bitmap->add(
                {_opts.rowset_ctx->rowset_id, _segment_id, DeleteBitmap::TEMP_VERSION_COMMON},
                cast_set<uint32_t>(segment_pos));
        ++stats.num_rows_deleted;
    } else {
        _mow_context->delete_bitmap->add(
                {loc.rowset_id, loc.segment_id, DeleteBitmap::TEMP_VERSION_COMMON}, loc.row_id);
        ++stats.num_rows_updated;
    }
    return Status::OK();
}

Status VerticalSegmentWriter::_check_column_writer_disk_capacity(size_t cid) {
    if (_data_dir != nullptr &&
        _data_dir->reach_capacity_limit(_column_writers[cid]->estimate_buffer_size())) {
        return Status::Error<DISK_REACH_CAPACITY_LIMIT>("disk {} exceed capacity limit.",
                                                        _data_dir->path_hash());
    }
    return Status::OK();
}

Status VerticalSegmentWriter::_finalize_column_writer_and_update_meta(size_t cid) {
    RETURN_IF_ERROR(_column_writers[cid]->finish());
    RETURN_IF_ERROR(_column_writers[cid]->write_data());

    auto* column_meta = _column_writers[cid]->get_column_meta();
    column_meta->set_compressed_data_bytes(
            _column_writers[cid]->get_total_compressed_data_pages_bytes());
    column_meta->set_uncompressed_data_bytes(
            _column_writers[cid]->get_total_uncompressed_data_pages_bytes());
    column_meta->set_raw_data_bytes(_column_writers[cid]->get_raw_data_bytes());
    return Status::OK();
}

Status VerticalSegmentWriter::_partial_update_preconditions_check(size_t row_pos,
                                                                  bool is_flexible_update) {
    if (!_is_mow()) {
        auto msg = fmt::format(
                "Can only do partial update on merge-on-write unique table, but found: "
                "keys_type={}, _opts.enable_unique_key_merge_on_write={}, tablet_id={}",
                _tablet_schema->keys_type(), _opts.enable_unique_key_merge_on_write,
                _tablet->tablet_id());
        DCHECK(false) << msg;
        return Status::InternalError<false>(msg);
    }
    if (_opts.rowset_ctx->partial_update_info == nullptr) {
        auto msg =
                fmt::format("partial_update_info should not be nullptr, please check, tablet_id={}",
                            _tablet->tablet_id());
        DCHECK(false) << msg;
        return Status::InternalError<false>(msg);
    }
    if (!is_flexible_update) {
        if (!_opts.rowset_ctx->partial_update_info->is_fixed_partial_update()) {
            auto msg = fmt::format(
                    "in fixed partial update code, but update_mode={}, please check, tablet_id={}",
                    _opts.rowset_ctx->partial_update_info->update_mode(), _tablet->tablet_id());
            DCHECK(false) << msg;
            return Status::InternalError<false>(msg);
        }
    } else {
        if (!_opts.rowset_ctx->partial_update_info->is_flexible_partial_update()) {
            auto msg = fmt::format(
                    "in flexible partial update code, but update_mode={}, please check, "
                    "tablet_id={}",
                    _opts.rowset_ctx->partial_update_info->update_mode(), _tablet->tablet_id());
            DCHECK(false) << msg;
            return Status::InternalError<false>(msg);
        }
    }
    if (row_pos != 0) {
        auto msg = fmt::format("row_pos should be 0, but found {}, tablet_id={}", row_pos,
                               _tablet->tablet_id());
        DCHECK(false) << msg;
        return Status::InternalError<false>(msg);
    }
    return Status::OK();
}

// for partial update, we should do following steps to fill content of block:
// 1. append the including (update) columns; snapshot their writers' staged
//    views into KeyEncodingTargets for the key / seq columns
// 2. encode each row's pk from the staged views, look up its location
//    {rowset_id, segment_id, row_id}, build a read plan, and fill the
//    missing columns into full_block
// 3. append the filled missing columns and build the primary key index
Status VerticalSegmentWriter::_append_block_with_partial_content(RowsInBlock& data,
                                                                 Block& full_block) {
    DBUG_EXECUTE_IF("_append_block_with_partial_content.block", DBUG_BLOCK);

    RETURN_IF_ERROR(_partial_update_preconditions_check(data.row_pos, false));
    // create full block and fill with input columns
    full_block = _tablet_schema->create_block();
    const auto& including_cids = _opts.rowset_ctx->partial_update_info->update_cids;
    size_t input_id = 0;
    for (auto i : including_cids) {
        full_block.replace_by_position(i, data.block->get_by_position(input_id++).column);
    }

    if (_opts.rowset_ctx->write_type != DataWriteType::TYPE_COMPACTION &&
        _tablet_schema->num_variant_columns() > 0) {
        RETURN_IF_ERROR(variant_util::parse_and_materialize_variant_columns(
                full_block, *_tablet_schema, including_cids));
    }
    bool have_input_seq_column = false;
    // write including columns via the IColumn-based path; snapshot the
    // writer's just-staged view into a per-batch KeyEncodingTarget so the
    // row-by-row key encode below reads post-conversion bytes without
    // consulting ColumnWriter internals.
    std::vector<KeyEncodingTarget> key_targets;
    KeyEncodingTarget seq_target;
    bool has_seq_target = false;
    uint32_t segment_start_pos = 0;
    for (auto cid : including_cids) {
        RETURN_IF_ERROR(_create_column_writer(cid, _tablet_schema->column(cid), _tablet_schema));
        // here we get segment column row num before append data.
        segment_start_pos = cast_set<uint32_t>(_column_writers[cid]->get_next_rowid());
        const auto& col = full_block.get_by_position(cid).column;
        RETURN_IF_ERROR(_column_writers[cid]->append(*col, data.row_pos, data.num_rows));
        if (cid < _num_sort_key_columns) {
            auto* scalar_writer = assert_cast<ScalarColumnWriter*>(_column_writers[cid].get());
            key_targets.push_back(
                    {get_key_coder(_tablet_schema->column(cid).type()), &scalar_writer->view()});
        } else if (_tablet_schema->has_sequence_col() &&
                   cid == _tablet_schema->sequence_col_idx()) {
            auto* scalar_writer = assert_cast<ScalarColumnWriter*>(_column_writers[cid].get());
            seq_target = {_seq_coder, &scalar_writer->view()};
            has_seq_target = true;
            have_input_seq_column = true;
        }
        RETURN_IF_ERROR(_finalize_column_writer_and_update_meta(cid));
    }

    bool has_default_or_nullable = false;
    std::vector<bool> use_default_or_null_flag;
    use_default_or_null_flag.reserve(data.num_rows);
    const auto* delete_signs =
            BaseTablet::get_delete_sign_column_data(full_block, data.row_pos + data.num_rows);

    DBUG_EXECUTE_IF("VerticalSegmentWriter._append_block_with_partial_content.sleep",
                    { sleep(60); })
    const std::vector<RowsetSharedPtr>& specified_rowsets = _mow_context->rowset_ptrs;
    std::vector<std::unique_ptr<SegmentCacheHandle>> segment_caches(specified_rowsets.size());

    FixedReadPlan read_plan;

    // locate rows in base data
    PartialUpdateStats stats;

    for (size_t block_pos = data.row_pos; block_pos < data.row_pos + data.num_rows; block_pos++) {
        // block   segment
        //   2   ->   0
        //   3   ->   1
        //   4   ->   2
        //   5   ->   3
        // here row_pos = 2, num_rows = 4.
        size_t delta_pos = block_pos - data.row_pos;
        size_t segment_pos = segment_start_pos + delta_pos;
        std::string key;
        RETURN_IF_ERROR(_full_encode_keys(key_targets, delta_pos, &key));
        _maybe_invalid_row_cache(key);
        if (have_input_seq_column) {
            // have_input_seq_column implies has_seq_target (set together above).
            DCHECK(has_seq_target);
            RETURN_IF_ERROR(_encode_seq_column(&seq_target, delta_pos, &key));
        }
        // If the table have sequence column, and the include-cids don't contain the sequence
        // column, we need to update the primary key index builder at the end of this method.
        // At that time, we have a valid sequence column to encode the key with seq col.
        if (!_tablet_schema->has_sequence_col() || have_input_seq_column) {
            RETURN_IF_ERROR(_primary_key_index_builder->add_item(key));
        }

        // mark key with delete sign as deleted.
        bool have_delete_sign = (delete_signs != nullptr && delete_signs[block_pos] != 0);

        auto not_found_cb = [&]() {
            return _opts.rowset_ctx->partial_update_info->handle_new_key(
                    *_tablet_schema, [&]() -> std::string {
                        return data.block->dump_one_line(block_pos,
                                                         cast_set<int>(_num_sort_key_columns));
                    });
        };
        auto update_read_plan = [&](const RowLocation& loc) {
            read_plan.prepare_to_read(loc, segment_pos);
        };
        RETURN_IF_ERROR(_probe_key_for_mow(std::move(key), segment_pos, have_input_seq_column,
                                           have_delete_sign, specified_rowsets, segment_caches,
                                           has_default_or_nullable, use_default_or_null_flag,
                                           update_read_plan, not_found_cb, stats));
    }
    CHECK_EQ(use_default_or_null_flag.size(), data.num_rows);

    if (config::enable_merge_on_write_correctness_check) {
        _tablet->add_sentinel_mark_to_delete_bitmap(_mow_context->delete_bitmap.get(),
                                                    *_mow_context->rowset_ids);
    }

    // read to fill full_block
    RETURN_IF_ERROR(read_plan.fill_missing_columns(
            _opts.rowset_ctx->make_historical_row_retriever_context(), _rsid_to_rowset,
            *_tablet_schema, full_block, use_default_or_null_flag, has_default_or_nullable,
            segment_start_pos, data.block));

    if (_tablet_schema->num_variant_columns() > 0) {
        RETURN_IF_ERROR(variant_util::parse_and_materialize_variant_columns(
                full_block, *_tablet_schema, _opts.rowset_ctx->partial_update_info->missing_cids));
    }

    // convert missing columns and send to column writer
    const auto& missing_cids = _opts.rowset_ctx->partial_update_info->missing_cids;
    for (auto cid : missing_cids) {
        RETURN_IF_ERROR(_create_column_writer(cid, _tablet_schema->column(cid), _tablet_schema));
        if (_tablet_schema->column(cid).is_row_store_column()) {
            RETURN_IF_ERROR(_append_row_store_column(full_block, data.row_pos, data.num_rows, cid));
            RETURN_IF_ERROR(_finalize_column_writer_and_update_meta(cid));
            continue;
        }
        const auto& col = full_block.get_by_position(cid).column;
        RETURN_IF_ERROR(_column_writers[cid]->append(*col, data.row_pos, data.num_rows));
        if (_tablet_schema->has_sequence_col() && !have_input_seq_column &&
            cid == _tablet_schema->sequence_col_idx()) {
            DCHECK(!has_seq_target);
            auto* scalar_writer = assert_cast<ScalarColumnWriter*>(_column_writers[cid].get());
            seq_target = {_seq_coder, &scalar_writer->view()};
            has_seq_target = true;
        }
        RETURN_IF_ERROR(_finalize_column_writer_and_update_meta(cid));
    }

    _num_rows_updated += stats.num_rows_updated;
    _num_rows_deleted += stats.num_rows_deleted;
    _num_rows_new_added += stats.num_rows_new_added;
    _num_rows_filtered += stats.num_rows_filtered;
    if (_tablet_schema->has_sequence_col() && !have_input_seq_column) {
        DCHECK(has_seq_target);
        if (_num_rows_written != data.row_pos ||
            _primary_key_index_builder->num_rows() != _num_rows_written) {
            return Status::InternalError(
                    "Correctness check failed, _num_rows_written: {}, row_pos: {}, primary key "
                    "index builder num rows: {}",
                    _num_rows_written, data.row_pos, _primary_key_index_builder->num_rows());
        }
        RETURN_IF_ERROR(_generate_primary_key_index_from_views(key_targets, &seq_target,
                                                               data.num_rows,
                                                               /*need_sort=*/false));
    }

    _num_rows_written += data.num_rows;
    DCHECK_EQ(_primary_key_index_builder->num_rows(), _num_rows_written)
            << "primary key index builder num rows(" << _primary_key_index_builder->num_rows()
            << ") not equal to segment writer's num rows written(" << _num_rows_written << ")";
    return Status::OK();
}

Status VerticalSegmentWriter::_append_block_with_flexible_partial_content(RowsInBlock& data,
                                                                          Block& full_block) {
    RETURN_IF_ERROR(_partial_update_preconditions_check(data.row_pos, true));

    // data.block has the same schema with full_block
    DCHECK(data.block->columns() == _tablet_schema->num_columns());

    // create full block and fill with sort key columns
    full_block = _tablet_schema->create_block();

    // Use _num_rows_written instead of creating column writer 0, since all column writers
    // should have the same row count, which equals _num_rows_written.
    uint32_t segment_start_pos = cast_set<uint32_t>(_num_rows_written);

    DCHECK(_tablet_schema->has_skip_bitmap_col());
    auto skip_bitmap_col_idx = _tablet_schema->skip_bitmap_col_idx();

    bool has_default_or_nullable = false;
    std::vector<bool> use_default_or_null_flag;
    use_default_or_null_flag.reserve(data.num_rows);

    int32_t seq_map_col_unique_id = _opts.rowset_ctx->partial_update_info->sequence_map_col_uid();
    bool schema_has_sequence_col = _tablet_schema->has_sequence_col();

    DBUG_EXECUTE_IF("VerticalSegmentWriter._append_block_with_flexible_partial_content.sleep",
                    { sleep(60); })
    const std::vector<RowsetSharedPtr>& specified_rowsets = _mow_context->rowset_ptrs;
    std::vector<std::unique_ptr<SegmentCacheHandle>> segment_caches(specified_rowsets.size());

    // Ensure all primary key column writers and sequence column writer are created
    // before aggregate_for_flexible_partial_update — it stages the pk/seq columns
    // into BlockAggregator's own StorageViews for the in-batch dedup key compares.
    for (uint32_t cid = 0; cid < _tablet_schema->num_key_columns(); ++cid) {
        RETURN_IF_ERROR(_create_column_writer(cid, _tablet_schema->column(cid), _tablet_schema));
    }
    if (schema_has_sequence_col) {
        uint32_t cid = _tablet_schema->sequence_col_idx();
        RETURN_IF_ERROR(_create_column_writer(cid, _tablet_schema->column(cid), _tablet_schema));
    }
    // For the post-read-plan _generate_primary_key_index_from_views call below
    // we maintain per-batch KeyEncodingTargets for the PK + seq columns.
    std::vector<KeyEncodingTarget> key_targets;
    KeyEncodingTarget seq_target;
    bool has_seq_target = false;

    // 1. aggregate duplicate rows in block
    RETURN_IF_ERROR(_block_aggregator.aggregate_for_flexible_partial_update(
            const_cast<Block*>(data.block), data.num_rows, specified_rowsets, segment_caches));
    if (data.block->rows() != data.num_rows) {
        data.num_rows = data.block->rows();
    }

    // 2. Re-stage PK + seq columns into BlockAggregator's internal StorageViews
    //    over the final (post-dedup) block. _generate_flexible_read_plan below
    //    encodes its probe keys from these views via key_targets()/seq_target().
    RETURN_IF_ERROR(_block_aggregator.convert_pk_columns(const_cast<Block*>(data.block),
                                                         data.row_pos, data.num_rows));
    RETURN_IF_ERROR(_block_aggregator.convert_seq_column(const_cast<Block*>(data.block),
                                                         data.row_pos, data.num_rows));

    auto* mutable_block = const_cast<Block*>(data.block);
    std::vector<BitmapValue>* skip_bitmaps =
            &get_mutable_skip_bitmap_column(mutable_block, skip_bitmap_col_idx)->get_data();
    const auto* delete_signs =
            BaseTablet::get_delete_sign_column_data(*data.block, data.row_pos + data.num_rows);
    DCHECK(delete_signs != nullptr);

    // 3. fill the sort key columns of full_block from the aggregated block
    for (std::size_t cid {0}; cid < _tablet_schema->num_key_columns(); cid++) {
        full_block.replace_by_position(cid, data.block->get_by_position(cid).column);
    }

    // 4. write primary key columns data via the IColumn-based path. data.block
    // was already aggregated; its key columns are what should land on disk.
    // Snapshot each writer's view() into key_targets for the PK-index encode
    // at step 9.
    for (std::size_t cid {0}; cid < _tablet_schema->num_key_columns(); cid++) {
        DCHECK(_column_writers[cid]->get_next_rowid() == _num_rows_written);
        const auto& col = data.block->get_by_position(cid).column;
        RETURN_IF_ERROR(_column_writers[cid]->append(*col, data.row_pos, data.num_rows));
        DCHECK(_column_writers[cid]->get_next_rowid() == _num_rows_written + data.num_rows);
        auto* scalar_writer = assert_cast<ScalarColumnWriter*>(_column_writers[cid].get());
        key_targets.push_back(
                {get_key_coder(_tablet_schema->column(cid).type()), &scalar_writer->view()});
        RETURN_IF_ERROR(_finalize_column_writer_and_update_meta(cid));
    }

    // 5. genreate read plan
    FlexibleReadPlan read_plan {_tablet_schema->has_row_store_for_all_columns()};
    PartialUpdateStats stats;
    RETURN_IF_ERROR(_generate_flexible_read_plan(
            read_plan, data, segment_start_pos, schema_has_sequence_col, seq_map_col_unique_id,
            skip_bitmaps, delete_signs, specified_rowsets, segment_caches, has_default_or_nullable,
            use_default_or_null_flag, stats));
    CHECK_EQ(use_default_or_null_flag.size(), data.num_rows);

    if (config::enable_merge_on_write_correctness_check) {
        _tablet->add_sentinel_mark_to_delete_bitmap(_mow_context->delete_bitmap.get(),
                                                    *_mow_context->rowset_ids);
    }

    // 6. read according plan to fill full_block
    RETURN_IF_ERROR(read_plan.fill_non_primary_key_columns(
            _opts.rowset_ctx->make_historical_row_retriever_context(), _rsid_to_rowset,
            *_tablet_schema, full_block, use_default_or_null_flag, has_default_or_nullable,
            segment_start_pos, cast_set<uint32_t>(data.row_pos), data.block, skip_bitmaps));

    // TODO(bobhan1): should we replace the skip bitmap column with empty bitmaps to reduce storage occupation?
    // this column is not needed in read path for merge-on-write table

    // 7. fill row store column
    for (auto cid = _tablet_schema->num_key_columns(); cid < _tablet_schema->num_columns(); cid++) {
        if (!_tablet_schema->column(cid).is_row_store_column()) {
            continue;
        }
        RETURN_IF_ERROR(_create_column_writer(cast_set<uint32_t>(cid), _tablet_schema->column(cid),
                                              _tablet_schema));
        RETURN_IF_ERROR(_append_row_store_column(full_block, data.row_pos, data.num_rows,
                                                 cast_set<uint32_t>(cid)));
        RETURN_IF_ERROR(_finalize_column_writer_and_update_meta(cid));
    }

    std::vector<uint32_t> column_ids;
    for (uint32_t i = 0; i < _tablet_schema->num_columns(); ++i) {
        column_ids.emplace_back(i);
    }
    if (_opts.rowset_ctx->write_type != DataWriteType::TYPE_COMPACTION &&
        _tablet_schema->num_variant_columns() > 0) {
        RETURN_IF_ERROR(variant_util::parse_and_materialize_variant_columns(
                full_block, *_tablet_schema, column_ids));
    }

    // 8. encode and write all non-primary key columns (including sequence column
    // if exists) via the IColumn-based path. For the seq column, also snapshot
    // its writer view into seq_target for the PK-index encode at step 9.
    for (auto cid = _tablet_schema->num_key_columns(); cid < _tablet_schema->num_columns(); cid++) {
        if (_tablet_schema->column(cid).is_row_store_column()) {
            continue;
        }
        if (cid != _tablet_schema->sequence_col_idx()) {
            RETURN_IF_ERROR(_create_column_writer(cast_set<uint32_t>(cid),
                                                  _tablet_schema->column(cid), _tablet_schema));
        }
        DCHECK(_column_writers[cid]->get_next_rowid() == _num_rows_written);
        const auto& col = full_block.get_by_position(cid).column;
        RETURN_IF_ERROR(_column_writers[cid]->append(*col, data.row_pos, data.num_rows));
        DCHECK(_column_writers[cid]->get_next_rowid() == _num_rows_written + data.num_rows);
        if (schema_has_sequence_col && cid == _tablet_schema->sequence_col_idx()) {
            auto* scalar_writer = assert_cast<ScalarColumnWriter*>(_column_writers[cid].get());
            seq_target = {_seq_coder, &scalar_writer->view()};
            has_seq_target = true;
        }
        RETURN_IF_ERROR(_finalize_column_writer_and_update_meta(cid));
    }

    _num_rows_updated += stats.num_rows_updated;
    _num_rows_deleted += stats.num_rows_deleted;
    _num_rows_new_added += stats.num_rows_new_added;
    _num_rows_filtered += stats.num_rows_filtered;

    if (_num_rows_written != data.row_pos ||
        _primary_key_index_builder->num_rows() != _num_rows_written) {
        return Status::InternalError(
                "Correctness check failed, _num_rows_written: {}, row_pos: {}, primary key "
                "index builder num rows: {}",
                _num_rows_written, data.row_pos, _primary_key_index_builder->num_rows());
    }

    // 9. build primary key index using KeyEncodingTarget-based encoders.
    RETURN_IF_ERROR(_generate_primary_key_index_from_views(
            key_targets, has_seq_target ? &seq_target : nullptr, data.num_rows,
            /*need_sort=*/false));

    _num_rows_written += data.num_rows;
    DCHECK_EQ(_primary_key_index_builder->num_rows(), _num_rows_written)
            << "primary key index builder num rows(" << _primary_key_index_builder->num_rows()
            << ") not equal to segment writer's num rows written(" << _num_rows_written << ")";
    return Status::OK();
}

Status VerticalSegmentWriter::_generate_encoded_default_seq_value(const TabletSchema& tablet_schema,
                                                                  const PartialUpdateInfo& info,
                                                                  std::string* encoded_value) {
    const auto& seq_column = tablet_schema.column(tablet_schema.sequence_col_idx());
    auto block = tablet_schema.create_block_by_cids(
            {cast_set<uint32_t>(tablet_schema.sequence_col_idx())});
    if (seq_column.has_default_value()) {
        auto idx = tablet_schema.sequence_col_idx() - tablet_schema.num_key_columns();
        const auto& default_value = info.default_values[idx];
        StringRef str {default_value};
        RETURN_IF_ERROR(block.get_by_position(0).type->get_serde()->default_from_string(
                str, *block.get_by_position(0).column->assert_mutable().get()));

    } else {
        block.get_by_position(0).column->assert_mutable()->insert_default();
    }
    DCHECK_EQ(block.rows(), 1);
    // Stage 1 row of the seq column to its storage-format bytes, then encode
    // null/normal marker + key bytes (same shape as _encode_seq_column above).
    StorageView view;
    RETURN_IF_ERROR(block.get_by_position(0).column->storage_view(seq_column, 0, 1, &view));
    const auto* coder = get_key_coder(seq_column.type());
    if (storage_view_is_null_at(view, 0)) {
        encoded_value->push_back(static_cast<char>(KeyConsts::KEY_NULL_FIRST_MARKER));
        encoded_value->append(seq_column.length(),
                              static_cast<char>(KeyConsts::KEY_MINIMAL_MARKER));
        return Status::OK();
    }
    return storage_view_encode_full_key_ascending(coder, view, 0, encoded_value);
}

Status VerticalSegmentWriter::_generate_flexible_read_plan(
        FlexibleReadPlan& read_plan, RowsInBlock& data, size_t segment_start_pos,
        bool schema_has_sequence_col, int32_t seq_map_col_unique_id,
        std::vector<BitmapValue>* skip_bitmaps, const signed char* delete_signs,
        const std::vector<RowsetSharedPtr>& specified_rowsets,
        std::vector<std::unique_ptr<SegmentCacheHandle>>& segment_caches,
        bool& has_default_or_nullable, std::vector<bool>& use_default_or_null_flag,
        PartialUpdateStats& stats) {
    int32_t delete_sign_col_unique_id =
            _tablet_schema->column(_tablet_schema->delete_sign_idx()).unique_id();
    int32_t seq_col_unique_id =
            (_tablet_schema->has_sequence_col()
                     ? _tablet_schema->column(_tablet_schema->sequence_col_idx()).unique_id()
                     : -1);
    for (size_t block_pos = data.row_pos; block_pos < data.row_pos + data.num_rows; block_pos++) {
        size_t delta_pos = block_pos - data.row_pos;
        size_t segment_pos = segment_start_pos + delta_pos;
        auto& skip_bitmap = skip_bitmaps->at(block_pos);

        std::string key;
        // The probe key is encoded from BlockAggregator's StorageViews (staged
        // over the post-dedup block at step 2), not from the writers' views —
        // the writers were already finalized at step 4 and their views cover
        // the same bytes, but the aggregator's targets carry the coders too.
        const auto& ka_key_targets = _block_aggregator.key_targets();
        for (const auto& t : ka_key_targets) {
            RETURN_IF_ERROR(
                    storage_view_encode_full_key_ascending(t.coder, *t.view, delta_pos, &key));
        }
        _maybe_invalid_row_cache(key);
        bool row_has_sequence_col =
                (schema_has_sequence_col && !skip_bitmap.contains(seq_col_unique_id));
        if (row_has_sequence_col) {
            const auto* seq_t = _block_aggregator.seq_target();
            DCHECK(seq_t != nullptr);
            if (storage_view_is_null_at(*seq_t->view, delta_pos)) {
                key.push_back(KEY_NULL_FIRST_MARKER);
                size_t seq_col_length =
                        _tablet_schema->column(_tablet_schema->sequence_col_idx()).length();
                key.append(seq_col_length, KEY_MINIMAL_MARKER);
            } else {
                RETURN_IF_ERROR(storage_view_encode_full_key_ascending(seq_t->coder, *seq_t->view,
                                                                        delta_pos, &key));
            }
        }

        // mark key with delete sign as deleted.
        bool have_delete_sign =
                (!skip_bitmap.contains(delete_sign_col_unique_id) && delete_signs[block_pos] != 0);

        auto not_found_cb = [&]() {
            return _opts.rowset_ctx->partial_update_info->handle_new_key(
                    *_tablet_schema,
                    [&]() -> std::string {
                        return data.block->dump_one_line(block_pos,
                                                         cast_set<int>(_num_sort_key_columns));
                    },
                    &skip_bitmap);
        };
        auto update_read_plan = [&](const RowLocation& loc) {
            read_plan.prepare_to_read(loc, segment_pos, skip_bitmap);
        };

        RETURN_IF_ERROR(_probe_key_for_mow(std::move(key), segment_pos, row_has_sequence_col,
                                           have_delete_sign, specified_rowsets, segment_caches,
                                           has_default_or_nullable, use_default_or_null_flag,
                                           update_read_plan, not_found_cb, stats));
    }
    return Status::OK();
}

Status VerticalSegmentWriter::batch_block(const Block* block, size_t row_pos, size_t num_rows) {
    if (_opts.rowset_ctx->partial_update_info &&
        _opts.rowset_ctx->partial_update_info->is_partial_update() &&
        _opts.write_type == DataWriteType::TYPE_DIRECT &&
        !_opts.rowset_ctx->is_transient_rowset_writer) {
        if (_opts.rowset_ctx->partial_update_info->is_flexible_partial_update()) {
            if (block->columns() != _tablet_schema->num_columns()) {
                return Status::InvalidArgument(
                        "illegal flexible partial update block columns, block columns = {}, "
                        "tablet_schema columns = {}",
                        block->dump_structure(), _tablet_schema->dump_structure());
            }
        } else {
            if (block->columns() < _tablet_schema->num_key_columns() ||
                block->columns() >= _tablet_schema->num_columns()) {
                return Status::InvalidArgument(fmt::format(
                        "illegal partial update block columns: {}, num key columns: {}, total "
                        "schema columns: {}",
                        block->columns(), _tablet_schema->num_key_columns(),
                        _tablet_schema->num_columns()));
            }
        }
    } else if (block->columns() != _tablet_schema->num_columns()) {
        return Status::InvalidArgument(
                "illegal block columns, block columns = {}, tablet_schema columns = {}",
                block->dump_structure(), _tablet_schema->dump_structure());
    }
    _batched_blocks.emplace_back(block, row_pos, num_rows);
    return Status::OK();
}

Status VerticalSegmentWriter::write_batch() {
    if (_opts.rowset_ctx->partial_update_info &&
        _opts.rowset_ctx->partial_update_info->is_partial_update() &&
        _opts.write_type == DataWriteType::TYPE_DIRECT &&
        !_opts.rowset_ctx->is_transient_rowset_writer) {
        bool is_flexible_partial_update =
                _opts.rowset_ctx->partial_update_info->is_flexible_partial_update();
        Block full_block;
        for (auto& data : _batched_blocks) {
            if (is_flexible_partial_update) {
                RETURN_IF_ERROR(_append_block_with_flexible_partial_content(data, full_block));
            } else {
                RETURN_IF_ERROR(_append_block_with_partial_content(data, full_block));
            }
        }
        return Status::OK();
    }
    // Row column should be filled here when it's a directly write from memtable
    // or it's schema change write(since column data type maybe changed, so we should reubild)
    bool should_write_row_store_column = _opts.write_type == DataWriteType::TYPE_DIRECT ||
                                         _opts.write_type == DataWriteType::TYPE_SCHEMA_CHANGE;
    if (should_write_row_store_column) {
        for (uint32_t cid = 0; cid < _tablet_schema->num_columns(); ++cid) {
            if (!_tablet_schema->column(cid).is_row_store_column()) {
                continue;
            }
            RETURN_IF_ERROR(
                    _create_column_writer(cid, _tablet_schema->column(cid), _tablet_schema));
            for (auto& data : _batched_blocks) {
                RETURN_IF_ERROR(
                        _append_row_store_column(*data.block, data.row_pos, data.num_rows, cid));
            }
            RETURN_IF_ERROR(_check_column_writer_disk_capacity(cid));
            RETURN_IF_ERROR(_finalize_column_writer_and_update_meta(cid));
        }
    }

    std::vector<uint32_t> column_ids;
    for (uint32_t i = 0; i < _tablet_schema->num_columns(); ++i) {
        column_ids.emplace_back(i);
    }
    if (_opts.rowset_ctx->write_type != DataWriteType::TYPE_COMPACTION &&
        _tablet_schema->num_variant_columns() > 0) {
        for (auto& data : _batched_blocks) {
            RETURN_IF_ERROR(variant_util::parse_and_materialize_variant_columns(
                    const_cast<Block&>(*data.block), *_tablet_schema, column_ids));
        }
    }

    // Single unified ColumnWriter::append(IColumn&) path. We first append all
    // data for every cid across every batched block (page builders demand the
    // contiguous stream). Key encoding is then done block-by-block in a second
    // loop, freshly staging the keys for each block.
    for (uint32_t cid = 0; cid < _tablet_schema->num_columns(); ++cid) {
        if (should_write_row_store_column && _tablet_schema->column(cid).is_row_store_column()) {
            continue;
        }
        RETURN_IF_ERROR(_create_column_writer(cid, _tablet_schema->column(cid), _tablet_schema));
        for (auto& data : _batched_blocks) {
            const auto& col = data.block->get_by_position(cid).column;
            RETURN_IF_ERROR(_column_writers[cid]->append(*col, data.row_pos, data.num_rows));
        }
        RETURN_IF_ERROR(_check_column_writer_disk_capacity(cid));
        RETURN_IF_ERROR(_finalize_column_writer_and_update_meta(cid));
    }

    for (auto& data : _batched_blocks) {
        // find row positions for short key index.
        std::vector<size_t> short_key_pos;
        if (_short_key_row_pos == 0 && _num_rows_written == 0) {
            short_key_pos.push_back(0);
        }
        while (_short_key_row_pos + _opts.num_rows_per_block <
               _num_rows_written + data.num_rows) {
            _short_key_row_pos += _opts.num_rows_per_block;
            short_key_pos.push_back(_short_key_row_pos - _num_rows_written);
        }

        // Stage this block's key / seq / cluster-key columns fresh into the
        // owned per-cid StorageView slots. The writers were appended (and
        // finalized) cid-major across ALL batched blocks above, so each
        // writer's internal view only holds the LAST block's bytes — useless
        // for the per-block key encode here. Build per-batch KeyEncodingTargets
        // pointing at the owned views instead.
        std::vector<KeyEncodingTarget> key_targets;
        key_targets.reserve(_tablet_schema->num_key_columns());
        KeyEncodingTarget seq_target;
        bool has_seq_target = false;
        std::map<uint32_t, KeyEncodingTarget> cluster_by_uid;
        for (uint32_t cid = 0; cid < _tablet_schema->num_columns(); ++cid) {
            if (should_write_row_store_column &&
                _tablet_schema->column(cid).is_row_store_column()) {
                continue;
            }
            const auto& col = data.block->get_by_position(cid).column;
            if (cid < _tablet_schema->num_key_columns()) {
                auto& view = _owned_key_views[cid];
                RETURN_IF_ERROR(
                        col->storage_view(_tablet_schema->column(cid), data.row_pos, data.num_rows,
                                          &view));
                key_targets.push_back(
                        {get_key_coder(_tablet_schema->column(cid).type()), &view});
            } else if (_tablet_schema->has_sequence_col() &&
                       cid == _tablet_schema->sequence_col_idx()) {
                RETURN_IF_ERROR(
                        col->storage_view(_tablet_schema->column(cid), data.row_pos, data.num_rows,
                                          &_owned_seq_view));
                seq_target = {_seq_coder, &_owned_seq_view};
                has_seq_target = true;
            }
            if (_is_mow_with_cluster_key()) {
                auto uid = _tablet_schema->column(cid).unique_id();
                if (std::find(_tablet_schema->cluster_key_uids().begin(),
                              _tablet_schema->cluster_key_uids().end(),
                              uid) != _tablet_schema->cluster_key_uids().end()) {
                    auto& view = _owned_cluster_key_views[cid];
                    RETURN_IF_ERROR(
                            col->storage_view(_tablet_schema->column(cid), data.row_pos,
                                              data.num_rows, &view));
                    cluster_by_uid[uid] = {get_key_coder(_tablet_schema->column(cid).type()),
                                           &view};
                }
            }
        }

        if (_is_mow_with_cluster_key()) {
            // 1. primary key index uses primary-key targets, with rowid suffix +
            //    in-memory sort.
            RETURN_IF_ERROR(_generate_primary_key_index_from_views(
                    key_targets, has_seq_target ? &seq_target : nullptr, data.num_rows,
                    /*need_sort=*/true));
            // 2. short key index uses cluster-key targets in cluster_key_uids order.
            std::vector<KeyEncodingTarget> cluster_key_targets;
            cluster_key_targets.reserve(_tablet_schema->cluster_key_uids().size());
            for (const auto& uid : _tablet_schema->cluster_key_uids()) {
                auto it = cluster_by_uid.find(uid);
                if (it == cluster_by_uid.end()) {
                    return Status::InternalError(
                            "could not find cluster key column with unique_id=" +
                            std::to_string(uid) + " in tablet schema");
                }
                cluster_key_targets.push_back(it->second);
            }
            RETURN_IF_ERROR(_generate_short_key_index_from_views(cluster_key_targets, data,
                                                                 short_key_pos));
        } else if (_is_mow()) {
            RETURN_IF_ERROR(_generate_primary_key_index_from_views(
                    key_targets, has_seq_target ? &seq_target : nullptr, data.num_rows,
                    /*need_sort=*/false));
        } else {
            RETURN_IF_ERROR(
                    _generate_short_key_index_from_views(key_targets, data, short_key_pos));
        }
        _num_rows_written += data.num_rows;
    }

    _batched_blocks.clear();
    return Status::OK();
}

// KeyEncodingTarget-based key encoders. Each target pairs a KeyCoder with
// the already-staged storage-byte view for the current write batch.

Status VerticalSegmentWriter::_full_encode_keys(const std::vector<KeyEncodingTarget>& key_targets,
                                                size_t pos, std::string* encoded_keys) {
    for (const auto& t : key_targets) {
        DCHECK(t.coder != nullptr && t.view != nullptr);
        RETURN_IF_ERROR(
                storage_view_encode_full_key_ascending(t.coder, *t.view, pos, encoded_keys));
    }
    return Status::OK();
}

Status VerticalSegmentWriter::_encode_keys(const std::vector<KeyEncodingTarget>& key_targets,
                                           size_t pos, std::string* encoded_keys) {
    for (size_t cid = 0; cid < key_targets.size(); ++cid) {
        const auto& t = key_targets[cid];
        DCHECK(t.coder != nullptr && t.view != nullptr);
        RETURN_IF_ERROR(storage_view_encode_short_key_ascending(t.coder, *t.view, pos, encoded_keys,
                                                                  _key_index_size[cid]));
    }
    return Status::OK();
}

Status VerticalSegmentWriter::_encode_seq_column(const KeyEncodingTarget* seq_target, size_t pos,
                                                  std::string* encoded_keys) {
    DCHECK(seq_target != nullptr && seq_target->coder != nullptr && seq_target->view != nullptr);
    if (storage_view_is_null_at(*seq_target->view, pos)) {
        encoded_keys->push_back(KEY_NULL_FIRST_MARKER);
        size_t seq_col_length = _tablet_schema->column(_tablet_schema->sequence_col_idx()).length();
        encoded_keys->append(seq_col_length, KEY_MINIMAL_MARKER);
        return Status::OK();
    }
    return storage_view_encode_full_key_ascending(seq_target->coder, *seq_target->view, pos,
                                                   encoded_keys);
}

Status VerticalSegmentWriter::_generate_primary_key_index_from_views(
        const std::vector<KeyEncodingTarget>& primary_key_targets,
        const KeyEncodingTarget* seq_target, size_t num_rows, bool need_sort) {
    if (!need_sort) { // mow without cluster key
        std::string last_key;
        for (size_t pos = 0; pos < num_rows; pos++) {
            std::string key;
            RETURN_IF_ERROR(_full_encode_keys(primary_key_targets, pos, &key));
            _maybe_invalid_row_cache(key);
            if (_tablet_schema->has_sequence_col()) {
                RETURN_IF_ERROR(_encode_seq_column(seq_target, pos, &key));
            }
            DCHECK(key.compare(last_key) > 0)
                    << "found duplicate key or key is not sorted! current key: " << key
                    << ", last key: " << last_key;
            RETURN_IF_ERROR(_primary_key_index_builder->add_item(key));
            last_key = std::move(key);
        }
        return Status::OK();
    }
    // mow with cluster key: encode + sort all primary keys in memory, then add.
    std::vector<std::string> primary_keys;
    primary_keys.reserve(num_rows);
    for (uint32_t pos = 0; pos < num_rows; pos++) {
        std::string key;
        RETURN_IF_ERROR(_full_encode_keys(primary_key_targets, pos, &key));
        _maybe_invalid_row_cache(key);
        if (_tablet_schema->has_sequence_col()) {
            RETURN_IF_ERROR(_encode_seq_column(seq_target, pos, &key));
        }
        _encode_rowid(pos, &key);
        primary_keys.emplace_back(std::move(key));
    }
    std::sort(primary_keys.begin(), primary_keys.end());
    std::string last_key;
    for (const auto& key : primary_keys) {
        DCHECK(key.compare(last_key) > 0)
                << "found duplicate key or key is not sorted! current key: " << key
                << ", last key: " << last_key;
        RETURN_IF_ERROR(_primary_key_index_builder->add_item(key));
        last_key = key;
    }
    return Status::OK();
}

Status VerticalSegmentWriter::_generate_short_key_index_from_views(
        const std::vector<KeyEncodingTarget>& key_targets, RowsInBlock& data,
        const std::vector<size_t>& short_key_pos) {
    std::string min_key;
    std::string max_key;
    RETURN_IF_ERROR(_full_encode_keys(key_targets, 0, &min_key));
    RETURN_IF_ERROR(_full_encode_keys(key_targets, data.num_rows - 1, &max_key));
    _set_min_key(Slice(min_key));
    _set_max_key(Slice(max_key));
    DCHECK(Slice(_max_key.data(), _max_key.size())
                   .compare(Slice(_min_key.data(), _min_key.size())) >= 0)
            << "key is not sorted! min key: " << _min_key << ", max key: " << _max_key;

    std::vector<KeyEncodingTarget> short_targets(key_targets.begin(),
                                                  key_targets.begin() + _num_short_key_columns);

    std::string last_key;
    for (const auto pos : short_key_pos) {
        std::string key;
        RETURN_IF_ERROR(_encode_keys(short_targets, pos, &key));
        DCHECK(key.compare(last_key) >= 0)
                << "key is not sorted! current key: " << key << ", last key: " << last_key;
        RETURN_IF_ERROR(_short_key_index_builder->add_item(key));
        last_key = std::move(key);
    }
    return Status::OK();
}

void VerticalSegmentWriter::_encode_rowid(const uint32_t rowid, std::string* encoded_keys) {
    encoded_keys->push_back(KEY_NORMAL_MARKER);
    _rowid_coder->full_encode_ascending(&rowid, encoded_keys);
}

// TODO(lingbin): Currently this function does not include the size of various indexes,
// We should make this more precise.
uint64_t VerticalSegmentWriter::_estimated_remaining_size() {
    // footer_size(4) + checksum(4) + segment_magic(4)
    uint64_t size = 12;
    if (_is_mow_with_cluster_key()) {
        size += _primary_key_index_builder->size() + _short_key_index_builder->size();
    } else if (_is_mow()) {
        size += _primary_key_index_builder->size();
    } else {
        size += _short_key_index_builder->size();
    }

    // update the mem_tracker of segment size
    _mem_tracker->consume(size - _mem_tracker->consumption());
    return size;
}

Status VerticalSegmentWriter::finalize_columns_index(uint64_t* index_size) {
    uint64_t index_start = _file_writer->bytes_appended();
    RETURN_IF_ERROR(_write_ordinal_index());
    RETURN_IF_ERROR(_write_zone_map());
    RETURN_IF_ERROR(_write_inverted_index());
    RETURN_IF_ERROR(_write_ann_index());
    RETURN_IF_ERROR(_write_bloom_filter_index());

    *index_size = _file_writer->bytes_appended() - index_start;
    if (_is_mow_with_cluster_key()) {
        RETURN_IF_ERROR(_write_short_key_index());
        *index_size = _file_writer->bytes_appended() - index_start;
        RETURN_IF_ERROR(_write_primary_key_index());
        *index_size += _primary_key_index_builder->disk_size();
    } else if (_is_mow()) {
        RETURN_IF_ERROR(_write_primary_key_index());
        // IndexedColumnWriter write data pages mixed with segment data, we should use
        // the stat from primary key index builder.
        *index_size += _primary_key_index_builder->disk_size();
    } else {
        RETURN_IF_ERROR(_write_short_key_index());
        *index_size = _file_writer->bytes_appended() - index_start;
    }

    // reset all column writers and data_conveter
    clear();

    return Status::OK();
}

Status VerticalSegmentWriter::finalize_footer(uint64_t* segment_file_size) {
    RETURN_IF_ERROR(_write_footer());
    // finish
    RETURN_IF_ERROR(_file_writer->close(true));
    *segment_file_size = _file_writer->bytes_appended();
    if (*segment_file_size == 0) {
        return Status::Corruption("Bad segment, file size = 0");
    }
    return Status::OK();
}

Status VerticalSegmentWriter::finalize(uint64_t* segment_file_size, uint64_t* index_size) {
    MonotonicStopWatch timer;
    timer.start();
    // check disk capacity
    if (_data_dir != nullptr &&
        _data_dir->reach_capacity_limit((int64_t)_estimated_remaining_size())) {
        return Status::Error<DISK_REACH_CAPACITY_LIMIT>("disk {} exceed capacity limit.",
                                                        _data_dir->path_hash());
    }
    _row_count = _num_rows_written;
    _num_rows_written = 0;
    // write index
    RETURN_IF_ERROR(finalize_columns_index(index_size));
    // write footer
    RETURN_IF_ERROR(finalize_footer(segment_file_size));

    if (timer.elapsed_time() > 5000000000L) {
        LOG(INFO) << "segment flush consumes a lot time_ns " << timer.elapsed_time()
                  << ", segmemt_size " << *segment_file_size;
    }
    return Status::OK();
}

void VerticalSegmentWriter::clear() {
    for (auto& column_writer : _column_writers) {
        column_writer.reset();
    }
    _column_writers.clear();
}

// write ordinal index after data has been written
Status VerticalSegmentWriter::_write_ordinal_index() {
    for (auto& column_writer : _column_writers) {
        RETURN_IF_ERROR(column_writer->write_ordinal_index());
    }
    return Status::OK();
}

Status VerticalSegmentWriter::_write_zone_map() {
    for (auto& column_writer : _column_writers) {
        RETURN_IF_ERROR(column_writer->write_zone_map());
    }
    return Status::OK();
}

Status VerticalSegmentWriter::_write_inverted_index() {
    for (auto& column_writer : _column_writers) {
        RETURN_IF_ERROR(column_writer->write_inverted_index());
    }
    return Status::OK();
}

Status VerticalSegmentWriter::_write_ann_index() {
    for (auto& column_writer : _column_writers) {
        RETURN_IF_ERROR(column_writer->write_ann_index());
    }
    return Status::OK();
}

Status VerticalSegmentWriter::_write_bloom_filter_index() {
    for (auto& column_writer : _column_writers) {
        RETURN_IF_ERROR(column_writer->write_bloom_filter_index());
    }
    return Status::OK();
}

Status VerticalSegmentWriter::_write_short_key_index() {
    std::vector<Slice> body;
    PageFooterPB footer;
    RETURN_IF_ERROR(_short_key_index_builder->finalize(_row_count, &body, &footer));
    PagePointer pp;
    // short key index page is not compressed right now
    RETURN_IF_ERROR(PageIO::write_page(_file_writer, body, footer, &pp));
    pp.to_proto(_footer.mutable_short_key_index_page());
    return Status::OK();
}

Status VerticalSegmentWriter::_write_primary_key_index() {
    CHECK_EQ(_primary_key_index_builder->num_rows(), _row_count);
    return _primary_key_index_builder->finalize(_footer.mutable_primary_key_index_meta());
}

Status VerticalSegmentWriter::_write_footer() {
    _footer.set_num_rows(_row_count);

    // Decide whether to externalize ColumnMetaPB by tablet default, and stamp footer version

    if (_tablet_schema->storage_format() == TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V3) {
        _footer.set_version(SEGMENT_FOOTER_VERSION_V3_EXT_COL_META);
        VLOG_DEBUG << "use external column meta";
        // External ColumnMetaPB writing (optional)
        RETURN_IF_ERROR(ExternalColMetaUtil::write_external_column_meta(
                _file_writer, &_footer, _opts.compression_type,
                [this](const std::vector<Slice>& slices) { return _write_raw_data(slices); }));
    }

    // Footer := SegmentFooterPB, FooterPBSize(4), FooterPBChecksum(4), MagicNumber(4)
    VLOG_DEBUG << "footer " << _footer.DebugString();
    std::string footer_buf;
    if (!_footer.SerializeToString(&footer_buf)) {
        return Status::InternalError("failed to serialize segment footer");
    }

    faststring fixed_buf;
    // footer's size
    put_fixed32_le(&fixed_buf, cast_set<uint32_t>(footer_buf.size()));
    // footer's checksum
    uint32_t checksum = crc32c::Crc32c(footer_buf.data(), footer_buf.size());
    put_fixed32_le(&fixed_buf, checksum);
    // Append magic number. we don't write magic number in the header because
    // that will need an extra seek when reading
    fixed_buf.append(k_segment_magic, k_segment_magic_length);

    std::vector<Slice> slices {footer_buf, fixed_buf};
    return _write_raw_data(slices);
}

Status VerticalSegmentWriter::_write_raw_data(const std::vector<Slice>& slices) {
    RETURN_IF_ERROR(_file_writer->appendv(&slices[0], slices.size()));
    return Status::OK();
}

Slice VerticalSegmentWriter::min_encoded_key() {
    return (_primary_key_index_builder == nullptr) ? Slice(_min_key.data(), _min_key.size())
                                                   : _primary_key_index_builder->min_key();
}
Slice VerticalSegmentWriter::max_encoded_key() {
    return (_primary_key_index_builder == nullptr) ? Slice(_max_key.data(), _max_key.size())
                                                   : _primary_key_index_builder->max_key();
}

void VerticalSegmentWriter::_set_min_max_key(const Slice& key) {
    if (UNLIKELY(_is_first_row)) {
        _min_key.append(key.get_data(), key.get_size());
        _is_first_row = false;
    }
    if (key.compare(_max_key) > 0) {
        _max_key.clear();
        _max_key.append(key.get_data(), key.get_size());
    }
}

void VerticalSegmentWriter::_set_min_key(const Slice& key) {
    if (UNLIKELY(_is_first_row)) {
        _min_key.append(key.get_data(), key.get_size());
        _is_first_row = false;
    }
}

void VerticalSegmentWriter::_set_max_key(const Slice& key) {
    _max_key.clear();
    _max_key.append(key.get_data(), key.get_size());
}

inline bool VerticalSegmentWriter::_is_mow() {
    return _tablet_schema->keys_type() == UNIQUE_KEYS && _opts.enable_unique_key_merge_on_write;
}

inline bool VerticalSegmentWriter::_is_mow_with_cluster_key() {
    return _is_mow() && !_tablet_schema->cluster_key_uids().empty();
}

} // namespace doris::segment_v2
