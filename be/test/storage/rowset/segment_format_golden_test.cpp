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

// Golden tests pinning the physical segment (.dat) format produced by every
// write path, so that write-path refactors can be verified to be byte-for-byte
// format preserving. Each case writes deterministic data through one
// generation path and records, per produced segment:
//   - a byte fingerprint (md5 of the raw file; for schemas containing VARIANT
//     columns the footer region is excluded because VariantStatisticsPB uses
//     protobuf map fields whose serialization order is process-dependent)
//   - a content fingerprint (md5 over the decoded rows plus a normalized,
//     layout-insensitive footer dump; MoW cases also fold in the delete
//     bitmap produced during flush)
//   - the footer column layout order (diagnostic aid when bytes diverge)
//
// Golden data lives in be/test/expected_result/storage/segment_format/ and is
// (re)generated with:
//   run-be-ut.sh --run --gen_out --filter='SegmentFormatGolden*'
// Checking mode (default) compares against the committed golden files, so
// running this suite on a write-path refactor branch proves the produced
// segments are identical to the ones the base branch produces.

#include <gen_cpp/olap_common.pb.h>
#include <gen_cpp/olap_file.pb.h>
#include <gen_cpp/segment_v2.pb.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "agent/be_exec_version_manager.h"
#include "common/config.h"
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column.h"
#include "core/column/column_complex.h"
#include "core/column/column_fixed_length_object.h"
#include "core/column/column_string.h"
#include "core/column/column_variant.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type_serde/data_type_serde.h"
#include "core/field.h"
#include "core/value/bitmap_value.h"
#include "core/value/hll.h"
#include "core/value/quantile_state.h"
#include "exec/sink/autoinc_buffer.h"
#include "io/fs/file_reader.h"
#include "io/fs/local_file_system.h"
#include "io/io_common.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_profile.h"
#include "storage/binlog.h"
#include "storage/data_dir.h"
#include "storage/delete/calc_delete_bitmap_executor.h"
#include "storage/index/index_writer.h"
#include "storage/merger.h"
#include "storage/olap_common.h"
#include "storage/olap_define.h"
#include "storage/options.h"
#include "storage/partial_update_info.h"
#include "storage/rowid_conversion.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/rowset/group_rowset_writer.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_reader.h"
#include "storage/rowset/rowset_reader_context.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/schema.h"
#include "storage/segment/column_meta_accessor.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet.h"
#include "storage/tablet/tablet_manager.h"
#include "storage/tablet/tablet_meta.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/utils.h"
#include "testutil/creators.h"
#include "testutil/test_util.h"
#include "testutil/variant_util.h"
#include "util/coding.h"
#include "util/md5.h"
#include "util/slice.h"
#include "util/threadpool.h"

namespace doris {

namespace {

constexpr uint32_t MAX_PATH_LEN = 1024;
constexpr std::string_view kTestDir = "/ut_dir/segment_format_golden_test";

StorageEngine* s_engine = nullptr;

Status read_segment_footer(const io::FileReaderSPtr& fr, segment_v2::SegmentFooterPB* footer,
                           uint64_t* footer_start) {
    const auto file_size = fr->size();
    if (file_size < 12) {
        return Status::Corruption("file too small");
    }
    uint8_t fixed_buf[12];
    size_t bytes_read = 0;
    RETURN_IF_ERROR(fr->read_at(file_size - 12, Slice(fixed_buf, sizeof(fixed_buf)), &bytes_read));
    if (bytes_read != sizeof(fixed_buf)) {
        return Status::Corruption("short read footer trailer");
    }
    const uint32_t footer_length = decode_fixed32_le(fixed_buf);
    if (file_size < 12 + footer_length) {
        return Status::Corruption("bad footer length");
    }
    std::string footer_buf;
    footer_buf.resize(footer_length);
    bytes_read = 0;
    RETURN_IF_ERROR(fr->read_at(file_size - 12 - footer_length,
                                Slice(footer_buf.data(), footer_buf.size()), &bytes_read));
    if (bytes_read != footer_length) {
        return Status::Corruption("short read footer");
    }
    if (!footer->ParseFromArray(footer_buf.data(), static_cast<int>(footer_buf.size()))) {
        return Status::Corruption("failed to parse footer");
    }
    *footer_start = file_size - 12 - footer_length;
    return Status::OK();
}

std::string md5_of(const std::string& data) {
    Md5Digest digest;
    digest.update(data.data(), data.size());
    digest.digest();
    return digest.hex();
}

} // namespace

class SegmentFormatGoldenTest : public ::testing::Test {
protected:
    void SetUp() override {
        _saved_vertical_writer = config::enable_vertical_segment_writer;

        char buffer[MAX_PATH_LEN];
        ASSERT_NE(getcwd(buffer, MAX_PATH_LEN), nullptr);
        _absolute_dir = std::string(buffer) + std::string(kTestDir);
        ASSERT_TRUE(io::global_local_filesystem()->delete_directory(_absolute_dir).ok());
        ASSERT_TRUE(io::global_local_filesystem()->create_directory(_absolute_dir).ok());

        EngineOptions options;
        auto engine = std::make_unique<StorageEngine>(options);
        s_engine = engine.get();
        // MoW rowset writers take a CalcDeleteBitmapExecutor token; the
        // minimal engine (no open()) does not create them
        s_engine->_calc_delete_bitmap_executor = std::make_unique<CalcDeleteBitmapExecutor>();
        s_engine->_calc_delete_bitmap_executor->init(2);
        s_engine->_calc_delete_bitmap_executor_for_load =
                std::make_unique<CalcDeleteBitmapExecutor>();
        s_engine->_calc_delete_bitmap_executor_for_load->init(2);
        ExecEnv::GetInstance()->set_storage_engine(std::move(engine));

        _data_dir = std::make_unique<DataDir>(*s_engine, _absolute_dir, 100000000);
        ASSERT_TRUE(_data_dir->init().ok());

        const char* root = getenv("ROOT");
        ASSERT_NE(root, nullptr) << "ROOT env is required to locate golden files";
        _expected_dir = std::string(root) + "/be/test/expected_result/storage/segment_format";
        if (FLAGS_gen_out) {
            std::filesystem::create_directories(_expected_dir);
        }
    }

    void TearDown() override {
        config::enable_vertical_segment_writer = _saved_vertical_writer;
        _data_dir.reset();
        s_engine = nullptr;
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_absolute_dir).ok());
    }

    // ---------------------------------------------------------------------
    // Schema construction
    // ---------------------------------------------------------------------

    struct ColSpec {
        std::string name;
        std::string type;
        bool is_key = false;
        bool is_nullable = false;
        std::string aggregation; // empty = NONE for key/dup, explicit for agg tables
        int32_t length = 0;      // 0 = derive from type
        int32_t precision = 0;
        int32_t frac = 0;
        std::vector<ColSpec> children;
        bool is_agg_state = false; // aggregation holds the function name
        std::string default_value;
    };

    static ColSpec key_col(const std::string& name, const std::string& type, int32_t length = 0,
                           int32_t precision = 0, int32_t frac = 0) {
        ColSpec c;
        c.name = name;
        c.type = type;
        c.is_key = true;
        c.length = length;
        c.precision = precision;
        c.frac = frac;
        return c;
    }

    static ColSpec val_col(const std::string& name, const std::string& type, bool nullable = false,
                           const std::string& agg = "", int32_t length = 0, int32_t precision = 0,
                           int32_t frac = 0) {
        ColSpec c;
        c.name = name;
        c.type = type;
        c.is_nullable = nullable;
        c.aggregation = agg;
        c.length = length;
        c.precision = precision;
        c.frac = frac;
        return c;
    }

    struct SchemaOptions {
        KeysType keys_type = DUP_KEYS;
        int num_short_key_columns = 0; // 0 = all key columns
        int sequence_col_idx = -1;
        bool store_row_column = false;
        std::vector<int32_t> row_store_column_unique_ids;
        std::vector<int32_t> cluster_key_uids;
        TabletStorageFormatPB storage_format = TABLET_STORAGE_FORMAT_DEFAULT;
        int32_t skip_bitmap_col_idx = -1;
    };

    static int32_t default_length(const std::string& type) {
        if (type == "BOOLEAN" || type == "TINYINT") {
            return 1;
        }
        if (type == "SMALLINT") {
            return 2;
        }
        if (type == "INT" || type == "FLOAT" || type == "DATEV2" || type == "IPV4" ||
            type == "DECIMAL32") {
            return 4;
        }
        if (type == "BIGINT" || type == "DOUBLE" || type == "DATETIME" || type == "DATETIMEV2" ||
            type == "TIMESTAMPTZ" || type == "DECIMAL64") {
            return 8;
        }
        if (type == "LARGEINT" || type == "IPV6" || type == "DECIMAL128I") {
            return 16;
        }
        if (type == "DECIMAL256") {
            return 32;
        }
        if (type == "DATE") {
            return 3;
        }
        if (type == "DECIMAL") {
            return 12;
        }
        if (type == "VARCHAR") {
            return 65533;
        }
        // STRING / JSONB / HLL / OBJECT / QUANTILE_STATE / ARRAY / MAP / STRUCT / VARIANT
        return INT32_MAX - 4;
    }

    static void fill_column_pb(ColumnPB* column_pb, const ColSpec& spec, int32_t* next_unique_id) {
        column_pb->set_unique_id((*next_unique_id)++);
        column_pb->set_name(spec.name);
        column_pb->set_type(spec.type);
        column_pb->set_is_key(spec.is_key);
        column_pb->set_is_nullable(spec.is_nullable);
        int32_t length = spec.length != 0 ? spec.length : default_length(spec.type);
        column_pb->set_length(length);
        column_pb->set_index_length(spec.type == "VARCHAR" || spec.type == "STRING"
                                            ? std::min<int32_t>(length, 10)
                                            : std::min<int32_t>(length, 36));
        if (!spec.aggregation.empty()) {
            column_pb->set_aggregation(spec.aggregation);
        }
        if (spec.precision > 0) {
            column_pb->set_precision(spec.precision);
        }
        if (spec.frac > 0 || spec.type == "DECIMAL32" || spec.type == "DECIMAL64" ||
            spec.type == "DECIMAL128I" || spec.type == "DECIMAL256" || spec.type == "DATETIMEV2") {
            column_pb->set_frac(spec.frac);
        }
        if (spec.type == "VARIANT") {
            column_pb->set_variant_max_subcolumns_count(10);
            column_pb->set_variant_max_sparse_column_statistics_size(10000);
        }
        if (spec.is_agg_state) {
            column_pb->set_result_is_nullable(false);
            column_pb->set_be_exec_version(BeExecVersionManager::get_newest_version());
        }
        if (!spec.default_value.empty()) {
            column_pb->set_default_value(spec.default_value);
        } else if (spec.name == DELETE_SIGN) {
            column_pb->set_default_value("0"); // as FE defines the hidden column
        }
        for (const auto& child : spec.children) {
            fill_column_pb(column_pb->add_children_columns(), child, next_unique_id);
        }
    }

    static TabletSchemaSPtr make_schema(const std::vector<ColSpec>& cols,
                                        const SchemaOptions& opts) {
        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(opts.keys_type);
        int num_keys = 0;
        for (const auto& c : cols) {
            num_keys += c.is_key ? 1 : 0;
        }
        schema_pb.set_num_short_key_columns(
                opts.num_short_key_columns > 0 ? opts.num_short_key_columns : num_keys);
        schema_pb.set_num_rows_per_row_block(1024);
        // special-column ordinals live in TabletSchemaPB fields, not in
        // column-name detection (that only happens in append_column)
        for (int i = 0; i < static_cast<int>(cols.size()); ++i) {
            if (cols[i].name == DELETE_SIGN) {
                schema_pb.set_delete_sign_idx(i);
            } else if (cols[i].name == SEQUENCE_COL) {
                schema_pb.set_sequence_col_idx(i);
            } else if (cols[i].name == SKIP_BITMAP_COL) {
                schema_pb.set_skip_bitmap_col_idx(i);
            } else if (cols[i].name == BINLOG_LSN_COL) {
                schema_pb.set_binlog_lsn_col_idx(i);
            } else if (cols[i].name == BINLOG_TIMESTAMP_COL) {
                schema_pb.set_binlog_timestamp_col_idx(i);
            }
        }
        if (opts.sequence_col_idx >= 0) {
            schema_pb.set_sequence_col_idx(opts.sequence_col_idx);
        }
        if (opts.skip_bitmap_col_idx >= 0) {
            schema_pb.set_skip_bitmap_col_idx(opts.skip_bitmap_col_idx);
        }
        if (opts.store_row_column) {
            schema_pb.set_store_row_column(true);
            for (int32_t uid : opts.row_store_column_unique_ids) {
                schema_pb.add_row_store_column_unique_ids(uid);
            }
        }
        for (int32_t uid : opts.cluster_key_uids) {
            schema_pb.add_cluster_key_uids(uid);
        }
        if (opts.storage_format != TABLET_STORAGE_FORMAT_DEFAULT) {
            schema_pb.set_storage_format(opts.storage_format);
        }
        int32_t next_unique_id = 1;
        for (const auto& c : cols) {
            fill_column_pb(schema_pb.add_column(), c, &next_unique_id);
        }
        schema_pb.set_next_column_unique_id(next_unique_id);
        auto schema = std::make_shared<TabletSchema>();
        schema->init_from_pb(schema_pb);
        return schema;
    }

    // ---------------------------------------------------------------------
    // Deterministic data generation
    // ---------------------------------------------------------------------

    // Text for one cell, deserialized through the column's DataTypeSerDe. Key
    // columns are monotonic in `r` (the leading key column of every schema in
    // this suite has a wide domain), so hand-built blocks respect the
    // memtable output invariant (sorted, unique keys).
    static std::string cell_text(const TabletColumn& col, int64_t r) {
        const int64_t salt = col.unique_id();
        const int64_t v = col.is_key() ? r : (r * 131 + salt * 31) % 10000;
        switch (col.type()) {
        case FieldType::OLAP_FIELD_TYPE_BOOL:
            return (v % 2 == 0) ? "1" : "0";
        case FieldType::OLAP_FIELD_TYPE_TINYINT:
            return std::to_string(v % 128 - 64);
        case FieldType::OLAP_FIELD_TYPE_SMALLINT:
            return std::to_string(v % 30000 - 15000);
        case FieldType::OLAP_FIELD_TYPE_INT:
            return std::to_string(col.is_key() ? 1000000 + v : v * 977 - 4444444);
        case FieldType::OLAP_FIELD_TYPE_BIGINT:
            return std::to_string(col.is_key() ? 3000000000LL + v : v * 100000007LL - 999999999LL);
        case FieldType::OLAP_FIELD_TYPE_LARGEINT:
            return fmt::format("{}{:018d}", v % 2 == 0 ? "" : "-", 170141183460469LL + v * 7);
        case FieldType::OLAP_FIELD_TYPE_FLOAT:
            return fmt::format("{}.{}", v % 1000 - 500, v % 100);
        case FieldType::OLAP_FIELD_TYPE_DOUBLE:
            return fmt::format("{}.{:06d}", v % 100000 - 50000, v % 1000000);
        case FieldType::OLAP_FIELD_TYPE_DECIMAL: // DECIMALV2(27, 9)
            return fmt::format("{}.{:09d}", v % 100000 - 50000, v % 1000000000);
        case FieldType::OLAP_FIELD_TYPE_DECIMAL32: // (9, 2)
            return fmt::format("{}.{:02d}", v % 1000000 - 500000, v % 100);
        case FieldType::OLAP_FIELD_TYPE_DECIMAL64: // (18, 6)
            return fmt::format("{}.{:06d}", col.is_key() ? 100000 + v : v % 1000000 - 500000,
                               v % 1000000);
        case FieldType::OLAP_FIELD_TYPE_DECIMAL128I: // (38, 10)
            return fmt::format("{}.{:010d}", v % 10000000 - 5000000, v % 10000000000LL);
        case FieldType::OLAP_FIELD_TYPE_DECIMAL256: // (76, 20)
            return fmt::format("{}12345678901234567890.{:020d}", v % 2 == 0 ? "" : "-",
                               v % 100000000000LL);
        case FieldType::OLAP_FIELD_TYPE_DATE:
        case FieldType::OLAP_FIELD_TYPE_DATEV2:
            return fmt::format("{:04d}-{:02d}-{:02d}", 2020 + v % 6, 1 + (v / 28) % 12, 1 + v % 28);
        case FieldType::OLAP_FIELD_TYPE_DATETIME:
            return fmt::format("{:04d}-{:02d}-{:02d} {:02d}:{:02d}:{:02d}", 2020 + v % 6,
                               1 + (v / 28) % 12, 1 + v % 28, v % 24, v % 60, (v * 7) % 60);
        case FieldType::OLAP_FIELD_TYPE_DATETIMEV2: // frac=6
            return fmt::format("{:04d}-{:02d}-{:02d} {:02d}:{:02d}:{:02d}.{:06d}", 2020 + v % 6,
                               1 + (v / 28) % 12, 1 + v % 28, v % 24, v % 60, (v * 7) % 60,
                               v % 1000000);
        case FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ:
            return fmt::format("{:04d}-{:02d}-{:02d} {:02d}:{:02d}:{:02d}+00:00", 2020 + v % 6,
                               1 + (v / 28) % 12, 1 + v % 28, v % 24, v % 60, (v * 7) % 60);
        case FieldType::OLAP_FIELD_TYPE_IPV4:
            return fmt::format("10.{}.{}.{}", (v / 65536) % 256, (v / 256) % 256, v % 256);
        case FieldType::OLAP_FIELD_TYPE_IPV6:
            return fmt::format("2001:db8::{:x}:{:x}", (v / 65536) % 65536, v % 65536);
        case FieldType::OLAP_FIELD_TYPE_CHAR:
            return fmt::format("c{:06d}", v % 1000000);
        case FieldType::OLAP_FIELD_TYPE_VARCHAR:
            // mix repeated values (dict friendly) with unique ones
            return col.is_key() ? fmt::format("key_{:08d}", v)
                                : (v % 3 == 0 ? fmt::format("repeat_{}", v % 5)
                                              : fmt::format("val_{}_{}", salt, v));
        case FieldType::OLAP_FIELD_TYPE_STRING:
            return v % 4 == 0 ? fmt::format("common_{}", v % 7)
                              : fmt::format("string_value_{}_{}_{}", salt, v,
                                            std::string(v % 30, 'x'));
        case FieldType::OLAP_FIELD_TYPE_JSONB:
            return fmt::format(R"({{"id":{},"name":"n{}","tags":[{},{}],"nested":{{"x":{}}}}})", v,
                               v % 10, v % 5, v % 3, v * 2);
        case FieldType::OLAP_FIELD_TYPE_ARRAY: {
            const auto& item = col.get_sub_column(0);
            if (item.type() == FieldType::OLAP_FIELD_TYPE_ARRAY) {
                return fmt::format("[[{}, {}], [{}]]", v, v + 1, v * 2);
            }
            if (item.type() == FieldType::OLAP_FIELD_TYPE_VARCHAR ||
                item.type() == FieldType::OLAP_FIELD_TYPE_STRING ||
                item.type() == FieldType::OLAP_FIELD_TYPE_CHAR) {
                return v % 5 == 0 ? fmt::format(R"(["a{}", null, "b{}"])", v, v * 2)
                                  : fmt::format(R"(["a{}", "b{}"])", v, v * 2);
            }
            return v % 5 == 0 ? fmt::format("[{}, null, {}]", v, v * 2)
                              : fmt::format("[{}, {}, {}]", v, v + 1, v * 2);
        }
        case FieldType::OLAP_FIELD_TYPE_MAP: // built as MAP<VARCHAR, INT> here
            return fmt::format(R"({{"k{}":{},"k{}":{}}})", v % 10, v, 10 + (v + 1) % 10, v * 3);
        case FieldType::OLAP_FIELD_TYPE_STRUCT: // built as <f_int INT, f_str VARCHAR>
            return fmt::format(R"({{"f_int":{},"f_str":"s{}"}})", v, v % 100);
        default:
            ADD_FAILURE() << "no text generator for type " << int(col.type());
            return "0";
        }
    }

    // Build a block for [start_row, start_row + num_rows) of the schema.
    // Nullable value columns get a null every 7 rows. The special hidden
    // columns are filled as their writers expect: rows listed in delete_rows
    // get delete sign 1, sequence gets 100 + row, row-store column is
    // defaulted (rewritten by the write path itself).
    static Block build_block(const TabletSchemaSPtr& schema, int64_t start_row, int64_t num_rows,
                             const std::vector<int64_t>& delete_rows = {}) {
        Block block = schema->create_block();
        auto columns = std::move(block).mutate_columns();
        DataTypeSerDe::FormatOptions format_options;
        for (size_t cid = 0; cid < schema->num_columns(); ++cid) {
            const auto& col = *schema->columns()[cid];
            const auto& data_type = block.get_by_position(cid).type;
            auto serde = data_type->get_serde();
            // agg_state columns hold opaque serialized states; fill raw bytes
            // through the physical column (string- or fixed-length-serialized)
            if (col.type() == FieldType::OLAP_FIELD_TYPE_AGG_STATE) {
                if (columns[cid]->is_column_string()) {
                    auto& str_col = assert_cast<ColumnString&>(*columns[cid]);
                    for (int64_t i = 0; i < num_rows; ++i) {
                        auto state = fmt::format("state_{}_{}", col.unique_id(), start_row + i);
                        str_col.insert_data(state.data(), state.size());
                    }
                } else {
                    auto& fixed_col = assert_cast<ColumnFixedLengthObject&>(*columns[cid]);
                    fixed_col.set_item_size(sizeof(int64_t));
                    for (int64_t i = 0; i < num_rows; ++i) {
                        int64_t state = start_row + i;
                        fixed_col.insert_data(reinterpret_cast<const char*>(&state), sizeof(state));
                    }
                }
                continue;
            }
            for (int64_t i = 0; i < num_rows; ++i) {
                const int64_t r = start_row + i;
                if (col.name() == DELETE_SIGN) {
                    int8_t sign = std::find(delete_rows.begin(), delete_rows.end(), i) !=
                                  delete_rows.end();
                    columns[cid]->insert_data(reinterpret_cast<const char*>(&sign), sizeof(sign));
                    continue;
                }
                if (col.name() == SEQUENCE_COL) {
                    int64_t seq = 100 + r;
                    columns[cid]->insert_data(reinterpret_cast<const char*>(&seq), sizeof(seq));
                    continue;
                }
                if (col.name() == BeConsts::ROW_STORE_COL) {
                    columns[cid]->insert_default();
                    continue;
                }
                // monotonic sort column for the MoW cluster-key case
                if (col.name() == "v_ck_sort") {
                    int64_t sort_val = 70000 + r;
                    columns[cid]->insert_data(reinterpret_cast<const char*>(&sort_val),
                                              sizeof(sort_val));
                    continue;
                }
                if (col.type() == FieldType::OLAP_FIELD_TYPE_VARIANT) {
                    // root-scalar JSON string: the not-yet-parsed shape a load
                    // produces, so the write path's variant parse is exercised
                    VariantUtil::insert_root_scalar_field(
                            assert_cast<ColumnVariant&>(*columns[cid]),
                            Field::create_field<TYPE_STRING>(
                                    String(fmt::format(R"({{"a":{},"b":"str{}","c":{{"d":{}}}}})",
                                                       r, r % 10, r * 5))));
                    continue;
                }
                if (col.is_nullable() && !col.is_key() && (r + col.unique_id()) % 7 == 0) {
                    columns[cid]->insert_default(); // null
                    continue;
                }
                if (col.type() == FieldType::OLAP_FIELD_TYPE_HLL) {
                    HyperLogLog hll;
                    if (r % 5 != 0) {
                        hll = HyperLogLog(static_cast<uint64_t>(r) * 2654435761ULL);
                    }
                    assert_cast<ColumnHLL&>(*columns[cid]).insert_value(std::move(hll));
                    continue;
                }
                if (col.type() == FieldType::OLAP_FIELD_TYPE_BITMAP) {
                    // avoid 2..32 elements: that range serializes a hash-set
                    // iteration order (config::enable_set_in_bitmap_value)
                    BitmapValue bitmap;
                    if (r % 3 == 1) {
                        bitmap.add(static_cast<uint64_t>(r));
                    } else if (r % 3 == 2) {
                        for (int j = 0; j < 40; ++j) {
                            bitmap.add(static_cast<uint64_t>(r) * 100 + j);
                        }
                    }
                    assert_cast<ColumnBitmap&>(*columns[cid]).insert_value(std::move(bitmap));
                    continue;
                }
                if (col.type() == FieldType::OLAP_FIELD_TYPE_QUANTILE_STATE) {
                    QuantileState quantile;
                    quantile.add_value(static_cast<double>(r) * 0.5);
                    if (r % 2 == 1) {
                        quantile.add_value(static_cast<double>(r) * 1.5);
                    }
                    assert_cast<ColumnQuantileState&>(*columns[cid])
                            .insert_value(std::move(quantile));
                    continue;
                }
                std::string text = cell_text(col, r);
                Slice slice(text.data(), text.size());
                auto st =
                        serde->deserialize_one_cell_from_json(*columns[cid], slice, format_options);
                EXPECT_TRUE(st.ok()) << "col=" << col.name() << " text=" << text << " " << st;
            }
        }
        block.set_columns(std::move(columns));
        return block;
    }

    // ---------------------------------------------------------------------
    // Write drivers
    // ---------------------------------------------------------------------

    // bare tablet, enough for the MoW flush path (meta flags + rowset lookup
    // on an empty version map); not registered in any tablet manager
    TabletSharedPtr make_fake_tablet(const TabletSchemaSPtr& schema, int64_t tablet_id,
                                     bool enable_mow) {
        auto tablet_meta = std::make_shared<TabletMeta>();
        tablet_meta->_tablet_id = tablet_id;
        static_cast<void>(tablet_meta->set_partition_id(30));
        tablet_meta->_schema = schema;
        tablet_meta->_enable_unique_key_merge_on_write = enable_mow;
        return std::make_shared<Tablet>(*s_engine, tablet_meta, _data_dir.get(), "test_str");
    }

    // normalized dump: rowset ids replaced by stable aliases in map order
    static std::string dump_delete_bitmap(const DeleteBitmap& db) {
        std::string out = "delete_bitmap:\n";
        std::map<RowsetId, int> alias;
        for (const auto& [key, bitmap] : db.delete_bitmap) {
            const auto& [rowset_id, seg_id, version] = key;
            auto [it, _] = alias.try_emplace(rowset_id, alias.size());
            out += fmt::format("rs{}|seg{}|v{}|rows=", it->second, seg_id, version);
            for (auto row : bitmap) {
                out += fmt::format("{},", row);
            }
            out += "\n";
        }
        return out;
    }

    RowsetWriterContext make_context(const TabletSchemaSPtr& schema, int64_t case_id,
                                     DataWriteType write_type) {
        RowsetWriterContext ctx;
        RowsetId rowset_id;
        rowset_id.init(10000 + case_id);
        ctx.rowset_id = rowset_id;
        ctx.tablet_id = 20000 + case_id;
        ctx.tablet_schema_hash = 1111;
        ctx.partition_id = 30;
        ctx.rowset_type = BETA_ROWSET;
        ctx.rowset_state = VISIBLE;
        ctx.tablet_schema = schema;
        ctx.tablet_path = _absolute_dir + fmt::format("/tablet_{}", case_id);
        EXPECT_TRUE(io::global_local_filesystem()->create_directory(ctx.tablet_path).ok());
        ctx.version = Version(2, 2);
        ctx.segments_overlap = OVERLAPPING;
        ctx.max_rows_per_segment = UINT32_MAX;
        ctx.write_type = write_type;
        ctx.data_dir = _data_dir.get();
        return ctx;
    }

    // one segment per block, through SegmentFlusher::flush_single_block
    RowsetSharedPtr write_by_flush_single_block(
            const TabletSchemaSPtr& schema, int64_t case_id, bool vertical_writer,
            const std::vector<Block>& blocks,
            const std::function<void(RowsetWriterContext&)>& tweak = nullptr) {
        config::enable_vertical_segment_writer = vertical_writer;
        auto ctx = make_context(schema, case_id, DataWriteType::TYPE_DIRECT);
        if (tweak) {
            tweak(ctx);
        }
        auto res = RowsetFactory::create_rowset_writer(*s_engine, ctx, false);
        EXPECT_TRUE(res.has_value()) << res.error();
        auto writer = std::move(res).value();
        for (const auto& block : blocks) {
            auto st = writer->flush_single_block(&block);
            EXPECT_TRUE(st.ok()) << st;
        }
        RowsetSharedPtr rowset;
        EXPECT_EQ(Status::OK(), writer->build(rowset));
        config::enable_vertical_segment_writer = _saved_vertical_writer;
        return rowset;
    }

    // add_block (+ flush per segment), the buffered SegmentCreator path used
    // by compaction and schema change
    RowsetSharedPtr write_by_add_block(const TabletSchemaSPtr& schema, int64_t case_id,
                                       DataWriteType write_type, const std::vector<Block>& blocks) {
        auto ctx = make_context(schema, case_id, write_type);
        auto res = RowsetFactory::create_rowset_writer(*s_engine, ctx, false);
        EXPECT_TRUE(res.has_value()) << res.error();
        auto writer = std::move(res).value();
        for (const auto& block : blocks) {
            auto st = writer->add_block(&block);
            EXPECT_TRUE(st.ok()) << st;
            st = writer->flush();
            EXPECT_TRUE(st.ok()) << st;
        }
        RowsetSharedPtr rowset;
        EXPECT_EQ(Status::OK(), writer->build(rowset));
        return rowset;
    }

    // ---------------------------------------------------------------------
    // Fingerprinting
    // ---------------------------------------------------------------------

    static void normalize_column_meta(const segment_v2::ColumnMetaPB& meta, int depth,
                                      std::string* out) {
        std::string indexes;
        for (const auto& idx : meta.indexes()) {
            indexes += fmt::format("{},", int(idx.type()));
        }
        std::string variant_stats;
        if (meta.has_variant_statistics()) {
            const auto& stats = meta.variant_statistics();
            std::map<std::string, uint32_t> sparse(stats.sparse_column_non_null_size().begin(),
                                                   stats.sparse_column_non_null_size().end());
            std::map<std::string, uint32_t> doc(stats.doc_value_column_non_null_size().begin(),
                                                stats.doc_value_column_non_null_size().end());
            variant_stats =
                    fmt::format(" vstat_nested={} vstat_sparse=[", stats.has_nested_group());
            for (const auto& [k, v] : sparse) {
                variant_stats += fmt::format("{}:{},", k, v);
            }
            variant_stats += "] vstat_doc=[";
            for (const auto& [k, v] : doc) {
                variant_stats += fmt::format("{}:{},", k, v);
            }
            variant_stats += "]";
        }
        out->append(fmt::format(
                "{}col uid={} type={} len={} enc={} cmp={} null={} prec={} frac={} nrows={} "
                "path={} fn={} idx=[{}] dict={} children={}{}\n",
                std::string(depth * 2, ' '), meta.unique_id(), meta.type(), meta.length(),
                int(meta.encoding()), int(meta.compression()), meta.is_nullable(), meta.precision(),
                meta.frac(), meta.num_rows(),
                meta.has_column_path_info() ? meta.column_path_info().path() : "",
                meta.function_name(), indexes, meta.has_dict_page(), meta.children_columns_size(),
                variant_stats));
        for (const auto& child : meta.children_columns()) {
            normalize_column_meta(child, depth + 1, out);
        }
    }

    // all physical column metas, from the footer (V2) or the external column
    // meta region (V3), in physical order
    static std::vector<segment_v2::ColumnMetaPB> load_column_metas(
            const segment_v2::SegmentFooterPB& footer, const io::FileReaderSPtr& file_reader) {
        std::vector<segment_v2::ColumnMetaPB> metas;
        for (const auto& c : footer.columns()) {
            metas.push_back(c);
        }
        if (!metas.empty()) {
            return metas;
        }
        segment_v2::ColumnMetaAccessor accessor;
        if (!accessor.init(footer, file_reader).ok()) {
            return metas;
        }
        for (uint32_t ordinal = 0;; ++ordinal) {
            segment_v2::ColumnMetaPB meta;
            if (!accessor.get_column_meta_by_column_ordinal_id(footer, ordinal, &meta).ok()) {
                break;
            }
            metas.push_back(std::move(meta));
        }
        return metas;
    }

    // layout-insensitive footer summary: columns sorted by (unique_id, path),
    // page pointers excluded
    static std::string normalize_footer(const segment_v2::SegmentFooterPB& footer,
                                        const std::vector<segment_v2::ColumnMetaPB>& metas) {
        std::string out = fmt::format(
                "footer version={} num_rows={} compress={} short_key_index={} pk_index={} "
                "columns={}\n",
                footer.version(), footer.num_rows(), int(footer.compress_type()),
                footer.has_short_key_index_page(), footer.has_primary_key_index_meta(),
                metas.size());
        std::vector<const segment_v2::ColumnMetaPB*> sorted;
        sorted.reserve(metas.size());
        for (const auto& m : metas) {
            sorted.push_back(&m);
        }
        auto sort_key = [](const segment_v2::ColumnMetaPB* m) {
            return std::make_tuple(m->unique_id(), m->has_column_path_info()
                                                           ? m->column_path_info().path()
                                                           : std::string());
        };
        std::sort(sorted.begin(), sorted.end(),
                  [&](const auto* a, const auto* b) { return sort_key(a) < sort_key(b); });
        for (const auto* meta : sorted) {
            normalize_column_meta(*meta, 1, &out);
        }
        return out;
    }

    // column unique ids in physical order (diagnostic for layout changes)
    static std::string footer_column_order(const std::vector<segment_v2::ColumnMetaPB>& metas) {
        std::string out;
        for (const auto& c : metas) {
            out += fmt::format("{},", c.unique_id());
        }
        if (!out.empty()) {
            out.pop_back();
        }
        return out;
    }

    // decoded content of the whole rowset via the standard rowset reader
    std::string dump_rowset_content(const TabletSchemaSPtr& schema, const RowsetSharedPtr& rowset) {
        RowsetReaderContext reader_context;
        reader_context.tablet_schema = schema;
        reader_context.need_ordered_result = false;
        std::vector<uint32_t> return_columns;
        for (uint32_t cid = 0; cid < schema->num_columns(); ++cid) {
            return_columns.push_back(cid);
        }
        reader_context.return_columns = &return_columns;
        RowsetReaderSharedPtr reader;
        EXPECT_TRUE(rowset->create_reader(&reader).ok());
        EXPECT_TRUE(reader->init(&reader_context).ok());

        Schema read_schema(schema->columns(), return_columns);
        std::string out;
        Status st;
        do {
            Block block;
            for (uint32_t cid : return_columns) {
                const auto* column_desc = read_schema.column(cid);
                auto data_type = Schema::get_data_type_ptr(*column_desc);
                block.insert(ColumnWithTypeAndName(data_type->create_column(), data_type,
                                                   column_desc->name()));
            }
            st = reader->next_batch(&block);
            if (block.rows() > 0) {
                out += block.dump_data(0, block.rows());
                out += "\n";
            }
        } while (st.ok());
        EXPECT_TRUE(st.is<ErrorCode::END_OF_FILE>()) << st;
        return out;
    }

    // One golden line per segment of the rowset (-1 = take the actual count,
    // pinned by the golden line set instead).
    // extra_content is folded into the content fingerprint (delete bitmaps...).
    // layout_sensitive: the physical column order of this case legitimately
    // depends on the write-path implementation (readers resolve columns
    // through the footer metas, not physical order), so byte md5 and column
    // order are excluded from the golden and only size + decoded content +
    // normalized metas are asserted. Used for the vertical-writer fixed
    // partial update with a non-prefix update column set: the pre-refactor
    // writer lays columns out update-cids-then-missing-cids, the
    // transform-chain implementation writes them in ascending cid order.
    void fingerprint_rowset(const std::string& case_name, const TabletSchemaSPtr& schema,
                            const RowsetSharedPtr& rowset, int expected_segments,
                            std::vector<std::string>* lines, const std::string& extra_content = "",
                            bool layout_sensitive = false) {
        ASSERT_NE(rowset, nullptr) << case_name;
        if (expected_segments < 0) {
            expected_segments = static_cast<int>(rowset->rowset_meta()->num_segments());
        }
        ASSERT_EQ(rowset->rowset_meta()->num_segments(), expected_segments) << case_name;
        bool has_variant = false;
        for (const auto& col : schema->columns()) {
            has_variant |= col->type() == FieldType::OLAP_FIELD_TYPE_VARIANT;
        }
        std::string content = dump_rowset_content(schema, rowset);
        content += extra_content;
        for (int seg = 0; seg < expected_segments; ++seg) {
            auto path =
                    local_segment_path(rowset->tablet_path(), rowset->rowset_id().to_string(), seg);
            io::FileReaderSPtr file_reader;
            ASSERT_TRUE(io::global_local_filesystem()->open_file(path, &file_reader).ok()) << path;
            segment_v2::SegmentFooterPB footer;
            uint64_t footer_start = 0;
            ASSERT_TRUE(read_segment_footer(file_reader, &footer, &footer_start).ok()) << path;

            const size_t file_size = file_reader->size();
            std::string file_bytes;
            file_bytes.resize(file_size);
            size_t bytes_read = 0;
            ASSERT_TRUE(
                    file_reader->read_at(0, Slice(file_bytes.data(), file_size), &bytes_read).ok());
            ASSERT_EQ(bytes_read, file_size);
            // variant column metas contain protobuf map fields
            // (VariantStatisticsPB) whose byte order is process-dependent;
            // those serialize into the footer (V2) or the external column
            // meta region (V3) - hash only the region before both, and cover
            // the metas via the normalized dump instead
            uint64_t byte_region_end = file_size;
            if (has_variant) {
                byte_region_end = footer_start;
                if (footer.has_col_meta_region_start()) {
                    byte_region_end =
                            std::min<uint64_t>(byte_region_end, footer.col_meta_region_start());
                }
            }
            std::string byte_md5 = has_variant
                                           ? "data:" + md5_of(file_bytes.substr(0, byte_region_end))
                                           : md5_of(file_bytes);

            auto metas = load_column_metas(footer, file_reader);
            std::string seg_content = content + normalize_footer(footer, metas);
            std::string order = footer_column_order(metas);
            if (layout_sensitive) {
                LOG(INFO) << "[golden-layout] " << case_name << " seg" << seg
                          << " byte=" << byte_md5 << " order=" << order;
                byte_md5 = "layout-dependent";
                order = "layout-dependent";
            }
            lines->push_back(fmt::format("{}|seg{}|rows={}|size={}|byte={}|content={}|order={}",
                                         case_name, seg, footer.num_rows(), byte_region_end,
                                         byte_md5, md5_of(seg_content), order));
        }
    }

    void check_golden(const std::string& name, const std::vector<std::string>& lines) {
        for (const auto& line : lines) {
            LOG(INFO) << "[golden] " << line;
        }
        check_or_generate_res_file(_expected_dir + "/" + name + ".out", {lines});
    }

    // ---------------------------------------------------------------------
    // Schemas of the case matrix
    // ---------------------------------------------------------------------

    // all FE-allowed key types, grouped so every group leads with a strictly
    // increasing wide-domain column
    std::vector<ColSpec> numeric_key_cols() {
        return {
                key_col("k_int", "INT"),
                key_col("k_bigint", "BIGINT"),
                key_col("k_tinyint", "TINYINT"),
                key_col("k_smallint", "SMALLINT"),
                key_col("k_largeint", "LARGEINT"),
                key_col("k_bool", "BOOLEAN"),
                val_col("v_int", "INT", /*nullable=*/true),
        };
    }

    std::vector<ColSpec> date_key_cols() {
        return {
                key_col("k_datev2", "DATEV2"),
                key_col("k_datetimev2", "DATETIMEV2", 0, 0, 6),
                key_col("k_date", "DATE"),
                key_col("k_datetime", "DATETIME"),
                val_col("v_int", "INT", /*nullable=*/true),
        };
    }

    std::vector<ColSpec> decimal_key_cols() {
        return {
                key_col("k_dec64", "DECIMAL64", 0, 18, 6),
                key_col("k_dec32", "DECIMAL32", 0, 9, 2),
                key_col("k_dec128", "DECIMAL128I", 0, 38, 10),
                key_col("k_dec256", "DECIMAL256", 0, 76, 20),
                key_col("k_decv2", "DECIMAL", 0, 27, 9),
                val_col("v_int", "INT", /*nullable=*/true),
        };
    }

    std::vector<ColSpec> string_ip_key_cols() {
        return {
                key_col("k_char", "CHAR", 16),
                key_col("k_ipv4", "IPV4"),
                key_col("k_ipv6", "IPV6"),
                key_col("k_varchar", "VARCHAR", 64),
                val_col("v_int", "INT", /*nullable=*/true),
        };
    }

    // every supported scalar value type, non-nullable and nullable twin
    std::vector<ColSpec> wide_scalar_value_cols() {
        std::vector<ColSpec> cols = {key_col("k_int", "INT")};
        struct T {
            const char* type;
            int precision;
            int frac;
            int length;
        };
        std::vector<T> types = {
                {"BOOLEAN", 0, 0, 0},      {"TINYINT", 0, 0, 0},    {"SMALLINT", 0, 0, 0},
                {"INT", 0, 0, 0},          {"BIGINT", 0, 0, 0},     {"LARGEINT", 0, 0, 0},
                {"FLOAT", 0, 0, 0},        {"DOUBLE", 0, 0, 0},     {"DECIMAL", 27, 9, 0},
                {"DECIMAL32", 9, 2, 0},    {"DECIMAL64", 18, 6, 0}, {"DECIMAL128I", 38, 10, 0},
                {"DECIMAL256", 76, 20, 0}, {"DATE", 0, 0, 0},       {"DATETIME", 0, 0, 0},
                {"DATEV2", 0, 0, 0},       {"DATETIMEV2", 0, 6, 0}, {"TIMESTAMPTZ", 0, 0, 0},
                {"IPV4", 0, 0, 0},         {"IPV6", 0, 0, 0},       {"CHAR", 0, 0, 20},
                {"VARCHAR", 0, 0, 64},     {"STRING", 0, 0, 0},     {"JSONB", 0, 0, 0},
        };
        for (const auto& t : types) {
            std::string lower = t.type;
            std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);
            cols.push_back(val_col("v_" + lower, t.type, false, "", t.length, t.precision, t.frac));
            cols.push_back(val_col("vn_" + lower, t.type, true, "", t.length, t.precision, t.frac));
        }
        return cols;
    }

    // ---------------------------------------------------------------------

    std::string _absolute_dir;
    std::string _expected_dir;
    std::unique_ptr<DataDir> _data_dir;
    bool _saved_vertical_writer = true;
};

// flush_single_block over all key-type groups x DUP keys x both segment
// writer classes, two segments each
TEST_F(SegmentFormatGoldenTest, DupKeyTypesFlush) {
    std::vector<std::string> lines;
    int64_t case_id = 100;
    struct Group {
        std::string name;
        std::vector<ColSpec> cols;
    };
    std::vector<Group> groups = {
            {"numeric", numeric_key_cols()},
            {"date", date_key_cols()},
            {"decimal", decimal_key_cols()},
            {"string_ip", string_ip_key_cols()},
    };
    for (const auto& group : groups) {
        SchemaOptions opts;
        opts.keys_type = DUP_KEYS;
        auto schema = make_schema(group.cols, opts);
        std::vector<Block> blocks;
        blocks.push_back(build_block(schema, 0, 100));
        blocks.push_back(build_block(schema, 100, 50));
        for (bool vertical : {true, false}) {
            auto rowset = write_by_flush_single_block(schema, case_id++, vertical, blocks);
            fingerprint_rowset(fmt::format("dup_{}_flush_{}", group.name,
                                           vertical ? "vertical" : "horizontal"),
                               schema, rowset, 2, &lines);
        }
    }
    check_golden("dup_key_types_flush", lines);
}

// wide scalar value coverage under both storage formats (V2/V3 default
// encodings) and both segment writer classes; one bigger multi-page segment
TEST_F(SegmentFormatGoldenTest, WideScalarValuesFlush) {
    std::vector<std::string> lines;
    int64_t case_id = 200;
    for (auto format : {TABLET_STORAGE_FORMAT_V2, TABLET_STORAGE_FORMAT_V3}) {
        SchemaOptions opts;
        opts.keys_type = DUP_KEYS;
        opts.storage_format = format;
        auto schema = make_schema(wide_scalar_value_cols(), opts);
        std::vector<Block> blocks;
        blocks.push_back(build_block(schema, 0, 100));
        blocks.push_back(build_block(schema, 100, 3000)); // multi-page, dict growth
        for (bool vertical : {true, false}) {
            auto rowset = write_by_flush_single_block(schema, case_id++, vertical, blocks);
            fingerprint_rowset(fmt::format("wide_scalar_v{}_flush_{}",
                                           format == TABLET_STORAGE_FORMAT_V2 ? 2 : 3,
                                           vertical ? "vertical" : "horizontal"),
                               schema, rowset, 2, &lines);
        }
    }
    check_golden("wide_scalar_values_flush", lines);
}

// aggregate-keys table: SUM/MIN/MAX/REPLACE/REPLACE_IF_NOT_NULL over scalars
TEST_F(SegmentFormatGoldenTest, AggScalarFlush) {
    std::vector<std::string> lines;
    std::vector<ColSpec> cols = {
            key_col("k_int", "INT"),
            key_col("k_varchar", "VARCHAR", 32),
            val_col("v_sum_bigint", "BIGINT", false, "SUM"),
            val_col("v_sum_double", "DOUBLE", false, "SUM"),
            val_col("v_sum_dec", "DECIMAL64", false, "SUM", 0, 18, 6),
            val_col("v_min_int", "INT", false, "MIN"),
            val_col("v_max_datev2", "DATEV2", false, "MAX"),
            val_col("v_max_varchar", "VARCHAR", false, "MAX", 64),
            val_col("v_replace_str", "STRING", false, "REPLACE"),
            val_col("v_rinn_largeint", "LARGEINT", true, "REPLACE_IF_NOT_NULL"),
    };
    SchemaOptions opts;
    opts.keys_type = AGG_KEYS;
    auto schema = make_schema(cols, opts);
    std::vector<Block> blocks;
    blocks.push_back(build_block(schema, 0, 100));
    blocks.push_back(build_block(schema, 100, 50));
    int64_t case_id = 300;
    for (bool vertical : {true, false}) {
        auto rowset = write_by_flush_single_block(schema, case_id++, vertical, blocks);
        fingerprint_rowset(fmt::format("agg_scalar_flush_{}", vertical ? "vertical" : "horizontal"),
                           schema, rowset, 2, &lines);
    }
    check_golden("agg_scalar_flush", lines);
}

// unique keys, merge-on-read: REPLACE values + hidden delete sign
TEST_F(SegmentFormatGoldenTest, UniqueMorFlush) {
    std::vector<std::string> lines;
    std::vector<ColSpec> cols = {
            key_col("k_bigint", "BIGINT"),
            key_col("k_char", "CHAR", 12),
            val_col("v_str", "STRING", false, "REPLACE"),
            val_col("v_dec", "DECIMAL128I", true, "REPLACE", 0, 38, 10),
            val_col(std::string(DELETE_SIGN), "TINYINT", false, "REPLACE"),
    };
    SchemaOptions opts;
    opts.keys_type = UNIQUE_KEYS;
    auto schema = make_schema(cols, opts);
    std::vector<Block> blocks;
    blocks.push_back(build_block(schema, 0, 100, {5, 17}));
    blocks.push_back(build_block(schema, 100, 50));
    int64_t case_id = 400;
    for (bool vertical : {true, false}) {
        auto rowset = write_by_flush_single_block(schema, case_id++, vertical, blocks);
        fingerprint_rowset(fmt::format("unique_mor_flush_{}", vertical ? "vertical" : "horizontal"),
                           schema, rowset, 2, &lines);
    }
    check_golden("unique_mor_flush", lines);
}

// unique keys merge-on-write: primary key index (+ sequence column suffix)
TEST_F(SegmentFormatGoldenTest, MowFlush) {
    std::vector<std::string> lines;
    int64_t case_id = 500;
    for (bool with_seq : {false, true}) {
        std::vector<ColSpec> cols = {
                key_col("k_int", "INT"),
                key_col("k_varchar", "VARCHAR", 32),
                val_col("v_str", "STRING", false, "REPLACE"),
                val_col("v_int", "INT", true, "REPLACE"),
        };
        SchemaOptions opts;
        opts.keys_type = UNIQUE_KEYS;
        if (with_seq) {
            cols.push_back(val_col(std::string(SEQUENCE_COL), "BIGINT", false, "REPLACE"));
            opts.sequence_col_idx = static_cast<int>(cols.size()) - 1;
        }
        cols.push_back(val_col(std::string(DELETE_SIGN), "TINYINT", false, "REPLACE"));
        auto schema = make_schema(cols, opts);
        std::vector<Block> blocks;
        blocks.push_back(build_block(schema, 0, 100, {9}));
        // overlaps rows 90..99 of segment 0: flush-time delete bitmap between
        // segments is non-trivial
        blocks.push_back(build_block(schema, 90, 50));
        for (bool vertical : {true, false}) {
            auto mow_delete_bitmap = std::make_shared<DeleteBitmap>(1);
            auto tablet = make_fake_tablet(schema, 20000 + case_id, /*enable_mow=*/true);
            auto rowset = write_by_flush_single_block(
                    schema, case_id++, vertical, blocks, [&](RowsetWriterContext& ctx) {
                        ctx.tablet = tablet;
                        ctx.enable_unique_key_merge_on_write = true;
                        ctx.mow_context = std::make_shared<MowContext>(
                                1, 1000, std::make_shared<RowsetIdUnorderedSet>(),
                                std::vector<RowsetSharedPtr> {}, mow_delete_bitmap);
                    });
            fingerprint_rowset(fmt::format("mow{}_flush_{}", with_seq ? "_seq" : "",
                                           vertical ? "vertical" : "horizontal"),
                               schema, rowset, 2, &lines, dump_delete_bitmap(*mow_delete_bitmap));
        }
    }
    check_golden("mow_flush", lines);
}

// buffered SegmentCreator::add_block path as used by horizontal compaction
// and schema change
TEST_F(SegmentFormatGoldenTest, AddBlockPaths) {
    std::vector<std::string> lines;
    int64_t case_id = 600;
    SchemaOptions dup_opts;
    dup_opts.keys_type = DUP_KEYS;
    auto schema = make_schema(numeric_key_cols(), dup_opts);
    std::vector<Block> blocks;
    blocks.push_back(build_block(schema, 0, 100));
    blocks.push_back(build_block(schema, 100, 50));
    {
        auto rowset = write_by_add_block(schema, case_id++, DataWriteType::TYPE_COMPACTION, blocks);
        fingerprint_rowset("dup_numeric_add_block_compaction", schema, rowset, 2, &lines);
    }
    {
        auto rowset =
                write_by_add_block(schema, case_id++, DataWriteType::TYPE_SCHEMA_CHANGE, blocks);
        fingerprint_rowset("dup_numeric_add_block_schema_change", schema, rowset, 2, &lines);
    }
    auto wide_schema = make_schema(wide_scalar_value_cols(), dup_opts);
    std::vector<Block> wide_blocks;
    wide_blocks.push_back(build_block(wide_schema, 0, 100));
    {
        auto rowset = write_by_add_block(wide_schema, case_id++, DataWriteType::TYPE_COMPACTION,
                                         wide_blocks);
        fingerprint_rowset("wide_scalar_add_block_compaction", wide_schema, rowset, 1, &lines);
    }
    check_golden("add_block_paths", lines);
}

// nested value types: array (incl. nested array + null items), map, struct
TEST_F(SegmentFormatGoldenTest, ComplexTypesFlush) {
    std::vector<std::string> lines;
    auto array_of = [](const std::string& name, ColSpec item, bool nullable) {
        ColSpec c = val_col(name, "ARRAY", nullable);
        item.is_nullable = true; // FE default: array items nullable
        c.children.push_back(std::move(item));
        return c;
    };
    std::vector<ColSpec> cols = {
            key_col("k_int", "INT"),
            array_of("v_arr_int", val_col("item", "INT"), false),
            array_of("v_arrn_int", val_col("item", "INT"), true),
            array_of("v_arr_str", val_col("item", "VARCHAR", false, "", 32), false),
    };
    {
        ColSpec inner = val_col("item", "ARRAY");
        ColSpec inner_item = val_col("item", "INT");
        inner_item.is_nullable = true;
        inner.is_nullable = true;
        inner.children.push_back(std::move(inner_item));
        ColSpec c = val_col("v_arr_arr_int", "ARRAY");
        c.children.push_back(std::move(inner));
        cols.push_back(std::move(c));
    }
    {
        ColSpec c = val_col("v_map_si", "MAP");
        // storage-level map key/value columns are both nullable
        c.children.push_back(val_col("key", "VARCHAR", true, "", 32));
        c.children.push_back(val_col("value", "INT", true));
        ColSpec cn = c;
        cn.name = "v_mapn_si";
        cn.is_nullable = true;
        cols.push_back(std::move(c));
        cols.push_back(std::move(cn));
    }
    {
        ColSpec c = val_col("v_struct", "STRUCT");
        c.children.push_back(val_col("f_int", "INT"));
        c.children.push_back(val_col("f_str", "VARCHAR", false, "", 32));
        ColSpec cn = c;
        cn.name = "v_structn";
        cn.is_nullable = true;
        cols.push_back(std::move(c));
        cols.push_back(std::move(cn));
    }
    SchemaOptions opts;
    opts.keys_type = DUP_KEYS;
    auto schema = make_schema(cols, opts);
    std::vector<Block> blocks;
    blocks.push_back(build_block(schema, 0, 100));
    blocks.push_back(build_block(schema, 100, 50));
    int64_t case_id = 700;
    for (bool vertical : {true, false}) {
        auto rowset = write_by_flush_single_block(schema, case_id++, vertical, blocks);
        fingerprint_rowset(fmt::format("complex_flush_{}", vertical ? "vertical" : "horizontal"),
                           schema, rowset, 2, &lines);
    }
    {
        auto rowset = write_by_add_block(schema, case_id++, DataWriteType::TYPE_COMPACTION, blocks);
        fingerprint_rowset("complex_add_block_compaction", schema, rowset, 2, &lines);
    }
    check_golden("complex_types_flush", lines);
}

// object value types on an aggregate table: HLL_UNION / BITMAP_UNION /
// QUANTILE_UNION / generic agg_state (fixed-length and string serialized)
TEST_F(SegmentFormatGoldenTest, ObjectTypesAggFlush) {
    std::vector<std::string> lines;
    std::vector<ColSpec> cols = {
            key_col("k_int", "INT"),
            val_col("v_hll", "HLL", false, "HLL_UNION"),
            val_col("v_bitmap", "OBJECT", false, "BITMAP_UNION"),
            val_col("v_qs", "QUANTILE_STATE", false, "QUANTILE_UNION"),
    };
    {
        // agg_state count(int): fixed-length-object serialized state
        ColSpec c = val_col("v_agg_count", "AGG_STATE", false, "count");
        c.is_agg_state = true;
        c.children.push_back(val_col("arg0", "INT"));
        cols.push_back(std::move(c));
    }
    {
        // agg_state hll_union(hll): string serialized state
        ColSpec c = val_col("v_agg_hllu", "AGG_STATE", false, "hll_union");
        c.is_agg_state = true;
        c.children.push_back(val_col("arg0", "HLL"));
        cols.push_back(std::move(c));
    }
    int64_t case_id = 800;
    for (auto format : {TABLET_STORAGE_FORMAT_V2, TABLET_STORAGE_FORMAT_V3}) {
        SchemaOptions opts;
        opts.keys_type = AGG_KEYS;
        opts.storage_format = format;
        auto schema = make_schema(cols, opts);
        std::vector<Block> blocks;
        blocks.push_back(build_block(schema, 0, 100));
        blocks.push_back(build_block(schema, 100, 50));
        for (bool vertical : {true, false}) {
            auto rowset = write_by_flush_single_block(schema, case_id++, vertical, blocks);
            fingerprint_rowset(fmt::format("object_agg_v{}_flush_{}",
                                           format == TABLET_STORAGE_FORMAT_V2 ? 2 : 3,
                                           vertical ? "vertical" : "horizontal"),
                               schema, rowset, 2, &lines);
        }
    }
    check_golden("object_types_agg_flush", lines);
}

// row-store column generation (full and subset), on flush and on the
// schema-change add_block path
TEST_F(SegmentFormatGoldenTest, RowStoreFlush) {
    std::vector<std::string> lines;
    std::vector<ColSpec> cols = {
            key_col("k_int", "INT"),
            key_col("k_varchar", "VARCHAR", 32),
            val_col("v_int", "INT", /*nullable=*/true),
            val_col("v_str", "STRING"),
            val_col("v_jsonb", "JSONB", /*nullable=*/true),
            val_col("v_datetimev2", "DATETIMEV2", false, "", 0, 0, 6),
            val_col(std::string(BeConsts::ROW_STORE_COL), "STRING"),
    };
    int64_t case_id = 900;
    for (bool subset : {false, true}) {
        SchemaOptions opts;
        opts.keys_type = DUP_KEYS;
        opts.store_row_column = true;
        if (subset) {
            // uids of k_int, v_str (assigned 1-based in schema order)
            opts.row_store_column_unique_ids = {1, 4};
        }
        auto schema = make_schema(cols, opts);
        std::vector<Block> blocks;
        blocks.push_back(build_block(schema, 0, 100));
        blocks.push_back(build_block(schema, 100, 50));
        for (bool vertical : {true, false}) {
            auto rowset = write_by_flush_single_block(schema, case_id++, vertical, blocks);
            fingerprint_rowset(fmt::format("row_store{}_flush_{}", subset ? "_subset" : "",
                                           vertical ? "vertical" : "horizontal"),
                               schema, rowset, 2, &lines);
        }
        auto rowset =
                write_by_add_block(schema, case_id++, DataWriteType::TYPE_SCHEMA_CHANGE, blocks);
        fingerprint_rowset(
                fmt::format("row_store{}_add_block_schema_change", subset ? "_subset" : ""), schema,
                rowset, 2, &lines);
    }
    check_golden("row_store_flush", lines);
}

// variant columns: unparsed root-scalar input so the write path performs the
// variant parse; with and without a row-store column (the row-store bytes are
// serialized from the parsed variant)
TEST_F(SegmentFormatGoldenTest, VariantFlush) {
    std::vector<std::string> lines;
    int64_t case_id = 1000;
    for (bool with_row_store : {false, true}) {
        std::vector<ColSpec> cols = {
                key_col("k_int", "INT"),
                val_col("v_variant", "VARIANT"),
                val_col("v_int", "INT", /*nullable=*/true),
        };
        SchemaOptions opts;
        opts.keys_type = DUP_KEYS;
        opts.storage_format = TABLET_STORAGE_FORMAT_V3;
        if (with_row_store) {
            cols.push_back(val_col(std::string(BeConsts::ROW_STORE_COL), "STRING"));
            opts.store_row_column = true;
        }
        auto schema = make_schema(cols, opts);
        std::vector<Block> blocks;
        blocks.push_back(build_block(schema, 0, 100));
        blocks.push_back(build_block(schema, 100, 50));
        for (bool vertical : {true, false}) {
            auto rowset = write_by_flush_single_block(schema, case_id++, vertical, blocks);
            fingerprint_rowset(fmt::format("variant{}_flush_{}", with_row_store ? "_row_store" : "",
                                           vertical ? "vertical" : "horizontal"),
                               schema, rowset, 2, &lines);
        }
        auto rowset =
                write_by_add_block(schema, case_id++, DataWriteType::TYPE_SCHEMA_CHANGE, blocks);
        fingerprint_rowset(fmt::format("variant{}_add_block_schema_change",
                                       with_row_store ? "_row_store" : ""),
                           schema, rowset, 2, &lines);
    }
    check_golden("variant_flush", lines);
}

// MoW with cluster keys: short key index from the cluster-key view, primary
// key index from the schema-key view (+ rowid / seq suffixes)
TEST_F(SegmentFormatGoldenTest, MowClusterKeyFlush) {
    std::vector<std::string> lines;
    int64_t case_id = 1100;
    for (bool with_seq : {false, true}) {
        std::vector<ColSpec> cols = {
                key_col("k_int", "INT"),             // uid 1
                key_col("k_varchar", "VARCHAR", 32), // uid 2
                val_col("v_ck_sort", "BIGINT"),      // uid 3, cluster sort column
                val_col("v_str", "STRING", false, "REPLACE"),
        };
        SchemaOptions opts;
        opts.keys_type = UNIQUE_KEYS;
        opts.cluster_key_uids = {3, 1};
        opts.num_short_key_columns = 2;
        if (with_seq) {
            cols.push_back(val_col(std::string(SEQUENCE_COL), "BIGINT", false, "REPLACE"));
            opts.sequence_col_idx = static_cast<int>(cols.size()) - 1;
        }
        cols.push_back(val_col(std::string(DELETE_SIGN), "TINYINT", false, "REPLACE"));
        auto schema = make_schema(cols, opts);
        // v_ck_sort is monotonic in row index, so blocks are sorted in
        // cluster-key order as the memtable guarantees for MoW-CK
        std::vector<Block> blocks;
        blocks.push_back(build_block(schema, 0, 100));
        blocks.push_back(build_block(schema, 100, 50));
        for (bool vertical : {true, false}) {
            auto mow_delete_bitmap = std::make_shared<DeleteBitmap>(1);
            auto tablet = make_fake_tablet(schema, 20000 + case_id, /*enable_mow=*/true);
            auto rowset = write_by_flush_single_block(
                    schema, case_id++, vertical, blocks, [&](RowsetWriterContext& ctx) {
                        ctx.tablet = tablet;
                        ctx.enable_unique_key_merge_on_write = true;
                        ctx.mow_context = std::make_shared<MowContext>(
                                1, 1000, std::make_shared<RowsetIdUnorderedSet>(),
                                std::vector<RowsetSharedPtr> {}, mow_delete_bitmap);
                    });
            fingerprint_rowset(fmt::format("mow_ck{}_flush_{}", with_seq ? "_seq" : "",
                                           vertical ? "vertical" : "horizontal"),
                               schema, rowset, 2, &lines, dump_delete_bitmap(*mow_delete_bitmap));
        }
    }
    check_golden("mow_cluster_key_flush", lines);
}

// vertical compaction output: Merger::vertical_merge_rowsets ->
// VerticalBetaRowsetWriter::add_columns -> column-group SegmentWriter.
// This path bypasses SegmentFlusher entirely; it pins the short-key /
// primary-key index building of the column-group writer.
TEST_F(SegmentFormatGoldenTest, VerticalCompactionMerge) {
    std::vector<std::string> lines;
    int64_t case_id = 1300;

    struct MergeCase {
        std::string name;
        TabletSchemaSPtr schema;
        bool mow = false;
        uint32_t max_rows_per_segment = 100000;
    };
    std::vector<MergeCase> cases;
    {
        SchemaOptions opts;
        opts.keys_type = DUP_KEYS;
        cases.push_back({"vcomp_dup_numeric", make_schema(numeric_key_cols(), opts)});
    }
    {
        SchemaOptions opts;
        opts.keys_type = AGG_KEYS;
        std::vector<ColSpec> cols = {
                key_col("k_int", "INT"),
                val_col("v_sum_bigint", "BIGINT", false, "SUM"),
                val_col("v_max_varchar", "VARCHAR", false, "MAX", 64),
                val_col("v_rinn_int", "INT", true, "REPLACE_IF_NOT_NULL"),
        };
        cases.push_back({"vcomp_agg", make_schema(cols, opts)});
    }
    {
        SchemaOptions opts;
        opts.keys_type = UNIQUE_KEYS;
        std::vector<ColSpec> cols = {
                key_col("k_int", "INT"),
                key_col("k_varchar", "VARCHAR", 32),
                val_col("v_str", "STRING", false, "REPLACE"),
                val_col(std::string(SEQUENCE_COL), "BIGINT", false, "REPLACE"),
                val_col(std::string(DELETE_SIGN), "TINYINT", false, "REPLACE"),
        };
        opts.sequence_col_idx = 3;
        cases.push_back({"vcomp_mow_seq", make_schema(cols, opts), /*mow=*/true});
    }
    {
        SchemaOptions opts;
        opts.keys_type = UNIQUE_KEYS;
        std::vector<ColSpec> cols = {
                key_col("k_int", "INT"),
                key_col("k_varchar", "VARCHAR", 32),
                val_col("v_ck_sort", "BIGINT"),
                val_col("v_str", "STRING", false, "REPLACE"),
                val_col(std::string(DELETE_SIGN), "TINYINT", false, "REPLACE"),
        };
        opts.cluster_key_uids = {3, 1};
        opts.num_short_key_columns = 2;
        cases.push_back({"vcomp_mow_ck", make_schema(cols, opts), /*mow=*/true});
    }

    for (const auto& merge_case : cases) {
        const auto& schema = merge_case.schema;
        // two overlapping input rowsets (keys 0..149 and 50..199)
        std::vector<RowsetSharedPtr> input_rowsets;
        for (int i = 0; i < 2; ++i) {
            std::vector<Block> blocks;
            blocks.push_back(build_block(schema, i * 50, 100));
            blocks.push_back(build_block(schema, i * 50 + 100, 50));
            auto rowset = write_by_flush_single_block(
                    schema, case_id++, true, blocks,
                    [&](RowsetWriterContext& ctx) { ctx.version = Version(2 + i, 2 + i); });
            ASSERT_NE(rowset, nullptr);
            input_rowsets.push_back(rowset);
        }
        std::vector<RowsetReaderSharedPtr> input_rs_readers;
        for (auto& rowset : input_rowsets) {
            RowsetReaderSharedPtr rs_reader;
            ASSERT_TRUE(rowset->create_reader(&rs_reader).ok());
            input_rs_readers.push_back(std::move(rs_reader));
        }

        auto ctx = make_context(schema, case_id++, DataWriteType::TYPE_COMPACTION);
        ctx.version = Version(2, 3);
        ctx.segments_overlap = NONOVERLAPPING;
        ctx.max_rows_per_segment = merge_case.max_rows_per_segment;
        auto res = RowsetFactory::create_rowset_writer(*s_engine, ctx, /*is_vertical=*/true);
        ASSERT_TRUE(res.has_value()) << res.error();
        auto output_writer = std::move(res).value();

        auto tablet = make_fake_tablet(schema, 20000 + case_id, merge_case.mow);
        Merger::Statistics stats;
        RowIdConversion rowid_conversion;
        stats.rowid_conversion = &rowid_conversion;
        auto st = Merger::vertical_merge_rowsets(
                tablet, ReaderType::READER_BASE_COMPACTION, *schema, input_rs_readers,
                output_writer.get(), merge_case.max_rows_per_segment, /*merge_way_num=*/2, &stats);
        ASSERT_TRUE(st.ok()) << merge_case.name << " " << st;
        RowsetSharedPtr out_rowset;
        ASSERT_EQ(Status::OK(), output_writer->build(out_rowset));
        fingerprint_rowset(merge_case.name, schema, out_rowset, -1, &lines);
    }
    check_golden("vertical_compaction_merge", lines);
}

// transient rowset writer (publish-conflict repair path): full sorted block
// through flush_single_block with partial_update_info set but the transient
// flag routing it through the plain write path
TEST_F(SegmentFormatGoldenTest, TransientPublishConflictFlush) {
    std::vector<std::string> lines;
    std::vector<ColSpec> cols = {
            key_col("k_int", "INT"),
            key_col("k_varchar", "VARCHAR", 32),
            val_col("v_str", "STRING", false, "REPLACE"),
            val_col("v_int", "INT", true, "REPLACE"),
            val_col(std::string(DELETE_SIGN), "TINYINT", false, "REPLACE"),
    };
    SchemaOptions opts;
    opts.keys_type = UNIQUE_KEYS;
    auto schema = make_schema(cols, opts);
    auto partial_update_info = std::make_shared<PartialUpdateInfo>();
    ASSERT_TRUE(partial_update_info
                        ->init(30001, 1000, *schema, UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS,
                               PartialUpdateNewRowPolicyPB::APPEND, {"k_int", "k_varchar", "v_str"},
                               /*is_strict_mode=*/false,
                               /*timestamp_ms=*/0, /*nano_seconds=*/0, "Asia/Shanghai",
                               /*auto_increment_column=*/"", -1, /*cur_max_version=*/1)
                        .ok());
    std::vector<Block> blocks;
    blocks.push_back(build_block(schema, 0, 100));
    blocks.push_back(build_block(schema, 100, 50));
    int64_t case_id = 1200;
    for (bool vertical : {true, false}) {
        auto mow_delete_bitmap = std::make_shared<DeleteBitmap>(1);
        auto tablet = make_fake_tablet(schema, 20000 + case_id, /*enable_mow=*/true);
        auto rowset = write_by_flush_single_block(
                schema, case_id++, vertical, blocks, [&](RowsetWriterContext& ctx) {
                    ctx.tablet = tablet;
                    ctx.enable_unique_key_merge_on_write = true;
                    ctx.is_transient_rowset_writer = true;
                    ctx.partial_update_info = partial_update_info;
                    ctx.mow_context = std::make_shared<MowContext>(
                            1, 1000, std::make_shared<RowsetIdUnorderedSet>(),
                            std::vector<RowsetSharedPtr> {}, mow_delete_bitmap);
                });
        fingerprint_rowset(fmt::format("transient_flush_{}", vertical ? "vertical" : "horizontal"),
                           schema, rowset, 2, &lines);
    }
    check_golden("transient_flush", lines);
}

// fixed partial update: narrow input block widened with values read from a
// history rowset (or defaults/null for new keys); covers prefix and
// non-prefix update column sets and a sequence-column schema without an
// input sequence column
TEST_F(SegmentFormatGoldenTest, FixedPartialUpdateFlush) {
    std::vector<std::string> lines;
    int64_t case_id = 1500;
    for (bool with_seq : {false, true}) {
        std::vector<ColSpec> cols = {
                key_col("k_int", "INT"),             // uid 1
                key_col("k_varchar", "VARCHAR", 32), // uid 2
        };
        ColSpec str_col = val_col("v_str", "STRING", false, "REPLACE"); // uid 3
        str_col.default_value = "default_str";
        cols.push_back(std::move(str_col));
        cols.push_back(val_col("v_int", "INT", true, "REPLACE"));       // uid 4
        ColSpec def_col = val_col("v_def", "BIGINT", false, "REPLACE"); // uid 5
        def_col.default_value = "9999";
        cols.push_back(std::move(def_col));
        SchemaOptions opts;
        opts.keys_type = UNIQUE_KEYS;
        if (with_seq) {
            // the hidden sequence column FE creates is nullable
            cols.push_back(val_col(std::string(SEQUENCE_COL), "BIGINT", true, "REPLACE"));
            opts.sequence_col_idx = static_cast<int>(cols.size()) - 1;
        }
        cols.push_back(val_col(std::string(DELETE_SIGN), "TINYINT", false, "REPLACE"));
        auto schema = make_schema(cols, opts);
        auto tablet = make_fake_tablet(schema, 21500 + case_id, /*enable_mow=*/true);

        // history rowset: full upsert of keys 0..99 at version 2
        auto history_bitmap = std::make_shared<DeleteBitmap>(1);
        std::vector<Block> history_blocks;
        history_blocks.push_back(build_block(schema, 0, 100));
        auto history = write_by_flush_single_block(
                schema, case_id++, true, history_blocks, [&](RowsetWriterContext& ctx) {
                    ctx.tablet = tablet;
                    ctx.enable_unique_key_merge_on_write = true;
                    ctx.mow_context = std::make_shared<MowContext>(
                            1, 999, std::make_shared<RowsetIdUnorderedSet>(),
                            std::vector<RowsetSharedPtr> {}, history_bitmap);
                });
        ASSERT_NE(history, nullptr);

        struct PuCase {
            std::string name;
            std::set<std::string> update_cols;
        };
        std::vector<PuCase> pu_cases = {
                {"prefix", {"k_int", "k_varchar", "v_str"}},
                {"nonprefix", {"k_int", "k_varchar", "v_int"}},
        };
        for (const auto& pu_case : pu_cases) {
            // updated rows 50..99 exist in history, 100..129 are new keys
            Block full_block = build_block(schema, 50, 80);
            Block narrow_block;
            for (size_t cid = 0; cid < schema->num_columns(); ++cid) {
                if (pu_case.update_cols.contains(schema->column(cid).name())) {
                    narrow_block.insert(full_block.get_by_position(cid));
                }
            }
            for (bool vertical : {true, false}) {
                auto partial_update_info = std::make_shared<PartialUpdateInfo>();
                ASSERT_TRUE(partial_update_info
                                    ->init(21500 + case_id, 1001, *schema,
                                           UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS,
                                           PartialUpdateNewRowPolicyPB::APPEND, pu_case.update_cols,
                                           /*is_strict_mode=*/false,
                                           /*timestamp_ms=*/0, /*nano_seconds=*/0, "Asia/Shanghai",
                                           /*auto_increment_column=*/"", -1,
                                           /*cur_max_version=*/2)
                                    .ok());
                auto pu_bitmap = std::make_shared<DeleteBitmap>(1);
                auto rowset_ids = std::make_shared<RowsetIdUnorderedSet>();
                rowset_ids->insert(history->rowset_id());
                auto rowset = write_by_flush_single_block(
                        schema, case_id++, vertical, {narrow_block}, [&](RowsetWriterContext& ctx) {
                            ctx.tablet = tablet;
                            ctx.enable_unique_key_merge_on_write = true;
                            ctx.partial_update_info = partial_update_info;
                            ctx.mow_context = std::make_shared<MowContext>(
                                    2, 1001, rowset_ids, std::vector<RowsetSharedPtr> {history},
                                    pu_bitmap);
                        });
                // vertical + non-prefix update set: physical column order is
                // implementation-defined (see fingerprint_rowset comment)
                bool layout_sensitive = vertical && pu_case.name == "nonprefix";
                fingerprint_rowset(fmt::format("fixed_pu{}_{}_{}", with_seq ? "_seq" : "",
                                               pu_case.name, vertical ? "vertical" : "horizontal"),
                                   schema, rowset, 1, &lines, dump_delete_bitmap(*pu_bitmap),
                                   layout_sensitive);
            }
        }
    }
    check_golden("fixed_partial_update_flush", lines);
}

// flexible partial update: full-width block + per-row skip bitmaps (column
// unique ids), incl. new keys with defaults, skipped default column, an
// existing-key delete row and a delete-then-insert same-key pair
TEST_F(SegmentFormatGoldenTest, FlexiblePartialUpdateFlush) {
    std::vector<std::string> lines;
    std::vector<ColSpec> cols = {
            key_col("k_int", "INT"),                // uid 1
            val_col("v_a", "INT", true, "REPLACE"), // uid 2
    };
    ColSpec b_col = val_col("v_b", "STRING", false, "REPLACE"); // uid 3
    b_col.default_value = "bdef";
    cols.push_back(std::move(b_col));
    ColSpec def_col = val_col("v_def", "BIGINT", false, "REPLACE"); // uid 4
    def_col.default_value = "9999";
    cols.push_back(std::move(def_col));
    cols.push_back(val_col(std::string(DELETE_SIGN), "TINYINT", false, "REPLACE"));    // uid 5
    cols.push_back(val_col(std::string(SKIP_BITMAP_COL), "OBJECT", false, "REPLACE")); // uid 6
    SchemaOptions opts;
    opts.keys_type = UNIQUE_KEYS;
    opts.skip_bitmap_col_idx = 5;
    auto schema = make_schema(cols, opts);
    int64_t case_id = 1600;
    auto tablet = make_fake_tablet(schema, 21600, /*enable_mow=*/true);

    // history: keys 0..99 full upsert (empty skip bitmaps)
    auto history_bitmap = std::make_shared<DeleteBitmap>(1);
    std::vector<Block> history_blocks;
    history_blocks.push_back(build_block(schema, 0, 100));
    auto history = write_by_flush_single_block(
            schema, case_id++, true, history_blocks, [&](RowsetWriterContext& ctx) {
                ctx.tablet = tablet;
                ctx.enable_unique_key_merge_on_write = true;
                ctx.mow_context = std::make_shared<MowContext>(
                        1, 999, std::make_shared<RowsetIdUnorderedSet>(),
                        std::vector<RowsetSharedPtr> {}, history_bitmap);
            });
    ASSERT_NE(history, nullptr);

    // flexible load: keys 60..99 exist, 100..109 new, then an existing-key
    // delete (k index 130) and a delete-then-insert pair (k index 200)
    struct FlexRow {
        int64_t r;
        std::vector<int64_t> skip_uids;
        bool delete_sign = false;
    };
    std::vector<FlexRow> rows;
    for (int64_t r = 60; r < 110; ++r) {
        FlexRow row;
        row.r = r;
        switch (r % 4) {
        case 0:
            row.skip_uids = {2};
            break;
        case 1:
            row.skip_uids = {3};
            break;
        case 2:
            row.skip_uids = {2, 3};
            break;
        default:
            break;
        }
        if (r % 2 == 0) {
            row.skip_uids.push_back(4); // skipped default column
        }
        rows.push_back(std::move(row));
    }
    {
        // delete row for an existing key (history has 0..99; use 80? already
        // in range - use a dedicated key outside the update range)
        FlexRow del;
        del.r = 30; // exists in history, not in 60..109
        del.skip_uids = {2, 3, 4};
        del.delete_sign = true;
        rows.insert(rows.begin(), del); // keys ascending: 30 first
    }
    {
        FlexRow del;
        del.r = 200;
        del.skip_uids = {2, 3, 4};
        del.delete_sign = true;
        rows.push_back(del);
        FlexRow ins;
        ins.r = 200;
        ins.skip_uids = {2};
        rows.push_back(ins);
    }

    Block block = schema->create_block();
    {
        auto columns = std::move(block).mutate_columns();
        DataTypeSerDe::FormatOptions format_options;
        for (size_t cid = 0; cid < schema->num_columns(); ++cid) {
            const auto& col = *schema->columns()[cid];
            auto serde = block.get_by_position(cid).type->get_serde();
            for (const auto& row : rows) {
                if (col.name() == DELETE_SIGN) {
                    int8_t sign = row.delete_sign ? 1 : 0;
                    columns[cid]->insert_data(reinterpret_cast<const char*>(&sign), sizeof(sign));
                    continue;
                }
                if (col.name() == SKIP_BITMAP_COL) {
                    BitmapValue skip;
                    for (int64_t uid : row.skip_uids) {
                        skip.add(static_cast<uint64_t>(uid));
                    }
                    assert_cast<ColumnBitmap&>(*columns[cid]).insert_value(std::move(skip));
                    continue;
                }
                bool skipped = std::find(row.skip_uids.begin(), row.skip_uids.end(),
                                         col.unique_id()) != row.skip_uids.end();
                if (skipped ||
                    (col.is_nullable() && !col.is_key() && (row.r + col.unique_id()) % 7 == 0)) {
                    columns[cid]->insert_default();
                    continue;
                }
                std::string text = cell_text(col, row.r);
                Slice slice(text.data(), text.size());
                auto st =
                        serde->deserialize_one_cell_from_json(*columns[cid], slice, format_options);
                EXPECT_TRUE(st.ok()) << "col=" << col.name() << " text=" << text << " " << st;
            }
        }
        block.set_columns(std::move(columns));
    }

    auto partial_update_info = std::make_shared<PartialUpdateInfo>();
    ASSERT_TRUE(partial_update_info
                        ->init(21600, 1001, *schema, UniqueKeyUpdateModePB::UPDATE_FLEXIBLE_COLUMNS,
                               PartialUpdateNewRowPolicyPB::APPEND, {}, /*is_strict_mode=*/false,
                               /*timestamp_ms=*/0, /*nano_seconds=*/0, "Asia/Shanghai",
                               /*auto_increment_column=*/"", -1, /*cur_max_version=*/2)
                        .ok());
    auto pu_bitmap = std::make_shared<DeleteBitmap>(1);
    auto rowset_ids = std::make_shared<RowsetIdUnorderedSet>();
    rowset_ids->insert(history->rowset_id());
    // base branch supports flexible partial update only on the vertical writer
    auto rowset = write_by_flush_single_block(
            schema, case_id++, /*vertical_writer=*/true, {block}, [&](RowsetWriterContext& ctx) {
                ctx.tablet = tablet;
                ctx.enable_unique_key_merge_on_write = true;
                ctx.partial_update_info = partial_update_info;
                ctx.mow_context = std::make_shared<MowContext>(
                        2, 1001, rowset_ids, std::vector<RowsetSharedPtr> {history}, pu_bitmap);
            });
    fingerprint_rowset("flexible_pu_vertical", schema, rowset, 1, &lines,
                       dump_delete_bitmap(*pu_bitmap));
    check_golden("flexible_partial_update_flush", lines);
}

// segcompaction: many small flushed segments merged in the background by
// SegcompactionWorker through create_segment_writer_for_segcompaction
TEST_F(SegmentFormatGoldenTest, Segcompaction) {
    bool saved_enable = config::enable_segcompaction;
    int64_t saved_batch = config::segcompaction_batch_size;
    int64_t saved_max_rows = config::segcompaction_candidate_max_rows;
    config::enable_segcompaction = true;
    config::segcompaction_batch_size = 5;
    config::segcompaction_candidate_max_rows = 6000;

    if (s_engine->_seg_compaction_thread_pool == nullptr) {
        ASSERT_TRUE(ThreadPoolBuilder("SegCompactionTaskThreadPool")
                            .set_min_threads(2)
                            .set_max_threads(2)
                            .build(&s_engine->_seg_compaction_thread_pool)
                            .ok());
    }
    {
        // RowSourcesBuffer used by the vertical group merge spills under the
        // tmp file dirs
        std::vector<StorePath> paths;
        paths.emplace_back(_absolute_dir + "/tmp", 1024000000);
        auto tmp_file_dirs = std::make_unique<segment_v2::TmpFileDirs>(paths);
        ASSERT_TRUE(tmp_file_dirs->init().ok());
        ExecEnv::GetInstance()->set_tmp_file_dir(std::move(tmp_file_dirs));
    }

    std::vector<std::string> lines;
    SchemaOptions opts;
    opts.keys_type = DUP_KEYS;
    auto schema = make_schema(numeric_key_cols(), opts);

    auto ctx = make_context(schema, 1400, DataWriteType::TYPE_DIRECT);
    ctx.tablet = make_fake_tablet(schema, 21400, /*enable_mow=*/false);
    ctx.enable_segcompaction = true;
    auto res = RowsetFactory::create_rowset_writer(*s_engine, ctx, false);
    ASSERT_TRUE(res.has_value()) << res.error();
    auto writer = std::move(res).value();
    for (int seg = 0; seg < 8; ++seg) {
        Block block = build_block(schema, seg * 50, 50);
        ASSERT_TRUE(writer->add_block(&block).ok());
        ASSERT_TRUE(writer->flush().ok());
        sleep(1); // let the async segcompaction trigger points stay deterministic
    }
    RowsetSharedPtr rowset;
    ASSERT_EQ(Status::OK(), writer->build(rowset));
    fingerprint_rowset("segcompaction_dup", schema, rowset, -1, &lines);
    check_golden("segcompaction", lines);

    config::enable_segcompaction = saved_enable;
    config::segcompaction_batch_size = saved_batch;
    config::segcompaction_candidate_max_rows = saved_max_rows;
}

// horizontal binlog compaction pass-through: add_block on a binlog-schema
// rowset writer with the binlog option set (base wraps it in
// RowBinlogSegmentWriter's direct mode, the refactor uses the plain writer)
TEST_F(SegmentFormatGoldenTest, BinlogCompactionAddBlock) {
    std::vector<std::string> lines;
    std::vector<ColSpec> cols = {
            key_col("k_int", "INT"),
            val_col("v_int", "INT"),
            val_col(BINLOG_LSN_COL, "LARGEINT"),
            val_col(std::string(kRowBinlogOpColName), "BIGINT", /*nullable=*/true),
            val_col(BINLOG_TIMESTAMP_COL, "BIGINT", /*nullable=*/true),
    };
    SchemaOptions opts;
    opts.keys_type = DUP_KEYS;
    auto schema = make_schema(cols, opts);
    std::vector<Block> blocks;
    blocks.push_back(build_block(schema, 0, 100));
    blocks.push_back(build_block(schema, 100, 50));
    auto ctx = make_context(schema, 1450, DataWriteType::TYPE_COMPACTION);
    ctx.write_binlog_opt().enable = true;
    auto res = RowsetFactory::create_rowset_writer(*s_engine, ctx, false);
    ASSERT_TRUE(res.has_value()) << res.error();
    auto writer = std::move(res).value();
    for (const auto& block : blocks) {
        ASSERT_TRUE(writer->add_block(&block).ok());
        ASSERT_TRUE(writer->flush().ok());
    }
    RowsetSharedPtr rowset;
    ASSERT_EQ(Status::OK(), writer->build(rowset));
    fingerprint_rowset("binlog_compaction_add_block", schema, rowset, 2, &lines);
    check_golden("binlog_compaction_add_block", lines);
}

// Row-binlog segments written alongside data segments by GroupRowsetWriter.
// Needs a real tablet (row_binlog_tablet_schema, LSN bookkeeping), so this
// fixture opens a full storage engine.
class SegmentFormatGoldenBinlogTest : public SegmentFormatGoldenTest {
protected:
    void SetUp() override {
        _saved_vertical_writer = config::enable_vertical_segment_writer;

        char buffer[MAX_PATH_LEN];
        ASSERT_NE(getcwd(buffer, MAX_PATH_LEN), nullptr);
        _absolute_dir = std::string(buffer) + "/ut_dir/segment_format_golden_binlog";
        config::storage_root_path = _absolute_dir;
        ASSERT_TRUE(io::global_local_filesystem()->delete_directory(_absolute_dir).ok());
        ASSERT_TRUE(io::global_local_filesystem()->create_directory(_absolute_dir).ok());

        std::vector<StorePath> paths;
        paths.emplace_back(_absolute_dir, -1);
        EngineOptions options;
        options.store_paths = paths;
        auto engine = std::make_unique<StorageEngine>(options);
        s_engine = engine.get();
        ASSERT_TRUE(s_engine->open().ok());
        ExecEnv::GetInstance()->set_storage_engine(std::move(engine));

        const char* root = getenv("ROOT");
        ASSERT_NE(root, nullptr);
        _expected_dir = std::string(root) + "/be/test/expected_result/storage/segment_format";
        if (FLAGS_gen_out) {
            std::filesystem::create_directories(_expected_dir);
        }
    }

    void TearDown() override {
        config::enable_vertical_segment_writer = _saved_vertical_writer;
        _tablet.reset();
        s_engine = nullptr;
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_absolute_dir).ok());
    }

    void create_binlog_tablet(TCreateTabletReq request) {
        testutil::enable_row_binlog(&request);
        auto profile = std::make_unique<RuntimeProfile>("SegmentFormatGoldenBinlogTest");
        ASSERT_TRUE(s_engine->create_tablet(request, profile.get()).ok());
        _tablet = s_engine->tablet_manager()->get_tablet(request.tablet_id);
        ASSERT_NE(_tablet, nullptr);
        ASSERT_TRUE(
                io::global_local_filesystem()->create_directory(_tablet->row_binlog_path()).ok());
    }

    // group write: one segment per block on both the data and binlog side,
    // deterministic LSN ranges registered per segment
    void write_group_rowsets(const std::vector<Block>& blocks,
                             std::vector<RowsetSharedPtr>* rowsets) {
        RowsetWriterContext data_context;
        data_context.tablet = _tablet;
        data_context.tablet_schema = _tablet->tablet_schema();
        data_context.rowset_state = PREPARED;
        data_context.segments_overlap = OVERLAPPING;
        data_context.max_rows_per_segment = UINT32_MAX;
        data_context.write_type = DataWriteType::TYPE_DIRECT;
        data_context.is_transient_rowset_writer = true;
        RowsetId data_rowset_id = s_engine->next_rowset_id();
        auto data_writer_res =
                _tablet->create_transient_rowset_writer(data_context, data_rowset_id);
        ASSERT_TRUE(data_writer_res.has_value()) << data_writer_res.error();

        RowsetWriterContext binlog_context;
        binlog_context.tablet = _tablet;
        binlog_context.tablet_schema = _tablet->row_binlog_tablet_schema();
        binlog_context.rowset_state = PREPARED;
        binlog_context.segments_overlap = NONOVERLAPPING;
        binlog_context.max_rows_per_segment = UINT32_MAX;
        binlog_context.write_type = DataWriteType::TYPE_DIRECT;
        binlog_context.is_transient_rowset_writer = true;
        binlog_context.write_binlog_opt().enable = true;
        auto& cfg = binlog_context.write_binlog_opt().write_binlog_config();
        cfg.source.tablet_schema = _tablet->tablet_schema();
        cfg.source.is_transient_rowset_writer = true;
        cfg.source.source_write_type = DataWriteType::TYPE_DIRECT;

        size_t total_rows = 0;
        for (const auto& block : blocks) {
            total_rows += block.rows();
        }
        auto lsn_buffer = AutoIncIDBuffer::create_shared(1, 1, kBinlogLsnAutoIncId);
        lsn_buffer->append_range_for_test(1000, total_rows);
        for (size_t seg = 0; seg < blocks.size(); ++seg) {
            std::shared_ptr<std::vector<int64_t>> lsn_ids;
            ASSERT_TRUE(allocate_binlog_lsn(lsn_buffer, blocks[seg].rows(), &lsn_ids).ok());
            cfg.insert_seg_lsn(static_cast<int32_t>(seg), lsn_ids);
        }
        auto binlog_writer_res = _tablet->create_rowset_writer(binlog_context, false);
        ASSERT_TRUE(binlog_writer_res.has_value()) << binlog_writer_res.error();

        GroupRowsetWriter group_writer;
        group_writer.set_data_writer(
                std::shared_ptr<RowsetWriter>(std::move(data_writer_res.value())));
        group_writer.set_row_binlog_writer(
                std::shared_ptr<RowsetWriter>(std::move(binlog_writer_res.value())));
        for (const auto& block : blocks) {
            auto st = group_writer.flush_single_block(&block);
            ASSERT_TRUE(st.ok()) << st;
        }
        ASSERT_TRUE(group_writer.build_rowsets(*rowsets).ok());
        ASSERT_EQ(rowsets->size(), 2);
    }

    TabletSharedPtr _tablet;
};

// DUP source (plain APPEND ops) and MoW source with delete-sign rows
// (DELETE ops), under both segment-writer configs. On the base branch the
// binlog side is always written by the horizontal RowBinlogSegmentWriter
// regardless of config.
TEST_F(SegmentFormatGoldenBinlogTest, RowBinlogDirect) {
    std::vector<std::string> lines;
    int64_t tablet_id = 30000;
    for (bool vertical : {false, true}) {
        config::enable_vertical_segment_writer = vertical;
        const std::string cfg_name = vertical ? "vertical" : "horizontal";
        {
            ++tablet_id;
            auto request = testutil::create_tablet_request(
                    tablet_id, 270070000 + tablet_id, 10001, 1, TKeysType::DUP_KEYS,
                    {{"k1", TPrimitiveType::INT, true}, {"v1", TPrimitiveType::INT, false}});
            create_binlog_tablet(request);
            std::vector<Block> blocks;
            blocks.push_back(build_block(_tablet->tablet_schema(), 0, 100));
            blocks.push_back(build_block(_tablet->tablet_schema(), 100, 50));
            std::vector<RowsetSharedPtr> rowsets;
            write_group_rowsets(blocks, &rowsets);
            fingerprint_rowset(fmt::format("binlog_dup_cfg_{}_data", cfg_name),
                               _tablet->tablet_schema(), rowsets[0], 2, &lines);
            fingerprint_rowset(fmt::format("binlog_dup_cfg_{}_binlog", cfg_name),
                               _tablet->row_binlog_tablet_schema(), rowsets[1], 2, &lines);
            _tablet.reset();
        }
        {
            ++tablet_id;
            auto request = testutil::create_tablet_request(
                    tablet_id, 270070000 + tablet_id, 10001, 1, TKeysType::UNIQUE_KEYS,
                    {{"k1", TPrimitiveType::INT, true},
                     {"v1", TPrimitiveType::INT, false},
                     {std::string(DELETE_SIGN), TPrimitiveType::TINYINT, false}});
            request.__set_enable_unique_key_merge_on_write(true);
            create_binlog_tablet(request);
            std::vector<Block> blocks;
            blocks.push_back(build_block(_tablet->tablet_schema(), 0, 100, {7, 8}));
            blocks.push_back(build_block(_tablet->tablet_schema(), 100, 50));
            std::vector<RowsetSharedPtr> rowsets;
            write_group_rowsets(blocks, &rowsets);
            fingerprint_rowset(fmt::format("binlog_mow_cfg_{}_data", cfg_name),
                               _tablet->tablet_schema(), rowsets[0], 2, &lines);
            fingerprint_rowset(fmt::format("binlog_mow_cfg_{}_binlog", cfg_name),
                               _tablet->row_binlog_tablet_schema(), rowsets[1], 2, &lines);
            _tablet.reset();
        }
    }
    config::enable_vertical_segment_writer = _saved_vertical_writer;
    check_golden("row_binlog_direct", lines);
}

} // namespace doris
