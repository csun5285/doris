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

#include <gtest/gtest.h>

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "core/assert_cast.h"
#include "core/column/column_vector.h"
#include "core/field.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "storage/iterators.h"
#include "storage/olap_common.h"
#include "storage/olap_tuple.h"
#include "storage/row_cursor.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/segment/row_ranges.h"
#include "storage/segment/segment.h"
#include "storage/segment/shared_segment_index_result.h"
#include "storage/segment/test_segment_writer.h"
#include "storage/tablet/tablet_schema_helper.h"

namespace doris::segment_v2 {
namespace {

constexpr auto kTestDir = "./ut_dir/segment_iterator_short_key_test";
constexpr int kNumRows = 4000;
constexpr int kRowsPerBlock = 16;
// Every k0 value spans more than two short key blocks.
constexpr int kRowsPerK0 = 100;
const RowsetId kRowsetId {.version = 1};

// Row `rid` holds (k0, k1) = (rid / kRowsPerK0, rid % kRowsPerK0 * 2): sorted, with only even k1.
std::pair<int32_t, int32_t> row_key(int rid) {
    return {rid / kRowsPerK0, rid % kRowsPerK0 * 2};
}

TabletSchemaSPtr make_tablet_schema(int num_short_key_columns) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    schema_pb.set_num_short_key_columns(num_short_key_columns);
    create_int_key(0, false)->to_schema_pb(schema_pb.add_column());
    create_int_key(1, false)->to_schema_pb(schema_pb.add_column());
    auto tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->init_from_pb(schema_pb);
    tablet_schema->set_storage_page_size(4096);
    return tablet_schema;
}

// A key range bound of the leading `values.size()` key columns.
struct Bound {
    std::vector<int32_t> values;
    bool include;
};

// Compares the key of `rid` with `values` on the columns `values` holds.
int compare_prefix(int rid, const std::vector<int32_t>& values) {
    auto [k0, k1] = row_key(rid);
    const int32_t key[] = {k0, k1};
    for (size_t i = 0; i < values.size(); ++i) {
        if (key[i] != values[i]) {
            return key[i] < values[i] ? -1 : 1;
        }
    }
    return 0;
}

std::vector<std::pair<int32_t, int32_t>> expected_keys(const std::optional<Bound>& lower,
                                                       const std::optional<Bound>& upper,
                                                       int first_row, int end_row) {
    std::vector<std::pair<int32_t, int32_t>> keys;
    for (int rid = first_row; rid < end_row; ++rid) {
        if (lower.has_value()) {
            int cmp = compare_prefix(rid, lower->values);
            if (cmp < 0 || (cmp == 0 && !lower->include)) {
                continue;
            }
        }
        if (upper.has_value()) {
            int cmp = compare_prefix(rid, upper->values);
            if (cmp > 0 || (cmp == 0 && !upper->include)) {
                continue;
            }
        }
        keys.push_back(row_key(rid));
    }
    return keys;
}

class SegmentIteratorShortKeyTest : public testing::Test {
protected:
    void SetUp() override {
        auto st = io::global_local_filesystem()->delete_directory(kTestDir);
        ASSERT_TRUE(st.ok()) << st;
        st = io::global_local_filesystem()->create_directory(kTestDir);
        ASSERT_TRUE(st.ok()) << st;
    }

    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
    }

    void build_segment(int num_short_key_columns) {
        _tablet_schema = make_tablet_schema(num_short_key_columns);
        const auto path = std::string(kTestDir) + "/short_key_segment.dat";
        auto fs = io::global_local_filesystem();
        io::FileWriterPtr file_writer;
        auto st = fs->create_file(path, &file_writer);
        ASSERT_TRUE(st.ok()) << st;

        VerticalSegmentWriterOptions opts;
        opts.num_rows_per_block = kRowsPerBlock;
        TestVerticalSegmentWriter writer(file_writer.get(), 0, _tablet_schema, nullptr, nullptr,
                                         opts, nullptr);
        st = writer.init();
        ASSERT_TRUE(st.ok()) << st;

        RowCursor row;
        std::vector<Field> fields(_tablet_schema->num_columns(), Field(PrimitiveType::TYPE_NULL));
        st = row.init_scan_key(_tablet_schema, std::move(fields));
        ASSERT_TRUE(st.ok()) << st;
        for (int rid = 0; rid < kNumRows; ++rid) {
            auto [k0, k1] = row_key(rid);
            row.mutable_field(0) = Field::create_field<TYPE_INT>(k0);
            row.mutable_field(1) = Field::create_field<TYPE_INT>(k1);
            st = writer.append_row(row);
            ASSERT_TRUE(st.ok()) << st;
        }

        uint64_t file_size = 0;
        uint64_t index_size = 0;
        st = writer.finalize_columns(&index_size);
        ASSERT_TRUE(st.ok()) << st;
        st = writer.finalize_footer(&file_size);
        ASSERT_TRUE(st.ok()) << st;
        st = file_writer->close();
        ASSERT_TRUE(st.ok()) << st;

        st = Segment::open(fs, path, 100, 0, kRowsetId, _tablet_schema, io::FileReaderOptions {},
                           &_segment);
        ASSERT_TRUE(st.ok()) << st;
        ASSERT_EQ(kNumRows, _segment->num_rows());
    }

    void make_cursor(const Bound& bound, RowCursor* cursor) {
        OlapTuple tuple;
        for (int32_t value : bound.values) {
            tuple.add_field(Field::create_field<TYPE_INT>(value));
        }
        auto st = cursor->init(_tablet_schema, tuple);
        ASSERT_TRUE(st.ok()) << st;
    }

    // Reads (k0, k1) of the rows in [lower, upper] restricted to `row_ranges` (all rows when
    // empty), sharing `shared_index_result` when it is set.
    void read_keys(const std::optional<Bound>& lower, const std::optional<Bound>& upper,
                   const RowRanges& row_ranges,
                   const std::shared_ptr<SharedSegmentIndexResult>& shared_index_result,
                   OlapReaderStatistics* stats, std::vector<std::pair<int32_t, int32_t>>* keys) {
        RowCursor lower_cursor;
        RowCursor upper_cursor;
        if (lower.has_value()) {
            ASSERT_NO_FATAL_FAILURE(make_cursor(*lower, &lower_cursor));
        }
        if (upper.has_value()) {
            ASSERT_NO_FATAL_FAILURE(make_cursor(*upper, &upper_cursor));
        }
        StorageReadOptions read_options(*stats);
        read_options.io_ctx.reader_type = ReaderType::READER_QUERY;
        read_options.key_ranges.emplace_back(
                lower.has_value() ? &lower_cursor : nullptr, lower.has_value() && lower->include,
                upper.has_value() ? &upper_cursor : nullptr, upper.has_value() && upper->include);
        read_options.row_ranges = row_ranges;
        read_options.shared_index_result = shared_index_result;
        ASSERT_NO_FATAL_FAILURE(read_all_keys(read_options, keys));
    }

    void read_all_keys(const StorageReadOptions& read_options,
                       std::vector<std::pair<int32_t, int32_t>>* keys) {
        auto read_schema = std::make_shared<ReadSchema>(_tablet_schema->columns());
        std::unique_ptr<RowwiseIterator> iterator;
        auto st = _segment->new_iterator(read_schema, read_options, &iterator);
        ASSERT_TRUE(st.ok()) << st;
        st = iterator->init(read_options);
        ASSERT_TRUE(st.ok()) << st;

        Block block = read_schema->create_read_block();
        while (true) {
            block.clear_column_data();
            st = iterator->next_batch(&block);
            if (st.is<ErrorCode::END_OF_FILE>()) {
                return;
            }
            ASSERT_TRUE(st.ok()) << st;
            const auto& k0 = assert_cast<const ColumnInt32&>(*block.get_by_position(0).column);
            const auto& k1 = assert_cast<const ColumnInt32&>(*block.get_by_position(1).column);
            for (size_t i = 0; i < block.rows(); ++i) {
                keys->emplace_back(k0.get_element(i), k1.get_element(i));
            }
        }
    }

    void expect_range(const std::optional<Bound>& lower, const std::optional<Bound>& upper) {
        OlapReaderStatistics stats;
        std::vector<std::pair<int32_t, int32_t>> keys;
        ASSERT_NO_FATAL_FAILURE(read_keys(lower, upper, RowRanges(), nullptr, &stats, &keys));
        EXPECT_EQ(expected_keys(lower, upper, 0, kNumRows), keys);
    }

    void expect_ranges() {
        // Prefix bounds: every k0 spans many short key blocks.
        expect_range(Bound {.values = {10}, .include = true},
                     Bound {.values = {12}, .include = true});
        expect_range(Bound {.values = {10}, .include = false},
                     Bound {.values = {12}, .include = false});
        // Full keys between, at and around existing rows.
        expect_range(Bound {.values = {10, 51}, .include = true},
                     Bound {.values = {10, 151}, .include = false});
        expect_range(Bound {.values = {10, 50}, .include = false},
                     Bound {.values = {11, 0}, .include = true});
        expect_range(Bound {.values = {0, 0}, .include = true},
                     Bound {.values = {0, 0}, .include = true});
        expect_range(Bound {.values = {39, 198}, .include = true}, std::nullopt);
        expect_range(std::nullopt, Bound {.values = {0, 2}, .include = false});
        // Bounds outside the data.
        expect_range(Bound {.values = {-1}, .include = true},
                     Bound {.values = {1000}, .include = true});
        expect_range(Bound {.values = {41}, .include = true}, std::nullopt);
    }

    TabletSchemaSPtr _tablet_schema;
    std::shared_ptr<Segment> _segment;
};

// The short key holds k0 only, so a full key lands in a window of many blocks sharing one short
// key, which is narrowed by peeking single rows before it is read.
TEST_F(SegmentIteratorShortKeyTest, KeyRangesWithShortKeyPrefix) {
    ASSERT_NO_FATAL_FAILURE(build_segment(1));
    ASSERT_NO_FATAL_FAILURE(expect_ranges());
}

// The short key holds the whole key, so every window is read at once.
TEST_F(SegmentIteratorShortKeyTest, KeyRangesWithFullShortKey) {
    ASSERT_NO_FATAL_FAILURE(build_segment(2));
    ASSERT_NO_FATAL_FAILURE(expect_ranges());
}

TEST_F(SegmentIteratorShortKeyTest, BlockWindowHoldsTheTargetRow) {
    ASSERT_NO_FATAL_FAILURE(build_segment(2));
    OlapReaderStatistics stats;
    ASSERT_TRUE(_segment->load_index(&stats).ok());
    for (const auto& bound :
         {Bound {.values = {10, 51}, .include = true}, Bound {.values = {10, 50}, .include = true},
          Bound {.values = {10, 50}, .include = false},
          Bound {.values = {-1, 0}, .include = true}}) {
        RowCursor cursor;
        ASSERT_NO_FATAL_FAILURE(make_cursor(bound, &cursor));
        auto [first, second] = _segment->short_key_block_window(cursor, bound.include, kNumRows);
        int target = 0;
        while (target < kNumRows) {
            int cmp = compare_prefix(target, bound.values);
            if (cmp > 0 || (cmp == 0 && bound.include)) {
                break;
            }
            ++target;
        }
        EXPECT_LE(first, target);
        EXPECT_GE(second, target);
        EXPECT_LE(second - first, 2 * kRowsPerBlock);
    }
}

// Two readers of the two halves of one segment look up the key range once between them.
TEST_F(SegmentIteratorShortKeyTest, SplitsShareKeyRanges) {
    ASSERT_NO_FATAL_FAILURE(build_segment(2));
    const std::optional<Bound> lower = Bound {.values = {10, 51}, .include = true};
    const std::optional<Bound> upper = Bound {.values = {30, 0}, .include = true};

    OlapReaderStatistics whole_stats;
    std::vector<std::pair<int32_t, int32_t>> whole_keys;
    ASSERT_NO_FATAL_FAILURE(
            read_keys(lower, upper, RowRanges(), nullptr, &whole_stats, &whole_keys));
    EXPECT_EQ(expected_keys(lower, upper, 0, kNumRows), whole_keys);

    auto shared = std::make_shared<SharedSegmentIndexResult>();
    OlapReaderStatistics split_stats;
    std::vector<std::pair<int32_t, int32_t>> first_keys;
    std::vector<std::pair<int32_t, int32_t>> second_keys;
    ASSERT_NO_FATAL_FAILURE(read_keys(lower, upper, RowRanges::create_single(0, kNumRows / 2),
                                      shared, &split_stats, &first_keys));
    ASSERT_NO_FATAL_FAILURE(read_keys(lower, upper,
                                      RowRanges::create_single(kNumRows / 2, kNumRows), shared,
                                      &split_stats, &second_keys));
    EXPECT_EQ(expected_keys(lower, upper, 0, kNumRows / 2), first_keys);
    EXPECT_EQ(expected_keys(lower, upper, kNumRows / 2, kNumRows), second_keys);
    EXPECT_GT(whole_stats.block_init_seek_num, 0);
    EXPECT_EQ(whole_stats.block_init_seek_num, split_stats.block_init_seek_num);
}

TEST(SharedRowRangesTest, ComputesOnceAndSharesTheResult) {
    SharedRowRanges shared;
    int calls = 0;
    auto compute = [&calls](RowRanges* ranges) {
        ++calls;
        *ranges = RowRanges::create_single(3, 7);
        return Status::OK();
    };
    RowRanges first;
    RowRanges second;
    ASSERT_TRUE(shared.get_or_compute(compute, &first).ok());
    ASSERT_TRUE(shared.get_or_compute(compute, &second).ok());
    EXPECT_EQ(1, calls);
    EXPECT_EQ(4, first.count());
    EXPECT_EQ(4, second.count());

    SharedRowRanges failing;
    calls = 0;
    auto fail = [&calls](RowRanges*) {
        ++calls;
        return Status::InternalError("index read failed");
    };
    EXPECT_FALSE(failing.get_or_compute(fail, &first).ok());
    EXPECT_FALSE(failing.get_or_compute(fail, &first).ok());
    EXPECT_EQ(1, calls);
}

} // namespace
} // namespace doris::segment_v2
