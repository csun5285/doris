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

#include "olap/cumulative_compaction_policy.h"

#include <gtest/gtest.h>

#include <sstream>

#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/tablet.h"
#include "olap/tablet_meta.h"

namespace doris {

TEST(TestSizeBasedCumulativeCompactionPolicy, level_size) {
    constexpr int MB = 1024 * 1024;
    SizeBasedCumulativeCompactionPolicy policy(1024 * MB);
    EXPECT_EQ(512 * MB, policy._level_size(1200 * MB));
    EXPECT_EQ(0, policy._level_size(1000));
    EXPECT_EQ(2 * MB, policy._level_size(2 * MB + 100));
    EXPECT_EQ(1 * MB, policy._level_size(2 * MB - 100));
}

static RowsetSharedPtr create_rowset(Version version, int num_segments, bool overlapping,
                                     int data_size) {
    auto rs_meta = std::make_shared<RowsetMeta>();
    rs_meta->set_rowset_type(BETA_ROWSET); // important
    rs_meta->_rowset_meta_pb.set_start_version(version.first);
    rs_meta->_rowset_meta_pb.set_end_version(version.second);
    rs_meta->set_num_segments(num_segments);
    rs_meta->set_segments_overlap(overlapping ? OVERLAPPING : NONOVERLAPPING);
    rs_meta->set_total_disk_size(data_size);
    RowsetSharedPtr rowset;
    RowsetFactory::create_rowset(nullptr, "", std::move(rs_meta), &rowset);
    return rowset;
}

TEST(TestSizeBasedCumulativeCompactionPolicy, pick_input_rowsets) {
    constexpr int MB = 1024 * 1024;
    SizeBasedCumulativeCompactionPolicy policy(1024 * MB, 0.05, 64 * MB, 64 * MB);
    constexpr int min_compaction_score = 2;
    constexpr int max_compaction_score = 100;
    Version last_delete_version = {-1, -1};
    size_t compaction_score = 0;
    {
        // [2-2] score=1 size=1, [3-3] score=1 size=1, [4-4] score=1 size=1
        Tablet tablet(std::make_shared<TabletMeta>(), nullptr);
        std::vector<RowsetSharedPtr> rowsets;
        rowsets.push_back(create_rowset({2, 2}, 1, false, MB));
        rowsets.push_back(create_rowset({3, 3}, 1, false, MB));
        rowsets.push_back(create_rowset({4, 4}, 1, false, MB));
        tablet.cloud_add_rowsets(rowsets, false);
        std::vector<RowsetSharedPtr> input_rowsets;
        policy.pick_input_rowsets(&tablet, rowsets, max_compaction_score, min_compaction_score,
                                  &input_rowsets, &last_delete_version, &compaction_score);
        ASSERT_EQ(input_rowsets.size(), 3);
        EXPECT_EQ(input_rowsets[0]->end_version(), 2);
        EXPECT_EQ(input_rowsets[1]->end_version(), 3);
        EXPECT_EQ(input_rowsets[2]->end_version(), 4);
        EXPECT_EQ(compaction_score, 3);
    }
    {
        // [2-2] score=1 size=4, [3-3] score=1 size=1, [4-4] score=1 size=1
        Tablet tablet(std::make_shared<TabletMeta>(), nullptr);
        std::vector<RowsetSharedPtr> rowsets;
        rowsets.push_back(create_rowset({2, 2}, 1, false, 4 * MB));
        rowsets.push_back(create_rowset({3, 3}, 1, false, MB));
        rowsets.push_back(create_rowset({4, 4}, 1, false, MB));
        tablet.cloud_add_rowsets(rowsets, false);
        std::vector<RowsetSharedPtr> input_rowsets;
        policy.pick_input_rowsets(&tablet, rowsets, max_compaction_score, min_compaction_score,
                                  &input_rowsets, &last_delete_version, &compaction_score);
        ASSERT_EQ(input_rowsets.size(), 2);
        EXPECT_EQ(input_rowsets[0]->end_version(), 3);
        EXPECT_EQ(input_rowsets[1]->end_version(), 4);
        EXPECT_EQ(compaction_score, 2);
    }
    {
        // [2-2] score=1 size=4, [3-3] score=1 size=2, [4-4] score=1 size=1
        Tablet tablet(std::make_shared<TabletMeta>(), nullptr);
        std::vector<RowsetSharedPtr> rowsets;
        rowsets.push_back(create_rowset({2, 2}, 1, false, 4 * MB));
        rowsets.push_back(create_rowset({3, 3}, 1, false, 2 * MB));
        rowsets.push_back(create_rowset({4, 4}, 1, false, MB));
        tablet.cloud_add_rowsets(rowsets, false);
        std::vector<RowsetSharedPtr> input_rowsets;
        policy.pick_input_rowsets(&tablet, rowsets, max_compaction_score, min_compaction_score,
                                  &input_rowsets, &last_delete_version, &compaction_score);
        ASSERT_EQ(input_rowsets.size(), 0);
        EXPECT_EQ(compaction_score, 0);
    }
    {
        // [2-2] score=90 size=100, [3-3] score=1 size=1, [4-4] score=1 size=1
        Tablet tablet(std::make_shared<TabletMeta>(), nullptr);
        std::vector<RowsetSharedPtr> rowsets;
        rowsets.push_back(create_rowset({2, 2}, 90, true, 100 * MB));
        rowsets.push_back(create_rowset({3, 3}, 1, false, MB));
        rowsets.push_back(create_rowset({4, 4}, 1, false, MB));
        tablet.cloud_add_rowsets(rowsets, false);
        std::vector<RowsetSharedPtr> input_rowsets;
        policy.pick_input_rowsets(&tablet, rowsets, max_compaction_score, min_compaction_score,
                                  &input_rowsets, &last_delete_version, &compaction_score);
        ASSERT_EQ(input_rowsets.size(), 2);
        EXPECT_EQ(input_rowsets[0]->end_version(), 3);
        EXPECT_EQ(input_rowsets[1]->end_version(), 4);
        EXPECT_EQ(compaction_score, 2);
    }
    {
        // [2-2] score=3 size=100
        Tablet tablet(std::make_shared<TabletMeta>(), nullptr);
        std::vector<RowsetSharedPtr> rowsets;
        rowsets.push_back(create_rowset({2, 2}, 3, true, 100 * MB));
        tablet.cloud_add_rowsets(rowsets, false);
        std::vector<RowsetSharedPtr> input_rowsets;
        policy.pick_input_rowsets(&tablet, rowsets, max_compaction_score, min_compaction_score,
                                  &input_rowsets, &last_delete_version, &compaction_score);
        ASSERT_EQ(input_rowsets.size(), 1);
        EXPECT_EQ(input_rowsets[0]->end_version(), 2);
        EXPECT_EQ(compaction_score, 3);
    }
    {
        // [2-2] score=100 size=100, [3-3] score=1 size=1, [4-4] score=1 size=1
        Tablet tablet(std::make_shared<TabletMeta>(), nullptr);
        std::vector<RowsetSharedPtr> rowsets;
        rowsets.push_back(create_rowset({2, 2}, 100, true, 100 * MB));
        rowsets.push_back(create_rowset({3, 3}, 1, false, MB));
        rowsets.push_back(create_rowset({4, 4}, 1, false, MB));
        tablet.cloud_add_rowsets(rowsets, false);
        std::vector<RowsetSharedPtr> input_rowsets;
        policy.pick_input_rowsets(&tablet, rowsets, max_compaction_score, min_compaction_score,
                                  &input_rowsets, &last_delete_version, &compaction_score);
        ASSERT_EQ(input_rowsets.size(), 1);
        EXPECT_EQ(input_rowsets[0]->end_version(), 2);
        EXPECT_EQ(compaction_score, 100);
    }
    {
        // [2-2] score=50 size=100, [3-3] score=60 size=120, [4-4] score=1 size=1
        Tablet tablet(std::make_shared<TabletMeta>(), nullptr);
        std::vector<RowsetSharedPtr> rowsets;
        rowsets.push_back(create_rowset({2, 2}, 50, true, 100 * MB));
        rowsets.push_back(create_rowset({3, 3}, 60, true, 120 * MB));
        rowsets.push_back(create_rowset({4, 4}, 1, false, MB));
        tablet.cloud_add_rowsets(rowsets, false);
        std::vector<RowsetSharedPtr> input_rowsets;
        policy.pick_input_rowsets(&tablet, rowsets, max_compaction_score, min_compaction_score,
                                  &input_rowsets, &last_delete_version, &compaction_score);
        ASSERT_EQ(input_rowsets.size(), 2);
        EXPECT_EQ(input_rowsets[0]->end_version(), 2);
        EXPECT_EQ(input_rowsets[1]->end_version(), 3);
        EXPECT_EQ(compaction_score, 110);
    }
    {
        // [2-2] score=1 size=200, [3-3] score=100 size=100, [4-4] score=1 size=1
        Tablet tablet(std::make_shared<TabletMeta>(), nullptr);
        std::vector<RowsetSharedPtr> rowsets;
        rowsets.push_back(create_rowset({2, 2}, 10, false, 200 * MB));
        rowsets.push_back(create_rowset({3, 3}, 100, true, 100 * MB));
        rowsets.push_back(create_rowset({4, 4}, 1, false, MB));
        tablet.cloud_add_rowsets(rowsets, false);
        std::vector<RowsetSharedPtr> input_rowsets;
        policy.pick_input_rowsets(&tablet, rowsets, max_compaction_score, min_compaction_score,
                                  &input_rowsets, &last_delete_version, &compaction_score);
        ASSERT_EQ(input_rowsets.size(), 1);
        EXPECT_EQ(input_rowsets[0]->end_version(), 3);
        EXPECT_EQ(compaction_score, 100);
    }
}

} // namespace doris

// @brief Test Stub
