
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

#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/olap_common.pb.h>
#include <gen_cpp/olap_file.pb.h>
#include <glog/logging.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <stdint.h>
#include <unistd.h>

#include <memory>
#include <ostream>
#include <string>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "gtest/gtest_pred_impl.h"
#include "gutil/stringprintf.h"
#include "olap/data_dir.h"
#include "olap/olap_common.h"
#include "olap/options.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/single_replica_compaction.h"
#include "olap/storage_engine.h"
#include "olap/tablet.h"
#include "olap/tablet_meta.h"
#include "olap/utils.h"
namespace doris {
namespace vectorized {

static StorageEngine* engine_ref = nullptr;

class SingleCompactionTest : public ::testing::Test {
protected:
    void SetUp() override {
        const std::string dir_path = "ut_dir/path_gc_test";
        _engine = new StorageEngine({});
        _data_dir = new DataDir(*_engine, dir_path);
        engine_ref = _engine;
    }

    TabletSharedPtr create_tablet(int64_t tablet_id) {
        auto tablet_meta = std::make_shared<TabletMeta>();
        tablet_meta->_tablet_id = tablet_id;
        (void)tablet_meta->set_partition_id(10000);
        tablet_meta->set_tablet_uid({tablet_id, 0});
        tablet_meta->set_shard_id(tablet_id % 4);
        tablet_meta->_schema_hash = tablet_id;
        return std::make_shared<Tablet>(*_engine, std::move(tablet_meta), data_dir);
    }
    RowsetSharedPtr create_rowset(TabletSharedPtr tablet, int64_t start, int64 end) {
        auto rowset_meta = std::make_shared<RowsetMeta>();
        Version version(start, end);
        rowset_meta->set_version(version);
        rowset_meta->set_tablet_id(tablet->tablet_id());
        rowset_meta->set_tablet_uid(tablet->tablet_uid());
        rowset_meta->set_rowset_id(_engine->next_rowset_id());
        return std::make_shared<BetaRowset>(tablet->tablet_schema(), tablet->tablet_path(),
                                            std::move(rowset_meta));
    }
    void TearDown() override {
        delete _engine;
        delete _data_dir;
    }

private:
    StorageEngine* _engine;
    DataDir* _data_dir;
};

TEST_F(SingleCompactionTest, test_input_rowset) {
    TabletSharedPtr tablet = create_tablet(10000);

    SingleReplicaCompaction single_compaction(engine_ref, tablet,
                                              CompactionType::CUMULATIVE_COMPACTION);

    // load 30 rowsets
    for (int i = 1; i <= 30; ++i) {
        auto rs = create_rowset(tablet, i, i);
        auto st = tablet->add_inc_rowset(rs);
        ASSERT_TRUE(st.ok()) << st;
    }

    // prepare pick input rowsets, but now no picked
    single_compaction.prepare_compact();

    // load 2 rowsets
    for (int i = 31; i <= 32; i++) {
        auto rs = create_rowset(tablet, i, i);
        auto st = tablet->add_inc_rowset(rs);
        ASSERT_TRUE(st.ok()) << st;
    }

    // create peer compacted rowset
    auto v1 = Version(1, 32);
    auto v2 = Version(33, 38);
    std::vetcor<Version> peer_version {v1, v2};
    Version proper_version;
    bool find = single_compaction._find_rowset_to_fetch(peer_version, &proper_version);
    EXPECT_EQ(find, true);
    EXPECT_EQ(single_compaction._input_rowsets_size, 32);
    EXPECT_EQ(single_compaction._input_rowsets.front()->start_version(),
              single_compaction._output_rowset->start_version())
    EXPECT_EQ(single_compaction._input_rowsets.back()->end_version(),
              single_compaction._output_rowset->end_version())
}

} // namespace vectorized
} // namespace doris
