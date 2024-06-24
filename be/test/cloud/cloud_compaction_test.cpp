#include <gtest/gtest.h>

#include <memory>

#include "cloud/cloud_base_compaction.h"
#include "cloud/cloud_cumulative_compaction.h"
#include "cloud/cloud_full_compaction.h"
#include "cloud/cloud_meta_mgr.h"
#include "cloud/cloud_tablet_mgr.h"
#include "cloud/olap/storage_engine.h"
#include "cloud/utils.h"
#include "common/config.h"
#include "common/status.h"
#include "common/sync_point.h"
#include "gen_cpp/selectdb_cloud.pb.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/unique_rowset_id_generator.h"
#include "olap/tablet.h"

using namespace doris;
using namespace doris::ErrorCode;

static UniqueRowsetIdGenerator id_generator({0, 1});

static RowsetMetaPB create_rowset_pb(Version version) {
    RowsetMetaPB rowset;
    rowset.set_rowset_id(0); // Required
    rowset.set_rowset_id_v2(id_generator.next_id().to_string());
    rowset.set_start_version(version.first);
    rowset.set_end_version(version.second);
    rowset.set_num_segments(1);
    rowset.set_num_rows(100);
    rowset.set_total_disk_size(10000);
    rowset.set_rowset_type(BETA_ROWSET);
    return rowset;
}

static RowsetMetaSharedPtr create_rowset_meta(Version version) {
    auto rowset_pb = create_rowset_pb(version);
    auto rowset_meta = std::make_shared<RowsetMeta>();
    rowset_meta->init_from_pb(rowset_pb);
    return rowset_meta;
}

static RowsetSharedPtr create_rowset(Version version) {
    auto rs_meta = create_rowset_meta(version);
    RowsetSharedPtr rowset;
    RowsetFactory::create_rowset(nullptr, "", std::move(rs_meta), &rowset);
    return rowset;
}

// Copy from cloud/src/meta_service.cpp
static Versions calc_sync_versions(int64_t req_bc_cnt, int64_t bc_cnt, int64_t req_cc_cnt,
                                   int64_t cc_cnt, int64_t req_cp, int64_t cp, int64_t req_start,
                                   int64_t req_end) {
    if (req_end == -1) req_end = INT64_MAX;
    // combine `v1` `v2`  to `v1`, return true if success
    static auto combine_if_overlapping = [](Version& v1, Version& v2) -> bool {
        if (v1.second + 1 < v2.first || v2.second + 1 < v1.first) return false;
        v1.first = std::min(v1.first, v2.first);
        v1.second = std::max(v1.second, v2.second);
        return true;
    };
    // [xxx]: compacted versions
    // ^~~~~: cumulative point
    // ^___^: related versions
    Versions versions;
    if (req_bc_cnt < bc_cnt) {
        // * for any BC happended
        // BE  [=][=][=][=][=====][=][=]
        //                  ^~~~~ req_cp
        // MS  [xxxxxxxxxx][xxxxxxxxxxxxxx][=======][=][=]
        //                                  ^~~~~~~ ms_cp
        //     ^_________________________^ versions_return: [0, ms_cp - 1]
        versions.emplace_back(0, cp - 1);
    }

    if (req_cc_cnt < cc_cnt) {
        Version cc_version;
        if (req_cp < cp && req_cc_cnt + 1 == cc_cnt) {
            // * only one CC happened and CP changed
            // BE  [=][=][=][=][=====][=][=]
            //                  ^~~~~ req_cp
            // MS  [=][=][=][=][xxxxxxxxxxxxxx][=======][=][=]
            //                                  ^~~~~~~ ms_cp
            //                  ^____________^ related_versions: [req_cp, ms_cp - 1]
            //
            cc_version = {req_cp, cp - 1};
        } else {
            // * more than one CC happened and CP changed
            // BE  [=][=][=][=][=====][=][=]
            //                  ^~~~~ req_cp
            // MS  [=][=][=][=][xxxxxxxxxxxxxx][xxxxxxx][=][=]
            //                                  ^~~~~~~ ms_cp
            //                  ^_____________________^ related_versions: [req_cp, max]
            //
            // * more than one CC happened and CP remain unchanged
            // BE  [=][=][=][=][=====][=][=]
            //                  ^~~~~ req_cp
            // MS  [=][=][=][=][xxxxxxxxxxxxxx][xxxxxxx][=][=]
            //                  ^~~~~~~~~~~~~~ ms_cp
            //                  ^_____________________^ related_versions: [req_cp, max]
            //                                           there may be holes if we don't return all version
            //                                           after ms_cp, however it can be optimized.
            cc_version = {req_cp, std::numeric_limits<int64_t>::max() - 1};
        }
        if (versions.empty() || !combine_if_overlapping(versions.front(), cc_version)) {
            versions.push_back(cc_version);
        }
    }

    Version query_version {req_start, req_end};
    bool combined = false;
    for (auto& v : versions) {
        if ((combined = combine_if_overlapping(v, query_version))) break;
    }
    if (!combined) {
        versions.push_back(query_version);
    }
    std::sort(versions.begin(), versions.end(),
              [](const Version& v1, const Version& v2) { return v1.first < v2.first; });
    return versions;
}

// Mock meta-service for testing `sync_tablet_rowsets` after compaction
class MockMetaService : public selectdb::MetaService_Stub {
public:
    MockMetaService() : selectdb::MetaService_Stub(nullptr) {}

    void get_tablet(google::protobuf::RpcController* controller,
                    const selectdb::GetTabletRequest* request,
                    selectdb::GetTabletResponse* response,
                    google::protobuf::Closure* done) override {
        response->mutable_status()->set_code(selectdb::OK);
        auto tablet = response->mutable_tablet_meta();
        tablet->set_tablet_id(request->tablet_id());
        tablet->set_tablet_state(TabletStatePB::PB_RUNNING);
        tablet->set_index_id(10002);
        tablet->set_schema_version(0);
    }

    void get_rowset(google::protobuf::RpcController* controller,
                    const selectdb::GetRowsetRequest* request,
                    selectdb::GetRowsetResponse* response,
                    google::protobuf::Closure* done) override {
        response->mutable_status()->set_code(selectdb::OK);
        auto& tablet_rowsets = tablet_rowsets_map[request->idx().tablet_id()];
        response->mutable_stats()->CopyFrom(tablet_rowsets.stats);
        auto versions = calc_sync_versions(
                request->base_compaction_cnt(), tablet_rowsets.stats.base_compaction_cnt(),
                request->cumulative_compaction_cnt(),
                tablet_rowsets.stats.cumulative_compaction_cnt(), request->cumulative_point(),
                tablet_rowsets.stats.cumulative_point(), request->start_version(),
                request->end_version());
        for (auto [start_v, end_v] : versions) {
            auto start_it = tablet_rowsets.rowsets.lower_bound(start_v);
            auto end_it = tablet_rowsets.rowsets.upper_bound(end_v);
            for (auto it = start_it; it != end_it; ++it) {
                response->add_rowset_meta()->CopyFrom(it->second);
            }
        }
    }

public:
    struct TabletRowsets {
        std::map<int64_t, RowsetMetaPB> rowsets;
        selectdb::TabletStatsPB stats;

        void add_rowset(const RowsetMetaPB& rowset) {
            auto start_it = rowsets.lower_bound(rowset.start_version());
            auto end_it = rowsets.upper_bound(rowset.end_version());
            int64_t data_size = 0;
            int64_t num_segs = 0;
            int64_t num_rows = 0;
            int64_t num_rowsets = 0;
            for (auto it = start_it; it != end_it; ++it) {
                data_size += it->second.total_disk_size();
                num_segs += it->second.num_segments();
                num_rows += it->second.num_rows();
                ++num_rowsets;
            }
            rowsets.erase(start_it, end_it);
            rowsets.emplace(rowset.end_version(), rowset);
            stats.set_num_rowsets(stats.num_rowsets() - num_rowsets + 1);
            stats.set_num_rows(stats.num_rows() - num_rows + rowset.num_rows());
            stats.set_num_segments(stats.num_segments() - num_segs + rowset.num_segments());
            stats.set_data_size(stats.data_size() - data_size + rowset.total_disk_size());
        }
    };

    std::unordered_map<int64_t, TabletRowsets> tablet_rowsets_map;
};

TEST(CloudCompactionTest, schedule) {
    StorageEngine storage_engine({});

    // Launch cumu compaction pool to submit compaction
    ThreadPoolBuilder("CumuCompactionTaskThreadPool")
            .set_min_threads(2)
            .set_max_threads(2)
            .build(&storage_engine._cumu_compaction_thread_pool);

    auto sp = SyncPoint::get_instance();
    Defer defer {[sp] {
        sp->clear_call_back("MetaServiceProxy::get_client");
        sp->clear_call_back("CloudMetaMgr::prepare_tablet_job");
        sp->clear_call_back("CloudCumulativeCompaction::execute_compact_impl");
        sp->load_dependency({}); // Clear dependency
    }};
    sp->enable_processing();

    // Create MockMetaService
    auto mock_service = std::make_shared<MockMetaService>();
    sp->set_call_back("MetaServiceProxy::get_client", [&mock_service](auto&& args) {
        auto stub = try_any_cast<std::shared_ptr<selectdb::MetaService_Stub>*>(args[0]);
        *stub = mock_service;
        auto pair = try_any_cast<std::pair<Status, bool>*>(args.back());
        pair->second = true;
    });

    // Prepare tablet
    constexpr int tablet_id = 10011;
    {
        auto& tablet_rowsets = mock_service->tablet_rowsets_map[tablet_id];
        tablet_rowsets.add_rowset(create_rowset_pb({0, 1}));
        tablet_rowsets.add_rowset(create_rowset_pb({2, 2}));
        tablet_rowsets.add_rowset(create_rowset_pb({3, 3}));
        auto& stats = tablet_rowsets.stats;
        stats.set_base_compaction_cnt(0);
        stats.set_cumulative_compaction_cnt(0);
        stats.set_cumulative_point(2);
    }

    sp->set_call_back("CloudMetaMgr::prepare_tablet_job", [](auto&& args) {
        auto pair = try_any_cast<std::pair<Status, bool>*>(args.back());
        pair->second = true;
    });
    sp->set_call_back("CloudCumulativeCompaction::execute_compact_impl", [](auto&& args) {
        ::sleep(1);
        auto pair = try_any_cast<std::pair<Status, bool>*>(args.back());
        pair->second = true;
    });

    config::cumulative_compaction_min_deltas = 2;
    config::enable_parallel_cumu_compaction = true;
    TabletSharedPtr tablet;
    auto st = cloud::tablet_mgr()->get_tablet(tablet_id, &tablet);
    ASSERT_TRUE(st.ok()) << st;
    // To meet the condition of not skipping in `get_topn_tablets_to_compact`
    tablet->last_load_time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                        std::chrono::system_clock::now().time_since_epoch())
                                        .count();
    // Submit cumu compaction on `tablet`
    tablet->set_tablet_state(TabletState::TABLET_NOTREADY);
    st = storage_engine.submit_compaction_task(tablet, CompactionType::CUMULATIVE_COMPACTION);
    ASSERT_TRUE(st.is<ErrorCode::INTERNAL_ERROR>()); // Can only do compaction on RUNNING tablet
    tablet->set_tablet_state(TabletState::TABLET_RUNNING);
    st = storage_engine.submit_compaction_task(tablet, CompactionType::CUMULATIVE_COMPACTION);
    ASSERT_EQ(st, Status::OK());
    ASSERT_TRUE(storage_engine.has_cumu_compaction(tablet->tablet_id()));
    // `tablet` should in candidate tablets for cumu compaction
    auto candidate_tablets = storage_engine._generate_cloud_compaction_tasks(
            CompactionType::CUMULATIVE_COMPACTION, false);
    ASSERT_EQ(candidate_tablets.size(), 1);
    ASSERT_EQ(candidate_tablets[0], tablet);
    ASSERT_TRUE(storage_engine.has_cumu_compaction(tablet->tablet_id()));
    // Submit cumu compaction on `tablet` again, but no suitable version
    st = storage_engine.submit_compaction_task(tablet, CompactionType::CUMULATIVE_COMPACTION);
    ASSERT_TRUE(st.is<CUMULATIVE_NO_SUITABLE_VERSION>());
    // `tablet` should not in candidate tablets for cumu compaction
    candidate_tablets = storage_engine._generate_cloud_compaction_tasks(
            CompactionType::CUMULATIVE_COMPACTION, false);
    ASSERT_EQ(candidate_tablets.size(), 0);
    ::sleep(2); // Wait for `tablet` to complete cumu compaction
    ASSERT_FALSE(storage_engine.has_cumu_compaction(tablet->tablet_id()));
    // No cumu compaction on `tablet`, `tablet` should be candidate tablet again
    candidate_tablets = storage_engine._generate_cloud_compaction_tasks(
            CompactionType::CUMULATIVE_COMPACTION, false);
    ASSERT_EQ(candidate_tablets.size(), 1);
    ASSERT_EQ(candidate_tablets[0], tablet);
    std::thread t1([&] {
        auto st = storage_engine.submit_compaction_task(tablet,
                                                        CompactionType::CUMULATIVE_COMPACTION);
        ASSERT_EQ(st, Status::OK());
        ASSERT_TRUE(storage_engine.has_cumu_compaction(tablet->tablet_id()));
    });
    sp->load_dependency({{"CloudCompactionTest::schedule1", "CloudMetaMgr::prepare_tablet_job"}});
    ::sleep(1); // Wait for preparing cumu compaction on `tablet`
    // `tablet` should not in candidate tablets for cumu compaction when there is preparing cumu compaction on `tablet`
    candidate_tablets = storage_engine._generate_cloud_compaction_tasks(
            CompactionType::CUMULATIVE_COMPACTION, false);
    ASSERT_EQ(candidate_tablets.size(), 0);
    // Should not submit cumu compaction on `tablet` when there is preparing cumu compaction on `tablet`
    st = storage_engine.submit_compaction_task(tablet, CompactionType::CUMULATIVE_COMPACTION);
    ASSERT_TRUE(st.is<ALREADY_EXIST>());
    TEST_SYNC_POINT("CloudCompactionTest::schedule1");
    t1.join();
    sp->load_dependency({}); // Clear dependency

    config::enable_parallel_cumu_compaction = false;
    ::sleep(2); // Wait for `tablet` to complete cumu compaction
    // Submit cumu compaction on `tablet`
    st = storage_engine.submit_compaction_task(tablet, CompactionType::CUMULATIVE_COMPACTION);
    ASSERT_EQ(st, Status::OK());
    ASSERT_TRUE(storage_engine.has_cumu_compaction(tablet->tablet_id()));
    // `tablet` should not in candidate tablets for cumu compaction when !enable_parallel_cumu_compaction
    candidate_tablets = storage_engine._generate_cloud_compaction_tasks(
            CompactionType::CUMULATIVE_COMPACTION, false);
    ASSERT_EQ(candidate_tablets.size(), 0);
    // Should not submit cumu compaction on `tablet` when !enable_parallel_cumu_compaction
    st = storage_engine.submit_compaction_task(tablet, CompactionType::CUMULATIVE_COMPACTION);
    ASSERT_TRUE(st.is<ALREADY_EXIST>());
    ASSERT_TRUE(storage_engine.has_cumu_compaction(tablet->tablet_id()));
    ::sleep(2); // Wait for `tablet` to complete cumu compaction
    ASSERT_FALSE(storage_engine.has_cumu_compaction(tablet->tablet_id()));
    // No cumu compaction on `tablet`, `tablet` should be candidate tablet again
    candidate_tablets = storage_engine._generate_cloud_compaction_tasks(
            CompactionType::CUMULATIVE_COMPACTION, false);
    ASSERT_EQ(candidate_tablets.size(), 1);
    ASSERT_EQ(candidate_tablets[0], tablet);
    std::thread t2([&] {
        auto st = storage_engine.submit_compaction_task(tablet,
                                                        CompactionType::CUMULATIVE_COMPACTION);
        ASSERT_EQ(st, Status::OK());
        ASSERT_TRUE(storage_engine.has_cumu_compaction(tablet->tablet_id()));
    });
    sp->load_dependency({{"CloudCompactionTest::schedule2", "CloudMetaMgr::prepare_tablet_job"}});
    ::sleep(1); // Wait for preparing cumu compaction on `tablet`
    // `tablet` should not in candidate tablets for cumu compaction when there is preparing cumu compaction on `tablet`
    candidate_tablets = storage_engine._generate_cloud_compaction_tasks(
            CompactionType::CUMULATIVE_COMPACTION, false);
    ASSERT_EQ(candidate_tablets.size(), 0);
    // Should not submit cumu compaction on `tablet` when there is preparing cumu compaction on `tablet`
    st = storage_engine.submit_compaction_task(tablet, CompactionType::CUMULATIVE_COMPACTION);
    ASSERT_TRUE(st.is<ALREADY_EXIST>());
    TEST_SYNC_POINT("CloudCompactionTest::schedule2");
    t2.join();
}

TEST(CloudCompactionTest, parallel_base_cumu_compaction) {
    StorageEngine storage_engine({});

    auto sp = SyncPoint::get_instance();
    Defer defer {[sp] {
        sp->clear_call_back("MetaServiceProxy::get_client");
        sp->clear_call_back("CloudMetaMgr::prepare_tablet_job");
        sp->clear_call_back("CloudMetaMgr::commit_tablet_job");
    }};
    sp->enable_processing();

    // Create MockMetaService
    auto mock_service = std::make_shared<MockMetaService>();
    sp->set_call_back("MetaServiceProxy::get_client", [&mock_service](auto&& args) {
        auto stub = try_any_cast<std::shared_ptr<selectdb::MetaService_Stub>*>(args[0]);
        *stub = mock_service;
        auto pair = try_any_cast<std::pair<Status, bool>*>(args.back());
        pair->second = true;
    });

    auto prepare_tablet = [&](int64_t tablet_id) {
        auto& tablet_rowsets = mock_service->tablet_rowsets_map[tablet_id];
        tablet_rowsets.add_rowset(create_rowset_pb({0, 1}));
        tablet_rowsets.add_rowset(create_rowset_pb({2, 10}));
        tablet_rowsets.add_rowset(create_rowset_pb({11, 20}));
        tablet_rowsets.add_rowset(create_rowset_pb({21, 30}));
        tablet_rowsets.add_rowset(create_rowset_pb({31, 32}));
        tablet_rowsets.add_rowset(create_rowset_pb({33, 33}));
        tablet_rowsets.add_rowset(create_rowset_pb({34, 34}));
        auto& stats = tablet_rowsets.stats;
        stats.set_base_compaction_cnt(1);
        stats.set_cumulative_compaction_cnt(10);
        stats.set_cumulative_point(31);
    };

    sp->set_call_back("CloudMetaMgr::prepare_tablet_job", [](auto&& args) {
        auto pair = try_any_cast<std::pair<Status, bool>*>(args.back());
        pair->second = true;
    });

    config::cumulative_compaction_min_deltas = 2;
    config::base_compaction_num_cumulative_deltas = 2;

    selectdb::TabletStatsPB commit_job_stats;
    sp->set_call_back("CloudMetaMgr::commit_tablet_job", [&](auto&& args) {
        selectdb::FinishTabletJobResponse* res =
                try_any_cast<selectdb::FinishTabletJobResponse*>(args[1]);
        *res->mutable_stats() = commit_job_stats;
        auto pair = try_any_cast<std::pair<Status, bool>*>(args.back());
        pair->second = true;
    });

    {
        // Simulate executing commit job and receiving response out of sequence:
        //  commit cumu -> commit base -> base update cache -> cumu update cache
        prepare_tablet(10011);
        TabletSharedPtr tablet;
        cloud::tablet_mgr()->get_tablet(10011, &tablet);
        CloudCumulativeCompaction cumu(tablet);
        auto st = cumu.prepare_compact();
        ASSERT_EQ(st, Status::OK());
        cumu._output_rowset = create_rowset({31, 34});
        CloudBaseCompaction base(tablet);
        st = base.prepare_compact();
        ASSERT_EQ(st, Status::OK());
        base._output_rowset = create_rowset({2, 30});
        // - Thread1: commit cumu
        auto& tablet_rowsets = mock_service->tablet_rowsets_map[10011];
        tablet_rowsets.add_rowset(create_rowset_pb({31, 34}));
        auto& stats = tablet_rowsets.stats;
        stats.set_cumulative_compaction_cnt(11);
        auto cumu_stats = stats;
        // - Thread2: commit base
        tablet_rowsets.add_rowset(create_rowset_pb({2, 30}));
        stats.set_base_compaction_cnt(2);
        auto base_stats = stats;
        // - Thread2: base update tablet cache
        commit_job_stats = base_stats;
        base.modify_rowsets(nullptr);
        // - Thread1: cumu update tablet cache
        commit_job_stats = cumu_stats;
        cumu.modify_rowsets(nullptr);
        Versions versions;
        tablet->capture_consistent_versions({0, 34}, &versions, false);
        ASSERT_EQ(versions.size(), 3);
        EXPECT_EQ(versions[1], Version(2, 30));
        EXPECT_EQ(versions[2], Version(31, 34));
        EXPECT_EQ(tablet->base_compaction_cnt(), 2);
        EXPECT_EQ(tablet->cumulative_compaction_cnt(), 11);
        EXPECT_EQ(tablet->cumulative_layer_point(), 31);
        EXPECT_EQ(tablet->fetch_add_approximate_num_rowsets(0), 3);
    }
    {
        // Simulate executing commit job and receiving response out of sequence:
        //  commit base -> commit cumu -> cumu update cache -> base update cache
        prepare_tablet(10012);
        TabletSharedPtr tablet;
        cloud::tablet_mgr()->get_tablet(10012, &tablet);
        CloudCumulativeCompaction cumu(tablet);
        auto st = cumu.prepare_compact();
        ASSERT_EQ(st, Status::OK());
        cumu._output_rowset = create_rowset({31, 34});
        CloudBaseCompaction base(tablet);
        st = base.prepare_compact();
        ASSERT_EQ(st, Status::OK());
        base._output_rowset = create_rowset({2, 30});
        // - Thread1: commit base
        auto& tablet_rowsets = mock_service->tablet_rowsets_map[10012];
        tablet_rowsets.add_rowset(create_rowset_pb({2, 30}));
        auto& stats = tablet_rowsets.stats;
        stats.set_base_compaction_cnt(2);
        auto base_stats = stats;
        // - Thread2: commit cumu
        tablet_rowsets.add_rowset(create_rowset_pb({31, 34}));
        stats.set_cumulative_compaction_cnt(11);
        auto cumu_stats = stats;
        // - Thread2: cumu update tablet cache
        commit_job_stats = cumu_stats;
        cumu.modify_rowsets(nullptr);
        // - Thread1: base update tablet cache
        commit_job_stats = base_stats;
        base.modify_rowsets(nullptr);
        Versions versions;
        tablet->capture_consistent_versions({0, 34}, &versions, false);
        ASSERT_EQ(versions.size(), 3);
        EXPECT_EQ(versions[1], Version(2, 30));
        EXPECT_EQ(versions[2], Version(31, 34));
        EXPECT_EQ(tablet->base_compaction_cnt(), 2);
        EXPECT_EQ(tablet->cumulative_compaction_cnt(), 11);
        EXPECT_EQ(tablet->cumulative_layer_point(), 31);
        EXPECT_EQ(tablet->fetch_add_approximate_num_rowsets(0), 3);
    }
    {
        // Simulate executing commit job and update tablet cache, suppose this BE is BE1:
        //  BE1 prepare cumu -> BE2 commit base -> BE1 commit cumu -> BE1 cumu update cache
        prepare_tablet(10013);
        TabletSharedPtr tablet;
        cloud::tablet_mgr()->get_tablet(10013, &tablet);
        CloudCumulativeCompaction cumu(tablet);
        auto st = cumu.prepare_compact();
        ASSERT_EQ(st, Status::OK());
        cumu._output_rowset = create_rowset({31, 34});
        // - BE2 commit base
        auto& tablet_rowsets = mock_service->tablet_rowsets_map[10013];
        tablet_rowsets.add_rowset(create_rowset_pb({2, 30}));
        auto& stats = tablet_rowsets.stats;
        stats.set_base_compaction_cnt(2);
        // - BE1 commit cumu
        tablet_rowsets.add_rowset(create_rowset_pb({31, 34}));
        stats.set_cumulative_compaction_cnt(11);
        // - BE1 cumu update tablet cache
        commit_job_stats = stats;
        cumu.modify_rowsets(nullptr);
        tablet->cloud_sync_rowsets();
        Versions versions;
        tablet->capture_consistent_versions({0, 34}, &versions, false);
        ASSERT_EQ(versions.size(), 3);
        EXPECT_EQ(versions[1], Version(2, 30));
        EXPECT_EQ(versions[2], Version(31, 34));
        EXPECT_EQ(tablet->base_compaction_cnt(), 2);
        EXPECT_EQ(tablet->cumulative_compaction_cnt(), 11);
        EXPECT_EQ(tablet->cumulative_layer_point(), 31);
        EXPECT_EQ(tablet->fetch_add_approximate_num_rowsets(0), 3);
    }
    {
        // Simulate executing commit job and update tablet cache, suppose this BE is BE1:
        //  BE1 prepare base -> BE2 commit cumu -> BE1 commit base -> BE1 base update cache
        prepare_tablet(10014);
        TabletSharedPtr tablet;
        cloud::tablet_mgr()->get_tablet(10014, &tablet);
        CloudBaseCompaction base(tablet);
        auto st = base.prepare_compact();
        ASSERT_EQ(st, Status::OK());
        base._output_rowset = create_rowset({2, 30});
        // - BE2 commit cumu and update cumu point
        auto& tablet_rowsets = mock_service->tablet_rowsets_map[10014];
        tablet_rowsets.add_rowset(create_rowset_pb({31, 34}));
        auto& stats = tablet_rowsets.stats;
        stats.set_cumulative_compaction_cnt(11);
        stats.set_cumulative_point(35);
        // - BE1 commit base
        tablet_rowsets.add_rowset(create_rowset_pb({2, 30}));
        stats.set_base_compaction_cnt(2);
        // - BE1 cumu update tablet cache
        commit_job_stats = stats;
        base.modify_rowsets(nullptr);
        tablet->cloud_sync_rowsets();
        Versions versions;
        tablet->capture_consistent_versions({0, 34}, &versions, false);
        ASSERT_EQ(versions.size(), 3);
        EXPECT_EQ(versions[1], Version(2, 30));
        EXPECT_EQ(versions[2], Version(31, 34));
        EXPECT_EQ(tablet->base_compaction_cnt(), 2);
        EXPECT_EQ(tablet->cumulative_compaction_cnt(), 11);
        EXPECT_EQ(tablet->cumulative_layer_point(), 35);
        EXPECT_EQ(tablet->fetch_add_approximate_num_rowsets(0), 3);
    }
}

TEST(CloudCompactionTest, parallel_cumu_compaction) {
    StorageEngine storage_engine({});

    auto sp = SyncPoint::get_instance();
    Defer defer {[sp] {
        sp->clear_call_back("MetaServiceProxy::get_client");
        sp->clear_call_back("CloudMetaMgr::prepare_tablet_job");
        sp->clear_call_back("CloudMetaMgr::commit_tablet_job");
    }};
    sp->enable_processing();

    selectdb::TabletStatsPB commit_job_stats;
    sp->set_call_back("CloudMetaMgr::commit_tablet_job", [&](auto&& args) {
        auto res = try_any_cast<selectdb::FinishTabletJobResponse*>(args[1]);
        *res->mutable_stats() = commit_job_stats;
        auto pair = try_any_cast<std::pair<Status, bool>*>(args.back());
        pair->second = true;
    });

    // Create MockMetaService
    auto mock_service = std::make_shared<MockMetaService>();
    sp->set_call_back("MetaServiceProxy::get_client", [&mock_service](auto&& args) {
        auto stub = try_any_cast<std::shared_ptr<selectdb::MetaService_Stub>*>(args[0]);
        *stub = mock_service;
        auto pair = try_any_cast<std::pair<Status, bool>*>(args.back());
        pair->second = true;
    });

    config::cumulative_compaction_min_deltas = 2;
    config::enable_parallel_cumu_compaction = true;

    auto prepare_tablet = [&](int64_t tablet_id) {
        auto& tablet_rowsets = mock_service->tablet_rowsets_map[tablet_id];
        tablet_rowsets.add_rowset(create_rowset_pb({0, 1}));
        tablet_rowsets.add_rowset(create_rowset_pb({2, 5}));
        tablet_rowsets.add_rowset(create_rowset_pb({6, 10}));
        auto& stats = tablet_rowsets.stats;
        stats.set_base_compaction_cnt(0);
        stats.set_cumulative_compaction_cnt(2);
        stats.set_cumulative_point(2);
    };

    {
        prepare_tablet(10011);
        TabletSharedPtr tablet;
        cloud::tablet_mgr()->get_tablet(10011, &tablet);
        // Prepare cumu1
        CloudCumulativeCompaction cumu1(tablet);
        sp->set_call_back("CloudMetaMgr::prepare_tablet_job", [](auto&& args) {
            auto pair = try_any_cast<std::pair<Status, bool>*>(args.back());
            pair->second = true;
        });
        auto st = cumu1.prepare_compact();
        ASSERT_EQ(st, Status::OK());
        cumu1._output_rowset = create_rowset({2, 10});
        // Load [11][12]
        auto& tablet_rowsets = mock_service->tablet_rowsets_map[10011];
        tablet_rowsets.add_rowset(create_rowset_pb({11, 11}));
        tablet_rowsets.add_rowset(create_rowset_pb({12, 12}));
        tablet->cloud_sync_rowsets();
        // Prepare cumu2
        CloudCumulativeCompaction cumu2(tablet);
        sp->set_call_back("CloudMetaMgr::prepare_tablet_job", [&](auto&& args) {
            auto job = try_any_cast<const selectdb::TabletJobInfoPB&>(args[0]);
            auto pair = try_any_cast<std::pair<Status, bool>*>(args.back());
            if (job.compaction(0).input_versions(0) <= 10 &&
                job.compaction(0).input_versions(1) >= 2) {
                auto res = try_any_cast<selectdb::StartTabletJobResponse*>(args[1]);
                res->mutable_status()->set_code(selectdb::JOB_TABLET_BUSY);
                res->add_version_in_compaction(2);
                res->add_version_in_compaction(10);
                pair->first = Status::Error<ErrorCode::INTERNAL_ERROR>("");
            }
            pair->second = true;
        });
        st = cumu2.prepare_compact();
        ASSERT_EQ(st, Status::OK());
        auto& input_rowsets = cumu2.get_input_rowsets();
        ASSERT_EQ(input_rowsets.size(), 2);
        ASSERT_EQ(input_rowsets[0]->start_version(), 11);
        ASSERT_EQ(input_rowsets[1]->end_version(), 12);
        cumu2._output_rowset = create_rowset({11, 12});
        // Load [13][14]
        tablet_rowsets.add_rowset(create_rowset_pb({13, 13}));
        tablet_rowsets.add_rowset(create_rowset_pb({14, 14}));
        tablet->cloud_sync_rowsets();
        // Prepare cumu3
        CloudCumulativeCompaction cumu3(tablet);
        sp->set_call_back("CloudMetaMgr::prepare_tablet_job", [&](auto&& args) {
            auto job = try_any_cast<const selectdb::TabletJobInfoPB&>(args[0]);
            auto pair = try_any_cast<std::pair<Status, bool>*>(args.back());
            if (job.compaction(0).input_versions(0) <= 12 &&
                job.compaction(0).input_versions(1) >= 2) {
                auto res = try_any_cast<selectdb::StartTabletJobResponse*>(args[1]);
                res->mutable_status()->set_code(selectdb::JOB_TABLET_BUSY);
                res->add_version_in_compaction(2);
                res->add_version_in_compaction(10);
                res->add_version_in_compaction(11);
                res->add_version_in_compaction(12);
                pair->first = Status::Error<ErrorCode::INTERNAL_ERROR>("");
            }
            pair->second = true;
        });
        st = cumu3.prepare_compact();
        ASSERT_EQ(st, Status::OK());
        auto& cumu3_input_rowsets = cumu3.get_input_rowsets();
        ASSERT_EQ(cumu3_input_rowsets.size(), 2);
        ASSERT_EQ(cumu3_input_rowsets[0]->start_version(), 13);
        ASSERT_EQ(cumu3_input_rowsets[1]->end_version(), 14);
        cumu3._output_rowset = create_rowset({13, 14});
        // - Thread1 commit cumu1
        tablet_rowsets.add_rowset(create_rowset_pb({2, 10}));
        auto& stats = tablet_rowsets.stats;
        stats.set_cumulative_compaction_cnt(3);
        auto cumu1_stats = stats;
        // - Thread2 commit cumu2
        tablet_rowsets.add_rowset(create_rowset_pb({11, 12}));
        stats.set_cumulative_compaction_cnt(4);
        auto cumu2_stats = stats;
        // - Thread3 commit cumu3
        tablet_rowsets.add_rowset(create_rowset_pb({13, 14}));
        stats.set_cumulative_compaction_cnt(5);
        auto cumu3_stats = stats;
        // - Thread2 cumu2 update tablet cache
        commit_job_stats = cumu2_stats;
        cumu2.modify_rowsets(nullptr);
        // - Thread1 cumu1 update tablet cache
        commit_job_stats = cumu1_stats;
        cumu1.modify_rowsets(nullptr); // Don't update tablet cache
        // - Thread3 cumu3 update tablet cache
        commit_job_stats = cumu3_stats;
        cumu3.modify_rowsets(nullptr);
        Versions versions;
        tablet->capture_consistent_versions({0, 14}, &versions, false);
        ASSERT_EQ(versions.size(), 5);
        EXPECT_EQ(versions[1], Version(2, 5));
        EXPECT_EQ(versions[2], Version(6, 10));
        EXPECT_EQ(versions[3], Version(11, 12));
        EXPECT_EQ(versions[4], Version(13, 14));
        tablet->cloud_sync_rowsets();
        versions.clear();
        tablet->capture_consistent_versions({0, 14}, &versions, false);
        ASSERT_EQ(versions.size(), 4);
        EXPECT_EQ(versions[1], Version(2, 10));
        EXPECT_EQ(versions[2], Version(11, 12));
        EXPECT_EQ(versions[3], Version(13, 14));
        EXPECT_EQ(tablet->base_compaction_cnt(), 0);
        EXPECT_EQ(tablet->cumulative_compaction_cnt(), 5);
        EXPECT_EQ(tablet->cumulative_layer_point(), 2);
        EXPECT_EQ(tablet->fetch_add_approximate_num_rowsets(0), 4);
    }

    {
        prepare_tablet(10012);
        // - BE2 prepare cumu3 [2-10]
        // Load [11][12]
        auto& tablet_rowsets = mock_service->tablet_rowsets_map[10012];
        tablet_rowsets.add_rowset(create_rowset_pb({11, 11}));
        tablet_rowsets.add_rowset(create_rowset_pb({12, 12}));
        TabletSharedPtr tablet;
        cloud::tablet_mgr()->get_tablet(10012, &tablet);
        // - BE1 Prepare cumu1
        CloudCumulativeCompaction cumu1(tablet);
        sp->set_call_back("CloudMetaMgr::prepare_tablet_job", [](auto&& args) {
            auto job = try_any_cast<const selectdb::TabletJobInfoPB&>(args[0]);
            auto pair = try_any_cast<std::pair<Status, bool>*>(args.back());
            if (job.compaction(0).input_versions(0) <= 10 &&
                job.compaction(0).input_versions(1) >= 2) {
                auto res = try_any_cast<selectdb::StartTabletJobResponse*>(args[1]);
                res->mutable_status()->set_code(selectdb::JOB_TABLET_BUSY);
                res->add_version_in_compaction(2);
                res->add_version_in_compaction(10);
                pair->first = Status::Error<ErrorCode::INTERNAL_ERROR>("");
            }
            pair->second = true;
        });
        auto st = cumu1.prepare_compact();
        ASSERT_EQ(st, Status::OK());
        auto& cumu1_input_rowsets = cumu1.get_input_rowsets();
        ASSERT_EQ(cumu1_input_rowsets.size(), 2);
        ASSERT_EQ(cumu1_input_rowsets[0]->start_version(), 11);
        ASSERT_EQ(cumu1_input_rowsets[1]->end_version(), 12);
        cumu1._output_rowset = create_rowset({11, 12});
        // Load [13][14][15][16]
        tablet_rowsets.add_rowset(create_rowset_pb({13, 13}));
        tablet_rowsets.add_rowset(create_rowset_pb({14, 14}));
        tablet_rowsets.add_rowset(create_rowset_pb({15, 15}));
        tablet_rowsets.add_rowset(create_rowset_pb({16, 16}));
        // - BE2 prepare cumu4 [15-16]
        // Load [17][18]
        tablet_rowsets.add_rowset(create_rowset_pb({17, 17}));
        tablet_rowsets.add_rowset(create_rowset_pb({18, 18}));
        // - BE1 Prepare cumu2
        tablet->cloud_sync_rowsets();
        CloudCumulativeCompaction cumu2(tablet);
        sp->set_call_back("CloudMetaMgr::prepare_tablet_job", [](auto&& args) {
            auto job = try_any_cast<const selectdb::TabletJobInfoPB&>(args[0]);
            auto pair = try_any_cast<std::pair<Status, bool>*>(args.back());
            if ((job.compaction(0).input_versions(0) <= 12 &&
                 job.compaction(0).input_versions(1) >= 2) ||
                (job.compaction(0).input_versions(0) <= 16 &&
                 job.compaction(0).input_versions(1) >= 15)) {
                auto res = try_any_cast<selectdb::StartTabletJobResponse*>(args[1]);
                res->mutable_status()->set_code(selectdb::JOB_TABLET_BUSY);
                res->add_version_in_compaction(2);
                res->add_version_in_compaction(10);
                res->add_version_in_compaction(11);
                res->add_version_in_compaction(12);
                res->add_version_in_compaction(15);
                res->add_version_in_compaction(16);
                pair->first = Status::Error<ErrorCode::INTERNAL_ERROR>("");
            }
            pair->second = true;
        });
        st = cumu2.prepare_compact();
        ASSERT_EQ(st, Status::OK());
        auto& cumu2_input_rowsets = cumu2.get_input_rowsets();
        ASSERT_EQ(cumu2_input_rowsets.size(), 2);
        ASSERT_EQ(cumu2_input_rowsets[0]->start_version(), 17);
        ASSERT_EQ(cumu2_input_rowsets[1]->end_version(), 18);
        cumu2._output_rowset = create_rowset({17, 18});
        // - BE2 commit cumu3 and prompt cumu point
        tablet_rowsets.add_rowset(create_rowset_pb({2, 10}));
        auto& stats = tablet_rowsets.stats;
        stats.set_cumulative_compaction_cnt(3);
        stats.set_cumulative_point(11);
        // - BE2 commit cumu4
        tablet_rowsets.add_rowset(create_rowset_pb({15, 16}));
        stats.set_cumulative_compaction_cnt(4);
        // - BE1 Thread1 commit cumu1
        tablet_rowsets.add_rowset(create_rowset_pb({11, 12}));
        stats.set_cumulative_compaction_cnt(5);
        auto cumu1_stats = stats;
        // - BE1 Thread2 commit cumu2
        tablet_rowsets.add_rowset(create_rowset_pb({17, 18}));
        stats.set_cumulative_compaction_cnt(6);
        auto cumu2_stats = stats;
        // - BE1 Thread2 cumu2 update tablet cache
        commit_job_stats = cumu2_stats;
        cumu2.modify_rowsets(nullptr);
        // - BE1 Thread1 cumu1 update tablet cache
        commit_job_stats = cumu1_stats;
        cumu1.modify_rowsets(nullptr);
        tablet->cloud_sync_rowsets();
        Versions versions;
        tablet->capture_consistent_versions({0, 18}, &versions, false);
        ASSERT_EQ(versions.size(), 7);
        EXPECT_EQ(versions[1], Version(2, 10));
        EXPECT_EQ(versions[2], Version(11, 12));
        EXPECT_EQ(versions[3], Version(13, 13));
        EXPECT_EQ(versions[4], Version(14, 14));
        EXPECT_EQ(versions[5], Version(15, 16));
        EXPECT_EQ(versions[6], Version(17, 18));
        EXPECT_EQ(tablet->base_compaction_cnt(), 0);
        EXPECT_EQ(tablet->cumulative_compaction_cnt(), 6);
        EXPECT_EQ(tablet->cumulative_layer_point(), 11);
        EXPECT_EQ(tablet->fetch_add_approximate_num_rowsets(0), 7);
    }
    {
        prepare_tablet(10013);
        // Load [11][12]
        auto& tablet_rowsets = mock_service->tablet_rowsets_map[10013];
        tablet_rowsets.add_rowset(create_rowset_pb({11, 11}));
        tablet_rowsets.add_rowset(create_rowset_pb({12, 12}));
        TabletSharedPtr tablet;
        cloud::tablet_mgr()->get_tablet(10013, &tablet);
        // BE2 commit cumu [2-10]
        tablet_rowsets.add_rowset(create_rowset_pb({2, 10}));
        auto& stats = tablet_rowsets.stats;
        stats.set_cumulative_compaction_cnt(3);
        stats.set_cumulative_point(11);
        // BE2 commit cumu [11-12]
        tablet_rowsets.add_rowset(create_rowset_pb({11, 12}));
        stats.set_cumulative_compaction_cnt(4);
        // BE1 sync tablet cache
        tablet->cloud_sync_rowsets();
        Versions versions;
        tablet->capture_consistent_versions({0, 18}, &versions, false);
        ASSERT_EQ(versions.size(), 3);
        EXPECT_EQ(versions[1], Version(2, 10));
        EXPECT_EQ(versions[2], Version(11, 12));
        EXPECT_EQ(tablet->base_compaction_cnt(), 0);
        EXPECT_EQ(tablet->cumulative_compaction_cnt(), 4);
        EXPECT_EQ(tablet->cumulative_layer_point(), 11);
        EXPECT_EQ(tablet->fetch_add_approximate_num_rowsets(0), 3);
    }
}

TEST(CloudCompactionTest, full_compaction_schedule) {
    StorageEngine storage_engine({});

    // Launch base compaction pool to submit compaction
    ThreadPoolBuilder("BaseCompactionTaskThreadPool")
            .set_min_threads(2)
            .set_max_threads(2)
            .build(&storage_engine._base_compaction_thread_pool);

    auto sp = SyncPoint::get_instance();
    Defer defer {[sp] {
        sp->clear_call_back("MetaServiceProxy::get_client");
        sp->clear_call_back("CloudMetaMgr::prepare_tablet_job");
        sp->clear_call_back("CloudCumulativeCompaction::execute_compact_impl");
        sp->load_dependency({}); // Clear dependency
    }};
    sp->enable_processing();

    // Create MockMetaService
    auto mock_service = std::make_shared<MockMetaService>();
    sp->set_call_back("MetaServiceProxy::get_client", [&mock_service](auto&& args) {
        auto stub = try_any_cast<std::shared_ptr<selectdb::MetaService_Stub>*>(args[0]);
        *stub = mock_service;
        auto pair = try_any_cast<std::pair<Status, bool>*>(args.back());
        pair->second = true;
    });

    // Prepare tablet
    constexpr int tablet_id = 10011;
    {
        auto& tablet_rowsets = mock_service->tablet_rowsets_map[tablet_id];
        tablet_rowsets.add_rowset(create_rowset_pb({0, 1}));
        tablet_rowsets.add_rowset(create_rowset_pb({2, 2}));
        tablet_rowsets.add_rowset(create_rowset_pb({3, 3}));
        auto& stats = tablet_rowsets.stats;
        stats.set_base_compaction_cnt(0);
        stats.set_cumulative_compaction_cnt(0);
        stats.set_cumulative_point(2);
    }

    sp->set_call_back("CloudMetaMgr::prepare_tablet_job", [](auto&& args) {
        auto pair = try_any_cast<std::pair<Status, bool>*>(args.back());
        pair->second = true;
    });
    sp->set_call_back("CloudFullCompaction::execute_compact_impl", [](auto&& args) {
        ::sleep(1);
        auto pair = try_any_cast<std::pair<Status, bool>*>(args.back());
        pair->second = true;
    });

    config::cumulative_compaction_min_deltas = 2;
    config::enable_parallel_cumu_compaction = true;
    TabletSharedPtr tablet;
    auto st = cloud::tablet_mgr()->get_tablet(tablet_id, &tablet);
    ASSERT_TRUE(st.ok()) << st;
    // To meet the condition of not skipping in `get_topn_tablets_to_compact`
    tablet->last_load_time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                        std::chrono::system_clock::now().time_since_epoch())
                                        .count();
    // Submit full compaction on `tablet`
    tablet->set_tablet_state(TabletState::TABLET_RUNNING);
    st = storage_engine.submit_compaction_task(tablet, CompactionType::FULL_COMPACTION);
    ASSERT_EQ(st, Status::OK());

    // `tablet` should in candidate tablets for cumu compaction
    auto candidate_tablets = storage_engine._generate_cloud_compaction_tasks(
            CompactionType::CUMULATIVE_COMPACTION, false);
    ASSERT_EQ(candidate_tablets.size(), 1);
    ASSERT_EQ(candidate_tablets[0], tablet);

    // `tablet` should not in candidate tablets for base compaction
    candidate_tablets =
            storage_engine._generate_cloud_compaction_tasks(CompactionType::BASE_COMPACTION, false);
    ASSERT_EQ(candidate_tablets.size(), 0);

    st = storage_engine.submit_compaction_task(tablet, CompactionType::FULL_COMPACTION);
    ASSERT_TRUE(st.is<ALREADY_EXIST>());
}

TEST(CloudCompactionTest, parallel_full_cumu_compaction) {
    StorageEngine storage_engine({});

    auto sp = SyncPoint::get_instance();
    Defer defer {[sp] {
        sp->clear_call_back("MetaServiceProxy::get_client");
        sp->clear_call_back("CloudMetaMgr::prepare_tablet_job");
        sp->clear_call_back("CloudMetaMgr::commit_tablet_job");
    }};
    sp->enable_processing();

    // Create MockMetaService
    auto mock_service = std::make_shared<MockMetaService>();
    sp->set_call_back("MetaServiceProxy::get_client", [&mock_service](auto&& args) {
        auto stub = try_any_cast<std::shared_ptr<selectdb::MetaService_Stub>*>(args[0]);
        *stub = mock_service;
        auto pair = try_any_cast<std::pair<Status, bool>*>(args.back());
        pair->second = true;
    });

    auto prepare_tablet = [&](int64_t tablet_id) {
        auto& tablet_rowsets = mock_service->tablet_rowsets_map[tablet_id];
        tablet_rowsets.add_rowset(create_rowset_pb({0, 1}));
        tablet_rowsets.add_rowset(create_rowset_pb({2, 10}));
        tablet_rowsets.add_rowset(create_rowset_pb({11, 20}));
        tablet_rowsets.add_rowset(create_rowset_pb({21, 30}));
        tablet_rowsets.add_rowset(create_rowset_pb({31, 32}));
        tablet_rowsets.add_rowset(create_rowset_pb({33, 33}));
        tablet_rowsets.add_rowset(create_rowset_pb({34, 34}));
        auto& stats = tablet_rowsets.stats;
        stats.set_base_compaction_cnt(1);
        stats.set_cumulative_compaction_cnt(10);
        stats.set_cumulative_point(31);
    };

    sp->set_call_back("CloudMetaMgr::prepare_tablet_job", [](auto&& args) {
        auto pair = try_any_cast<std::pair<Status, bool>*>(args.back());
        pair->second = true;
    });

    config::cumulative_compaction_min_deltas = 2;
    config::base_compaction_num_cumulative_deltas = 2;

    selectdb::TabletStatsPB commit_job_stats;
    sp->set_call_back("CloudMetaMgr::commit_tablet_job", [&](auto&& args) {
        selectdb::FinishTabletJobResponse* res =
                try_any_cast<selectdb::FinishTabletJobResponse*>(args[1]);
        *res->mutable_stats() = commit_job_stats;
        auto pair = try_any_cast<std::pair<Status, bool>*>(args.back());
        pair->second = true;
    });

    {
        // Simulate executing commit job and receiving response out of sequence:
        //  commit cumu -> commit full -> full update cache -> cumu update cache
        prepare_tablet(10011);
        TabletSharedPtr tablet;
        cloud::tablet_mgr()->get_tablet(10011, &tablet);
        CloudCumulativeCompaction cumu(tablet);
        auto st = cumu.prepare_compact();
        ASSERT_EQ(st, Status::OK());
        cumu._output_rowset = create_rowset({31, 34});

        CloudFullCompaction full(tablet);
        st = full.prepare_compact();
        ASSERT_EQ(st, Status::OK());
        full._output_rowset = create_rowset({2, 30});

        // - Thread1: commit cumu
        auto& tablet_rowsets = mock_service->tablet_rowsets_map[10011];
        tablet_rowsets.add_rowset(create_rowset_pb({31, 34}));
        auto& stats = tablet_rowsets.stats;
        stats.set_cumulative_compaction_cnt(11);
        auto cumu_stats = stats;

        // - Thread2: commit full
        tablet_rowsets.add_rowset(create_rowset_pb({2, 30}));
        stats.set_base_compaction_cnt(2);
        auto base_stats = stats;

        // - Thread2: base update tablet cache
        commit_job_stats = base_stats;
        full.modify_rowsets(nullptr);

        // - Thread1: cumu update tablet cache
        commit_job_stats = cumu_stats;
        cumu.modify_rowsets(nullptr);

        Versions versions;
        tablet->capture_consistent_versions({0, 34}, &versions, false);
        ASSERT_EQ(versions.size(), 3);
        EXPECT_EQ(versions[1], Version(2, 30));
        EXPECT_EQ(versions[2], Version(31, 34));
        EXPECT_EQ(tablet->base_compaction_cnt(), 2);
        EXPECT_EQ(tablet->cumulative_compaction_cnt(), 11);
        EXPECT_EQ(tablet->cumulative_layer_point(), 31);
        EXPECT_EQ(tablet->fetch_add_approximate_num_rowsets(0), 3);
    }
    {
        // Simulate executing commit job and receiving response out of sequence:
        //  commit full -> commit cumu -> cumu update cache -> full update cache
        prepare_tablet(10012);
        TabletSharedPtr tablet;
        cloud::tablet_mgr()->get_tablet(10012, &tablet);
        CloudCumulativeCompaction cumu(tablet);
        auto st = cumu.prepare_compact();
        ASSERT_EQ(st, Status::OK());
        cumu._output_rowset = create_rowset({31, 34});

        CloudFullCompaction full(tablet);
        st = full.prepare_compact();
        ASSERT_EQ(st, Status::OK());
        full._output_rowset = create_rowset({2, 30});

        // - Thread1: commit base
        auto& tablet_rowsets = mock_service->tablet_rowsets_map[10012];
        tablet_rowsets.add_rowset(create_rowset_pb({2, 30}));
        auto& stats = tablet_rowsets.stats;
        stats.set_base_compaction_cnt(2);
        auto base_stats = stats;

        // - Thread2: commit cumu
        tablet_rowsets.add_rowset(create_rowset_pb({31, 34}));
        stats.set_cumulative_compaction_cnt(11);
        auto cumu_stats = stats;

        // - Thread2: cumu update tablet cache
        commit_job_stats = cumu_stats;
        cumu.modify_rowsets(nullptr);

        // - Thread1: base update tablet cache
        commit_job_stats = base_stats;
        full.modify_rowsets(nullptr);

        Versions versions;
        tablet->capture_consistent_versions({0, 34}, &versions, false);
        ASSERT_EQ(versions.size(), 3);
        EXPECT_EQ(versions[1], Version(2, 30));
        EXPECT_EQ(versions[2], Version(31, 34));
        EXPECT_EQ(tablet->base_compaction_cnt(), 2);
        EXPECT_EQ(tablet->cumulative_compaction_cnt(), 11);
        EXPECT_EQ(tablet->cumulative_layer_point(), 31);
        EXPECT_EQ(tablet->fetch_add_approximate_num_rowsets(0), 3);
    }
    {
        //sleep(30);
        // Simulate executing commit job and update tablet cache, suppose this BE is BE1:
        //  BE1 prepare full -> BE2 commit cumu -> BE1 commit full -> BE1 full update cache
        prepare_tablet(10013);
        TabletSharedPtr tablet;
        cloud::tablet_mgr()->get_tablet(10013, &tablet);
        CloudFullCompaction full(tablet);
        auto st = full.prepare_compact();
        ASSERT_EQ(st, Status::OK());
        full._output_rowset = create_rowset({2, 30});

        // - BE2 commit cumu and update cumu point
        auto& tablet_rowsets = mock_service->tablet_rowsets_map[10013];
        tablet_rowsets.add_rowset(create_rowset_pb({31, 34}));
        auto& stats = tablet_rowsets.stats;
        stats.set_cumulative_compaction_cnt(11);
        stats.set_cumulative_point(35);

        // - BE1 commit base
        tablet_rowsets.add_rowset(create_rowset_pb({2, 30}));
        stats.set_base_compaction_cnt(2);

        // - BE1 cumu update tablet cache
        auto base_stats = stats;
        base_stats.set_cumulative_point(31);
        commit_job_stats = base_stats;
        full.modify_rowsets(nullptr);
        tablet->cloud_sync_rowsets();
        Versions versions;
        tablet->capture_consistent_versions({0, 34}, &versions, false);
        ASSERT_EQ(versions.size(), 3);
        EXPECT_EQ(versions[1], Version(2, 30));
        EXPECT_EQ(versions[2], Version(31, 34));
        EXPECT_EQ(tablet->base_compaction_cnt(), 2);
        EXPECT_EQ(tablet->cumulative_compaction_cnt(), 11);
        EXPECT_EQ(tablet->cumulative_layer_point(), 35);
        EXPECT_EQ(tablet->fetch_add_approximate_num_rowsets(0), 3);
    }
}
