#include <brpc/controller.h>
#include <fmt/format.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <cstdlib>
#include <limits>
#include <random>

#include "common/sync_point.h"
#include "common/util.h"
#include "gen_cpp/olap_file.pb.h"
#include "gen_cpp/selectdb_cloud.pb.h"
#include "meta-service/keys.h"
#include "meta-service/meta_service.h"

namespace selectdb {
extern std::unique_ptr<MetaServiceImpl> get_meta_service();

static std::string instance_id = "MetaServiceJobTest";

TEST(MetaServiceJobTest, CompactionJobTest) {
    auto meta_service = get_meta_service();
    meta_service->resource_mgr_.reset(); // Do not use resource manager

    auto sp = SyncPoint::get_instance();
    sp->clear_all_call_backs();
    sp->set_call_back("get_instance_id::pred", [](void* p) { *((bool*)p) = true; });
    sp->set_call_back("get_instance_id", [&](void* p) { *((std::string*)p) = instance_id; });
    sp->enable_processing();

    brpc::Controller cntl;

    int start_compaction_job_cnt = 0;

    int64_t table_id = 1;
    int64_t index_id = 2;
    int64_t partition_id = 3;
    int64_t tablet_id = 4;

    // Start compaction job
    auto start_compaction_job = [&] {
        StartTabletJobRequest req;
        StartTabletJobResponse res;
        std::string job_id = "job_id123";

        auto compaction = req.mutable_job()->add_compaction();
        compaction->set_id(job_id);
        compaction->set_initiator("ip:port");

        meta_service->start_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                       &req, &res, nullptr);
        ASSERT_NE(res.status().msg().find("no valid tablet_id given"), std::string::npos);

        req.mutable_job()->mutable_idx()->set_tablet_id(tablet_id);
        meta_service->start_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                       &req, &res, nullptr);
        if (start_compaction_job_cnt++ < 1) {
            ASSERT_NE(res.status().msg().find("failed to get table id with tablet_id"),
                      std::string::npos);
        }

        auto index_key = meta_tablet_idx_key({instance_id, tablet_id});
        TabletIndexPB idx_pb;
        idx_pb.set_table_id(1);
        idx_pb.set_index_id(2);
        idx_pb.set_partition_id(3);
        idx_pb.set_tablet_id(tablet_id + 1); // error, tablet_id not match
        compaction->set_base_compaction_cnt(10);
        compaction->set_cumulative_compaction_cnt(20);
        std::string idx_val = idx_pb.SerializeAsString();
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv_->create_txn(&txn), 0);
        txn->put(index_key, idx_val);
        std::string stats_key =
                stats_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id});
        TabletStatsPB stats;
        stats.set_base_compaction_cnt(9);
        stats.set_cumulative_compaction_cnt(19);
        txn->put(stats_key, stats.SerializeAsString());
        ASSERT_EQ(txn->commit(), 0);
        meta_service->start_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                       &req, &res, nullptr);
        ASSERT_NE(res.status().msg().find("internal error"), std::string::npos);
        idx_pb.set_tablet_id(tablet_id); // Correct tablet_id
        idx_val = idx_pb.SerializeAsString();
        ASSERT_EQ(meta_service->txn_kv_->create_txn(&txn), 0);
        txn->put(index_key, idx_val);
        ASSERT_EQ(txn->commit(), 0);
        meta_service->start_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                       &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

        ASSERT_EQ(meta_service->txn_kv_->create_txn(&txn), 0);
        auto job_key = job_tablet_key({instance_id, idx_pb.table_id(), idx_pb.index_id(),
                                       idx_pb.partition_id(), idx_pb.tablet_id()});
        std::string job_val;
        ASSERT_EQ(txn->get(job_key, &job_val), 0);
        TabletJobInfoPB job_pb;
        ASSERT_TRUE(job_pb.ParseFromString(job_val));
        meta_service->start_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                       &req, &res, nullptr);
        ASSERT_NE(res.status().msg().find("already started"), std::string::npos);
    };

    // Finish, this unit test relies on the previous (start_job)
    ASSERT_NO_FATAL_FAILURE(start_compaction_job());
    {
        FinishTabletJobRequest req;
        FinishTabletJobResponse res;
        std::string job_id = "job_id123";

        auto compaction = req.mutable_job()->add_compaction();
        compaction->set_id(job_id);
        compaction->set_initiator("ip:port");
        meta_service->finish_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                        &req, &res, nullptr);
        ASSERT_NE(res.status().msg().find("no valid tablet_id given"), std::string::npos);

        req.mutable_job()->mutable_idx()->set_table_id(table_id);
        req.mutable_job()->mutable_idx()->set_index_id(index_id);
        req.mutable_job()->mutable_idx()->set_partition_id(partition_id);
        req.mutable_job()->mutable_idx()->set_tablet_id(tablet_id);
        compaction->set_base_compaction_cnt(10);
        compaction->set_cumulative_compaction_cnt(20);
        // Action is not set
        meta_service->finish_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                        &req, &res, nullptr);
        ASSERT_NE(res.status().msg().find("unsupported action"), std::string::npos);

        //======================================================================
        // Test commit
        //======================================================================
        req.set_action(FinishTabletJobRequest::COMMIT);

        // Tablet meta not found, this is unexpected
        meta_service->finish_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                        &req, &res, nullptr);
        ASSERT_NE(res.status().msg().find("failed to get tablet meta"), std::string::npos);

        // Create txn meta, and try again
        auto tablet_meta_key =
                meta_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id});
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv_->create_txn(&txn), 0);
        doris::TabletMetaPB tablet_meta_pb;
        tablet_meta_pb.set_table_id(table_id);
        tablet_meta_pb.set_index_id(index_id);
        tablet_meta_pb.set_partition_id(partition_id);
        tablet_meta_pb.set_tablet_id(tablet_id);
        tablet_meta_pb.set_cumulative_layer_point(50);
        std::string tablet_meta_val = tablet_meta_pb.SerializeAsString();
        ASSERT_FALSE(tablet_meta_val.empty());
        txn->put(tablet_meta_key, tablet_meta_val);
        ASSERT_EQ(txn->commit(), 0);

        meta_service->finish_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                        &req, &res, nullptr);

        // Create create tablet stats, compation job will will update stats
        auto tablet_stats_key =
                stats_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id});
        TabletStatsPB tablet_stats_pb;
        tablet_stats_pb.mutable_idx()->set_table_id(table_id);
        tablet_stats_pb.mutable_idx()->set_index_id(index_id);
        tablet_stats_pb.mutable_idx()->set_partition_id(partition_id);
        tablet_stats_pb.mutable_idx()->set_tablet_id(tablet_id);

        std::mt19937 rng(std::chrono::system_clock::now().time_since_epoch().count());
        std::uniform_int_distribution<int> dist(1, 10000); // Positive numbers

        compaction->set_output_cumulative_point(tablet_stats_pb.cumulative_point() + dist(rng));
        compaction->set_num_output_rows(dist(rng));
        compaction->set_num_output_rowsets(dist(rng));
        compaction->set_num_output_segments(dist(rng));
        compaction->set_num_input_rows(dist(rng));
        compaction->set_num_input_rowsets(dist(rng));
        compaction->set_num_input_segments(dist(rng));
        compaction->set_size_input_rowsets(dist(rng));
        compaction->set_size_output_rowsets(dist(rng));
        compaction->set_type((dist(rng) % 10) < 2 ? TabletCompactionJobPB::BASE
                                                  : TabletCompactionJobPB::CUMULATIVE);

        tablet_stats_pb.set_cumulative_compaction_cnt(dist(rng));
        tablet_stats_pb.set_base_compaction_cnt(dist(rng));
        tablet_stats_pb.set_cumulative_point(tablet_meta_pb.cumulative_layer_point());
        // MUST let data stats be larger than input data size
        tablet_stats_pb.set_num_rows(dist(rng) + compaction->num_input_rows());
        tablet_stats_pb.set_data_size(dist(rng) + compaction->size_input_rowsets());
        tablet_stats_pb.set_num_rowsets(dist(rng) + compaction->num_input_rowsets());
        tablet_stats_pb.set_num_segments(dist(rng) + compaction->num_input_segments());
        tablet_stats_pb.set_last_compaction_time(dist(rng));

        std::string tablet_stats_val = tablet_stats_pb.SerializeAsString();
        ASSERT_FALSE(tablet_stats_val.empty());
        ASSERT_EQ(meta_service->txn_kv_->create_txn(&txn), 0);
        txn->put(tablet_stats_key, tablet_stats_val);
        ASSERT_EQ(txn->commit(), 0);

        // Input rowset not valid
        meta_service->finish_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                        &req, &res, nullptr);
        ASSERT_NE(res.status().msg().find("invalid input"), std::string::npos);

        // Provide input and output rowset info
        int64_t input_version_start = dist(rng);
        int64_t input_version_end = input_version_start + 100;
        compaction->add_input_versions(input_version_start);
        compaction->add_input_versions(input_version_end);
        compaction->add_output_versions(input_version_end);
        compaction->add_output_rowset_ids("output rowset id");

        // Input rowsets must exist, and more than 0
        // Check number input rowsets
        sp->set_call_back("process_compaction_job::loop_input_done", [](void* c) {
            int& num_input_rowsets = *(int*)c;
            ASSERT_EQ(num_input_rowsets, 0); // zero existed rowsets
        });
        sp->set_call_back("process_compaction_job::too_few_rowsets", [](void* c) {
            auto& need_commit = *(bool*)c;
            ASSERT_EQ(need_commit, true);
            need_commit = false; // Donot remove tablet job in order to continue test
        });

        meta_service->finish_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                        &req, &res, nullptr);
        ASSERT_NE(res.status().msg().find("too few input rowsets"), std::string::npos);

        // Provide input rowset KVs, boundary test, 5 input rowsets
        ASSERT_EQ(meta_service->txn_kv_->create_txn(&txn), 0);
        // clang-format off
        std::vector<std::string> input_rowset_keys = {
                meta_rowset_key({instance_id, tablet_id, input_version_start - 1}),
                meta_rowset_key({instance_id, tablet_id, input_version_start}),
                meta_rowset_key({instance_id, tablet_id, input_version_start + 1}),
                meta_rowset_key({instance_id, tablet_id, (input_version_start + input_version_end) / 2}),
                meta_rowset_key({instance_id, tablet_id, input_version_end - 1}),
                meta_rowset_key({instance_id, tablet_id, input_version_end}),
                meta_rowset_key({instance_id, tablet_id, input_version_end + 1}),
        };
        // clang-format on
        std::vector<std::unique_ptr<std::string>> input_rowset_vals;
        for (auto& i : input_rowset_keys) {
            doris::RowsetMetaPB rs_pb;
            rs_pb.set_rowset_id(0);
            rs_pb.set_rowset_id_v2(hex(i));
            input_rowset_vals.emplace_back(new std::string(rs_pb.SerializeAsString()));
            txn->put(i, *input_rowset_vals.back());
        }
        ASSERT_EQ(txn->commit(), 0);

        // Check number input rowsets
        sp->set_call_back("process_compaction_job::loop_input_done", [](void* c) {
            int& num_input_rowsets = *(int*)c;
            ASSERT_EQ(num_input_rowsets, 5);
        });
        // No tmp rowset key (output rowset)
        meta_service->finish_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                        &req, &res, nullptr);
        ASSERT_NE(res.status().msg().find("invalid txn_id"), std::string::npos);

        int64_t txn_id = dist(rng);
        compaction->add_txn_id(txn_id);

        meta_service->finish_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                        &req, &res, nullptr);
        ASSERT_NE(res.status().msg().find("failed to get tmp rowset key"), std::string::npos);

        // Provide invalid output rowset meta
        auto tmp_rowset_key = meta_rowset_tmp_key({instance_id, txn_id, tablet_id});
        doris::RowsetMetaPB tmp_rs_pb;
        tmp_rs_pb.set_rowset_id(0);
        auto tmp_rowset_val = tmp_rs_pb.SerializeAsString();
        ASSERT_EQ(meta_service->txn_kv_->create_txn(&txn), 0);
        txn->put(tmp_rowset_key, tmp_rowset_val);
        ASSERT_EQ(txn->commit(), 0);

        meta_service->finish_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                        &req, &res, nullptr);
        ASSERT_NE(res.status().msg().find("invalid txn_id in output tmp rowset meta"),
                  std::string::npos);

        // Provide txn_id in output rowset meta
        tmp_rs_pb.set_txn_id(10086);
        tmp_rowset_val = tmp_rs_pb.SerializeAsString();
        ASSERT_EQ(meta_service->txn_kv_->create_txn(&txn), 0);
        txn->put(tmp_rowset_key, tmp_rowset_val);
        ASSERT_EQ(txn->commit(), 0);

        meta_service->finish_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                        &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

        //=====================================================================
        // All branch tests done, we are done commit a compaction job
        //=====================================================================
        ASSERT_EQ(meta_service->txn_kv_->create_txn(&txn), 0);
        tablet_stats_val.clear();
        ASSERT_EQ(txn->get(tablet_stats_key, &tablet_stats_val), 0);
        TabletStatsPB stats;
        ASSERT_TRUE(stats.ParseFromString(tablet_stats_val));

        // clang-format off
        EXPECT_EQ(stats.base_compaction_cnt()      , tablet_stats_pb.base_compaction_cnt() + (req.job().compaction(0).type() == TabletCompactionJobPB::BASE));
        EXPECT_EQ(stats.cumulative_compaction_cnt(), tablet_stats_pb.cumulative_compaction_cnt() + (req.job().compaction(0).type() == TabletCompactionJobPB::CUMULATIVE));
        EXPECT_EQ(stats.cumulative_point()         , req.job().compaction(0).type() == TabletCompactionJobPB::BASE ? 50 : req.job().compaction(0).output_cumulative_point());
        EXPECT_EQ(stats.num_rows()                 , tablet_stats_pb.num_rows() + (req.job().compaction(0).num_output_rows() - req.job().compaction(0).num_input_rows()));
        EXPECT_EQ(stats.data_size()                , tablet_stats_pb.data_size() + (req.job().compaction(0).size_output_rowsets() - req.job().compaction(0).size_input_rowsets()));
        EXPECT_EQ(stats.num_rowsets()              , tablet_stats_pb.num_rowsets() + (req.job().compaction(0).num_output_rowsets() - req.job().compaction(0).num_input_rowsets()));
        EXPECT_EQ(stats.num_segments()             , tablet_stats_pb.num_segments() + (req.job().compaction(0).num_output_segments() - req.job().compaction(0).num_input_segments()));
        // EXPECT_EQ(stats.last_compaction_time     (), now);
        // clang-format on

        // Check job removed, tablet meta updated
        auto job_key = job_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id});
        std::string job_val;
        ASSERT_EQ(txn->get(job_key, &job_val), 0);
        TabletJobInfoPB job_pb;
        ASSERT_TRUE(job_pb.ParseFromString(job_val));
        ASSERT_TRUE(job_pb.compaction().empty());
        tablet_meta_val.clear();
        ASSERT_EQ(txn->get(tablet_meta_key, &tablet_meta_val), 0);
        ASSERT_TRUE(tablet_meta_pb.ParseFromString(tablet_meta_val));
        ASSERT_EQ(tablet_meta_pb.cumulative_layer_point(),
                  req.job().compaction(0).output_cumulative_point());
        ASSERT_EQ(tablet_meta_pb.cumulative_layer_point(), stats.cumulative_point());

        // Check tmp rowset removed
        ASSERT_EQ(txn->get(tmp_rowset_key, &tmp_rowset_val), 1);
        // Check input rowsets removed, the largest version remains
        for (int i = 1; i < input_rowset_keys.size() - 2; ++i) {
            std::string val;
            EXPECT_EQ(txn->get(input_rowset_keys[i], &val), 1) << hex(input_rowset_keys[i]);
        }
        // Check recycle rowsets added
        for (int i = 1; i < input_rowset_vals.size() - 1; ++i) {
            doris::RowsetMetaPB rs;
            ASSERT_TRUE(rs.ParseFromString(*input_rowset_vals[i]));
            auto key = recycle_rowset_key({instance_id, tablet_id, rs.rowset_id_v2()});
            std::string val;
            EXPECT_EQ(txn->get(key, &val), 0) << hex(key);
        }
        // Check output rowset added
        auto rowset_key = meta_rowset_key({instance_id, tablet_id, input_version_end});
        std::string rowset_val;
        EXPECT_EQ(txn->get(rowset_key, &rowset_val), 0) << hex(rowset_key);
    }

    // Test abort compaction
    ASSERT_NO_FATAL_FAILURE(start_compaction_job());
    {
        FinishTabletJobRequest req;
        FinishTabletJobResponse res;
        std::string job_id = "job_id123";

        auto compaction = req.mutable_job()->add_compaction();
        compaction->set_id(job_id);
        compaction->set_initiator("ip:port");
        req.mutable_job()->mutable_idx()->set_tablet_id(tablet_id);
        req.set_action(FinishTabletJobRequest::ABORT);
        meta_service->finish_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                        &req, &res, nullptr);

        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv_->create_txn(&txn), 0);
        auto job_key = job_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id});
        std::string job_val;
        ASSERT_EQ(txn->get(job_key, &job_val), 0);
        TabletJobInfoPB job_pb;
        ASSERT_TRUE(job_pb.ParseFromString(job_val));
        ASSERT_TRUE(job_pb.compaction().empty());
    }
}

static std::string next_rowset_id() {
    static int cnt = 0;
    return std::to_string(++cnt);
}

static void create_tablet(MetaServiceImpl* meta_service, int64_t table_id, int64_t index_id,
                          int64_t partition_id, int64_t tablet_id, bool not_ready = false) {
    brpc::Controller cntl;
    CreateTabletsRequest req;
    CreateTabletsResponse res;
    auto tablet = req.add_tablet_metas();
    tablet->set_tablet_state(not_ready ? doris::TabletStatePB::PB_NOTREADY
                                       : doris::TabletStatePB::PB_RUNNING);
    tablet->set_table_id(table_id);
    tablet->set_index_id(index_id);
    tablet->set_partition_id(partition_id);
    tablet->set_tablet_id(tablet_id);
    auto schema = tablet->mutable_schema();
    schema->set_schema_version(0);
    auto first_rowset = tablet->add_rs_metas();
    first_rowset->set_rowset_id(0); // required
    first_rowset->set_rowset_id_v2(next_rowset_id());
    first_rowset->set_start_version(0);
    first_rowset->set_end_version(1);
    first_rowset->mutable_tablet_schema()->CopyFrom(*schema);
    meta_service->create_tablets(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << tablet_id;
}

static void start_schema_change_job(MetaServiceImpl* meta_service, int64_t table_id,
                                    int64_t index_id, int64_t partition_id, int64_t tablet_id,
                                    int64_t new_tablet_id, const std::string& job_id,
                                    const std::string& initiator) {
    brpc::Controller cntl;
    StartTabletJobRequest req;
    StartTabletJobResponse res;
    req.mutable_job()->mutable_idx()->set_table_id(table_id);
    req.mutable_job()->mutable_idx()->set_index_id(index_id);
    req.mutable_job()->mutable_idx()->set_partition_id(partition_id);
    req.mutable_job()->mutable_idx()->set_tablet_id(tablet_id);
    auto sc = req.mutable_job()->mutable_schema_change();
    sc->set_id(job_id);
    sc->set_initiator(initiator);
    sc->mutable_new_tablet_idx()->set_table_id(table_id);
    sc->mutable_new_tablet_idx()->set_index_id(index_id);
    sc->mutable_new_tablet_idx()->set_partition_id(partition_id);
    sc->mutable_new_tablet_idx()->set_tablet_id(new_tablet_id);
    meta_service->start_tablet_job(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << job_id << ' ' << initiator;
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(meta_service->txn_kv_->create_txn(&txn), 0) << job_id << ' ' << initiator;
    auto job_key = job_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id});
    std::string job_val;
    ASSERT_EQ(txn->get(job_key, &job_val), 0) << job_id << ' ' << initiator;
    TabletJobInfoPB job_pb;
    ASSERT_TRUE(job_pb.ParseFromString(job_val)) << job_id << ' ' << initiator;
    ASSERT_TRUE(job_pb.has_schema_change()) << job_id << ' ' << initiator;
    EXPECT_EQ(job_pb.schema_change().id(), job_id) << ' ' << initiator;
};

static doris::RowsetMetaPB create_rowset(int64_t tablet_id, int64_t start_version,
                                         int64_t end_version, int num_rows = 100) {
    doris::RowsetMetaPB rowset;
    rowset.set_rowset_id(0); // required
    rowset.set_rowset_id_v2(next_rowset_id());
    rowset.set_tablet_id(tablet_id);
    rowset.set_txn_id(start_version << 32 | end_version);
    rowset.set_start_version(start_version);
    rowset.set_end_version(end_version);
    rowset.set_num_segments(1);
    rowset.set_num_rows(num_rows);
    rowset.set_data_disk_size(num_rows * 100);
    rowset.mutable_tablet_schema()->set_schema_version(0);
    return rowset;
}

static void commit_rowset(MetaServiceImpl* meta_service, const doris::RowsetMetaPB& rowset,
                          CreateRowsetResponse& res) {
    brpc::Controller cntl;
    CreateRowsetRequest req;
    req.set_temporary(true);
    req.mutable_rowset_meta()->CopyFrom(rowset);
    meta_service->commit_rowset(&cntl, &req, &res, nullptr);
}

static void finish_schema_change_job(MetaServiceImpl* meta_service, int64_t table_id,
                                     int64_t index_id, int64_t partition_id, int64_t tablet_id,
                                     int64_t new_tablet_id, const std::string& job_id,
                                     const std::string& initiator,
                                     const std::vector<doris::RowsetMetaPB>& output_rowsets,
                                     FinishTabletJobResponse& res) {
    brpc::Controller cntl;
    FinishTabletJobRequest req;
    req.set_action(FinishTabletJobRequest::COMMIT);
    req.mutable_job()->mutable_idx()->set_table_id(table_id);
    req.mutable_job()->mutable_idx()->set_index_id(index_id);
    req.mutable_job()->mutable_idx()->set_partition_id(partition_id);
    req.mutable_job()->mutable_idx()->set_tablet_id(tablet_id);
    auto sc = req.mutable_job()->mutable_schema_change();
    sc->mutable_new_tablet_idx()->set_table_id(table_id);
    sc->mutable_new_tablet_idx()->set_index_id(index_id);
    sc->mutable_new_tablet_idx()->set_partition_id(partition_id);
    sc->mutable_new_tablet_idx()->set_tablet_id(new_tablet_id);
    if (output_rowsets.empty()) {
        sc->set_alter_version(0);
    } else {
        sc->set_alter_version(output_rowsets.back().end_version());
        for (auto& rowset : output_rowsets) {
            sc->add_txn_ids(rowset.txn_id());
            sc->add_output_versions(rowset.end_version());
            sc->set_num_output_rows(sc->num_output_rows() + rowset.num_rows());
            sc->set_num_output_segments(sc->num_output_segments() + rowset.num_segments());
            sc->set_size_output_rowsets(sc->size_output_rowsets() + rowset.data_disk_size());
        }
        sc->set_num_output_rowsets(output_rowsets.size());
    }
    sc->set_id(job_id);
    sc->set_initiator(initiator);
    meta_service->finish_tablet_job(&cntl, &req, &res, nullptr);
}

static void insert_rowsets(TxnKv* txn_kv, int64_t table_id, int64_t index_id, int64_t partition_id,
                           int64_t tablet_id, const std::vector<doris::RowsetMetaPB>& rowsets) {
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv->create_txn(&txn), 0) << tablet_id;
    std::deque<std::string> buffer;
    int data_size = 0, num_rows = 0, num_seg = 0;
    for (auto& rowset : rowsets) {
        data_size += rowset.data_disk_size();
        num_rows += rowset.num_rows();
        num_seg += rowset.num_segments();
        auto& key = buffer.emplace_back();
        auto& val = buffer.emplace_back();
        meta_rowset_key({instance_id, tablet_id, rowset.end_version()}, &key);
        ASSERT_TRUE(rowset.SerializeToString(&val)) << tablet_id;
        txn->put(key, val);
    }
    auto tablet_stat_key =
            stats_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id});
    std::string tablet_stat_val;
    ASSERT_EQ(txn->get(tablet_stat_key, &tablet_stat_val), 0) << tablet_id;
    TabletStatsPB tablet_stat;
    ASSERT_TRUE(tablet_stat.ParseFromString(tablet_stat_val)) << tablet_id;
    tablet_stat.set_num_rows(tablet_stat.num_rows() + num_rows);
    tablet_stat.set_data_size(tablet_stat.data_size() + data_size);
    tablet_stat.set_num_segments(tablet_stat.num_segments() + num_seg);
    tablet_stat.set_num_rowsets(tablet_stat.num_rowsets() + rowsets.size());
    ASSERT_TRUE(tablet_stat.SerializeToString(&tablet_stat_val)) << tablet_id;
    txn->put(tablet_stat_key, tablet_stat_val);
    ASSERT_EQ(txn->commit(), 0) << tablet_id;
}

TEST(MetaServiceJobTest, SchemaChangeJobTest) {
    auto meta_service = get_meta_service();
    meta_service->resource_mgr_.reset(); // Do not use resource manager

    auto sp = SyncPoint::get_instance();
    sp->clear_all_call_backs();
    sp->set_call_back("get_instance_id::pred", [](void* p) { *((bool*)p) = true; });
    sp->set_call_back("get_instance_id", [&](void* p) { *((std::string*)p) = instance_id; });
    sp->enable_processing();

    brpc::Controller cntl;

    int64_t table_id = 1;
    int64_t index_id = 2;
    int64_t partition_id = 3;
    int64_t tablet_id = 4;
    ASSERT_NO_FATAL_FAILURE(
            create_tablet(meta_service.get(), table_id, index_id, partition_id, tablet_id));

    // commit schema_change job with alter_version == 1
    {
        int64_t new_tablet_id = 14;
        ASSERT_NO_FATAL_FAILURE(create_tablet(meta_service.get(), table_id, index_id, partition_id,
                                              new_tablet_id, true));
        ASSERT_NO_FATAL_FAILURE(start_schema_change_job(meta_service.get(), table_id, index_id,
                                                        partition_id, tablet_id, new_tablet_id,
                                                        "job1", "be1"));
        FinishTabletJobResponse res;
        finish_schema_change_job(meta_service.get(), table_id, index_id, partition_id, tablet_id,
                                 new_tablet_id, "job2", "be1", {}, res);
        ASSERT_NE(res.status().code(), MetaServiceCode::OK);
        ASSERT_NE(res.status().msg().find("unmatched job id or initiator"), std::string::npos);
        res.Clear();
        finish_schema_change_job(meta_service.get(), table_id, index_id, partition_id, tablet_id,
                                 new_tablet_id, "job1", "be2", {}, res);
        ASSERT_NE(res.status().code(), MetaServiceCode::OK);
        ASSERT_NE(res.status().msg().find("unmatched job id or initiator"), std::string::npos);
        res.Clear();
        finish_schema_change_job(meta_service.get(), table_id, index_id, partition_id, tablet_id,
                                 new_tablet_id, "job1", "be1", {}, res);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        res.Clear();
        finish_schema_change_job(meta_service.get(), table_id, index_id, partition_id, tablet_id,
                                 new_tablet_id, "job1", "be1", {}, res);
        ASSERT_EQ(res.status().code(), MetaServiceCode::JOB_ALREADY_SUCCESS);
    }

    // commit schema_change job with txn_ids
    {
        int64_t new_tablet_id = 24;
        ASSERT_NO_FATAL_FAILURE(create_tablet(meta_service.get(), table_id, index_id, partition_id,
                                              new_tablet_id, true));
        ASSERT_NO_FATAL_FAILURE(start_schema_change_job(meta_service.get(), table_id, index_id,
                                                        partition_id, tablet_id, new_tablet_id,
                                                        "job2", "be1"));

        std::vector<doris::RowsetMetaPB> output_rowsets;
        int data_size = 0, num_rows = 0, num_seg = 0;
        for (int64_t i = 0; i < 5; ++i) {
            output_rowsets.push_back(create_rowset(new_tablet_id, i + 2, i + 2));
            data_size += output_rowsets.back().data_disk_size();
            num_rows += output_rowsets.back().num_rows();
            num_seg += output_rowsets.back().num_segments();
            CreateRowsetResponse res;
            commit_rowset(meta_service.get(), output_rowsets.back(), res);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << i;
        }

        FinishTabletJobResponse res;
        finish_schema_change_job(meta_service.get(), table_id, index_id, partition_id, tablet_id,
                                 new_tablet_id, "job2", "be1", output_rowsets, res);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv_->create_txn(&txn), 0);
        // check tablet state
        auto tablet_key =
                meta_tablet_key({instance_id, table_id, index_id, partition_id, new_tablet_id});
        std::string tablet_val;
        ASSERT_EQ(txn->get(tablet_key, &tablet_val), 0);
        doris::TabletMetaPB tablet_pb;
        ASSERT_TRUE(tablet_pb.ParseFromString(tablet_val));
        ASSERT_EQ(tablet_pb.tablet_state(), doris::TabletStatePB::PB_RUNNING);
        // check visible rowsets
        std::unique_ptr<RangeGetIterator> it;
        auto rs_start = meta_rowset_key({instance_id, new_tablet_id, 0});
        auto rs_end = meta_rowset_key({instance_id, new_tablet_id, 100});
        ASSERT_EQ(txn->get(rs_start, rs_end, &it), 0);
        ASSERT_EQ(it->size(), 6);
        auto [k, v] = it->next();
        doris::RowsetMetaPB saved_rowset;
        ASSERT_TRUE(saved_rowset.ParseFromArray(v.data(), v.size()));
        EXPECT_EQ(saved_rowset.start_version(), 0);
        EXPECT_EQ(saved_rowset.end_version(), 1);
        for (auto& rs : output_rowsets) {
            auto [k, v] = it->next();
            ASSERT_TRUE(saved_rowset.ParseFromArray(v.data(), v.size()));
            EXPECT_EQ(saved_rowset.rowset_id_v2(), rs.rowset_id_v2());
            EXPECT_EQ(saved_rowset.start_version(), rs.start_version());
            EXPECT_EQ(saved_rowset.end_version(), rs.end_version());
        }

        // check tablet stats
        auto tablet_stat_key =
                stats_tablet_key({instance_id, table_id, index_id, partition_id, new_tablet_id});
        std::string tablet_stat_val;
        ASSERT_EQ(txn->get(tablet_stat_key, &tablet_stat_val), 0);
        TabletStatsPB tablet_stat;
        ASSERT_TRUE(tablet_stat.ParseFromString(tablet_stat_val));
        ASSERT_EQ(tablet_stat.num_rows(), num_rows);
        ASSERT_EQ(tablet_stat.num_rowsets(), 6);
        ASSERT_EQ(tablet_stat.num_segments(), num_seg);
        ASSERT_EQ(tablet_stat.data_size(), data_size);
    }

    // commit schema_change job with rowsets which overlapped with visible rowsets
    {
        int64_t new_tablet_id = 34;
        ASSERT_NO_FATAL_FAILURE(create_tablet(meta_service.get(), table_id, index_id, partition_id,
                                              new_tablet_id, true));
        ASSERT_NO_FATAL_FAILURE(start_schema_change_job(meta_service.get(), table_id, index_id,
                                                        partition_id, tablet_id, new_tablet_id,
                                                        "job3", "be1"));
        // provide existed rowsets
        std::vector<doris::RowsetMetaPB> existed_rowsets;
        int data_size = 0, num_rows = 0, num_seg = 0;
        for (int i = 0; i < 5; ++i) {
            existed_rowsets.push_back(create_rowset(new_tablet_id, i + 11, i + 11));
            data_size += existed_rowsets.back().data_disk_size();
            num_rows += existed_rowsets.back().num_rows();
            num_seg += existed_rowsets.back().num_segments();
        }
        ASSERT_NO_FATAL_FAILURE(insert_rowsets(meta_service->txn_kv_.get(), table_id, index_id,
                                               partition_id, new_tablet_id, existed_rowsets));

        std::vector<doris::RowsetMetaPB> output_rowsets;
        output_rowsets.push_back(create_rowset(new_tablet_id, 2, 8));
        output_rowsets.push_back(create_rowset(new_tablet_id, 9, 12));
        output_rowsets.push_back(create_rowset(new_tablet_id, 13, 13));
        int output_data_size = 0, output_num_rows = 0, output_num_seg = 0;
        for (auto& rs : output_rowsets) {
            output_data_size += rs.data_disk_size();
            output_num_rows += rs.num_rows();
            output_num_seg += rs.num_segments();
            CreateRowsetResponse res;
            commit_rowset(meta_service.get(), rs, res);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << rs.end_version();
        }

        FinishTabletJobResponse res;
        finish_schema_change_job(meta_service.get(), table_id, index_id, partition_id, tablet_id,
                                 new_tablet_id, "job3", "be1", output_rowsets, res);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv_->create_txn(&txn), 0);
        // check tablet state
        auto tablet_key =
                meta_tablet_key({instance_id, table_id, index_id, partition_id, new_tablet_id});
        std::string tablet_val;
        ASSERT_EQ(txn->get(tablet_key, &tablet_val), 0);
        doris::TabletMetaPB tablet_pb;
        ASSERT_TRUE(tablet_pb.ParseFromString(tablet_val));
        ASSERT_EQ(tablet_pb.tablet_state(), doris::TabletStatePB::PB_RUNNING);

        // check tablet stats
        auto tablet_stat_key =
                stats_tablet_key({instance_id, table_id, index_id, partition_id, new_tablet_id});
        std::string tablet_stat_val;
        ASSERT_EQ(txn->get(tablet_stat_key, &tablet_stat_val), 0);
        TabletStatsPB tablet_stat;
        ASSERT_TRUE(tablet_stat.ParseFromString(tablet_stat_val));
        // overwrite [11-13], retain [14-15]
        EXPECT_EQ(tablet_stat.num_rows(), output_num_rows + num_rows - 300);
        EXPECT_EQ(tablet_stat.num_rowsets(), 6);
        EXPECT_EQ(tablet_stat.num_segments(), output_num_seg + num_seg - 3);
        EXPECT_EQ(tablet_stat.data_size(), output_data_size + data_size - 30000);

        // check visible rowsets
        std::unique_ptr<RangeGetIterator> it;
        auto rs_start = meta_rowset_key({instance_id, new_tablet_id, 0});
        auto rs_end = meta_rowset_key({instance_id, new_tablet_id, 100});
        ASSERT_EQ(txn->get(rs_start, rs_end, &it), 0);
        // [0-1][2-8][9-12][13-13][14-14][15-15]
        EXPECT_EQ(it->size(), 6);
        auto [k, v] = it->next();
        doris::RowsetMetaPB saved_rowset;
        ASSERT_TRUE(saved_rowset.ParseFromArray(v.data(), v.size()));
        EXPECT_EQ(saved_rowset.start_version(), 0);
        EXPECT_EQ(saved_rowset.end_version(), 1);
        for (auto& rs : output_rowsets) {
            auto [k, v] = it->next();
            ASSERT_TRUE(saved_rowset.ParseFromArray(v.data(), v.size()));
            EXPECT_EQ(saved_rowset.start_version(), rs.start_version());
            EXPECT_EQ(saved_rowset.end_version(), rs.end_version());
            EXPECT_EQ(saved_rowset.rowset_id_v2(), rs.rowset_id_v2());
        }
        for (int i = 3; i < 5; ++i) { // [14-14][15-15]
            auto [k, v] = it->next();
            ASSERT_TRUE(saved_rowset.ParseFromArray(v.data(), v.size()));
            EXPECT_EQ(saved_rowset.start_version(), existed_rowsets[i].start_version());
            EXPECT_EQ(saved_rowset.end_version(), existed_rowsets[i].end_version());
            EXPECT_EQ(saved_rowset.rowset_id_v2(), existed_rowsets[i].rowset_id_v2());
        }

        // check recycled rowsets
        auto recycl_rs_start = recycle_rowset_key({instance_id, new_tablet_id, ""});
        auto recycl_rs_end = recycle_rowset_key({instance_id, new_tablet_id, "\xff"});
        ASSERT_EQ(txn->get(recycl_rs_start, recycl_rs_end, &it), 0);
        // [11-11], [12-12], old[13-13]
        ASSERT_EQ(it->size(), 3);
        for (int i = 0; i < 3; ++i) {
            auto [k, v] = it->next();
            k.remove_prefix(1);
            std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
            decode_key(&k, &out);
            // 0x01 "recycle" ${instance_id} "rowset" ${tablet_id} ${rowset_id} -> RecycleRowsetPB
            const auto& rowset_id = std::get<std::string>(std::get<0>(out[4]));
            EXPECT_EQ(rowset_id, existed_rowsets[i].rowset_id_v2());
        }
    }
}

TEST(MetaServiceJobTest, RetrySchemaChangeJobTest) {
    auto meta_service = get_meta_service();
    meta_service->resource_mgr_.reset(); // Do not use resource manager

    auto sp = SyncPoint::get_instance();
    sp->clear_all_call_backs();
    sp->set_call_back("get_instance_id::pred", [](void* p) { *((bool*)p) = true; });
    sp->set_call_back("get_instance_id", [&](void* p) { *((std::string*)p) = instance_id; });
    sp->enable_processing();

    brpc::Controller cntl;

    int64_t table_id = 1;
    int64_t index_id = 2;
    int64_t partition_id = 3;
    int64_t tablet_id = 4;
    ASSERT_NO_FATAL_FAILURE(
            create_tablet(meta_service.get(), table_id, index_id, partition_id, tablet_id));
    int64_t new_tablet_id = 14;
    // start "job1" on BE1
    ASSERT_NO_FATAL_FAILURE(create_tablet(meta_service.get(), table_id, index_id, partition_id,
                                          new_tablet_id, true));
    ASSERT_NO_FATAL_FAILURE(start_schema_change_job(meta_service.get(), table_id, index_id,
                                                    partition_id, tablet_id, new_tablet_id, "job1",
                                                    "be1"));
    // provide existed rowsets
    std::vector<doris::RowsetMetaPB> existed_rowsets;
    for (int i = 0; i < 5; ++i) {
        existed_rowsets.push_back(create_rowset(new_tablet_id, i + 11, i + 11));
    }
    ASSERT_NO_FATAL_FAILURE(insert_rowsets(meta_service->txn_kv_.get(), table_id, index_id,
                                           partition_id, new_tablet_id, existed_rowsets));

    // FE canceled "job1" and starts "job2" on BE1, should preempt previous "job1"
    ASSERT_NO_FATAL_FAILURE(start_schema_change_job(meta_service.get(), table_id, index_id,
                                                    partition_id, tablet_id, new_tablet_id, "job2",
                                                    "be1"));
    // retry "job2" on BE1
    ASSERT_NO_FATAL_FAILURE(start_schema_change_job(meta_service.get(), table_id, index_id,
                                                    partition_id, tablet_id, new_tablet_id, "job2",
                                                    "be1"));
    // BE1 output_versions=[2-8][9-9][10-10][11-11]
    std::vector<doris::RowsetMetaPB> be1_output_rowsets;
    be1_output_rowsets.push_back(create_rowset(new_tablet_id, 2, 8));
    be1_output_rowsets.push_back(create_rowset(new_tablet_id, 9, 9));
    be1_output_rowsets.push_back(create_rowset(new_tablet_id, 10, 10));
    be1_output_rowsets.push_back(create_rowset(new_tablet_id, 11, 11));
    for (auto& rs : be1_output_rowsets) {
        CreateRowsetResponse res;
        commit_rowset(meta_service.get(), rs, res);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << rs.end_version();
    }

    // FE thinks BE1 is not alive and retries "job2" on BE2, should preempt "job2" created by BE1
    ASSERT_NO_FATAL_FAILURE(start_schema_change_job(meta_service.get(), table_id, index_id,
                                                    partition_id, tablet_id, new_tablet_id, "job2",
                                                    "be2"));
    // BE2 output_versions=[2-8][9-12][13-13]
    std::vector<doris::RowsetMetaPB> be2_output_rowsets;
    {
        CreateRowsetResponse res;
        // [2-8] has committed by BE1
        commit_rowset(meta_service.get(), create_rowset(new_tablet_id, 2, 8), res);
        ASSERT_EQ(res.status().code(), MetaServiceCode::ALREADY_EXISTED);
        ASSERT_TRUE(res.has_existed_rowset_meta());
        ASSERT_EQ(res.existed_rowset_meta().rowset_id_v2(), be1_output_rowsets[0].rowset_id_v2());
        be2_output_rowsets.push_back(res.existed_rowset_meta());
        res.Clear();
        be2_output_rowsets.push_back(create_rowset(new_tablet_id, 9, 12));
        commit_rowset(meta_service.get(), be2_output_rowsets.back(), res);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        res.Clear();
        be2_output_rowsets.push_back(create_rowset(new_tablet_id, 13, 13));
        commit_rowset(meta_service.get(), be2_output_rowsets.back(), res);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // BE1 commit job, but check initiator failed
    FinishTabletJobResponse res;
    finish_schema_change_job(meta_service.get(), table_id, index_id, partition_id, tablet_id,
                             new_tablet_id, "job2", "be1", be1_output_rowsets, res);
    ASSERT_NE(res.status().code(), MetaServiceCode::OK);
    ASSERT_NE(res.status().msg().find("unmatched job id or initiator"), std::string::npos);
    // BE2 commit job
    res.Clear();
    finish_schema_change_job(meta_service.get(), table_id, index_id, partition_id, tablet_id,
                             new_tablet_id, "job2", "be2", be2_output_rowsets, res);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(meta_service->txn_kv_->create_txn(&txn), 0);
    // check tablet state
    auto tablet_key =
            meta_tablet_key({instance_id, table_id, index_id, partition_id, new_tablet_id});
    std::string tablet_val;
    ASSERT_EQ(txn->get(tablet_key, &tablet_val), 0);
    doris::TabletMetaPB tablet_pb;
    ASSERT_TRUE(tablet_pb.ParseFromString(tablet_val));
    ASSERT_EQ(tablet_pb.tablet_state(), doris::TabletStatePB::PB_RUNNING);

    // check tablet stats
    auto tablet_stat_key =
            stats_tablet_key({instance_id, table_id, index_id, partition_id, new_tablet_id});
    std::string tablet_stat_val;
    ASSERT_EQ(txn->get(tablet_stat_key, &tablet_stat_val), 0);
    TabletStatsPB tablet_stat;
    ASSERT_TRUE(tablet_stat.ParseFromString(tablet_stat_val));
    // [0-1][2-8][9-12][13-13][14-14][15-15]
    EXPECT_EQ(tablet_stat.num_rows(), 500);
    EXPECT_EQ(tablet_stat.num_rowsets(), 6);
    EXPECT_EQ(tablet_stat.num_segments(), 5);
    EXPECT_EQ(tablet_stat.data_size(), 50000);

    // check visible rowsets
    std::unique_ptr<RangeGetIterator> it;
    auto rs_start = meta_rowset_key({instance_id, new_tablet_id, 0});
    auto rs_end = meta_rowset_key({instance_id, new_tablet_id, 100});
    ASSERT_EQ(txn->get(rs_start, rs_end, &it), 0);
    EXPECT_EQ(it->size(), 6);
    auto [k, v] = it->next();
    doris::RowsetMetaPB saved_rowset;
    ASSERT_TRUE(saved_rowset.ParseFromArray(v.data(), v.size()));
    EXPECT_EQ(saved_rowset.start_version(), 0);
    EXPECT_EQ(saved_rowset.end_version(), 1);
    for (auto& rs : be2_output_rowsets) {
        auto [k, v] = it->next();
        ASSERT_TRUE(saved_rowset.ParseFromArray(v.data(), v.size()));
        EXPECT_EQ(saved_rowset.start_version(), rs.start_version());
        EXPECT_EQ(saved_rowset.end_version(), rs.end_version());
        EXPECT_EQ(saved_rowset.rowset_id_v2(), rs.rowset_id_v2());
    }
    for (int i = 3; i < 5; ++i) { // [14-14][15-15]
        auto [k, v] = it->next();
        ASSERT_TRUE(saved_rowset.ParseFromArray(v.data(), v.size()));
        EXPECT_EQ(saved_rowset.start_version(), existed_rowsets[i].start_version());
        EXPECT_EQ(saved_rowset.end_version(), existed_rowsets[i].end_version());
        EXPECT_EQ(saved_rowset.rowset_id_v2(), existed_rowsets[i].rowset_id_v2());
    }

    // check recycled rowsets
    auto recycl_rs_start = recycle_rowset_key({instance_id, new_tablet_id, ""});
    auto recycl_rs_end = recycle_rowset_key({instance_id, new_tablet_id, "\xff"});
    ASSERT_EQ(txn->get(recycl_rs_start, recycl_rs_end, &it), 0);
    // [11-11], [12-12], old[13-13]
    ASSERT_EQ(it->size(), 3);
    for (int i = 0; i < 3; ++i) {
        auto [k, v] = it->next();
        k.remove_prefix(1);
        std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
        decode_key(&k, &out);
        // 0x01 "recycle" ${instance_id} "rowset" ${tablet_id} ${rowset_id} -> RecycleRowsetPB
        const auto& rowset_id = std::get<std::string>(std::get<0>(out[4]));
        EXPECT_EQ(rowset_id, existed_rowsets[i].rowset_id_v2());
    }
}

TEST(MetaServiceJobTest, ConcurrentCompactionTest) {
    auto meta_service = get_meta_service();
    meta_service->resource_mgr_.reset(); // Do not use resource manager

    auto sp = SyncPoint::get_instance();
    sp->clear_all_call_backs();
    sp->set_call_back("get_instance_id::pred", [](void* p) { *((bool*)p) = true; });
    sp->set_call_back("get_instance_id", [&](void* p) { *((std::string*)p) = instance_id; });
    sp->enable_processing();

    brpc::Controller cntl;

    int64_t table_id = 1;
    int64_t index_id = 2;
    int64_t partition_id = 3;
    int64_t tablet_id = 4;
    ASSERT_NO_FATAL_FAILURE(
            create_tablet(meta_service.get(), table_id, index_id, partition_id, tablet_id));

    {
        StartTabletJobRequest req;
        StartTabletJobResponse res;
        auto compaction = req.mutable_job()->add_compaction();
        compaction->set_id("job1");
        compaction->set_initiator("BE1");
        compaction->set_type(TabletCompactionJobPB::CUMULATIVE);
        req.mutable_job()->mutable_idx()->set_tablet_id(tablet_id);
        compaction->set_base_compaction_cnt(0);
        compaction->set_cumulative_compaction_cnt(0);
        meta_service->start_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                       &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    {
        StartTabletJobRequest req;
        StartTabletJobResponse res;
        auto compaction = req.mutable_job()->add_compaction();
        compaction->set_id("job2");
        compaction->set_initiator("BE1");
        compaction->set_type(TabletCompactionJobPB::CUMULATIVE);
        req.mutable_job()->mutable_idx()->set_tablet_id(tablet_id);
        compaction->set_base_compaction_cnt(0);
        compaction->set_cumulative_compaction_cnt(0);
        meta_service->start_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                       &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK); // preempt
    }

    {
        StartTabletJobRequest req;
        StartTabletJobResponse res;
        auto compaction = req.mutable_job()->add_compaction();
        compaction->set_id("job3");
        compaction->set_initiator("BE2");
        compaction->set_type(TabletCompactionJobPB::CUMULATIVE);
        req.mutable_job()->mutable_idx()->set_tablet_id(tablet_id);
        compaction->set_base_compaction_cnt(0);
        compaction->set_cumulative_compaction_cnt(0);
        meta_service->start_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                       &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::JOB_TABLET_BUSY);
    }

    {
        StartTabletJobRequest req;
        StartTabletJobResponse res;
        auto compaction = req.mutable_job()->add_compaction();
        compaction->set_id("job4");
        compaction->set_initiator("BE1");
        compaction->set_type(TabletCompactionJobPB::BASE);
        req.mutable_job()->mutable_idx()->set_tablet_id(tablet_id);
        compaction->set_base_compaction_cnt(0);
        compaction->set_cumulative_compaction_cnt(0);
        meta_service->start_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                       &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // check job kv
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(meta_service->txn_kv_->create_txn(&txn), 0);
    std::string job_key =
            job_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id});
    std::string job_val;
    TabletJobInfoPB job_pb;
    ASSERT_EQ(txn->get(job_key, &job_val), 0);
    ASSERT_TRUE(job_pb.ParseFromString(job_val));
    ASSERT_EQ(job_pb.compaction(0).id(), "job2");
    ASSERT_EQ(job_pb.compaction(0).initiator(), "BE1");
    ASSERT_EQ(job_pb.compaction(1).id(), "job4");
    ASSERT_EQ(job_pb.compaction(1).initiator(), "BE1");

    {
        FinishTabletJobRequest req;
        FinishTabletJobResponse res;
        req.set_action(FinishTabletJobRequest::ABORT);
        auto compaction = req.mutable_job()->add_compaction();
        compaction->set_id("job2");
        compaction->set_initiator("BE1");
        compaction->set_type(TabletCompactionJobPB::CUMULATIVE);
        req.mutable_job()->mutable_idx()->set_tablet_id(tablet_id);
        meta_service->finish_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                        &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        meta_service->finish_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                        &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
        ASSERT_NE(res.status().msg().find("unmatched job id"), std::string::npos);
    }

    {
        FinishTabletJobRequest req;
        FinishTabletJobResponse res;
        req.set_action(FinishTabletJobRequest::LEASE);
        auto compaction = req.mutable_job()->add_compaction();
        compaction->set_id("job4");
        compaction->set_initiator("BE1");
        compaction->set_lease(12345);
        req.mutable_job()->mutable_idx()->set_tablet_id(tablet_id);
        meta_service->finish_tablet_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                        &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    ASSERT_EQ(meta_service->txn_kv_->create_txn(&txn), 0);
    ASSERT_EQ(txn->get(job_key, &job_val), 0);
    ASSERT_TRUE(job_pb.ParseFromString(job_val));
    ASSERT_EQ(job_pb.compaction(0).id(), "job4");
    ASSERT_EQ(job_pb.compaction(0).lease(), 12345);
}

} // namespace selectdb
