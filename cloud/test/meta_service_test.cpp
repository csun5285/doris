
// clang-format off
#include "meta-service/meta_service.h"
#include <bvar/window.h>
#include <google/protobuf/repeated_field.h>

#include "common/config.h"
#include "common/logging.h"
#include "common/sync_point.h"
#include "common/util.h"
#include "gen_cpp/olap_file.pb.h"
#include "gen_cpp/selectdb_cloud.pb.h"
#include "meta-service/keys.h"
#include "meta-service/mem_txn_kv.h"
#include "rate-limiter/rate_limiter.h"
#include "resource-manager/resource_manager.h"
#include "mock_resource_manager.h"

#include "brpc/controller.h"
#include "gtest/gtest.h"

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <random>
#include <thread>
// clang-format on

int main(int argc, char** argv) {
    const std::string conf_file = "selectdb_cloud.conf";
    if (!selectdb::config::init(conf_file.c_str(), true)) {
        std::cerr << "failed to init config file, conf=" << conf_file << std::endl;
        return -1;
    }

    if (!selectdb::init_glog("meta_service_test")) {
        std::cerr << "failed to init glog" << std::endl;
        return -1;
    }
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace selectdb {

std::unique_ptr<MetaServiceImpl> get_meta_service(bool mock_resource_mgr) {
    int ret = 0;
    // MemKv
    auto txn_kv = std::dynamic_pointer_cast<TxnKv>(std::make_shared<MemTxnKv>());
    if (txn_kv != nullptr) {
        ret = txn_kv->init();
        [&] { ASSERT_EQ(ret, 0); }();
    }
    [&] { ASSERT_NE(txn_kv.get(), nullptr); }();

    // FdbKv
    //     config::fdb_cluster_file_path = "fdb.cluster";
    //     static auto txn_kv = std::dynamic_pointer_cast<TxnKv>(std::make_shared<FdbTxnKv>());
    //     static std::atomic<bool> init {false};
    //     bool tmp = false;
    //     if (init.compare_exchange_strong(tmp, true)) {
    //         int ret = txn_kv->init();
    //         [&] { ASSERT_EQ(ret, 0); ASSERT_NE(txn_kv.get(), nullptr); }();
    //     }

    std::unique_ptr<Transaction> txn;
    txn_kv->create_txn(&txn);
    txn->remove("\x00", "\xfe"); // This is dangerous if the fdb is not correctly set
    txn->commit();

    auto rs = mock_resource_mgr ? std::make_shared<MockResourceManager>(txn_kv)
                                : std::make_shared<ResourceManager>(txn_kv);
    auto rl = std::make_shared<RateLimiter>();
    auto meta_service = std::make_unique<MetaServiceImpl>(txn_kv, rs, rl);
    return meta_service;
}

std::unique_ptr<MetaServiceImpl> get_meta_service() {
    return get_meta_service(true);
}

static std::string next_rowset_id() {
    static int cnt = 0;
    return std::to_string(++cnt);
}

static void add_tablet(CreateTabletsRequest& req, int64_t table_id, int64_t index_id,
                       int64_t partition_id, int64_t tablet_id) {
    auto tablet = req.add_tablet_metas();
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
}

static void create_tablet(MetaServiceImpl* meta_service, int64_t table_id, int64_t index_id,
                          int64_t partition_id, int64_t tablet_id) {
    brpc::Controller cntl;
    CreateTabletsRequest req;
    CreateTabletsResponse res;
    add_tablet(req, table_id, index_id, partition_id, tablet_id);
    meta_service->create_tablets(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << tablet_id;
}

static void begin_txn(MetaServiceImpl* meta_service, int64_t db_id, const std::string& label,
                      int64_t table_id, int64_t& txn_id) {
    brpc::Controller cntl;
    BeginTxnRequest req;
    BeginTxnResponse res;
    auto txn_info = req.mutable_txn_info();
    txn_info->set_db_id(db_id);
    txn_info->set_label(label);
    txn_info->add_table_ids(table_id);
    txn_info->set_timeout_ms(36000);
    meta_service->begin_txn(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << label;
    ASSERT_TRUE(res.has_txn_id()) << label;
    txn_id = res.txn_id();
}

static void commit_txn(MetaServiceImpl* meta_service, int64_t db_id, int64_t txn_id,
                       const std::string& label) {
    brpc::Controller cntl;
    CommitTxnRequest req;
    CommitTxnResponse res;
    req.set_db_id(db_id);
    req.set_txn_id(txn_id);
    meta_service->commit_txn(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << label;
}

static doris::RowsetMetaPB create_rowset(int64_t txn_id, int64_t tablet_id, int partition_id = 0,
                                         int64_t version = -1, int num_rows = 100) {
    doris::RowsetMetaPB rowset;
    rowset.set_rowset_id(0); // required
    rowset.set_rowset_id_v2(next_rowset_id());
    rowset.set_tablet_id(tablet_id);
    rowset.set_partition_id(partition_id);
    rowset.set_txn_id(txn_id);
    if (version > 0) {
        rowset.set_start_version(version);
        rowset.set_end_version(version);
    }
    rowset.set_num_segments(1);
    rowset.set_num_rows(num_rows);
    rowset.set_data_disk_size(num_rows * 100);
    rowset.mutable_tablet_schema()->set_schema_version(0);
    rowset.set_txn_expiration(::time(nullptr)); // Required by DCHECK
    return rowset;
}

static void prepare_rowset(MetaServiceImpl* meta_service, const doris::RowsetMetaPB& rowset,
                           CreateRowsetResponse& res) {
    brpc::Controller cntl;
    auto arena = res.GetArena();
    auto req = google::protobuf::Arena::CreateMessage<CreateRowsetRequest>(arena);
    req->set_temporary(true);
    req->mutable_rowset_meta()->CopyFrom(rowset);
    meta_service->prepare_rowset(&cntl, req, &res, nullptr);
    if (!arena) delete req;
}

static void commit_rowset(MetaServiceImpl* meta_service, const doris::RowsetMetaPB& rowset,
                          CreateRowsetResponse& res) {
    brpc::Controller cntl;
    auto arena = res.GetArena();
    auto req = google::protobuf::Arena::CreateMessage<CreateRowsetRequest>(arena);
    req->set_temporary(true);
    req->mutable_rowset_meta()->CopyFrom(rowset);
    meta_service->commit_rowset(&cntl, req, &res, nullptr);
    if (!arena) delete req;
}

static void insert_rowset(MetaServiceImpl* meta_service, int64_t db_id, const std::string& label,
                          int64_t table_id, int64_t partition_id, int64_t tablet_id) {
    int64_t txn_id = 0;
    ASSERT_NO_FATAL_FAILURE(begin_txn(meta_service, db_id, label, table_id, txn_id));
    CreateRowsetResponse res;
    auto rowset = create_rowset(txn_id, tablet_id, partition_id);
    prepare_rowset(meta_service, rowset, res);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << label;
    res.Clear();
    ASSERT_NO_FATAL_FAILURE(commit_rowset(meta_service, rowset, res));
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << label;
    commit_txn(meta_service, db_id, txn_id, label);
}

TEST(MetaServiceTest, GetInstanceIdTest) {
    extern std::string get_instance_id(const std::shared_ptr<ResourceManager>& rc_mgr,
                                       const std::string& cloud_unique_id);
    auto meta_service = get_meta_service();
    auto sp = SyncPoint::get_instance();
    std::unique_ptr<int, std::function<void(int*)>> defer(
            (int*)0x01, [](int*) { SyncPoint::get_instance()->clear_all_call_backs(); });
    sp->set_call_back("get_instance_id_err", [&](void* args) {
        std::string* err = reinterpret_cast<std::string*>(args);
        *err = "can't find node from cache";
    });
    sp->enable_processing();

    auto instance_id =
            get_instance_id(meta_service->resource_mgr_, "1:ALBJLH4Q:m-n3qdpyal27rh8iprxx");
    ASSERT_EQ(instance_id, "ALBJLH4Q");

    // version not support
    instance_id = get_instance_id(meta_service->resource_mgr_, "2:ALBJLH4Q:m-n3qdpyal27rh8iprxx");
    ASSERT_EQ(instance_id, "");

    // degraded format err
    instance_id = get_instance_id(meta_service->resource_mgr_, "1:ALBJLH4Q");
    ASSERT_EQ(instance_id, "");

    // std::invalid_argument
    instance_id = get_instance_id(meta_service->resource_mgr_,
                                  "invalid_version:ALBJLH4Q:m-n3qdpyal27rh8iprxx");
    ASSERT_EQ(instance_id, "");

    // std::out_of_range
    instance_id = get_instance_id(meta_service->resource_mgr_,
                                  "12345678901:ALBJLH4Q:m-n3qdpyal27rh8iprxx");
    ASSERT_EQ(instance_id, "");

    sp->clear_all_call_backs();
    sp->clear_trace();
    sp->disable_processing();
}

TEST(MetaServiceTest, CreateInstanceTest) {
    auto meta_service = get_meta_service();

    // case: normal create instance
    {
        brpc::Controller cntl;
        CreateInstanceRequest req;
        req.set_instance_id("test_instance");
        req.set_user_id("test_user");
        req.set_name("test_name");
        ObjectStoreInfoPB obj;
        obj.set_ak("123");
        obj.set_sk("321");
        obj.set_bucket("456");
        obj.set_prefix("654");
        obj.set_endpoint("789");
        obj.set_region("987");
        obj.set_external_endpoint("888");
        obj.set_provider(ObjectStoreInfoPB::BOS);
        req.mutable_obj_info()->CopyFrom(obj);

        auto sp = SyncPoint::get_instance();
        sp->set_call_back("encrypt_ak_sk:get_encryption_key_ret",
                          [](void* p) { *reinterpret_cast<int*>(p) = 0; });
        sp->set_call_back("encrypt_ak_sk:get_encryption_key",
                          [](void* p) { *reinterpret_cast<std::string*>(p) = "test"; });
        sp->set_call_back("encrypt_ak_sk:get_encryption_key_id",
                          [](void* p) { *reinterpret_cast<int*>(p) = 1; });
        sp->enable_processing();
        CreateInstanceResponse res;
        meta_service->create_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        sp->clear_all_call_backs();
        sp->clear_trace();
        sp->disable_processing();
    }

    // case: request has invalid argument
    {
        brpc::Controller cntl;
        CreateInstanceRequest req;
        CreateInstanceResponse res;
        meta_service->create_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    }

    // case: normal drop instance
    {
        brpc::Controller cntl;
        AlterInstanceRequest req;
        AlterInstanceResponse res;
        req.set_op(AlterInstanceRequest::DROP);
        req.set_instance_id("test_instance");
        meta_service->alter_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        InstanceKeyInfo key_info {"test_instance"};
        std::string key;
        std::string val;
        instance_key(key_info, &key);
        std::unique_ptr<Transaction> txn;
        meta_service->txn_kv_->create_txn(&txn);
        txn->get(key, &val);
        InstanceInfoPB instance;
        instance.ParseFromString(val);
        ASSERT_EQ(instance.status(), InstanceInfoPB::DELETED);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // case: normal refresh instance
    {
        brpc::Controller cntl;
        AlterInstanceRequest req;
        AlterInstanceResponse res;
        req.set_op(AlterInstanceRequest::REFRESH);
        req.set_instance_id("test_instance");
        meta_service->alter_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }
}

TEST(MetaServiceTest, AlterClusterTest) {
    auto meta_service = get_meta_service();
    ASSERT_NE(meta_service, nullptr);

    // case: normal add cluster
    {
        brpc::Controller cntl;
        AlterClusterRequest req;
        req.set_instance_id(mock_instance);
        req.mutable_cluster()->set_cluster_name(mock_cluster_name);
        req.set_op(AlterClusterRequest::ADD_CLUSTER);
        AlterClusterResponse res;
        meta_service->alter_cluster(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // case: request has invalid argument
    {
        brpc::Controller cntl;
        AlterClusterRequest req;
        req.set_op(AlterClusterRequest::DROP_CLUSTER);
        AlterClusterResponse res;
        meta_service->alter_cluster(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    }

    // add node
    {
        brpc::Controller cntl;
        AlterClusterRequest req;
        req.set_instance_id(mock_instance);
        req.set_op(AlterClusterRequest::ADD_NODE);
        req.mutable_cluster()->set_cluster_name(mock_cluster_name);
        req.mutable_cluster()->set_cluster_id(mock_cluster_id);
        req.mutable_cluster()->set_type(ClusterPB::COMPUTE);
        auto node = req.mutable_cluster()->add_nodes();
        node->set_ip("127.0.0.1");
        node->set_heartbeat_port(9999);
        AlterClusterResponse res;
        meta_service->alter_cluster(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // drop node
    {
        brpc::Controller cntl;
        AlterClusterRequest req;
        req.set_instance_id(mock_instance);
        req.set_op(AlterClusterRequest::DROP_NODE);
        req.mutable_cluster()->set_cluster_name(mock_cluster_name);
        req.mutable_cluster()->set_cluster_id(mock_cluster_id);
        req.mutable_cluster()->set_type(ClusterPB::COMPUTE);
        auto node = req.mutable_cluster()->add_nodes();
        node->set_ip("127.0.0.1");
        node->set_heartbeat_port(9999);
        AlterClusterResponse res;
        meta_service->alter_cluster(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // rename cluster
    {
        brpc::Controller cntl;
        AlterClusterRequest req;
        req.set_instance_id(mock_instance);
        req.mutable_cluster()->set_cluster_id(mock_cluster_id);
        req.mutable_cluster()->set_cluster_name("rename_cluster_name");
        req.set_op(AlterClusterRequest::RENAME_CLUSTER);
        AlterClusterResponse res;
        meta_service->alter_cluster(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // set cluster status
    {
        brpc::Controller cntl;
        AlterClusterRequest req;
        req.set_instance_id(mock_instance);
        req.mutable_cluster()->set_cluster_id(mock_cluster_id);
        req.mutable_cluster()->set_cluster_status(ClusterStatus::SUSPENDED);
        req.set_op(AlterClusterRequest::SET_CLUSTER_STATUS);
        AlterClusterResponse res;
        meta_service->alter_cluster(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }
}

TEST(MetaServiceTest, GetClusterTest) {
    auto meta_service = get_meta_service();

    // add cluster first
    InstanceKeyInfo key_info {mock_instance};
    std::string key;
    std::string val;
    instance_key(key_info, &key);

    InstanceInfoPB instance;
    instance.set_instance_id(mock_instance);
    ClusterPB c1;
    c1.set_cluster_name(mock_cluster_name);
    c1.set_cluster_id(mock_cluster_id);
    c1.add_mysql_user_name()->append("m1");
    instance.add_clusters()->CopyFrom(c1);
    val = instance.SerializeAsString();

    std::unique_ptr<Transaction> txn;
    std::string get_val;
    int ret = meta_service->txn_kv_->create_txn(&txn);
    ASSERT_EQ(ret, 0);
    txn->put(key, val);
    ASSERT_EQ(txn->commit(), 0);

    // case: normal get
    {
        brpc::Controller cntl;
        GetClusterRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_cluster_id(mock_cluster_id);
        req.set_cluster_name("test_cluster");
        GetClusterResponse res;
        meta_service->get_cluster(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                  &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }
}

TEST(MetaServiceTest, BeginTxnTest) {
    auto meta_service = get_meta_service();
    int64_t db_id = 666;
    int64_t table_id = 123;
    const std::string& label = "test_label";
    int64_t timeout_ms = 60 * 1000;

    // test invalid argument
    {
        brpc::Controller cntl;
        BeginTxnRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        BeginTxnResponse res;
        meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    }

    {
        brpc::Controller cntl;
        BeginTxnRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");

        TxnInfoPB txn_info;
        txn_info.set_db_id(db_id);
        txn_info.add_table_ids(table_id);
        txn_info.set_timeout_ms(timeout_ms);
        req.mutable_txn_info()->CopyFrom(txn_info);
        BeginTxnResponse res;
        meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    }

    {
        brpc::Controller cntl;
        BeginTxnRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");

        TxnInfoPB txn_info;
        txn_info.set_db_id(db_id);
        txn_info.set_label(label);
        txn_info.add_table_ids(table_id);
        txn_info.set_timeout_ms(timeout_ms);
        req.mutable_txn_info()->CopyFrom(txn_info);
        BeginTxnResponse res;
        meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // case: label already used
    {
        brpc::Controller cntl;
        BeginTxnRequest req;

        req.set_cloud_unique_id("test_cloud_unique_id");
        TxnInfoPB txn_info;
        txn_info.set_db_id(888);
        txn_info.set_label("test_label_already_in_use");
        txn_info.add_table_ids(456);
        txn_info.set_timeout_ms(36000);
        req.mutable_txn_info()->CopyFrom(txn_info);
        BeginTxnResponse res;
        meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

        meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::TXN_LABEL_ALREADY_USED);
    }

    // case: dup begin txn request
    {
        brpc::Controller cntl;
        BeginTxnRequest req;

        req.set_cloud_unique_id("test_cloud_unique_id");
        TxnInfoPB txn_info;
        txn_info.set_db_id(999);
        txn_info.set_label("test_label_dup_request");
        txn_info.add_table_ids(789);
        UniqueIdPB unique_id_pb;
        unique_id_pb.set_hi(100);
        unique_id_pb.set_lo(10);
        txn_info.mutable_request_id()->CopyFrom(unique_id_pb);
        txn_info.set_timeout_ms(36000);
        req.mutable_txn_info()->CopyFrom(txn_info);
        BeginTxnResponse res;
        meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

        meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::TXN_DUPLICATED_REQ);
    }

    {
        // ===========================================================================
        // threads concurrent execution with sequence in begin_txn with same label:
        //
        //      thread1              thread2
        //         |                    |
        //         |                commit_txn1
        //         |                    |
        //         |                    |
        //         |                    |
        //       commit_txn2            |
        //         |                    |
        //         v                    v
        //

        std::mutex go_mutex;
        std::condition_variable go_cv;
        bool go = false;
        auto sp = selectdb::SyncPoint::get_instance();
        std::unique_ptr<int, std::function<void(int*)>> defer(
                (int*)0x01, [](int*) { SyncPoint::get_instance()->clear_all_call_backs(); });

        std::atomic<int32_t> count_txn1 = {0};
        std::atomic<int32_t> count_txn2 = {0};
        std::atomic<int32_t> count_txn3 = {0};

        int64_t db_id = 1928354123;
        int64_t table_id = 12131231231;
        std::string test_label = "test_race_with_same_label";

        std::atomic<int32_t> success_txn = {0};

        sp->set_call_back("begin_txn:before:commit_txn:1", [&](void* args) {
            std::string label = *reinterpret_cast<std::string*>(args);
            std::unique_lock<std::mutex> _lock(go_mutex);
            count_txn1++;
            LOG(INFO) << "count_txn1:" << count_txn1 << " label=" << label;
            if (count_txn1 == 1) {
                {
                    LOG(INFO) << "count_txn1:" << count_txn1 << " label=" << label << " go=" << go;
                    go_cv.wait(_lock);
                }
            }

            if (count_txn1 == 2) {
                {
                    LOG(INFO) << "count_txn1:" << count_txn1 << " label=" << label << " go=" << go;
                    go_cv.notify_all();
                }
            }
        });

        sp->set_call_back("begin_txn:after:commit_txn:1", [&](void* args) {
            std::string label = *reinterpret_cast<std::string*>(args);
            std::unique_lock<std::mutex> _lock(go_mutex);
            count_txn2++;
            LOG(INFO) << "count_txn2:" << count_txn2 << " label=" << label;
            if (count_txn2 == 1) {
                {
                    LOG(INFO) << "count_txn2:" << count_txn2 << " label=" << label << " go=" << go;
                    go_cv.wait(_lock);
                }
            }

            if (count_txn2 == 2) {
                {
                    LOG(INFO) << "count_txn2:" << count_txn2 << " label=" << label << " go=" << go;
                    go_cv.notify_all();
                }
            }
        });

        sp->set_call_back("begin_txn:after:commit_txn:2", [&](void* args) {
            int64_t txn_id = *reinterpret_cast<int64_t*>(args);
            count_txn3++;
            LOG(INFO) << "count_txn3:" << count_txn3 << " txn_id=" << txn_id;
        });

        sp->enable_processing();

        std::thread thread1([&] {
            {
                std::unique_lock<std::mutex> _lock(go_mutex);
                go_cv.wait(_lock, [&] { return go; });
            }
            brpc::Controller cntl;
            BeginTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            TxnInfoPB txn_info;
            txn_info.set_db_id(db_id);
            txn_info.set_label(test_label);
            txn_info.add_table_ids(table_id);
            UniqueIdPB unique_id_pb;
            unique_id_pb.set_hi(1001);
            unique_id_pb.set_lo(11);
            txn_info.mutable_request_id()->CopyFrom(unique_id_pb);
            txn_info.set_timeout_ms(36000);
            req.mutable_txn_info()->CopyFrom(txn_info);
            BeginTxnResponse res;
            meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            if (res.status().code() == MetaServiceCode::OK) {
                success_txn++;
            } else {
                ASSERT_EQ(res.status().code(), MetaServiceCode::KV_TXN_CONFLICT);
            }
        });

        std::thread thread2([&] {
            {
                std::unique_lock<std::mutex> _lock(go_mutex);
                go_cv.wait(_lock, [&] { return go; });
            }
            brpc::Controller cntl;
            BeginTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            TxnInfoPB txn_info;
            txn_info.set_db_id(db_id);
            txn_info.set_label(test_label);
            txn_info.add_table_ids(table_id);
            UniqueIdPB unique_id_pb;
            unique_id_pb.set_hi(100);
            unique_id_pb.set_lo(10);
            txn_info.mutable_request_id()->CopyFrom(unique_id_pb);
            txn_info.set_timeout_ms(36000);
            req.mutable_txn_info()->CopyFrom(txn_info);
            BeginTxnResponse res;
            meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            if (res.status().code() == MetaServiceCode::OK) {
                success_txn++;
            } else {
                ASSERT_EQ(res.status().code(), MetaServiceCode::KV_TXN_CONFLICT);
            }
        });

        std::unique_lock<std::mutex> go_lock(go_mutex);
        go = true;
        go_lock.unlock();
        go_cv.notify_all();

        thread1.join();
        thread2.join();
        sp->clear_all_call_backs();
        sp->clear_trace();
        sp->disable_processing();
        ASSERT_EQ(success_txn.load(), 1);
    }
    {
        // ===========================================================================
        // threads concurrent execution with sequence in begin_txn with different label:
        //
        //      thread1              thread2
        //         |                    |
        //         |                commit_txn1
        //         |                    |
        //         |                    |
        //         |                    |
        //       commit_txn2            |
        //         |                    |
        //         v                    v

        std::mutex go_mutex;
        std::condition_variable go_cv;
        bool go = false;
        auto sp = selectdb::SyncPoint::get_instance();
        std::unique_ptr<int, std::function<void(int*)>> defer(
                (int*)0x01, [](int*) { SyncPoint::get_instance()->clear_all_call_backs(); });

        std::atomic<int32_t> count_txn1 = {0};
        std::atomic<int32_t> count_txn2 = {0};
        std::mutex flow_mutex_1;
        std::condition_variable flow_cv_1;

        int64_t db_id = 19541231112;
        int64_t table_id = 312312321211;
        std::string test_label1 = "test_race_with_diff_label1";
        std::string test_label2 = "test_race_with_diff_label2";

        std::atomic<int32_t> success_txn = {0};

        sp->set_call_back("begin_txn:before:commit_txn:1", [&](void* args) {
            std::string label = *reinterpret_cast<std::string*>(args);
            if (count_txn1.load() == 1) {
                std::unique_lock<std::mutex> flow_lock_1(flow_mutex_1);
                flow_cv_1.wait(flow_lock_1);
            }
            count_txn1++;
            LOG(INFO) << "count_txn1:" << count_txn1 << " label=" << label;
        });

        sp->set_call_back("begin_txn:after:commit_txn:2", [&](void* args) {
            int64_t txn_id = *reinterpret_cast<int64_t*>(args);
            while (count_txn2.load() == 0 && count_txn1.load() == 1) {
                sleep(1);
                flow_cv_1.notify_all();
            }
            count_txn2++;
            LOG(INFO) << "count_txn2:" << count_txn2 << " txn_id=" << txn_id;
        });
        sp->enable_processing();

        std::thread thread1([&] {
            {
                std::unique_lock<std::mutex> _lock(go_mutex);
                go_cv.wait(_lock, [&] { return go; });
            }
            brpc::Controller cntl;
            BeginTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            TxnInfoPB txn_info;
            txn_info.set_db_id(db_id);
            txn_info.set_label(test_label1);
            txn_info.add_table_ids(table_id);
            UniqueIdPB unique_id_pb;
            unique_id_pb.set_hi(1001);
            unique_id_pb.set_lo(11);
            txn_info.mutable_request_id()->CopyFrom(unique_id_pb);
            txn_info.set_timeout_ms(36000);
            req.mutable_txn_info()->CopyFrom(txn_info);
            BeginTxnResponse res;
            meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            if (res.status().code() == MetaServiceCode::OK) {
                success_txn++;
            } else {
                ASSERT_EQ(res.status().code(), MetaServiceCode::TXN_LABEL_ALREADY_USED);
            }
        });

        std::thread thread2([&] {
            {
                std::unique_lock<std::mutex> _lock(go_mutex);
                go_cv.wait(_lock, [&] { return go; });
            }
            brpc::Controller cntl;
            BeginTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            TxnInfoPB txn_info;
            txn_info.set_db_id(db_id);
            txn_info.set_label(test_label2);
            txn_info.add_table_ids(table_id);
            txn_info.set_timeout_ms(36000);
            UniqueIdPB unique_id_pb;
            unique_id_pb.set_hi(100);
            unique_id_pb.set_lo(10);
            txn_info.mutable_request_id()->CopyFrom(unique_id_pb);
            req.mutable_txn_info()->CopyFrom(txn_info);
            BeginTxnResponse res;
            meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            if (res.status().code() == MetaServiceCode::OK) {
                success_txn++;
            } else {
                ASSERT_EQ(res.status().code(), MetaServiceCode::TXN_LABEL_ALREADY_USED);
            }
        });

        std::unique_lock<std::mutex> go_lock(go_mutex);
        go = true;
        go_lock.unlock();
        go_cv.notify_all();

        thread1.join();
        thread2.join();
        sp->clear_all_call_backs();
        sp->clear_trace();
        sp->disable_processing();
        ASSERT_EQ(success_txn.load(), 2);
    }
    {
        // test reuse label
        // 1. beigin_txn
        // 2. abort_txn
        // 3. begin_txn again can successfully

        std::string cloud_unique_id = "test_cloud_unique_id";
        int64_t db_id = 124343989;
        int64_t table_id = 1231311;
        int64_t txn_id = -1;
        std::string label = "test_reuse_label";
        {
            brpc::Controller cntl;
            BeginTxnRequest req;
            req.set_cloud_unique_id(cloud_unique_id);
            TxnInfoPB txn_info;
            txn_info.set_db_id(db_id);
            txn_info.set_label(label);
            txn_info.add_table_ids(table_id);
            txn_info.set_timeout_ms(36000);
            UniqueIdPB unique_id_pb;
            unique_id_pb.set_hi(100);
            unique_id_pb.set_lo(10);
            txn_info.mutable_request_id()->CopyFrom(unique_id_pb);
            req.mutable_txn_info()->CopyFrom(txn_info);
            BeginTxnResponse res;
            meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            txn_id = res.txn_id();
        }
        // abort txn
        {
            brpc::Controller cntl;
            AbortTxnRequest req;
            req.set_cloud_unique_id(cloud_unique_id);
            ASSERT_GT(txn_id, 0);
            req.set_txn_id(txn_id);
            req.set_reason("test");
            AbortTxnResponse res;
            meta_service->abort_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            ASSERT_EQ(res.txn_info().status(), TxnStatusPB::TXN_STATUS_ABORTED);
        }
        {
            brpc::Controller cntl;
            BeginTxnRequest req;
            req.set_cloud_unique_id(cloud_unique_id);
            TxnInfoPB txn_info;
            txn_info.set_db_id(db_id);
            txn_info.set_label(label);
            txn_info.add_table_ids(table_id);
            UniqueIdPB unique_id_pb;
            unique_id_pb.set_hi(100);
            unique_id_pb.set_lo(10);
            txn_info.mutable_request_id()->CopyFrom(unique_id_pb);
            txn_info.set_timeout_ms(36000);
            req.mutable_txn_info()->CopyFrom(txn_info);
            BeginTxnResponse res;
            meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            ASSERT_GT(res.txn_id(), txn_id);
        }
    }
}

TEST(MetaServiceTest, PrecommitTest1) {
    // PrecommitTestCase1: only use db_id for precommit_txn
    auto meta_service = get_meta_service();
    const int64_t db_id = 563413;
    const int64_t table_id = 417417878;
    const std::string& label = "label_123dae121das";
    int64_t txn_id = -1;
    {
        brpc::Controller cntl;
        BeginTxnRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        TxnInfoPB txn_info;
        txn_info.set_db_id(db_id);
        txn_info.set_label(label);
        txn_info.add_table_ids(table_id);
        txn_info.set_timeout_ms(36000);
        req.mutable_txn_info()->CopyFrom(txn_info);
        BeginTxnResponse res;
        meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        txn_id = res.txn_id();
        ASSERT_GT(txn_id, -1);
    }

    {
        brpc::Controller cntl;
        PrecommitTxnRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_precommit_timeout_ms(36000);
        PrecommitTxnResponse res;
        meta_service->precommit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    }

    {
        std::unique_ptr<Transaction> txn;
        int ret = meta_service->txn_kv_->create_txn(&txn);
        ASSERT_EQ(ret, 0);

        const std::string info_key = txn_info_key({mock_instance, db_id, txn_id});
        std::string info_val;
        ASSERT_EQ(txn->get(info_key, &info_val), 0);
        TxnInfoPB txn_info;
        txn_info.ParseFromString(info_val);
        ASSERT_EQ(txn_info.status(), TxnStatusPB::TXN_STATUS_PREPARED);

        brpc::Controller cntl;
        PrecommitTxnRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_txn_id(txn_id);
        req.set_precommit_timeout_ms(36000);
        PrecommitTxnResponse res;
        meta_service->precommit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

        ret = meta_service->txn_kv_->create_txn(&txn);
        ASSERT_EQ(ret, 0);
        ASSERT_EQ(txn->get(info_key, &info_val), 0);
        txn_info.ParseFromString(info_val);
        ASSERT_EQ(txn_info.status(), TxnStatusPB::TXN_STATUS_PRECOMMITTED);
    }
}

TEST(MetaServiceTest, PrecommitTxnTest2) {
    auto meta_service = get_meta_service();
    const int64_t db_id = 563413;
    const int64_t table_id = 417417878;
    const std::string& label = "label_123dae121das";
    int64_t txn_id = -1;
    // begin txn first
    {
        brpc::Controller cntl;
        BeginTxnRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        TxnInfoPB txn_info;
        txn_info.set_db_id(db_id);
        txn_info.set_label(label);
        txn_info.add_table_ids(table_id);
        txn_info.set_timeout_ms(36000);
        req.mutable_txn_info()->CopyFrom(txn_info);
        BeginTxnResponse res;
        meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        txn_id = res.txn_id();
        ASSERT_GT(txn_id, -1);
    }

    // case: txn's status should be TXN_STATUS_PRECOMMITTED
    {
        std::unique_ptr<Transaction> txn;
        int ret = meta_service->txn_kv_->create_txn(&txn);
        ASSERT_EQ(ret, 0);

        const std::string info_key = txn_info_key({mock_instance, db_id, txn_id});
        std::string info_val;
        ASSERT_EQ(txn->get(info_key, &info_val), 0);
        TxnInfoPB txn_info;
        txn_info.ParseFromString(info_val);
        // before call precommit_txn, txn's status is TXN_STATUS_PREPARED
        ASSERT_EQ(txn_info.status(), TxnStatusPB::TXN_STATUS_PREPARED);

        brpc::Controller cntl;
        PrecommitTxnRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_db_id(db_id);
        req.set_txn_id(txn_id);
        req.set_precommit_timeout_ms(36000);
        PrecommitTxnResponse res;
        meta_service->precommit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

        ret = meta_service->txn_kv_->create_txn(&txn);
        ASSERT_EQ(ret, 0);
        ASSERT_EQ(txn->get(info_key, &info_val), 0);
        txn_info.ParseFromString(info_val);
        // after call precommit_txn, txn's status is TXN_STATUS_PRECOMMITTED
        ASSERT_EQ(txn_info.status(), TxnStatusPB::TXN_STATUS_PRECOMMITTED);
    }

    // case: when txn's status is TXN_STATUS_ABORTED/TXN_STATUS_VISIBLE/TXN_STATUS_PRECOMMITTED
    {
        // TXN_STATUS_ABORTED
        std::unique_ptr<Transaction> txn;
        int ret = meta_service->txn_kv_->create_txn(&txn);
        ASSERT_EQ(ret, 0);

        const std::string info_key = txn_info_key({mock_instance, db_id, txn_id});
        std::string info_val;
        ASSERT_EQ(txn->get(info_key, &info_val), 0);
        TxnInfoPB txn_info;
        txn_info.ParseFromString(info_val);
        txn_info.set_status(TxnStatusPB::TXN_STATUS_ABORTED);
        info_val.clear();
        txn_info.SerializeToString(&info_val);
        txn->put(info_key, info_val);
        ASSERT_EQ(ret = txn->commit(), 0);

        brpc::Controller cntl;
        PrecommitTxnRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_db_id(db_id);
        req.set_txn_id(txn_id);
        req.set_precommit_timeout_ms(36000);
        PrecommitTxnResponse res;
        meta_service->precommit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::TXN_ALREADY_ABORTED);

        // TXN_STATUS_VISIBLE
        txn_info.set_status(TxnStatusPB::TXN_STATUS_VISIBLE);
        info_val.clear();
        txn_info.SerializeToString(&info_val);
        txn->put(info_key, info_val);
        ASSERT_EQ(ret = txn->commit(), 0);
        meta_service->precommit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::TXN_ALREADY_VISIBLE);

        // TXN_STATUS_PRECOMMITTED
        txn_info.set_status(TxnStatusPB::TXN_STATUS_PRECOMMITTED);
        info_val.clear();
        txn_info.SerializeToString(&info_val);
        txn->put(info_key, info_val);
        ASSERT_EQ(ret = txn->commit(), 0);
        meta_service->precommit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::TXN_ALREADY_PRECOMMITED);
    }
}

TEST(MetaServiceTest, CommitTxnTest) {
    auto meta_service = get_meta_service();
    // case: first version of rowset
    {
        int64_t txn_id = -1;
        // begin txn
        {
            brpc::Controller cntl;
            BeginTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            TxnInfoPB txn_info_pb;
            txn_info_pb.set_db_id(666);
            txn_info_pb.set_label("test_label");
            txn_info_pb.add_table_ids(1234);
            txn_info_pb.set_timeout_ms(36000);
            req.mutable_txn_info()->CopyFrom(txn_info_pb);
            BeginTxnResponse res;
            meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            txn_id = res.txn_id();
        }

        // mock rowset and tablet
        int64_t tablet_id_base = 1103;
        for (int i = 0; i < 5; ++i) {
            create_tablet(meta_service.get(), 1234, 1235, 1236, tablet_id_base + i);
            auto tmp_rowset = create_rowset(txn_id, tablet_id_base + i);
            CreateRowsetResponse res;
            commit_rowset(meta_service.get(), tmp_rowset, res);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        }

        // precommit txn
        {
            brpc::Controller cntl;
            PrecommitTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            req.set_db_id(666);
            req.set_txn_id(txn_id);
            req.set_precommit_timeout_ms(36000);
            PrecommitTxnResponse res;
            meta_service->precommit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                        &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        }

        // commit txn
        {
            brpc::Controller cntl;
            CommitTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            req.set_db_id(666);
            req.set_txn_id(txn_id);
            CommitTxnResponse res;
            meta_service->commit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        }
    }
}

TEST(MetaServiceTest, CommitTxnExpiredTest) {
    auto meta_service = get_meta_service();

    // case: first version of rowset
    {
        int64_t txn_id = -1;
        int64_t db_id = 713232132;
        // begin txn
        {
            brpc::Controller cntl;
            BeginTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            TxnInfoPB txn_info_pb;
            txn_info_pb.set_db_id(db_id);
            txn_info_pb.set_label("test_commit_txn_expired");
            txn_info_pb.add_table_ids(1234789234);
            txn_info_pb.set_timeout_ms(1);
            req.mutable_txn_info()->CopyFrom(txn_info_pb);
            BeginTxnResponse res;
            meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            txn_id = res.txn_id();
        }

        // mock rowset and tablet
        int64_t tablet_id_base = 1103;
        for (int i = 0; i < 5; ++i) {
            create_tablet(meta_service.get(), 1234789234, 1235, 1236, tablet_id_base + i);
            auto tmp_rowset = create_rowset(txn_id, tablet_id_base + i);
            CreateRowsetResponse res;
            commit_rowset(meta_service.get(), tmp_rowset, res);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        }
        // sleep 1 second for txn timeout
        sleep(1);
        // commit txn
        {
            brpc::Controller cntl;
            CommitTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            req.set_db_id(db_id);
            req.set_txn_id(txn_id);
            CommitTxnResponse res;
            meta_service->commit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::UNDEFINED_ERR);
            ASSERT_TRUE(res.status().msg().find("txn is expired, not allow to commit txn_id=") !=
                        std::string::npos);
        }
    }
}

TEST(MetaServiceTest, AbortTxnTest) {
    auto meta_service = get_meta_service();

    // case: abort txn by txn_id
    {
        int64_t db_id = 666;
        int64_t table_id = 12345;
        std::string label = "abort_txn_by_txn_id";
        std::string cloud_unique_id = "test_cloud_unique_id";
        int64_t tablet_id_base = 1104;
        int64_t txn_id = -1;
        // begin txn
        {
            brpc::Controller cntl;
            BeginTxnRequest req;
            req.set_cloud_unique_id(cloud_unique_id);
            TxnInfoPB txn_info_pb;
            txn_info_pb.set_db_id(db_id);
            txn_info_pb.set_label(label);
            txn_info_pb.add_table_ids(table_id);
            txn_info_pb.set_timeout_ms(36000);
            req.mutable_txn_info()->CopyFrom(txn_info_pb);
            BeginTxnResponse res;
            meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            txn_id = res.txn_id();
        }

        // mock rowset and tablet
        for (int i = 0; i < 5; ++i) {
            create_tablet(meta_service.get(), 12345, 1235, 1236, tablet_id_base + i);
            auto tmp_rowset = create_rowset(txn_id, tablet_id_base + i);
            CreateRowsetResponse res;
            commit_rowset(meta_service.get(), tmp_rowset, res);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        }

        // abort txn by txn_id
        {
            brpc::Controller cntl;
            AbortTxnRequest req;
            req.set_cloud_unique_id(cloud_unique_id);
            req.set_txn_id(txn_id);
            req.set_reason("test");
            AbortTxnResponse res;
            meta_service->abort_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            ASSERT_EQ(res.txn_info().status(), TxnStatusPB::TXN_STATUS_ABORTED);
        }
    }

    // case: abort txn by db_id + label
    {
        int64_t db_id = 66631313131;
        int64_t table_id = 12345;
        std::string label = "abort_txn_by_db_id_and_label";
        std::string cloud_unique_id = "test_cloud_unique_id";
        int64_t tablet_id_base = 1104;
        int64_t txn_id = -1;
        // begin txn
        {
            brpc::Controller cntl;
            BeginTxnRequest req;
            req.set_cloud_unique_id(cloud_unique_id);
            TxnInfoPB txn_info_pb;
            txn_info_pb.set_db_id(db_id);
            txn_info_pb.set_label(label);
            txn_info_pb.add_table_ids(table_id);
            txn_info_pb.set_timeout_ms(36000);
            req.mutable_txn_info()->CopyFrom(txn_info_pb);
            BeginTxnResponse res;
            meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            txn_id = res.txn_id();
        }

        // mock rowset and tablet
        for (int i = 0; i < 5; ++i) {
            create_tablet(meta_service.get(), table_id, 1235, 1236, tablet_id_base + i);
            auto tmp_rowset = create_rowset(txn_id, tablet_id_base + i);
            CreateRowsetResponse res;
            commit_rowset(meta_service.get(), tmp_rowset, res);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        }

        // abort txn by db_id and label
        {
            brpc::Controller cntl;
            AbortTxnRequest req;
            req.set_cloud_unique_id(cloud_unique_id);
            req.set_db_id(db_id);
            req.set_label(label);
            req.set_reason("test");
            AbortTxnResponse res;
            meta_service->abort_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            ASSERT_EQ(res.txn_info().status(), TxnStatusPB::TXN_STATUS_ABORTED);

            std::string recycle_txn_key_;
            std::string recycle_txn_val;
            RecycleTxnKeyInfo recycle_txn_key_info {mock_instance, db_id, txn_id};
            recycle_txn_key(recycle_txn_key_info, &recycle_txn_key_);
            std::unique_ptr<Transaction> txn;
            meta_service->txn_kv_->create_txn(&txn);
            int ret = txn->get(recycle_txn_key_, &recycle_txn_val);
            ASSERT_NE(txn_id, -1);
            ASSERT_EQ(ret, 0);
        }
    }
}

TEST(MetaServiceTest, GetCurrentMaxTxnIdTest) {
    auto meta_service = get_meta_service();

    const int64_t db_id = 123;
    const std::string label = "test_label123";
    const std::string cloud_unique_id = "test_cloud_unique_id";

    brpc::Controller begin_txn_cntl;
    BeginTxnRequest begin_txn_req;
    BeginTxnResponse begin_txn_res;
    TxnInfoPB txn_info_pb;

    begin_txn_req.set_cloud_unique_id(cloud_unique_id);
    txn_info_pb.set_db_id(db_id);
    txn_info_pb.set_label(label);
    txn_info_pb.add_table_ids(12345);
    txn_info_pb.set_timeout_ms(36000);
    begin_txn_req.mutable_txn_info()->CopyFrom(txn_info_pb);

    meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&begin_txn_cntl),
                            &begin_txn_req, &begin_txn_res, nullptr);
    ASSERT_EQ(begin_txn_res.status().code(), MetaServiceCode::OK);

    brpc::Controller max_txn_id_cntl;
    GetCurrentMaxTxnRequest max_txn_id_req;
    GetCurrentMaxTxnResponse max_txn_id_res;

    max_txn_id_req.set_cloud_unique_id(cloud_unique_id);

    meta_service->get_current_max_txn_id(
            reinterpret_cast<::google::protobuf::RpcController*>(&max_txn_id_cntl), &max_txn_id_req,
            &max_txn_id_res, nullptr);

    ASSERT_EQ(max_txn_id_res.status().code(), MetaServiceCode::OK);
    ASSERT_GE(max_txn_id_res.current_max_txn_id(), begin_txn_res.txn_id());
}

TEST(MetaServiceTest, CheckTxnConflictTest) {
    auto meta_service = get_meta_service();

    const int64_t db_id = 666;
    const int64_t table_id = 777;
    const std::string label = "test_label";
    const std::string cloud_unique_id = "test_cloud_unique_id";
    int64_t txn_id = -1;

    brpc::Controller begin_txn_cntl;
    BeginTxnRequest begin_txn_req;
    BeginTxnResponse begin_txn_res;
    TxnInfoPB txn_info_pb;

    begin_txn_req.set_cloud_unique_id(cloud_unique_id);
    txn_info_pb.set_db_id(db_id);
    txn_info_pb.set_label(label);
    txn_info_pb.add_table_ids(table_id);
    txn_info_pb.set_timeout_ms(36000);
    begin_txn_req.mutable_txn_info()->CopyFrom(txn_info_pb);

    meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&begin_txn_cntl),
                            &begin_txn_req, &begin_txn_res, nullptr);
    ASSERT_EQ(begin_txn_res.status().code(), MetaServiceCode::OK);
    txn_id = begin_txn_res.txn_id();
    ASSERT_GT(txn_id, -1);

    brpc::Controller check_txn_conflict_cntl;
    CheckTxnConflictRequest check_txn_conflict_req;
    CheckTxnConflictResponse check_txn_conflict_res;

    check_txn_conflict_req.set_cloud_unique_id(cloud_unique_id);
    check_txn_conflict_req.set_db_id(db_id);
    check_txn_conflict_req.set_end_txn_id(txn_id + 1);
    check_txn_conflict_req.add_table_ids(table_id);

    // first time to check txn conflict
    meta_service->check_txn_conflict(
            reinterpret_cast<::google::protobuf::RpcController*>(&begin_txn_cntl),
            &check_txn_conflict_req, &check_txn_conflict_res, nullptr);

    ASSERT_EQ(check_txn_conflict_res.status().code(), MetaServiceCode::OK);
    ASSERT_EQ(check_txn_conflict_res.finished(), false);

    // mock rowset and tablet
    int64_t tablet_id_base = 123456;
    for (int i = 0; i < 5; ++i) {
        create_tablet(meta_service.get(), table_id, 1235, 1236, tablet_id_base + i);
        auto tmp_rowset = create_rowset(txn_id, tablet_id_base + i);
        CreateRowsetResponse res;
        commit_rowset(meta_service.get(), tmp_rowset, res);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    brpc::Controller commit_txn_cntl;
    CommitTxnRequest commit_txn_req;
    commit_txn_req.set_cloud_unique_id(cloud_unique_id);
    commit_txn_req.set_db_id(db_id);
    commit_txn_req.set_txn_id(txn_id);
    CommitTxnResponse commit_txn_res;
    meta_service->commit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&commit_txn_cntl),
                             &commit_txn_req, &commit_txn_res, nullptr);
    ASSERT_EQ(commit_txn_res.status().code(), MetaServiceCode::OK);

    // second time to check txn conflict
    meta_service->check_txn_conflict(
            reinterpret_cast<::google::protobuf::RpcController*>(&check_txn_conflict_cntl),
            &check_txn_conflict_req, &check_txn_conflict_res, nullptr);

    ASSERT_EQ(check_txn_conflict_res.status().code(), MetaServiceCode::OK);
    ASSERT_EQ(check_txn_conflict_res.finished(), true);

    {
        std::string running_key = txn_running_key({mock_instance, db_id, txn_id});
        std::string running_value;
        std::unique_ptr<Transaction> txn;
        int ret = meta_service->txn_kv_->create_txn(&txn);
        ASSERT_EQ(ret, 0);
        ret = txn->get(running_key, &running_value);
        ASSERT_EQ(ret, 1);
    }
}

TEST(MetaServiceTest, CheckNotTimeoutTxnConflictTest) {
    auto meta_service = get_meta_service();

    const int64_t db_id = 666;
    const int64_t table_id = 777;
    const std::string label = "test_label";
    const std::string cloud_unique_id = "test_cloud_unique_id";
    int64_t txn_id = -1;

    brpc::Controller begin_txn_cntl;
    BeginTxnRequest begin_txn_req;
    BeginTxnResponse begin_txn_res;
    TxnInfoPB txn_info_pb;

    begin_txn_req.set_cloud_unique_id(cloud_unique_id);
    txn_info_pb.set_db_id(db_id);
    txn_info_pb.set_label(label);
    txn_info_pb.add_table_ids(table_id);
    txn_info_pb.set_timeout_ms(3);
    begin_txn_req.mutable_txn_info()->CopyFrom(txn_info_pb);

    meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&begin_txn_cntl),
                            &begin_txn_req, &begin_txn_res, nullptr);
    ASSERT_EQ(begin_txn_res.status().code(), MetaServiceCode::OK);
    txn_id = begin_txn_res.txn_id();
    ASSERT_GT(txn_id, -1);

    brpc::Controller check_txn_conflict_cntl;
    CheckTxnConflictRequest check_txn_conflict_req;
    CheckTxnConflictResponse check_txn_conflict_res;

    check_txn_conflict_req.set_cloud_unique_id(cloud_unique_id);
    check_txn_conflict_req.set_db_id(db_id);
    check_txn_conflict_req.set_end_txn_id(txn_id + 1);
    check_txn_conflict_req.add_table_ids(table_id);

    // wait txn timeout
    sleep(5);
    // first time to check txn conflict
    meta_service->check_txn_conflict(
            reinterpret_cast<::google::protobuf::RpcController*>(&begin_txn_cntl),
            &check_txn_conflict_req, &check_txn_conflict_res, nullptr);

    ASSERT_EQ(check_txn_conflict_res.status().code(), MetaServiceCode::OK);
    ASSERT_EQ(check_txn_conflict_res.finished(), true);
}

TEST(MetaServiceTest, CheckTxnConflictWithAbortLabelTest) {
    int ret = 0;

    auto txn_kv = std::dynamic_pointer_cast<TxnKv>(std::make_shared<MemTxnKv>());
    if (txn_kv != nullptr) {
        ret = txn_kv->init();
        [&] { ASSERT_EQ(ret, 0); }();
    }
    [&] { ASSERT_NE(txn_kv.get(), nullptr); }();

    {
        std::unique_ptr<Transaction> txn;
        txn_kv->create_txn(&txn);
        txn->remove("\x00", "\xfe"); // This is dangerous if the fdb is not correctly set
        txn->commit();
    }

    auto rs = std::make_shared<MockResourceManager>(txn_kv);
    auto rl = std::make_shared<RateLimiter>();
    auto meta_service = std::make_unique<MetaServiceImpl>(txn_kv, rs, rl);

    const int64_t db_id = 666;
    const int64_t table_id = 777;
    const std::string label = "test_label";
    const std::string cloud_unique_id = "test_cloud_unique_id";
    int64_t txn_id = -1;

    brpc::Controller begin_txn_cntl;
    BeginTxnRequest begin_txn_req;
    BeginTxnResponse begin_txn_res;
    TxnInfoPB txn_info_pb;

    begin_txn_req.set_cloud_unique_id(cloud_unique_id);
    txn_info_pb.set_db_id(db_id);
    txn_info_pb.set_label(label);
    txn_info_pb.add_table_ids(table_id);
    txn_info_pb.set_timeout_ms(36000);
    begin_txn_req.mutable_txn_info()->CopyFrom(txn_info_pb);

    meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&begin_txn_cntl),
                            &begin_txn_req, &begin_txn_res, nullptr);
    ASSERT_EQ(begin_txn_res.status().code(), MetaServiceCode::OK);
    txn_id = begin_txn_res.txn_id();
    ASSERT_GT(txn_id, -1);

    brpc::Controller check_txn_conflict_cntl;
    CheckTxnConflictRequest check_txn_conflict_req;
    CheckTxnConflictResponse check_txn_conflict_res;

    check_txn_conflict_req.set_cloud_unique_id(cloud_unique_id);
    check_txn_conflict_req.set_db_id(db_id);
    check_txn_conflict_req.set_end_txn_id(txn_id + 1);
    check_txn_conflict_req.add_table_ids(table_id);

    // first time to check txn conflict
    meta_service->check_txn_conflict(
            reinterpret_cast<::google::protobuf::RpcController*>(&begin_txn_cntl),
            &check_txn_conflict_req, &check_txn_conflict_res, nullptr);

    ASSERT_EQ(check_txn_conflict_res.status().code(), MetaServiceCode::OK);
    ASSERT_EQ(check_txn_conflict_res.finished(), false);

    std::string running_key;
    std::string running_val;
    txn_running_key({mock_instance, db_id, txn_id}, &running_key);
    {
        std::unique_ptr<Transaction> txn;
        txn_kv->create_txn(&txn);
        ret = txn->get(running_key, &running_val);
        ASSERT_EQ(ret, 0);
    }

    brpc::Controller abort_txn_cntl;
    AbortTxnRequest abort_txn_req;
    abort_txn_req.set_cloud_unique_id(cloud_unique_id);
    abort_txn_req.set_db_id(db_id);
    abort_txn_req.set_label(label);
    AbortTxnResponse abort_txn_res;
    meta_service->abort_txn(reinterpret_cast<::google::protobuf::RpcController*>(&abort_txn_cntl),
                            &abort_txn_req, &abort_txn_res, nullptr);
    ASSERT_EQ(abort_txn_res.status().code(), MetaServiceCode::OK);

    // second time to check txn conflict
    meta_service->check_txn_conflict(
            reinterpret_cast<::google::protobuf::RpcController*>(&check_txn_conflict_cntl),
            &check_txn_conflict_req, &check_txn_conflict_res, nullptr);

    ASSERT_EQ(check_txn_conflict_res.status().code(), MetaServiceCode::OK);
    ASSERT_EQ(check_txn_conflict_res.finished(), true);

    {
        std::unique_ptr<Transaction> txn;
        txn_kv->create_txn(&txn);
        ret = txn->get(running_key, &running_val);
        ASSERT_EQ(ret, 1);
    }
}

TEST(MetaServiceTest, CleanTxnLabelTest) {
    int ret = 0;
    auto txn_kv = std::dynamic_pointer_cast<TxnKv>(std::make_shared<MemTxnKv>());
    if (txn_kv != nullptr) {
        ret = txn_kv->init();
        [&] { ASSERT_EQ(ret, 0); }();
    }
    [&] { ASSERT_NE(txn_kv.get(), nullptr); }();

    {
        std::unique_ptr<Transaction> txn;
        txn_kv->create_txn(&txn);
        txn->remove("\x00", "\xfe"); // This is dangerous if the fdb is not correctly set
        txn->commit();
    }

    auto rs = std::make_shared<MockResourceManager>(txn_kv);
    auto rl = std::make_shared<RateLimiter>();
    auto meta_service = std::make_unique<MetaServiceImpl>(txn_kv, rs, rl);

    {
        int64_t txn_id = -1;
        int64_t db_id = 1987211;
        const std::string& label = "test_clean_label";

        // begin txn
        {
            brpc::Controller cntl;
            BeginTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            TxnInfoPB txn_info_pb;
            txn_info_pb.set_db_id(db_id);
            txn_info_pb.set_label(label);
            txn_info_pb.add_table_ids(1234);
            txn_info_pb.set_timeout_ms(36000);
            req.mutable_txn_info()->CopyFrom(txn_info_pb);
            BeginTxnResponse res;
            meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            txn_id = res.txn_id();
        }

        const std::string info_key = txn_info_key({mock_instance, db_id, txn_id});
        std::string info_val;

        const std::string label_key = txn_label_key({mock_instance, db_id, label});
        std::string label_val;

        const std::string index_key = txn_index_key({mock_instance, txn_id});
        std::string index_val;

        const std::string running_key = txn_running_key({mock_instance, db_id, txn_id});
        std::string running_val;

        const std::string recycle_key = recycle_txn_key({mock_instance, db_id, txn_id});
        std::string recycle_val;

        {
            std::unique_ptr<Transaction> txn;
            txn_kv->create_txn(&txn);
            ret = txn->get(info_key, &info_val);
            ASSERT_EQ(ret, 0);
            ret = txn->get(label_key, &label_val);
            ASSERT_EQ(ret, 0);
            ret = txn->get(index_key, &index_val);
            ASSERT_EQ(ret, 0);
            ret = txn->get(running_key, &running_val);
            ASSERT_EQ(ret, 0);
            ret = txn->get(recycle_key, &recycle_val);
            ASSERT_EQ(ret, 1);
        }

        // mock rowset and tablet
        int64_t tablet_id_base = 110313131;
        for (int i = 0; i < 2; ++i) {
            create_tablet(meta_service.get(), 1234, 1235, 1236, tablet_id_base + i);
            auto tmp_rowset = create_rowset(txn_id, tablet_id_base + i);
            CreateRowsetResponse res;
            commit_rowset(meta_service.get(), tmp_rowset, res);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        }

        // commit txn
        {
            brpc::Controller cntl;
            CommitTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            req.set_db_id(db_id);
            req.set_txn_id(txn_id);
            CommitTxnResponse res;
            meta_service->commit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        }

        // begin txn
        {
            brpc::Controller cntl;
            BeginTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            TxnInfoPB txn_info_pb;
            txn_info_pb.set_db_id(db_id);
            txn_info_pb.set_label(label);
            txn_info_pb.add_table_ids(1234);
            txn_info_pb.set_timeout_ms(36000);
            req.mutable_txn_info()->CopyFrom(txn_info_pb);
            BeginTxnResponse res;
            meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::TXN_LABEL_ALREADY_USED);
        }

        // clean txn label
        {
            brpc::Controller cntl;
            CleanTxnLabelRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            req.set_db_id(db_id);
            CleanTxnLabelResponse res;
            meta_service->clean_txn_label(
                    reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res,
                    nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
        }

        // clean txn label
        {
            brpc::Controller cntl;
            CleanTxnLabelRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            req.set_db_id(db_id);
            req.add_labels(label);
            CleanTxnLabelResponse res;
            meta_service->clean_txn_label(
                    reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res,
                    nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        }

        {
            std::unique_ptr<Transaction> txn;
            txn_kv->create_txn(&txn);
            ret = txn->get(info_key, &info_val);
            ASSERT_EQ(ret, 1);
            ret = txn->get(label_key, &label_val);
            ASSERT_EQ(ret, 1);
            ret = txn->get(index_key, &index_val);
            ASSERT_EQ(ret, 1);
            ret = txn->get(running_key, &running_val);
            ASSERT_EQ(ret, 1);
            ret = txn->get(recycle_key, &recycle_val);
            ASSERT_EQ(ret, 1);
        }

        {
            brpc::Controller cntl;
            BeginTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            TxnInfoPB txn_info_pb;
            txn_info_pb.set_db_id(db_id);
            txn_info_pb.set_label(label);
            txn_info_pb.add_table_ids(1234);
            txn_info_pb.set_timeout_ms(36000);
            req.mutable_txn_info()->CopyFrom(txn_info_pb);
            BeginTxnResponse res;
            meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        }
    }
}

//
TEST(MetaServiceTest, GetTxnTest) {
    int ret = 0;
    auto txn_kv = std::dynamic_pointer_cast<TxnKv>(std::make_shared<MemTxnKv>());
    if (txn_kv != nullptr) {
        ret = txn_kv->init();
        [&] { ASSERT_EQ(ret, 0); }();
    }
    [&] { ASSERT_NE(txn_kv.get(), nullptr); }();

    {
        std::unique_ptr<Transaction> txn;
        txn_kv->create_txn(&txn);
        txn->remove("\x00", "\xfe"); // This is dangerous if the fdb is not correctly set
        txn->commit();
    }

    auto rs = std::make_shared<MockResourceManager>(txn_kv);
    auto rl = std::make_shared<RateLimiter>();
    auto meta_service = std::make_unique<MetaServiceImpl>(txn_kv, rs, rl);

    {
        int64_t txn_id = -1;
        int64_t db_id = 34521431231;
        const std::string& label = "test_get_txn";

        // begin txn
        {
            brpc::Controller cntl;
            BeginTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            TxnInfoPB txn_info_pb;
            txn_info_pb.set_db_id(db_id);
            txn_info_pb.set_label(label);
            txn_info_pb.add_table_ids(1234);
            txn_info_pb.set_timeout_ms(36000);
            req.mutable_txn_info()->CopyFrom(txn_info_pb);
            BeginTxnResponse res;
            meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            txn_id = res.txn_id();
        }

        {
            brpc::Controller cntl;
            GetTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            req.set_db_id(db_id);
            req.set_txn_id(-1);
            GetTxnResponse res;
            meta_service->get_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                  &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
        }

        {
            brpc::Controller cntl;
            GetTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            req.set_db_id(db_id);
            req.set_txn_id(txn_id);
            GetTxnResponse res;
            meta_service->get_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                  &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        }

        {
            brpc::Controller cntl;
            GetTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            req.set_txn_id(txn_id);
            GetTxnResponse res;
            meta_service->get_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                  &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        }
    }
}
//

TEST(MetaServiceTest, CopyJobTest) {
    auto meta_service = get_meta_service();
    brpc::Controller cntl;
    auto cloud_unique_id = "test_cloud_unique_id";
    auto stage_id = "test_stage_id";
    int64_t table_id = 100;
    std::string instance_id = "copy_job_test_instance_id";

    [[maybe_unused]] auto sp = SyncPoint::get_instance();
    std::unique_ptr<int, std::function<void(int*)>> defer(
            (int*)0x01, [](int*) { SyncPoint::get_instance()->clear_all_call_backs(); });
    sp->set_call_back("get_instance_id::pred", [](void* p) { *((bool*)p) = true; });
    sp->set_call_back("get_instance_id", [&](void* p) { *((std::string*)p) = instance_id; });
    sp->enable_processing();

    // generate a begin copy request
    BeginCopyRequest begin_copy_request;
    begin_copy_request.set_cloud_unique_id(cloud_unique_id);
    begin_copy_request.set_stage_id(stage_id);
    begin_copy_request.set_stage_type(StagePB::EXTERNAL);
    begin_copy_request.set_table_id(table_id);
    begin_copy_request.set_copy_id("test_copy_id");
    begin_copy_request.set_group_id(0);
    begin_copy_request.set_start_time_ms(200);
    begin_copy_request.set_timeout_time_ms(300);
    for (int i = 0; i < 20; ++i) {
        ObjectFilePB object_file_pb;
        object_file_pb.set_relative_path("obj_" + std::to_string(i));
        object_file_pb.set_etag("obj_" + std::to_string(i) + "_etag");
        begin_copy_request.add_object_files()->CopyFrom(object_file_pb);
    }

    // generate a finish copy request
    FinishCopyRequest finish_copy_request;
    finish_copy_request.set_cloud_unique_id(cloud_unique_id);
    finish_copy_request.set_stage_id(stage_id);
    finish_copy_request.set_stage_type(StagePB::EXTERNAL);
    finish_copy_request.set_table_id(table_id);
    finish_copy_request.set_copy_id("test_copy_id");
    finish_copy_request.set_group_id(0);
    finish_copy_request.set_action(FinishCopyRequest::COMMIT);

    // generate a get copy files request
    GetCopyFilesRequest get_copy_file_req;
    get_copy_file_req.set_cloud_unique_id(cloud_unique_id);
    get_copy_file_req.set_stage_id(stage_id);
    get_copy_file_req.set_table_id(table_id);

    // generate a get copy job request
    GetCopyJobRequest get_copy_job_request;
    get_copy_job_request.set_cloud_unique_id(cloud_unique_id);
    get_copy_job_request.set_stage_id(stage_id);
    get_copy_job_request.set_table_id(table_id);
    get_copy_job_request.set_copy_id("test_copy_id");
    get_copy_job_request.set_group_id(0);

    // get copy job
    {
        GetCopyJobResponse res;
        meta_service->get_copy_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                   &get_copy_job_request, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_EQ(res.has_copy_job(), false);
    }
    // begin copy
    {
        BeginCopyResponse res;
        meta_service->begin_copy(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                 &begin_copy_request, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_EQ(res.filtered_object_files_size(), 20);
    }
    // get copy files
    {
        GetCopyFilesResponse res;
        meta_service->get_copy_files(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &get_copy_file_req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_EQ(res.object_files_size(), 20);
    }
    // get copy job
    {
        GetCopyJobResponse res;
        meta_service->get_copy_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                   &get_copy_job_request, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_EQ(res.copy_job().object_files().size(), 20);
    }
    // begin copy with duplicate files
    {
        begin_copy_request.set_copy_id("test_copy_id_1");
        begin_copy_request.clear_object_files();
        for (int i = 15; i < 30; ++i) {
            ObjectFilePB object_file_pb;
            object_file_pb.set_relative_path("obj_" + std::to_string(i));
            object_file_pb.set_etag("obj_" + std::to_string(i) + "_etag");
            begin_copy_request.add_object_files()->CopyFrom(object_file_pb);
        }

        BeginCopyResponse res;
        meta_service->begin_copy(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                 &begin_copy_request, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_EQ(res.filtered_object_files_size(), 10);
    }
    // get copy files
    {
        GetCopyFilesResponse res;
        meta_service->get_copy_files(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &get_copy_file_req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_EQ(res.object_files_size(), 30);
    }
    // finish the first copy job
    {
        FinishCopyResponse res;
        meta_service->finish_copy(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                  &finish_copy_request, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }
    // get copy files
    {
        GetCopyFilesResponse res;
        meta_service->get_copy_files(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &get_copy_file_req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_EQ(res.object_files_size(), 30);
    }
    // abort the second copy job
    {
        finish_copy_request.set_copy_id("test_copy_id_1");
        finish_copy_request.set_action(FinishCopyRequest::ABORT);

        FinishCopyResponse res;
        meta_service->finish_copy(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                  &finish_copy_request, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }
    // get copy files
    {
        GetCopyFilesResponse res;
        meta_service->get_copy_files(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &get_copy_file_req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_EQ(res.object_files_size(), 20);
    }
    {
        // begin a copy job whose files are all loaded, the copy job key should not be created
        begin_copy_request.set_copy_id("tmp_id");
        begin_copy_request.clear_object_files();
        for (int i = 0; i < 20; ++i) {
            ObjectFilePB object_file_pb;
            object_file_pb.set_relative_path("obj_" + std::to_string(i));
            object_file_pb.set_etag("obj_" + std::to_string(i) + "_etag");
            begin_copy_request.add_object_files()->CopyFrom(object_file_pb);
        }
        BeginCopyResponse res;
        meta_service->begin_copy(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                 &begin_copy_request, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_EQ(res.filtered_object_files_size(), 0);
        // get copy job
        get_copy_job_request.set_copy_id("tmp_id");
        GetCopyJobResponse res2;
        meta_service->get_copy_job(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                   &get_copy_job_request, &res2, nullptr);
        ASSERT_EQ(res2.status().code(), MetaServiceCode::OK);
        ASSERT_FALSE(res2.has_copy_job());
    }
    // scan fdb
    {
        std::unique_ptr<Transaction> txn;
        std::string get_val;
        int ret = meta_service->txn_kv_->create_txn(&txn);
        ASSERT_EQ(ret, 0);
        // 20 copy files
        {
            CopyFileKeyInfo key_info0 {instance_id, stage_id, table_id, "", ""};
            CopyFileKeyInfo key_info1 {instance_id, stage_id, table_id + 1, "", ""};
            std::string key0;
            std::string key1;
            copy_file_key(key_info0, &key0);
            copy_file_key(key_info1, &key1);
            std::unique_ptr<RangeGetIterator> it;
            ret = txn->get(key0, key1, &it);
            ASSERT_EQ(ret, 0);
            int file_cnt = 0;
            do {
                ret = txn->get(key0, key1, &it);
                ASSERT_EQ(ret, 0);
                while (it->has_next()) {
                    auto [k, v] = it->next();
                    CopyFilePB copy_file;
                    ASSERT_TRUE(copy_file.ParseFromArray(v.data(), v.size()));
                    ASSERT_EQ(copy_file.copy_id(), "test_copy_id");
                    ++file_cnt;
                    if (!it->has_next()) {
                        key0 = k;
                    }
                }
                key0.push_back('\x00');
            } while (it->more());
            ASSERT_EQ(file_cnt, 20);
        }
        // 1 copy job with finish status
        {
            CopyJobKeyInfo key_info0 {instance_id, stage_id, table_id, "", 0};
            CopyJobKeyInfo key_info1 {instance_id, stage_id, table_id + 1, "", 0};
            std::string key0;
            std::string key1;
            copy_job_key(key_info0, &key0);
            copy_job_key(key_info1, &key1);
            std::unique_ptr<RangeGetIterator> it;
            int job_cnt = 0;
            do {
                ret = txn->get(key0, key1, &it);
                ASSERT_EQ(ret, 0);
                while (it->has_next()) {
                    auto [k, v] = it->next();
                    CopyJobPB copy_job;
                    ASSERT_EQ(copy_job.ParseFromArray(v.data(), v.size()), true);
                    ASSERT_EQ(copy_job.object_files_size(), 20);
                    ASSERT_EQ(copy_job.job_status(), CopyJobPB::FINISH);
                    ++job_cnt;
                    if (!it->has_next()) {
                        key0 = k;
                    }
                }
                key0.push_back('\x00');
            } while (it->more());
            ASSERT_EQ(job_cnt, 1);
        }
    }
}

TEST(MetaServiceTest, FilterCopyFilesTest) {
    auto meta_service = get_meta_service();
    brpc::Controller cntl;
    auto cloud_unique_id = "test_cloud_unique_id";
    std::string instance_id = "stage_test_instance_id";
    auto stage_id = "test_stage_id";
    int64_t table_id = 100;
    [[maybe_unused]] auto sp = SyncPoint::get_instance();
    sp->set_call_back("get_instance_id::pred", [](void* p) { *((bool*)p) = true; });
    sp->set_call_back("get_instance_id", [&](void* p) { *((std::string*)p) = instance_id; });
    sp->set_call_back("encrypt_ak_sk:get_encryption_key_ret",
                      [](void* p) { *reinterpret_cast<int*>(p) = 0; });
    sp->set_call_back("encrypt_ak_sk:get_encryption_key",
                      [](void* p) { *reinterpret_cast<std::string*>(p) = "test"; });
    sp->set_call_back("encrypt_ak_sk:get_encryption_key_id",
                      [](void* p) { *reinterpret_cast<int*>(p) = 1; });
    sp->set_call_back("decrypt_ak_sk:get_encryption_key_ret",
                      [](void* p) { *reinterpret_cast<int*>(p) = 0; });
    sp->set_call_back("decrypt_ak_sk:get_encryption_key",
                      [](void* p) { *reinterpret_cast<std::string*>(p) = "test"; });
    sp->enable_processing();

    FilterCopyFilesRequest request;
    request.set_cloud_unique_id(cloud_unique_id);
    request.set_stage_id(stage_id);
    request.set_table_id(table_id);
    for (int i = 0; i < 10; ++i) {
        ObjectFilePB object_file;
        object_file.set_relative_path("file" + std::to_string(i));
        object_file.set_etag("etag" + std::to_string(i));
        request.add_object_files()->CopyFrom(object_file);
    }

    // all files are not loaded
    {
        FilterCopyFilesResponse res;
        meta_service->filter_copy_files(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                        &request, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_EQ(res.object_files().size(), 10);
    }

    // some files are loaded
    {
        std::unique_ptr<Transaction> txn;
        int ret = meta_service->txn_kv_->create_txn(&txn);
        ASSERT_EQ(ret, 0);
        for (int i = 0; i < 4; ++i) {
            CopyFileKeyInfo key_info {instance_id, stage_id, table_id, "file" + std::to_string(i),
                                      "etag" + std::to_string(i)};
            std::string key;
            copy_file_key(key_info, &key);
            CopyFilePB copy_file;
            copy_file.set_copy_id("test_copy_id");
            std::string val;
            copy_file.SerializeToString(&val);
            txn->put(key, val);
        }
        ASSERT_EQ(txn->commit(), 0);
        FilterCopyFilesResponse res;
        meta_service->filter_copy_files(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                        &request, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_EQ(res.object_files().size(), 6);
        ASSERT_EQ(res.object_files().at(0).relative_path(), "file4");
    }

    // all files are loaded
    {
        std::unique_ptr<Transaction> txn;
        int ret = meta_service->txn_kv_->create_txn(&txn);
        ASSERT_EQ(ret, 0);
        for (int i = 4; i < 10; ++i) {
            CopyFileKeyInfo key_info {instance_id, stage_id, table_id, "file" + std::to_string(i),
                                      "etag" + std::to_string(i)};
            std::string key;
            copy_file_key(key_info, &key);
            CopyFilePB copy_file;
            copy_file.set_copy_id("test_copy_id");
            std::string val;
            copy_file.SerializeToString(&val);
            txn->put(key, val);
        }
        ASSERT_EQ(txn->commit(), 0);
        FilterCopyFilesResponse res;
        meta_service->filter_copy_files(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                        &request, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_EQ(res.object_files().size(), 0);
    }
}

extern std::vector<std::pair<int64_t, int64_t>> calc_sync_versions(
        int64_t req_bc_cnt, int64_t bc_cnt, int64_t req_cc_cnt, int64_t cc_cnt, int64_t req_cp,
        int64_t cp, int64_t req_start, int64_t req_end);

TEST(MetaServiceTest, CalcSyncVersionsTest) {
    using Versions = std::vector<std::pair<int64_t, int64_t>>;
    // * no compaction happened
    // req_cc_cnt == ms_cc_cnt && req_bc_cnt == ms_bc_cnt && req_cp == ms_cp
    // BE  [=][=][=][=][=====][=][=]<.......>
    //                  ^~~~~ req_cp
    // BE  [=][=][=][=][=====][=][=][=][=][=][=]
    //                  ^~~~~ ms_cp
    //                               ^_____^ versions_return: [req_start, req_end]
    {
        auto [req_bc_cnt, bc_cnt] = std::tuple {0, 0};
        auto [req_cc_cnt, cc_cnt] = std::tuple {1, 1};
        auto [req_cp, cp] = std::tuple {5, 5};
        auto [req_start, req_end] = std::tuple {8, 12};
        auto versions = calc_sync_versions(req_bc_cnt, bc_cnt, req_cc_cnt, cc_cnt, req_cp, cp,
                                           req_start, req_end);
        ASSERT_EQ(versions, (Versions {{8, 12}}));
    }
    // * only one CC happened and CP changed
    // req_cc_cnt == ms_cc_cnt - 1 && req_bc_cnt == ms_bc_cnt && req_cp < ms_cp
    // BE  [=][=][=][=][=====][=][=]<.......>
    //                  ^~~~~ req_cp
    // MS  [=][=][=][=][xxxxxxxxxxxxxx][=======][=][=]
    //                                  ^~~~~~~ ms_cp
    //                  ^__________________^ versions_return: [req_cp, ms_cp - 1] v [req_start, req_end]
    {
        auto [req_bc_cnt, bc_cnt] = std::tuple {0, 0};
        auto [req_cc_cnt, cc_cnt] = std::tuple {1, 2};
        auto [req_cp, cp] = std::tuple {5, 10};
        auto [req_start, req_end] = std::tuple {8, 12};
        auto versions = calc_sync_versions(req_bc_cnt, bc_cnt, req_cc_cnt, cc_cnt, req_cp, cp,
                                           req_start, req_end);
        ASSERT_EQ(versions, (Versions {{5, 12}})); // [5, 9] v [8, 12]
    }
    {
        auto [req_bc_cnt, bc_cnt] = std::tuple {0, 0};
        auto [req_cc_cnt, cc_cnt] = std::tuple {1, 2};
        auto [req_cp, cp] = std::tuple {5, 15};
        auto [req_start, req_end] = std::tuple {8, 12};
        auto versions = calc_sync_versions(req_bc_cnt, bc_cnt, req_cc_cnt, cc_cnt, req_cp, cp,
                                           req_start, req_end);
        ASSERT_EQ(versions, (Versions {{5, 14}})); // [5, 14] v [8, 12]
    }
    // * only one CC happened and CP remain unchanged
    // req_cc_cnt == ms_cc_cnt - 1 && req_bc_cnt == ms_bc_cnt && req_cp == ms_cp
    // BE  [=][=][=][=][=====][=][=]<.......>
    //                  ^~~~~ req_cp
    // MS  [=][=][=][=][xxxxxxxxxxxxxx][=][=][=][=][=]
    //                  ^~~~~~~~~~~~~~ ms_cp
    //                  ^__________________^ versions_return: [req_cp, max] v [req_start, req_end]
    //
    {
        auto [req_bc_cnt, bc_cnt] = std::tuple {0, 0};
        auto [req_cc_cnt, cc_cnt] = std::tuple {1, 2};
        auto [req_cp, cp] = std::tuple {5, 5};
        auto [req_start, req_end] = std::tuple {8, 12};
        auto versions = calc_sync_versions(req_bc_cnt, bc_cnt, req_cc_cnt, cc_cnt, req_cp, cp,
                                           req_start, req_end);
        ASSERT_EQ(versions, (Versions {{5, INT64_MAX - 1}})); // [5, max] v [8, 12]
    }
    // * more than one CC happened and CP remain unchanged
    // req_cc_cnt < ms_cc_cnt - 1 && req_bc_cnt == ms_bc_cnt && req_cp == ms_cp
    // BE  [=][=][=][=][=====][=][=]<.......>
    //                  ^~~~~ req_cp
    // MS  [=][=][=][=][xxxxxxxxxxxxxx][xxxxxxx][=][=]
    //                  ^~~~~~~~~~~~~~ ms_cp
    //                  ^_____________________^ versions_return: [req_cp, max] v [req_start, req_end]
    {
        auto [req_bc_cnt, bc_cnt] = std::tuple {0, 0};
        auto [req_cc_cnt, cc_cnt] = std::tuple {1, 3};
        auto [req_cp, cp] = std::tuple {5, 5};
        auto [req_start, req_end] = std::tuple {8, 12};
        auto versions = calc_sync_versions(req_bc_cnt, bc_cnt, req_cc_cnt, cc_cnt, req_cp, cp,
                                           req_start, req_end);
        ASSERT_EQ(versions, (Versions {{5, INT64_MAX - 1}})); // [5, max] v [8, 12]
    }
    // * more than one CC happened and CP changed
    // BE  [=][=][=][=][=====][=][=]
    //                  ^~~~~ req_cp
    // MS  [=][=][=][=][xxxxxxxxxxxxxx][xxxxxxx][=][=]
    //                                  ^~~~~~~ ms_cp
    //                  ^_____________________^ related_versions: [req_cp, max] v [req_start, req_end]
    {
        auto [req_bc_cnt, bc_cnt] = std::tuple {0, 0};
        auto [req_cc_cnt, cc_cnt] = std::tuple {1, 3};
        auto [req_cp, cp] = std::tuple {5, 15};
        auto [req_start, req_end] = std::tuple {8, 12};
        auto versions = calc_sync_versions(req_bc_cnt, bc_cnt, req_cc_cnt, cc_cnt, req_cp, cp,
                                           req_start, req_end);
        ASSERT_EQ(versions, (Versions {{5, INT64_MAX - 1}})); // [5, max] v [8, 12]
    }
    // * for any BC happended
    // req_bc_cnt < ms_bc_cnt
    // BE  [=][=][=][=][=====][=][=]<.......>
    //                  ^~~~~ req_cp
    // MS  [xxxxxxxxxx][xxxxxxxxxxxxxx][=======][=][=]
    //                                  ^~~~~~~ ms_cp
    //     ^_________________________^ versions_return: [0, ms_cp - 1] v versions_return_in_above_case
    {
        auto [req_bc_cnt, bc_cnt] = std::tuple {0, 1};
        auto [req_cc_cnt, cc_cnt] = std::tuple {1, 1};
        auto [req_cp, cp] = std::tuple {5, 5};
        auto [req_start, req_end] = std::tuple {8, 12};
        auto versions = calc_sync_versions(req_bc_cnt, bc_cnt, req_cc_cnt, cc_cnt, req_cp, cp,
                                           req_start, req_end);
        ASSERT_EQ(versions, (Versions {{0, 4}, {8, 12}}));
    }
    {
        auto [req_bc_cnt, bc_cnt] = std::tuple {0, 1};
        auto [req_cc_cnt, cc_cnt] = std::tuple {1, 1};
        auto [req_cp, cp] = std::tuple {8, 8};
        auto [req_start, req_end] = std::tuple {8, 12};
        auto versions = calc_sync_versions(req_bc_cnt, bc_cnt, req_cc_cnt, cc_cnt, req_cp, cp,
                                           req_start, req_end);
        ASSERT_EQ(versions, (Versions {{0, 12}})); // [0, 7] v [8, 12]
    }
    {
        auto [req_bc_cnt, bc_cnt] = std::tuple {0, 1};
        auto [req_cc_cnt, cc_cnt] = std::tuple {1, 2};
        auto [req_cp, cp] = std::tuple {5, 10};
        auto [req_start, req_end] = std::tuple {8, 12};
        auto versions = calc_sync_versions(req_bc_cnt, bc_cnt, req_cc_cnt, cc_cnt, req_cp, cp,
                                           req_start, req_end);
        ASSERT_EQ(versions, (Versions {{0, 12}})); // [0, 4] v [5, 9] v [8, 12]
    }
    {
        auto [req_bc_cnt, bc_cnt] = std::tuple {0, 1};
        auto [req_cc_cnt, cc_cnt] = std::tuple {1, 2};
        auto [req_cp, cp] = std::tuple {5, 15};
        auto [req_start, req_end] = std::tuple {8, 12};
        auto versions = calc_sync_versions(req_bc_cnt, bc_cnt, req_cc_cnt, cc_cnt, req_cp, cp,
                                           req_start, req_end);
        ASSERT_EQ(versions, (Versions {{0, 14}})); // [0, 4] v [5, 14] v [8, 12]
    }
    {
        auto [req_bc_cnt, bc_cnt] = std::tuple {0, 1};
        auto [req_cc_cnt, cc_cnt] = std::tuple {1, 2};
        auto [req_cp, cp] = std::tuple {5, 5};
        auto [req_start, req_end] = std::tuple {8, 12};
        auto versions = calc_sync_versions(req_bc_cnt, bc_cnt, req_cc_cnt, cc_cnt, req_cp, cp,
                                           req_start, req_end);
        // [0, 4] v [5, max] v [8, 12]
        ASSERT_EQ(versions, (Versions {{0, INT64_MAX - 1}}));
    }
}

TEST(MetaServiceTest, StageTest) {
    auto meta_service = get_meta_service();
    brpc::Controller cntl;
    auto cloud_unique_id = "test_cloud_unique_id";
    std::string instance_id = "stage_test_instance_id";
    [[maybe_unused]] auto sp = SyncPoint::get_instance();
    std::unique_ptr<int, std::function<void(int*)>> defer(
            (int*)0x01, [](int*) { SyncPoint::get_instance()->clear_all_call_backs(); });
    sp->set_call_back("get_instance_id::pred", [](void* p) { *((bool*)p) = true; });
    sp->set_call_back("get_instance_id", [&](void* p) { *((std::string*)p) = instance_id; });
    sp->set_call_back("encrypt_ak_sk:get_encryption_key_ret",
                      [](void* p) { *reinterpret_cast<int*>(p) = 0; });
    sp->set_call_back("encrypt_ak_sk:get_encryption_key",
                      [](void* p) { *reinterpret_cast<std::string*>(p) = "test"; });
    sp->set_call_back("encrypt_ak_sk:get_encryption_key_id",
                      [](void* p) { *reinterpret_cast<int*>(p) = 1; });
    sp->set_call_back("decrypt_ak_sk:get_encryption_key_ret",
                      [](void* p) { *reinterpret_cast<int*>(p) = 0; });
    sp->set_call_back("decrypt_ak_sk:get_encryption_key",
                      [](void* p) { *reinterpret_cast<std::string*>(p) = "test"; });
    sp->enable_processing();

    ObjectStoreInfoPB obj;
    obj.set_ak("123");
    obj.set_sk("321");
    obj.set_bucket("456");
    obj.set_prefix("654");
    obj.set_endpoint("789");
    obj.set_region("987");
    obj.set_external_endpoint("888");
    obj.set_provider(ObjectStoreInfoPB::BOS);

    // create instance
    {
        CreateInstanceRequest req;
        req.set_instance_id(instance_id);
        req.set_user_id("test_user");
        req.set_name("test_name");
        req.mutable_obj_info()->CopyFrom(obj);

        CreateInstanceResponse res;
        meta_service->create_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // test create and get internal stage
    {
        // get a non-existent internal stage
        GetStageRequest get_stage_req;
        get_stage_req.set_cloud_unique_id(cloud_unique_id);
        get_stage_req.set_type(StagePB::INTERNAL);
        get_stage_req.set_mysql_user_name("root");
        get_stage_req.set_mysql_user_id("root_id");
        GetStageResponse res;
        meta_service->get_stage(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                &get_stage_req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::STAGE_NOT_FOUND);

        // create an internal stage
        CreateStageRequest create_stage_request;
        StagePB stage;
        stage.set_type(StagePB::INTERNAL);
        stage.add_mysql_user_name("root");
        stage.add_mysql_user_id("root_id");
        stage.set_stage_id("internal_stage_id");
        create_stage_request.set_cloud_unique_id(cloud_unique_id);
        create_stage_request.mutable_stage()->CopyFrom(stage);
        CreateStageResponse create_stage_response;
        meta_service->create_stage(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                   &create_stage_request, &create_stage_response, nullptr);
        ASSERT_EQ(create_stage_response.status().code(), MetaServiceCode::OK);

        // get existent internal stage
        GetStageResponse res2;
        meta_service->get_stage(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                &get_stage_req, &res2, nullptr);
        ASSERT_EQ(res2.status().code(), MetaServiceCode::OK);
        ASSERT_EQ(1, res2.stage().size());

        // drop internal stage
        DropStageRequest drop_stage_request;
        drop_stage_request.set_cloud_unique_id(cloud_unique_id);
        drop_stage_request.set_type(StagePB::INTERNAL);
        drop_stage_request.set_mysql_user_id("root_id");
        drop_stage_request.set_reason("Drop");
        DropStageResponse drop_stage_response;
        meta_service->drop_stage(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                 &drop_stage_request, &drop_stage_response, nullptr);
        ASSERT_EQ(drop_stage_response.status().code(), MetaServiceCode::OK);
        // scan fdb has recycle_stage key
        {
            RecycleStageKeyInfo key_info0 {instance_id, ""};
            RecycleStageKeyInfo key_info1 {instance_id, "{"};
            std::string key0;
            std::string key1;
            recycle_stage_key(key_info0, &key0);
            recycle_stage_key(key_info1, &key1);
            std::unique_ptr<Transaction> txn;
            std::string get_val;
            int ret = meta_service->txn_kv_->create_txn(&txn);
            std::unique_ptr<RangeGetIterator> it;
            ret = txn->get(key0, key1, &it);
            ASSERT_EQ(ret, 0);
            int stage_cnt = 0;
            do {
                ret = txn->get(key0, key1, &it);
                ASSERT_EQ(ret, 0);
                while (it->has_next()) {
                    auto [k, v] = it->next();
                    ++stage_cnt;
                    if (!it->has_next()) {
                        key0 = k;
                    }
                }
                key0.push_back('\x00');
            } while (it->more());
            ASSERT_EQ(stage_cnt, 1);
        }

        // get internal stage
        meta_service->get_stage(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                &get_stage_req, &res2, nullptr);
        ASSERT_EQ(res2.status().code(), MetaServiceCode::STAGE_NOT_FOUND);

        // drop a non-exist internal stage
        drop_stage_request.set_mysql_user_id("root_id2");
        meta_service->drop_stage(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                 &drop_stage_request, &drop_stage_response, nullptr);
        ASSERT_EQ(drop_stage_response.status().code(), MetaServiceCode::STAGE_NOT_FOUND);
    }

    // test create and get external stage
    {
        // get an external stage with name
        GetStageRequest get_stage_req;
        get_stage_req.set_cloud_unique_id(cloud_unique_id);
        get_stage_req.set_type(StagePB::EXTERNAL);
        get_stage_req.set_stage_name("ex_name_1");

        {
            GetStageResponse res;
            meta_service->get_stage(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &get_stage_req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::STAGE_NOT_FOUND);
        }

        // create 2 stages
        for (auto i = 0; i < 2; ++i) {
            StagePB stage;
            stage.set_type(StagePB::EXTERNAL);
            stage.set_stage_id("ex_id_" + std::to_string(i));
            stage.set_name("ex_name_" + std::to_string(i));
            stage.mutable_obj_info()->CopyFrom(obj);

            CreateStageRequest create_stage_req;
            create_stage_req.set_cloud_unique_id(cloud_unique_id);
            create_stage_req.mutable_stage()->CopyFrom(stage);

            CreateStageResponse create_stage_res;
            meta_service->create_stage(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                       &create_stage_req, &create_stage_res, nullptr);
            ASSERT_EQ(create_stage_res.status().code(), MetaServiceCode::OK);
        }

        // get an external stage with name
        {
            GetStageResponse res;
            meta_service->get_stage(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &get_stage_req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            ASSERT_EQ(1, res.stage().size());
            ASSERT_EQ("ex_id_1", res.stage().at(0).stage_id());
        }

        GetStageRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_type(StagePB::EXTERNAL);
        // get all stages
        {
            GetStageResponse res;
            meta_service->get_stage(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            ASSERT_EQ(2, res.stage().size());
            ASSERT_EQ("ex_id_0", res.stage().at(0).stage_id());
            ASSERT_EQ("ex_id_1", res.stage().at(1).stage_id());
        }

        // drop one stage
        {
            DropStageRequest drop_stage_req;
            drop_stage_req.set_cloud_unique_id(cloud_unique_id);
            drop_stage_req.set_type(StagePB::EXTERNAL);
            drop_stage_req.set_stage_name("tmp");
            DropStageResponse res;
            meta_service->drop_stage(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &drop_stage_req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::STAGE_NOT_FOUND);

            drop_stage_req.set_stage_name("ex_name_1");
            meta_service->drop_stage(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &drop_stage_req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

            // get all stage
            GetStageResponse get_stage_res;
            meta_service->get_stage(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &get_stage_res, nullptr);
            ASSERT_EQ(get_stage_res.status().code(), MetaServiceCode::OK);
            ASSERT_EQ(1, get_stage_res.stage().size());
            ASSERT_EQ("ex_name_0", get_stage_res.stage().at(0).name());
        }
    }
    sp->clear_all_call_backs();
    sp->clear_trace();
    sp->disable_processing();
}

TEST(MetaServiceTest, GetIamTest) {
    auto meta_service = get_meta_service();
    brpc::Controller cntl;
    auto cloud_unique_id = "test_cloud_unique_id";
    std::string instance_id = "get_iam_test_instance_id";
    [[maybe_unused]] auto sp = SyncPoint::get_instance();
    std::unique_ptr<int, std::function<void(int*)>> defer(
            (int*)0x01, [](int*) { SyncPoint::get_instance()->clear_all_call_backs(); });
    sp->set_call_back("get_instance_id::pred", [](void* p) { *((bool*)p) = true; });
    sp->set_call_back("get_instance_id", [&](void* p) { *((std::string*)p) = instance_id; });
    sp->set_call_back("encrypt_ak_sk:get_encryption_key_ret",
                      [](void* p) { *reinterpret_cast<int*>(p) = 0; });
    sp->set_call_back("encrypt_ak_sk:get_encryption_key",
                      [](void* p) { *reinterpret_cast<std::string*>(p) = "test"; });
    sp->set_call_back("encrypt_ak_sk:get_encryption_key_id",
                      [](void* p) { *reinterpret_cast<int*>(p) = 1; });
    sp->set_call_back("decrypt_ak_sk:get_encryption_key_ret",
                      [](void* p) { *reinterpret_cast<int*>(p) = 0; });
    sp->set_call_back("decrypt_ak_sk:get_encryption_key",
                      [](void* p) { *reinterpret_cast<std::string*>(p) = "test"; });
    sp->enable_processing();

    config::arn_id = "iam_arn";
    config::arn_ak = "iam_ak";
    config::arn_sk = "iam_sk";

    // create instance
    {
        ObjectStoreInfoPB obj;
        obj.set_ak("123");
        obj.set_sk("321");
        obj.set_bucket("456");
        obj.set_prefix("654");
        obj.set_endpoint("789");
        obj.set_region("987");
        obj.set_external_endpoint("888");
        obj.set_provider(ObjectStoreInfoPB::BOS);

        RamUserPB ram_user;
        ram_user.set_user_id("test_user_id");
        ram_user.set_ak("test_ak");
        ram_user.set_sk("test_sk");

        CreateInstanceRequest req;
        req.set_instance_id(instance_id);
        req.set_user_id("test_user");
        req.set_name("test_name");
        req.mutable_ram_user()->CopyFrom(ram_user);
        req.mutable_obj_info()->CopyFrom(obj);

        CreateInstanceResponse res;
        meta_service->create_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    GetIamRequest request;
    request.set_cloud_unique_id(cloud_unique_id);
    GetIamResponse response;
    meta_service->get_iam(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &request,
                          &response, nullptr);
    ASSERT_EQ(response.status().code(), MetaServiceCode::OK);
    ASSERT_EQ(response.ram_user().user_id(), "test_user_id");
    ASSERT_EQ(response.ram_user().ak(), "test_ak");
    ASSERT_EQ(response.ram_user().sk(), "test_sk");
    ASSERT_TRUE(response.ram_user().external_id().empty());

    ASSERT_EQ(response.iam_user().user_id(), "iam_arn");
    ASSERT_EQ(response.iam_user().external_id(), instance_id);
    ASSERT_EQ(response.iam_user().ak(), "iam_ak");
    ASSERT_EQ(response.iam_user().sk(), "iam_sk");
    sp->clear_all_call_backs();
    sp->clear_trace();
    sp->disable_processing();
}

TEST(MetaServiceTest, AlterIamTest) {
    auto meta_service = get_meta_service();
    brpc::Controller cntl;
    auto cloud_unique_id = "test_cloud_unique_id";
    std::string instance_id = "alter_iam_test_instance_id";
    [[maybe_unused]] auto sp = SyncPoint::get_instance();
    std::unique_ptr<int, std::function<void(int*)>> defer(
            (int*)0x01, [](int*) { SyncPoint::get_instance()->clear_all_call_backs(); });
    sp->set_call_back("get_instance_id::pred", [](void* p) { *((bool*)p) = true; });
    sp->set_call_back("get_instance_id", [&](void* p) { *((std::string*)p) = instance_id; });
    sp->set_call_back("encrypt_ak_sk:get_encryption_key_ret",
                      [](void* p) { *reinterpret_cast<int*>(p) = 0; });
    sp->set_call_back("encrypt_ak_sk:get_encryption_key",
                      [](void* p) { *reinterpret_cast<std::string*>(p) = "test"; });
    sp->set_call_back("encrypt_ak_sk:get_encryption_key_id",
                      [](void* p) { *reinterpret_cast<int*>(p) = 1; });
    sp->set_call_back("decrypt_ak_sk:get_encryption_key_ret",
                      [](void* p) { *reinterpret_cast<int*>(p) = 0; });
    sp->set_call_back("decrypt_ak_sk:get_encryption_key",
                      [](void* p) { *reinterpret_cast<std::string*>(p) = "test"; });
    sp->enable_processing();

    config::arn_id = "iam_arn";
    config::arn_ak = "iam_ak";
    config::arn_sk = "iam_sk";

    ObjectStoreInfoPB obj;
    obj.set_ak("123");
    obj.set_sk("321");
    obj.set_bucket("456");
    obj.set_prefix("654");
    obj.set_endpoint("789");
    obj.set_region("987");
    obj.set_external_endpoint("888");
    obj.set_provider(ObjectStoreInfoPB::BOS);

    // create instance without ram user
    CreateInstanceRequest create_instance_req;
    create_instance_req.set_instance_id(instance_id);
    create_instance_req.set_user_id("test_user");
    create_instance_req.set_name("test_name");
    create_instance_req.mutable_obj_info()->CopyFrom(obj);
    CreateInstanceResponse create_instance_res;
    meta_service->create_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                  &create_instance_req, &create_instance_res, nullptr);
    ASSERT_EQ(create_instance_res.status().code(), MetaServiceCode::OK);

    // get iam and ram user
    GetIamRequest request;
    request.set_cloud_unique_id(cloud_unique_id);
    GetIamResponse response;
    meta_service->get_iam(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &request,
                          &response, nullptr);
    ASSERT_EQ(response.status().code(), MetaServiceCode::OK);
    ASSERT_EQ(response.has_ram_user(), false);
    ASSERT_EQ(response.iam_user().user_id(), "iam_arn");
    ASSERT_EQ(response.iam_user().ak(), "iam_ak");
    ASSERT_EQ(response.iam_user().sk(), "iam_sk");

    // alter ram user
    RamUserPB ram_user;
    ram_user.set_user_id("test_user_id");
    ram_user.set_ak("test_ak");
    ram_user.set_sk("test_sk");
    AlterRamUserRequest alter_ram_user_request;
    alter_ram_user_request.set_instance_id(instance_id);
    alter_ram_user_request.mutable_ram_user()->CopyFrom(ram_user);
    AlterRamUserResponse alter_ram_user_response;
    meta_service->alter_ram_user(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                 &alter_ram_user_request, &alter_ram_user_response, nullptr);

    // get iam and ram user
    meta_service->get_iam(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &request,
                          &response, nullptr);
    ASSERT_EQ(response.status().code(), MetaServiceCode::OK);
    ASSERT_EQ(response.has_ram_user(), true);
    ASSERT_EQ(response.ram_user().user_id(), "test_user_id");
    ASSERT_EQ(response.ram_user().ak(), "test_ak");
    ASSERT_EQ(response.ram_user().sk(), "test_sk");
    sp->clear_all_call_backs();
    sp->clear_trace();
    sp->disable_processing();
}

std::string to_raw_string(std::string_view v) {
    std::string ret;
    ret.reserve(v.size() / 1.5);
    while (!v.empty()) {
        if (v[0] == '\\') {
            if (v[1] == 'x') {
                ret.push_back(unhex(std::string_view {v.data() + 2, 2})[0]);
                v.remove_prefix(4);
            } else if (v[1] == '\\') {
                ret.push_back('\\');
                v.remove_prefix(2);
            } else {
                std::abort();
            }
            continue;
        }
        ret.push_back(v[0]);
        v.remove_prefix(1);
    }
    return ret;
}

TEST(MetaServiceTest, DecodeTest) {
    // 504
    std::string v1 =
            R"(\x08\x00\x10\xa0[\x18\xb3[ \xde\xc5\xa4\x8e\xbd\xf0\x97\xc62(\xf4\x96\xe6\xb0\x070\x018\x02@\x02H\x0bX\x05`\xa0\x07h\xa0\x07p\xa0\x01\x88\x01\x00\xa0\x01\x86\x8b\x9a\x9b\x06\xaa\x01\x16\x08\xe6\x9e\x91\xa3\xfb\xbe\xf5\xf0\xc4\x01\x10\xfe\x8b\x90\xa7\xb5\xec\xd5\xc8\xbf\x01\xb0\x01\x01\xba\x0100200000000000071fb4aabb58c570cbcadb10857d3131b97\xc2\x01\x011\xc8\x01\x84\x8b\x9a\x9b\x06\xd0\x01\x85\x8b\x9a\x9b\x06\xda\x01\x04\x0a\x00\x12\x00\xe2\x01\xcd\x02\x08\x02\x121\x08\x00\x12\x06datek1\x1a\x04DATE \x01*\x04NONE0\x01:\x0a2022-01-01@\x00H\x00P\x03X\x03\x80\x01\x01\x12>\x08\x01\x12\x06datek2\x1a\x08DATETIME \x01*\x04NONE0\x01:\x132022-01-01 11:11:11@\x00H\x00P\x08X\x08\x80\x01\x01\x123\x08\x04\x12\x06datev3\x1a\x06DATEV2 \x01*\x04NONE0\x01:\x0a2022-01-01@\x00H\x00P\x04X\x04\x80\x01\x01\x120\x08\x02\x12\x06datev1\x1a\x04DATE \x00*\x03MAX0\x01:\x0a2022-01-01@\x00H\x00P\x03X\x03\x80\x01\x01\x12=\x08\x03\x12\x06datev2\x1a\x08DATETIME \x00*\x03MAX0\x01:\x132022-01-01 11:11:11@\x00H\x00P\x08X\x08\x80\x01\x01\x18\x03 \x80\x08(\x021\x00\x00\x00\x00\x00\x00\x00\x008\x00@\xff\xff\xff\xff\xff\xff\xff\xff\xff\x01H\xff\xff\xff\xff\xff\xff\xff\xff\xff\x01P\x00X\x02`\x05h\x00p\x00\xe8\x01\x85\xae\x9f\x9b\x06\x98\x03\x02)";
    std::string val1 = to_raw_string(v1);
    std::cout << "val1 size " << val1.size() << std::endl;

    // 525
    std::string v2 =
            R"(\x08\x00\x10\xa0[\x18\xb3[ \x80\xb0\x85\xe3\xda\xcc\x8c\x0f(\xf4\x96\xe6\xb0\x070\x018\x01@\x0cH\x0cX\x00`\x00h\x00p\x00\x82\x01\x1e\x08\xff\xff\xff\xff\xff\xff\xff\xff\xff\x01\x12\x11datev3=2022-01-01\x88\x01\x01\x92\x01\x04\x08\x00\x10\x00\xa0\x01\x87\x8b\x9a\x9b\x06\xaa\x01\x16\x08\xe6\x9e\x91\xa3\xfb\xbe\xf5\xf0\xc4\x01\x10\xfe\x8b\x90\xa7\xb5\xec\xd5\xc8\xbf\x01\xb0\x01\x00\xba\x0100200000000000072fb4aabb58c570cbcadb10857d3131b97\xc8\x01\x87\x8b\x9a\x9b\x06\xd0\x01\x87\x8b\x9a\x9b\x06\xe2\x01\xcd\x02\x08\x02\x121\x08\x00\x12\x06datek1\x1a\x04DATE \x01*\x04NONE0\x01:\x0a2022-01-01@\x00H\x00P\x03X\x03\x80\x01\x01\x12>\x08\x01\x12\x06datek2\x1a\x08DATETIME \x01*\x04NONE0\x01:\x132022-01-01 11:11:11@\x00H\x00P\x08X\x08\x80\x01\x01\x123\x08\x04\x12\x06datev3\x1a\x06DATEV2 \x01*\x04NONE0\x01:\x0a2022-01-01@\x00H\x00P\x04X\x04\x80\x01\x01\x120\x08\x02\x12\x06datev1\x1a\x04DATE \x00*\x03MAX0\x01:\x0a2022-01-01@\x00H\x00P\x03X\x03\x80\x01\x01\x12=\x08\x03\x12\x06datev2\x1a\x08DATETIME \x00*\x03MAX0\x01:\x132022-01-01 11:11:11@\x00H\x00P\x08X\x08\x80\x01\x01\x18\x03 \x80\x08(\x021\x00\x00\x00\x00\x00\x00\x00\x008\x00@\xff\xff\xff\xff\xff\xff\xff\xff\xff\x01H\xff\xff\xff\xff\xff\xff\xff\xff\xff\x01P\x00X\x02`\x05h\x00p\x00\xe8\x01\x00\x98\x03\x02)";
    std::string val2 = to_raw_string(v2);
    std::cout << "val2 size " << val2.size() << std::endl;

    [[maybe_unused]] std::string key1(
            "\x01\x10meta\x00\x01\x10selectdb-cloud-"
            "dev\x00\x01\x10rowset\x00\x01\x12\x00\x00\x00\x00\x00\x00-"
            "\xb3\x12\x00\x00\x00\x00\x00\x00\x00\x0b",
            56);
    [[maybe_unused]] std::string key2(
            "\x01\x10meta\x00\x01\x10selectdb-cloud-"
            "dev\x00\x01\x10rowset\x00\x01\x12\x00\x00\x00\x00\x00\x00-"
            "\xb3\x12\x00\x00\x00\x00\x00\x00\x00\x0c",
            56);
    std::cout << "key1 " << key1.size() << " " << hex(key1) << std::endl;
    std::cout << "key2 " << key2.size() << " " << hex(key2) << std::endl;

    doris::RowsetMetaPB rowset1;
    doris::RowsetMetaPB rowset2;

    rowset1.ParseFromString(val1);
    rowset2.ParseFromString(val2);
    std::cout << "rowset1=" << proto_to_json(rowset1) << std::endl;
    std::cout << "rowset2=" << proto_to_json(rowset2) << std::endl;
}

static void get_tablet_stats(MetaServiceImpl* meta_service, int64_t table_id, int64_t index_id,
                             int64_t partition_id, int64_t tablet_id, GetTabletStatsResponse& res) {
    brpc::Controller cntl;
    GetTabletStatsRequest req;
    auto idx = req.add_tablet_idx();
    idx->set_table_id(table_id);
    idx->set_index_id(index_id);
    idx->set_partition_id(partition_id);
    idx->set_tablet_id(tablet_id);
    meta_service->get_tablet_stats(&cntl, &req, &res, nullptr);
}

TEST(MetaServiceTest, GetTabletStatsTest) {
    auto meta_service = get_meta_service();

    constexpr auto table_id = 10001, index_id = 10002, partition_id = 10003, tablet_id = 10004;
    ASSERT_NO_FATAL_FAILURE(
            create_tablet(meta_service.get(), table_id, index_id, partition_id, tablet_id));
    GetTabletStatsResponse res;
    get_tablet_stats(meta_service.get(), table_id, index_id, partition_id, tablet_id, res);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    ASSERT_EQ(res.tablet_stats_size(), 1);
    EXPECT_EQ(res.tablet_stats(0).data_size(), 0);
    EXPECT_EQ(res.tablet_stats(0).num_rows(), 0);
    EXPECT_EQ(res.tablet_stats(0).num_rowsets(), 1);
    EXPECT_EQ(res.tablet_stats(0).num_segments(), 0);
    // Insert rowset
    config::split_tablet_stats = false;
    ASSERT_NO_FATAL_FAILURE(
            insert_rowset(meta_service.get(), 10000, "label1", table_id, partition_id, tablet_id));
    ASSERT_NO_FATAL_FAILURE(
            insert_rowset(meta_service.get(), 10000, "label2", table_id, partition_id, tablet_id));
    config::split_tablet_stats = true;
    ASSERT_NO_FATAL_FAILURE(
            insert_rowset(meta_service.get(), 10000, "label3", table_id, partition_id, tablet_id));
    ASSERT_NO_FATAL_FAILURE(
            insert_rowset(meta_service.get(), 10000, "label4", table_id, partition_id, tablet_id));
    // Check tablet stats kv
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(meta_service->txn_kv_->create_txn(&txn), 0);
    std::string data_size_key, data_size_val;
    stats_tablet_data_size_key({mock_instance, table_id, index_id, partition_id, tablet_id},
                               &data_size_key);
    ASSERT_EQ(txn->get(data_size_key, &data_size_val), 0);
    EXPECT_EQ(*(int64_t*)data_size_val.data(), 20000);
    std::string num_rows_key, num_rows_val;
    stats_tablet_num_rows_key({mock_instance, table_id, index_id, partition_id, tablet_id},
                              &num_rows_key);
    ASSERT_EQ(txn->get(num_rows_key, &num_rows_val), 0);
    EXPECT_EQ(*(int64_t*)num_rows_val.data(), 200);
    std::string num_rowsets_key, num_rowsets_val;
    stats_tablet_num_rowsets_key({mock_instance, table_id, index_id, partition_id, tablet_id},
                                 &num_rowsets_key);
    ASSERT_EQ(txn->get(num_rowsets_key, &num_rowsets_val), 0);
    EXPECT_EQ(*(int64_t*)num_rowsets_val.data(), 2);
    std::string num_segs_key, num_segs_val;
    stats_tablet_num_segs_key({mock_instance, table_id, index_id, partition_id, tablet_id},
                              &num_segs_key);
    ASSERT_EQ(txn->get(num_segs_key, &num_segs_val), 0);
    EXPECT_EQ(*(int64_t*)num_segs_val.data(), 2);
    // Get tablet stats
    res.Clear();
    get_tablet_stats(meta_service.get(), table_id, index_id, partition_id, tablet_id, res);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    ASSERT_EQ(res.tablet_stats_size(), 1);
    EXPECT_EQ(res.tablet_stats(0).data_size(), 40000);
    EXPECT_EQ(res.tablet_stats(0).num_rows(), 400);
    EXPECT_EQ(res.tablet_stats(0).num_rowsets(), 5);
    EXPECT_EQ(res.tablet_stats(0).num_segments(), 4);
}

TEST(MetaServiceTest, GetDeleteBitmapUpdateLock) {
    auto meta_service = get_meta_service();

    brpc::Controller cntl;
    GetDeleteBitmapUpdateLockRequest req;
    GetDeleteBitmapUpdateLockResponse res;
    req.set_cloud_unique_id("test_cloud_unique_id");
    req.set_table_id(111);
    req.add_partition_ids(123);
    req.set_expiration(5);
    req.set_lock_id(888);
    req.set_initiator(-1);
    meta_service->get_delete_bitmap_update_lock(
            reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

    // same lock_id
    meta_service->get_delete_bitmap_update_lock(
            reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

    // different lock_id
    req.set_lock_id(999);
    meta_service->get_delete_bitmap_update_lock(
            reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::LOCK_CONFLICT);

    // lock expired
    req.set_cloud_unique_id("test_cloud_unique_id");
    req.set_table_id(222);
    req.set_expiration(0);
    req.set_lock_id(666);
    meta_service->get_delete_bitmap_update_lock(
            reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    sleep(1);
    req.set_lock_id(667);
    meta_service->get_delete_bitmap_update_lock(
            reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
}

static std::string generate_random_string(int length) {
    std::string char_set = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    std::random_device rd;
    std::mt19937 generator(rd());
    std::uniform_int_distribution<int> distribution(0, char_set.length() - 1);

    std::string randomString;
    for (int i = 0; i < length; ++i) {
        randomString += char_set[distribution(generator)];
    }
    return randomString;
}

TEST(MetaServiceTest, UpdateDeleteBitmap) {
    auto meta_service = get_meta_service();

    // get delete bitmap update lock
    brpc::Controller cntl;
    GetDeleteBitmapUpdateLockRequest get_lock_req;
    GetDeleteBitmapUpdateLockResponse get_lock_res;
    get_lock_req.set_cloud_unique_id("test_cloud_unique_id");
    get_lock_req.set_table_id(112);
    get_lock_req.add_partition_ids(123);
    get_lock_req.set_expiration(5);
    get_lock_req.set_lock_id(888);
    get_lock_req.set_initiator(-1);
    meta_service->get_delete_bitmap_update_lock(
            reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &get_lock_req,
            &get_lock_res, nullptr);
    ASSERT_EQ(get_lock_res.status().code(), MetaServiceCode::OK);

    // first update delete bitmap
    UpdateDeleteBitmapRequest update_delete_bitmap_req;
    UpdateDeleteBitmapResponse update_delete_bitmap_res;
    update_delete_bitmap_req.set_cloud_unique_id("test_cloud_unique_id");
    update_delete_bitmap_req.set_table_id(112);
    update_delete_bitmap_req.set_partition_id(123);
    update_delete_bitmap_req.set_lock_id(888);
    update_delete_bitmap_req.set_initiator(-1);
    update_delete_bitmap_req.set_tablet_id(333);

    update_delete_bitmap_req.add_rowset_ids("123");
    update_delete_bitmap_req.add_segment_ids(1);
    update_delete_bitmap_req.add_versions(2);
    update_delete_bitmap_req.add_segment_delete_bitmaps("abc0");

    update_delete_bitmap_req.add_rowset_ids("123");
    update_delete_bitmap_req.add_segment_ids(0);
    update_delete_bitmap_req.add_versions(3);
    update_delete_bitmap_req.add_segment_delete_bitmaps("abc1");

    update_delete_bitmap_req.add_rowset_ids("123");
    update_delete_bitmap_req.add_segment_ids(1);
    update_delete_bitmap_req.add_versions(3);
    update_delete_bitmap_req.add_segment_delete_bitmaps("abc2");

    update_delete_bitmap_req.add_rowset_ids("124");
    update_delete_bitmap_req.add_segment_ids(0);
    update_delete_bitmap_req.add_versions(2);
    update_delete_bitmap_req.add_segment_delete_bitmaps("abc3");

    std::string large_value = generate_random_string(300 * 1000);
    update_delete_bitmap_req.add_rowset_ids("124");
    update_delete_bitmap_req.add_segment_ids(1);
    update_delete_bitmap_req.add_versions(2);
    update_delete_bitmap_req.add_segment_delete_bitmaps(large_value);

    update_delete_bitmap_req.add_rowset_ids("124");
    update_delete_bitmap_req.add_segment_ids(0);
    update_delete_bitmap_req.add_versions(3);
    update_delete_bitmap_req.add_segment_delete_bitmaps("abc4");

    meta_service->update_delete_bitmap(reinterpret_cast<google::protobuf::RpcController*>(&cntl),
                                       &update_delete_bitmap_req, &update_delete_bitmap_res,
                                       nullptr);
    ASSERT_EQ(update_delete_bitmap_res.status().code(), MetaServiceCode::OK);

    // first get delete bitmap
    GetDeleteBitmapRequest get_delete_bitmap_req;
    GetDeleteBitmapResponse get_delete_bitmap_res;
    get_delete_bitmap_req.set_cloud_unique_id("test_cloud_unique_id");
    get_delete_bitmap_req.set_tablet_id(333);

    get_delete_bitmap_req.add_rowset_ids("123");
    get_delete_bitmap_req.add_begin_versions(3);
    get_delete_bitmap_req.add_end_versions(3);

    get_delete_bitmap_req.add_rowset_ids("124");
    get_delete_bitmap_req.add_begin_versions(0);
    get_delete_bitmap_req.add_end_versions(3);

    meta_service->get_delete_bitmap(reinterpret_cast<google::protobuf::RpcController*>(&cntl),
                                    &get_delete_bitmap_req, &get_delete_bitmap_res, nullptr);
    ASSERT_EQ(get_delete_bitmap_res.status().code(), MetaServiceCode::OK);
    ASSERT_EQ(get_delete_bitmap_res.rowset_ids_size(), 5);
    ASSERT_EQ(get_delete_bitmap_res.segment_delete_bitmaps_size(), 5);
    ASSERT_EQ(get_delete_bitmap_res.versions_size(), 5);
    ASSERT_EQ(get_delete_bitmap_res.segment_delete_bitmaps_size(), 5);

    ASSERT_EQ(get_delete_bitmap_res.rowset_ids(0), "123");
    ASSERT_EQ(get_delete_bitmap_res.segment_ids(0), 0);
    ASSERT_EQ(get_delete_bitmap_res.versions(0), 3);
    ASSERT_EQ(get_delete_bitmap_res.segment_delete_bitmaps(0), "abc1");

    ASSERT_EQ(get_delete_bitmap_res.rowset_ids(1), "123");
    ASSERT_EQ(get_delete_bitmap_res.segment_ids(1), 1);
    ASSERT_EQ(get_delete_bitmap_res.versions(1), 3);
    ASSERT_EQ(get_delete_bitmap_res.segment_delete_bitmaps(1), "abc2");

    ASSERT_EQ(get_delete_bitmap_res.rowset_ids(2), "124");
    ASSERT_EQ(get_delete_bitmap_res.segment_ids(2), 0);
    ASSERT_EQ(get_delete_bitmap_res.versions(2), 2);
    ASSERT_EQ(get_delete_bitmap_res.segment_delete_bitmaps(2), "abc3");

    ASSERT_EQ(get_delete_bitmap_res.rowset_ids(3), "124");
    ASSERT_EQ(get_delete_bitmap_res.segment_ids(3), 1);
    ASSERT_EQ(get_delete_bitmap_res.versions(3), 2);
    ASSERT_EQ(get_delete_bitmap_res.segment_delete_bitmaps(3), large_value);

    ASSERT_EQ(get_delete_bitmap_res.rowset_ids(4), "124");
    ASSERT_EQ(get_delete_bitmap_res.segment_ids(4), 0);
    ASSERT_EQ(get_delete_bitmap_res.versions(4), 3);
    ASSERT_EQ(get_delete_bitmap_res.segment_delete_bitmaps(4), "abc4");

    // second update delete bitmap
    UpdateDeleteBitmapRequest update_delete_bitmap_req1;
    UpdateDeleteBitmapResponse update_delete_bitmap_res1;
    update_delete_bitmap_req1.set_cloud_unique_id("test_cloud_unique_id");
    update_delete_bitmap_req1.set_table_id(112);
    update_delete_bitmap_req1.set_partition_id(123);
    update_delete_bitmap_req1.set_lock_id(888);
    update_delete_bitmap_req1.set_initiator(-1);
    update_delete_bitmap_req1.set_tablet_id(333);

    std::string large_value1 = generate_random_string(200 * 1000);
    update_delete_bitmap_req1.add_rowset_ids("123");
    update_delete_bitmap_req1.add_segment_ids(0);
    update_delete_bitmap_req1.add_versions(2);
    update_delete_bitmap_req1.add_segment_delete_bitmaps(large_value1);

    update_delete_bitmap_req1.add_rowset_ids("123");
    update_delete_bitmap_req1.add_segment_ids(1);
    update_delete_bitmap_req1.add_versions(2);
    update_delete_bitmap_req1.add_segment_delete_bitmaps("bbb0");

    update_delete_bitmap_req1.add_rowset_ids("123");
    update_delete_bitmap_req1.add_segment_ids(1);
    update_delete_bitmap_req1.add_versions(3);
    update_delete_bitmap_req1.add_segment_delete_bitmaps("bbb1");

    update_delete_bitmap_req1.add_rowset_ids("124");
    update_delete_bitmap_req1.add_segment_ids(1);
    update_delete_bitmap_req1.add_versions(3);
    update_delete_bitmap_req1.add_segment_delete_bitmaps("bbb2");

    meta_service->update_delete_bitmap(reinterpret_cast<google::protobuf::RpcController*>(&cntl),
                                       &update_delete_bitmap_req1, &update_delete_bitmap_res1,
                                       nullptr);
    ASSERT_EQ(update_delete_bitmap_res1.status().code(), MetaServiceCode::OK);

    // second get delete bitmap
    GetDeleteBitmapRequest get_delete_bitmap_req1;
    GetDeleteBitmapResponse get_delete_bitmap_res1;
    get_delete_bitmap_req1.set_cloud_unique_id("test_cloud_unique_id");
    get_delete_bitmap_req1.set_tablet_id(333);

    get_delete_bitmap_req1.add_rowset_ids("123");
    get_delete_bitmap_req1.add_begin_versions(0);
    get_delete_bitmap_req1.add_end_versions(3);

    get_delete_bitmap_req1.add_rowset_ids("124");
    get_delete_bitmap_req1.add_begin_versions(0);
    get_delete_bitmap_req1.add_end_versions(3);

    meta_service->get_delete_bitmap(reinterpret_cast<google::protobuf::RpcController*>(&cntl),
                                    &get_delete_bitmap_req1, &get_delete_bitmap_res1, nullptr);
    ASSERT_EQ(get_delete_bitmap_res1.status().code(), MetaServiceCode::OK);
    ASSERT_EQ(get_delete_bitmap_res1.rowset_ids_size(), 4);
    ASSERT_EQ(get_delete_bitmap_res1.segment_delete_bitmaps_size(), 4);
    ASSERT_EQ(get_delete_bitmap_res1.versions_size(), 4);
    ASSERT_EQ(get_delete_bitmap_res1.segment_delete_bitmaps_size(), 4);

    ASSERT_EQ(get_delete_bitmap_res1.rowset_ids(0), "123");
    ASSERT_EQ(get_delete_bitmap_res1.segment_ids(0), 0);
    ASSERT_EQ(get_delete_bitmap_res1.versions(0), 2);
    ASSERT_EQ(get_delete_bitmap_res1.segment_delete_bitmaps(0), large_value1);

    ASSERT_EQ(get_delete_bitmap_res1.rowset_ids(1), "123");
    ASSERT_EQ(get_delete_bitmap_res1.segment_ids(1), 1);
    ASSERT_EQ(get_delete_bitmap_res1.versions(1), 2);
    ASSERT_EQ(get_delete_bitmap_res1.segment_delete_bitmaps(1), "bbb0");

    ASSERT_EQ(get_delete_bitmap_res1.rowset_ids(2), "123");
    ASSERT_EQ(get_delete_bitmap_res1.segment_ids(2), 1);
    ASSERT_EQ(get_delete_bitmap_res1.versions(2), 3);
    ASSERT_EQ(get_delete_bitmap_res1.segment_delete_bitmaps(2), "bbb1");

    ASSERT_EQ(get_delete_bitmap_res1.rowset_ids(3), "124");
    ASSERT_EQ(get_delete_bitmap_res1.segment_ids(3), 1);
    ASSERT_EQ(get_delete_bitmap_res1.versions(3), 3);
    ASSERT_EQ(get_delete_bitmap_res1.segment_delete_bitmaps(3), "bbb2");
}

TEST(MetaServiceTest, DeleteBimapCommitTxnTest) {
    auto meta_service = get_meta_service();
    extern std::string get_instance_id(const std::shared_ptr<ResourceManager>& rc_mgr,
                                       const std::string& cloud_unique_id);
    auto instance_id = get_instance_id(meta_service->resource_mgr_, "test_cloud_unique_id");

    // case: first version of rowset
    {
        int64_t txn_id = -1;
        int64_t table_id = 123456; // same as table_id of tmp rowset
        int64_t db_id = 222;
        int64_t tablet_id_base = 8113;
        int64_t partition_id = 1234;
        // begin txn
        {
            brpc::Controller cntl;
            BeginTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            TxnInfoPB txn_info_pb;
            txn_info_pb.set_db_id(db_id);
            txn_info_pb.set_label("test_label");
            txn_info_pb.add_table_ids(table_id);
            txn_info_pb.set_timeout_ms(36000);
            req.mutable_txn_info()->CopyFrom(txn_info_pb);
            BeginTxnResponse res;
            meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            txn_id = res.txn_id();
        }

        // mock rowset and tablet
        for (int i = 0; i < 5; ++i) {
            create_tablet(meta_service.get(), table_id, 1235, partition_id, tablet_id_base + i);
            auto tmp_rowset = create_rowset(txn_id, tablet_id_base + i);
            tmp_rowset.set_partition_id(partition_id);
            CreateRowsetResponse res;
            commit_rowset(meta_service.get(), tmp_rowset, res);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        }

        // update delete bitmap
        {
            // get delete bitmap update lock
            brpc::Controller cntl;
            GetDeleteBitmapUpdateLockRequest get_lock_req;
            GetDeleteBitmapUpdateLockResponse get_lock_res;
            get_lock_req.set_cloud_unique_id("test_cloud_unique_id");
            get_lock_req.set_table_id(table_id);
            get_lock_req.add_partition_ids(partition_id);
            get_lock_req.set_expiration(5);
            get_lock_req.set_lock_id(txn_id);
            get_lock_req.set_initiator(-1);
            meta_service->get_delete_bitmap_update_lock(
                    reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &get_lock_req,
                    &get_lock_res, nullptr);
            ASSERT_EQ(get_lock_res.status().code(), MetaServiceCode::OK);

            // first update delete bitmap
            UpdateDeleteBitmapRequest update_delete_bitmap_req;
            UpdateDeleteBitmapResponse update_delete_bitmap_res;
            update_delete_bitmap_req.set_cloud_unique_id("test_cloud_unique_id");
            update_delete_bitmap_req.set_table_id(table_id);
            update_delete_bitmap_req.set_partition_id(partition_id);
            update_delete_bitmap_req.set_lock_id(txn_id);
            update_delete_bitmap_req.set_initiator(-1);
            update_delete_bitmap_req.set_tablet_id(tablet_id_base);

            update_delete_bitmap_req.add_rowset_ids("123");
            update_delete_bitmap_req.add_segment_ids(1);
            update_delete_bitmap_req.add_versions(2);
            update_delete_bitmap_req.add_segment_delete_bitmaps("abc0");

            meta_service->update_delete_bitmap(
                    reinterpret_cast<google::protobuf::RpcController*>(&cntl),
                    &update_delete_bitmap_req, &update_delete_bitmap_res, nullptr);
            ASSERT_EQ(update_delete_bitmap_res.status().code(), MetaServiceCode::OK);
        }

        // check delete bitmap update lock and pending delete bitmap
        {
            std::unique_ptr<Transaction> txn;
            ASSERT_EQ(meta_service->txn_kv_->create_txn(&txn), 0);
            std::string lock_key = meta_delete_bitmap_update_lock_key({instance_id, table_id, -1});
            std::string lock_val;
            auto ret = txn->get(lock_key, &lock_val);
            ASSERT_EQ(ret, 0);

            std::string pending_key = meta_pending_delete_bitmap_key({instance_id, tablet_id_base});
            std::string pending_val;
            ret = txn->get(pending_key, &pending_val);
            ASSERT_EQ(ret, 0);
        }

        // commit txn
        {
            brpc::Controller cntl;
            CommitTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            req.set_db_id(db_id);
            req.set_txn_id(txn_id);
            req.add_mow_table_ids(table_id);
            CommitTxnResponse res;
            meta_service->commit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        }

        // check delete bitmap update lock and pending delete bitmap
        {
            std::unique_ptr<Transaction> txn;
            ASSERT_EQ(meta_service->txn_kv_->create_txn(&txn), 0);
            std::string lock_key = meta_delete_bitmap_update_lock_key({instance_id, table_id, -1});
            std::string lock_val;
            auto ret = txn->get(lock_key, &lock_val);
            ASSERT_EQ(ret, 1);

            std::string pending_key = meta_pending_delete_bitmap_key({instance_id, tablet_id_base});
            std::string pending_val;
            ret = txn->get(pending_key, &pending_val);
            ASSERT_EQ(ret, 1);
        }
    }
}

TEST(MetaServiceTest, GetVersion) {
    auto service = get_meta_service();

    int64_t table_id = 1;
    int64_t partition_id = 1;
    int64_t tablet_id = 1;

    {
        brpc::Controller ctrl;
        GetVersionRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_db_id(1);
        req.set_table_id(table_id);
        req.set_partition_id(partition_id);

        GetVersionResponse resp;
        service->get_version(&ctrl, &req, &resp, nullptr);

        ASSERT_EQ(resp.status().code(), MetaServiceCode::VERSION_NOT_FOUND)
                << " status is " << resp.status().DebugString();
    }

    create_tablet(service.get(), table_id, 1, partition_id, tablet_id);
    insert_rowset(service.get(), 1, "get_version_label_1", table_id, partition_id, tablet_id);

    {
        brpc::Controller ctrl;
        GetVersionRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_db_id(1);
        req.set_table_id(table_id);
        req.set_partition_id(partition_id);

        GetVersionResponse resp;
        service->get_version(&ctrl, &req, &resp, nullptr);

        ASSERT_EQ(resp.status().code(), MetaServiceCode::OK)
                << " status is " << resp.status().DebugString();
        ASSERT_EQ(resp.version(), 2);
    }
}

TEST(MetaServiceTest, BatchGetVersion) {
    struct TestCase {
        std::vector<int64_t> table_ids;
        std::vector<int64_t> partition_ids;
        std::vector<int64_t> expected_versions;
        std::vector<
                std::tuple<int64_t /*table_id*/, int64_t /*partition_id*/, int64_t /*tablet_id*/>>
                insert_rowsets;
    };

    std::vector<TestCase> cases = {
            // all version are missing
            {{1, 1, 2, 3}, {1, 2, 1, 2}, {-1, -1, -1, -1}, {}},
            // update table 1, partition 1
            {{1, 1, 2, 3}, {1, 2, 1, 2}, {2, -1, -1, -1}, {{1, 1, 1}}},
            // update table 2, partition 1
            // update table 3, partition 2
            {{1, 1, 2, 3}, {1, 2, 1, 2}, {2, -1, 2, 2}, {{2, 1, 3}, {3, 2, 4}}},
            // update table 1, partition 2 twice
            {{1, 1, 2, 3}, {1, 2, 1, 2}, {2, 3, 2, 2}, {{1, 2, 2}, {1, 2, 2}}},
    };

    auto service = get_meta_service();
    create_tablet(service.get(), 1, 1, 1, 1);
    create_tablet(service.get(), 1, 1, 2, 2);
    create_tablet(service.get(), 2, 1, 1, 3);
    create_tablet(service.get(), 3, 1, 2, 4);

    size_t num_cases = cases.size();
    size_t label_index = 0;
    for (size_t i = 0; i < num_cases; ++i) {
        auto& [table_ids, partition_ids, expected_versions, insert_rowsets] = cases[i];
        for (auto [table_id, partition_id, tablet_id] : insert_rowsets) {
            LOG(INFO) << "insert rowset for table " << table_id << " partition " << partition_id
                      << " table_id " << tablet_id;
            insert_rowset(service.get(), 1, std::to_string(++label_index), table_id, partition_id,
                          tablet_id);
        }

        brpc::Controller ctrl;
        GetVersionRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_db_id(-1);
        req.set_table_id(-1);
        req.set_partition_id(-1);
        req.set_batch_mode(true);
        for (size_t i = 0; i < table_ids.size(); ++i) req.add_db_ids(1);
        std::copy(table_ids.begin(), table_ids.end(),
                  google::protobuf::RepeatedFieldBackInserter(req.mutable_table_ids()));
        std::copy(partition_ids.begin(), partition_ids.end(),
                  google::protobuf::RepeatedFieldBackInserter(req.mutable_partition_ids()));

        GetVersionResponse resp;
        service->get_version(&ctrl, &req, &resp, nullptr);

        ASSERT_EQ(resp.status().code(), MetaServiceCode::OK)
                << "case " << i << " status is " << resp.status().msg()
                << ", code=" << resp.status().code();

        std::vector<int64_t> versions(resp.versions().begin(), resp.versions().end());
        EXPECT_EQ(versions, expected_versions) << "case " << i;
    }
}

TEST(MetaServiceTest, BatchGetVersionFallback) {
    constexpr size_t N = 100;
    size_t i = 0;
    auto sp = SyncPoint::get_instance();
    std::unique_ptr<int, std::function<void(int*)>> defer(
            (int*)0x01, [](int*) { SyncPoint::get_instance()->clear_all_call_backs(); });
    sp->set_call_back("batch_get_version_ret", [&](void* args) {
        if (i++ == N / 10) {
            *reinterpret_cast<int*>(args) = -2;
        }
    });

    sp->enable_processing();

    auto service = get_meta_service();
    for (int64_t i = 1; i <= N; ++i) {
        create_tablet(service.get(), 1, 1, i, i);
        insert_rowset(service.get(), 1, std::to_string(i), 1, i, i);
    }

    brpc::Controller ctrl;
    GetVersionRequest req;
    req.set_cloud_unique_id("test_cloud_unique_id");
    req.set_db_id(-1);
    req.set_table_id(-1);
    req.set_partition_id(-1);
    req.set_batch_mode(true);
    for (size_t i = 1; i <= N; ++i) {
        req.add_db_ids(1);
        req.add_table_ids(1);
        req.add_partition_ids(i);
    }

    GetVersionResponse resp;
    service->get_version(&ctrl, &req, &resp, nullptr);

    ASSERT_EQ(resp.status().code(), MetaServiceCode::OK)
            << "case " << i << " status is " << resp.status().msg()
            << ", code=" << resp.status().code();

    ASSERT_EQ(resp.versions_size(), N);
}

extern bool is_dropped_tablet(Transaction* txn, const std::string& instance_id, int64_t index_id,
                              int64_t partition_id);

TEST(MetaServiceTest, IsDroppedTablet) {
    auto meta_service = get_meta_service();
    std::string instance_id = "IsDroppedTablet";
    auto sp = SyncPoint::get_instance();
    std::unique_ptr<int, std::function<void(int*)>> defer(
            (int*)0x01, [](int*) { SyncPoint::get_instance()->clear_all_call_backs(); });
    sp->set_call_back("get_instance_id::pred", [](void* p) { *((bool*)p) = true; });
    sp->set_call_back("get_instance_id", [&](void* p) { *((std::string*)p) = instance_id; });
    sp->enable_processing();

    auto& txn_kv = meta_service->txn_kv_;
    auto reset_txn_kv = [&txn_kv] {
        txn_kv = std::make_shared<MemTxnKv>();
        txn_kv->init();
    };

    constexpr int64_t index_id = 10002;
    constexpr int64_t partition_id = 10003;

    std::unique_ptr<Transaction> txn;
    RecycleIndexPB index_pb;
    auto index_key = recycle_index_key({instance_id, index_id});
    RecyclePartitionPB partition_pb;
    auto partition_key = recycle_partition_key({instance_id, partition_id});
    std::string val;
    // No recycle index and partition kv
    ASSERT_EQ(txn_kv->create_txn(&txn), 0);
    EXPECT_FALSE(is_dropped_tablet(txn.get(), instance_id, index_id, partition_id));
    // Tablet in PREPARED index
    index_pb.set_state(RecycleIndexPB::PREPARED);
    val = index_pb.SerializeAsString();
    ASSERT_EQ(txn_kv->create_txn(&txn), 0);
    txn->put(index_key, val);
    ASSERT_EQ(txn->commit(), 0);
    ASSERT_EQ(txn_kv->create_txn(&txn), 0);
    EXPECT_FALSE(is_dropped_tablet(txn.get(), instance_id, index_id, partition_id));
    // Tablet in DROPPED/RECYCLING index
    reset_txn_kv();
    index_pb.set_state(RecycleIndexPB::DROPPED);
    val = index_pb.SerializeAsString();
    ASSERT_EQ(txn_kv->create_txn(&txn), 0);
    txn->put(index_key, val);
    ASSERT_EQ(txn->commit(), 0);
    ASSERT_EQ(txn_kv->create_txn(&txn), 0);
    EXPECT_TRUE(is_dropped_tablet(txn.get(), instance_id, index_id, partition_id));
    reset_txn_kv();
    index_pb.set_state(RecycleIndexPB::RECYCLING);
    val = index_pb.SerializeAsString();
    ASSERT_EQ(txn_kv->create_txn(&txn), 0);
    txn->put(index_key, val);
    ASSERT_EQ(txn->commit(), 0);
    ASSERT_EQ(txn_kv->create_txn(&txn), 0);
    EXPECT_TRUE(is_dropped_tablet(txn.get(), instance_id, index_id, partition_id));
    // Tablet in PREPARED partition
    reset_txn_kv();
    partition_pb.set_state(RecyclePartitionPB::PREPARED);
    val = partition_pb.SerializeAsString();
    ASSERT_EQ(txn_kv->create_txn(&txn), 0);
    txn->put(partition_key, val);
    ASSERT_EQ(txn->commit(), 0);
    ASSERT_EQ(txn_kv->create_txn(&txn), 0);
    EXPECT_FALSE(is_dropped_tablet(txn.get(), instance_id, index_id, partition_id));
    // Tablet in DROPPED/RECYCLING partition
    reset_txn_kv();
    partition_pb.set_state(RecyclePartitionPB::DROPPED);
    val = partition_pb.SerializeAsString();
    ASSERT_EQ(txn_kv->create_txn(&txn), 0);
    txn->put(partition_key, val);
    ASSERT_EQ(txn->commit(), 0);
    ASSERT_EQ(txn_kv->create_txn(&txn), 0);
    EXPECT_TRUE(is_dropped_tablet(txn.get(), instance_id, index_id, partition_id));
    reset_txn_kv();
    partition_pb.set_state(RecyclePartitionPB::RECYCLING);
    val = partition_pb.SerializeAsString();
    ASSERT_EQ(txn_kv->create_txn(&txn), 0);
    txn->put(partition_key, val);
    ASSERT_EQ(txn->commit(), 0);
    ASSERT_EQ(txn_kv->create_txn(&txn), 0);
    EXPECT_TRUE(is_dropped_tablet(txn.get(), instance_id, index_id, partition_id));
}

TEST(MetaServiceTest, IndexRequest) {
    auto meta_service = get_meta_service();
    std::string instance_id = "IndexRequest";
    auto sp = SyncPoint::get_instance();
    std::unique_ptr<int, std::function<void(int*)>> defer(
            (int*)0x01, [](int*) { SyncPoint::get_instance()->clear_all_call_backs(); });
    sp->set_call_back("get_instance_id::pred", [](void* p) { *((bool*)p) = true; });
    sp->set_call_back("get_instance_id", [&](void* p) { *((std::string*)p) = instance_id; });
    sp->enable_processing();

    auto& txn_kv = meta_service->txn_kv_;
    auto reset_txn_kv = [&txn_kv] {
        txn_kv = std::make_shared<MemTxnKv>();
        txn_kv->init();
    };
    constexpr int64_t table_id = 10001;
    constexpr int64_t index_id = 10002;
    constexpr int64_t partition_id = 10003;
    constexpr int64_t tablet_id = 10004;

    std::unique_ptr<Transaction> txn;
    doris::TabletMetaPB tablet_pb;
    tablet_pb.set_table_id(table_id);
    tablet_pb.set_index_id(index_id);
    tablet_pb.set_partition_id(partition_id);
    tablet_pb.set_tablet_id(tablet_id);
    auto tablet_key = meta_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id});
    auto tablet_val = tablet_pb.SerializeAsString();
    RecycleIndexPB index_pb;
    auto index_key = recycle_index_key({instance_id, index_id});
    std::string val;

    // ------------Test prepare index------------
    brpc::Controller ctrl;
    IndexRequest req;
    IndexResponse res;
    meta_service->prepare_index(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    req.set_table_id(table_id);
    req.add_index_ids(index_id);
    // Last state UNKNOWN
    res.Clear();
    meta_service->prepare_index(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    ASSERT_EQ(txn_kv->create_txn(&txn), 0);
    ASSERT_EQ(txn->get(index_key, &val), 0);
    ASSERT_TRUE(index_pb.ParseFromString(val));
    ASSERT_EQ(index_pb.state(), RecycleIndexPB::PREPARED);
    // Last state PREPARED
    res.Clear();
    meta_service->prepare_index(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    ASSERT_EQ(txn_kv->create_txn(&txn), 0);
    ASSERT_EQ(txn->get(index_key, &val), 0);
    ASSERT_TRUE(index_pb.ParseFromString(val));
    ASSERT_EQ(index_pb.state(), RecycleIndexPB::PREPARED);
    // Last state DROPPED
    reset_txn_kv();
    index_pb.set_state(RecycleIndexPB::DROPPED);
    val = index_pb.SerializeAsString();
    ASSERT_EQ(txn_kv->create_txn(&txn), 0);
    txn->put(index_key, val);
    ASSERT_EQ(txn->commit(), 0);
    res.Clear();
    meta_service->prepare_index(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    ASSERT_EQ(txn_kv->create_txn(&txn), 0);
    ASSERT_EQ(txn->get(index_key, &val), 0);
    ASSERT_TRUE(index_pb.ParseFromString(val));
    ASSERT_EQ(index_pb.state(), RecycleIndexPB::DROPPED);
    // Last state RECYCLING
    reset_txn_kv();
    index_pb.set_state(RecycleIndexPB::RECYCLING);
    val = index_pb.SerializeAsString();
    ASSERT_EQ(txn_kv->create_txn(&txn), 0);
    txn->put(index_key, val);
    ASSERT_EQ(txn->commit(), 0);
    res.Clear();
    meta_service->prepare_index(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    ASSERT_EQ(txn_kv->create_txn(&txn), 0);
    ASSERT_EQ(txn->get(index_key, &val), 0);
    ASSERT_TRUE(index_pb.ParseFromString(val));
    ASSERT_EQ(index_pb.state(), RecycleIndexPB::RECYCLING);
    // Last state UNKNOWN but tablet meta existed
    reset_txn_kv();
    ASSERT_EQ(txn_kv->create_txn(&txn), 0);
    txn->remove(index_key);
    txn->put(tablet_key, tablet_val);
    ASSERT_EQ(txn->commit(), 0);
    res.Clear();
    meta_service->prepare_index(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::ALREADY_EXISTED);
    ASSERT_EQ(txn_kv->create_txn(&txn), 0);
    ASSERT_EQ(txn->get(index_key, &val), 1);
    // ------------Test commit index------------
    reset_txn_kv();
    req.Clear();
    meta_service->commit_index(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    req.set_table_id(table_id);
    req.add_index_ids(index_id);
    // Last state UNKNOWN
    res.Clear();
    meta_service->commit_index(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    ASSERT_EQ(txn_kv->create_txn(&txn), 0);
    ASSERT_EQ(txn->get(index_key, &val), 1);
    // Last state PREPARED
    reset_txn_kv();
    index_pb.set_state(RecycleIndexPB::PREPARED);
    val = index_pb.SerializeAsString();
    ASSERT_EQ(txn_kv->create_txn(&txn), 0);
    txn->put(index_key, val);
    ASSERT_EQ(txn->commit(), 0);
    res.Clear();
    meta_service->commit_index(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    ASSERT_EQ(txn_kv->create_txn(&txn), 0);
    ASSERT_EQ(txn->get(index_key, &val), 1);
    // Last state DROPPED
    reset_txn_kv();
    index_pb.set_state(RecycleIndexPB::DROPPED);
    val = index_pb.SerializeAsString();
    ASSERT_EQ(txn_kv->create_txn(&txn), 0);
    txn->put(index_key, val);
    ASSERT_EQ(txn->commit(), 0);
    res.Clear();
    meta_service->commit_index(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    ASSERT_EQ(txn_kv->create_txn(&txn), 0);
    ASSERT_EQ(txn->get(index_key, &val), 0);
    ASSERT_TRUE(index_pb.ParseFromString(val));
    ASSERT_EQ(index_pb.state(), RecycleIndexPB::DROPPED);
    // Last state RECYCLING
    reset_txn_kv();
    index_pb.set_state(RecycleIndexPB::RECYCLING);
    val = index_pb.SerializeAsString();
    ASSERT_EQ(txn_kv->create_txn(&txn), 0);
    txn->put(index_key, val);
    ASSERT_EQ(txn->commit(), 0);
    res.Clear();
    meta_service->commit_index(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    ASSERT_EQ(txn_kv->create_txn(&txn), 0);
    ASSERT_EQ(txn->get(index_key, &val), 0);
    ASSERT_TRUE(index_pb.ParseFromString(val));
    ASSERT_EQ(index_pb.state(), RecycleIndexPB::RECYCLING);
    // Last state UNKNOWN but tablet meta existed
    reset_txn_kv();
    ASSERT_EQ(txn_kv->create_txn(&txn), 0);
    txn->put(tablet_key, tablet_val);
    ASSERT_EQ(txn->commit(), 0);
    res.Clear();
    meta_service->commit_index(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    ASSERT_EQ(txn_kv->create_txn(&txn), 0);
    ASSERT_EQ(txn->get(index_key, &val), 1);
    // ------------Test drop index------------
    reset_txn_kv();
    req.Clear();
    meta_service->drop_index(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    req.set_table_id(table_id);
    req.add_index_ids(index_id);
    // Last state UNKNOWN
    res.Clear();
    meta_service->drop_index(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    ASSERT_EQ(txn_kv->create_txn(&txn), 0);
    ASSERT_EQ(txn->get(index_key, &val), 0);
    ASSERT_TRUE(index_pb.ParseFromString(val));
    ASSERT_EQ(index_pb.state(), RecycleIndexPB::DROPPED);
    // Last state PREPARED
    reset_txn_kv();
    index_pb.set_state(RecycleIndexPB::PREPARED);
    val = index_pb.SerializeAsString();
    ASSERT_EQ(txn_kv->create_txn(&txn), 0);
    txn->put(index_key, val);
    ASSERT_EQ(txn->commit(), 0);
    res.Clear();
    meta_service->drop_index(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    ASSERT_EQ(txn_kv->create_txn(&txn), 0);
    ASSERT_EQ(txn->get(index_key, &val), 0);
    ASSERT_TRUE(index_pb.ParseFromString(val));
    ASSERT_EQ(index_pb.state(), RecycleIndexPB::DROPPED);
    // Last state DROPPED
    reset_txn_kv();
    index_pb.set_state(RecycleIndexPB::DROPPED);
    val = index_pb.SerializeAsString();
    ASSERT_EQ(txn_kv->create_txn(&txn), 0);
    txn->put(index_key, val);
    ASSERT_EQ(txn->commit(), 0);
    res.Clear();
    meta_service->drop_index(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    ASSERT_EQ(txn_kv->create_txn(&txn), 0);
    ASSERT_EQ(txn->get(index_key, &val), 0);
    ASSERT_TRUE(index_pb.ParseFromString(val));
    ASSERT_EQ(index_pb.state(), RecycleIndexPB::DROPPED);
    // Last state RECYCLING
    reset_txn_kv();
    index_pb.set_state(RecycleIndexPB::RECYCLING);
    val = index_pb.SerializeAsString();
    ASSERT_EQ(txn_kv->create_txn(&txn), 0);
    txn->put(index_key, val);
    ASSERT_EQ(txn->commit(), 0);
    res.Clear();
    meta_service->drop_index(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    ASSERT_EQ(txn_kv->create_txn(&txn), 0);
    ASSERT_EQ(txn->get(index_key, &val), 0);
    ASSERT_TRUE(index_pb.ParseFromString(val));
    ASSERT_EQ(index_pb.state(), RecycleIndexPB::RECYCLING);
}

TEST(MetaServiceTest, PartitionRequest) {
    auto meta_service = get_meta_service();
    std::string instance_id = "PartitionRequest";
    auto sp = SyncPoint::get_instance();
    std::unique_ptr<int, std::function<void(int*)>> defer(
            (int*)0x01, [](int*) { SyncPoint::get_instance()->clear_all_call_backs(); });
    sp->set_call_back("get_instance_id::pred", [](void* p) { *((bool*)p) = true; });
    sp->set_call_back("get_instance_id", [&](void* p) { *((std::string*)p) = instance_id; });
    sp->enable_processing();

    auto& txn_kv = meta_service->txn_kv_;
    auto reset_txn_kv = [&txn_kv] {
        txn_kv = std::make_shared<MemTxnKv>();
        txn_kv->init();
    };
    constexpr int64_t table_id = 10001;
    constexpr int64_t index_id = 10002;
    constexpr int64_t partition_id = 10003;
    constexpr int64_t tablet_id = 10004;

    std::unique_ptr<Transaction> txn;
    doris::TabletMetaPB tablet_pb;
    tablet_pb.set_table_id(table_id);
    tablet_pb.set_index_id(index_id);
    tablet_pb.set_partition_id(partition_id);
    tablet_pb.set_tablet_id(tablet_id);
    auto tablet_key = meta_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id});
    auto tablet_val = tablet_pb.SerializeAsString();
    RecyclePartitionPB partition_pb;
    auto partition_key = recycle_partition_key({instance_id, partition_id});
    std::string val;
    // ------------Test prepare partition------------
    brpc::Controller ctrl;
    PartitionRequest req;
    PartitionResponse res;
    meta_service->prepare_partition(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    req.set_table_id(table_id);
    req.add_index_ids(index_id);
    req.add_partition_ids(partition_id);
    // Last state UNKNOWN
    res.Clear();
    meta_service->prepare_partition(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    ASSERT_EQ(txn_kv->create_txn(&txn), 0);
    ASSERT_EQ(txn->get(partition_key, &val), 0);
    ASSERT_TRUE(partition_pb.ParseFromString(val));
    ASSERT_EQ(partition_pb.state(), RecyclePartitionPB::PREPARED);
    // Last state PREPARED
    res.Clear();
    meta_service->prepare_partition(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    ASSERT_EQ(txn_kv->create_txn(&txn), 0);
    ASSERT_EQ(txn->get(partition_key, &val), 0);
    ASSERT_TRUE(partition_pb.ParseFromString(val));
    ASSERT_EQ(partition_pb.state(), RecyclePartitionPB::PREPARED);
    // Last state DROPPED
    reset_txn_kv();
    partition_pb.set_state(RecyclePartitionPB::DROPPED);
    val = partition_pb.SerializeAsString();
    ASSERT_EQ(txn_kv->create_txn(&txn), 0);
    txn->put(partition_key, val);
    ASSERT_EQ(txn->commit(), 0);
    res.Clear();
    meta_service->prepare_partition(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    ASSERT_EQ(txn_kv->create_txn(&txn), 0);
    ASSERT_EQ(txn->get(partition_key, &val), 0);
    ASSERT_TRUE(partition_pb.ParseFromString(val));
    ASSERT_EQ(partition_pb.state(), RecyclePartitionPB::DROPPED);
    // Last state RECYCLING
    reset_txn_kv();
    partition_pb.set_state(RecyclePartitionPB::RECYCLING);
    val = partition_pb.SerializeAsString();
    ASSERT_EQ(txn_kv->create_txn(&txn), 0);
    txn->put(partition_key, val);
    ASSERT_EQ(txn->commit(), 0);
    res.Clear();
    meta_service->prepare_partition(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    ASSERT_EQ(txn_kv->create_txn(&txn), 0);
    ASSERT_EQ(txn->get(partition_key, &val), 0);
    ASSERT_TRUE(partition_pb.ParseFromString(val));
    ASSERT_EQ(partition_pb.state(), RecyclePartitionPB::RECYCLING);
    // Last state UNKNOWN but tablet meta existed
    reset_txn_kv();
    ASSERT_EQ(txn_kv->create_txn(&txn), 0);
    txn->remove(partition_key);
    txn->put(tablet_key, tablet_val);
    ASSERT_EQ(txn->commit(), 0);
    res.Clear();
    meta_service->prepare_partition(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::ALREADY_EXISTED);
    ASSERT_EQ(txn_kv->create_txn(&txn), 0);
    ASSERT_EQ(txn->get(partition_key, &val), 1);
    // ------------Test commit partition------------
    reset_txn_kv();
    req.Clear();
    meta_service->commit_partition(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    req.set_table_id(table_id);
    req.add_index_ids(index_id);
    req.add_partition_ids(partition_id);
    // Last state UNKNOWN
    res.Clear();
    meta_service->commit_partition(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    ASSERT_EQ(txn_kv->create_txn(&txn), 0);
    ASSERT_EQ(txn->get(partition_key, &val), 1);
    // Last state PREPARED
    reset_txn_kv();
    partition_pb.set_state(RecyclePartitionPB::PREPARED);
    val = partition_pb.SerializeAsString();
    ASSERT_EQ(txn_kv->create_txn(&txn), 0);
    txn->put(partition_key, val);
    ASSERT_EQ(txn->commit(), 0);
    res.Clear();
    meta_service->commit_partition(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    ASSERT_EQ(txn_kv->create_txn(&txn), 0);
    ASSERT_EQ(txn->get(partition_key, &val), 1);
    // Last state DROPPED
    reset_txn_kv();
    partition_pb.set_state(RecyclePartitionPB::DROPPED);
    val = partition_pb.SerializeAsString();
    ASSERT_EQ(txn_kv->create_txn(&txn), 0);
    txn->put(partition_key, val);
    ASSERT_EQ(txn->commit(), 0);
    res.Clear();
    meta_service->commit_partition(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    ASSERT_EQ(txn_kv->create_txn(&txn), 0);
    ASSERT_EQ(txn->get(partition_key, &val), 0);
    ASSERT_TRUE(partition_pb.ParseFromString(val));
    ASSERT_EQ(partition_pb.state(), RecyclePartitionPB::DROPPED);
    // Last state RECYCLING
    reset_txn_kv();
    partition_pb.set_state(RecyclePartitionPB::RECYCLING);
    val = partition_pb.SerializeAsString();
    ASSERT_EQ(txn_kv->create_txn(&txn), 0);
    txn->put(partition_key, val);
    ASSERT_EQ(txn->commit(), 0);
    res.Clear();
    meta_service->commit_partition(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    ASSERT_EQ(txn_kv->create_txn(&txn), 0);
    ASSERT_EQ(txn->get(partition_key, &val), 0);
    ASSERT_TRUE(partition_pb.ParseFromString(val));
    ASSERT_EQ(partition_pb.state(), RecyclePartitionPB::RECYCLING);
    // Last state UNKNOWN but tablet meta existed
    reset_txn_kv();
    ASSERT_EQ(txn_kv->create_txn(&txn), 0);
    txn->put(tablet_key, tablet_val);
    ASSERT_EQ(txn->commit(), 0);
    res.Clear();
    meta_service->commit_partition(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    ASSERT_EQ(txn_kv->create_txn(&txn), 0);
    ASSERT_EQ(txn->get(partition_key, &val), 1);
    // Last state UNKNOWN and tablet meta existed, but request has no index ids
    reset_txn_kv();
    ASSERT_EQ(txn_kv->create_txn(&txn), 0);
    txn->put(tablet_key, tablet_val);
    ASSERT_EQ(txn->commit(), 0);
    res.Clear();
    req.clear_index_ids();
    meta_service->commit_partition(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    ASSERT_EQ(txn_kv->create_txn(&txn), 0);
    ASSERT_EQ(txn->get(partition_key, &val), 1);
    req.add_index_ids(index_id);
    // ------------Test drop partition------------
    reset_txn_kv();
    req.Clear();
    meta_service->drop_partition(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    req.set_table_id(table_id);
    req.add_index_ids(index_id);
    req.add_partition_ids(partition_id);
    // Last state UNKNOWN
    res.Clear();
    meta_service->drop_partition(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    ASSERT_EQ(txn_kv->create_txn(&txn), 0);
    ASSERT_EQ(txn->get(partition_key, &val), 0);
    ASSERT_TRUE(partition_pb.ParseFromString(val));
    ASSERT_EQ(partition_pb.state(), RecyclePartitionPB::DROPPED);
    // Last state PREPARED
    reset_txn_kv();
    partition_pb.set_state(RecyclePartitionPB::PREPARED);
    val = partition_pb.SerializeAsString();
    ASSERT_EQ(txn_kv->create_txn(&txn), 0);
    txn->put(partition_key, val);
    ASSERT_EQ(txn->commit(), 0);
    res.Clear();
    meta_service->drop_partition(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    ASSERT_EQ(txn_kv->create_txn(&txn), 0);
    ASSERT_EQ(txn->get(partition_key, &val), 0);
    ASSERT_TRUE(partition_pb.ParseFromString(val));
    ASSERT_EQ(partition_pb.state(), RecyclePartitionPB::DROPPED);
    // Last state DROPPED
    reset_txn_kv();
    partition_pb.set_state(RecyclePartitionPB::DROPPED);
    val = partition_pb.SerializeAsString();
    ASSERT_EQ(txn_kv->create_txn(&txn), 0);
    txn->put(partition_key, val);
    ASSERT_EQ(txn->commit(), 0);
    res.Clear();
    meta_service->drop_partition(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    ASSERT_EQ(txn_kv->create_txn(&txn), 0);
    ASSERT_EQ(txn->get(partition_key, &val), 0);
    ASSERT_TRUE(partition_pb.ParseFromString(val));
    ASSERT_EQ(partition_pb.state(), RecyclePartitionPB::DROPPED);
    // Last state RECYCLING
    reset_txn_kv();
    partition_pb.set_state(RecyclePartitionPB::RECYCLING);
    val = partition_pb.SerializeAsString();
    ASSERT_EQ(txn_kv->create_txn(&txn), 0);
    txn->put(partition_key, val);
    ASSERT_EQ(txn->commit(), 0);
    res.Clear();
    meta_service->drop_partition(&ctrl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    ASSERT_EQ(txn_kv->create_txn(&txn), 0);
    ASSERT_EQ(txn->get(partition_key, &val), 0);
    ASSERT_TRUE(partition_pb.ParseFromString(val));
    ASSERT_EQ(partition_pb.state(), RecyclePartitionPB::RECYCLING);
}

TEST(MetaServiceTxnStoreRetryableTest, MockGetVersion) {
    size_t index = 0;
    SyncPoint::get_instance()->set_call_back("get_version_code", [&](void* arg) {
        LOG(INFO) << "GET_VERSION_CODE";
        if (++index < 5) {
            *reinterpret_cast<MetaServiceCode*>(arg) = MetaServiceCode::KV_TXN_STORE_GET_RETRYABLE;
        }
    });
    SyncPoint::get_instance()->enable_processing();
    config::enable_txn_store_retry = true;

    auto service = get_meta_service();
    create_tablet(service.get(), 1, 1, 1, 1);
    insert_rowset(service.get(), 1, std::to_string(1), 1, 1, 1);

    auto service_proxy = std::make_unique<MetaServiceProxy>(std::move(service));
    brpc::Controller ctrl;
    GetVersionRequest req;
    req.set_cloud_unique_id("test_cloud_unique_id");
    req.set_db_id(1);
    req.set_table_id(1);
    req.set_partition_id(1);

    GetVersionResponse resp;
    service_proxy->get_version(&ctrl, &req, &resp, nullptr);

    ASSERT_EQ(resp.status().code(), MetaServiceCode::OK)
            << " status is " << resp.status().msg() << ", code=" << resp.status().code();
    EXPECT_EQ(resp.version(), 2);
    EXPECT_GE(index, 5);

    SyncPoint::get_instance()->disable_processing();
    SyncPoint::get_instance()->clear_all_call_backs();
    config::enable_txn_store_retry = false;
}

TEST(MetaServiceTxnStoreRetryableTest, DoNotReturnRetryableCode) {
    SyncPoint::get_instance()->set_call_back("get_version_code", [&](void* arg) {
        *reinterpret_cast<MetaServiceCode*>(arg) = MetaServiceCode::KV_TXN_STORE_GET_RETRYABLE;
    });
    SyncPoint::get_instance()->enable_processing();
    config::enable_txn_store_retry = true;
    int32_t retry_times = config::txn_store_retry_times;
    config::txn_store_retry_times = 3;

    auto service = get_meta_service();
    create_tablet(service.get(), 1, 1, 1, 1);
    insert_rowset(service.get(), 1, std::to_string(1), 1, 1, 1);

    auto service_proxy = std::make_unique<MetaServiceProxy>(std::move(service));
    brpc::Controller ctrl;
    GetVersionRequest req;
    req.set_cloud_unique_id("test_cloud_unique_id");
    req.set_db_id(1);
    req.set_table_id(1);
    req.set_partition_id(1);

    GetVersionResponse resp;
    service_proxy->get_version(&ctrl, &req, &resp, nullptr);

    ASSERT_EQ(resp.status().code(), MetaServiceCode::KV_TXN_GET_ERR)
            << " status is " << resp.status().msg() << ", code=" << resp.status().code();

    SyncPoint::get_instance()->disable_processing();
    SyncPoint::get_instance()->clear_all_call_backs();
    config::enable_txn_store_retry = false;
    config::txn_store_retry_times = retry_times;
}

} // namespace selectdb
// vim: et tw=100 ts=4 sw=4 cc=80:
