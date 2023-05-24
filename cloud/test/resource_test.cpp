
// clang-format off
//#define private public
#include "meta-service/meta_service.h"
//#undef private

#include <bvar/window.h>

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
std::unique_ptr<MetaServiceImpl> get_meta_service() {
    int ret = 0;
    // MemKv
    auto txn_kv = std::dynamic_pointer_cast<TxnKv>(std::make_shared<MemTxnKv>());
    if (txn_kv != nullptr) {
        ret = txn_kv->init();
        [&] { ASSERT_EQ(ret, 0); }();
    }
    [&] { ASSERT_NE(txn_kv.get(), nullptr); }();

    std::unique_ptr<Transaction> txn;
    txn_kv->create_txn(&txn);
    txn->remove("\x00", "\xfe"); // This is dangerous if the fdb is not correctly set
    txn->commit();

    auto rs = std::make_shared<ResourceManager>(txn_kv);
    auto rl = std::make_shared<RateLimiter>();
    auto meta_service = std::make_unique<MetaServiceImpl>(txn_kv, rs, rl);
    return meta_service;
}


static void create_args_to_add(std::vector<NodeInfo>* to_add, std::vector<NodeInfo>* to_del, bool is_host = false) {
    to_add->clear();to_del->clear();
    auto ni_1 = NodeInfo {
        .role = Role::COMPUTE_NODE,
        .instance_id = "test-resource-instance",
        .cluster_name = "cluster_name_1",
        .cluster_id = "cluster_id_1",
        .node_info= NodeInfoPB {
        }
    };
    is_host ? ni_1.node_info.set_host("host1") : ni_1.node_info.set_ip("127.0.0.1");
    ni_1.node_info.set_cloud_unique_id("test_cloud_unique_id_1");
    ni_1.node_info.set_heartbeat_port(9999);
    to_add->push_back(ni_1);

    auto ni_2 = NodeInfo {
        .role = Role::COMPUTE_NODE,
        .instance_id = "test-resource-instance",
        .cluster_name = "cluster_name_1",
        .cluster_id = "cluster_id_1",
        .node_info= NodeInfoPB {
        }
    };
    is_host ? ni_2.node_info.set_host("host2") : ni_2.node_info.set_ip("127.0.0.2");
    ni_2.node_info.set_cloud_unique_id("test_cloud_unique_id_1");
    ni_2.node_info.set_heartbeat_port(9999);
    to_add->push_back(ni_2);

    auto ni_3 = NodeInfo {
        .role = Role::COMPUTE_NODE,
        .instance_id = "test-resource-instance",
        .cluster_name = "cluster_name_2",
        .cluster_id = "cluster_id_2",
        .node_info= NodeInfoPB {
        }
    };
    is_host ? ni_3.node_info.set_host("host3") : ni_3.node_info.set_ip("127.0.0.3");
    ni_3.node_info.set_cloud_unique_id("test_cloud_unique_id_2");
    ni_3.node_info.set_heartbeat_port(9999);
    to_add->push_back(ni_3);
}

static void create_args_to_del(std::vector<NodeInfo>* to_add, std::vector<NodeInfo>* to_del, bool is_host = false) {
    to_add->clear();to_del->clear();
    auto ni_1 = NodeInfo {
        .role = Role::COMPUTE_NODE,
        .instance_id = "test-resource-instance",
        .cluster_name = "cluster_name_1",
        .cluster_id = "cluster_id_1",
        .node_info= NodeInfoPB {
        }
    };
    is_host ? ni_1.node_info.set_host("host2") : ni_1.node_info.set_ip("127.0.0.2");
    ni_1.node_info.set_cloud_unique_id("test_cloud_unique_id_1");
    ni_1.node_info.set_heartbeat_port(9999);
    to_del->push_back(ni_1);
}

static void get_instance_info(MetaServiceImpl* ms, InstanceInfoPB* instance) {
    InstanceKeyInfo key_info {"test-resource-instance"};
    std::string key;
    std::string val;
    instance_key(key_info, &key);
    std::unique_ptr<Transaction> txn;
    ms->txn_kv_->create_txn(&txn);
    txn->get(key, &val);
    instance->ParseFromString(val);
}

// test cluster's node addr use ip
TEST(ResourceTest, ModifyNodesIpTest) {
    auto meta_service = get_meta_service();
    std::vector<NodeInfo> to_add = {};
    std::vector<NodeInfo> to_del = {};
    auto ins = InstanceInfoPB{};
    create_args_to_add(&to_add, &to_del);
    auto sp = SyncPoint::get_instance();
    sp->set_call_back("modify_nodes:get_instance",
                      [](void* p) { *reinterpret_cast<int*>(p) = 0; });
    sp->set_call_back("modify_nodes:get_instance_ret",
                      [&](void* p) {
        ins.set_instance_id("test-resource-instance");
        ins.set_status(InstanceInfoPB::NORMAL);
        auto c = ins.mutable_clusters()->Add();
        c->set_cluster_name("cluster_name_1");
        c->set_cluster_id("cluster_id_1");
        auto c1 = ins.mutable_clusters()->Add();
        c1->set_cluster_name("cluster_name_2");
        c1->set_cluster_id("cluster_id_2");
        *reinterpret_cast<InstanceInfoPB*>(p) = ins;
    });
    sp->enable_processing();

    // test cluster add nodes
    auto r = meta_service->resource_mgr_->modify_nodes("test-resource-instance", to_add, to_del);
    ASSERT_EQ(r, "");
    InstanceInfoPB instance;
    get_instance_info(meta_service.get(), &instance);
    std::cout << "after to add = " << proto_to_json(instance) << std::endl;
    ASSERT_EQ(instance.clusters().size(), 2);
    // after add assert cluster_name_1 has 2 nodes
    ASSERT_EQ(instance.clusters(0).nodes().size(), 2);
    // after add assert cluster_name_2 has 1 nodes
    ASSERT_EQ(instance.clusters(1).nodes().size(), 1);
    sp->clear_all_call_backs();
    sp->clear_trace();
    sp->disable_processing();

    sp->set_call_back("modify_nodes:get_instance",
                      [](void* p) { *reinterpret_cast<int*>(p) = 0; });
    sp->set_call_back("modify_nodes:get_instance_ret",
                      [&](void* p) {
                          *reinterpret_cast<InstanceInfoPB*>(p) = instance;
    });
    sp->enable_processing();
    create_args_to_del(&to_add, &to_del);
    // test cluster del node
    r = meta_service->resource_mgr_->modify_nodes("test-resource-instance", to_add, to_del);
    InstanceInfoPB instance1;
    get_instance_info(meta_service.get(), &instance1);
    ASSERT_EQ(r, "");
    std::cout << "after to del = " << proto_to_json(instance1) << std::endl;
    ASSERT_EQ(instance1.clusters().size(), 2);
    // after del assert cluster_name_1 has 1 nodes
    ASSERT_EQ(instance1.clusters(0).nodes().size(), 1);
    // after del assert cluster_name_2 has 1 nodes
    ASSERT_EQ(instance1.clusters(1).nodes().size(), 1);
    sp->clear_all_call_backs();
    sp->clear_trace();
    sp->disable_processing();
}

// test cluster's node addr use host
TEST(ResourceTest, ModifyNodesHostTest) {
    auto meta_service = get_meta_service();
    std::vector<NodeInfo> to_add = {};
    std::vector<NodeInfo> to_del = {};
    auto ins = InstanceInfoPB{};
    create_args_to_add(&to_add, &to_del, true);
    auto sp = SyncPoint::get_instance();
    sp->set_call_back("modify_nodes:get_instance",
                      [](void* p) { *reinterpret_cast<int*>(p) = 0; });
    sp->set_call_back("modify_nodes:get_instance_ret",
                      [&](void* p) {
                          ins.set_instance_id("test-resource-instance");
                          ins.set_status(InstanceInfoPB::NORMAL);
                          auto c = ins.mutable_clusters()->Add();
                          c->set_cluster_name("cluster_name_1");
                          c->set_cluster_id("cluster_id_1");
                          auto c1 = ins.mutable_clusters()->Add();
                          c1->set_cluster_name("cluster_name_2");
                          c1->set_cluster_id("cluster_id_2");
                          *reinterpret_cast<InstanceInfoPB*>(p) = ins;
                      });
    sp->enable_processing();

    // test cluster add nodes
    auto r = meta_service->resource_mgr_->modify_nodes("test-resource-instance", to_add, to_del);
    ASSERT_EQ(r, "");
    InstanceInfoPB instance;
    get_instance_info(meta_service.get(), &instance);
    std::cout << "after to add = " << proto_to_json(instance) << std::endl;
    ASSERT_EQ(instance.clusters().size(), 2);
    // after add assert cluster_name_1 has 2 nodes
    ASSERT_EQ(instance.clusters(0).nodes().size(), 2);
    // after add assert cluster_name_2 has 1 nodes
    ASSERT_EQ(instance.clusters(1).nodes().size(), 1);
    sp->clear_all_call_backs();
    sp->clear_trace();
    sp->disable_processing();

    sp->set_call_back("modify_nodes:get_instance",
                      [](void* p) { *reinterpret_cast<int*>(p) = 0; });
    sp->set_call_back("modify_nodes:get_instance_ret",
                      [&](void* p) {
                          *reinterpret_cast<InstanceInfoPB*>(p) = instance;
                      });
    sp->enable_processing();
    create_args_to_del(&to_add, &to_del, true);
    r = meta_service->resource_mgr_->modify_nodes("test-resource-instance", to_add, to_del);
    InstanceInfoPB instance1;
    get_instance_info(meta_service.get(), &instance1);
    ASSERT_EQ(r, "");
    std::cout << "after to del = " << proto_to_json(instance1) << std::endl;
    ASSERT_EQ(instance1.clusters().size(), 2);
    // after del assert cluster_name_1 has 1 nodes
    ASSERT_EQ(instance1.clusters(0).nodes().size(), 1);
    // after del assert cluster_name_2 has 1 nodes
    ASSERT_EQ(instance1.clusters(1).nodes().size(), 1);
    sp->clear_all_call_backs();
    sp->clear_trace();
    sp->disable_processing();
}
}