

// clang-format off
#include <brpc/server.h>
#include <butil/endpoint.h>
#include <bvar/window.h>
#include <gtest/gtest.h>

#include "common/config.h"
#include "common/logging.h"
#include "common/sync_point.h"
#include "gen_cpp/selectdb_cloud.pb.h"
#include "meta-service/keys.h"
#include "meta-service/mem_txn_kv.h"
#include "meta-service/meta_server.h"
#include "meta-service/meta_service.h"
#include "meta-service/txn_kv.h"
#include "meta-service/txn_kv_error.h"
#include "rate-limiter/rate_limiter.h"
#include "resource-manager/resource_manager.h"
#include "mock_resource_manager.h"

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <fstream>
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

    if (!selectdb::init_glog("meta_server_test")) {
        std::cerr << "failed to init glog" << std::endl;
        return -1;
    }
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace selectdb {
void notify_refresh_instance(std::shared_ptr<TxnKv> txn_kv, const std::string& instance_id);
}

TEST(MetaServerTest, FQDNRefreshInstance) {
    class MockMetaService : public selectdb::MetaServiceImpl {
    public:
        MockMetaService(std::shared_ptr<TxnKv> txn_kv,
                        std::shared_ptr<ResourceManager> resource_mgr,
                        std::shared_ptr<RateLimiter> rate_controller)
                : MetaServiceImpl(txn_kv, resource_mgr, rate_controller) {}
        ~MockMetaService() override = default;

        void alter_instance(google::protobuf::RpcController* controller,
                            const ::selectdb::AlterInstanceRequest* request,
                            ::selectdb::AlterInstanceResponse* response,
                            ::google::protobuf::Closure* done) override {
            (void)controller;
            response->mutable_status()->set_code(selectdb::MetaServiceCode::OK);
            LOG(INFO) << "alter instance " << request->DebugString();
            if (request->op() == selectdb::AlterInstanceRequest::REFRESH) {
                std::unique_lock<std::mutex> lock(mu_);
                LOG(INFO) << "refresh instance " << request->instance_id();
                refreshed_instances_.insert(request->instance_id());
            }
            done->Run();
        }

        bool is_instance_refreshed(std::string instance_id) {
            std::unique_lock<std::mutex> lock(mu_);
            return refreshed_instances_.count(instance_id) > 0;
        }

        std::mutex mu_;
        std::unordered_set<std::string> refreshed_instances_;
    };

    std::shared_ptr<selectdb::TxnKv> txn_kv = std::make_shared<selectdb::MemTxnKv>();
    auto resource_mgr = std::make_shared<MockResourceManager>(txn_kv);
    auto rate_limiter = std::make_shared<selectdb::RateLimiter>();
    auto mock_service = std::make_unique<MockMetaService>(txn_kv, resource_mgr, rate_limiter);
    MockMetaService* mock_service_ptr = mock_service.get();
    MetaServiceProxy meta_service(std::move(mock_service));

    brpc::ServerOptions options;
    options.num_threads = 1;
    brpc::Server server;
    ASSERT_EQ(server.AddService(&meta_service, brpc::ServiceOwnership::SERVER_DOESNT_OWN_SERVICE),
              0);
    ASSERT_EQ(server.Start(0, &options), 0);
    auto addr = server.listen_address();

    config::hostname = butil::my_hostname();
    config::brpc_listen_port = addr.port;
    config::meta_server_register_interval_ms = 1;

    // Register meta service.
    selectdb::MetaServerRegister server_register(txn_kv);
    ASSERT_EQ(server_register.start(), 0);

    while (true) {
        std::unique_ptr<selectdb::Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        auto system_key = selectdb::system_meta_service_registry_key();
        std::string value;
        if (txn->get(system_key, &value) == TxnErrorCode::TXN_OK) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    server_register.stop();

    // Refresh instance with FQDN endpoint.
    config::hostname = "";
    notify_refresh_instance(txn_kv, "fqdn_instance_id");

    bool refreshed = false;
    for (size_t i = 0; i < 100; ++i) {
        if (mock_service_ptr->is_instance_refreshed("fqdn_instance_id")) {
            refreshed = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    EXPECT_TRUE(refreshed);

    server.Stop(1);
    server.Join();
}

TEST(MetaServerTest, StartAndStop) {
    std::shared_ptr<selectdb::TxnKv> txn_kv = std::make_shared<selectdb::MemTxnKv>();
    auto server = std::make_unique<MetaServer>(txn_kv);
    auto resource_mgr = std::make_shared<MockResourceManager>(txn_kv);
    auto rate_limiter = std::make_shared<selectdb::RateLimiter>();

    brpc::ServerOptions options;
    options.num_threads = 1;
    brpc::Server brpc_server;

    auto sp = selectdb::SyncPoint::get_instance();

    std::array<std::string, 3> sps{"MetaServer::start:1", "MetaServer::start:2", "MetaServer::start:3"};
    // use structured binding for point alias (avoid multi lines of declaration)
    auto [meta_server_start_1, meta_server_start_2, meta_server_start_3] = sps;
    sp->enable_processing();
    std::unique_ptr<int, std::function<void(int*)>> defer((int*)0x01, [&](...) {
        for (auto& i : sps) { sp->clear_call_back(i); } // redundant
        sp->disable_processing();
    });

    auto foo = [](void* ret) { *((int*)ret) = 1; };

    // failed to init resource mgr
    sp->set_call_back(meta_server_start_1, foo);
    ASSERT_EQ(server->start(&brpc_server), 1);
    sp->clear_call_back(meta_server_start_1);

    // failed to start registry
    sp->set_call_back(meta_server_start_2, foo);
    ASSERT_EQ(server->start(&brpc_server), -1);
    sp->clear_call_back(meta_server_start_2);

    // failed to start fdb metrics exporter
    sp->set_call_back(meta_server_start_3, foo);
    ASSERT_EQ(server->start(&brpc_server), -2);
    sp->clear_call_back(meta_server_start_3);

    ASSERT_EQ(server->start(&brpc_server), 0);
    ASSERT_EQ(brpc_server.Start(0, &options), 0);
    auto addr = brpc_server.listen_address();

    config::hostname = butil::my_hostname();
    config::brpc_listen_port = addr.port;
    config::meta_server_register_interval_ms = 1;

    while (true) {
        std::unique_ptr<selectdb::Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        auto system_key = selectdb::system_meta_service_registry_key();
        std::string value;
        if (txn->get(system_key, &value) == TxnErrorCode::TXN_OK) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    server->stop();
    brpc_server.Stop(1);
    brpc_server.Join();
}

