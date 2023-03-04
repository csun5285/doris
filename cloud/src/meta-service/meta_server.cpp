
// clang-format off
#include "meta_server.h"

#include "common/config.h"
#include "common/metric.h"
#include "common/util.h"
#include "gen_cpp/selectdb_cloud.pb.h"
#include "meta-service/keys.h"
#include "meta-service/mem_txn_kv.h"
#include "meta-service/meta_service.h"
#include "rate-limiter/rate_limiter.h"
#include "resource-manager/resource_manager.h"

#include "brpc/server.h"
#include "butil/endpoint.h"
#include "txn_kv.h"

#include <chrono>
#include <condition_variable>
#include <memory>
#include <random>
#include <thread>
// clang-format on


namespace brpc {
DECLARE_uint64(max_body_size);
DECLARE_int64(socket_max_unwritten_bytes);
} // namespace brpc

namespace selectdb {

MetaServer::MetaServer() {
    server_.reset(new brpc::Server());
}

int MetaServer::start() {
    if (config::use_mem_kv) {
        // MUST NOT be used in production environment
        std::cerr << "use volatile mem kv, please make sure it is not a production environment"
                  << std::endl;
        txn_kv_ = std::make_shared<MemTxnKv>();
    } else {
        txn_kv_ = std::dynamic_pointer_cast<TxnKv>(std::make_shared<FdbTxnKv>());
    }
    if (txn_kv_ == nullptr) {
        LOG(WARNING) << "failed to create txn kv, invalid txnkv type";
        return 1;
    }
    LOG(INFO) << "begin to init txn kv";
    int ret = txn_kv_->init();
    if (ret != 0) {
        LOG(WARNING) << "failed to init txnkv, ret=" << ret;
        return 1;
    }
    LOG(INFO) << "successfully init txn kv";

    auto rc_mgr = std::make_shared<ResourceManager>(txn_kv_);
    ret = rc_mgr->init();
    if (ret != 0) {
        LOG(WARNING) << "failed to init resrouce manager, ret=" << ret;
        return 1;
    }

    // Add server register
    server_register_.reset(new MetaServerRegister(txn_kv_));
    if (server_register_->start() != 0) {
        LOG(WARNING) << "failed to start server register";
        return -1;
    }

    fdb_metric_exporter_.reset(new FdbMetricExporter(txn_kv_));
    if (fdb_metric_exporter_->start() != 0) {
        LOG(WARNING) << "failed to start fdb metric exporter";
        return -1;
    }

    auto rate_limiter = std::make_shared<RateLimiter>();

    brpc::FLAGS_max_body_size = selectdb::config::brpc_max_body_size;
    brpc::FLAGS_socket_max_unwritten_bytes = selectdb::config::brpc_socket_max_unwritten_bytes;

    // Add service
    auto meta_service = new MetaServiceImpl(txn_kv_, rc_mgr, rate_limiter);
    server_->AddService(meta_service, brpc::SERVER_OWNS_SERVICE);
    // start service
    brpc::ServerOptions options;
    if (config::brpc_num_threads != -1) {
        options.num_threads = config::brpc_num_threads;
    }
    int port = selectdb::config::brpc_listen_port;
    if (server_->Start(port, &options) != 0) {
        char buf[64];
        LOG(WARNING) << "failed to start brpc, errno=" << errno
                     << ", errmsg=" << strerror_r(errno, buf, 64) << ", port=" << port;
        return -1;
    }
    LOG(INFO) << "succesfully started brpc listening on port=" << port;

    return 0;
}

void MetaServer::join() {
    server_->RunUntilAskedToQuit();
    // server_->Stop(1000);
    // server_->Join();
    server_->ClearServices();
    server_register_->stop();
    fdb_metric_exporter_->stop();
}

void MetaServerRegister::prepare_registry(ServiceRegistryPB* reg) {
    using namespace std::chrono;
    auto now = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
    std::string ip = butil::my_ip_cstr();
    int32_t port = config::brpc_listen_port;
    std::string id = ip + ":" + std::to_string(port);
    ServiceRegistryPB::Item item;
    item.set_id(id);
    item.set_ip(ip);
    item.set_port(port);
    item.set_ctime_ms(now);
    item.set_mtime_ms(now);
    item.set_expiration_time_ms(now + config::meta_server_lease_ms);

    if (!id_.empty() && id_ != id) {
        LOG(WARNING) << "server id changed, old=" << id_ << " new=" << id;
        id_ = id;
    }

    int num_items = reg->items_size();
    ServiceRegistryPB out;
    for (int i = 0; i < num_items; ++i) {
        auto& e = reg->items(i);
        if (e.expiration_time_ms() < now) continue;
        if (e.id() == id) {
            item.set_ctime_ms(e.ctime_ms());
            continue;
        }
        *out.add_items() = e;
    }
    *out.add_items() = item;
    *reg = out;
}

MetaServerRegister::MetaServerRegister(std::shared_ptr<TxnKv> txn_kv)
        : running_(false), txn_kv_(std::move(txn_kv)) {
    register_thread_.reset(new std::thread([this] {
        while (!running_.load()) {
            LOG(INFO) << "register thread wait for start";
            std::unique_lock l(mtx_);
            cv_.wait_for(l, std::chrono::seconds(config::meta_server_register_interval_ms));
        }
        LOG(INFO) << "register thread begins to run";
        std::mt19937 gen(std::random_device("/dev/urandom")());
        std::uniform_int_distribution<int> rd_len(50, 300);

        while (running_.load()) {
            std::string key = system_meta_service_registry_key();
            std::string val;
            std::unique_ptr<Transaction> txn;
            int tried = 0;
            do {
                int ret = txn_kv_->create_txn(&txn);
                if (ret != 0) break;
                ret = txn->get(key, &val);
                if (ret != 0 && ret != 1) break;
                ServiceRegistryPB reg;
                if (ret == 0 && !reg.ParseFromString(val)) break;
                LOG(INFO) << "get server registry, key=" << hex(key)
                          << " reg=" << proto_to_json(reg);
                prepare_registry(&reg);
                val = reg.SerializeAsString();
                if (val.empty()) break;
                txn->put(key, val);
                LOG(INFO) << "put server registry, key=" << hex(key)
                          << " reg=" << proto_to_json(reg);
                ret = txn->commit();
                if (ret != 0) {
                    LOG(WARNING) << "failed to commit registry, key=" << hex(key)
                                 << " val=" << proto_to_json(reg) << " retry times=" << ++tried;
                    std::this_thread::sleep_for(std::chrono::milliseconds(rd_len(gen)));
                    continue;
                }
            } while (false);
            std::unique_lock l(mtx_);
            cv_.wait_for(l, std::chrono::milliseconds(config::meta_server_register_interval_ms));
        }
        LOG(INFO) << "register thread quits";
    }));
}

int MetaServerRegister::start() {
    if (txn_kv_ == nullptr) return -1;
    running_.store(true);
    cv_.notify_all();
    return 0;
}

void MetaServerRegister::stop() {
    running_.store(false);
    cv_.notify_all();
    if (register_thread_ != nullptr) register_thread_->join();
}

} // namespace selectdb
// vim: et tw=120 ts=4 sw=4 cc=80:
