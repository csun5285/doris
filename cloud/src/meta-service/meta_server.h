// Container of meta service
#pragma once

// clang-format off
#include "common/metric.h"
#include "txn_kv.h"

#include "brpc/server.h"

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
// clang-format on

namespace selectdb {

class MetaServerRegister;

class MetaServer {
public:
    MetaServer();
    ~MetaServer() = default;

    /**
     * Starts to listen and server
     *
     * return 0 for success otherwise failure
     */
    int start();

    void join();

private:
    std::unique_ptr<brpc::Server> server_;
    std::shared_ptr<TxnKv> txn_kv_;
    std::unique_ptr<MetaServerRegister> server_register_;
    std::unique_ptr<FdbMetricExporter> fdb_metric_exporter_;
};

class ServiceRegistryPB;

class MetaServerRegister {
public:
    MetaServerRegister(std::shared_ptr<TxnKv> txn_kv);
    ~MetaServerRegister() {
        bool expect = true;
        if (running_.compare_exchange_strong(expect, false)) {
            cv_.notify_all();
            if (register_thread_ != nullptr) register_thread_->join();
        }
    };

    /**
     * Starts registering
     *
     * @return 0 on success, otherwise failure.
     */
    int start();

    /**
     * Notifies all the threads to quit and stop registering current server.
     * TODO(gavin): should we remove the server from the registry list actively
     *              when we call stop().
     */
    void stop();

private:
    /**
     * Prepares registry with given existing registry. If the server already
     * exists in the registry list, update mtime and lease, otherwise create a
     * new item for the server in the rgistry list.
     * 
     * @param reg input and output param
     */
    void prepare_registry(ServiceRegistryPB* reg);

private:
    std::unique_ptr<std::thread> register_thread_;
    std::atomic<bool> running_;
    std::mutex mtx_;
    std::condition_variable cv_;
    std::string id_;

    std::shared_ptr<TxnKv> txn_kv_; // Relies on other members, must be the
                                    // first to destruct
};

} // namespace selectdb
// vim: et ts=4 sw=4 cc=80:
