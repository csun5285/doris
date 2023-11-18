#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <thread>

#include "common/logging.h"
#include "meta-service/txn_kv.h"

namespace selectdb {

class FdbMetricExporter {
public:
    FdbMetricExporter(std::shared_ptr<TxnKv> txn_kv)
            : txn_kv_(std::move(txn_kv)), running_(false) {}
    ~FdbMetricExporter();

    int start();
    void stop();

    static void export_fdb_metrics(TxnKv* txn_kv);

private:
    std::shared_ptr<TxnKv> txn_kv_;
    std::unique_ptr<std::thread> thread_;
    std::atomic<bool> running_;
    std::mutex running_mtx_;
    std::condition_variable running_cond_;
    int sleep_interval_ms_ = 5000;
};

} // namespace selectdb