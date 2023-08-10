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
    ~FdbMetricExporter() {
        bool expect = true;
        if (running_.compare_exchange_strong(expect, false)) {
            running_cond_.notify_all();
            if (thread_ != nullptr) thread_->join();
        }
    }

    int start() {
        if (txn_kv_ == nullptr) return -1;
        running_ = true;
        thread_.reset(new std::thread([this] {
            while(running_.load(std::memory_order_acquire)) {
                export_fdb_metrics(txn_kv_.get());
                std::unique_lock l(running_mtx_);
                running_cond_.wait_for(l, std::chrono::milliseconds(sleep_interval_ms_),
                                [this]() { return !running_.load(std::memory_order_acquire); });
                LOG(INFO) << "finish to collect fdb metric";
            }
        }));
        return 0;
    }

    void stop() {
        running_ = false;
        running_cond_.notify_all();
        if (thread_ != nullptr) {
            thread_->join();
        }
    }

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