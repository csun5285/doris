
#pragma once

#include <atomic>
#include <condition_variable>
#include <deque>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include "recycler/white_black_list.h"

namespace selectdb {
class ObjStoreAccessor;
class InstanceChecker;
class TxnKv;
class InstanceInfoPB;

class Checker {
public:
    explicit Checker(std::shared_ptr<TxnKv> txn_kv);
    ~Checker();

    int start();

    void stop();
    bool stopped() const { return stopped_.load(std::memory_order_acquire); }

private:
    void lease_check_jobs();

private:
    friend class RecyclerServiceImpl;

    std::shared_ptr<TxnKv> txn_kv_;
    std::atomic_bool stopped_;
    std::string ip_port_;
    std::vector<std::thread> workers_;

    std::mutex mtx_;
    // notify check workers
    std::condition_variable pending_instance_cond_;
    std::deque<InstanceInfoPB> pending_instance_queue_;
    std::unordered_set<std::string> pending_instance_set_;
    std::unordered_map<std::string, std::shared_ptr<InstanceChecker>> working_instance_map_;
    // notify instance scanner and lease thread
    std::condition_variable notifier_;

    WhiteBlackList instance_filter_;
};

class InstanceChecker {
public:
    explicit InstanceChecker(std::shared_ptr<TxnKv> txn_kv, const std::string& instance_id);
    // Return 0 if success, otherwise error
    int init(const InstanceInfoPB& instance);
    // Return 0 if success, otherwise failed
    int do_check();
    // Check whether the objects in the object store of the instance belong to the visible rowsets.
    // This function is used to verify that there is no garbage data leakage, should only be called in recycler test.
    // Return 0 if success, otherwise failed
    int do_inverted_check();
    void stop() { stopped_.store(true, std::memory_order_release); }
    bool stopped() const { return stopped_.load(std::memory_order_acquire); }

private:
    std::atomic_bool stopped_ {false};
    std::shared_ptr<TxnKv> txn_kv_;
    std::string instance_id_;
    std::unordered_map<std::string, std::shared_ptr<ObjStoreAccessor>> accessor_map_;
};

} // namespace selectdb
