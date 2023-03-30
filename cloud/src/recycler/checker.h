
#pragma once

#include <condition_variable>
#include <deque>

#include "gen_cpp/selectdb_cloud.pb.h"
#include "meta-service/txn_kv.h"

namespace selectdb {

class Checker {
public:
    Checker(std::shared_ptr<TxnKv> txn_kv) : txn_kv_(std::move(txn_kv)) {}

    int start();

    void stop();

private:
    // FIXME(plat1ko): duplicate with `Recycler::get_instances`
    std::vector<InstanceInfoPB> get_instances();

    void check_instance_objects(const InstanceInfoPB& instance);

private:
    std::shared_ptr<TxnKv> txn_kv_;
    std::vector<std::thread> workers_;

    // notify check workers
    std::condition_variable pending_instance_cond_;
    std::mutex pending_instance_mtx_;
    std::deque<InstanceInfoPB> pending_instance_queue_;
    std::unordered_set<std::string> pending_instance_set_;

    std::mutex working_instance_set_mtx_;
    std::unordered_set<std::string> working_instance_set_; 

    // notify instance scanner
    std::condition_variable instance_scanner_cond_;
    std::mutex instance_scanner_mtx_;
};

} // namespace selectdb
