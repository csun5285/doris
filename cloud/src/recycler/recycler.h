#pragma once

#include <brpc/server.h>
#include <gen_cpp/selectdb_cloud.pb.h>

#include <condition_variable>
#include <deque>
#include <functional>
#include <memory>
#include <string>
#include <string_view>

#include "meta-service/txn_kv.h"
#include "recycler/s3_accessor.h"

namespace selectdb {

class Recycler {
public:
    Recycler();
    ~Recycler() = default;

    // returns 0 for success otherwise error
    int start();

    void join();

    void add_pending_instances(std::vector<InstanceInfoPB> instances);

private:
    void recycle_callback();

    // standalone mode
    std::vector<InstanceInfoPB> get_instances();
    void instance_scanner_callback();

    bool prepare_instance_recycle_job(const std::string& instance_id);
    void finish_instance_recycle_job(const std::string& instance_id);
    void lease_instance_recycle_job(const std::string& instance_id);
    void do_lease();

private:
    std::shared_ptr<TxnKv> txn_kv_;
    std::unique_ptr<brpc::Server> server_;

    std::vector<std::thread> workers_;
    std::condition_variable pending_instance_queue_cond_;
    std::mutex pending_instance_queue_mtx_;

    std::deque<InstanceInfoPB> pending_instance_queue_;

    std::mutex recycling_instance_set_mtx_;
    std::unordered_set<std::string> recycling_instance_set_;

    // standalone mode
    std::mutex instance_scanner_mtx_;
    std::condition_variable instance_scanner_cond_;

    std::string ip_port_;

    class InstanceFilter {
    public:
        void reset(const std::string& whitelist, const std::string& blacklist);
        bool filter_out(const std::string& instance_id) const;

    private:
        std::set<std::string> whitelist_;
        std::set<std::string> blacklist_;
    };
    InstanceFilter instance_filter_;
};

class InstanceRecycler {
public:
    explicit InstanceRecycler(std::shared_ptr<TxnKv> txn_kv, const InstanceInfoPB& instance);
    ~InstanceRecycler() = default;

    // scan and recycle expired indexes
    void recycle_indexes();

    // scan and recycle expired partitions
    void recycle_partitions();

    // scan and recycle expired prepared rowsets
    void recycle_rowsets();

    // scan and recycle expired tmp rowsets
    void recycle_tmp_rowsets();

    /**
     * recycle all tablets belonging to the index specified by `index_id`
     *
     * @param partition_id if positive, only recycle tablets in this partition belonging to the specified index
     * @return 0 for success otherwise error
     */
    int recycle_tablets(int64_t table_id, int64_t index_id, int64_t partition_id = -1);

    /**
     * recycle all rowsets belonging to the tablet specified by `tablet_id`
     *
     * @return 0 for success otherwise error
     */
    int recycle_tablet(int64_t tablet_id);

    // scan and abort timeout txn label
    void abort_timeout_txn();

    //scan and recycle expire txn label
    void recycle_expired_txn_label();

    // scan and recycle finished or timeout copy jobs
    void recycle_copy_jobs();

    // scan and recycle dropped internal stage
    void recycle_stage();

    // scan and recycle expired stage objects
    void recycle_expired_stage_objects();

private:
    /**
     * Scan key-value pairs between [`begin`, `end`), and perform `recycle_func` on each key-value pair.
     * This function will also remove successfully recycled key-value pairs from kv store.
     *
     * @param recycle_func defines how to recycle resources corresponding to a key-value pair. Returns true if the recycling is successful.
     * @param try_range_remove_kv if true, try to remove key-value pairs from kv store by range which is more efficient
     * @return 0 if all corresponding resources(including key-value pairs) are recycled successfully, otherwise non-zero
     */
    int scan_and_recycle(std::string begin, std::string_view end,
                         std::function<bool(std::string_view k, std::string_view v)> recycle_func,
                         bool try_range_remove_kv = false);

    int delete_rowset_data(const doris::RowsetMetaPB& rs_meta_pb);

    int delete_rowset_data(const std::string& rowset_id, const RecycleRowsetPB& recycl_rs_pb);

    /**
     * Get stage storage info from instance and init ObjStoreAccessor
     * @return 0 if accessor is successfully inited, 1 if stage not found, negative for error
     */
    int init_copy_job_accessor(const std::string& stage_id, const StagePB::StageType& stage_type,
                               std::shared_ptr<ObjStoreAccessor>* accessor);

private:
    std::shared_ptr<TxnKv> txn_kv_;
    std::string instance_id_;
    InstanceInfoPB instance_info_;
    std::unordered_map<std::string, std::shared_ptr<ObjStoreAccessor>> accessor_map_;
};

} // namespace selectdb
