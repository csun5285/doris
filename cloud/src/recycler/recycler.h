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
#include "recycler/white_black_list.h"

namespace selectdb {
class Checker;

class Recycler {
public:
    Recycler();
    ~Recycler();

    // returns 0 for success otherwise error
    int start(bool with_brpc);

    void join();

    void add_pending_instances(std::vector<InstanceInfoPB> instances);

private:
    void recycle_callback();

    std::vector<InstanceInfoPB> get_instances();
    void instance_scanner_callback();

    bool prepare_instance_recycle_job(const std::string& instance_id);
    void finish_instance_recycle_job(const std::string& instance_id);
    void lease_instance_recycle_job(const std::string& instance_id);
    void do_lease();

private:
    std::shared_ptr<TxnKv> txn_kv_;
    std::unique_ptr<brpc::Server> server_;

    std::unique_ptr<Checker> checker_;

    std::vector<std::thread> workers_;

    // notify recycle workers
    std::condition_variable pending_instance_cond_;
    std::mutex pending_instance_mtx_;
    std::deque<InstanceInfoPB> pending_instance_queue_;
    std::unordered_set<std::string> pending_instance_set_;

    // for lease mechanism
    std::mutex recycling_instance_set_mtx_;
    std::unordered_set<std::string> recycling_instance_set_;

    std::string ip_port_;

    WhiteBlackList instance_filter_;
};

class InstanceRecycler {
public:
    explicit InstanceRecycler(std::shared_ptr<TxnKv> txn_kv, const InstanceInfoPB& instance);
    ~InstanceRecycler();

    int init();

    // remove all kv and data in this instance, ONLY be called when instance has been deleted
    void recycle_instance();

    // scan and recycle expired indexes
    void recycle_indexes();

    // scan and recycle expired partitions
    void recycle_partitions();

    // scan and recycle expired rowsets
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
     *
     * @param recycle_func defines how to recycle resources corresponding to a key-value pair. Returns 0 if the recycling is successful.
     * @param loop_done is called after `RangeGetIterator` has no next kv. Usually used to perform a batch recycling. Returns 0 if success. 
     * @return 0 if all corresponding resources are recycled successfully, otherwise non-zero
     */
    int scan_and_recycle(std::string begin, std::string_view end,
                         std::function<int(std::string_view k, std::string_view v)> recycle_func,
                         std::function<int()> loop_done = nullptr);
    // return 0 for success otherwise error
    int delete_rowset_data(const doris::RowsetMetaPB& rs_meta_pb);
    // return 0 for success otherwise error
    // NOTE: this function ONLY be called when the file paths cannot be calculated
    int delete_rowset_data(const std::string& resource_id, int64_t tablet_id,
                           const std::string& rowset_id);
    // return 0 for success otherwise error
    int delete_rowset_data(const std::vector<doris::RowsetMetaPB>& rowsets);

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

    class InvertedIndexIdCache;
    std::unique_ptr<InvertedIndexIdCache> inverted_index_id_cache_;

    std::mutex recycled_tablets_mtx_;
    // Store recycled tablets, we can skip deleting rowset data of these tablets because these data has already been deleted.
    std::unordered_set<int64_t> recycled_tablets_;
};

} // namespace selectdb
