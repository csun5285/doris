#pragma once

#include <gen_cpp/selectdb_cloud.pb.h>

#include <atomic>
#include <condition_variable>
#include <deque>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <thread>

#include "recycler/white_black_list.h"

namespace brpc {
class Server;
} // namespace brpc

namespace selectdb {
class TxnKv;
class InstanceRecycler;
class ObjStoreAccessor;
class Checker;
struct RecyclerThreadPoolGroup;
class SimpleThreadPool;
struct RecyclerThreadPoolGroup {
    RecyclerThreadPoolGroup();
    ~RecyclerThreadPoolGroup() = default;
    std::unique_ptr<SimpleThreadPool> s3_producer_pool;
    std::unique_ptr<SimpleThreadPool> recycle_tablet_pool;
    std::unique_ptr<SimpleThreadPool> group_recycle_function_pool;
};

class Recycler {
public:
    explicit Recycler(std::shared_ptr<TxnKv> txn_kv);
    ~Recycler();

    // returns 0 for success otherwise error
    int start(brpc::Server* server);

    void stop();

    bool stopped() const { return stopped_.load(std::memory_order_acquire); }

private:
    void recycle_callback();

    void instance_scanner_callback();

    void lease_recycle_jobs();

    void check_recycle_tasks();

private:
    friend class RecyclerServiceImpl;

    std::shared_ptr<TxnKv> txn_kv_;
    std::atomic_bool stopped_ {false};

    std::vector<std::thread> workers_;

    std::mutex mtx_;
    // notify recycle workers
    std::condition_variable pending_instance_cond_;
    std::deque<InstanceInfoPB> pending_instance_queue_;
    std::unordered_set<std::string> pending_instance_set_;
    std::unordered_map<std::string, std::shared_ptr<InstanceRecycler>> recycling_instance_map_;
    // notify instance scanner and lease thread
    std::condition_variable notifier_;

    std::string ip_port_;

    WhiteBlackList instance_filter_;
    std::unique_ptr<Checker> checker_;
    std::unique_ptr<RecyclerThreadPoolGroup> _thread_pool_group;
};

class InstanceRecycler {
public:
    explicit InstanceRecycler(std::shared_ptr<TxnKv> txn_kv, const InstanceInfoPB& instance,
                              const RecyclerThreadPoolGroup& thread_pool_group);
    ~InstanceRecycler();

    int init();

    void stop() { stopped_.store(true, std::memory_order_release); }
    bool stopped() const { return stopped_.load(std::memory_order_acquire); }

    // returns 0 for success otherwise error
    int do_recycle();

    // remove all kv and data in this instance, ONLY be called when instance has been deleted
    // returns 0 for success otherwise error
    int recycle_deleted_instance();

    // scan and recycle expired indexes
    // returns 0 for success otherwise error
    int recycle_indexes();

    // scan and recycle expired partitions
    // returns 0 for success otherwise error
    int recycle_partitions();

    // scan and recycle expired rowsets
    // returns 0 for success otherwise error
    int recycle_rowsets();

    // scan and recycle expired tmp rowsets
    // returns 0 for success otherwise error
    int recycle_tmp_rowsets();

    /**
     * recycle all tablets belonging to the index specified by `index_id`
     *
     * @param partition_id if positive, only recycle tablets in this partition belonging to the specified index
     * @param is_empty_tablet indicates whether the tablet has object files, can skip delete objects if tablet is empty
     * @return 0 for success otherwise error
     */
    int recycle_tablets(int64_t table_id, int64_t index_id, int64_t partition_id = -1,
                        bool is_empty_tablet = false);

    /**
     * recycle all rowsets belonging to the tablet specified by `tablet_id`
     *
     * @return 0 for success otherwise error
     */
    int recycle_tablet(int64_t tablet_id);

    // scan and recycle useless partition version kv
    int recycle_versions();

    // scan and abort timeout txn label
    // returns 0 for success otherwise error
    int abort_timeout_txn();

    //scan and recycle expire txn label
    // returns 0 for success otherwise error
    int recycle_expired_txn_label();

    // scan and recycle finished or timeout copy jobs
    // returns 0 for success otherwise error
    int recycle_copy_jobs();

    // scan and recycle dropped internal stage
    // returns 0 for success otherwise error
    int recycle_stage();

    // scan and recycle expired stage objects
    // returns 0 for success otherwise error
    int recycle_expired_stage_objects();

    bool check_recycle_tasks();

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

    void register_recycle_task(const std::string& task_name, int64_t start_time);

    void unregister_recycle_task(const std::string& task_name);

private:
    std::atomic_bool stopped_ {false};
    std::shared_ptr<TxnKv> txn_kv_;
    std::string instance_id_;
    InstanceInfoPB instance_info_;
    std::unordered_map<std::string, std::shared_ptr<ObjStoreAccessor>> accessor_map_;

    class InvertedIndexIdCache;
    std::unique_ptr<InvertedIndexIdCache> inverted_index_id_cache_;

    std::mutex recycled_tablets_mtx_;
    // Store recycled tablets, we can skip deleting rowset data of these tablets because these data has already been deleted.
    std::unordered_set<int64_t> recycled_tablets_;

    std::mutex recycle_tasks_mutex;
    // <task_name, start_time>>
    std::map<std::string, int64_t> running_recycle_tasks;
    const RecyclerThreadPoolGroup& _thread_pool_group;
};

} // namespace selectdb
