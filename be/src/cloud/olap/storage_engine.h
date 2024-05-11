// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <pthread.h>

#include <condition_variable>
#include <ctime>
#include <list>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "cloud/meta_mgr.h"
#include "cloud/olap/delete_bitmap_txn_manager.h"
#include "common/status.h"
#include "gen_cpp/AgentService_types.h"
#include "gen_cpp/BackendService_types.h"
#include "gen_cpp/MasterService_types.h"
#include "gutil/ref_counted.h"
#include "io/fs/file_system.h"
#include "olap/calc_delete_bitmap_executor.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/olap_meta.h"
#include "olap/options.h"
#include "olap/rowset/rowset_id_generator.h"
#include "olap/tablet.h"
#include "olap/tablet_manager.h"
#include "olap/task/engine_task.h"
#include "olap/txn_manager.h"
#include "rapidjson/document.h"
#include "runtime/heartbeat_flags.h"
#include "runtime/stream_load/stream_load_recorder.h"
#include "util/countdown_latch.h"
#include "util/thread.h"
#include "util/threadpool.h"

namespace doris {
namespace cloud {
class MetaMgr;
class CloudTabletMgr;
} // namespace cloud

class DataDir;
class EngineTask;
class BlockManager;
class MemTableFlushExecutor;
class Tablet;
class TaskWorkerPool;
class BetaRowsetWriter;
class CloudCumulativeCompaction;
class CloudBaseCompaction;
class CloudFullCompaction;
class CumulativeCompactionPolicy;

using SegCompactionCandidates = std::vector<segment_v2::SegmentSharedPtr>;
using SegCompactionCandidatesSharedPtr = std::shared_ptr<SegCompactionCandidates>;

// StorageEngine singleton to manage all Table pointers.
// Providing add/drop/get operations.
// StorageEngine instance doesn't own the Table resources, just hold the pointer,
// allocation/deallocation must be done outside.
class StorageEngine {
public:
    StorageEngine(const EngineOptions& options);
    ~StorageEngine();

    int64_t memory_limitation_bytes_per_thread_for_schema_change() const;

    static Status open(const EngineOptions& options, StorageEngine** engine_ptr);

    static StorageEngine* instance() { return _s_instance; }

    io::FileSystemSPtr latest_fs() const {
        std::lock_guard lock(_latest_fs_mtx);
        return _latest_fs;
    }

    void set_latest_fs(const io::FileSystemSPtr& fs) {
        std::lock_guard lock(_latest_fs_mtx);
        _latest_fs = fs;
    }

    Status create_tablet(const TCreateTabletReq& request);

    void clear_transaction_task(const TTransactionId transaction_id);
    void clear_transaction_task(const TTransactionId transaction_id,
                                const std::vector<TPartitionId>& partition_ids);

    // Note: 这里只能reload原先已经存在的root path，即re-load启动时就登记的root path
    // 是允许的，但re-load全新的path是不允许的，因为此处没有彻底更新ce调度器信息
    void load_data_dirs(const std::vector<DataDir*>& stores);

    template <bool include_unused = false>
    std::vector<DataDir*> get_stores();

    // @brief 设置root_path是否可用
    void set_store_used_flag(const std::string& root_path, bool is_used);

    // @brief 获取所有root_path信息
    Status get_all_data_dir_info(std::vector<DataDirInfo>* data_dir_infos, bool need_update);

    int64_t get_file_or_directory_size(const std::string& file_path);

    // get root path for creating tablet. The returned vector of root path should be random,
    // for avoiding that all the tablet would be deployed one disk.
    std::vector<DataDir*> get_stores_for_create_tablet(TStorageMedium::type storage_medium);
    DataDir* get_store(const std::string& path);

    uint32_t available_storage_medium_type_count() const {
        return _available_storage_medium_type_count;
    }

    Status set_cluster_id(int32_t cluster_id);
    int32_t effective_cluster_id() const { return _effective_cluster_id; }

    void start_delete_unused_rowset();
    void add_unused_rowset(const RowsetSharedPtr& rowset);

    // Obtain shard path for new tablet.
    //
    // @param [out] shard_path choose an available root_path to clone new tablet
    // @return error code
    Status obtain_shard_path(TStorageMedium::type storage_medium, std::string* shared_path,
                             DataDir** store);

    // Load new tablet to make it effective.
    //
    // @param [in] root_path specify root path of new tablet
    // @param [in] request specify new tablet info
    // @param [in] restore whether we're restoring a tablet from trash
    // @return OLAP_SUCCESS if load tablet success
    Status load_header(const std::string& shard_path, const TCloneReq& request,
                       bool restore = false);

    void register_report_listener(TaskWorkerPool* listener);
    void deregister_report_listener(TaskWorkerPool* listener);
    void notify_listeners();

    Status execute_task(EngineTask* task);

    TabletManager* tablet_manager() { return _tablet_manager.get(); }
    TxnManager* txn_manager() { return _txn_manager.get(); }

    DeleteBitmapTxnManager* delete_bitmap_txn_manager() { return _delete_bitmap_txn_manager.get(); }

    MemTableFlushExecutor* memtable_flush_executor() { return _memtable_flush_executor.get(); }
    CalcDeleteBitmapExecutor* calc_delete_bitmap_executor() {
        return _calc_delete_bitmap_executor.get();
    }

    cloud::MetaMgr* meta_mgr() { return _meta_mgr.get(); }
    cloud::CloudTabletMgr* tablet_mgr() { return _tablet_mgr.get(); }

    bool has_base_compaction(int64_t tablet_id) const;
    bool has_cumu_compaction(int64_t tablet_id) const;
    bool has_full_compaction(int64_t tablet_id) const;
    void get_cumu_compaction(int64_t tablet_id,
                             std::vector<std::shared_ptr<CloudCumulativeCompaction>>& res);

    bool check_rowset_id_in_unused_rowsets(const RowsetId& rowset_id);

    RowsetId next_rowset_id() { return _rowset_id_generator->next_id(); };

    bool rowset_id_in_use(const RowsetId& rowset_id) {
        return _rowset_id_generator->id_in_use(rowset_id);
    }

    void release_rowset_id(const RowsetId& rowset_id) {
        return _rowset_id_generator->release_id(rowset_id);
    }

    RowsetTypePB default_rowset_type() const {
        if (_heartbeat_flags != nullptr && _heartbeat_flags->is_set_default_rowset_type_to_beta()) {
            return BETA_ROWSET;
        }
        return _default_rowset_type;
    }

    void set_heartbeat_flags(HeartbeatFlags* heartbeat_flags) {
        _heartbeat_flags = heartbeat_flags;
    }

    // start all background threads. This should be call after env is ready.
    Status start_bg_threads();

    // CLOUD_MODE
    Status cloud_start_bg_threads();

    // clear trash and snapshot file
    // option: update disk usage after sweep
    Status start_trash_sweep(double* usage, bool ignore_guard = false);

    void stop();

    bool stopped() { return _stopped; }

    std::shared_ptr<StreamLoadRecorder> get_stream_load_recorder() { return _stream_load_recorder; }

    Status get_compaction_status_json(std::string* result);

    std::shared_ptr<MemTracker> segment_meta_mem_tracker() { return _segment_meta_mem_tracker; }
    std::shared_ptr<MemTracker> segcompaction_mem_tracker() { return _segcompaction_mem_tracker; }

    // check cumulative compaction config
    void check_cumulative_compaction_config();

    Status submit_compaction_task(const TabletSharedPtr& tablet, CompactionType compaction_type);

    std::unique_ptr<ThreadPool>& tablet_publish_txn_thread_pool() {
        return _tablet_publish_txn_thread_pool;
    }

    std::unique_ptr<ThreadPool>& calc_tablet_delete_bitmap_task_thread_pool() {
        return _calc_tablet_delete_bitmap_task_thread_pool;
    }

    std::shared_ptr<CumulativeCompactionPolicy> get_cumulative_compaction_policy(
            std::string_view compaction_policy);

private:
    // Instance should be inited from `static open()`
    // MUST NOT be called in other circumstances.
    Status _open();

    // Clear status(tables, ...)
    void _clear();

    Status _init_store_map();

    void _update_storage_medium_type_count();

    // Some check methods
    Status _check_file_descriptor_number();
    Status _check_all_root_path_cluster_id();
    Status _judge_and_update_effective_cluster_id(int32_t cluster_id);

    bool _delete_tablets_on_unused_root_path();

    void _clean_unused_txns();

    void _clean_unused_rowset_metas();

    Status _do_sweep(const std::string& scan_root, const time_t& local_tm_now,
                     const int32_t expire);

    // CLOUD_MODE
    void _refresh_s3_info_thread_callback();

    // CLOUD_MODE
    void _vacuum_stale_rowsets_thread_callback();

    // CLOUD_MODE
    void _check_file_cache_ttl_block_valid();

    // CLOUD_MODE
    void _sync_tablets_thread_callback();

    // CLOUD_MODE
    void _lease_compaction_thread_callback();

    // All these xxx_callback() functions are for Background threads
    // unused rowset monitor thread
    void _unused_rowset_monitor_thread_callback();

    // garbage sweep thread process function. clear snapshot and trash folder
    void _garbage_sweeper_thread_callback();

    // delete tablet with io error process function
    void _disk_stat_monitor_thread_callback();

    // clean file descriptors cache
    void _fd_cache_clean_callback();

    // Check bucket enable versioning
    void _check_bucket_enable_versioning_callback();

    void _path_scan_thread_callback(DataDir* data_dir);

    void _tablet_checkpoint_callback(const std::vector<DataDir*>& data_dirs);

    // parse the default rowset type config to RowsetTypePB
    void _parse_default_rowset_type();

    void _start_clean_cache();

    void _compaction_tasks_producer_callback();

    std::vector<TabletSharedPtr> _generate_cloud_compaction_tasks(CompactionType compaction_type,
                                                                  bool check_score);

    Status _init_stream_load_recorder(const std::string& stream_load_record_path);

    void _adjust_compaction_thread_num();

    void _cooldown_tasks_producer_callback();

    Status _handle_seg_compaction(BetaRowsetWriter* writer,
                                  SegCompactionCandidatesSharedPtr segments);

    Status _submit_base_compaction_task(const TabletSharedPtr& tablet);

    Status _submit_cumulative_compaction_task(const TabletSharedPtr& tablet);

    Status _submit_full_compaction_task(const TabletSharedPtr& tablet);

private:
    EngineOptions _options;

    std::unique_ptr<cloud::MetaMgr> _meta_mgr;
    std::unique_ptr<cloud::CloudTabletMgr> _tablet_mgr;

    std::mutex _store_lock;
    std::mutex _trash_sweep_lock;
    std::map<std::string, DataDir*> _store_map;
    uint32_t _available_storage_medium_type_count;

    int32_t _effective_cluster_id;
    bool _is_all_cluster_id_exist;

    bool _stopped;

    static StorageEngine* _s_instance;

    // CLOUD_MODE
    // FileSystem with latest object store info, new data will be written to this fs.
    mutable std::mutex _latest_fs_mtx;
    io::FileSystemSPtr _latest_fs;

    std::mutex _gc_mutex;
    // map<rowset_id(str), RowsetSharedPtr>, if we use RowsetId as the key, we need custom hash func
    std::unordered_map<std::string, RowsetSharedPtr> _unused_rowsets;

    // StorageEngine oneself
    std::shared_ptr<MemTracker> _mem_tracker;
    // Count the memory consumption of segment compaction tasks.
    std::shared_ptr<MemTracker> _segcompaction_mem_tracker;
    // This mem tracker is only for tracking memory use by segment meta data such as footer or index page.
    // The memory consumed by querying is tracked in segment iterator.
    std::shared_ptr<MemTracker> _segment_meta_mem_tracker;

    CountDownLatch _stop_background_threads_latch;
    scoped_refptr<Thread> _unused_rowset_monitor_thread;
    // thread to monitor snapshot expiry
    scoped_refptr<Thread> _garbage_sweeper_thread;
    // thread to monitor disk stat
    scoped_refptr<Thread> _disk_stat_monitor_thread;
    // thread to produce both base and cumulative compaction tasks
    scoped_refptr<Thread> _compaction_tasks_producer_thread;
    scoped_refptr<Thread> _fd_cache_clean_thread;
    // threads to clean all file descriptor not actively in use
    std::vector<scoped_refptr<Thread>> _path_gc_threads;
    // threads to scan disk paths
    std::vector<scoped_refptr<Thread>> _path_scan_threads;
    // thread to produce tablet checkpoint tasks
    scoped_refptr<Thread> _tablet_checkpoint_tasks_producer_thread;

    // CLOUD_MODE
    std::vector<scoped_refptr<Thread>> _bg_threads;

    // For tablet and disk-stat report
    std::mutex _report_mtx;
    std::set<TaskWorkerPool*> _report_listeners;

    std::mutex _engine_task_mutex;

    std::unique_ptr<TabletManager> _tablet_manager;
    std::unique_ptr<TxnManager> _txn_manager;
    std::unique_ptr<DeleteBitmapTxnManager> _delete_bitmap_txn_manager;

    std::unique_ptr<RowsetIdGenerator> _rowset_id_generator;

    std::unique_ptr<MemTableFlushExecutor> _memtable_flush_executor;
    std::unique_ptr<CalcDeleteBitmapExecutor> _calc_delete_bitmap_executor;

    // Used to control the migration from segment_v1 to segment_v2, can be deleted in futrue.
    // Type of new loaded data
    RowsetTypePB _default_rowset_type;

    HeartbeatFlags* _heartbeat_flags;

    std::unique_ptr<ThreadPool> _tablet_publish_txn_thread_pool;

    std::unique_ptr<ThreadPool> _tablet_meta_checkpoint_thread_pool;

    std::unique_ptr<ThreadPool> _calc_tablet_delete_bitmap_task_thread_pool;

    // ATTN: Compactions in maps depend on `CloudTabletMgr` and `CloudMetaMgr`
    mutable std::mutex _compaction_mtx;
    // tablet_id -> submitted base compaction, guarded by `_compaction_mtx`
    std::unordered_map<int64_t, std::shared_ptr<CloudBaseCompaction>> _submitted_base_compactions;
    // tablet_id -> submitted full compaction, guarded by `_compaction_mtx`
    std::unordered_map<int64_t, std::shared_ptr<CloudFullCompaction>> _submitted_full_compactions;
    // Store tablets which are preparing cumu compaction, guarded by `_compaction_mtx`
    std::unordered_set<int64_t> _tablet_preparing_cumu_compaction;
    // tablet_id -> submitted cumu compactions, guarded by `_compaction_mtx`
    std::unordered_map<int64_t, std::vector<std::shared_ptr<CloudCumulativeCompaction>>>
            _submitted_cumu_compactions;

    // ATTN: MUST shutdown before destory compaction maps, as compaction maps are referenced by the
    // callbacks running on the threadpool
    std::unique_ptr<ThreadPool> _base_compaction_thread_pool;
    std::unique_ptr<ThreadPool> _cumu_compaction_thread_pool;

    std::shared_ptr<StreamLoadRecorder> _stream_load_recorder;

    using CumuPolices =
            std::unordered_map<std::string_view, std::shared_ptr<CumulativeCompactionPolicy>>;
    CumuPolices _cumulative_compaction_policies;

    scoped_refptr<Thread> _cooldown_tasks_producer_thread;

    scoped_refptr<Thread> _cache_file_cleaner_tasks_producer_thread;

    std::unique_ptr<ThreadPool> _cooldown_thread_pool;

    std::mutex _running_cooldown_mutex;
    std::unordered_map<DataDir*, int64_t> _running_cooldown_tasks_cnt;
    std::unordered_set<int64_t> _running_cooldown_tablets;

    int64_t _memory_limitation_bytes_for_schema_change;

    DISALLOW_COPY_AND_ASSIGN(StorageEngine);
};

} // namespace doris
