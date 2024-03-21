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

#include "cloud/olap/storage_engine.h"

// IWYU pragma: no_include <bthread/errno.h>
#include <errno.h> // IWYU pragma: keep
#include <fmt/format.h>
#include <gen_cpp/AgentService_types.h>
#include <rapidjson/document.h>
#include <rapidjson/encodings.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <thrift/protocol/TDebugProtocol.h>

#include <algorithm>
#include <boost/algorithm/string/case_conv.hpp>
#include <boost/container/detail/std_fwd.hpp>
#include <chrono>
#include <filesystem>
#include <iterator>
#include <list>
#include <new>
#include <ostream>
#include <random>
#include <set>
#include <thread>
#include <utility>

#include "agent/task_worker_pool.h"
#include "cloud/cloud_base_compaction.h"
#include "cloud/cloud_cumulative_compaction.h"
#include "cloud/cloud_meta_mgr.h"
#include "cloud/utils.h"
#include "common/config.h"
#include "common/logging.h"
#include "gutil/strings/substitute.h"
#include "io/fs/local_file_system.h"
#include "io/fs/s3_file_system.h"
#include "olap/base_compaction.h"
#include "olap/binlog.h"
#include "olap/cumulative_compaction.h"
#include "olap/cumulative_compaction_time_series_policy.h"
#include "olap/data_dir.h"
#include "olap/memtable_flush_executor.h"
#include "olap/olap_define.h"
#include "olap/olap_meta.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/rowset/rowset_meta_manager.h"
#include "olap/rowset/segment_v2/inverted_index_cache.h"
#include "olap/rowset/unique_rowset_id_generator.h"
#include "olap/schema_cache.h"
#include "olap/segment_loader.h"
#include "olap/single_replica_compaction.h"
#include "olap/storage_policy.h"
#include "olap/tablet_manager.h"
#include "olap/tablet_meta.h"
#include "olap/task/engine_task.h"
#include "olap/txn_manager.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/stream_load/stream_load_recorder.h"
#include "util/doris_metrics.h"
#include "util/metrics.h"
#include "util/spinlock.h"
#include "util/stopwatch.hpp"
#include "util/thread.h"
#include "util/threadpool.h"
#include "util/trace.h"
#include "util/uid_util.h"

using apache::thrift::ThriftDebugString;
using std::filesystem::canonical;
using std::filesystem::directory_iterator;
using std::filesystem::path;
using std::filesystem::recursive_directory_iterator;
using std::back_inserter;
using std::copy;
using std::inserter;
using std::list;
using std::map;
using std::nothrow;
using std::pair;
using std::set;
using std::string;
using std::stringstream;
using std::vector;
using strings::Substitute;

namespace doris {
using namespace ErrorCode;

DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(unused_rowsets_count, MetricUnit::ROWSETS);

StorageEngine* StorageEngine::_s_instance = nullptr;

static Status _validate_options(const EngineOptions& options) {
    if (options.store_paths.empty()) {
        return Status::InternalError("store paths is empty");
    }
    return Status::OK();
}

Status StorageEngine::open(const EngineOptions& options, StorageEngine** engine_ptr) {
    RETURN_IF_ERROR(_validate_options(options));
    LOG(INFO) << "starting backend using uid:" << options.backend_uid.to_string();
    std::unique_ptr<StorageEngine> engine(new StorageEngine(options));
    RETURN_NOT_OK_STATUS_WITH_WARN(engine->_open(), "open engine failed");
    *engine_ptr = engine.release();
    LOG(INFO) << "success to init storage engine.";
    return Status::OK();
}

StorageEngine::StorageEngine(const EngineOptions& options)
        : _options(options),
          _meta_mgr(std::make_unique<cloud::CloudMetaMgr>()),
          _tablet_mgr(std::make_unique<cloud::CloudTabletMgr>()),
          _available_storage_medium_type_count(0),
          _effective_cluster_id(-1),
          _is_all_cluster_id_exist(true),
          _stopped(false),
          _segcompaction_mem_tracker(std::make_shared<MemTracker>("SegCompaction")),
          _segment_meta_mem_tracker(std::make_shared<MemTracker>("SegmentMeta")),
          _stop_background_threads_latch(1),
          _delete_bitmap_txn_manager(nullptr),
          _rowset_id_generator(new UniqueRowsetIdGenerator(options.backend_uid)),
          _memtable_flush_executor(nullptr),
          _calc_delete_bitmap_executor(nullptr),
          _default_rowset_type(BETA_ROWSET),
          _heartbeat_flags(nullptr) {
    _s_instance = this;

    _cumulative_compaction_policies[CUMULATIVE_SIZE_BASED_POLICY] =
            CumulativeCompactionPolicyFactory::create_cumulative_compaction_policy(
                    CUMULATIVE_SIZE_BASED_POLICY);
    _cumulative_compaction_policies[CUMULATIVE_TIME_SERIES_POLICY] =
            CumulativeCompactionPolicyFactory::create_cumulative_compaction_policy(
                    CUMULATIVE_TIME_SERIES_POLICY);

    REGISTER_HOOK_METRIC(unused_rowsets_count, [this]() {
        // std::lock_guard<std::mutex> lock(_gc_mutex);
        return _unused_rowsets.size();
    });
}

StorageEngine::~StorageEngine() {
    DEREGISTER_HOOK_METRIC(unused_rowsets_count);

    for (auto&& thread : _bg_threads) {
        if (thread != nullptr) {
            thread->join();
        }
    }

    if (_base_compaction_thread_pool) {
        _base_compaction_thread_pool->shutdown();
    }
    if (_cumu_compaction_thread_pool) {
        _cumu_compaction_thread_pool->shutdown();
    }

    if (_tablet_meta_checkpoint_thread_pool) {
        _tablet_meta_checkpoint_thread_pool->shutdown();
    }
    _clear();
    _s_instance = nullptr;
}

void StorageEngine::load_data_dirs(const std::vector<DataDir*>& data_dirs) {
    std::vector<std::thread> threads;
    for (auto data_dir : data_dirs) {
        threads.emplace_back([data_dir] {
            auto res = data_dir->load();
            if (!res.ok()) {
                LOG(WARNING) << "io error when init load tables. res=" << res
                             << ", data dir=" << data_dir->path();
                // TODO(lingbin): why not exit progress, to force OP to change the conf
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
}

Status StorageEngine::_open() {
    // init store_map
    RETURN_NOT_OK_STATUS_WITH_WARN(_init_store_map(), "_init_store_map failed");

    _effective_cluster_id = config::cluster_id;
    RETURN_NOT_OK_STATUS_WITH_WARN(_check_all_root_path_cluster_id(), "fail to check cluster id");

    _update_storage_medium_type_count();

    RETURN_NOT_OK_STATUS_WITH_WARN(_check_file_descriptor_number(), "check fd number failed");

    auto dirs = get_stores<false>();
#ifdef CLOUD_MODE
    CHECK(dirs.size() == 1);
    _meta_mgr = std::make_unique<cloud::CloudMetaMgr>();
    RETURN_IF_ERROR(_meta_mgr->open());
    std::vector<std::tuple<std::string, S3Conf>> s3_infos;

    do {
        auto st = _meta_mgr->get_s3_info(&s3_infos);
        if (st.ok()) {
            break;
        }
        LOG(WARNING) << "failed to get s3 info, retry after 5s, err=" << st;
        std::this_thread::sleep_for(std::chrono::seconds(5));
    } while (true);

    CHECK(!s3_infos.empty()) << "no s3 infos";
    for (auto& [id, s3_conf] : s3_infos) {
        LOG(INFO) << "get s3 info: " << s3_conf.to_string() << " resource_id=" << id;
        std::shared_ptr<io::S3FileSystem> s3_fs;
        RETURN_IF_ERROR(io::S3FileSystem::create(std::move(s3_conf), id, &s3_fs));
        RETURN_IF_ERROR(s3_fs->connect());
        put_storage_resource(std::atol(id.c_str()), {s3_fs, 0});
    }
    set_latest_fs(get_filesystem(std::get<0>(s3_infos.back())));

    _delete_bitmap_txn_manager.reset(
            new DeleteBitmapTxnManager(config::delete_bitmap_agg_cache_capacity));
    RETURN_IF_ERROR(_delete_bitmap_txn_manager->init());
#else
    load_data_dirs(dirs);
#endif

    _memtable_flush_executor.reset(new MemTableFlushExecutor());
    _memtable_flush_executor->init(dirs);

    _calc_delete_bitmap_executor.reset(new CalcDeleteBitmapExecutor());
    _calc_delete_bitmap_executor->init();

    _parse_default_rowset_type();

    return Status::OK();
}

Status StorageEngine::_init_store_map() {
    std::vector<DataDir*> tmp_stores;
    std::vector<std::thread> threads;
    SpinLock error_msg_lock;
    std::string error_msg;
    for (auto& path : _options.store_paths) {
        DataDir* store = new DataDir(path.path, path.capacity_bytes, path.storage_medium,
                                     _tablet_manager.get(), _txn_manager.get());
        tmp_stores.emplace_back(store);
        threads.emplace_back([store, &error_msg_lock, &error_msg]() {
            auto st = store->init();
            if (!st.ok()) {
                {
                    std::lock_guard<SpinLock> l(error_msg_lock);
                    error_msg.append(st.to_string() + ";");
                }
                LOG(WARNING) << "Store load failed, status=" << st.to_string()
                             << ", path=" << store->path();
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }

    if (!error_msg.empty()) {
        for (auto store : tmp_stores) {
            delete store;
        }
        return Status::InternalError("init path failed, error={}", error_msg);
    }

    for (auto store : tmp_stores) {
        _store_map.emplace(store->path(), store);
    }

    std::string stream_load_record_path;
    if (!tmp_stores.empty()) {
        stream_load_record_path = tmp_stores[0]->path();
    }

    RETURN_NOT_OK_STATUS_WITH_WARN(_init_stream_load_recorder(stream_load_record_path),
                                   "init StreamLoadRecorder failed");

    return Status::OK();
}

Status StorageEngine::_init_stream_load_recorder(const std::string& stream_load_record_path) {
    LOG(INFO) << "stream load record path: " << stream_load_record_path;
    // init stream load record rocksdb
    _stream_load_recorder = StreamLoadRecorder::create_unique(stream_load_record_path);
    if (_stream_load_recorder == nullptr) {
        RETURN_NOT_OK_STATUS_WITH_WARN(
                Status::MemoryAllocFailed("allocate memory for StreamLoadRecorder failed"),
                "new StreamLoadRecorder failed");
    }
    auto st = _stream_load_recorder->init();
    if (!st.ok()) {
        RETURN_NOT_OK_STATUS_WITH_WARN(
                Status::IOError("open StreamLoadRecorder rocksdb failed, path={}",
                                stream_load_record_path),
                "init StreamLoadRecorder failed");
    }
    return Status::OK();
}

void StorageEngine::_update_storage_medium_type_count() {
    set<TStorageMedium::type> available_storage_medium_types;

    std::lock_guard<std::mutex> l(_store_lock);
    for (auto& it : _store_map) {
        if (it.second->is_used()) {
            available_storage_medium_types.insert(it.second->storage_medium());
        }
    }

    _available_storage_medium_type_count = available_storage_medium_types.size();
}

Status StorageEngine::_judge_and_update_effective_cluster_id(int32_t cluster_id) {
    if (cluster_id == -1 && _effective_cluster_id == -1) {
        // maybe this is a new cluster, cluster id will get from heartbeat message
        return Status::OK();
    } else if (cluster_id != -1 && _effective_cluster_id == -1) {
        _effective_cluster_id = cluster_id;
        return Status::OK();
    } else if (cluster_id == -1 && _effective_cluster_id != -1) {
        // _effective_cluster_id is the right effective cluster id
        return Status::OK();
    } else {
        if (cluster_id != _effective_cluster_id) {
            RETURN_NOT_OK_STATUS_WITH_WARN(
                    Status::Corruption("multiple cluster ids is not equal. one={}, other={}",
                                       _effective_cluster_id, cluster_id),
                    "cluster id not equal");
        }
    }

    return Status::OK();
}

template <bool include_unused>
std::vector<DataDir*> StorageEngine::get_stores() {
    std::vector<DataDir*> stores;
    stores.reserve(_store_map.size());

    std::lock_guard<std::mutex> l(_store_lock);
    if constexpr (include_unused) {
        for (auto& it : _store_map) {
            stores.push_back(it.second);
        }
    } else {
        for (auto& it : _store_map) {
            if (it.second->is_used()) {
                stores.push_back(it.second);
            }
        }
    }
    return stores;
}

template std::vector<DataDir*> StorageEngine::get_stores<false>();
template std::vector<DataDir*> StorageEngine::get_stores<true>();

Status StorageEngine::get_all_data_dir_info(std::vector<DataDirInfo>* data_dir_infos,
                                            bool need_update) {
    Status res = Status::OK();
    data_dir_infos->clear();

    MonotonicStopWatch timer;
    timer.start();

    // 1. update available capacity of each data dir
    // get all root path info and construct a path map.
    // path -> DataDirInfo
    std::map<std::string, DataDirInfo> path_map;
    {
        std::lock_guard<std::mutex> l(_store_lock);
        for (auto& it : _store_map) {
            if (need_update) {
                it.second->update_capacity();
            }
            path_map.emplace(it.first, it.second->get_dir_info());
        }
    }

    // 2. get total tablets' size of each data dir
    size_t tablet_count = 0;
    _tablet_manager->update_root_path_info(&path_map, &tablet_count);

    // 3. update metrics in DataDir
    for (auto& path : path_map) {
        std::lock_guard<std::mutex> l(_store_lock);
        auto data_dir = _store_map.find(path.first);
        DCHECK(data_dir != _store_map.end());
        data_dir->second->update_local_data_size(path.second.local_used_capacity);
        data_dir->second->update_remote_data_size(path.second.remote_used_capacity);
    }

    // add path info to data_dir_infos
    for (auto& entry : path_map) {
        data_dir_infos->emplace_back(entry.second);
    }

    timer.stop();
    LOG(INFO) << "get root path info cost: " << timer.elapsed_time() / 1000000
              << " ms. tablet counter: " << tablet_count;

    return res;
}

int64_t StorageEngine::get_file_or_directory_size(const std::string& file_path) {
    if (!std::filesystem::exists(file_path)) {
        return 0;
    }
    if (!std::filesystem::is_directory(file_path)) {
        return std::filesystem::file_size(file_path);
    }
    int64_t sum_size = 0;
    for (const auto& it : std::filesystem::directory_iterator(file_path)) {
        sum_size += get_file_or_directory_size(it.path());
    }
    return sum_size;
}

// TODO(lingbin): Should be in EnvPosix?
Status StorageEngine::_check_file_descriptor_number() {
    struct rlimit l;
    int ret = getrlimit(RLIMIT_NOFILE, &l);
    if (ret != 0) {
        LOG(WARNING) << "call getrlimit() failed. errno=" << strerror(errno)
                     << ", use default configuration instead.";
        return Status::OK();
    }
    if (l.rlim_cur < config::min_file_descriptor_number) {
        LOG(ERROR) << "File descriptor number is less than " << config::min_file_descriptor_number
                   << ". Please use (ulimit -n) to set a value equal or greater than "
                   << config::min_file_descriptor_number;
        return Status::InternalError("file descriptors limit is too small");
    }
    return Status::OK();
}

Status StorageEngine::_check_all_root_path_cluster_id() {
    int32_t cluster_id = -1;
    for (auto& it : _store_map) {
        int32_t tmp_cluster_id = it.second->cluster_id();
        if (it.second->cluster_id_incomplete()) {
            _is_all_cluster_id_exist = false;
        } else if (tmp_cluster_id == cluster_id) {
            // both have right cluster id, do nothing
        } else if (cluster_id == -1) {
            cluster_id = tmp_cluster_id;
        } else {
            RETURN_NOT_OK_STATUS_WITH_WARN(
                    Status::Corruption("multiple cluster ids is not equal. one={}, other={}",
                                       cluster_id, tmp_cluster_id),
                    "cluster id not equal");
        }
    }

    // judge and get effective cluster id
    RETURN_IF_ERROR(_judge_and_update_effective_cluster_id(cluster_id));

    // write cluster id into cluster_id_path if get effective cluster id success
    if (_effective_cluster_id != -1 && !_is_all_cluster_id_exist) {
        set_cluster_id(_effective_cluster_id);
    }

    return Status::OK();
}

Status StorageEngine::set_cluster_id(int32_t cluster_id) {
    std::lock_guard<std::mutex> l(_store_lock);
    for (auto& it : _store_map) {
        RETURN_IF_ERROR(it.second->set_cluster_id(cluster_id));
    }
    _effective_cluster_id = cluster_id;
    _is_all_cluster_id_exist = true;
    return Status::OK();
}

std::vector<DataDir*> StorageEngine::get_stores_for_create_tablet(
        TStorageMedium::type storage_medium) {
    std::vector<DataDir*> stores;
    return stores;
}

DataDir* StorageEngine::get_store(const std::string& path) {
    // _store_map is unchanged, no need to lock
    auto it = _store_map.find(path);
    if (it == _store_map.end()) {
        return nullptr;
    }
    return it->second;
}

void StorageEngine::stop() {
    // trigger the waiting threads
    notify_listeners();

    {
        std::lock_guard<std::mutex> l(_store_lock);
        for (auto& store_pair : _store_map) {
            store_pair.second->stop_bg_worker();
        }
    }

    _stop_background_threads_latch.count_down();
#define THREAD_JOIN(thread) \
    if (thread) {           \
        thread->join();     \
    }

    THREAD_JOIN(_compaction_tasks_producer_thread);
    THREAD_JOIN(_unused_rowset_monitor_thread);
    THREAD_JOIN(_garbage_sweeper_thread);
    THREAD_JOIN(_disk_stat_monitor_thread);
    THREAD_JOIN(_fd_cache_clean_thread);
    THREAD_JOIN(_tablet_checkpoint_tasks_producer_thread);
#undef THREAD_JOIN

#define THREADS_JOIN(threads)            \
    for (const auto& thread : threads) { \
        if (thread) {                    \
            thread->join();              \
        }                                \
    }

    THREADS_JOIN(_path_gc_threads);
    THREADS_JOIN(_path_scan_threads);
#undef THREADS_JOIN
    _stopped = true;
}

void StorageEngine::_clear() {
    std::lock_guard<std::mutex> l(_store_lock);
    for (auto& store_pair : _store_map) {
        delete store_pair.second;
        store_pair.second = nullptr;
    }
    _store_map.clear();
}

void StorageEngine::clear_transaction_task(const TTransactionId transaction_id) {
    // clear transaction task may not contains partitions ids, we should get partition id from txn manager.
    std::vector<int64_t> partition_ids;
    StorageEngine::instance()->txn_manager()->get_partition_ids(transaction_id, &partition_ids);
    clear_transaction_task(transaction_id, partition_ids);
}

void StorageEngine::clear_transaction_task(const TTransactionId transaction_id,
                                           const std::vector<TPartitionId>& partition_ids) {
    LOG(INFO) << "begin to clear transaction task. transaction_id=" << transaction_id;

    for (const TPartitionId& partition_id : partition_ids) {
        std::map<TabletInfo, RowsetSharedPtr> tablet_infos;
        StorageEngine::instance()->txn_manager()->get_txn_related_tablets(
                transaction_id, partition_id, &tablet_infos);

        // each tablet
        for (auto& tablet_info : tablet_infos) {
            // should use tablet uid to ensure clean txn correctly
            TabletSharedPtr tablet = _tablet_manager->get_tablet(tablet_info.first.tablet_id,
                                                                 tablet_info.first.tablet_uid);
            // The tablet may be dropped or altered, leave a INFO log and go on process other tablet
            if (tablet == nullptr) {
                LOG(INFO) << "tablet is no longer exist. tablet_id=" << tablet_info.first.tablet_id
                          << ", tablet_uid=" << tablet_info.first.tablet_uid;
                continue;
            }
            StorageEngine::instance()->txn_manager()->delete_txn(partition_id, tablet,
                                                                 transaction_id);
        }
    }
    LOG(INFO) << "finish to clear transaction task. transaction_id=" << transaction_id;
}

void StorageEngine::_start_clean_cache() {
    CacheManager::instance()->for_each_cache_prune_stale();
}

Status StorageEngine::start_trash_sweep(double* usage, bool ignore_guard) {
    Status res = Status::OK();

    std::unique_lock<std::mutex> l(_trash_sweep_lock, std::defer_lock);
    if (!l.try_lock()) {
        LOG(INFO) << "trash and snapshot sweep is running.";
        return res;
    }

    LOG(INFO) << "start trash and snapshot sweep.";

    const int32_t snapshot_expire = config::snapshot_expire_time_sec;
    const int32_t trash_expire = config::trash_file_expire_time_sec;
    // the guard space should be lower than storage_flood_stage_usage_percent,
    // so here we multiply 0.9
    // if ignore_guard is true, set guard_space to 0.
    const double guard_space =
            ignore_guard ? 0 : config::storage_flood_stage_usage_percent / 100.0 * 0.9;
    std::vector<DataDirInfo> data_dir_infos;
    RETURN_NOT_OK_STATUS_WITH_WARN(get_all_data_dir_info(&data_dir_infos, false),
                                   "failed to get root path stat info when sweep trash.")
    std::sort(data_dir_infos.begin(), data_dir_infos.end(), DataDirInfoLessAvailability());

    time_t now = time(nullptr); //获取UTC时间
    tm local_tm_now;
    local_tm_now.tm_isdst = 0;
    if (localtime_r(&now, &local_tm_now) == nullptr) {
        LOG(WARNING) << "fail to localtime_r time. time=" << now;
        return Status::Error<OS_ERROR>("fail to localtime_r time. time={}", now);
    }
    const time_t local_now = mktime(&local_tm_now); //得到当地日历时间

    double tmp_usage = 0.0;
    for (DataDirInfo& info : data_dir_infos) {
        LOG(INFO) << "Start to sweep path " << info.path;
        if (!info.is_used) {
            continue;
        }

        double curr_usage = (double)(info.disk_capacity - info.available) / info.disk_capacity;
        tmp_usage = std::max(tmp_usage, curr_usage);

        Status curr_res = Status::OK();
        auto snapshot_path = fmt::format("{}/{}", info.path, SNAPSHOT_PREFIX);
        curr_res = _do_sweep(snapshot_path, local_now, snapshot_expire);
        if (!curr_res.ok()) {
            LOG(WARNING) << "failed to sweep snapshot. path=" << snapshot_path
                         << ", err_code=" << curr_res;
            res = curr_res;
        }

        auto trash_path = fmt::format("{}/{}", info.path, TRASH_PREFIX);
        curr_res = _do_sweep(trash_path, local_now, curr_usage > guard_space ? 0 : trash_expire);
        if (!curr_res.ok()) {
            LOG(WARNING) << "failed to sweep trash. path=" << trash_path
                         << ", err_code=" << curr_res;
            res = curr_res;
        }
    }

    // _gc_binlogs();

    if (usage != nullptr) {
        *usage = tmp_usage; // update usage
    }

    // clear expire incremental rowset, move deleted tablet to trash
    _tablet_manager->start_trash_sweep();

    // clean rubbish transactions
    _clean_unused_txns();

    // clean unused rowset metas in OlapMeta
    _clean_unused_rowset_metas();

    // clean unused rowsets in remote storage backends
    for (auto data_dir : get_stores()) {
        data_dir->perform_remote_rowset_gc();
        data_dir->perform_remote_tablet_gc();
    }

    return res;
}

void StorageEngine::_clean_unused_rowset_metas() {
    std::vector<RowsetMetaSharedPtr> invalid_rowset_metas;
    auto clean_rowset_func = [this, &invalid_rowset_metas](TabletUid tablet_uid, RowsetId rowset_id,
                                                           const std::string& meta_str) -> bool {
        // return false will break meta iterator, return true to skip this error
        RowsetMetaSharedPtr rowset_meta(new RowsetMeta());
        bool parsed = rowset_meta->init(meta_str);
        if (!parsed) {
            LOG(WARNING) << "parse rowset meta string failed for rowset_id:" << rowset_id;
            invalid_rowset_metas.push_back(rowset_meta);
            return true;
        }
        if (rowset_meta->tablet_uid() != tablet_uid) {
            LOG(WARNING) << "tablet uid is not equal, skip the rowset"
                         << ", rowset_id=" << rowset_meta->rowset_id()
                         << ", in_put_tablet_uid=" << tablet_uid
                         << ", tablet_uid in rowset meta=" << rowset_meta->tablet_uid();
            invalid_rowset_metas.push_back(rowset_meta);
            return true;
        }

        TabletSharedPtr tablet = _tablet_manager->get_tablet(rowset_meta->tablet_id());
        if (tablet == nullptr) {
            // tablet may be dropped
            // TODO(cmy): this is better to be a VLOG, because drop table is a very common case.
            // leave it as INFO log for observation. Maybe change it in future.
            LOG(INFO) << "failed to find tablet " << rowset_meta->tablet_id()
                      << " for rowset: " << rowset_meta->rowset_id() << ", tablet may be dropped";
            invalid_rowset_metas.push_back(rowset_meta);
            return true;
        }
        if (tablet->tablet_uid() != rowset_meta->tablet_uid()) {
            // In this case, we get the tablet using the tablet id recorded in the rowset meta.
            // but the uid in the tablet is different from the one recorded in the rowset meta.
            // How this happened:
            // Replica1 of Tablet A exists on BE1. Because of the clone task, a new replica2 is createed on BE2,
            // and then replica1 deleted from BE1. After some time, we created replica again on BE1,
            // which will creates a new tablet with the same id but a different uid.
            // And in the historical version, when we deleted the replica, we did not delete the corresponding rowset meta,
            // thus causing the original rowset meta to remain(with same tablet id but different uid).
            LOG(WARNING) << "rowset's tablet uid " << rowset_meta->tablet_uid()
                         << " does not equal to tablet uid: " << tablet->tablet_uid();
            invalid_rowset_metas.push_back(rowset_meta);
            return true;
        }
        if (rowset_meta->rowset_state() == RowsetStatePB::VISIBLE &&
            (!tablet->rowset_meta_is_useful(rowset_meta))) {
            LOG(INFO) << "rowset meta is not used any more, remove it. rowset_id="
                      << rowset_meta->rowset_id();
            invalid_rowset_metas.push_back(rowset_meta);
        }
        return true;
    };
    auto data_dirs = get_stores();
    for (auto data_dir : data_dirs) {
        RowsetMetaManager::traverse_rowset_metas(data_dir->get_meta(), clean_rowset_func);
        for (auto& rowset_meta : invalid_rowset_metas) {
            RowsetMetaManager::remove(data_dir->get_meta(), rowset_meta->tablet_uid(),
                                      rowset_meta->rowset_id());
        }
        LOG(INFO) << "remove " << invalid_rowset_metas.size()
                  << " invalid rowset meta from dir: " << data_dir->path();
        invalid_rowset_metas.clear();
    }
}

void StorageEngine::_clean_unused_txns() {
    std::set<TabletInfo> tablet_infos;
    _txn_manager->get_all_related_tablets(&tablet_infos);
    for (auto& tablet_info : tablet_infos) {
        TabletSharedPtr tablet =
                _tablet_manager->get_tablet(tablet_info.tablet_id, tablet_info.tablet_uid, true);
        if (tablet == nullptr) {
            // TODO(ygl) :  should check if tablet still in meta, it's a improvement
            // case 1: tablet still in meta, just remove from memory
            // case 2: tablet not in meta store, remove rowset from meta
            // currently just remove them from memory
            // nullptr to indicate not remove them from meta store
            _txn_manager->force_rollback_tablet_related_txns(nullptr, tablet_info.tablet_id,
                                                             tablet_info.schema_hash,
                                                             tablet_info.tablet_uid);
        }
    }
}

Status StorageEngine::_do_sweep(const std::string& scan_root, const time_t& local_now,
                                const int32_t expire) {
    Status res = Status::OK();
    bool exists = true;
    RETURN_IF_ERROR(io::global_local_filesystem()->exists(scan_root, &exists));
    if (!exists) {
        // dir not existed. no need to sweep trash.
        return res;
    }

    try {
        // Sort pathes by name, that is by delete time.
        std::vector<path> sorted_pathes;
        std::copy(directory_iterator(scan_root), directory_iterator(),
                  std::back_inserter(sorted_pathes));
        std::sort(sorted_pathes.begin(), sorted_pathes.end());
        for (const auto& sorted_path : sorted_pathes) {
            string dir_name = sorted_path.filename().string();
            string str_time = dir_name.substr(0, dir_name.find('.'));
            tm local_tm_create;
            local_tm_create.tm_isdst = 0;
            if (strptime(str_time.c_str(), "%Y%m%d%H%M%S", &local_tm_create) == nullptr) {
                LOG(WARNING) << "fail to strptime time. time=" << str_time;
                res = Status::Error<OS_ERROR>("fail to strptime time. time={}", str_time);
                continue;
            }

            int32_t actual_expire = expire;
            // try get timeout in dir name, the old snapshot dir does not contain timeout
            // eg: 20190818221123.3.86400, the 86400 is timeout, in second
            size_t pos = dir_name.find('.', str_time.size() + 1);
            if (pos != string::npos) {
                actual_expire = std::stoi(dir_name.substr(pos + 1));
            }
            VLOG_TRACE << "get actual expire time " << actual_expire << " of dir: " << dir_name;

            string path_name = sorted_path.string();
            if (difftime(local_now, mktime(&local_tm_create)) >= actual_expire) {
                res = io::global_local_filesystem()->delete_directory(path_name);
                if (!res.ok()) {
                    continue;
                }
            } else {
                // Because files are ordered by filename, i.e. by create time, so all the left files are not expired.
                break;
            }
        }
    } catch (...) {
        LOG(WARNING) << "Exception occur when scan directory. path_desc=" << scan_root;
        res = Status::Error<IO_ERROR>("Exception occur when scan directory. path_desc={}",
                                      scan_root);
    }

    return res;
}

// invalid rowset type config will return ALPHA_ROWSET for system to run smoothly
void StorageEngine::_parse_default_rowset_type() {
    std::string default_rowset_type_config = config::default_rowset_type;
    boost::to_upper(default_rowset_type_config);
    if (default_rowset_type_config == "BETA") {
        _default_rowset_type = BETA_ROWSET;
    } else if (default_rowset_type_config == "ALPHA") {
        _default_rowset_type = ALPHA_ROWSET;
        LOG(WARNING) << "default_rowset_type in be.conf should be set to beta, alpha is not "
                        "supported any more";
    } else {
        LOG(FATAL) << "unknown value " << default_rowset_type_config
                   << " in default_rowset_type in be.conf";
    }
}

void StorageEngine::start_delete_unused_rowset() {
    std::unordered_map<std::string, RowsetSharedPtr> unused_rowsets_copy;
    {
        std::lock_guard<std::mutex> lock(_gc_mutex);
        for (auto it = _unused_rowsets.begin(); it != _unused_rowsets.end();) {
            if (it->second.use_count() == 1 && it->second->need_delete_file()) {
                if (it->second->is_local()) {
                    unused_rowsets_copy[it->first] = it->second;
                }
                // remote rowset data will be reclaimed by `remove_unused_remote_files`
                it = _unused_rowsets.erase(it);
            } else {
                ++it;
            }
        }
    }
    for (auto it = unused_rowsets_copy.begin(); it != unused_rowsets_copy.end(); ++it) {
        VLOG_NOTICE << "start to remove rowset:" << it->second->rowset_id()
                    << ", version:" << it->second->version().first << "-"
                    << it->second->version().second;
        Status status = it->second->remove();
        VLOG_NOTICE << "remove rowset:" << it->second->rowset_id()
                    << " finished. status:" << status;
    }
}

void StorageEngine::add_unused_rowset(const RowsetSharedPtr& rowset) {
#ifdef CLOUD_MODE
    LOG(FATAL) << "Should not call add_unused_rowset in cloud mode";
#else
    if (rowset == nullptr) {
        return;
    }

    VLOG_NOTICE << "add unused rowset, rowset id:" << rowset->rowset_id()
                << ", version:" << rowset->version().first << "-" << rowset->version().second
                << ", unique id:" << rowset->unique_id();

    auto rowset_id = rowset->rowset_id().to_string();

    std::lock_guard<std::mutex> lock(_gc_mutex);
    auto it = _unused_rowsets.find(rowset_id);
    if (it == _unused_rowsets.end()) {
        rowset->set_need_delete_file();
        rowset->close();
        _unused_rowsets[rowset_id] = rowset;
        release_rowset_id(rowset->rowset_id());
    }
#endif
}

// TODO(zc): refactor this funciton
Status StorageEngine::create_tablet(const TCreateTabletReq& request) {
    // Get all available stores, use ref_root_path if the caller specified
    std::vector<DataDir*> stores;
    stores = get_stores_for_create_tablet(request.storage_medium);
    if (stores.empty()) {
        LOG(WARNING) << "there is no available disk that can be used to create tablet.";
        return Status::Error<CE_CMD_PARAMS_ERROR>(
                "there is no available disk that can be used to create tablet.");
    }
    return _tablet_manager->create_tablet(request, stores, nullptr);
}

Status StorageEngine::obtain_shard_path(TStorageMedium::type storage_medium,
                                        std::string* shard_path, DataDir** store) {
    LOG(INFO) << "begin to process obtain root path. storage_medium=" << storage_medium;

    if (shard_path == nullptr) {
        LOG(WARNING) << "invalid output parameter which is null pointer.";
        return Status::Error<CE_CMD_PARAMS_ERROR>(
                "invalid output parameter which is null pointer.");
    }

    auto stores = get_stores_for_create_tablet(storage_medium);
    if (stores.empty()) {
        LOG(WARNING) << "no available disk can be used to create tablet.";
        return Status::Error<NO_AVAILABLE_ROOT_PATH>(
                "no available disk can be used to create tablet.");
    }

    Status res = Status::OK();
    uint64_t shard = 0;
    res = stores[0]->get_shard(&shard);
    if (!res.ok()) {
        LOG(WARNING) << "fail to get root path shard. res=" << res;
        return res;
    }

    std::stringstream root_path_stream;
    root_path_stream << stores[0]->path() << "/" << DATA_PREFIX << "/" << shard;
    *shard_path = root_path_stream.str();
    *store = stores[0];

    LOG(INFO) << "success to process obtain root path. path=" << shard_path;
    return res;
}

Status StorageEngine::load_header(const string& shard_path, const TCloneReq& request,
                                  bool restore) {
    LOG(INFO) << "begin to process load headers."
              << "tablet_id=" << request.tablet_id << ", schema_hash=" << request.schema_hash;
    Status res = Status::OK();

    DataDir* store = nullptr;
    {
        // TODO(zc)
        try {
            auto store_path =
                    std::filesystem::path(shard_path).parent_path().parent_path().string();
            store = get_store(store_path);
            if (store == nullptr) {
                LOG(WARNING) << "invalid shard path, path=" << shard_path;
                return Status::Error<INVALID_ROOT_PATH>("invalid shard path, path={}", shard_path);
            }
        } catch (...) {
            LOG(WARNING) << "invalid shard path, path=" << shard_path;
            return Status::Error<INVALID_ROOT_PATH>("invalid shard path, path={}", shard_path);
        }
    }

    std::stringstream schema_hash_path_stream;
    schema_hash_path_stream << shard_path << "/" << request.tablet_id << "/" << request.schema_hash;
    // not surely, reload and restore tablet action call this api
    // reset tablet uid here

    string header_path = TabletMeta::construct_header_file_path(schema_hash_path_stream.str(),
                                                                request.tablet_id);
    res = _tablet_manager->load_tablet_from_dir(store, request.tablet_id, request.schema_hash,
                                                schema_hash_path_stream.str(), false, restore);
    if (!res.ok()) {
        LOG(WARNING) << "fail to process load headers. res=" << res;
        return res;
    }

    LOG(INFO) << "success to process load headers.";
    return res;
}

void StorageEngine::register_report_listener(TaskWorkerPool* listener) {
    std::lock_guard<std::mutex> l(_report_mtx);
    CHECK(_report_listeners.find(listener) == _report_listeners.end());
    _report_listeners.insert(listener);
}

void StorageEngine::deregister_report_listener(TaskWorkerPool* listener) {
    std::lock_guard<std::mutex> l(_report_mtx);
    _report_listeners.erase(listener);
}

void StorageEngine::notify_listeners() {
    std::lock_guard<std::mutex> l(_report_mtx);
    for (auto& listener : _report_listeners) {
        listener->notify_thread();
    }
}

Status StorageEngine::execute_task(EngineTask* task) {
    RETURN_IF_ERROR(task->execute());
    return task->finish();
}

// check whether any unused rowsets's id equal to rowset_id
bool StorageEngine::check_rowset_id_in_unused_rowsets(const RowsetId& rowset_id) {
    std::lock_guard<std::mutex> lock(_gc_mutex);
    auto search = _unused_rowsets.find(rowset_id.to_string());
    return search != _unused_rowsets.end();
}

// Return json:
// {
//   "CumulativeCompaction": [10001, 10002],
//   "BaseCompaction": [10001, 10002]
// }
Status StorageEngine::get_compaction_status_json(std::string* result) {
    rapidjson::Document root;
    root.SetObject();

    std::lock_guard lock(_compaction_mtx);
    // cumu
    std::string_view cumu = "CumulativeCompaction";
    rapidjson::Value cumu_key;
    cumu_key.SetString(cumu.data(), cumu.length(), root.GetAllocator());
    rapidjson::Document cumu_arr;
    cumu_arr.SetArray();
    for (auto& [tablet_id, v] : _submitted_cumu_compactions) {
        for (int i = 0; i < v.size(); ++i) {
            cumu_arr.PushBack(tablet_id, root.GetAllocator());
        }
    }
    root.AddMember(cumu_key, cumu_arr, root.GetAllocator());
    // base
    std::string_view base = "BaseCompaction";
    rapidjson::Value base_key;
    base_key.SetString(base.data(), base.length(), root.GetAllocator());
    rapidjson::Document base_arr;
    base_arr.SetArray();
    for (auto& [tablet_id, _] : _submitted_base_compactions) {
        base_arr.PushBack(tablet_id, root.GetAllocator());
    }
    root.AddMember(base_key, base_arr, root.GetAllocator());

    rapidjson::StringBuffer strbuf;
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(strbuf);
    root.Accept(writer);
    *result = std::string(strbuf.GetString());
    return Status::OK();
}

std::shared_ptr<CumulativeCompactionPolicy> StorageEngine::get_cumulative_compaction_policy(
        std::string_view compaction_policy) {
    if (!_cumulative_compaction_policies.contains(compaction_policy)) {
        return _cumulative_compaction_policies.at(CUMULATIVE_SIZE_BASED_POLICY);
    }
    return _cumulative_compaction_policies.at(compaction_policy);
}

} // namespace doris
