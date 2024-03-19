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

#include <gperftools/profiler.h>
#include <sys/socket.h>
#include <unistd.h>

#include <boost/algorithm/string.hpp>
#include <chrono>
#include <cmath>
#include <ctime>
#include <random>
#include <ranges>
#include <string>

#include "cloud/cloud_base_compaction.h"
#include "cloud/cloud_cumulative_compaction.h"
#include "cloud/cloud_full_compaction.h"
#include "cloud/olap/storage_engine.h"
#include "cloud/utils.h"
#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "gutil/strings/substitute.h"
#include "io/cache/block/block_file_cache_factory.h"
#include "io/fs/s3_file_system.h"
#include "olap/cumulative_compaction.h"
#include "olap/cumulative_compaction_policy.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/rowset/beta_rowset_writer.h"
#include "olap/storage_policy.h"
#include "olap/tablet.h"
#include "util/time.h"

namespace doris {
using namespace ErrorCode;

// number of running SCHEMA-CHANGE threads
volatile uint32_t g_schema_change_active_threads = 0;

int get_cumu_thread_num() {
    if (config::max_cumu_compaction_threads > 0) {
        return config::max_cumu_compaction_threads;
    }

    int num_cores = doris::CpuInfo::num_cores();
    return std::min(std::max(int(num_cores * config::cumu_compaction_thread_num_factor), 2), 20);
}

int get_base_thread_num() {
    if (config::max_base_compaction_threads > 0) {
        return config::max_base_compaction_threads;
    }

    int num_cores = doris::CpuInfo::num_cores();
    return std::min(std::max(int(num_cores * config::base_compaction_thread_num_factor), 1), 10);
}

Status StorageEngine::start_bg_threads() {
    RETURN_IF_ERROR(Thread::create(
            "StorageEngine", "unused_rowset_monitor_thread",
            [this]() { this->_unused_rowset_monitor_thread_callback(); },
            &_unused_rowset_monitor_thread));
    LOG(INFO) << "unused rowset monitor thread started";

    // start thread for monitoring the snapshot and trash folder
    RETURN_IF_ERROR(Thread::create(
            "StorageEngine", "garbage_sweeper_thread",
            [this]() { this->_garbage_sweeper_thread_callback(); }, &_garbage_sweeper_thread));
    LOG(INFO) << "garbage sweeper thread started";

    // convert store map to vector
    std::vector<DataDir*> data_dirs;
    for (auto& tmp_store : _store_map) {
        data_dirs.push_back(tmp_store.second);
    }

    ThreadPoolBuilder("BaseCompactionTaskThreadPool")
            .set_min_threads(config::max_base_compaction_threads)
            .set_max_threads(config::max_base_compaction_threads)
            .build(&_base_compaction_thread_pool);
    ThreadPoolBuilder("CumuCompactionTaskThreadPool")
            .set_min_threads(config::max_cumu_compaction_threads)
            .set_max_threads(config::max_cumu_compaction_threads)
            .build(&_cumu_compaction_thread_pool);

    // compaction tasks producer thread
    RETURN_IF_ERROR(Thread::create(
            "StorageEngine", "compaction_tasks_producer_thread",
            [this]() { this->_compaction_tasks_producer_callback(); },
            &_compaction_tasks_producer_thread));
    LOG(INFO) << "compaction tasks producer thread started";
    int32_t max_checkpoint_thread_num = config::max_meta_checkpoint_threads;
    if (max_checkpoint_thread_num < 0) {
        max_checkpoint_thread_num = data_dirs.size();
    }
    ThreadPoolBuilder("TabletMetaCheckpointTaskThreadPool")
            .set_max_threads(max_checkpoint_thread_num)
            .build(&_tablet_meta_checkpoint_thread_pool);

    RETURN_IF_ERROR(Thread::create(
            "StorageEngine", "tablet_checkpoint_tasks_producer_thread",
            [this, data_dirs]() { this->_tablet_checkpoint_callback(data_dirs); },
            &_tablet_checkpoint_tasks_producer_thread));
    LOG(INFO) << "tablet checkpoint tasks producer thread started";

    // fd cache clean thread
    RETURN_IF_ERROR(Thread::create(
            "StorageEngine", "fd_cache_clean_thread",
            [this]() { this->_fd_cache_clean_callback(); }, &_fd_cache_clean_thread));
    LOG(INFO) << "fd cache clean thread started";

    ThreadPoolBuilder("CooldownTaskThreadPool")
            .set_min_threads(config::cooldown_thread_num)
            .set_max_threads(config::cooldown_thread_num)
            .build(&_cooldown_thread_pool);
    LOG(INFO) << "cooldown thread pool started";

    RETURN_IF_ERROR(Thread::create(
            "StorageEngine", "cooldown_tasks_producer_thread",
            [this]() { this->_cooldown_tasks_producer_callback(); },
            &_cooldown_tasks_producer_thread));
    LOG(INFO) << "cooldown tasks producer thread started";

    // add tablet publish version thread pool
    ThreadPoolBuilder("TabletPublishTxnThreadPool")
            .set_min_threads(config::tablet_publish_txn_max_thread)
            .set_max_threads(config::tablet_publish_txn_max_thread)
            .build(&_tablet_publish_txn_thread_pool);

    LOG(INFO) << "all storage engine's background threads are started.";
    return Status::OK();
}

Status StorageEngine::cloud_start_bg_threads() {
    RETURN_IF_ERROR(Thread::create(
            "StorageEngine", "refresh_s3_info_thread",
            [this]() { this->_refresh_s3_info_thread_callback(); }, &_bg_threads.emplace_back()));
    LOG(INFO) << "refresh s3 info thread started";

    RETURN_IF_ERROR(Thread::create(
            "StorageEngine", "vacuum_stale_rowsets_thread",
            [this]() { this->_vacuum_stale_rowsets_thread_callback(); },
            &_bg_threads.emplace_back()));
    LOG(INFO) << "vacuum stale rowsets thread started";

    if (config::file_cache_ttl_valid_check_interval_second != 0) {
        RETURN_IF_ERROR(Thread::create(
                "StorageEngine", "check_file_cache_ttl_block_valid_thread",
                [this]() { this->_check_file_cache_ttl_block_valid(); },
                &_bg_threads.emplace_back()));
        LOG(INFO) << "check file cache ttl block valid thread started";
    }

    RETURN_IF_ERROR(Thread::create(
            "StorageEngine", "sync_tablets_thread",
            [this]() { this->_sync_tablets_thread_callback(); }, &_bg_threads.emplace_back()));
    LOG(INFO) << "sync tablets thread started";

    RETURN_IF_ERROR(Thread::create(
            "StorageEngine", "lease_compaction_thread",
            [this]() { this->_lease_compaction_thread_callback(); }, &_bg_threads.emplace_back()));
    LOG(INFO) << "lease compaction thread started";

    // fd cache clean thread
    RETURN_IF_ERROR(Thread::create(
            "StorageEngine", "fd_cache_clean_thread",
            [this]() { this->_fd_cache_clean_callback(); }, &_bg_threads.emplace_back()));
    LOG(INFO) << "fd cache clean thread started";

    RETURN_IF_ERROR(Thread::create(
            "StorageEngine", "check_bucket_enable_versioning_thread",
            [this]() { this->_check_bucket_enable_versioning_callback(); },
            &_bg_threads.emplace_back()));
    LOG(INFO) << "check bucket enable versioning thread started";

    // compaction tasks producer thread
    int base_thread_num = get_base_thread_num();
    int cumu_thread_num = get_cumu_thread_num();
    ThreadPoolBuilder("BaseCompactionTaskThreadPool")
            .set_min_threads(base_thread_num)
            .set_max_threads(base_thread_num)
            .build(&_base_compaction_thread_pool);
    ThreadPoolBuilder("CumuCompactionTaskThreadPool")
            .set_min_threads(cumu_thread_num)
            .set_max_threads(cumu_thread_num)
            .build(&_cumu_compaction_thread_pool);
    RETURN_IF_ERROR(Thread::create(
            "StorageEngine", "compaction_tasks_producer_thread",
            [this]() { this->_compaction_tasks_producer_callback(); },
            &_bg_threads.emplace_back()));
    LOG(INFO) << "compaction tasks producer thread started,"
              << " base thread num " << base_thread_num << " cumu thread num " << cumu_thread_num;

    // add calculate tablet delete bitmap task thread pool
    ThreadPoolBuilder("TabletCalDeleteBitmapThreadPool")
            .set_min_threads(1)
            .set_max_threads(config::calc_tablet_delete_bitmap_task_max_thread)
            .build(&_calc_tablet_delete_bitmap_task_thread_pool);

    LOG(INFO) << "all storage engine's background threads for cloud are started.";
    return Status::OK();
}

void StorageEngine::_refresh_s3_info_thread_callback() {
    while (!_stop_background_threads_latch.wait_for(
            std::chrono::seconds(config::refresh_s3_info_interval_seconds))) {
        std::vector<std::tuple<std::string, S3Conf>> s3_infos;
        auto st = _meta_mgr->get_s3_info(&s3_infos);
        if (!st.ok()) {
            LOG(WARNING) << "failed to refresh object store info. err=" << st;
            continue;
        }
        CHECK(!s3_infos.empty()) << "no s3 infos";
        for (auto& [id, s3_conf] : s3_infos) {
            auto fs = get_filesystem(id);
            if (fs == nullptr) {
                LOG(INFO) << "get new s3 info: " << s3_conf.to_string() << " resource_id=" << id;
                std::shared_ptr<io::S3FileSystem> s3_fs;
                auto st = io::S3FileSystem::create(std::move(s3_conf), id, &s3_fs);
                if (!st.ok()) {
                    LOG(WARNING) << "failed to create s3 fs. id=" << id;
                    continue;
                }
                st = s3_fs->connect();
                if (!st.ok()) {
                    LOG(WARNING) << "failed to connect s3 fs. id=" << id;
                    continue;
                }
                put_storage_resource(std::atol(id.c_str()), {s3_fs, 0});
            } else {
                auto s3_fs = std::reinterpret_pointer_cast<io::S3FileSystem>(fs);
                if (s3_fs->s3_conf().ak != s3_conf.ak || s3_fs->s3_conf().sk != s3_conf.sk) {
                    auto cur_s3_conf = s3_fs->s3_conf();
                    LOG(INFO) << "update s3 info, old: " << cur_s3_conf.to_string()
                              << " new: " << s3_conf.to_string() << " resource_id=" << id;
                    cur_s3_conf.ak = s3_conf.ak;
                    cur_s3_conf.sk = s3_conf.sk;
                    s3_fs->set_conf(std::move(cur_s3_conf));
                    st = s3_fs->connect();
                    if (!st.ok()) {
                        LOG(WARNING) << "failed to connect s3 fs. id=" << id;
                    }
                }
                if (s3_conf.sse_enabled != s3_fs->s3_conf().sse_enabled) {
                    auto cur_s3_conf = s3_fs->s3_conf();
                    cur_s3_conf.sse_enabled = s3_conf.sse_enabled;
                    s3_fs->set_conf(std::move(cur_s3_conf));
                    s3_fs->reset_transfer_manager();
                }
            }
        }
        if (auto& id = std::get<0>(s3_infos.back()); latest_fs()->id() != id) {
            set_latest_fs(get_filesystem(id));
        }
    }
}

void StorageEngine::_vacuum_stale_rowsets_thread_callback() {
    while (!_stop_background_threads_latch.wait_for(
            std::chrono::seconds(config::vacuum_stale_rowsets_interval_seconds))) {
        cloud::tablet_mgr()->vacuum_stale_rowsets();
    }
}

void StorageEngine::_check_file_cache_ttl_block_valid() {
    int64_t interval_seconds = config::file_cache_ttl_valid_check_interval_second / 2;
    auto check_ttl = [](std::weak_ptr<Tablet>& tablet_wk) {
        auto tablet = tablet_wk.lock();
        if (!tablet) return;
        if (tablet->tablet_meta()->ttl_seconds() == 0) return;
        auto rowsets = tablet->get_snapshot_rowset();
        std::ranges::for_each(rowsets, [ttl_seconds = tablet->tablet_meta()->ttl_seconds()](
                                               RowsetSharedPtr& rowset) {
            if (rowset->newest_write_timestamp() + ttl_seconds > UnixSeconds()) { // still valid
                std::ranges::for_each(std::ranges::iota_view {0, rowset->num_segments()} |
                                              std::views::transform([&](int32_t seg_id) {
                                                  auto seg_path = rowset->segment_file_path(seg_id);
                                                  return io::BlockFileCache::hash(
                                                          io::Path(seg_path).filename().native());
                                              }),
                                      [](const io::Key& key) {
                                          auto file_cache =
                                                  io::FileCacheFactory::instance().get_by_path(key);
                                          file_cache->update_ttl_atime(key);
                                      });
            }
        });
    };
    while (!_stop_background_threads_latch.wait_for(std::chrono::seconds(interval_seconds))) {
        auto weak_tablets = cloud::tablet_mgr()->get_weak_tablets();
        std::ranges::for_each(weak_tablets, check_ttl);
    }
}

void StorageEngine::_sync_tablets_thread_callback() {
    while (!_stop_background_threads_latch.wait_for(
            std::chrono::seconds(config::schedule_sync_tablets_interval_seconds))) {
        cloud::tablet_mgr()->sync_tablets();
    }
}

void StorageEngine::_lease_compaction_thread_callback() {
    while (!_stop_background_threads_latch.wait_for(
            std::chrono::seconds(config::lease_compaction_interval_seconds))) {
        std::vector<std::shared_ptr<CloudBaseCompaction>> base_compactions;
        std::vector<std::shared_ptr<CloudCumulativeCompaction>> cumu_compactions;
        {
            std::lock_guard lock(_compaction_mtx);
            for (auto& [_, base] : _submitted_base_compactions) {
                if (base) { // `base` might be a nullptr placeholder
                    base_compactions.push_back(base);
                }
            }
            for (auto& [_, cumus] : _submitted_cumu_compactions) {
                for (auto& cumu : cumus) {
                    cumu_compactions.push_back(cumu);
                }
            }
        }
        // TODO(plat1ko): Support batch lease rpc
        for (auto& comp : cumu_compactions) {
            comp->do_lease();
        }
        for (auto& comp : base_compactions) {
            comp->do_lease();
        }
    }
}

void StorageEngine::_fd_cache_clean_callback() {
#ifdef GOOGLE_PROFILER
    ProfilerRegisterThread();
#endif
    int32_t interval = 600;
    while (!_stop_background_threads_latch.wait_for(std::chrono::seconds(interval))) {
        interval = config::cache_clean_interval;
        if (interval <= 0) {
            LOG(WARNING) << "config of file descriptor clean interval is illegal: [" << interval
                         << "], force set to 3600 ";
            interval = 3600;
        }

        _start_clean_cache();
    }
}

void StorageEngine::_check_bucket_enable_versioning_callback() {
#ifdef GOOGLE_PROFILER
    ProfilerRegisterThread();
#endif

    int32_t interval = config::refresh_s3_info_interval_seconds;
    while (!_stop_background_threads_latch.wait_for(std::chrono::seconds(interval))) {
        if (latest_fs() == nullptr) {
            LOG(WARNING) << "s3 fs is not ready";
            continue;
        }
        auto s3_fs = std::reinterpret_pointer_cast<io::S3FileSystem>(latest_fs());
        Status s = s3_fs->check_bucket_versioning();
        if (!s.ok()) {
            LOG(WARNING) << "failed to check bucket versioning. " << s;
        }
        interval = config::check_enable_versioning_interval_seconds;
    }
}

void StorageEngine::_garbage_sweeper_thread_callback() {
#ifdef GOOGLE_PROFILER
    ProfilerRegisterThread();
#endif
    uint32_t max_interval = config::max_garbage_sweep_interval;
    uint32_t min_interval = config::min_garbage_sweep_interval;

    if (!(max_interval >= min_interval && min_interval > 0)) {
        LOG(WARNING) << "garbage sweep interval config is illegal: [max=" << max_interval
                     << " min=" << min_interval << "].";
        min_interval = 1;
        max_interval = max_interval >= min_interval ? max_interval : min_interval;
        LOG(INFO) << "force reset garbage sweep interval. "
                  << "max_interval=" << max_interval << ", min_interval=" << min_interval;
    }

    const double pi = M_PI;
    double usage = 1.0;
    // After the program starts, the first round of cleaning starts after min_interval.
    uint32_t curr_interval = min_interval;
    while (!_stop_background_threads_latch.wait_for(std::chrono::seconds(curr_interval))) {
        // Function properties:
        // when usage < 0.6,          ratio close to 1.(interval close to max_interval)
        // when usage at [0.6, 0.75], ratio is rapidly decreasing from 0.87 to 0.27.
        // when usage > 0.75,         ratio is slowly decreasing.
        // when usage > 0.8,          ratio close to min_interval.
        // when usage = 0.88,         ratio is approximately 0.0057.
        double ratio = (1.1 * (pi / 2 - std::atan(usage * 100 / 5 - 14)) - 0.28) / pi;
        ratio = ratio > 0 ? ratio : 0;
        uint32_t curr_interval = max_interval * ratio;
        curr_interval = std::max(curr_interval, min_interval);
        curr_interval = std::min(curr_interval, max_interval);

        // start clean trash and update usage.
        Status res = start_trash_sweep(&usage);
        if (!res.ok()) {
            LOG(WARNING) << "one or more errors occur when sweep trash."
                         << "see previous message for detail. err code=" << res;
            // do nothing. continue next loop.
        }
    }
}

void StorageEngine::check_cumulative_compaction_config() {
    int64_t promotion_size = config::compaction_promotion_size_mbytes;
    int64_t promotion_min_size = config::compaction_promotion_min_size_mbytes;
    int64_t compaction_min_size = config::compaction_min_size_mbytes;

    // check size_based_promotion_size must be greater than size_based_promotion_min_size and 2 * size_based_compaction_lower_bound_size
    int64_t should_min_promotion_size = std::max(promotion_min_size, 2 * compaction_min_size);

    if (promotion_size < should_min_promotion_size) {
        promotion_size = should_min_promotion_size;
        LOG(WARNING) << "the config promotion_size is adjusted to "
                        "promotion_min_size or  2 * "
                        "compaction_min_size "
                     << should_min_promotion_size << ", because size_based_promotion_size is small";
    }
}

void StorageEngine::_unused_rowset_monitor_thread_callback() {
#ifdef GOOGLE_PROFILER
    ProfilerRegisterThread();
#endif
    int32_t interval = config::unused_rowset_monitor_interval;
    do {
        start_delete_unused_rowset();

        interval = config::unused_rowset_monitor_interval;
        if (interval <= 0) {
            LOG(WARNING) << "unused_rowset_monitor_interval config is illegal: " << interval
                         << ", force set to 1";
            interval = 1;
        }
    } while (!_stop_background_threads_latch.wait_for(std::chrono::seconds(interval)));
}

void StorageEngine::_tablet_checkpoint_callback(const std::vector<DataDir*>& data_dirs) {
    int64_t interval = config::generate_tablet_meta_checkpoint_tasks_interval_secs;
    do {
        LOG(INFO) << "begin to produce tablet meta checkpoint tasks.";
        for (auto data_dir : data_dirs) {
            auto st = _tablet_meta_checkpoint_thread_pool->submit_func(
                    [data_dir, this]() { _tablet_manager->do_tablet_meta_checkpoint(data_dir); });
            if (!st.ok()) {
                LOG(WARNING) << "submit tablet checkpoint tasks failed.";
            }
        }
        interval = config::generate_tablet_meta_checkpoint_tasks_interval_secs;
    } while (!_stop_background_threads_latch.wait_for(std::chrono::seconds(interval)));
}

void StorageEngine::_adjust_compaction_thread_num() {
    int base_thread_num = get_base_thread_num();
    if (_base_compaction_thread_pool->max_threads() != base_thread_num) {
        int old_max_threads = _base_compaction_thread_pool->max_threads();
        Status status = _base_compaction_thread_pool->set_max_threads(base_thread_num);
        if (status.ok()) {
            VLOG_NOTICE << "update base compaction thread pool max_threads from " << old_max_threads
                        << " to " << base_thread_num;
        }
    }
    if (_base_compaction_thread_pool->min_threads() != base_thread_num) {
        int old_min_threads = _base_compaction_thread_pool->min_threads();
        Status status = _base_compaction_thread_pool->set_min_threads(base_thread_num);
        if (status.ok()) {
            VLOG_NOTICE << "update base compaction thread pool min_threads from " << old_min_threads
                        << " to " << base_thread_num;
        }
    }

    int cumu_thread_num = get_cumu_thread_num();
    if (_cumu_compaction_thread_pool->max_threads() != cumu_thread_num) {
        int old_max_threads = _cumu_compaction_thread_pool->max_threads();
        Status status = _cumu_compaction_thread_pool->set_max_threads(cumu_thread_num);
        if (status.ok()) {
            VLOG_NOTICE << "update cumu compaction thread pool max_threads from " << old_max_threads
                        << " to " << cumu_thread_num;
        }
    }
    if (_cumu_compaction_thread_pool->min_threads() != cumu_thread_num) {
        int old_min_threads = _cumu_compaction_thread_pool->min_threads();
        Status status = _cumu_compaction_thread_pool->set_min_threads(cumu_thread_num);
        if (status.ok()) {
            VLOG_NOTICE << "update cumu compaction thread pool min_threads from " << old_min_threads
                        << " to " << cumu_thread_num;
        }
    }
}

void StorageEngine::_compaction_tasks_producer_callback() {
#ifdef GOOGLE_PROFILER
    ProfilerRegisterThread();
#endif
    LOG(INFO) << "try to start compaction producer process!";

    int round = 0;
    CompactionType compaction_type;

    // Used to record the time when the score metric was last updated.
    // The update of the score metric is accompanied by the logic of selecting the tablet.
    // If there is no slot available, the logic of selecting the tablet will be terminated,
    // which causes the score metric update to be terminated.
    // In order to avoid this situation, we need to update the score regularly.
    int64_t last_cumulative_score_update_time = 0;
    int64_t last_base_score_update_time = 0;
    static const int64_t check_score_interval_ms = 5000; // 5 secs

    int64_t interval = config::generate_compaction_tasks_interval_ms;
    do {
        if (!config::disable_auto_compaction) {
            _adjust_compaction_thread_num();

            bool check_score = false;
            int64_t cur_time = UnixMillis();
            if (round < config::cumulative_compaction_rounds_for_each_base_compaction_round) {
                compaction_type = CompactionType::CUMULATIVE_COMPACTION;
                round++;
                if (cur_time - last_cumulative_score_update_time >= check_score_interval_ms) {
                    check_score = true;
                    last_cumulative_score_update_time = cur_time;
                }
            } else {
                compaction_type = CompactionType::BASE_COMPACTION;
                round = 0;
                if (cur_time - last_base_score_update_time >= check_score_interval_ms) {
                    check_score = true;
                    last_base_score_update_time = cur_time;
                }
            }
            std::unique_ptr<ThreadPool>& thread_pool =
                    (compaction_type == CompactionType::CUMULATIVE_COMPACTION)
                            ? _cumu_compaction_thread_pool
                            : _base_compaction_thread_pool;
            VLOG_CRITICAL << "compaction thread pool. type: "
                          << (compaction_type == CompactionType::CUMULATIVE_COMPACTION ? "CUMU"
                                                                                       : "BASE")
                          << ", num_threads: " << thread_pool->num_threads()
                          << ", num_threads_pending_start: "
                          << thread_pool->num_threads_pending_start()
                          << ", num_active_threads: " << thread_pool->num_active_threads()
                          << ", max_threads: " << thread_pool->max_threads()
                          << ", min_threads: " << thread_pool->min_threads()
                          << ", num_total_queued_tasks: " << thread_pool->get_queue_size();
            std::vector<TabletSharedPtr> tablets_compaction =
                    _generate_cloud_compaction_tasks(compaction_type, check_score);

            /// Regardless of whether the tablet is submitted for compaction or not,
            /// we need to call 'reset_compaction' to clean up the base_compaction or cumulative_compaction objects
            /// in the tablet, because these two objects store the tablet's own shared_ptr.
            /// If it is not cleaned up, the reference count of the tablet will always be greater than 1,
            /// thus cannot be collected by the garbage collector. (TabletManager::start_trash_sweep)
            for (const auto& tablet : tablets_compaction) {
                Status st = submit_compaction_task(tablet, compaction_type);
                if (st.ok()) continue;
                if ((!st.is<BE_NO_SUITABLE_VERSION>() &&
                     !st.is<CUMULATIVE_NO_SUITABLE_VERSION>()) ||
                    VLOG_DEBUG_IS_ON) {
                    LOG(WARNING) << "failed to submit compaction task for tablet: "
                                 << tablet->tablet_id() << ", err: " << st;
                }
            }
            interval = config::generate_compaction_tasks_interval_ms;
        } else {
            interval = config::check_auto_compaction_interval_seconds * 1000;
        }
    } while (!_stop_background_threads_latch.wait_for(std::chrono::milliseconds(interval)));
}

std::vector<TabletSharedPtr> StorageEngine::_generate_cloud_compaction_tasks(
        CompactionType compaction_type, bool check_score) {
    std::vector<std::shared_ptr<Tablet>> tablets_compaction;

    int64_t max_compaction_score = 0;
    std::unordered_set<int64_t> tablet_preparing_cumu_compaction;
    std::unordered_map<int64_t, std::vector<std::shared_ptr<CloudCumulativeCompaction>>>
            submitted_cumu_compactions;
    std::unordered_map<int64_t, std::shared_ptr<CloudBaseCompaction>> submitted_base_compactions;
    std::unordered_map<int64_t, std::shared_ptr<CloudFullCompaction>> submitted_full_compactions;
    {
        std::lock_guard lock(_compaction_mtx);
        tablet_preparing_cumu_compaction = _tablet_preparing_cumu_compaction;
        submitted_cumu_compactions = _submitted_cumu_compactions;
        submitted_base_compactions = _submitted_base_compactions;
        submitted_full_compactions = _submitted_full_compactions;
    }

    bool need_pick_tablet = true;
    int thread_per_disk =
            config::compaction_task_num_per_fast_disk; // all disks are fast in cloud mode
    int num_cumu =
            std::accumulate(submitted_cumu_compactions.begin(), submitted_cumu_compactions.end(), 0,
                            [](int a, auto& b) { return a + b.second.size(); });
    int num_base = submitted_base_compactions.size() + submitted_full_compactions.size();
    int n = thread_per_disk - num_cumu - num_base;
    if (compaction_type == CompactionType::BASE_COMPACTION) {
        // We need to reserve at least one thread for cumulative compaction,
        // because base compactions may take too long to complete, which may
        // leads to "too many rowsets" error.
        int base_n = std::min(config::max_base_compaction_task_num_per_disk, thread_per_disk - 1) -
                     num_base;
        n = std::min(base_n, n);
    }
    if (n <= 0) { // No threads available
        if (!check_score) return tablets_compaction;
        need_pick_tablet = false;
        n = 0;
    }

    // Return true for skipping compaction
    std::function<bool(Tablet*)> filter_out;
    if (compaction_type == CompactionType::BASE_COMPACTION) {
        filter_out = [&submitted_base_compactions, &submitted_full_compactions](Tablet* t) {
            return !!submitted_base_compactions.count(t->tablet_id()) ||
                   !!submitted_full_compactions.count(t->tablet_id()) ||
                   t->tablet_state() != TABLET_RUNNING;
        };
    } else if (config::enable_parallel_cumu_compaction) {
        filter_out = [&tablet_preparing_cumu_compaction](Tablet* t) {
            return !!tablet_preparing_cumu_compaction.count(t->tablet_id()) ||
                   t->tablet_state() != TABLET_RUNNING;
        };
    } else {
        filter_out = [&tablet_preparing_cumu_compaction, &submitted_cumu_compactions](Tablet* t) {
            return !!tablet_preparing_cumu_compaction.count(t->tablet_id()) ||
                   !!submitted_cumu_compactions.count(t->tablet_id()) ||
                   t->tablet_state() != TABLET_RUNNING;
        };
    }

    // Even if need_pick_tablet is false, we still need to call find_best_tablet_to_compaction(),
    // So that we can update the max_compaction_score metric.
    do {
        std::vector<TabletSharedPtr> tablets;
        auto st = cloud::tablet_mgr()->get_topn_tablets_to_compact(n, compaction_type, filter_out,
                                                                   &tablets, &max_compaction_score);
        if (!st.ok()) {
            LOG(WARNING) << "failed to get tablets to compact, err=" << st;
            break;
        }
        if (!need_pick_tablet) break;
        tablets_compaction = std::move(tablets);
    } while (false);

    if (max_compaction_score > 0) {
        if (compaction_type == CompactionType::BASE_COMPACTION) {
            DorisMetrics::instance()->tablet_base_max_compaction_score->set_value(
                    max_compaction_score);
        } else {
            DorisMetrics::instance()->tablet_cumulative_max_compaction_score->set_value(
                    max_compaction_score);
        }
    }

    return tablets_compaction;
}

bool StorageEngine::has_base_compaction(int64_t tablet_id) const {
    std::lock_guard lock(_compaction_mtx);
    return _submitted_base_compactions.count(tablet_id);
}

bool StorageEngine::has_cumu_compaction(int64_t tablet_id) const {
    std::lock_guard lock(_compaction_mtx);
    return _submitted_cumu_compactions.count(tablet_id);
}

bool StorageEngine::has_full_compaction(int64_t tablet_id) const {
    std::lock_guard lock(_compaction_mtx);
    return _submitted_full_compactions.count(tablet_id);
}

void StorageEngine::get_cumu_compaction(
        int64_t tablet_id, std::vector<std::shared_ptr<CloudCumulativeCompaction>>& res) {
    std::lock_guard lock(_compaction_mtx);
    if (auto it = _submitted_cumu_compactions.find(tablet_id);
        it != _submitted_cumu_compactions.end()) {
        res = it->second;
    }
}

Status StorageEngine::_submit_base_compaction_task(const TabletSharedPtr& tablet) {
    using namespace std::chrono;
    {
        std::lock_guard lock(_compaction_mtx);
        // Take a placeholder for base compaction
        auto [_, success] = _submitted_base_compactions.emplace(tablet->tablet_id(), nullptr);
        if (!success) {
            return Status::AlreadyExist(
                    "other base compaction or full compaction is submitted, tablet_id={}",
                    tablet->tablet_id());
        }
    }
    auto compaction = std::make_shared<CloudBaseCompaction>(tablet);
    auto st = compaction->prepare_compact();
    if (!st.ok()) {
        long now = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
        tablet->set_last_base_compaction_failure_time(now);
        std::lock_guard lock(_compaction_mtx);
        _submitted_base_compactions.erase(tablet->tablet_id());
        return st;
    }
    {
        std::lock_guard lock(_compaction_mtx);
        _submitted_base_compactions[tablet->tablet_id()] = compaction;
    }
    st = _base_compaction_thread_pool->submit_func([=, this, compaction = std::move(compaction)]() {
        auto st = compaction->execute_compact();
        if (!st.ok()) {
            // Error log has been output in `execute_compact`
            long now = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
            tablet->set_last_base_compaction_failure_time(now);
        }
        std::lock_guard lock(_compaction_mtx);
        _submitted_base_compactions.erase(tablet->tablet_id());
    });
    if (!st.ok()) {
        std::lock_guard lock(_compaction_mtx);
        _submitted_base_compactions.erase(tablet->tablet_id());
        return Status::InternalError("failed to submit base compaction, tablet_id={}",
                                     tablet->tablet_id());
    }
    return st;
}

Status StorageEngine::_submit_cumulative_compaction_task(const TabletSharedPtr& tablet) {
    using namespace std::chrono;
    {
        std::lock_guard lock(_compaction_mtx);
        if (!config::enable_parallel_cumu_compaction &&
            _submitted_cumu_compactions.count(tablet->tablet_id())) {
            return Status::AlreadyExist("other cumu compaction is submitted, tablet_id={}",
                                        tablet->tablet_id());
        }
        auto [_, success] = _tablet_preparing_cumu_compaction.insert(tablet->tablet_id());
        if (!success) {
            return Status::AlreadyExist("other cumu compaction is preparing, tablet_id={}",
                                        tablet->tablet_id());
        }
    }
    auto compaction = std::make_shared<CloudCumulativeCompaction>(tablet);
    auto st = compaction->prepare_compact();
    if (!st.ok()) {
        long now = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
        if (st.is<CUMULATIVE_NO_SUITABLE_VERSION>()) {
            // Backoff strategy if no suitable version
            tablet->set_last_cumu_no_suitable_version_ms(now);
        }
        tablet->set_last_cumu_compaction_failure_time(now);
        std::lock_guard lock(_compaction_mtx);
        _tablet_preparing_cumu_compaction.erase(tablet->tablet_id());
        return st;
    }
    {
        std::lock_guard lock(_compaction_mtx);
        _tablet_preparing_cumu_compaction.erase(tablet->tablet_id());
        _submitted_cumu_compactions[tablet->tablet_id()].push_back(compaction);
    }
    auto erase_submitted_cumu_compaction = [=, this]() {
        std::lock_guard lock(_compaction_mtx);
        auto it = _submitted_cumu_compactions.find(tablet->tablet_id());
        DCHECK(it != _submitted_cumu_compactions.end());
        auto& compactions = it->second;
        auto it1 = std::find(compactions.begin(), compactions.end(), compaction);
        DCHECK(it1 != compactions.end());
        compactions.erase(it1);
        if (compactions.empty()) { // No compactions on this tablet, erase key
            _submitted_cumu_compactions.erase(it);
            // No cumu compaction on this tablet, reset `last_cumu_no_suitable_version_ms` to enable this tablet to
            // enter the compaction scheduling candidate set. The purpose of doing this is to have at least one BE perform
            // cumu compaction on tablet which has suitable versions for cumu compaction.
            tablet->set_last_cumu_no_suitable_version_ms(0);
        }
    };
    st = _cumu_compaction_thread_pool->submit_func([=, compaction = std::move(compaction)]() {
        auto st = compaction->execute_compact();
        if (!st.ok()) {
            // Error log has been output in `execute_compact`
            long now = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
            tablet->set_last_cumu_compaction_failure_time(now);
        }
        erase_submitted_cumu_compaction();
    });
    if (!st.ok()) {
        erase_submitted_cumu_compaction();
        return Status::InternalError("failed to submit cumu compaction, tablet_id={}",
                                     tablet->tablet_id());
    }
    return st;
}

Status StorageEngine::_submit_full_compaction_task(const TabletSharedPtr& tablet) {
    using namespace std::chrono;
    {
        std::lock_guard lock(_compaction_mtx);
        // Take a placeholder for full compaction
        auto [_, success] = _submitted_full_compactions.emplace(tablet->tablet_id(), nullptr);
        if (!success) {
            return Status::AlreadyExist(
                    "other full compaction or base compaction is submitted, tablet_id={}",
                    tablet->tablet_id());
        }
    }
    auto compaction = std::make_shared<CloudFullCompaction>(tablet);
    auto st = compaction->prepare_compact();
    if (!st.ok()) {
        long now = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
        tablet->set_last_full_compaction_failure_time(now);
        std::lock_guard lock(_compaction_mtx);
        _submitted_full_compactions.erase(tablet->tablet_id());
        return st;
    }
    {
        std::lock_guard lock(_compaction_mtx);
        _submitted_full_compactions[tablet->tablet_id()] = compaction;
    }
    st = _base_compaction_thread_pool->submit_func([=, this, compaction = std::move(compaction)]() {
        auto st = compaction->execute_compact();
        if (!st.ok()) {
            // Error log has been output in `execute_compact`
            long now = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
            tablet->set_last_full_compaction_failure_time(now);
        }
        std::lock_guard lock(_compaction_mtx);
        _submitted_full_compactions.erase(tablet->tablet_id());
    });
    if (!st.ok()) {
        std::lock_guard lock(_compaction_mtx);
        _submitted_full_compactions.erase(tablet->tablet_id());
        return Status::InternalError("failed to submit full compaction, tablet_id={}",
                                     tablet->tablet_id());
    }
    return st;
}

Status StorageEngine::submit_compaction_task(const TabletSharedPtr& tablet,
                                             CompactionType compaction_type) {
    DCHECK(compaction_type == CompactionType::CUMULATIVE_COMPACTION ||
           compaction_type == CompactionType::BASE_COMPACTION ||
           compaction_type == CompactionType::FULL_COMPACTION);
    switch (compaction_type) {
    case CompactionType::BASE_COMPACTION:
        RETURN_IF_ERROR(_submit_base_compaction_task(tablet));
        return Status::OK();
    case CompactionType::CUMULATIVE_COMPACTION:
        RETURN_IF_ERROR(_submit_cumulative_compaction_task(tablet));
        return Status::OK();
    case CompactionType::FULL_COMPACTION:
        RETURN_IF_ERROR(_submit_full_compaction_task(tablet));
        return Status::OK();
    default:
        return Status::InternalError("unknown compaction type!");
    }
}

Status StorageEngine::_handle_seg_compaction(BetaRowsetWriter* writer,
                                             SegCompactionCandidatesSharedPtr segments) {
    return Status::OK();
}

void StorageEngine::_cooldown_tasks_producer_callback() {}

} // namespace doris
