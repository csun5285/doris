#include "cloud/cloud_cumulative_compaction.h"

#include "cloud/meta_mgr.h"
#include "cloud/olap/storage_engine.h"
#include "cloud/utils.h"
#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "common/sync_point.h"
#include "gen_cpp/selectdb_cloud.pb.h"
#include "olap/cumulative_compaction_policy.h"
#include "util/trace.h"
#include "util/uuid_generator.h"
#include "service/backend_options.h"

namespace doris {
using namespace ErrorCode;

bvar::Adder<uint64_t> cumu_output_size("cumu_compaction", "output_size");

CloudCumulativeCompaction::CloudCumulativeCompaction(TabletSharedPtr tablet)
        : CumulativeCompaction(std::move(tablet)) {
    auto uuid = UUIDGenerator::instance()->next_uuid();
    std::stringstream ss;
    ss << uuid;
    _uuid = ss.str();
}

CloudCumulativeCompaction::~CloudCumulativeCompaction() = default;

Status CloudCumulativeCompaction::prepare_compact() {
    if (_tablet->tablet_state() != TABLET_RUNNING) {
        return Status::InternalError("invalid tablet state. tablet_id={}", _tablet->tablet_id());
    }

    std::vector<std::shared_ptr<CloudCumulativeCompaction>> cumu_compactions;
    StorageEngine::instance()->get_cumu_compaction(_tablet->tablet_id(), cumu_compactions);
    if (!cumu_compactions.empty()) {
        for (auto& cumu : cumu_compactions) {
            _max_conflict_version =
                    std::max(_max_conflict_version, cumu->_input_rowsets.back()->end_version());
        }
    }

    int tried = 0;
TRY_AGAIN:

    bool need_sync_tablet = true;
    {
        std::shared_lock rlock(_tablet->get_header_lock());
        // If number of rowsets is equal to approximate_num_rowsets, it is very likely that this tablet has been
        // synchronized with meta-service.
        if (_tablet->tablet_meta()->all_rs_metas().size() >=
                    _tablet->fetch_add_approximate_num_rowsets(0) &&
            _tablet->last_sync_time() > 0) {
            need_sync_tablet = false;
        }
    }
    if (need_sync_tablet) {
        RETURN_IF_ERROR(_tablet->cloud_sync_rowsets());
    }

    // pick rowsets to compact
    auto st = pick_rowsets_to_compact();
    if (!st.ok()) {
        if (tried == 0 && _last_delete_version.first != -1) {
            // we meet a delete version, should increase the cumulative point to let base compaction handle the delete version.
            // plus 1 to skip the delete version.
            // NOTICE: after that, the cumulative point may be larger than max version of this tablet, but it doesn't matter.
            update_cumulative_point();
        }
        return st;
    }

    // prepare compaction job
    selectdb::TabletJobInfoPB job;
    auto idx = job.mutable_idx();
    idx->set_tablet_id(_tablet->tablet_id());
    idx->set_table_id(_tablet->table_id());
    idx->set_index_id(_tablet->index_id());
    idx->set_partition_id(_tablet->partition_id());
    auto compaction_job = job.add_compaction();
    compaction_job->set_id(_uuid);
    compaction_job->set_initiator(BackendOptions::get_localhost() + ':' +
                                  std::to_string(config::heartbeat_service_port));
    compaction_job->set_type(selectdb::TabletCompactionJobPB::CUMULATIVE);
    compaction_job->set_base_compaction_cnt(_base_compaction_cnt);
    compaction_job->set_cumulative_compaction_cnt(_cumulative_compaction_cnt);
    using namespace std::chrono;
    int64_t now = duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
    _expiration = now + config::compaction_timeout_seconds;
    compaction_job->set_expiration(_expiration);
    compaction_job->set_lease(now + config::lease_compaction_interval_seconds * 4);
    if (config::enable_parallel_cumu_compaction) {
        // Set input version range to let meta-service judge version range conflict
        compaction_job->add_input_versions(_input_rowsets.front()->start_version());
        compaction_job->add_input_versions(_input_rowsets.back()->end_version());
    }
    selectdb::StartTabletJobResponse resp;
    st = cloud::meta_mgr()->prepare_tablet_job(job, &resp);
    if (!st.ok()) {
        if (resp.status().code() == selectdb::STALE_TABLET_CACHE) {
            // set last_sync_time to 0 to force sync tablet next time
            _tablet->set_last_sync_time(0);
        } else if (resp.status().code() == selectdb::TABLET_NOT_FOUND) {
            // tablet not found
            _tablet->recycle_resources_by_self();
        } else if (resp.status().code() == selectdb::JOB_TABLET_BUSY) {
            if (config::enable_parallel_cumu_compaction && resp.version_in_compaction_size() > 0 &&
                ++tried <= 2) {
                _max_conflict_version = *std::max_element(resp.version_in_compaction().begin(),
                                                          resp.version_in_compaction().end());
                LOG_INFO("retry pick input rowsets")
                        .tag("job_id", _uuid)
                        .tag("max_conflict_version", _max_conflict_version)
                        .tag("tried", tried)
                        .tag("msg", resp.status().msg());
                goto TRY_AGAIN;
            } else {
                LOG_WARNING("failed to prepare cumu compaction")
                        .tag("job_id", _uuid)
                        .tag("msg", resp.status().msg());
                return Status::Error<CUMULATIVE_NO_SUITABLE_VERSION>("no suitable versions");
            }
        }
        return st;
    }

    for (auto& rs : _input_rowsets) {
        _input_row_num += rs->num_rows();
        _input_segments += rs->num_segments();
        _input_rowsets_size += rs->data_disk_size();
    }
    LOG_INFO("start CloudCumulativeCompaction, tablet_id={}, range=[{}-{}]", _tablet->tablet_id(),
             _input_rowsets.front()->start_version(), _input_rowsets.back()->end_version())
            .tag("job_id", _uuid)
            .tag("input_rowsets", _input_rowsets.size())
            .tag("input_rows", _input_row_num)
            .tag("input_segments", _input_segments)
            .tag("input_data_size", _input_rowsets_size)
            .tag("tablet_max_version", _tablet->local_max_version())
            .tag("cumulative_point", _tablet->cumulative_layer_point())
            .tag("num_rowsets", _tablet->fetch_add_approximate_num_rowsets(0))
            .tag("cumu_num_rowsets", _tablet->fetch_add_approximate_cumu_num_rowsets(0));
    return st;
}

Status CloudCumulativeCompaction::execute_compact_impl() {
    TEST_SYNC_POINT_RETURN_WITH_VALUE("CloudCumulativeCompaction::execute_compact_impl",
                                      Status::OK(), this);
    int64_t permits = get_compaction_permits();
    using namespace std::chrono;
    auto start = steady_clock::now();
    RETURN_IF_ERROR(do_compaction(permits));
    LOG_INFO("finish CloudCumulativeCompaction, tablet_id={}, cost={}ms", _tablet->tablet_id(),
             duration_cast<milliseconds>(steady_clock::now() - start).count())
            .tag("job_id", _uuid)
            .tag("input_rowsets", _input_rowsets.size())
            .tag("input_rows", _input_row_num)
            .tag("input_segments", _input_segments)
            .tag("input_data_size", _input_rowsets_size)
            .tag("output_rows", _output_rowset->num_rows())
            .tag("output_segments", _output_rowset->num_segments())
            .tag("output_data_size", _output_rowset->data_disk_size())
            .tag("tablet_max_version", _tablet->local_max_version())
            .tag("cumulative_point", _tablet->cumulative_layer_point())
            .tag("num_rowsets", _tablet->fetch_add_approximate_num_rowsets(0))
            .tag("cumu_num_rowsets", _tablet->fetch_add_approximate_cumu_num_rowsets(0));

    _compaction_succeed = true;

    DorisMetrics::instance()->cumulative_compaction_deltas_total->increment(_input_rowsets.size());
    DorisMetrics::instance()->cumulative_compaction_bytes_total->increment(_input_rowsets_size);
    cumu_output_size << _output_rowset->data_disk_size();

    return Status::OK();
}

Status CloudCumulativeCompaction::modify_rowsets(const Merger::Statistics* merger_stats) {
    // calculate new cumulative point
    int64_t input_cumulative_point = _tablet->cumulative_layer_point();
    int64_t new_cumulative_point =
            StorageEngine::instance()->cumu_compaction_policy()->new_cumulative_point(
                    _tablet.get(), _output_rowset, _last_delete_version, input_cumulative_point);
    // commit compaction job
    selectdb::TabletJobInfoPB job;
    auto idx = job.mutable_idx();
    idx->set_tablet_id(_tablet->tablet_id());
    idx->set_table_id(_tablet->table_id());
    idx->set_index_id(_tablet->index_id());
    idx->set_partition_id(_tablet->partition_id());
    auto compaction_job = job.add_compaction();
    compaction_job->set_id(_uuid);
    compaction_job->set_initiator(BackendOptions::get_localhost() + ':' +
                                  std::to_string(config::heartbeat_service_port));
    compaction_job->set_type(selectdb::TabletCompactionJobPB::CUMULATIVE);
    compaction_job->set_input_cumulative_point(input_cumulative_point);
    compaction_job->set_output_cumulative_point(new_cumulative_point);
    compaction_job->set_num_input_rows(_input_row_num);
    compaction_job->set_num_output_rows(_output_rowset->num_rows());
    compaction_job->set_size_input_rowsets(_input_rowsets_size);
    compaction_job->set_size_output_rowsets(_output_rowset->data_disk_size());
    compaction_job->set_num_input_segments(_input_segments);
    compaction_job->set_num_output_segments(_output_rowset->num_segments());
    compaction_job->set_num_input_rowsets(_input_rowsets.size());
    compaction_job->set_num_output_rowsets(1);
    compaction_job->add_input_versions(_input_rowsets.front()->start_version());
    compaction_job->add_input_versions(_input_rowsets.back()->end_version());
    compaction_job->add_output_versions(_output_rowset->end_version());
    compaction_job->add_txn_id(_output_rowset->txn_id());
    compaction_job->add_output_rowset_ids(_output_rowset->rowset_id().to_string());

    DeleteBitmapPtr output_rowset_delete_bitmap = nullptr;
    if (_tablet->keys_type() == KeysType::UNIQUE_KEYS &&
        _tablet->enable_unique_key_merge_on_write()) {
        int64_t missed_rows = merger_stats ? merger_stats->merged_rows : -1;
        int64_t initiator = boost::uuids::hash_value(UUIDGenerator::instance()->next_uuid()) &
                            std::numeric_limits<int64_t>::max();
        RETURN_IF_ERROR(_tablet->cloud_calc_delete_bitmap_for_compaciton(
                _input_rowsets, _output_rowset, _rowid_conversion, compaction_type(), missed_rows,
                initiator, output_rowset_delete_bitmap));
        compaction_job->set_delete_bitmap_lock_initiator(initiator);
    }

    selectdb::FinishTabletJobResponse resp;
    auto st = cloud::meta_mgr()->commit_tablet_job(job, &resp);
    if (!st.ok()) {
        if (resp.status().code() == selectdb::TABLET_NOT_FOUND) {
            _tablet->recycle_resources_by_self();
        }
        return st;
    }
    auto& stats = resp.stats();
    LOG(INFO) << "tablet stats=" << stats.ShortDebugString();
    {
        std::lock_guard wrlock(_tablet->get_header_lock());
        // clang-format off
        _tablet->set_last_base_compaction_success_time(std::max(_tablet->last_base_compaction_success_time(), stats.last_base_compaction_time_ms()));
        _tablet->set_last_cumu_compaction_success_time(std::max(_tablet->last_cumu_compaction_success_time(), stats.last_cumu_compaction_time_ms()));
        // clang-format on
        if (_tablet->cumulative_compaction_cnt() >= stats.cumulative_compaction_cnt()) {
            // This could happen while calling `sync_tablet_rowsets` during `commit_tablet_job`, or parallel cumu compactions which are
            // committed later increase tablet.cumulative_compaction_cnt (see CloudCompactionTest.parallel_cumu_compaction)
            return Status::OK();
        }
        // Try to make output rowset visible immediately in tablet cache, instead of waiting for next synchronization from meta-service.
        if (stats.cumulative_point() > _tablet->cumulative_layer_point() &&
            stats.cumulative_compaction_cnt() != _tablet->cumulative_compaction_cnt() + 1) {
            // This could happen when there are multiple parallel cumu compaction committed, tablet cache lags several
            // cumu compactions behind meta-service (stats.cumulative_compaction_cnt > tablet.cumulative_compaction_cnt + 1).
            // If `cumu_point` of the tablet cache also falls behind, MUST ONLY synchronize tablet cache from meta-service,
            // otherwise may cause the tablet to be unable to synchronize the rowset meta changes generated by other cumu compaction.
            return Status::OK();
        }
        if (_input_rowsets.size() == 1) {
            DCHECK_EQ(_output_rowset->version(), _input_rowsets[0]->version());
            // MUST NOT move input rowset to stale path
            _tablet->cloud_add_rowsets({_output_rowset}, true);
        } else {
            _tablet->cloud_delete_rowsets(_input_rowsets);
            _tablet->cloud_add_rowsets({_output_rowset}, false);
        }
        // ATTN: MUST NOT update `base_compaction_cnt` which are used when sync rowsets, otherwise may cause
        // the tablet to be unable to synchronize the rowset meta changes generated by base compaction.
        _tablet->set_cumulative_compaction_cnt(_tablet->cumulative_compaction_cnt() + 1);
        _tablet->set_cumulative_layer_point(stats.cumulative_point());
        if (output_rowset_delete_bitmap) {
            _tablet->tablet_meta()->delete_bitmap().merge(*output_rowset_delete_bitmap);
        }
        if (stats.base_compaction_cnt() >= _tablet->base_compaction_cnt()) {
            _tablet->reset_approximate_stats(stats.num_rowsets(), stats.num_segments(),
                                             stats.num_rows(), stats.data_size());
        }
    }
    return Status::OK();
}

void CloudCumulativeCompaction::garbage_collection() {
    file_cache_garbage_collection();
    selectdb::TabletJobInfoPB job;
    auto idx = job.mutable_idx();
    idx->set_tablet_id(_tablet->tablet_id());
    idx->set_table_id(_tablet->table_id());
    idx->set_index_id(_tablet->index_id());
    idx->set_partition_id(_tablet->partition_id());
    auto compaction_job = job.add_compaction();
    compaction_job->set_id(_uuid);
    compaction_job->set_initiator(BackendOptions::get_localhost() + ':' +
                                  std::to_string(config::heartbeat_service_port));
    compaction_job->set_type(selectdb::TabletCompactionJobPB::CUMULATIVE);
    auto st = cloud::meta_mgr()->abort_tablet_job(job);
    if (!st.ok()) {
        LOG_WARNING("failed to abort compaction job")
                .tag("job_id", _uuid)
                .tag("tablet_id", _tablet->tablet_id())
                .error(st);
    }
}

Status CloudCumulativeCompaction::pick_rowsets_to_compact() {
    _input_rowsets.clear();

    std::vector<RowsetSharedPtr> candidate_rowsets;
    {
        std::shared_lock rlock(_tablet->get_header_lock());
        _base_compaction_cnt = _tablet->base_compaction_cnt();
        _cumulative_compaction_cnt = _tablet->cumulative_compaction_cnt();
        int64_t candidate_version =
                std::max(_tablet->cumulative_layer_point(), _max_conflict_version + 1);
        // Get all rowsets whose version >= `candidate_version` as candidate rowsets
        _tablet->traverse_rowsets(
                [&candidate_rowsets, candidate_version](const RowsetSharedPtr& rs) {
                    if (rs->start_version() >= candidate_version) {
                        candidate_rowsets.push_back(rs);
                    }
                });
    }
    if (candidate_rowsets.empty()) {
        return Status::Error<CUMULATIVE_NO_SUITABLE_VERSION>("no suitable versions");
    }
    std::sort(candidate_rowsets.begin(), candidate_rowsets.end(), Rowset::comparator);
    if (auto st = check_version_continuity(candidate_rowsets); !st.ok()) {
        DCHECK(false) << st;
        return st;
    }

    size_t compaction_score = 0;
    StorageEngine::instance()->cumu_compaction_policy()->pick_input_rowsets(
            _tablet.get(), candidate_rowsets,
            config::cumulative_compaction_max_deltas,
            config::cumulative_compaction_min_deltas, &_input_rowsets,
            &_last_delete_version, &compaction_score);

    if (_input_rowsets.empty()) {
        return Status::Error<CUMULATIVE_NO_SUITABLE_VERSION>("no suitable versions");
    } else if (_input_rowsets.size() == 1 &&
               !_input_rowsets.front()->rowset_meta()->is_segments_overlapping()) {
        VLOG_DEBUG << "there is only one rowset and not overlapping. tablet_id="
                   << _tablet->tablet_id() << ", version=" << _input_rowsets.front()->version();
        return Status::Error<CUMULATIVE_NO_SUITABLE_VERSION>("no suitable versions");
    }
    return Status::OK();
}

void CloudCumulativeCompaction::update_cumulative_point() {
    selectdb::TabletJobInfoPB job;
    auto idx = job.mutable_idx();
    idx->set_tablet_id(_tablet->tablet_id());
    idx->set_table_id(_tablet->table_id());
    idx->set_index_id(_tablet->index_id());
    idx->set_partition_id(_tablet->partition_id());
    auto compaction_job = job.add_compaction();
    compaction_job->set_id(_uuid);
    compaction_job->set_initiator(BackendOptions::get_localhost() + ':' +
                                  std::to_string(config::heartbeat_service_port));
    compaction_job->set_type(selectdb::TabletCompactionJobPB::EMPTY_CUMULATIVE);
    compaction_job->set_base_compaction_cnt(_base_compaction_cnt);
    compaction_job->set_cumulative_compaction_cnt(_cumulative_compaction_cnt);
    int64_t now = time(nullptr);
    compaction_job->set_lease(now + config::lease_compaction_interval_seconds);
    // No need to set expiration time, since there is no output rowset
    selectdb::StartTabletJobResponse start_resp;
    auto st = cloud::meta_mgr()->prepare_tablet_job(job, &start_resp);
    if (!st.ok()) {
        if (start_resp.status().code() == selectdb::STALE_TABLET_CACHE) {
            // set last_sync_time to 0 to force sync tablet next time
            _tablet->set_last_sync_time(0);
        } else if (start_resp.status().code() == selectdb::TABLET_NOT_FOUND) {
            // tablet not found
            _tablet->recycle_resources_by_self();
        }
        LOG_WARNING("failed to update cumulative point to meta srv")
                .tag("job_id", _uuid)
                .tag("tablet_id", _tablet->tablet_id())
                .error(st);
        return;
    }
    int64_t input_cumulative_point = _tablet->cumulative_layer_point();
    int64_t output_cumulative_point = _last_delete_version.first + 1;
    compaction_job->set_input_cumulative_point(input_cumulative_point);
    compaction_job->set_output_cumulative_point(output_cumulative_point);
    selectdb::FinishTabletJobResponse finish_resp;
    st = cloud::meta_mgr()->commit_tablet_job(job, &finish_resp);
    if (!st.ok()) {
        if (finish_resp.status().code() == selectdb::TABLET_NOT_FOUND) {
            _tablet->recycle_resources_by_self();
        }
        LOG_WARNING("failed to update cumulative point to meta srv")
                .tag("job_id", _uuid)
                .tag("tablet_id", _tablet->tablet_id())
                .error(st);
        return;
    }
    LOG_INFO("do empty cumulative compaction to update cumulative point")
            .tag("job_id", _uuid)
            .tag("tablet_id", _tablet->tablet_id())
            .tag("input_cumulative_point", input_cumulative_point)
            .tag("output_cumulative_point", output_cumulative_point);
    auto& stats = finish_resp.stats();
    LOG(INFO) << "tablet stats=" << stats.ShortDebugString();
    {
        std::lock_guard wrlock(_tablet->get_header_lock());
        // clang-format off
        _tablet->set_last_base_compaction_success_time(std::max(_tablet->last_base_compaction_success_time(), stats.last_base_compaction_time_ms()));
        _tablet->set_last_cumu_compaction_success_time(std::max(_tablet->last_cumu_compaction_success_time(), stats.last_cumu_compaction_time_ms()));
        // clang-format on
        if (_tablet->cumulative_compaction_cnt() >= stats.cumulative_compaction_cnt()) {
            // This could happen while calling `sync_tablet_rowsets` during `commit_tablet_job`
            return;
        }
        // ATTN: MUST NOT update `base_compaction_cnt` which are used when sync rowsets, otherwise may cause
        // the tablet to be unable to synchronize the rowset meta changes generated by base compaction.
        _tablet->set_cumulative_compaction_cnt(_tablet->cumulative_compaction_cnt() + 1);
        _tablet->set_cumulative_layer_point(stats.cumulative_point());
        if (stats.base_compaction_cnt() >= _tablet->base_compaction_cnt()) {
            _tablet->reset_approximate_stats(stats.num_rowsets(), stats.num_segments(),
                                             stats.num_rows(), stats.data_size());
        }
    }
}

void CloudCumulativeCompaction::do_lease() {
    TEST_INJECTION_POINT_RETURN_WITH_VOID("CloudCumulativeCompaction::do_lease");
    if (_compaction_succeed) {
        return ;
    }
    selectdb::TabletJobInfoPB job;
    auto idx = job.mutable_idx();
    idx->set_tablet_id(_tablet->tablet_id());
    idx->set_table_id(_tablet->table_id());
    idx->set_index_id(_tablet->index_id());
    idx->set_partition_id(_tablet->partition_id());
    auto compaction_job = job.add_compaction();
    compaction_job->set_id(_uuid);
    using namespace std::chrono;
    int64_t lease_time = duration_cast<seconds>(system_clock::now().time_since_epoch()).count() +
                         config::lease_compaction_interval_seconds * 4;
    compaction_job->set_lease(lease_time);
    auto st = cloud::meta_mgr()->lease_tablet_job(job);
    if (!st.ok()) {
        LOG_WARNING("failed to lease compaction job")
                .tag("job_id", _uuid)
                .tag("tablet_id", _tablet->tablet_id())
                .error(st);
    }
}

} // namespace doris
