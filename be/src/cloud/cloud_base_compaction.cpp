#include "cloud/cloud_base_compaction.h"

#include "cloud/meta_mgr.h"
#include "cloud/utils.h"
#include "common/config.h"
#include "gen_cpp/selectdb_cloud.pb.h"
#include "util/thread.h"
#include "util/uuid_generator.h"

namespace doris {
using namespace ErrorCode;

bvar::Adder<uint64_t> base_output_size("base_compaction", "output_size");

CloudBaseCompaction::CloudBaseCompaction(TabletSharedPtr tablet)
        : BaseCompaction(std::move(tablet)) {
    auto uuid = UUIDGenerator::instance()->next_uuid();
    std::stringstream ss;
    ss << uuid;
    _uuid = ss.str();
}

CloudBaseCompaction::~CloudBaseCompaction() = default;

Status CloudBaseCompaction::prepare_compact() {
    if (_tablet->tablet_state() != TABLET_RUNNING) {
        return Status::InternalError("invalid tablet state. tablet_id={}", _tablet->tablet_id());
    }

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

    RETURN_IF_ERROR(pick_rowsets_to_compact());

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
    compaction_job->set_type(selectdb::TabletCompactionJobPB::BASE);
    compaction_job->set_base_compaction_cnt(_base_compaction_cnt);
    compaction_job->set_cumulative_compaction_cnt(_cumulative_compaction_cnt);
    using namespace std::chrono;
    int64_t now = duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
    _expiration = now + config::compaction_timeout_seconds;
    compaction_job->set_expiration(_expiration);
    compaction_job->set_lease(now + config::lease_compaction_interval_seconds * 4);
    selectdb::StartTabletJobResponse resp;
    auto st = cloud::meta_mgr()->prepare_tablet_job(job, &resp);
    if (!st.ok()) {
        if (resp.status().code() == selectdb::STALE_TABLET_CACHE) {
            // set last_sync_time to 0 to force sync tablet next time
            _tablet->set_last_sync_time(0);
        } else if (resp.status().code() == selectdb::TABLET_NOT_FOUND) {
            // tablet not found
            cloud::tablet_mgr()->erase_tablet(_tablet->tablet_id());
        }
        return st;
    }

    for (auto& rs : _input_rowsets) {
        _input_row_num += rs->num_rows();
        _input_segments += rs->num_segments();
        _input_rowsets_size += rs->data_disk_size();
    }
    LOG_INFO("start CloudBaseCompaction, tablet_id={}, range=[{}-{}]", _tablet->tablet_id(),
             _input_rowsets.front()->start_version(), _input_rowsets.back()->end_version())
            .tag("job_id", _uuid)
            .tag("input_rowsets", _input_rowsets.size())
            .tag("input_rows", _input_row_num)
            .tag("input_segments", _input_segments)
            .tag("input_data_size", _input_rowsets_size);
    return st;
}

Status CloudBaseCompaction::pick_rowsets_to_compact() {
    _input_rowsets.clear();
    {
        std::shared_lock rlock(_tablet->get_header_lock());
        _base_compaction_cnt = _tablet->base_compaction_cnt();
        _cumulative_compaction_cnt = _tablet->cumulative_compaction_cnt();
        _input_rowsets = _tablet->pick_candidate_rowsets_to_base_compaction();
    }
    DCHECK(check_version_continuity(_input_rowsets).ok());
    _filter_input_rowset();
    if (_input_rowsets.size() <= 1) {
        return Status::Error<BE_NO_SUITABLE_VERSION>("insuffient compation input rowset, #rowsets={}",
                                                     _input_rowsets.size());
    }

    if (_input_rowsets.size() == 2 && _input_rowsets[0]->end_version() == 1) {
        // the tablet is with rowset: [0-1], [2-y]
        // and [0-1] has no data. in this situation, no need to do base compaction.
        return Status::Error<BE_NO_SUITABLE_VERSION>("no suitable versions for compaction");
    }

    // 1. cumulative rowset must reach base_compaction_min_rowset_num threshold
    if (_input_rowsets.size() > config::base_compaction_min_rowset_num) {
        VLOG_NOTICE << "satisfy the base compaction policy. tablet=" << _tablet->full_name()
                    << ", num_cumulative_rowsets=" << _input_rowsets.size() - 1
                    << ", base_compaction_num_cumulative_rowsets="
                    << config::base_compaction_min_rowset_num;
        return Status::OK();
    }

    // 2. the ratio between base rowset and all input cumulative rowsets reaches the threshold
    // `_input_rowsets` has been sorted by end version, so we consider `_input_rowsets[0]` is the base rowset.
    int64_t base_size = _input_rowsets.front()->data_disk_size();
    int64_t cumulative_total_size = 0;
    for (auto it = _input_rowsets.begin() + 1; it != _input_rowsets.end(); ++it) {
        cumulative_total_size += (*it)->data_disk_size();
    }

    double base_cumulative_delta_ratio = config::base_compaction_min_data_ratio;
    if (base_size == 0) {
        // base_size == 0 means this may be a base version [0-1], which has no data.
        // set to 1 to void divide by zero
        base_size = 1;
    }
    double cumulative_base_ratio = static_cast<double>(cumulative_total_size) / base_size;

    if (cumulative_base_ratio > base_cumulative_delta_ratio) {
        VLOG_NOTICE << "satisfy the base compaction policy. tablet=" << _tablet->full_name()
                    << ", cumulative_total_size=" << cumulative_total_size
                    << ", base_size=" << base_size
                    << ", cumulative_base_ratio=" << cumulative_base_ratio
                    << ", policy_ratio=" << base_cumulative_delta_ratio;
        return Status::OK();
    }

    // 3. the interval since last base compaction reaches the threshold
    int64_t base_creation_time = _input_rowsets[0]->creation_time();
    int64_t interval_threshold = config::base_compaction_interval_seconds_since_last_operation;
    int64_t interval_since_last_base_compaction = time(nullptr) - base_creation_time;
    if (interval_since_last_base_compaction > interval_threshold) {
        VLOG_NOTICE << "satisfy the base compaction policy. tablet=" << _tablet->full_name()
                    << ", interval_since_last_base_compaction="
                    << interval_since_last_base_compaction
                    << ", interval_threshold=" << interval_threshold;
        return Status::OK();
    }

    VLOG_NOTICE << "don't satisfy the base compaction policy. tablet=" << _tablet->full_name()
                << ", num_cumulative_rowsets=" << _input_rowsets.size() - 1
                << ", cumulative_base_ratio=" << cumulative_base_ratio
                << ", interval_since_last_base_compaction=" << interval_since_last_base_compaction;
    return Status::Error<BE_NO_SUITABLE_VERSION>("no suitable versions for compaction");
}

Status CloudBaseCompaction::execute_compact_impl() {
    if (config::enable_base_compaction_idle_sched) {
        Thread::set_idle_sched();
    }

    SCOPED_ATTACH_TASK(_mem_tracker);

    int64_t permits = get_compaction_permits();
    using namespace std::chrono;
    auto start = steady_clock::now();
    RETURN_IF_ERROR(do_compaction(permits));
    LOG_INFO("finish CloudBaseCompaction, tablet_id={}, cost={}ms", _tablet->tablet_id(),
             duration_cast<milliseconds>(steady_clock::now() - start).count())
            .tag("job_id", _uuid)
            .tag("input_rowsets", _input_rowsets.size())
            .tag("input_rows", _input_row_num)
            .tag("input_segments", _input_segments)
            .tag("input_data_size", _input_rowsets_size)
            .tag("output_rows", _output_rowset->num_rows())
            .tag("output_segments", _output_rowset->num_segments())
            .tag("output_data_size", _output_rowset->data_disk_size());

    _compaction_succeed = true;

    DorisMetrics::instance()->base_compaction_deltas_total->increment(_input_rowsets.size());
    DorisMetrics::instance()->base_compaction_bytes_total->increment(_input_rowsets_size);
    base_output_size << _output_rowset->data_disk_size();

    return Status::OK();
}

Status CloudBaseCompaction::modify_rowsets(const Merger::Statistics* merger_stats) {
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
    compaction_job->set_type(selectdb::TabletCompactionJobPB::BASE);
    compaction_job->set_input_cumulative_point(_tablet->cumulative_layer_point());
    compaction_job->set_output_cumulative_point(_tablet->cumulative_layer_point());
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
            cloud::tablet_mgr()->erase_tablet(_tablet->tablet_id());
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
        if (_tablet->base_compaction_cnt() >= stats.base_compaction_cnt()) {
            // This could happen while calling `sync_tablet_rowsets` during `commit_tablet_job`
            return Status::OK();
        }
        // Try to make output rowset visible immediately in tablet cache, instead of waiting for next synchronization from meta-service.
        _tablet->cloud_delete_rowsets(_input_rowsets);
        _tablet->cloud_add_rowsets({_output_rowset}, false);
        // ATTN: MUST NOT update `cumu_compaction_cnt` or `cumu_point` which are used when sync rowsets, otherwise may cause
        // the tablet to be unable to synchronize the rowset meta changes generated by cumu compaction.
        _tablet->set_base_compaction_cnt(_tablet->base_compaction_cnt() + 1);
        if (output_rowset_delete_bitmap) {
            _tablet->tablet_meta()->delete_bitmap().merge(*output_rowset_delete_bitmap);
        }
        if (stats.cumulative_compaction_cnt() >= _tablet->cumulative_compaction_cnt()) {
            _tablet->reset_approximate_stats(stats.num_rowsets(), stats.num_segments(),
                                             stats.num_rows(), stats.data_size());
        }
    }
    return Status::OK();
}

void CloudBaseCompaction::garbage_collection() {
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
    compaction_job->set_type(selectdb::TabletCompactionJobPB::BASE);
    auto st = cloud::meta_mgr()->abort_tablet_job(job);
    if (!st.ok()) {
        LOG_WARNING("failed to abort compaction job")
                .tag("job_id", _uuid)
                .tag("tablet_id", _tablet->tablet_id())
                .error(st);
    }
}

void CloudBaseCompaction::do_lease() {
    selectdb::TabletJobInfoPB job;
    if (_compaction_succeed) {
        return ;
    }
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
