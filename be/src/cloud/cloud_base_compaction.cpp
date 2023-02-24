#include "cloud/cloud_base_compaction.h"

#include "cloud/utils.h"
#include "common/config.h"
#include "gen_cpp/selectdb_cloud.pb.h"
#include "util/defer_op.h"
#include "util/trace.h"
#include "util/uuid_generator.h"

namespace doris {

static std::mutex s_base_compaction_mtx;
static std::vector<std::shared_ptr<CloudBaseCompaction>> s_base_compactions;

std::vector<std::shared_ptr<CloudBaseCompaction>> get_base_compactions() {
    std::lock_guard lock(s_base_compaction_mtx);
    return s_base_compactions;
}
void push_base_compaction(std::shared_ptr<CloudBaseCompaction> compaction) {
    std::lock_guard lock(s_base_compaction_mtx);
    s_base_compactions.push_back(std::move(compaction));
}
void pop_base_compaction(CloudBaseCompaction* compaction) {
    std::lock_guard lock(s_base_compaction_mtx);
    auto it = std::find_if(s_base_compactions.begin(), s_base_compactions.end(),
                           [compaction](auto& x) { return x.get() == compaction; });
    s_base_compactions.erase(it);
}

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
    std::unique_lock lock(_tablet->get_base_compaction_lock(), std::try_to_lock);
    if (!lock.owns_lock()) {
        return Status::OLAPInternalError(OLAP_ERR_BE_TRY_BE_LOCK_ERROR);
    }
    TRACE("got base compaction lock");

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

    // MUST get compaction_cnt before `pick_rowsets_to_compact` to ensure statistic of tablet during `pick_rowsets_to_compact`
    // not lag behind start tablet job request.
    int64_t base_compaction_cnt = _tablet->base_compaction_cnt();
    int64_t cumulative_compaction_cnt = _tablet->cumulative_compaction_cnt();

    RETURN_IF_ERROR(pick_rowsets_to_compact());
    TRACE("rowsets picked");

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
    compaction_job->set_base_compaction_cnt(base_compaction_cnt);
    compaction_job->set_cumulative_compaction_cnt(cumulative_compaction_cnt);
    using namespace std::chrono;
    int64_t now = duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
    _expiration = now + config::compaction_timeout_seconds;
    compaction_job->set_expiration(_expiration);
    compaction_job->set_lease(now + config::lease_compaction_interval_seconds * 4);
    auto st = cloud::meta_mgr()->prepare_tablet_job(job);
    if (st.precise_code() == STALE_TABLET_CACHE) {
        // set last_sync_time to 0 to force sync tablet next time
        _tablet->set_last_sync_time(0);
    } else if (st.is_not_found()) {
        // tablet not found
        cloud::tablet_mgr()->erase_tablet(_tablet->tablet_id());
    }
    return st;
}

Status CloudBaseCompaction::execute_compact_impl() {
    if (config::enable_base_compaction_idle_sched) {
        Thread::set_idle_sched();
    }
    std::unique_lock lock(_tablet->get_base_compaction_lock(), std::try_to_lock);
    if (!lock.owns_lock()) {
        return Status::OLAPInternalError(OLAP_ERR_BE_TRY_BE_LOCK_ERROR);
    }
    TRACE("got base compaction lock");

    push_base_compaction(std::static_pointer_cast<CloudBaseCompaction>(shared_from_this()));
    Defer defer {[&] { pop_base_compaction(this); }};

    SCOPED_ATTACH_TASK(_mem_tracker);

    int64_t permits = get_compaction_permits();
    using namespace std::chrono;
    auto start = steady_clock::now();
    RETURN_NOT_OK(do_compaction(permits));
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
    TRACE("compaction finished");

    _state = CompactionState::SUCCESS;

    DorisMetrics::instance()->base_compaction_deltas_total->increment(_input_rowsets.size());
    DorisMetrics::instance()->base_compaction_bytes_total->increment(_input_rowsets_size);
    TRACE("save base compaction metrics");

    return Status::OK();
}

Status CloudBaseCompaction::update_tablet_meta() {
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

    int64_t base_compaction_cnt = _tablet->base_compaction_cnt();
    selectdb::TabletStatsPB stats;
    RETURN_IF_ERROR(cloud::meta_mgr()->commit_tablet_job(job, &stats));
    LOG(INFO) << "tablet stats=" << stats.ShortDebugString();

    {
        std::lock_guard wrlock(_tablet->get_header_lock());
        if (_tablet->base_compaction_cnt() > base_compaction_cnt) {
            // This could happen while calling `sync_tablet_rowsets` during `commit_tablet_job`
            return Status::OK();
        }
        _tablet->cloud_delete_rowsets(_input_rowsets);
        _tablet->cloud_add_rowsets({_output_rowset}, false);
        _tablet->set_base_compaction_cnt(stats.base_compaction_cnt());
        _tablet->set_cumulative_compaction_cnt(stats.cumulative_compaction_cnt());
        _tablet->set_cumulative_layer_point(stats.cumulative_point());
        _tablet->reset_approximate_stats(stats.num_rowsets(), stats.num_segments(),
                                         stats.num_rows(), stats.data_size());
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
