#include "cloud/cloud_full_compaction.h"

#include <boost/container_hash/hash.hpp>

#include "cloud/meta_mgr.h"
#include "cloud/olap/storage_engine.h"
#include "cloud/utils.h"
#include "common/config.h"
#include "gen_cpp/selectdb_cloud.pb.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/tablet_meta.h"
#include "service/backend_options.h"
#include "util/thread.h"
#include "util/uuid_generator.h"
#include "vec/columns/column.h"

namespace doris {
using namespace ErrorCode;

bvar::Adder<uint64_t> full_output_size("full_compaction", "output_size");

CloudFullCompaction::CloudFullCompaction(TabletSharedPtr tablet)
        : FullCompaction(std::move(tablet)) {
    auto uuid = UUIDGenerator::instance()->next_uuid();
    std::stringstream ss;
    ss << uuid;
    _uuid = ss.str();
}

CloudFullCompaction::~CloudFullCompaction() = default;

Status CloudFullCompaction::prepare_compact() {
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
    compaction_job->set_type(selectdb::TabletCompactionJobPB::FULL);
    compaction_job->set_base_compaction_cnt(_base_compaction_cnt);
    compaction_job->set_cumulative_compaction_cnt(_cumulative_compaction_cnt);
    compaction_job->set_full_compaction_cnt(_full_compaction_cnt);
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
            _tablet->recycle_resources_by_self();
        }
        return st;
    }

    for (auto& rs : _input_rowsets) {
        _input_row_num += rs->num_rows();
        _input_segments += rs->num_segments();
        _input_rowsets_size += rs->data_disk_size();
    }
    LOG_INFO("start CloudFullCompaction, tablet_id={}, range=[{}-{}]", _tablet->tablet_id(),
             _input_rowsets.front()->start_version(), _input_rowsets.back()->end_version())
            .tag("job_id", _uuid)
            .tag("input_rowsets", _input_rowsets.size())
            .tag("input_rows", _input_row_num)
            .tag("input_segments", _input_segments)
            .tag("input_data_size", _input_rowsets_size);
    return st;
}

Status CloudFullCompaction::pick_rowsets_to_compact() {
    _input_rowsets.clear();
    {
        std::shared_lock rlock(_tablet->get_header_lock());
        _input_rowsets = _tablet->pick_candidate_rowsets_to_full_compaction();
    }
    if (auto st = check_version_continuity(_input_rowsets); !st.ok()) {
        DCHECK(false) << st;
        return st;
    }
    if (_input_rowsets.size() <= 1) {
        return Status::Error<BE_NO_SUITABLE_VERSION>(
                "insuffient compation input rowset, #rowsets={}", _input_rowsets.size());
    }

    if (_input_rowsets.size() == 2 && _input_rowsets[0]->end_version() == 1) {
        // the tablet is with rowset: [0-1], [2-y]
        // and [0-1] has no data. in this situation, no need to do full compaction.
        return Status::Error<BE_NO_SUITABLE_VERSION>("no suitable versions for compaction");
    }

    return Status::OK();
}

Status CloudFullCompaction::execute_compact_impl() {
    if (config::enable_base_compaction_idle_sched) {
        Thread::set_idle_sched();
    }

    SCOPED_ATTACH_TASK(_mem_tracker);

    int64_t permits = get_compaction_permits();
    using namespace std::chrono;
    auto start = steady_clock::now();
    RETURN_IF_ERROR(do_compaction(permits));
    LOG_INFO("finish CloudFullCompaction, tablet_id={}, cost={}ms", _tablet->tablet_id(),
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

    DorisMetrics::instance()->full_compaction_deltas_total->increment(_input_rowsets.size());
    DorisMetrics::instance()->full_compaction_bytes_total->increment(_input_rowsets_size);
    full_output_size << _output_rowset->data_disk_size();

    return Status::OK();
}

Status CloudFullCompaction::modify_rowsets(const Merger::Statistics* merger_stats) {
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
    compaction_job->set_type(selectdb::TabletCompactionJobPB::FULL);
    compaction_job->set_input_cumulative_point(_tablet->cumulative_layer_point());
    compaction_job->set_output_cumulative_point(_output_rowset->end_version() + 1);
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
    int64_t initiator =
            boost::hash_range(_uuid.begin(), _uuid.end()) & std::numeric_limits<int64_t>::max();
    RETURN_IF_ERROR(_cloud_full_compaction_update_delete_bitmap(initiator));
    compaction_job->set_delete_bitmap_lock_initiator(initiator);

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
        if (_tablet->full_compaction_cnt() >= stats.full_compaction_cnt()) {
            // This could happen while calling `sync_tablet_rowsets` during `commit_tablet_job`
            return Status::OK();
        }
        // Try to make output rowset visible immediately in tablet cache, instead of waiting for next synchronization from meta-service.
        _tablet->cloud_delete_rowsets(_input_rowsets);
        _tablet->cloud_add_rowsets({_output_rowset}, false);
        // ATTN: MUST NOT update `cumu_compaction_cnt` or `cumu_point` which are used when sync rowsets, otherwise may cause
        // the tablet to be unable to synchronize the rowset meta changes generated by cumu compaction.
        _tablet->set_full_compaction_cnt(_tablet->full_compaction_cnt() + 1);
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

void CloudFullCompaction::garbage_collection() {
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
    compaction_job->set_type(selectdb::TabletCompactionJobPB::FULL);
    if (_tablet->keys_type() == KeysType::UNIQUE_KEYS &&
        _tablet->enable_unique_key_merge_on_write()) {
        int64_t initiator =
                boost::hash_range(_uuid.begin(), _uuid.end()) & std::numeric_limits<int64_t>::max();
        compaction_job->set_delete_bitmap_lock_initiator(initiator);
    }
    auto st = cloud::meta_mgr()->abort_tablet_job(job);
    if (!st.ok()) {
        LOG_WARNING("failed to abort compaction job")
                .tag("job_id", _uuid)
                .tag("tablet_id", _tablet->tablet_id())
                .error(st);
    }
}

void CloudFullCompaction::do_lease() {
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

Status CloudFullCompaction::_cloud_full_compaction_update_delete_bitmap(int64_t initiator) {
    std::vector<RowsetSharedPtr> tmp_rowsets {};
    DeleteBitmapPtr delete_bitmap =
            std::make_shared<DeleteBitmap>(_tablet->tablet_meta()->tablet_id());
    RETURN_IF_ERROR(cloud::meta_mgr()->sync_tablet_rowsets(_tablet.get()));
    int64_t max_version = _tablet->max_version().second;
    DCHECK(max_version >= _output_rowset->version().second);
    if (max_version > _output_rowset->version().second) {
        _tablet->capture_consistent_rowsets({_output_rowset->version().second + 1, max_version},
                                            &tmp_rowsets);
    }
    for (const auto& it : tmp_rowsets) {
        const int64_t& cur_version = it->rowset_meta()->start_version();
        RETURN_IF_ERROR(_cloud_full_compaction_calc_delete_bitmap(it, cur_version, initiator,
                                                                  delete_bitmap));
    }

    RETURN_IF_ERROR(cloud::meta_mgr()->get_delete_bitmap_update_lock(_tablet.get(), -1, initiator));
    RETURN_IF_ERROR(cloud::meta_mgr()->sync_tablet_rowsets(_tablet.get()));
    std::lock_guard rowset_update_lock(_tablet->get_rowset_update_lock());
    std::lock_guard header_lock(_tablet->get_header_lock());
    for (const auto& it : _tablet->rowset_map()) {
        const int64_t& cur_version = it.first.first;
        const RowsetSharedPtr& published_rowset = it.second;
        if (cur_version > max_version) {
            RETURN_IF_ERROR(_cloud_full_compaction_calc_delete_bitmap(published_rowset, cur_version,
                                                                      initiator, delete_bitmap));
        }
    }
    RETURN_IF_ERROR(cloud::meta_mgr()->update_delete_bitmap(_tablet.get(), -1, initiator,
                                                            delete_bitmap.get()));
    _tablet->tablet_meta()->delete_bitmap().merge(*delete_bitmap);
    return Status::OK();
}

Status CloudFullCompaction::_cloud_full_compaction_calc_delete_bitmap(
        const RowsetSharedPtr& published_rowset, const int64_t& cur_version, int64_t initiator,
        const DeleteBitmapPtr& delete_bitmap) {
    std::vector<segment_v2::SegmentSharedPtr> segments;
    auto beta_rowset = reinterpret_cast<BetaRowset*>(published_rowset.get());
    RETURN_IF_ERROR(beta_rowset->load_segments(&segments));
    std::vector<RowsetSharedPtr> specified_rowsets(1, _output_rowset);

    OlapStopWatch watch;
    auto token = StorageEngine::instance()->calc_delete_bitmap_executor()->create_token();
    RETURN_IF_ERROR(_tablet->calc_delete_bitmap(published_rowset, segments, specified_rowsets,
                                                delete_bitmap, cur_version, token.get(),
                                                _output_rs_writer.get()));
    RETURN_IF_ERROR(token->wait());
    size_t total_rows = std::accumulate(
            segments.begin(), segments.end(), 0,
            [](size_t sum, const segment_v2::SegmentSharedPtr& s) { return sum += s->num_rows(); });
    VLOG_DEBUG << "[Full compaction] construct delete bitmap tablet: " << _tablet->tablet_id()
               << ", published rowset version: [" << published_rowset->version().first << "-"
               << published_rowset->version().second << "]"
               << ", full compaction rowset version: [" << _output_rowset->version().first << "-"
               << _output_rowset->version().second << "]"
               << ", cost: " << watch.get_elapse_time_us() << "(us), total rows: " << total_rows;
    return Status::OK();
}

} // namespace doris
