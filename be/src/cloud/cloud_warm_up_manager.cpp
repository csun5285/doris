#include "cloud/cloud_warm_up_manager.h"

#include <algorithm>
#include <tuple>

#include "cloud/utils.h"
#include "common/logging.h"
#include "io/cache/block/block_file_cache_downloader.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "olap/tablet.h"
#include "util/time.h"
#include "util/wait_group.h"

namespace doris::cloud {

CloudWarmUpManager::CloudWarmUpManager() {
    _download_thread = std::thread(&CloudWarmUpManager::handle_jobs, this);
}

CloudWarmUpManager::~CloudWarmUpManager() {
    {
        std::lock_guard lock(_mtx);
        _closed = true;
    }
    _cond.notify_all();
    if (_download_thread.joinable()) {
        _download_thread.join();
    }
}

void CloudWarmUpManager::handle_jobs() {
#ifndef BE_TEST
    while (true) {
        JobMeta* cur_job = nullptr;
        {
            std::unique_lock lock(_mtx);
            _cond.wait(lock, [this]() { return _closed || !_pending_job_metas.empty(); });
            if (_closed) break;
            cur_job = &_pending_job_metas.front();
        }
        for (int64_t tablet_id : cur_job->tablet_ids) {
            TabletSharedPtr tablet;
            auto st = cloud::tablet_mgr()->get_tablet(tablet_id, &tablet);
            if (!st) {
                LOG_WARNING("Warm up error ").tag("tablet_id", tablet_id).error(st);
                continue;
            }
            st = tablet->cloud_sync_rowsets();
            if (!st) {
                LOG_WARNING("Warm up error ").tag("tablet_id", tablet_id).error(st);
                continue;
            }
            std::shared_ptr<WaitGroup> wait = std::make_shared<WaitGroup>();
            auto callback = [=](Status st) {
                if (!st) {
                    LOG_WARNING("Warm up error ").error(st);
                }
                wait->done();
            };
            auto tablet_meta = tablet->tablet_meta();
            auto rs_metas = tablet_meta->snapshot_rs_metas();
            for (auto& [_, rs] : rs_metas) {
                auto inverted_indexes = rs->tablet_schema()->get_inverted_indexes();
                for (int64_t seg_id = 0; seg_id < rs->num_segments(); seg_id++) {
                    io::S3FileMeta download_file_meta;
                    download_file_meta.file_size = rs->get_segment_file_size(seg_id);
                    download_file_meta.file_system = rs->fs();
                    std::string seg_path = BetaRowset::remote_segment_path(rs->tablet_id(),
                                                                           rs->rowset_id(), seg_id);
                    download_file_meta.path = seg_path;
                    download_file_meta.expiration_time =
                            tablet_meta->ttl_seconds() == 0
                                    ? 0
                                    : rs->newest_write_timestamp() + tablet_meta->ttl_seconds();
                    if (download_file_meta.expiration_time <= UnixSeconds()) {
                        download_file_meta.expiration_time = 0;
                    }
                    download_file_meta.download_callback = callback;
                    wait->add();
                    io::FileCacheSegmentDownloader::instance()->submit_download_task(
                            std::move(download_file_meta));
                    for (auto index_meta : inverted_indexes) {
                        io::S3FileMeta download_file_meta;
                        download_file_meta.file_system = rs->fs();
                        download_file_meta.path = InvertedIndexDescriptor::get_index_file_name(
                                seg_path, index_meta->index_id());
                        download_file_meta.download_callback = callback;
                        wait->add();
                        io::FileCacheSegmentDownloader::instance()->submit_download_task(
                                std::move(download_file_meta));
                    }
                }
            }
            // TODO(liuchangliang): flow control
            if (!wait->wait()) {
                LOG_WARNING("Warm up tablet {} take a long time", tablet_meta->tablet_id());
            }
        }
        {
            std::unique_lock lock(_mtx);
            _finish_job.push_back(std::move(*cur_job));
            _pending_job_metas.pop_front();
        }
    }
#endif
}

JobMeta::JobMeta(const TJobMeta& meta)
        : be_ip(meta.be_ip), brpc_port(meta.brpc_port), tablet_ids(meta.tablet_ids) {
    switch (meta.download_type) {
    case TDownloadType::BE:
        download_type = DownloadType::BE;
        break;
    case TDownloadType::S3:
        download_type = DownloadType::S3;
        break;
    }
}

Status CloudWarmUpManager::check_and_set_job_id(int64_t job_id) {
    std::lock_guard lock(_mtx);
    if (_cur_job_id == 0) {
        _cur_job_id = job_id;
    }
    Status st = Status::OK();
    if (_cur_job_id != job_id) {
        st = Status::InternalError("The job {} is running", _cur_job_id);
    }
    return st;
}

Status CloudWarmUpManager::check_and_set_batch_id(int64_t job_id, int64_t batch_id, bool* retry) {
    std::lock_guard lock(_mtx);
    Status st = Status::OK();
    if (_cur_job_id != 0 && _cur_job_id != job_id) {
        st = Status::InternalError("The job {} is not current job, current job is {}", job_id,
                                   _cur_job_id);
        return st;
    }
    if (_cur_job_id == 0) {
        _cur_job_id = job_id;
    }
    if (_cur_batch_id == batch_id) {
        *retry = true;
        return st;
    }
    if (_pending_job_metas.empty()) {
        _cur_batch_id = batch_id;
    } else {
        st = Status::InternalError("The batch {} is not finish", _cur_batch_id);
    }
    return st;
}

void CloudWarmUpManager::add_job(const std::vector<TJobMeta>& job_metas) {
    {
        std::lock_guard lock(_mtx);
        std::for_each(job_metas.begin(), job_metas.end(),
                      [this](const TJobMeta& meta) { _pending_job_metas.emplace_back(meta); });
    }
    _cond.notify_all();
}

#ifdef BE_TEST
void CloudWarmUpManager::consumer_job() {
    {
        std::unique_lock lock(_mtx);
        _finish_job.push_back(_pending_job_metas.front());
        _pending_job_metas.pop_front();
    }
}

#endif

std::tuple<int64_t, int64_t, int64_t, int64_t> CloudWarmUpManager::get_current_job_state() {
    std::lock_guard lock(_mtx);
    return std::make_tuple(_cur_job_id, _cur_batch_id, _pending_job_metas.size(),
                           _finish_job.size());
}

Status CloudWarmUpManager::clear_job(int64_t job_id) {
    std::lock_guard lock(_mtx);
    Status st = Status::OK();
    if (job_id == _cur_job_id) {
        _cur_job_id = 0;
        _cur_batch_id = -1;
        _pending_job_metas.clear();
        _finish_job.clear();
    } else {
        st = Status::InternalError("The job {} is not current job, current job is {}", job_id,
                                   _cur_job_id);
    }
    return st;
}

} // namespace doris::cloud
