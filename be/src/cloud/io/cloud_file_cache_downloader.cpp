#include "cloud/io/cloud_file_cache_downloader.h"

#include <aws/transfer/TransferHandle.h>
#include <aws/transfer/TransferManager.h>
#include <bvar/bvar.h>
#include <fmt/core.h>
#include <gen_cpp/internal_service.pb.h>

#include <mutex>

#include "cloud/io/cloud_file_cache.h"
#include "cloud/io/cloud_file_cache_factory.h"
#include "cloud/io/cloud_file_segment.h"
#include "cloud/io/s3_file_system.h"
#include "cloud/utils.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/tablet.h"

namespace doris::io {

bvar::Adder<uint64_t> file_cache_downloader_counter("file_cache_downloader", "size");

void FileCacheSegmentDownloader::submit_download_task(DownloadTask task) {
    std::lock_guard lock(_mtx);
    if (_task_queue.size() == _max_size) {
        _task_queue.pop_front();
    }
    _task_queue.push_back(std::make_shared<DownloadTask>(std::move(task)));
    _empty.notify_all();
}

void FileCacheSegmentDownloader::polling_download_task() {
    const int64_t hot_interval = 2 * 60 * 60; // 2 hours
    while (true) {
        std::unique_lock lock(_mtx);
        if (_task_queue.empty()) {
            _empty.wait(lock, [this]() { return !_task_queue.empty() || _closed; });
        }
        if (_closed) {
            break;
        }
        std::shared_ptr<DownloadTask> task = _task_queue.front();
        if (std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() -
                                                             task->atime)
                    .count() < hot_interval) {
            bool is_busy = false;
            download_segments(task, &is_busy);
            if (is_busy) {
                lock.unlock();
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
                continue;
            }
        }
        _task_queue.pop_front();
    }
}

void FileCacheSegmentS3Downloader::check_download_task(const std::vector<int64_t>& tablets,
        std::map<int64_t, bool>* done) {
    std::unique_lock lock(_mtx);
    for (int64_t tablet : tablets) {
        bool ret = true;
        for (auto& segment : _inflight_segments) {
            if (segment.second == tablet) {
                ret = false;
            }
        }
        done->insert({tablet, ret});
    }
}

void FileCacheSegmentS3Downloader::download_segments(std::shared_ptr<DownloadTask> task, bool* is_busy) {
    *is_busy = false;
    std::for_each(task->metas.cbegin(), task->metas.cend(), [this, is_busy, task](const FileCacheSegmentMeta& meta) {
        TabletSharedPtr tablet;
        if (meta.done()) {
            return;
        }
        cloud::tablet_mgr()->get_tablet(meta.tablet_id(), &tablet);
        _inflight_tasks.insert(meta.tablet_id());
        auto id_to_rowset_meta_map = tablet->tablet_meta()->snapshot_rs_metas();
        if (auto iter = id_to_rowset_meta_map.find(meta.rowset_id());
            iter != id_to_rowset_meta_map.end()) {
            Key cache_key = CloudFileCache::hash(meta.file_name());
            CloudFileCachePtr cache = FileCacheFactory::instance().get_by_path(cache_key);
            CacheContext context;
            switch (meta.cache_type()) {
            case FileCacheType::TTL:
                context.cache_type = CacheType::TTL;
                break;
            case FileCacheType::INDEX:
                context.cache_type = CacheType::INDEX;
                break;
            default:
                context.cache_type = CacheType::NORMAL;
            }
            context.expiration_time = meta.expiration_time();
            FileSegmentsHolder holder =
                    cache->get_or_set(cache_key, meta.offset(), meta.size(), context);
            DCHECK(holder.file_segments.size() == 1);
            auto file_segment = holder.file_segments.front();
            if (file_segment->state() == FileSegment::State::EMPTY &&
                file_segment->get_or_set_downloader() == FileSegment::get_caller_id()) {
                S3FileSystem* s3_file_system =
                        dynamic_cast<S3FileSystem*>(iter->second->fs().get());
                std::tuple<std::string, int64> segment_id = {meta.rowset_id(), meta.segment_id()};
                auto set_it = _inflight_segments.find(segment_id);
                if (set_it != _inflight_segments.end()) {
                    return;
                }

                if (_inflight_segments.size() >= config::s3_transfer_executor_pool_size) {
                    *is_busy = true;
                    return;
                }

                _inflight_segments.insert({std::move(segment_id), meta.tablet_id()});

                auto transfer_manager = s3_file_system->get_transfer_manager();
                if (!transfer_manager) {
                    return;
                }
                auto download_callback =
                        [this, file_segment, &meta, task](const Aws::Transfer::TransferHandle* handle) {
                            if (handle->GetStatus() == Aws::Transfer::TransferStatus::NOT_STARTED ||
                                handle->GetStatus() == Aws::Transfer::TransferStatus::IN_PROGRESS) {
                                return; // not finish
                            }
                            if (handle->GetStatus() == Aws::Transfer::TransferStatus::COMPLETED) {
                                Status st = file_segment->finalize_write(true);
                                if (!st) {
                                    LOG(WARNING) << "s3 download error " << st;
                                }
                                file_cache_downloader_counter << file_segment->range().size();
                            } else {
                                LOG(WARNING) << "s3 download error " << handle->GetStatus();
                            }
                            _cur_download_file--;
                            {
                                std::unique_lock lock(_mtx);
                                _inflight_tasks.erase(meta.tablet_id());
                                const_cast<FileCacheSegmentMeta&>(meta).set_done(true);
                                std::tuple<std::string, int64> segment_id = {meta.rowset_id(), meta.segment_id()};
                                _inflight_segments.erase(segment_id);
                            }
                        };
                std::string download_file = file_segment->get_path_in_local_cache(true);
                auto createFileFn = [=]() {
                    return Aws::New<Aws::FStream>(meta.file_name().c_str(), download_file.c_str(),
                                                  std::ios_base::out | std::ios_base::in |
                                                          std::ios_base::binary |
                                                          std::ios_base::trunc);
                };
                transfer_manager->DownloadFile(
                        s3_file_system->s3_conf().bucket,
                        s3_file_system->get_key(BetaRowset::remote_segment_path(
                                meta.tablet_id(), meta.rowset_id(), meta.segment_id())),
                        meta.offset(), meta.size(), std::move(createFileFn),
                        Aws::Transfer::DownloadConfiguration(), download_file, nullptr,
                        download_callback);
            }
        }
    });
}

} // namespace doris::io
