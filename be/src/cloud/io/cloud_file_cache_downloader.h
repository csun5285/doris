#pragma once

#include <gen_cpp/internal_service.pb.h>
#include <glog/logging.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>

#include "cloud/io/s3_file_system.h"
#include "common/status.h"

namespace Aws::Transfer {
class TransferManager;
} // namespace Aws::Transfer

namespace doris::io {

struct DownloadTask {
    std::chrono::steady_clock::time_point atime;
    std::vector<FileCacheSegmentMeta> metas;
    DownloadTask(std::vector<FileCacheSegmentMeta> metas) : metas(std::move(metas)) {
        atime = std::chrono::steady_clock::now();
    }
};

class FileCacheSegmentDownloader {
public:
    FileCacheSegmentDownloader() {
        _download_thread = std::thread(&FileCacheSegmentDownloader::polling_download_task, this);
    }

    ~FileCacheSegmentDownloader() {
        _closed = true;
        _empty.notify_all();
        if (_download_thread.joinable()) {
            _download_thread.join();
        }
    }

    virtual void download_segments(DownloadTask task) = 0;

    inline static FileCacheSegmentDownloader* downloader {nullptr};

    static FileCacheSegmentDownloader* instance() {
        DCHECK(downloader);
        return downloader;
    }

    void submit_download_task(DownloadTask task);

    void polling_download_task();

    virtual void check_download_task(const std::vector<int64_t>& tablets, std::map<int64_t, bool>* done) = 0;

protected:
    std::mutex _mtx;

private:
    std::thread _download_thread;
    std::condition_variable _empty;
    std::deque<DownloadTask> _task_queue;
    std::atomic_bool _closed {false};
    const size_t _max_size {1024};
};

class FileCacheSegmentS3Downloader : FileCacheSegmentDownloader {
public:
    static void create_preheating_s3_downloader() {
        static FileCacheSegmentS3Downloader s3_downloader;
        FileCacheSegmentDownloader::downloader = &s3_downloader;
    }

    FileCacheSegmentS3Downloader() = default;

    void download_segments(DownloadTask task) override;

    void check_download_task(const std::vector<int64_t>& tablets, std::map<int64_t, bool>* done) override;

private:
    std::atomic<size_t> _cur_download_file {0};
    std::unordered_set<int64_t> _inflight_tasks;
};

} // namespace doris::io
