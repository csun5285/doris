#pragma once

#include <gen_cpp/internal_service.pb.h>
#include <glog/logging.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <memory>
#include <thread>
#include <variant>

#include "io/fs/file_system.h"

namespace Aws::Transfer {
class TransferManager;
} // namespace Aws::Transfer

namespace doris::io {

struct S3FileMeta {
    Path path;
    size_t file_size {0};
    io::FileSystemSPtr file_system;
    uint64_t expiration_time {0};
    bool is_cold_data {false};
    std::function<void(Status)> download_callback;
};

struct DownloadTask {
    std::chrono::steady_clock::time_point atime = std::chrono::steady_clock::now();
    std::variant<std::vector<FileCacheSegmentMeta>, S3FileMeta> task_message;
    DownloadTask(std::vector<FileCacheSegmentMeta> metas) : task_message(std::move(metas)) {}
    DownloadTask(S3FileMeta meta) : task_message(std::move(meta)) {}
    DownloadTask() = default;
};

class FileCacheSegmentDownloader {
public:
    FileCacheSegmentDownloader() {
        _download_thread = std::thread(&FileCacheSegmentDownloader::polling_download_task, this);
    }

    virtual ~FileCacheSegmentDownloader() {
        _closed = true;
        _empty.notify_all();
        if (_download_thread.joinable()) {
            _download_thread.join();
        }
    }

    virtual void download_segments(DownloadTask& task) = 0;

    static FileCacheSegmentDownloader* instance() {
        DCHECK(downloader);
        return downloader;
    }

    void submit_download_task(DownloadTask task);

    void polling_download_task();

    void check_download_task(const std::vector<int64_t>& tablets, std::map<int64_t, bool>* done);

protected:
    std::mutex _mtx;
    std::mutex _inflight_mtx;
    // tablet id -> inflight segment num of tablet
    std::unordered_map<int64_t, int64_t> _inflight_tablets;
    inline static FileCacheSegmentDownloader* downloader {nullptr};

private:
    std::thread _download_thread;
    std::condition_variable _empty;
    std::deque<DownloadTask> _task_queue;
    std::atomic_bool _closed {false};
    const size_t _max_size {10240};
};

class FileCacheSegmentS3Downloader : FileCacheSegmentDownloader {
public:
    static void create_preheating_s3_downloader() {
        static FileCacheSegmentS3Downloader s3_downloader;
        FileCacheSegmentDownloader::downloader = &s3_downloader;
    }

    FileCacheSegmentS3Downloader() = default;
    ~FileCacheSegmentS3Downloader() override = default;

    void download_segments(DownloadTask& task) override;

private:
    std::mutex _mtx;
    void download_file_cache_segment(std::vector<FileCacheSegmentMeta>&);
    void download_s3_file(S3FileMeta&);
    std::atomic<size_t> _cur_download_file {0};
};

} // namespace doris::io
