#pragma once

#include <condition_variable>
#include <list>
#include <memory>
#include <mutex>

#include "cloud/io/cloud_file_cache.h"
#include "cloud/io/file_reader.h"
#include "cloud/io/file_writer.h"
#include "common/status.h"

namespace doris {
namespace io {

class FileSegment;
using FileSegmentSPtr = std::shared_ptr<FileSegment>;
using FileSegments = std::list<FileSegmentSPtr>;

class FileSegment {
    friend class LRUFileCache;
    friend struct FileSegmentsHolder;

public:
    using Key = IFileCache::Key;
    using LocalWriterPtr = std::unique_ptr<FileWriter>;
    using LocalReaderPtr = std::shared_ptr<FileReader>;

    enum class State {
        DOWNLOADED,
        /**
         * When file segment is first created and returned to user, it has state EMPTY.
         * EMPTY state can become DOWNLOADING when getOrSetDownaloder is called successfully
         * by any owner of EMPTY state file segment.
         */
        EMPTY,
        /**
         * A newly created file segment never has DOWNLOADING state until call to getOrSetDownloader
         * because each cache user might acquire multiple file segments and reads them one by one,
         * so only user which actually needs to read this segment earlier than others - becomes a downloader.
         */
        DOWNLOADING,
        SKIP_CACHE,
    };

    FileSegment(size_t offset, size_t size, const Key& key, IFileCache* cache, State download_state,
                CacheType cache_type, int64_t expiration_time);

    ~FileSegment() = default;

    State state() const;

    static std::string state_to_string(FileSegment::State state);

    /// Represents an interval [left, right] including both boundaries.
    struct Range {
        size_t left;
        size_t right;

        Range(size_t left_, size_t right_) : left(left_), right(right_) {}

        bool operator==(const Range& other) const {
            return left == other.left && right == other.right;
        }

        size_t size() const { return right - left + 1; }

        std::string to_string() const {
            return fmt::format("[{}, {}]", std::to_string(left), std::to_string(right));
        }
    };

    const Range& range() const { return _segment_range; }

    const Key& key() const { return _file_key; }

    size_t offset() const { return range().left; }

    State wait();

    // append data to cache file
    Status append(Slice data);

    // read data from cache file
    Status read_at(Slice buffer, size_t offset_);

    // finish write, release the file writer
    Status finalize_write();

    // set downloader if state == EMPTY
    std::string get_or_set_downloader();

    std::string get_downloader() const;

    int64_t expiration_time() const { return _expiration_time; }

    void reset_downloader(std::lock_guard<std::mutex>& segment_lock);

    bool is_downloader() const;

    bool is_downloaded() const { return _is_downloaded.load(); }

    CacheType cache_type() const { return _cache_type; }

    static std::string get_caller_id();

    size_t get_download_offset() const;

    size_t get_downloaded_size() const;

    std::string get_info_for_log() const;

    std::string get_path_in_local_cache() const;

    bool change_cache_type(CacheType new_type);

    Status change_cache_type_self(CacheType new_type);

    void update_expiration_time(int64_t expiration_time) { _expiration_time = expiration_time; }

    // only used for s3 file writer
    // should guarantee that the fragment don't have concurrency problem
    void reset_range();

    FileSegment& operator=(const FileSegment&) = delete;
    FileSegment(const FileSegment&) = delete;

private:
    size_t get_downloaded_size(std::lock_guard<std::mutex>& segment_lock) const;
    std::string get_info_for_log_impl(std::lock_guard<std::mutex>& segment_lock) const;
    bool has_finalized_state() const;

    Status set_downloaded(std::lock_guard<std::mutex>& segment_lock);
    bool is_downloader_impl(std::lock_guard<std::mutex>& segment_lock) const;

    void complete_unlocked(std::lock_guard<std::mutex>& segment_lock);

    void reset_downloader_impl(std::lock_guard<std::mutex>& segment_lock);

    Range _segment_range;

    State _download_state;

    std::string _downloader_id;

    LocalWriterPtr _cache_writer;
    LocalReaderPtr _cache_reader;

    size_t _downloaded_size = 0;

    /// global locking order rule:
    /// 1. cache lock
    /// 2. segment lock

    mutable std::mutex _mutex;
    std::condition_variable _cv;

    /// Protects downloaded_size access with actual write into fs.
    /// downloaded_size is not protected by download_mutex in methods which
    /// can never be run in parallel to FileSegment::write() method
    /// as downloaded_size is updated only in FileSegment::write() method.
    /// Such methods are identified by isDownloader() check at their start,
    /// e.g. they are executed strictly by the same thread, sequentially.
    mutable std::mutex _download_mutex;

    Key _file_key;
    IFileCache* _cache;

    std::atomic<bool> _is_downloaded {false};
    CacheType _cache_type;
    int64_t _expiration_time {0};
};

struct FileSegmentsHolder {
    explicit FileSegmentsHolder(FileSegments&& file_segments_)
            : file_segments(std::move(file_segments_)) {}

    FileSegmentsHolder(FileSegmentsHolder&& other) noexcept
            : file_segments(std::move(other.file_segments)) {}

    FileSegmentsHolder& operator=(const FileSegmentsHolder&) = delete;
    FileSegmentsHolder(const FileSegmentsHolder&) = delete;

    ~FileSegmentsHolder();

    FileSegments file_segments {};

    std::string to_string();
};

using FileSegmentsHolderPtr = std::unique_ptr<FileSegmentsHolder>;

} // namespace io
} // namespace doris
