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

#include "cloud/io/cached_remote_file_reader.h"

#include <memory>
#include <utility>

#include "cloud/io/cloud_file_cache.h"
#include "cloud/io/cloud_file_cache_factory.h"
#include "cloud/io/cloud_file_segment.h"
#include "common/config.h"
#include "olap/olap_common.h"
#include "util/async_io.h"
#include "util/doris_metrics.h"
#include "util/runtime_profile.h"
#include "vec/common/sip_hash.h"

namespace doris {
namespace io {

CachedRemoteFileReader::CachedRemoteFileReader(FileReaderSPtr remote_file_reader,
                                               metrics_hook metrics)
        : _remote_file_reader(std::move(remote_file_reader)), _metrics(metrics) {
    _cache_key = IFileCache::hash(path().filename().native());
    _cache = FileCacheFactory::instance().get_by_path(_cache_key);
}

CachedRemoteFileReader::~CachedRemoteFileReader() {
    close();
}

Status CachedRemoteFileReader::close() {
    return _remote_file_reader->close();
}

std::pair<size_t, size_t> CachedRemoteFileReader::_align_size(size_t offset,
                                                              size_t read_size) const {
    size_t left = offset;
    size_t right = offset + read_size - 1;
    size_t align_left = (left / config::file_cache_max_file_segment_size) *
                        config::file_cache_max_file_segment_size;
    size_t align_right = (right / config::file_cache_max_file_segment_size + 1) *
                         config::file_cache_max_file_segment_size;
    align_right = align_right < size() ? align_right : size();
    size_t align_size = align_right - align_left;
    return std::make_pair(align_left, align_size);
}

Status CachedRemoteFileReader::read_at(size_t offset, Slice result, size_t* bytes_read,
                                       IOState* state) {
    if (bthread_self() == 0) {
        return read_at_impl(offset, result, bytes_read, state);
    }
    Status s;
    auto task = [&] { s = read_at_impl(offset, result, bytes_read, state); };
    AsyncIO::run_task(task, io::FileSystemType::S3);
    return s;
}

Status CachedRemoteFileReader::read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                                            IOState* state) {
    DCHECK(!closed());
    DCHECK(state);
    if (offset > size()) {
        return Status::IOError(
                fmt::format("offset exceeds file size(offset: {), file size: {}, path: {})", offset,
                            size(), path().native()));
    }
    size_t bytes_req = result.size;
    bytes_req = std::min(bytes_req, size() - offset);
    if (UNLIKELY(bytes_req == 0)) {
        *bytes_read = 0;
        return Status::OK();
    }
    ReadStatistics stats;
    auto [align_left, align_size] = _align_size(offset, bytes_req);
    DCHECK((align_left % config::file_cache_max_file_segment_size) == 0);
    auto cache_context = CacheContext::create(state);
    FileSegmentsHolder holder =
            _cache->get_or_set(_cache_key, align_left, align_size, cache_context);
    std::vector<FileSegmentSPtr> empty_segments;
    for (auto& segment : holder.file_segments) {
        switch (segment->state()) {
        case FileSegment::State::EMPTY:
            segment->get_or_set_downloader();
            if (segment->is_downloader()) {
                empty_segments.push_back(segment);
            }
            stats.hit_cache = false;
            break;
        case FileSegment::State::SKIP_CACHE:
            empty_segments.push_back(segment);
            stats.hit_cache = false;
            stats.skip_cache = true;
            break;
        case FileSegment::State::DOWNLOADING:
            stats.hit_cache = false;
            break;
        case FileSegment::State::DOWNLOADED:
            break;
        }
    }
    stats.bytes_read += bytes_req;

    size_t empty_start = 0;
    size_t empty_end = 0;
    if (!empty_segments.empty()) {
        empty_start = empty_segments.front()->range().left;
        empty_end = empty_segments.back()->range().right;
        size_t size = empty_end - empty_start + 1;
        std::unique_ptr<char[]> buffer(new char[size]);
        {
            SCOPED_RAW_TIMER(&stats.remote_read_timer);
            RETURN_IF_ERROR(_remote_file_reader->read_at(empty_start, Slice(buffer.get(), size),
                                                         &size, state));
        }
        for (auto& segment : empty_segments) {
            if (segment->state() == FileSegment::State::SKIP_CACHE) {
                continue;
            }
            SCOPED_RAW_TIMER(&stats.local_write_timer);
            char* cur_ptr = buffer.get() + segment->range().left - empty_start;
            size_t segment_size = segment->range().size();
            RETURN_IF_ERROR(segment->append(Slice(cur_ptr, segment_size)));
            RETURN_IF_ERROR(segment->finalize_write());
            stats.bytes_write_into_file_cache += segment_size;
        }
        // copy from memory directly
        size_t right_offset = offset + result.size - 1;
        if (empty_start <= right_offset && empty_end >= offset) {
            size_t copy_left_offset = offset < empty_start ? empty_start : offset;
            size_t copy_right_offset = right_offset < empty_end ? right_offset : empty_end;
            char* dst = result.data + (copy_left_offset - offset);
            char* src = buffer.get() + (copy_left_offset - empty_start);
            size_t copy_size = copy_right_offset - copy_left_offset + 1;
            memcpy(dst, src, copy_size);
        }
    }

    size_t current_offset = offset;
    size_t end_offset = offset + bytes_req - 1;
    *bytes_read = 0;
    for (auto& segment : holder.file_segments) {
        if (current_offset > end_offset) {
            break;
        }
        size_t left = segment->range().left;
        size_t right = segment->range().right;
        if (right < offset) {
            continue;
        }
        size_t read_size =
                end_offset > right ? right - current_offset + 1 : end_offset - current_offset + 1;
        if (empty_start <= left && right <= empty_end) {
            *bytes_read += read_size;
            current_offset = right + 1;
            continue;
        }
        FileSegment::State segment_state;
        int64_t wait_time = 0;
        static int64_t MAX_WAIT_TIME = 10;
        if (segment->state() != FileSegment::State::DOWNLOADED) {
            do {
                {
                    SCOPED_RAW_TIMER(&stats.remote_read_timer);
                    segment_state = segment->wait();
                }
                if (segment_state == FileSegment::State::DOWNLOADED) {
                    break;
                }
                if (segment_state != FileSegment::State::DOWNLOADING) {
                    return Status::IOError(
                            "File Cache State is {}, the cache downloader encounters an error, "
                            "please "
                            "retry it",
                            segment_state);
                }
            } while (++wait_time < MAX_WAIT_TIME);
        }
        if (UNLIKELY(wait_time) == MAX_WAIT_TIME) {
            return Status::IOError("Waiting too long for the download to complete");
        }
        size_t file_offset = current_offset - left;
        {
            SCOPED_RAW_TIMER(&stats.local_read_timer);
            RETURN_IF_ERROR(segment->read_at(
                    Slice(result.data + (current_offset - offset), read_size), file_offset));
        }
        *bytes_read += read_size;
        current_offset = right + 1;
    }
    DCHECK(*bytes_read == bytes_req);
    DorisMetrics::instance()->s3_bytes_read_total->increment(*bytes_read);
    if (state && state->stats && _metrics) {
        _update_state(stats, state);
        _metrics(state->stats);
    }
    return Status::OK();
}

void CachedRemoteFileReader::_update_state(const ReadStatistics& read_stats, IOState* state) const {
    auto stats = state->stats;
    if (read_stats.hit_cache) {
        stats->file_cache_stats.num_local_io_total++;
        stats->file_cache_stats.bytes_read_from_local += read_stats.bytes_read;
    } else {
        stats->file_cache_stats.num_remote_io_total++;
        stats->file_cache_stats.bytes_read_from_remote += read_stats.bytes_read;
    }
    stats->file_cache_stats.remote_io_timer += read_stats.remote_read_timer;
    stats->file_cache_stats.local_io_timer += read_stats.local_read_timer;
    stats->file_cache_stats.num_skip_cache_io_total += read_stats.skip_cache ? 1 : 0;
    stats->file_cache_stats.bytes_write_into_cache += read_stats.bytes_write_into_file_cache;
    stats->file_cache_stats.write_cache_io_timer += read_stats.local_write_timer;
}

} // namespace io
} // namespace doris
