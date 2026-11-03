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

#include "exec/scan/parallel_scanner_builder.h"

#include <algorithm>
#include <cstddef>
#include <list>
#include <optional>

#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet_hotspot.h"
#include "cloud/config.h"
#include "common/cast_set.h"
#include "common/status.h"
#include "exec/operator/olap_scan_operator.h"
#include "exec/scan/olap_scanner.h"
#include "exprs/function/search/search_predicate_collector.h"
#include "io/io_common.h"
#include "runtime/exec_env.h"
#include "runtime/query_context.h"
#include "storage/index/inverted/reader/collection_statistics.h"
#include "storage/row_cursor.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/segment/segment.h"
#include "storage/segment/segment_loader.h"
#include "storage/segment/shared_segment_index_result.h"
#include "storage/tablet/base_tablet.h"
#include "util/threadpool.h"

namespace doris {

namespace {

io::FileCacheStatistics take_initial_file_cache_stats(
        std::unordered_map<int64_t, io::FileCacheStatistics>* preload_stats, int64_t tablet_id) {
    auto it = preload_stats->find(tablet_id);
    if (it == preload_stats->end()) {
        return {};
    }
    auto stats = std::move(it->second);
    preload_stats->erase(it);
    return stats;
}

io::IOContext create_preload_io_context(RuntimeState* state, OlapReaderStatistics* preload_stats) {
    io::IOContext io_ctx;
    io_ctx.reader_type = ReaderType::READER_QUERY;
    io_ctx.file_cache_stats = preload_stats ? &preload_stats->file_cache_stats : nullptr;
    if (state == nullptr) {
        return io_ctx;
    }
    io_ctx.query_id = &state->query_id();
    io_ctx.read_file_cache = state->query_options().enable_file_cache;
    io_ctx.is_disposable = state->query_options().disable_file_cache;
    if (auto* query_ctx = state->get_query_ctx(); query_ctx != nullptr) {
        io_ctx.remote_scan_cache_write_limiter = query_ctx->remote_scan_cache_write_limiter();
    }
    return io_ctx;
}

// The bounds of one key range, as SegmentIterator receives them through TabletReader.
struct ScanKeyBounds {
    std::optional<RowCursor> lower;
    bool lower_include;
    std::optional<RowCursor> upper;
    bool upper_include;
};

// Narrows one segment to the rows its tablet's key ranges may hit.
struct SegmentPruneTask {
    BetaRowsetSharedPtr rowset;
    size_t segment_idx;
    const std::vector<ScanKeyBounds>* key_bounds;
    RowRanges* ranges;
    OlapReaderStatistics stats;
    Status status;
};

// A scan with a LIMIT and no ORDER BY reads only the first segments it opens, so the caller does
// not look up the index of every segment first for it. A z-order or cluster-key table is not
// sorted by the short key, and a full scan has no key range to narrow by.
bool can_prune_by_short_key(const TabletSchema& tablet_schema,
                            const std::vector<OlapScanRange*>& key_ranges) {
    return tablet_schema.sort_type() != SortType::ZORDER &&
           tablet_schema.cluster_key_uids().empty() &&
           std::ranges::any_of(key_ranges, [](const OlapScanRange* key_range) {
               return key_range->has_lower_bound;
           });
}

Status build_key_bounds(const TabletSchemaSPtr& tablet_schema,
                        const std::vector<OlapScanRange*>& key_ranges,
                        std::vector<ScanKeyBounds>* key_bounds) {
    for (const auto* key_range : key_ranges) {
        if (!key_range->has_lower_bound) {
            continue;
        }
        auto& bounds = key_bounds->emplace_back(ScanKeyBounds {
                .lower = std::nullopt,
                .lower_include = key_range->begin_include,
                .upper = std::nullopt,
                .upper_include = key_range->end_include,
        });
        RETURN_IF_ERROR(bounds.lower.emplace().init(tablet_schema, key_range->begin_scan_range));
        RETURN_IF_ERROR(bounds.upper.emplace().init(tablet_schema, key_range->end_scan_range));
    }
    return Status::OK();
}

// The same search as SegmentIterator::_get_row_ranges_by_keys, stopped at the short key blocks:
// SegmentIterator still finds the exact rows inside the ranges this leaves.
Status prune_segment_by_short_key(RuntimeState* state, bool enable_segment_cache,
                                  SegmentPruneTask* task) {
    auto io_ctx = create_preload_io_context(state, &task->stats);
    SegmentCacheHandle segment_cache_handle;
    RETURN_IF_ERROR(SegmentLoader::instance()->load_segment(
            task->rowset, task->rowset->segment(task->segment_idx).ref(), &segment_cache_handle,
            enable_segment_cache, false, &task->stats, &io_ctx));
    const auto& segment = segment_cache_handle.get_segments()[0];
    RETURN_IF_ERROR(segment->load_index(&task->stats, &io_ctx));
    // A merge-on-write segment looks up keys in its primary key index instead.
    if (task->rowset->tablet_schema()->keys_type() == UNIQUE_KEYS &&
        segment->get_primary_key_index() != nullptr) {
        return Status::OK();
    }
    const auto num_rows = segment->num_rows();
    RowRanges key_ranges;
    for (const auto& bounds : *task->key_bounds) {
        rowid_t upper_rowid =
                segment->short_key_block_window(*bounds.upper, !bounds.upper_include, num_rows)
                        .second;
        rowid_t lower_rowid = 0;
        if (upper_rowid > 0) {
            lower_rowid = segment->short_key_block_window(*bounds.lower, bounds.lower_include,
                                                          upper_rowid)
                                  .first;
        }
        if (lower_rowid < upper_rowid) {
            RowRanges::ranges_union(key_ranges, RowRanges::create_single(lower_rowid, upper_rowid),
                                    &key_ranges);
        }
    }
    *task->ranges = std::move(key_ranges);
    return Status::OK();
}

} // namespace

Status ParallelScannerBuilder::build_scanners(std::list<ScannerSPtr>& scanners) {
    RETURN_IF_ERROR(_load());
    if (_scan_parallelism_by_per_segment) {
        return _build_scanners_by_per_segment(scanners);
    } else if (_is_dup_mow_key) {
        // Default strategy for DUP/MOW tables: split by rowids within segments
        return _build_scanners_by_rowid(scanners);
    } else {
        // TODO: support to split by key range
        return Status::NotSupported("split by key range not supported yet.");
    }
}

Status ParallelScannerBuilder::_build_scanners_by_rowid(std::list<ScannerSPtr>& scanners) {
    DCHECK_GE(_rows_per_scanner, _min_rows_per_scanner);

    for (size_t tablet_idx = 0; tablet_idx < _tablets.size(); ++tablet_idx) {
        const auto& key_ranges = *_tablet_key_ranges[tablet_idx];
        if (key_ranges.empty()) {
            continue;
        }
        const auto& tablet = _tablets[tablet_idx].tablet;
        const auto version = _tablets[tablet_idx].version;
        const auto& scan_range = *_scan_ranges[tablet_idx];
        DCHECK(_all_read_sources.contains(tablet->tablet_id()));
        auto& entire_read_source = _all_read_sources[tablet->tablet_id()];
        auto tablet_collection_statistics =
                _create_tablet_collection_statistics(entire_read_source);

        if (config::is_cloud_mode()) {
            // FIXME(plat1ko): Avoid pointer cast
            ExecEnv::GetInstance()->storage_engine().to_cloud().tablet_hotspot().count(*tablet);
        }

        // `rs_splits` in `entire read source` will be devided into several partitial read sources
        // to build several parallel scanners, based on segment rows number. All the partitial read sources
        // share the same delete predicates from their corresponding entire read source.
        TabletReadSource partitial_read_source;
        const auto rows_per_scanner = cast_set<int64_t>(_rows_per_scanner);
        int64_t rows_collected = 0;
        auto build_scanner = [&]() {
            scanners.emplace_back(
                    _build_scanner(tablet, version, key_ranges, scan_range,
                                   {.rs_splits = std::move(partitial_read_source.rs_splits),
                                    .delete_predicates = entire_read_source.delete_predicates,
                                    .delete_bitmap = entire_read_source.delete_bitmap},
                                   take_initial_file_cache_stats(&_tablet_preload_file_cache_stats,
                                                                 tablet->tablet_id()),
                                   tablet_collection_statistics));
            partitial_read_source = {};
            rows_collected = 0;
        };
        for (auto& rs_split : entire_read_source.rs_splits) {
            auto reader = rs_split.rs_reader;
            auto rowset = reader->rowset();
            auto& segments_ranges = _all_segments_ranges[rowset->rowset_id()];

            // The split being built reads the contiguous segments [segment_start, segment_end)
            // of the rowset; it is empty until a segment with rows to read starts it.
            std::optional<RowSetSplits> split;
            int64_t segment_start = 0;
            auto finish_split = [&](int64_t segment_end) {
                if (!split.has_value()) {
                    return;
                }
                split->segment_offsets = {segment_start, segment_end};
                DCHECK_EQ(split->segment_offsets.second - split->segment_offsets.first,
                          split->segment_row_ranges.size());
                DCHECK_EQ(split->segment_row_ranges.size(), split->segment_index_results.size());
                partitial_read_source.rs_splits.emplace_back(std::move(*split));
                split.reset();
            };

            for (size_t i = 0; i != segments_ranges.size(); ++i) {
                auto& segment_ranges = segments_ranges[i];
                // Empty row ranges read the whole segment, so a segment without rows to read
                // ends the split instead.
                if (segment_ranges.is_empty()) {
                    finish_split(i);
                    continue;
                }
                if (!split.has_value()) {
                    split.emplace(reader->clone());
                    segment_start = i;
                }
                // Every split that reads a part of this segment shares its index results.
                auto index_result = std::make_shared<segment_v2::SharedSegmentIndexResult>();
                int64_t remaining_rows = segment_ranges.count();
                RowRanges row_ranges;
                for (size_t range_idx = 0; range_idx < segment_ranges.range_size(); ++range_idx) {
                    int64_t offset = segment_ranges.get_range_from(range_idx);
                    const int64_t range_to = segment_ranges.get_range_to(range_idx);
                    while (offset < range_to) {
                        int64_t rows_need = rows_per_scanner - rows_collected;
                        // 0.9: try to avoid splitting the segments into excessively small parts.
                        if (rows_need >= remaining_rows * 9 / 10) {
                            rows_need = remaining_rows;
                        }
                        const int64_t rows_taken = std::min<int64_t>(rows_need, range_to - offset);
                        // RowRange stands for range: [From, To), From is inclusive, To is exclusive.
                        row_ranges.add({offset, offset + rows_taken});
                        rows_collected += rows_taken;
                        remaining_rows -= rows_taken;
                        offset += rows_taken;

                        // If collected enough rows, build a new scanner
                        if (rows_collected >= rows_per_scanner) {
                            split->segment_row_ranges.emplace_back(std::move(row_ranges));
                            split->segment_index_results.emplace_back(index_result);
                            row_ranges = RowRanges();
                            finish_split(i + 1);
                            build_scanner();
                            if (remaining_rows > 0) {
                                split.emplace(reader->clone());
                                segment_start = i;
                            }
                        }
                    }
                }
                DCHECK_EQ(remaining_rows, 0);

                // The non-empty `row_ranges` means there are some rows left in this segment not added into `split`.
                if (!row_ranges.is_empty()) {
                    DCHECK_GT(rows_collected, 0);
                    split->segment_row_ranges.emplace_back(std::move(row_ranges));
                    split->segment_index_results.emplace_back(index_result);
                }
            }
            finish_split(segments_ranges.size());
        } // end `for (auto& rowset : rowsets)`

        DCHECK_LE(rows_collected, rows_per_scanner);
        if (rows_collected > 0) {
            DCHECK_GT(partitial_read_source.rs_splits.size(), 0);
#ifndef NDEBUG
            for (auto& split : partitial_read_source.rs_splits) {
                DCHECK(split.rs_reader != nullptr);
                DCHECK_LT(split.segment_offsets.first, split.segment_offsets.second);
                DCHECK_EQ(split.segment_row_ranges.size(),
                          split.segment_offsets.second - split.segment_offsets.first);
            }
#endif
            build_scanner();
        }
    }

    return Status::OK();
}

// Build scanners so that each segment is exclusively scanned by a single scanner.
// This guarantees the number of scanners equals the number of segments across all rowsets
// for the involved tablets. It preserves delete predicates and key ranges, and clones
// RowsetReader per scanner to avoid sharing between scanners.
Status ParallelScannerBuilder::_build_scanners_by_per_segment(std::list<ScannerSPtr>& scanners) {
    DCHECK_GE(_rows_per_scanner, _min_rows_per_scanner);

    for (size_t tablet_idx = 0; tablet_idx < _tablets.size(); ++tablet_idx) {
        const auto& key_ranges = *_tablet_key_ranges[tablet_idx];
        if (key_ranges.empty()) {
            continue;
        }
        auto&& [tablet, version] = _tablets[tablet_idx];
        const auto& scan_range = *_scan_ranges[tablet_idx];
        DCHECK(_all_read_sources.contains(tablet->tablet_id()));
        auto& entire_read_source = _all_read_sources[tablet->tablet_id()];
        auto tablet_collection_statistics =
                _create_tablet_collection_statistics(entire_read_source);

        if (config::is_cloud_mode()) {
            // FIXME(plat1ko): Avoid pointer cast
            ExecEnv::GetInstance()->storage_engine().to_cloud().tablet_hotspot().count(*tablet);
        }

        // For each RowSet split in the read source, split by segment id and build
        // one scanner per segment. Keep delete predicates shared.
        for (auto& rs_split : entire_read_source.rs_splits) {
            auto reader = rs_split.rs_reader;
            auto rowset = reader->rowset();
            const auto rowset_id = rowset->rowset_id();
            const auto& segments_rows = _all_segments_rows[rowset_id];
            if (segments_rows.empty() || rowset->num_rows() == 0) {
                continue;
            }

            // Build scanners for [i, i+1) segment range, without row-range slicing.
            auto& segments_ranges = _all_segments_ranges[rowset_id];
            for (int64_t i = 0; i < rowset->num_segments(); ++i) {
                // The key ranges hit no row of this segment.
                if (segments_ranges[i].is_empty()) {
                    continue;
                }
                RowSetSplits split(reader->clone());
                split.segment_offsets.first = i;
                split.segment_offsets.second = i + 1;
                // No row-ranges slicing; scan whole segment i.
                DCHECK_GE(split.segment_offsets.second, split.segment_offsets.first + 1);

                TabletReadSource partitial_read_source;
                partitial_read_source.rs_splits.emplace_back(std::move(split));

                scanners.emplace_back(_build_scanner(
                        tablet, version, key_ranges, scan_range,
                        {.rs_splits = std::move(partitial_read_source.rs_splits),
                         .delete_predicates = entire_read_source.delete_predicates,
                         .delete_bitmap = entire_read_source.delete_bitmap},
                        take_initial_file_cache_stats(&_tablet_preload_file_cache_stats,
                                                      tablet->tablet_id()),
                        tablet_collection_statistics));
            }
        }
    }

    return Status::OK();
}

/**
 * Load rowsets of each tablet with specified version, segments of each rowset.
 */
Status ParallelScannerBuilder::_load() {
    _total_rows = 0;
    bool enable_segment_cache = _state->query_options().__isset.enable_segment_cache
                                        ? _state->query_options().enable_segment_cache
                                        : true;
    for (size_t idx = 0; idx < _tablets.size(); ++idx) {
        if (_tablet_key_ranges[idx]->empty()) {
            continue;
        }
        auto&& [tablet, version] = _tablets[idx];
        const auto tablet_id = tablet->tablet_id();
        _all_read_sources[tablet_id] = _read_sources[idx];
        const auto& read_source = _all_read_sources[tablet_id];
        for (auto& rs_split : read_source.rs_splits) {
            auto rowset = rs_split.rs_reader->rowset();
            RETURN_IF_ERROR(rowset->load());
            const auto rowset_id = rowset->rowset_id();

            auto beta_rowset = std::dynamic_pointer_cast<BetaRowset>(rowset);
            std::vector<uint32_t> segment_rows;
            OlapReaderStatistics preload_stats;
            auto preload_io_ctx = create_preload_io_context(_state, &preload_stats);
            RETURN_IF_ERROR(beta_rowset->get_segment_num_rows(&segment_rows, enable_segment_cache,
                                                              &preload_stats, &preload_io_ctx));
            _tablet_preload_file_cache_stats[tablet_id].merge_from(preload_stats.file_cache_stats);
            auto segment_count = rowset->num_segments();
            for (int64_t i = 0; i != segment_count; i++) {
                _all_segments_rows[rowset_id].emplace_back(segment_rows[i]);
            }
        }
    }

    RETURN_IF_ERROR(_init_segments_ranges());
    for (auto& [rowset_id, segments_ranges] : _all_segments_ranges) {
        for (auto& ranges : segments_ranges) {
            _total_rows += ranges.count();
        }
    }

    _rows_per_scanner = _total_rows / _max_scanners_count;
    _rows_per_scanner = std::max<size_t>(_rows_per_scanner, _min_rows_per_scanner);

    return Status::OK();
}

Status ParallelScannerBuilder::_init_segments_ranges() {
    // Tasks point into these, so neither changes size once the tasks are created.
    std::list<std::vector<ScanKeyBounds>> tablets_key_bounds;
    std::list<SegmentPruneTask> tasks;

    for (size_t idx = 0; idx < _tablets.size(); ++idx) {
        const auto& key_ranges = *_tablet_key_ranges[idx];
        if (key_ranges.empty()) {
            continue;
        }
        const auto& tablet = _tablets[idx].tablet;
        const std::vector<ScanKeyBounds>* key_bounds = nullptr;
        if (_limit < 0 && can_prune_by_short_key(*tablet->tablet_schema(), key_ranges)) {
            auto& bounds = tablets_key_bounds.emplace_back();
            RETURN_IF_ERROR(build_key_bounds(tablet->tablet_schema(), key_ranges, &bounds));
            key_bounds = &bounds;
        }
        for (const auto& rs_split : _all_read_sources[tablet->tablet_id()].rs_splits) {
            auto rowset = std::dynamic_pointer_cast<BetaRowset>(rs_split.rs_reader->rowset());
            const auto& segments_rows = _all_segments_rows[rowset->rowset_id()];
            auto& segments_ranges = _all_segments_ranges[rowset->rowset_id()];
            segments_ranges.resize(segments_rows.size());
            for (size_t i = 0; i < segments_rows.size(); ++i) {
                segments_ranges[i] = RowRanges::create_single(segments_rows[i]);
                if (key_bounds != nullptr && segments_rows[i] > 0) {
                    tasks.push_back(SegmentPruneTask {.rowset = rowset,
                                                      .segment_idx = i,
                                                      .key_bounds = key_bounds,
                                                      .ranges = &segments_ranges[i],
                                                      .stats = {},
                                                      .status = Status::OK()});
                }
            }
        }
    }
    if (tasks.empty()) {
        return Status::OK();
    }

    bool enable_segment_cache = _state->query_options().__isset.enable_segment_cache
                                        ? _state->query_options().enable_segment_cache
                                        : true;
    auto token = ExecEnv::GetInstance()->segment_prefetch_thread_pool()->new_token(
            ThreadPool::ExecutionMode::CONCURRENT, cast_set<int>(_max_scanners_count));
    Status submit_status;
    for (auto& task : tasks) {
        submit_status = token->submit_func([this, enable_segment_cache, &task] {
            task.status = prune_segment_by_short_key(_state, enable_segment_cache, &task);
        });
        if (!submit_status.ok()) {
            break;
        }
    }
    // The submitted tasks reference `tasks`, so wait for them before any return.
    token->wait();
    RETURN_IF_ERROR(submit_status);
    for (auto& task : tasks) {
        RETURN_IF_ERROR(task.status);
        _tablet_preload_file_cache_stats[task.rowset->rowset_meta()->tablet_id()].merge_from(
                task.stats.file_cache_stats);
    }
    return Status::OK();
}

std::shared_ptr<TabletCollectionStatistics>
ParallelScannerBuilder::_create_tablet_collection_statistics(
        const TabletReadSource& entire_read_source) const {
    if (!_share_tablet_collection_statistics) {
        return nullptr;
    }
    std::vector<RowsetSharedPtr> rowsets;
    rowsets.reserve(entire_read_source.rs_splits.size());
    for (const auto& rs_split : entire_read_source.rs_splits) {
        rowsets.emplace_back(rs_split.rs_reader->rowset());
    }
    return std::make_shared<TabletCollectionStatistics>(
            std::move(rowsets), std::make_unique<SearchPredicateCollector>());
}

std::shared_ptr<OlapScanner> ParallelScannerBuilder::_build_scanner(
        BaseTabletSPtr tablet, int64_t version, const std::vector<OlapScanRange*>& key_ranges,
        const TPaloScanRange& scan_range, TabletReadSource&& read_source,
        io::FileCacheStatistics&& initial_file_cache_stats,
        std::shared_ptr<TabletCollectionStatistics> tablet_collection_statistics) {
    OlapScanner::Params params {
            .state = _state,
            .profile = _scanner_profile.get(),
            .key_ranges = key_ranges,
            .tablet = std::move(tablet),
            .version = version,
            .read_source = std::move(read_source),
            .initial_file_cache_stats = std::move(initial_file_cache_stats),
            .limit = _limit,
            .aggregation = _is_preaggregation,
            .read_changelog = false,
            .changelog_scan_type = TChangelogScanType::NONE,
            .bucket_seq = scan_range.bucket_seq,
            .bucket_num = scan_range.bucket_num,
            .start_tso = std::nullopt,
            .end_tso = std::nullopt,
            .tablet_collection_statistics = std::move(tablet_collection_statistics),
    };
    return OlapScanner::create_shared(_parent, std::move(params));
}

} // namespace doris
