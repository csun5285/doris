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

#include "olap/compaction.h"

#include <gen_cpp/olap_file.pb.h>

#include "cloud/utils.h"
#include "common/status.h"
#include "common/sync_point.h"
#include "gutil/strings/substitute.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/rowset/segment_v2/inverted_index_compaction.h"
#include "olap/tablet.h"
#include "olap/task/engine_checksum_task.h"
#include "util/time.h"
#include "util/trace.h"

using std::vector;

namespace doris {

Compaction::Compaction(TabletSharedPtr tablet, const std::string& label)
        : _tablet(tablet),
          _input_rowsets_size(0),
          _input_row_num(0),
          _input_num_segments(0),
          _input_index_size(0),
          _state(CompactionState::INITED) {
    _mem_tracker = std::make_shared<MemTrackerLimiter>(MemTrackerLimiter::Type::COMPACTION, label);
}

Compaction::~Compaction() {}

Status Compaction::compact() {
    RETURN_NOT_OK(prepare_compact());
    RETURN_NOT_OK(execute_compact());
    return Status::OK();
}

Status Compaction::execute_compact() {
    Status st = execute_compact_impl();
    if (!st.ok()) {
        garbage_collection();
    }
    return st;
}

Status Compaction::quick_rowsets_compact() {
    std::unique_lock<std::mutex> lock(_tablet->get_cumulative_compaction_lock(), std::try_to_lock);
    if (!lock.owns_lock()) {
        LOG(WARNING) << "The tablet is under cumulative compaction. tablet="
                     << _tablet->full_name();
        return Status::OLAPInternalError(OLAP_ERR_CE_TRY_CE_LOCK_ERROR);
    }

    // Clone task may happen after compaction task is submitted to thread pool, and rowsets picked
    // for compaction may change. In this case, current compaction task should not be executed.
    if (_tablet->get_clone_occurred()) {
        _tablet->set_clone_occurred(false);
        return Status::OLAPInternalError(OLAP_ERR_CUMULATIVE_CLONE_OCCURRED);
    }

    _input_rowsets.clear();
    int version_count = _tablet->version_count();
    MonotonicStopWatch watch;
    watch.start();
    int64_t permits = 0;
    _tablet->pick_quick_compaction_rowsets(&_input_rowsets, &permits);
    std::vector<Version> missedVersions;
    find_longest_consecutive_version(&_input_rowsets, &missedVersions);
    if (missedVersions.size() != 0) {
        LOG(WARNING) << "quick_rowsets_compaction, find missed version"
                     << ",input_size:" << _input_rowsets.size();
    }
    int nums = _input_rowsets.size();
    if (_input_rowsets.size() >= config::quick_compaction_min_rowsets) {
        Status st = check_version_continuity(_input_rowsets);
        if (!st.ok()) {
            LOG(WARNING) << "quick_rowsets_compaction failed, cause version not continuous";
            return st;
        }
        st = do_compaction(permits);
        if (!st.ok()) {
            garbage_collection();
            LOG(WARNING) << "quick_rowsets_compaction failed";
        } else {
            LOG(INFO) << "quick_compaction succ"
                      << ", before_versions:" << version_count
                      << ", after_versions:" << _tablet->version_count()
                      << ", cost:" << (watch.elapsed_time() / 1000 / 1000) << "ms"
                      << ", merged: " << nums << ", batch:" << config::quick_compaction_batch_size
                      << ", segments:" << permits << ", tabletid:" << _tablet->tablet_id();
            _tablet->set_last_quick_compaction_success_time(UnixMillis());
        }
    }
    return Status::OK();
}

Status Compaction::do_compaction(int64_t permits) {
    TRACE("start to do compaction");
    TEST_INJECTION_POINT_RETURN_WITH_VALUE("Compaction::do_compaction", Status());
    uint32_t checksum_before;
    uint32_t checksum_after;
    if (config::enable_compaction_checksum) {
        EngineChecksumTask checksum_task(_tablet->tablet_id(), _tablet->schema_hash(),
                                         _input_rowsets.back()->end_version(), &checksum_before);
        checksum_task.execute();
    }

    _tablet->data_dir()->disks_compaction_score_increment(permits);
    _tablet->data_dir()->disks_compaction_num_increment(1);
    Status st = do_compaction_impl(permits);
    _tablet->data_dir()->disks_compaction_score_increment(-permits);
    _tablet->data_dir()->disks_compaction_num_increment(-1);

    if (config::enable_compaction_checksum) {
        EngineChecksumTask checksum_task(_tablet->tablet_id(), _tablet->schema_hash(),
                                         _input_rowsets.back()->end_version(), &checksum_after);
        checksum_task.execute();
        if (checksum_before != checksum_after) {
            LOG(WARNING) << "Compaction tablet=" << _tablet->tablet_id()
                         << " checksum not consistent"
                         << ", before=" << checksum_before << ", checksum_after=" << checksum_after;
        }
    }
    return st;
}

bool Compaction::should_vertical_compaction() {
    // some conditions that not use vertical compaction
    if (!config::enable_vertical_compaction) {
        return false;
    }
    if (_tablet->enable_unique_key_merge_on_write()) {
        return false;
    }
    return true;
}

int64_t Compaction::get_avg_segment_rows() {
    // take care of empty rowset
    // input_rowsets_size is total disk_size of input_rowset, this size is the
    // final size after codec and compress, so expect dest segment file size
    // in disk is config::max_segment_size_in_vertical_compaction
    return config::max_segment_size_in_vertical_compaction /
           (_input_rowsets_size / (_input_row_num + 1) + 1);
}

bool Compaction::is_rowset_tidy(std::string& pre_max_key, const RowsetSharedPtr& rhs) {
    size_t min_tidy_size = config::ordered_data_compaction_min_segment_size;
    if (rhs->num_segments() == 0) {
        return true;
    }
    if (rhs->is_segments_overlapping()) {
        return false;
    }
    // check segment size
    auto beta_rowset = reinterpret_cast<BetaRowset*>(rhs.get());
    std::vector<size_t> segments_size;
    beta_rowset->get_segments_size(&segments_size);
    for (auto segment_size : segments_size) {
        // is segment is too small, need to do compaction
        if (segment_size < min_tidy_size) {
            return false;
        }
    }
    std::string min_key;
    auto ret = rhs->min_key(&min_key);
    if (!ret) {
        return false;
    }
    if (min_key < pre_max_key) {
        return false;
    }
    CHECK(rhs->max_key(&pre_max_key));

    return true;
}

Status Compaction::do_compact_ordered_rowsets() {
    build_basic_info();
    RowsetWriterContext context;
    context.txn_id = boost::uuids::hash_value(UUIDGenerator::instance()->next_uuid()) &
                     std::numeric_limits<int64_t>::max(); // MUST be positive
    context.txn_expiration = _expiration;
    context.version = _output_version;
    context.rowset_state = VISIBLE;
    context.segments_overlap = NONOVERLAPPING;
    context.tablet_schema = _cur_tablet_schema;
    context.oldest_write_timestamp = _oldest_write_timestamp;
    context.newest_write_timestamp = _newest_write_timestamp;
    RETURN_IF_ERROR(_tablet->create_rowset_writer(context, &_output_rs_writer));

    LOG(INFO) << "start to do ordered data compaction, tablet=" << _tablet->full_name()
              << ", output_version=" << _output_version;
    // link data to new rowset
    auto seg_id = 0;
    std::vector<KeyBoundsPB> segment_key_bounds;
    for (auto rowset : _input_rowsets) {
        RETURN_NOT_OK(rowset->link_files_to(_tablet->tablet_path(), _output_rs_writer->rowset_id(),
                                            seg_id));
        seg_id += rowset->num_segments();

        std::vector<KeyBoundsPB> key_bounds;
        rowset->get_segments_key_bounds(&key_bounds);
        segment_key_bounds.insert(segment_key_bounds.end(), key_bounds.begin(), key_bounds.end());
    }
    // build output rowset
    RowsetMetaSharedPtr rowset_meta = std::make_shared<RowsetMeta>();
    rowset_meta->set_num_rows(_input_row_num);
    rowset_meta->set_total_disk_size(_input_rowsets_size);
    rowset_meta->set_data_disk_size(_input_rowsets_size);
    rowset_meta->set_index_disk_size(_input_index_size);
    rowset_meta->set_empty(_input_row_num == 0);
    rowset_meta->set_num_segments(_input_num_segments);
    rowset_meta->set_segments_overlap(NONOVERLAPPING);
    rowset_meta->set_rowset_state(VISIBLE);

    rowset_meta->set_segments_key_bounds(segment_key_bounds);
    _output_rowset = _output_rs_writer->manual_build(rowset_meta);
    return Status::OK();
}

void Compaction::build_basic_info() {
    for (auto& rowset : _input_rowsets) {
        _input_rowsets_size += rowset->data_disk_size();
        _input_index_size += rowset->index_disk_size();
        _input_row_num += rowset->num_rows();
        _input_num_segments += rowset->num_segments();
    }
    TRACE_COUNTER_INCREMENT("input_rowsets_data_size", _input_rowsets_size);
    TRACE_COUNTER_INCREMENT("input_row_num", _input_row_num);
    TRACE_COUNTER_INCREMENT("input_segments_num", _input_num_segments);

    _output_version =
            Version(_input_rowsets.front()->start_version(), _input_rowsets.back()->end_version());

    _oldest_write_timestamp = _input_rowsets.front()->oldest_write_timestamp();
    _newest_write_timestamp = _input_rowsets.back()->newest_write_timestamp();

    std::vector<RowsetMetaSharedPtr> rowset_metas(_input_rowsets.size());
    std::transform(_input_rowsets.begin(), _input_rowsets.end(), rowset_metas.begin(),
                   [](const RowsetSharedPtr& rowset) { return rowset->rowset_meta(); });
    _cur_tablet_schema =
            _tablet->rowset_meta_with_max_schema_version(rowset_metas)->tablet_schema();
}

bool Compaction::handle_ordered_data_compaction() {
    if (!config::enable_ordered_data_compaction) {
        return false;
    }
    if (_tablet->keys_type() == KeysType::UNIQUE_KEYS &&
        _tablet->enable_unique_key_merge_on_write()) {
        return false;
    }
    // check delete version: if compaction type is base compaction and
    // has a delete version, use original compaction
    if (compaction_type() == ReaderType::READER_BASE_COMPACTION) {
        for (auto rowset : _input_rowsets) {
            if (_tablet->version_for_delete_predicate(rowset->version())) {
                return false;
            }
        }
    }

    // check if rowsets are tidy so we can just modify meta and do link
    // files to handle compaction
    auto input_size = _input_rowsets.size();
    std::string pre_max_key;
    for (auto i = 0; i < input_size; ++i) {
        if (!is_rowset_tidy(pre_max_key, _input_rowsets[i])) {
            if (i <= input_size / 2) {
                return false;
            } else {
                _input_rowsets.resize(i);
                break;
            }
        }
    }
    // most rowset of current compaction is nonoverlapping
    // just handle nonoverlappint rowsets
    auto st = do_compact_ordered_rowsets();
    if (!st.ok()) {
        return false;
    }
    return true;
}

Status Compaction::do_compaction_impl(int64_t permits) {
    OlapStopWatch watch;

    auto use_vectorized_compaction = config::enable_vectorized_compaction;
    string merge_type = use_vectorized_compaction ? "v" : "";

    if (handle_ordered_data_compaction()) {
        RETURN_NOT_OK(update_tablet_meta());
        TRACE("modify rowsets finished");

        int64_t now = UnixMillis();
        if (compaction_type() == ReaderType::READER_CUMULATIVE_COMPACTION) {
            _tablet->set_last_cumu_compaction_success_time(now);
        } else {
            _tablet->set_last_base_compaction_success_time(now);
        }
        auto cumu_policy = _tablet->cumulative_compaction_policy();
        LOG(INFO) << "succeed to do ordered data " << merge_type << compaction_name()
                  << ". tablet=" << _tablet->full_name() << ", output_version=" << _output_version
                  << ", disk=" << _tablet->data_dir()->path()
                  << ", segments=" << _input_num_segments << ", input_row_num=" << _input_row_num
                  << ", output_row_num=" << _output_rowset->num_rows()
                  << ". elapsed time=" << watch.get_elapse_second()
                  << "s. cumulative_compaction_policy="
                  << (cumu_policy == nullptr ? "quick" : cumu_policy->name());
        return Status::OK();
    }
    build_basic_info();

    LOG(INFO) << "start " << merge_type << compaction_name() << ". tablet=" << _tablet->full_name()
              << ", output_version=" << _output_version << ", permits: " << permits;
    bool vertical_compaction = should_vertical_compaction();

	  // construct output rowset writer
    RowsetWriterContext context;
    context.txn_id = boost::uuids::hash_value(UUIDGenerator::instance()->next_uuid()) &
                     std::numeric_limits<int64_t>::max(); // MUST be positive
    context.txn_expiration = _expiration;
    context.version = _output_version;
    context.rowset_state = VISIBLE;
    context.segments_overlap = NONOVERLAPPING;
    context.tablet_schema = _cur_tablet_schema;
    context.oldest_write_timestamp = _oldest_write_timestamp;
    context.newest_write_timestamp = _newest_write_timestamp;
#ifdef CLOUD_MODE
    context.fs = cloud::latest_fs();
#endif
    if (use_vectorized_compaction && config::enable_index_compaction &&
        ((_tablet->keys_type() == KeysType::UNIQUE_KEYS ||
          _tablet->keys_type() == KeysType::DUP_KEYS))) {
        for (auto& index : _cur_tablet_schema->indexes()) {
            if (index.index_type() == IndexType::INVERTED) {
                auto unique_id = index.col_unique_ids()[0];
                if (field_is_slice_type(_cur_tablet_schema->column_by_uid(unique_id).type())) {
                    context.skip_inverted_index.insert(unique_id);
                }
            }
        }
    }
    if (vertical_compaction) {
        RETURN_IF_ERROR(_tablet->create_vertical_rowset_writer(context, &_output_rs_writer));
    }
    RETURN_IF_ERROR(_tablet->create_rowset_writer(context, &_output_rs_writer));
#ifdef CLOUD_MODE
    RETURN_IF_ERROR(cloud::meta_mgr()->prepare_rowset(_output_rs_writer->rowset_meta(), true));
#endif

		RETURN_NOT_OK(construct_input_rowset_readers());
    TRACE("prepare finished");

    // 2. write merged rows to output rowset
    // The test results show that merger is low-memory-footprint, there is no need to tracker its mem pool
    Merger::Statistics stats;
    Status res;
    if ( context.skip_inverted_index.size() > 0 ||
        (_tablet->keys_type() == KeysType::UNIQUE_KEYS &&
         _tablet->enable_unique_key_merge_on_write())) {
        stats.rowid_conversion = &_rowid_conversion;
    }

    if (use_vectorized_compaction) {
        if (vertical_compaction) {
            res = Merger::vertical_merge_rowsets(_tablet, compaction_type(), _cur_tablet_schema,
                                                 _input_rs_readers, _output_rs_writer.get(),
                                                 get_avg_segment_rows(), &stats);
        } else {
            res = Merger::vmerge_rowsets(_tablet, compaction_type(), _cur_tablet_schema,
                                         _input_rs_readers, _output_rs_writer.get(), &stats);
        }
    } else {
        res = Merger::merge_rowsets(_tablet, compaction_type(), _cur_tablet_schema,
                                    _input_rs_readers, _output_rs_writer.get(), &stats);
    }

    if (!res.ok()) {
        LOG(WARNING) << "fail to do " << merge_type << compaction_name() << ". res=" << res
                     << ", tablet=" << _tablet->full_name()
                     << ", output_version=" << _output_version;
        return res;
    }
    TRACE("merge rowsets finished");
    TRACE_COUNTER_INCREMENT("merged_rows", stats.merged_rows);
    TRACE_COUNTER_INCREMENT("filtered_rows", stats.filtered_rows);

    _output_rowset = _output_rs_writer->build();
    TRACE("build output_rowset finished");
    if (_output_rowset == nullptr) {
        LOG(WARNING) << "rowset writer build failed. writer version:"
                     << ", output_version=" << _output_version;
        return Status::OLAPInternalError(OLAP_ERR_ROWSET_BUILDER_INIT);
    }
#ifdef CLOUD_MODE
    RETURN_IF_ERROR(cloud::meta_mgr()->commit_rowset(_output_rowset->rowset_meta(), true));
#endif
    TRACE_COUNTER_INCREMENT("output_rowset_data_size", _output_rowset->data_disk_size());
    TRACE_COUNTER_INCREMENT("output_row_num", _output_rowset->num_rows());
    TRACE_COUNTER_INCREMENT("output_segments_num", _output_rowset->num_segments());
    TRACE("output rowset built");

    // 3. check correctness
    RETURN_NOT_OK(check_correctness(stats));
    TRACE("check correctness finished");
    if (_input_row_num > 0 && stats.rowid_conversion && use_vectorized_compaction &&
        config::enable_index_compaction) {
        OlapStopWatch inverted_watch;
        // translation vec
        // <<dest_idx_num, desc_docId>>
        std::vector<std::vector<std::pair<uint32_t, uint32_t>>> trans_vec =
                stats.rowid_conversion->get_rowid_conversion_map();

        // source rowset,segment -> index_id
        std::map<std::pair<RowsetId, uint32_t>, uint32_t> src_seg_to_id_map =
                stats.rowid_conversion->get_src_segment_to_id_map();
        // dest rowset id
        RowsetId dest_rowset_id = stats.rowid_conversion->get_dst_rowset_id();
        // dest segment id -> num rows
        std::vector<uint32_t> dest_segment_num_rows;
        RETURN_IF_ERROR(_output_rs_writer->get_segment_num_rows(&dest_segment_num_rows));

        auto src_segment_num = src_seg_to_id_map.size();
        auto dest_segment_num = dest_segment_num_rows.size();

        // src index files
        // format: rowsetId_segmentId
        std::vector<std::string> src_index_files(src_segment_num);
        for (auto m : src_seg_to_id_map) {
            std::pair<RowsetId, uint32_t> p = m.first;
            src_index_files[m.second] = p.first.to_string() + "_" + std::to_string(p.second);
        }

        // dest index files
        // format: rowsetId_segmentId
        std::vector<std::string> dest_index_files(dest_segment_num);
        for (int i = 0; i < dest_segment_num; ++i) {
            auto prefix = dest_rowset_id.to_string() + "_" + std::to_string(i);
            dest_index_files[i] = prefix;
        }

        // create index_writer to compaction indexes
        auto& fs = _output_rowset->rowset_meta()->fs();
        auto tablet_path = _output_rowset->tablet_path();

        DCHECK(dest_index_files.size() > 0);
        // we choose the first destination segment name as the temporary index writer path
        // Used to distinguish between different index compaction
        auto index_writer_path = tablet_path + "/" + dest_index_files[0];
        LOG(INFO) << "start index compaction"
                  << ". tablet=" << _tablet->full_name()
                  << ", source index size=" << src_segment_num
                  << ", destination index size=" << dest_segment_num << ".";
        std::for_each(context.skip_inverted_index.cbegin(), context.skip_inverted_index.cend(),
                      [this, &src_segment_num, &dest_segment_num, &index_writer_path, &src_index_files,
                       &dest_index_files, &fs, &tablet_path, &trans_vec, &dest_segment_num_rows](int32_t column_uniq_id) {
            compact_column(_cur_tablet_schema->get_inverted_index(column_uniq_id)->index_id(), src_segment_num, dest_segment_num, src_index_files,
                           dest_index_files, fs, index_writer_path, tablet_path, trans_vec,
                           dest_segment_num_rows);
        });

        LOG(INFO) << "succeed to do index compaction"
                  << ". tablet=" << _tablet->full_name() << ", input row number=" << _input_row_num
                  << ", output row number=" << _output_rowset->num_rows()
                  << ". elapsed time=" << inverted_watch.get_elapse_second() << "s.";
    }

    // 4. update and persistent tablet meta
    RETURN_NOT_OK(update_tablet_meta());
    TRACE("update tablet finished");

    // 5. update last success compaction time
    int64_t now = UnixMillis();
    // TODO(yingchun): do the judge in Tablet class
    if (compaction_type() == ReaderType::READER_CUMULATIVE_COMPACTION) {
        _tablet->set_last_cumu_compaction_success_time(now);
    } else {
        _tablet->set_last_base_compaction_success_time(now);
    }

#ifndef CLOUD_MODE
    int64_t current_max_version;
    {
        std::shared_lock rdlock(_tablet->get_header_lock());
        RowsetSharedPtr max_rowset = _tablet->rowset_with_max_version();
        if (max_rowset == nullptr) {
            current_max_version = -1;
        } else {
            current_max_version = _tablet->rowset_with_max_version()->end_version();
        }
    }

    auto cumu_policy = _tablet->cumulative_compaction_policy();
    LOG(INFO) << "succeed to do " << merge_type << compaction_name()
              << " is_vertical=" << vertical_compaction << ". tablet=" << _tablet->full_name()
              << ", output_version=" << _output_version
              << ", current_max_version=" << current_max_version
              << ", disk=" << _tablet->data_dir()->path() << ", segments=" << _input_num_segments
              << ", input_row_num=" << _input_row_num
              << ", output_row_num=" << _output_rowset->num_rows()
              << ". elapsed time=" << watch.get_elapse_second()
              << "s. cumulative_compaction_policy="
              << (cumu_policy == nullptr ? "quick" : cumu_policy->name())
              << ", compact_row_per_second=" << int(_input_row_num / watch.get_elapse_second());
#endif

    return Status::OK();
}

Status Compaction::construct_input_rowset_readers() {
    for (auto& rowset : _input_rowsets) {
        RowsetReaderSharedPtr rs_reader;
        RETURN_NOT_OK(rowset->create_reader(&rs_reader));
        _input_rs_readers.push_back(std::move(rs_reader));
    }
    return Status::OK();
}

Status Compaction::update_tablet_meta() {
    std::vector<RowsetSharedPtr> output_rowsets;
    output_rowsets.push_back(_output_rowset);
    {
        std::lock_guard<std::mutex> wrlock_(_tablet->get_rowset_update_lock());
        std::lock_guard<std::shared_mutex> wrlock(_tablet->get_header_lock());

        // update dst rowset delete bitmap
        if (_tablet->keys_type() == KeysType::UNIQUE_KEYS &&
            _tablet->enable_unique_key_merge_on_write()) {
            _tablet->tablet_meta()->update_delete_bitmap(
                    _input_rowsets, _output_rs_writer->version(), _rowid_conversion);
        }

        RETURN_NOT_OK(_tablet->modify_rowsets(output_rowsets, _input_rowsets, true));
    }
    {
        std::shared_lock rlock(_tablet->get_header_lock());
        _tablet->save_meta();
    }
    return Status::OK();
}

void Compaction::garbage_collection() {
    if (_state != CompactionState::SUCCESS && _output_rowset != nullptr) {
        StorageEngine::instance()->add_unused_rowset(_output_rowset);
    }
}

// Find the longest consecutive version path in "rowset", from beginning.
// Two versions before and after the missing version will be saved in missing_version,
// if missing_version is not null.
Status Compaction::find_longest_consecutive_version(std::vector<RowsetSharedPtr>* rowsets,
                                                    std::vector<Version>* missing_version) {
    if (rowsets->empty()) {
        return Status::OK();
    }
    RowsetSharedPtr prev_rowset = rowsets->front();
    size_t i = 1;
    for (; i < rowsets->size(); ++i) {
        RowsetSharedPtr rowset = (*rowsets)[i];
        if (rowset->start_version() != prev_rowset->end_version() + 1) {
            if (missing_version != nullptr) {
                missing_version->push_back(prev_rowset->version());
                missing_version->push_back(rowset->version());
            }
            break;
        }
        prev_rowset = rowset;
    }

    rowsets->resize(i);
    return Status::OK();
}

Status Compaction::check_version_continuity(const std::vector<RowsetSharedPtr>& rowsets) {
    if (rowsets.empty()) {
        return Status::OK();
    }
    RowsetSharedPtr prev_rowset = rowsets.front();
    for (size_t i = 1; i < rowsets.size(); ++i) {
        RowsetSharedPtr rowset = rowsets[i];
        if (rowset->start_version() != prev_rowset->end_version() + 1) {
            LOG(WARNING) << "There are missed versions among rowsets. "
                         << "prev_rowset version=" << prev_rowset->start_version() << "-"
                         << prev_rowset->end_version()
                         << ", rowset version=" << rowset->start_version() << "-"
                         << rowset->end_version();
            return Status::OLAPInternalError(OLAP_ERR_CUMULATIVE_MISS_VERSION);
        }
        prev_rowset = rowset;
    }

    return Status::OK();
}

Status Compaction::check_correctness(const Merger::Statistics& stats) {
    // 1. check row number
    int64_t read_rows = 0;
    // if some segments are pad segments, `read_rows` != `_input_row_num`
    std::vector<uint32_t> segment_num_rows;
    for (auto& rs_reader : _input_rs_readers) {
        rs_reader->get_segment_num_rows(&segment_num_rows);
        for (auto n : segment_num_rows) read_rows += n;
    }
    if (read_rows != _output_rowset->num_rows() + stats.merged_rows + stats.filtered_rows) {
        LOG(WARNING) << "row_num does not match between cumulative input and output! "
                     << "tablet=" << _tablet->full_name() << ", read_rows=" << read_rows
                     << ", merged_row_num=" << stats.merged_rows
                     << ", filtered_row_num=" << stats.filtered_rows
                     << ", output_row_num=" << _output_rowset->num_rows();
        return Status::OLAPInternalError(OLAP_ERR_CHECK_LINES_ERROR);
    }
    return Status::OK();
}

int64_t Compaction::get_compaction_permits() {
    int64_t permits = 0;
    for (auto rowset : _input_rowsets) {
        permits += rowset->rowset_meta()->get_compaction_score();
    }
    return permits;
}

#ifdef BE_TEST
void Compaction::set_input_rowset(const std::vector<RowsetSharedPtr>& rowsets) {
    _input_rowsets = rowsets;
}

RowsetSharedPtr Compaction::output_rowset() {
    return _output_rowset;
}
#endif
} // namespace doris
