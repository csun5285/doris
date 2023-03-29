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

#pragma once

#include "util/runtime_profile.h"
#include "vec/exec/scan/vscan_node.h"

namespace doris::vectorized {

class NewOlapScanner;
class NewOlapScanNode : public VScanNode {
public:
    NewOlapScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    friend class NewOlapScanner;

    Status prepare(RuntimeState* state) override;
    Status collect_query_statistics(QueryStatistics* statistics) override;

    void set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) override;

    std::string get_name() override;

protected:
    Status _init_profile() override;
    Status _process_conjuncts() override;
    bool _is_key_column(const std::string& col_name) override;

    Status _should_push_down_function_filter(VectorizedFnCall* fn_call, VExprContext* expr_ctx,
                                             StringVal* constant_str,
                                             doris_udf::FunctionContext** fn_ctx,
                                             PushDownType& pdt) override;

    PushDownType _should_push_down_bloom_filter() override { return PushDownType::ACCEPTABLE; }

    PushDownType _should_push_down_bitmap_filter() override { return PushDownType::ACCEPTABLE; }

    PushDownType _should_push_down_is_null_predicate() override { return PushDownType::ACCEPTABLE; }

    Status _init_scanners(std::list<VScanner*>* scanners) override;

    std::unique_ptr<vectorized::Block> _allocate_block(const TupleDescriptor* desc, size_t sz) override;
private:
    Status _build_key_ranges_and_filters();

    bool _maybe_prune_columns();

    bool is_pruned_column(int32_t col_unique_id);

    // iterate through conjuncts tre
    void _iterate_conjuncts_tree(const VExpr* conjunct_expr_root, std::function<void(const VExpr*)> fn);

    // get all slot ref column unique ids
    void _collect_conjuncts_slot_column_unique_ids(const VExpr* expr);

private:
    TOlapScanNode _olap_scan_node;
    std::vector<std::unique_ptr<TPaloScanRange>> _scan_ranges;
    OlapScanKeys _scan_keys;
    std::vector<TCondition> _olap_filters;
    // compound filters in every conjunct, like: "(a or b) and (c or d)", (a or b) in conjuct[0], (c or d) in conjuct[1]
    std::vector<std::vector<TCondition>> _compound_filters;

    std::set<int32_t> _pruned_column_ids;
    // If column id in this set, indicate that we need to read data after index filtering
    std::set<int32_t> _maybe_read_column_ids;

private:
    std::unique_ptr<RuntimeProfile> _segment_profile;

    RuntimeProfile::Counter* _num_disks_accessed_counter = nullptr;

    RuntimeProfile::Counter* _tablet_counter = nullptr;
    RuntimeProfile::Counter* _rows_pushed_cond_filtered_counter = nullptr;
    RuntimeProfile::Counter* _reader_init_timer = nullptr;
    RuntimeProfile::Counter* _scanner_init_timer = nullptr;
    RuntimeProfile::Counter* _process_conjunct_timer = nullptr;

    RuntimeProfile::Counter* _io_timer = nullptr;
    RuntimeProfile::Counter* _read_compressed_counter = nullptr;
    RuntimeProfile::Counter* _decompressor_timer = nullptr;
    RuntimeProfile::Counter* _read_uncompressed_counter = nullptr;
    RuntimeProfile::Counter* _raw_rows_counter = nullptr;

    RuntimeProfile::Counter* _rows_vec_cond_filtered_counter = nullptr;
    RuntimeProfile::Counter* _rows_short_circuit_cond_filtered_counter = nullptr;
    RuntimeProfile::Counter* _rows_vec_cond_input_counter = nullptr;
    RuntimeProfile::Counter* _rows_short_circuit_cond_input_counter = nullptr;
    RuntimeProfile::Counter* _vec_cond_timer = nullptr;
    RuntimeProfile::Counter* _short_cond_timer = nullptr;
    RuntimeProfile::Counter* _output_col_timer = nullptr;

    RuntimeProfile::Counter* _stats_filtered_counter = nullptr;
    RuntimeProfile::Counter* _bf_filtered_counter = nullptr;
    RuntimeProfile::Counter* _del_filtered_counter = nullptr;
    RuntimeProfile::Counter* _conditions_filtered_counter = nullptr;
    RuntimeProfile::Counter* _key_range_filtered_counter = nullptr;

    RuntimeProfile::Counter* _block_fetch_timer = nullptr;
    RuntimeProfile::Counter* _block_load_timer = nullptr;
    RuntimeProfile::Counter* _block_load_counter = nullptr;
    // Add more detail seek timer and counter profile
    // Read process is split into 3 stages: init, first read, lazy read
    RuntimeProfile::Counter* _block_init_timer = nullptr;
    RuntimeProfile::Counter* _block_init_seek_timer = nullptr;
    RuntimeProfile::Counter* _block_init_seek_counter = nullptr;
    RuntimeProfile::Counter* _block_conditions_filtered_timer = nullptr;
    RuntimeProfile::Counter* _first_read_timer = nullptr;
    RuntimeProfile::Counter* _first_read_seek_timer = nullptr;
    RuntimeProfile::Counter* _first_read_seek_counter = nullptr;
    RuntimeProfile::Counter* _lazy_read_timer = nullptr;
    RuntimeProfile::Counter* _lazy_read_seek_timer = nullptr;
    RuntimeProfile::Counter* _lazy_read_seek_counter = nullptr;

    RuntimeProfile::Counter* _block_convert_timer = nullptr;

    // total pages read
    // used by segment v2
    RuntimeProfile::Counter* _total_pages_num_counter = nullptr;
    // page read from cache
    // used by segment v2
    RuntimeProfile::Counter* _cached_pages_num_counter = nullptr;

    // row count filtered by bitmap inverted index
    RuntimeProfile::Counter* _bitmap_index_filter_counter = nullptr;
    // time fro bitmap inverted index read and filter
    RuntimeProfile::Counter* _bitmap_index_filter_timer = nullptr;

    RuntimeProfile::Counter* _inverted_index_filter_counter = nullptr;
    RuntimeProfile::Counter* _inverted_index_filter_timer = nullptr;

    RuntimeProfile::Counter* _output_index_return_column_timer = nullptr;

    // number of created olap scanners
    RuntimeProfile::Counter* _num_scanners = nullptr;

    // number of segment filtered by column stat when creating seg iterator
    RuntimeProfile::Counter* _filtered_segment_counter = nullptr;
    // total number of segment related to this scan node
    RuntimeProfile::Counter* _total_segment_counter = nullptr;

    // for debugging or profiling, record any info as you want
    RuntimeProfile::Counter* _general_debug_timer[GENERAL_DEBUG_COUNT] = {};

    // file cache
    RuntimeProfile::Counter* _num_local_io_total = nullptr;
    RuntimeProfile::Counter* _num_remote_io_total = nullptr;
    RuntimeProfile::Counter* _local_io_timer = nullptr;
    RuntimeProfile::Counter* _bytes_scanned_from_cache = nullptr;
    RuntimeProfile::Counter* _bytes_scanned_from_remote = nullptr;
    RuntimeProfile::Counter* _remote_io_timer = nullptr;
    RuntimeProfile::Counter* _write_cache_io_timer = nullptr;
    RuntimeProfile::Counter* _bytes_write_into_cache = nullptr;
    RuntimeProfile::Counter* _num_skip_cache_io_total = nullptr;

    RuntimeProfile::Counter* _cloud_get_rowset_version_timer = nullptr;
    RuntimeProfile::Counter* _load_segments_timer = nullptr;

    // Async io
    RuntimeProfile::Counter* _async_remote_total_use_timer_ns = nullptr;
    RuntimeProfile::Counter* _async_remote_task_wait_worker_timer_ns = nullptr;
    RuntimeProfile::Counter* _async_remote_task_wake_up_timer_ns = nullptr;
    RuntimeProfile::Counter* _async_remote_task_exec_timer_ns = nullptr;
    RuntimeProfile::Counter* _async_remote_task_total = nullptr;
    RuntimeProfile::Counter* _async_remote_wait_for_putting_queue = nullptr;

    RuntimeProfile::Counter* _async_local_total_use_timer_ns = nullptr;
    RuntimeProfile::Counter* _async_local_task_wait_worker_timer_ns = nullptr;
    RuntimeProfile::Counter* _async_local_task_wake_up_timer_ns = nullptr;
    RuntimeProfile::Counter* _async_local_task_exec_timer_ns = nullptr;
    RuntimeProfile::Counter* _async_local_task_total = nullptr;
    RuntimeProfile::Counter* _async_local_wait_for_putting_queue = nullptr;

};

} // namespace doris::vectorized
