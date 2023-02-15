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

#include "vec/exec/vexchange_node.h"

#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "vec/runtime/vdata_stream_mgr.h"
#include "vec/runtime/vdata_stream_recvr.h"
#include "exec/rowid_fetcher.h"
#include "common/consts.h"
#include "util/defer_op.h"

namespace doris::vectorized {
VExchangeNode::VExchangeNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs),
          _num_senders(0),
          _is_merging(tnode.exchange_node.__isset.sort_info),
          _stream_recvr(nullptr),
          _input_row_desc(descs, tnode.exchange_node.input_row_tuples,
                          std::vector<bool>(tnode.nullable_tuples.begin(),
                                            tnode.nullable_tuples.begin() +
                                                    tnode.exchange_node.input_row_tuples.size())),
          _offset(tnode.exchange_node.__isset.offset ? tnode.exchange_node.offset : 0),
          _num_rows_skipped(0) {}

Status VExchangeNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    if (!_is_merging) {
        return Status::OK();
    }

    RETURN_IF_ERROR(_vsort_exec_exprs.init(tnode.exchange_node.sort_info, _pool));
    _is_asc_order = tnode.exchange_node.sort_info.is_asc_order;
    _nulls_first = tnode.exchange_node.sort_info.nulls_first;

    if (tnode.exchange_node.__isset.nodes_info) {
        _nodes_info = _pool->add(new DorisNodesInfo(tnode.exchange_node.nodes_info));
    }
    _use_two_phase_read = tnode.exchange_node.sort_info.use_two_phase_read;
    return Status::OK();
}

Status VExchangeNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::prepare(state));
    DCHECK_GT(_num_senders, 0);
    _sub_plan_query_statistics_recvr.reset(new QueryStatisticsRecvr());
    _stream_recvr = state->exec_env()->vstream_mgr()->create_recvr(
            state, _input_row_desc, state->fragment_instance_id(), _id, _num_senders,
            config::exchg_node_buffer_size_bytes, _runtime_profile.get(), _is_merging,
            _sub_plan_query_statistics_recvr);

    if (_is_merging) {
        RETURN_IF_ERROR(_vsort_exec_exprs.prepare(state, _row_descriptor, _row_descriptor));
    }
    return Status::OK();
}
Status VExchangeNode::open(RuntimeState* state) {
    START_AND_SCOPE_SPAN(state->get_tracer(), span, "VExchangeNode::open");
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::open(state));

    if (_is_merging) {
        RETURN_IF_ERROR(_vsort_exec_exprs.open(state));
        RETURN_IF_ERROR(_stream_recvr->create_merger(_vsort_exec_exprs.lhs_ordering_expr_ctxs(),
                                                     _is_asc_order, _nulls_first,
                                                     state->batch_size(), _limit, _offset));
    }

    return Status::OK();
}
Status VExchangeNode::get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) {
    return Status::NotSupported("Not Implemented VExchange Node::get_next scalar");
}

Status VExchangeNode::second_phase_fetch_data(RuntimeState* state, Block* final_block) {
    if (!_use_two_phase_read) {
        return Status::OK();
    }
    if (final_block->rows() == 0) {
        return Status::OK();
    }
    auto row_id_col = final_block->get_by_position(final_block->columns() - 1);
    MonotonicStopWatch watch;
    watch.start();
    auto tuple_desc = _row_descriptor.tuple_descriptors()[0];
    RowIDFetcher id_fetcher(tuple_desc, state);
    RETURN_IF_ERROR(id_fetcher.init(_nodes_info));
    vectorized::Block materialized_block(tuple_desc->slots(), final_block->rows());
    auto tmp_block = MutableBlock::build_mutable_block(&materialized_block);
    // fetch will sort block by sequence of ROWID_COL
    RETURN_IF_ERROR(id_fetcher.fetch(row_id_col.column, &tmp_block));
    materialized_block.swap(tmp_block.to_block());

    // materialize by name
    for (auto& column_type_name : *final_block) {
        auto materialized_column = materialized_block.try_get_by_name(column_type_name.name);
        if (materialized_column != nullptr) {
            column_type_name.column = std::move(materialized_column->column);
        }
    }
    LOG(INFO) << "fetch_id finished, cost(ms):" << watch.elapsed_time() / 1000 / 1000;
    return Status::OK();
}


Status VExchangeNode::get_next(RuntimeState* state, Block* block, bool* eos) {
    INIT_AND_SCOPE_GET_NEXT_SPAN(state->get_tracer(), _get_next_span, "VExchangeNode::get_next");
    SCOPED_TIMER(runtime_profile()->total_time_counter());
    auto status = _stream_recvr->get_next(block, eos);
    if (block != nullptr) {
        if (!_is_merging) {
            if (_num_rows_skipped + block->rows() < _offset) {
                _num_rows_skipped += block->rows();
                block->set_num_rows(0);
            } else if (_num_rows_skipped < _offset) {
                auto offset = _offset - _num_rows_skipped;
                _num_rows_skipped = _offset;
                block->set_num_rows(block->rows() - offset);
            }
        }
        if (_num_rows_returned + block->rows() < _limit) {
            _num_rows_returned += block->rows();
        } else {
            *eos = true;
            auto limit = _limit - _num_rows_returned;
            block->set_num_rows(limit);
        }
        COUNTER_SET(_rows_returned_counter, _num_rows_returned);
    }
    RETURN_IF_ERROR(second_phase_fetch_data(state, block)); 
    return status;
}

Status VExchangeNode::collect_query_statistics(QueryStatistics* statistics) {
    RETURN_IF_ERROR(ExecNode::collect_query_statistics(statistics));
    statistics->merge(_sub_plan_query_statistics_recvr.get());
    return Status::OK();
}

Status VExchangeNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    START_AND_SCOPE_SPAN(state->get_tracer(), span, "VExchangeNode::close");

    if (_stream_recvr != nullptr) {
        _stream_recvr->close();
    }
    if (_is_merging) _vsort_exec_exprs.close(state);

    return ExecNode::close(state);
}

} // namespace doris::vectorized
