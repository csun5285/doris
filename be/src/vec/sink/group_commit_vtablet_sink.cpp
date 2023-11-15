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

#include "vec/sink/group_commit_vtablet_sink.h"

#include "olap/wal_manager.h"
#include "runtime/runtime_state.h"
#include "vec/core/future_block.h"

namespace doris {

namespace stream_load {

GroupCommitVOlapTableSink::GroupCommitVOlapTableSink(ObjectPool* pool,
                                                     const RowDescriptor& row_desc,
                                                     const std::vector<TExpr>& texprs,
                                                     Status* status)
        : VOlapTableSink(pool, row_desc, texprs, status) {}

Status GroupCommitVOlapTableSink::write_wal(vectorized::Block* block, RuntimeState* state,
                                            int64_t num_rows, int64_t filtered_rows,
                                            std::vector<char>& filter_bitmap) {
    PBlock pblock;
    size_t uncompressed_bytes = 0, compressed_bytes = 0;
    if (filtered_rows == 0) {
        block->get_columns_with_type_and_name();
        RETURN_IF_ERROR(block->serialize(state->be_exec_version(), &pblock, &uncompressed_bytes,
                                         &compressed_bytes, segment_v2::CompressionTypePB::SNAPPY));
        RETURN_IF_ERROR(_wal_writer->append_blocks(std::vector<PBlock*> {&pblock}));
    } else {
        auto cloneBlock = block->clone_without_columns();
        auto res_block = vectorized::MutableBlock::build_mutable_block(&cloneBlock);
        for (int i = 0; i < num_rows; ++i) {
            if (filter_bitmap[i]) {
                continue;
            }
            res_block.add_row(block, i);
        }
        RETURN_IF_ERROR(res_block.to_block().serialize(state->be_exec_version(), &pblock,
                                                       &uncompressed_bytes, &compressed_bytes,
                                                       segment_v2::CompressionTypePB::SNAPPY));
        RETURN_IF_ERROR(_wal_writer->append_blocks(std::vector<PBlock*> {&pblock}));
    }
    return Status::OK();
}
void GroupCommitVOlapTableSink::handle_block(vectorized::Block* input_block, int64_t rows,
                                             int64_t filter_rows, RuntimeState* state,
                                             vectorized::Block* output_block,
                                             std::vector<char>& filter_bitmap) {
    write_wal(output_block, state, rows, filter_rows, filter_bitmap);
#ifndef BE_TEST
    auto* future_block = dynamic_cast<vectorized::FutureBlock*>(input_block);
    std::unique_lock<doris::Mutex> l(*(future_block->lock));
    future_block->set_result(Status::OK(), rows, rows - filter_rows);
    future_block->cv->notify_all();
#else
#endif
}
Status GroupCommitVOlapTableSink::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(VOlapTableSink::prepare(state));
    RETURN_IF_ERROR(state->exec_env()->wal_mgr()->add_wal_path(_db_id, _tb_id, _wal_id,
                                                               state->import_label()));
    RETURN_IF_ERROR(state->exec_env()->wal_mgr()->create_wal_writer(_wal_id, _wal_writer));
    return Status::OK();
}

Status GroupCommitVOlapTableSink::close(RuntimeState* state, Status exec_status) {
    if (_wal_writer.get() != nullptr) {
        _wal_writer->finalize();
    }
    RETURN_IF_ERROR(VOlapTableSink::close(state, exec_status));
    return Status::OK();
}
Status GroupCommitVOlapTableSink::init(const TDataSink& t_sink) {
    RETURN_IF_ERROR(VOlapTableSink::init(t_sink));
    DCHECK(t_sink.__isset.olap_table_sink);
    auto& table_sink = t_sink.olap_table_sink;
    _db_id = table_sink.db_id;
    _tb_id = table_sink.table_id;
    _wal_id = table_sink.txn_id;
    return Status::OK();
}
Status GroupCommitVOlapTableSink::open(doris::RuntimeState* state) {
    RETURN_IF_ERROR(VOlapTableSink::open(state));
    return Status::OK();
}
} // namespace stream_load
} // namespace doris
