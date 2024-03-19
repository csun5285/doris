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
#include "exec/data_sink.h"
#include "vec/exprs/vexpr_fwd.h"
#include "vec/sink/vtablet_sink.h"
#include "vec/sink/vtablet_validator.h"

namespace doris {

class OlapTableSchemaParam;
class MemTracker;
class LoadBlockQueue;

namespace vectorized {

class GroupCommitBlockSink : public DataSink {
public:
    GroupCommitBlockSink(ObjectPool* pool, const RowDescriptor& row_desc,
                         const std::vector<TExpr>& texprs, Status* status);

    ~GroupCommitBlockSink() override;

    RuntimeProfile* profile() override { return _profile; }

    Status init(const TDataSink& sink) override;

    Status prepare(RuntimeState* state) override;

    Status open(RuntimeState* state) override;

    Status send(RuntimeState* state, vectorized::Block* block, bool eos = false) override;

    Status close(RuntimeState* state, Status close_status) override;

private:
    Status _add_block(RuntimeState* state, vectorized::Block* block);
    Status _add_blocks(RuntimeState* state, bool is_blocks_contain_all_load_data);
    size_t _calculate_estimated_wal_bytes(bool is_blocks_contain_all_load_data);
    void _remove_estimated_wal_bytes();

    // ObjectPool* _pool;
    const RowDescriptor& _input_row_desc;
    vectorized::VExprContextSPtrs _output_vexpr_ctxs;

    int _tuple_desc_id = -1;
    std::shared_ptr<OlapTableSchemaParam> _schema;

    RuntimeState* _state = nullptr;
    std::shared_ptr<MemTracker> _mem_tracker;
    RuntimeProfile* _profile = nullptr;
    // this is tuple descriptor of destination OLAP table
    TupleDescriptor* _output_tuple_desc = nullptr;
    std::unique_ptr<stream_load::OlapTableValidator> _validator = nullptr;

    int64_t _db_id;
    int64_t _table_id;
    int64_t _base_schema_version = 0;
    int32_t _column_num = 0;
    TGroupCommitMode::type _group_commit_mode;
    UniqueId _load_id;
    std::shared_ptr<LoadBlockQueue> _load_block_queue;
    // used to calculate if meet the max filter ratio
    std::vector<std::shared_ptr<vectorized::Block>> _blocks;
    bool _is_block_appended = false;
    double _max_filter_ratio = 0.0;
    std::vector<char> _filter_bitmap;

    // used for find_partition
    std::unique_ptr<VOlapTablePartitionParam> _vpartition = nullptr;
    size_t _estimated_wal_bytes = 0;
};

} // namespace vectorized
} // namespace doris
