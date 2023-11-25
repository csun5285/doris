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
#include "olap/wal_writer.h"
#include "vtablet_sink.h"

namespace doris {

namespace stream_load {

class GroupCommitVOlapTableSink : public VOlapTableSink {
public:
    GroupCommitVOlapTableSink(ObjectPool* pool, const RowDescriptor& row_desc,
                              const std::vector<TExpr>& texprs, Status* status);

    Status write_wal(vectorized::Block* block, RuntimeState* state, int64_t num_rows,
                     int64_t filtered_rows, std::vector<char>& filter_bitmap);

    void handle_block(vectorized::Block* input_block, int64_t rows, int64_t filter_rows,
                      RuntimeState* state, vectorized::Block* output_block,
                      std::vector<char>& filter_bitmap) override;
    Status init(const TDataSink& sink) override;
    Status prepare(RuntimeState* state) override;
    Status close(RuntimeState* state, Status exec_status) override;
    Status open(RuntimeState* state) override;

private:
    std::shared_ptr<WalWriter> _wal_writer = nullptr;
    int64_t _tb_id;
    int64_t _db_id;
    int64_t _wal_id;
};

} // namespace stream_load
} // namespace doris
