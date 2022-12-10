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

#include "exec/rowid_fetcher.h"
#include "exec/tablet_info.h"           // DorisNodesInfo
#include "vec/core/block.h"             // Block
#include "runtime/exec_env.h"           // ExecEnv
#include "runtime/runtime_state.h"      // RuntimeState
#include "util/brpc_client_cache.h"     // BrpcClientCache

#include "gen_cpp/Types_types.h"
#include "gen_cpp/internal_service.pb.h"

namespace doris {

Status RowIDFetcher::init(DorisNodesInfo* nodes_info) {
    for (auto [node_id, node_info] : nodes_info->nodes_info()) {
        auto client = ExecEnv::GetInstance()
                    ->brpc_internal_client_cache()->get_client(node_info.host, node_info.brpc_port);
        if (!client) {
            LOG(WARNING) << "Get rpc stub failed, host=" << node_info.host
                     << ", port=" << node_info.brpc_port;
            return Status::InternalError("RowIDFetcher failed to init rpc client");
        }
        _stubs.push_back(client);
    } 
    return Status::OK();
}

static std::string format_rowid(const GlobalRowLoacation& location) {
    return fmt::format("{} {} {} {}", location.tablet_id,
                    location.row_location.rowset_id.to_string(),
                    location.row_location.segment_id, location.row_location.row_id);
}

PMultiGetRequest RowIDFetcher::init_fetch_request(const vectorized::ColumnString& row_ids) {
    PMultiGetRequest mget_req; 
    _tuple_desc->to_protobuf(mget_req.mutable_desc());
    for (auto slot : _tuple_desc->slots()) {
        slot->to_protobuf(mget_req.add_slots());
    }
    for (size_t i = 0; i < row_ids.size(); ++i) {
        PMultiGetRequest::RowId row_id;
        StringRef row_id_rep = row_ids.get_data_at(i);
        auto location =  reinterpret_cast<const GlobalRowLoacation*>(row_id_rep.data);
        row_id.set_tablet_id(location->tablet_id);
        row_id.set_rowset_id(location->row_location.rowset_id.to_string());
        row_id.set_segment_id(location->row_location.segment_id);
        row_id.set_ordinal_id(location->row_location.row_id);
        *mget_req.add_rowids() = std::move(row_id);
    }
    mget_req.set_be_exec_version(_st->be_exec_version());
    return mget_req;
}

Status RowIDFetcher::fetch(const vectorized::ColumnPtr& row_ids, vectorized::MutableBlock* res_block) {
    res_block->clear_column_data();
    vectorized::Block block(_tuple_desc->slots(), row_ids->size());
    vectorized::MutableBlock mblock(std::move(block));
    assert(!_stubs.empty());
    for (auto stub : _stubs) {
        // TODO we should make rpc parellel
        brpc::Controller cntl;
        cntl.set_timeout_ms(20000); // hard code

        PMultiGetRequest mget_req = init_fetch_request(
                assert_cast<const vectorized::ColumnString&>(*vectorized::remove_nullable(row_ids).get()));
        PMultiGetResponse mget_res; 
        stub->multiget_data(&cntl, &mget_req, &mget_res, nullptr);
        if (cntl.Failed()) {
            LOG(WARNING) << "Failed to fetch " << cntl.ErrorText();
            return Status::InternalError(cntl.ErrorText());
        }
        Status st(mget_res.status());
        if (!st.ok()) {
            LOG(WARNING) << "Failed to fetch " << st.get_error_msg();
            return st;
        }
        vectorized::Block partial_block(mget_res.block());
        mblock.merge(partial_block);
    }
    // final sort by row_ids sequence, since row_ids is already sorted
    vectorized::Block tmp = mblock.to_block();
    std::unordered_map<std::string, uint32_t> row_order;
    vectorized::ColumnPtr row_id_column = tmp.get_columns().back();
    for (size_t x = 0; x < row_id_column->size(); ++ x) {
        auto location =  reinterpret_cast<const GlobalRowLoacation*>(row_id_column->get_data_at(x).data);
        row_order[format_rowid(*location)] = x;
    }
    for (size_t x = 0; x < row_ids->size(); ++x) {
        auto location =  reinterpret_cast<const GlobalRowLoacation*>(row_ids->get_data_at(x).data);
        res_block->add_row(&tmp, row_order[format_rowid(*location)]);
    }
    return Status::OK();
}

}
