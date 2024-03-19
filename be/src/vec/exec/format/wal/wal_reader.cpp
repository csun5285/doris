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

#include "wal_reader.h"

#include "common/logging.h"
#include "common/sync_point.h"
#include "gutil/strings/split.h"
#include "olap/wal/wal_manager.h"
#include "runtime/runtime_state.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_string.h"
#include "vec/exprs/vexpr_context.h"

namespace doris::vectorized {
WalReader::WalReader(RuntimeState* state) : _state(state) {
    _wal_id = state->wal_id();
}

WalReader::~WalReader() {
    if (_wal_reader.get() != nullptr) {
        static_cast<void>(_wal_reader->finalize());
    }
}

Status WalReader::init_reader(
        const TupleDescriptor* tuple_descriptor,
        std::unordered_map<std::string, vectorized::VExprContextSPtr>& col_default_value_ctx) {
    _tuple_descriptor = tuple_descriptor;
    _col_default_value_ctx = col_default_value_ctx;
    RETURN_IF_ERROR(_state->exec_env()->wal_mgr()->get_wal_path(_wal_id, _wal_path));
    _wal_reader = std::make_shared<doris::WalReader>(_wal_path);
    RETURN_IF_ERROR(_wal_reader->init());
    return Status::OK();
}

Status WalReader::get_next_block(Block* block, size_t* read_rows, bool* eof) {
    //read src block
    PBlock pblock;
    auto st = _wal_reader->read_block(pblock);
    if (st.is<ErrorCode::END_OF_FILE>()) {
        LOG(INFO) << "read eof on wal:" << _wal_path;
        *read_rows = 0;
        *eof = true;
        return Status::OK();
    }
    if (!st.ok()) {
        LOG(WARNING) << "Failed to read wal on path = " << _wal_path;
        return st;
    }
    vectorized::Block src_block;
    RETURN_IF_ERROR(src_block.deserialize(pblock));
    //convert to dst block
    vectorized::Block dst_block;
    int index = 0;
    size_t output_block_column_size = block->get_columns_with_type_and_name().size();
    TEST_SYNC_POINT_CALLBACK("WalReader::set_column_id_count", &_column_id_count);
    TEST_SYNC_POINT_CALLBACK("WalReader::set_out_block_column_size", &output_block_column_size);
    if (_column_id_count != src_block.columns() ||
        output_block_column_size != _tuple_descriptor->slots().size()) {
        return Status::InternalError(
                "not equal wal _column_id_count={} vs wal block columns size={}, "
                "output block columns size={} vs tuple_descriptor size={}",
                std::to_string(_column_id_count), std::to_string(src_block.columns()),
                std::to_string(output_block_column_size),
                std::to_string(_tuple_descriptor->slots().size()));
    }
    vectorized::ColumnPtr column_ptr = nullptr;
    for (auto slot_desc : _tuple_descriptor->slots()) {
        auto it = _column_pos_map.find(slot_desc->col_unique_id());
        if (it != _column_pos_map.end()) {
            if (it->second.empty()) {
                return Status::InternalError("pos list is empty");
            }
            auto pos = it->second.front();
            it->second.pop_front();
            if (pos >= src_block.columns()) {
                return Status::InternalError("read wal {} fail, pos {}, columns size {}", _wal_path,
                                             pos, src_block.columns());
            }
            column_ptr = src_block.get_by_position(pos).column;
            it->second.emplace_back(pos);
        } else {
            auto default_it = _col_default_value_ctx.find(slot_desc->col_name());
            if (default_it != _col_default_value_ctx.end()) {
                int result_column_id = -1;
                st = default_it->second->execute(&src_block, &result_column_id);
                column_ptr = src_block.get_by_position(result_column_id).column;
                column_ptr = column_ptr->convert_to_full_column_if_const();
            } else {
                return Status::InternalError("can't find default_value for column " +
                                             slot_desc->col_name());
            }
        }
        if (column_ptr != nullptr && slot_desc->is_nullable()) {
            column_ptr = make_nullable(column_ptr);
        }
        DataTypePtr data_type;
        RETURN_IF_CATCH_EXCEPTION(data_type = DataTypeFactory::instance().create_data_type(
                                          slot_desc->type(), slot_desc->is_nullable()));
        dst_block.insert(index, vectorized::ColumnWithTypeAndName(std::move(column_ptr), data_type,
                                                                  slot_desc->col_name()));
        index++;
    }
    block->swap(dst_block);
    *read_rows = block->rows();
    VLOG_DEBUG << "read block rows:" << *read_rows;
    return Status::OK();
}

Status WalReader::get_columns(std::unordered_map<std::string, TypeDescriptor>* name_to_type,
                              std::unordered_set<std::string>* missing_cols) {
    std::string col_ids;
    RETURN_IF_ERROR(_wal_reader->read_header(col_ids));
    std::vector<std::string> column_id_vector =
            strings::Split(col_ids, ",", strings::SkipWhitespace());
    _column_id_count = column_id_vector.size();
    try {
        int64_t pos = 0;
        for (auto col_id_str : column_id_vector) {
            auto col_id = std::strtoll(col_id_str.c_str(), NULL, 10);
            auto it = _column_pos_map.find(col_id);
            if (it == _column_pos_map.end()) {
                std::list<int64_t> list;
                list.emplace_back(pos);
                _column_pos_map.emplace(col_id, list);
            } else {
                it->second.emplace_back(pos);
            }
            pos++;
        }
    } catch (const std::invalid_argument& e) {
        return Status::InvalidArgument("Invalid format, {}", e.what());
    }
    return Status::OK();
}

} // namespace doris::vectorized