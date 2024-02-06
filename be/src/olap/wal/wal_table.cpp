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

#include "olap/wal/wal_table.h"

#include <thrift/protocol/TDebugProtocol.h>

#include "gutil/strings/split.h"
#include "http/action/stream_load.h"
#include "http/http_common.h"
#include "http/utils.h"
#include "io/fs/local_file_system.h"
#include "io/fs/stream_load_pipe.h"
#include "olap/wal/wal_manager.h"
#include "runtime/client_cache.h"
#include "runtime/fragment_mgr.h"
#include "runtime/plan_fragment_executor.h"
#include "util/path_util.h"
#include "util/thrift_rpc_helper.h"

namespace doris {
bvar::Adder<uint64_t> wal_fail("group_commit_wal_fail");
WalTable::WalTable(ExecEnv* exec_env, int64_t db_id, int64_t table_id)
        : _exec_env(exec_env), _db_id(db_id), _table_id(table_id) {
    _stream_load_action = std::make_shared<StreamLoadAction>(exec_env);
}
WalTable::~WalTable() {}

#ifdef BE_TEST
Status k_stream_load_exec_status;
#endif

void WalTable::add_wal(int64_t wal_id, std::string wal) {
    std::lock_guard<std::mutex> lock(_replay_wal_lock);
    LOG(INFO) << "add replay wal=" << wal;
    auto wal_info = std::make_shared<WalInfo>(wal_id, wal, 0, UnixMillis());
    _replay_wal_map.emplace(wal, wal_info);
}

void WalTable::_pick_relay_wals() {
    std::lock_guard<std::mutex> lock(_replay_wal_lock);
    std::vector<std::string> need_replay_wals;
    std::vector<std::string> need_erase_wals;
    for (const auto& [wal_path, wal_info] : _replay_wal_map) {
        if (wal_info->get_retry_num() >= config::group_commit_replay_wal_retry_num) {
            LOG(WARNING) << "failed to replay wal=" << wal_path << " after retry "
                         << wal_info->get_retry_num() << " times";
            [[maybe_unused]] auto st = _exec_env->wal_mgr()->rename_to_tmp_path(
                    wal_path, _table_id, wal_info->get_wal_id());
            if (config::group_commit_wait_replay_wal_finish) {
                auto notify_st = _exec_env->wal_mgr()->notify_relay_wal(wal_info->get_wal_id());
                if (!notify_st.ok()) {
                    LOG(WARNING) << "notify wal " << wal_info->get_wal_id() << " fail";
                }
            }
            need_erase_wals.push_back(wal_path);
            continue;
        }
        if (_need_replay(wal_info)) {
            need_replay_wals.push_back(wal_path);
        }
    }
    for (const auto& wal : need_erase_wals) {
        _replay_wal_map.erase(wal);
    }
    std::sort(need_replay_wals.begin(), need_replay_wals.end());
    for (const auto& wal : need_replay_wals) {
        _replaying_queue.emplace_back(_replay_wal_map[wal]);
        _replay_wal_map.erase(wal);
    }
}

Status WalTable::_relay_wal_one_by_one() {
    std::vector<std::shared_ptr<WalInfo>> need_retry_wals;
    std::vector<std::shared_ptr<WalInfo>> need_delete_wals;
    for (auto wal_info : _replaying_queue) {
        wal_info->add_retry_num();
        auto st = _replay_wal_internal(wal_info->get_wal_path());
        if (!st.ok()) {
            doris::wal_fail << 1;
            LOG(WARNING) << "failed to replay wal=" << wal_info->get_wal_path()
                         << ", st=" << st.to_string();
            if (!st.is<ErrorCode::NOT_FOUND>()) {
                need_retry_wals.push_back(wal_info);
            } else {
                need_delete_wals.push_back(wal_info);
            }
        } else {
            LOG(INFO) << "succeed to replay wal=" << wal_info->get_wal_path()
                      << ", st=" << st.to_string();
            need_delete_wals.push_back(wal_info);
        }
    }
    {
        std::lock_guard<std::mutex> lock(_replay_wal_lock);
        _replaying_queue.clear();
        for (auto retry_wal_info : need_retry_wals) {
            _replay_wal_map.emplace(retry_wal_info->get_wal_path(), retry_wal_info);
        }
    }
    for (auto delete_wal_info : need_delete_wals) {
        [[maybe_unused]] auto st =
                _exec_env->wal_mgr()->delete_wal(_table_id, delete_wal_info->get_wal_id());
        if (config::group_commit_wait_replay_wal_finish) {
            RETURN_IF_ERROR(_exec_env->wal_mgr()->notify_relay_wal(delete_wal_info->get_wal_id()));
        }
    }
    return Status::OK();
}

Status WalTable::replay_wals() {
    {
        std::lock_guard<std::mutex> lock(_replay_wal_lock);
        if (_replay_wal_map.empty()) {
            LOG(INFO) << "_replay_wal_map is empty, skip relaying for table_id=" << _table_id;
            return Status::OK();
        }
        if (!_replaying_queue.empty()) {
            LOG(INFO) << "_replaying_queue is not empty, skip relaying for table_id=" << _table_id;
            return Status::OK();
        }
    }
    VLOG_DEBUG << "Start replay wals for db=" << _db_id << ", table=" << _table_id
               << ", wal size=" << _replay_wal_map.size();
    _pick_relay_wals();
    RETURN_IF_ERROR(_relay_wal_one_by_one());
    return Status::OK();
}

bool WalTable::_need_replay(std::shared_ptr<WalInfo> wal_info) {
    if (config::group_commit_wait_replay_wal_finish) {
        return true;
    }
#ifndef BE_TEST
    auto replay_interval = pow(2, wal_info->get_retry_num()) *
                           config::group_commit_replay_wal_retry_interval_seconds * 1000;
    return UnixMillis() - wal_info->get_start_time_ms() >= replay_interval;
#else
    return true;
#endif
}

Status WalTable::_try_abort_txn(int64_t db_id, std::string& label) {
    TLoadTxnRollbackRequest request;
    request.__set_auth_code(0); // this is a fake, fe not check it now
    request.__set_db_id(db_id);
    request.__set_label(label);
    std::string reason = "relay wal with label " + label;
    request.__set_reason(reason);
    TLoadTxnRollbackResult result;
    TNetworkAddress master_addr = _exec_env->master_info()->network_address;
    auto st = ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&request, &result](FrontendServiceConnection& client) {
                client->loadTxnRollback(result, request);
            },
            10000L);
    auto result_status = Status::create<false>(result.status);
    LOG(INFO) << "abort label " << label << ", st:" << st << ", result_status:" << result_status;
    return result_status;
}

Status WalTable::_replay_wal_internal(const std::string& wal) {
    LOG(INFO) << "start replay wal=" << wal;
    int64_t version = -1;
    int64_t backend_id = -1;
    int64_t wal_id = -1;
    std::string label = "";
    io::Path wal_path = wal;
    auto file_name = wal_path.filename().string();
    RETURN_IF_ERROR(WalManager::parse_wal_path(file_name, version, backend_id, wal_id, label));
#ifndef BE_TEST
    if (!config::group_commit_wait_replay_wal_finish) {
        [[maybe_unused]] auto st = _try_abort_txn(_db_id, label);
    }
#endif
    return _replay_one_txn_with_stremaload(wal_id, wal, label);
}

Status WalTable::_handle_stream_load(int64_t wal_id, const std::string& wal,
                                     const std::string& label) {
    std::shared_ptr<StreamLoadContext> ctx = std::make_shared<StreamLoadContext>(_exec_env);
    ctx->wal_id = wal_id;
    ctx->label = label;
    ctx->table_id = _table_id;
    ctx->auth.auth_code = wal_id;
    ctx->group_commit = false;
    ctx->format = TFileFormatType::FORMAT_WAL;
    ctx->load_type = TLoadType::MANUL_LOAD;
    ctx->load_src_type = TLoadSourceType::RAW;
    RETURN_IF_ERROR(_exec_env->stream_load_executor()->begin_txn(ctx.get()));
    auto st = _stream_load_action->process_put(nullptr, ctx);
    if (st.ok()) {
        // wait stream load finish
        RETURN_IF_ERROR(ctx->future.get());
        if (ctx->status.ok()) {
            st = _exec_env->stream_load_executor()->commit_txn(ctx.get());
        } else {
            st = ctx->status;
        }
    }
    if (!st.ok()) {
        _exec_env->stream_load_executor()->rollback_txn(ctx.get());
    }
    return st;
}

Status WalTable::_replay_one_txn_with_stremaload(int64_t wal_id, const std::string& wal,
                                                 const std::string& label) {
    bool success = false;
#ifndef BE_TEST
    auto st = _handle_stream_load(wal_id, wal, label);
    auto msg = st.msg();
    success = st.ok() || st.is<ErrorCode::PUBLISH_TIMEOUT>() ||
              st.is<ErrorCode::LABEL_ALREADY_EXISTS>();
    LOG(INFO) << "handle_stream_load:" << st.to_string();
#else
    success = k_stream_load_exec_status.ok();
    auto st = Status::OK();
#endif
    return success ? Status::OK() : st;
}

void WalTable::stop() {
    do {
        {
            std::lock_guard<std::mutex> lock(_replay_wal_lock);
            if (_replay_wal_map.empty() && _replaying_queue.empty()) {
                break;
            }
            LOG(INFO) << "stopping wal_table,wait for relay wal task done, now "
                      << _replay_wal_map.size() << " wals wait to replay, "
                      << _replaying_queue.size() << " wals are replaying";
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }
    } while (true);
}

size_t WalTable::size() {
    std::lock_guard<std::mutex> lock(_replay_wal_lock);
    return _replay_wal_map.size() + _replaying_queue.size();
}

} // namespace doris