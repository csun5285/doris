#pragma once

#include <brpc/controller.h>

#include <memory>
#include <string>
#include <string_view>

#include "common/bvars.h"
#include "common/stopwatch.h"
#include "common/util.h"
#include "gen_cpp/selectdb_cloud.pb.h"
#include "meta-service/keys.h"
#include "meta-service/txn_kv.h"
#include "resource-manager/resource_manager.h"

namespace selectdb {

template <class Request>
void begin_rpc(std::string_view func_name, brpc::Controller* ctrl, const Request* req) {
    if constexpr (std::is_same_v<Request, CreateRowsetRequest>) {
        LOG(INFO) << "begin " << func_name << " from " << ctrl->remote_side();
    } else if constexpr (std::is_same_v<Request, CreateTabletsRequest>) {
        LOG(INFO) << "begin " << func_name << " from " << ctrl->remote_side();
    } else if constexpr (std::is_same_v<Request, UpdateDeleteBitmapRequest>) {
        LOG(INFO) << "begin " << func_name << " from " << ctrl->remote_side()
                  << " tablet_id=" << req->tablet_id() << " lock_id=" << req->lock_id()
                  << " initiator=" << req->initiator()
                  << " delete_bitmap_size=" << req->segment_delete_bitmaps_size();
    } else if constexpr (std::is_same_v<Request, GetDeleteBitmapRequest>) {
        LOG(INFO) << "begin " << func_name << " from " << ctrl->remote_side()
                  << " tablet_id=" << req->tablet_id() << " rowset_size=" << req->rowset_ids_size();
    } else if constexpr (std::is_same_v<Request, GetTabletStatsRequest>) {
        VLOG_DEBUG << "begin " << func_name << " from " << ctrl->remote_side()
                   << " tablet size: " << req->tablet_idx().size();
    } else if constexpr (std::is_same_v<Request, GetVersionRequest> ||
            std::is_same_v<Request, GetRowsetRequest> ||
            std::is_same_v<Request, GetTabletRequest>) {
        VLOG_DEBUG << "begin " << func_name << " from " << ctrl->remote_side()
                   << " request=" << req->ShortDebugString();
    } else {
        LOG(INFO) << "begin " << func_name << " from " << ctrl->remote_side()
                  << " request=" << req->ShortDebugString();
    }
}

template <class Response>
void finish_rpc(std::string_view func_name, brpc::Controller* ctrl, Response* res) {
    if constexpr (std::is_same_v<Response, CommitTxnResponse>) {
        if (res->status().code() != MetaServiceCode::OK) {
            res->clear_table_ids();
            res->clear_partition_ids();
            res->clear_versions();
        }
        LOG(INFO) << "finish " << func_name << " from " << ctrl->remote_side()
                  << " response=" << res->ShortDebugString();
    } else if constexpr (std::is_same_v<Response, GetRowsetResponse>) {
        if (res->status().code() != MetaServiceCode::OK) {
            res->clear_rowset_meta();
        }
        VLOG_DEBUG << "finish " << func_name << " from " << ctrl->remote_side()
                   << " status=" << res->status().ShortDebugString();
    } else if constexpr (std::is_same_v<Response, GetTabletStatsResponse>) {
        VLOG_DEBUG << "finish " << func_name << " from " << ctrl->remote_side()
                   << " status=" << res->status().ShortDebugString()
                   << " tablet size: " << res->tablet_stats().size();
    } else if constexpr (std::is_same_v<Response, GetVersionResponse> ||
            std::is_same_v<Response, GetTabletResponse>) {
        VLOG_DEBUG << "finish " << func_name << " from " << ctrl->remote_side()
                   << " response=" << res->ShortDebugString();
    } else if constexpr (std::is_same_v<Response, GetDeleteBitmapResponse>) {
        LOG(INFO) << "finish " << func_name << " from " << ctrl->remote_side()
                  << " status=" << res->status().ShortDebugString()
                  << " delete_bitmap_size=" << res->segment_delete_bitmaps_size();

    } else {
        LOG(INFO) << "finish " << func_name << " from " << ctrl->remote_side()
                  << " response=" << res->ShortDebugString();
    }
}

#define RPC_PREPROCESS(func_name)                                                        \
    StopWatch sw;                                                                        \
    auto ctrl = static_cast<brpc::Controller*>(controller);                              \
    begin_rpc(#func_name, ctrl, request);                                                \
    brpc::ClosureGuard closure_guard(done);                                              \
    [[maybe_unused]] int ret = 0;                                                        \
    [[maybe_unused]] std::stringstream ss;                                               \
    [[maybe_unused]] MetaServiceCode code = MetaServiceCode::OK;                         \
    [[maybe_unused]] std::string msg;                                                    \
    [[maybe_unused]] std::string instance_id;                                            \
    [[maybe_unused]] bool drop_request = false;                                          \
    std::unique_ptr<int, std::function<void(int*)>> defer_status((int*)0x01, [&](int*) { \
        response->mutable_status()->set_code(code);                                      \
        response->mutable_status()->set_msg(msg);                                        \
        finish_rpc(#func_name, ctrl, response);                                          \
        closure_guard.reset(nullptr);                                                    \
        if (config::use_detailed_metrics && !instance_id.empty() && !drop_request) {     \
            g_bvar_ms_##func_name.put(instance_id, sw.elapsed_us());                     \
        }                                                                                \
    });

#define RPC_RATE_LIMIT(func_name)                                                            \
    if (config::enable_rate_limit && config::use_detailed_metrics && !instance_id.empty()) { \
        auto rate_limiter = rate_limiter_->get_rpc_rate_limiter(#func_name);                 \
        assert(rate_limiter != nullptr);                                                     \
        std::function<int()> get_bvar_qps = [&] {                                            \
            return g_bvar_ms_##func_name.get(instance_id)->qps();                            \
        };                                                                                   \
        if (!rate_limiter->get_qps_token(instance_id, get_bvar_qps)) {                       \
            drop_request = true;                                                             \
            code = MetaServiceCode::MAX_QPS_LIMIT;                                           \
            msg = "reach max qps limit";                                                     \
            return;                                                                          \
        }                                                                                    \
    }

// FIXME(gavin): should it be a member function of ResourceManager?
std::string get_instance_id(const std::shared_ptr<ResourceManager>& rc_mgr,
                            const std::string& cloud_unique_id);

int decrypt_instance_info(InstanceInfoPB& instance, const std::string& instance_id,
                          MetaServiceCode& code, std::string& msg,
                          std::shared_ptr<Transaction>& txn);

/**
 * Nodtifies other metaservice to refresh instance
 */
void notify_refresh_instance(std::shared_ptr<TxnKv> txn_kv, const std::string& instance_id);

} // namespace selectdb
