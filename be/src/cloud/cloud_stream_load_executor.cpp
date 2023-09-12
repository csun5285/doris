#include "cloud/cloud_stream_load_executor.h"

#include "cloud/meta_mgr.h"
#include "cloud/utils.h"
#include "common/logging.h"
#include "runtime/stream_load/stream_load_context.h"
#include "runtime/stream_load/stream_load_executor.h"

namespace doris::cloud {

CloudStreamLoadExecutor::CloudStreamLoadExecutor(ExecEnv* exec_env)
        : StreamLoadExecutor(exec_env) {}

CloudStreamLoadExecutor::~CloudStreamLoadExecutor() = default;

Status CloudStreamLoadExecutor::pre_commit_txn(StreamLoadContext* ctx) {
    auto st = meta_mgr()->precommit_txn(ctx);
    if (!st.ok()) {
        LOG(WARNING) << "Failed to precommit txn: " << st << ", " << ctx->brief();
        return st;
    }
    ctx->need_rollback = false;
    return st;
}

Status CloudStreamLoadExecutor::operate_txn_2pc(StreamLoadContext* ctx) {
    VLOG_DEBUG << "operate_txn_2pc, op: " << ctx->txn_operation;
    if (ctx->txn_operation.compare("commit") == 0) {
        return meta_mgr()->commit_txn(ctx, true);
    } else {
        // 2pc abort
        return meta_mgr()->abort_txn(ctx);
    }
}

Status CloudStreamLoadExecutor::commit_txn(StreamLoadContext* ctx) {
    if (ctx->load_type == TLoadType::ROUTINE_LOAD) {
        return StreamLoadExecutor::commit_txn(ctx);
    }

    // forward to fe to excute commit transaction for MoW table
    if (ctx->is_mow_table()) {
        return StreamLoadExecutor::commit_txn(ctx);
    }

    auto st = meta_mgr()->commit_txn(ctx, false);
    if (!st.ok()) {
        LOG(WARNING) << "Failed to commit txn: " << st << ", " << ctx->brief();
        return st;
    }
    ctx->need_rollback = false;
    return st;
}

void CloudStreamLoadExecutor::rollback_txn(StreamLoadContext* ctx) {
    WARN_IF_ERROR(meta_mgr()->abort_txn(ctx), "Failed to rollback txn");
}

} // namespace doris::cloud
