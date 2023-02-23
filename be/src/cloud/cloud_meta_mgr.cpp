#include "cloud/cloud_meta_mgr.h"

#include <brpc/channel.h>
#include <brpc/controller.h>
#include <glog/logging.h>

#include <chrono>
#include <random>

#include "common/config.h"
#include "common/logging.h"
#include "gen_cpp/olap_file.pb.h"
#include "gen_cpp/selectdb_cloud.pb.h"
#include "olap/rowset/rowset_factory.h"
#include "util/s3_util.h"

namespace doris::cloud {

static constexpr int BRPC_RETRY_TIMES = 3;

#define RETRY_RPC(rpc_name)                                                                      \
    int retry_times = config::meta_service_rpc_retry_times;                                      \
    uint32_t duration_ms = 0;                                                                    \
    auto rng = std::default_random_engine {                                                      \
            static_cast<uint32_t>(std::chrono::steady_clock::now().time_since_epoch().count())}; \
    std::uniform_int_distribution<uint32_t> u(20, 200);                                          \
    std::uniform_int_distribution<uint32_t> u2(500, 1000);                                       \
    do {                                                                                         \
        brpc::Controller cntl;                                                                   \
        cntl.set_timeout_ms(config::meta_service_brpc_timeout_ms);                               \
        cntl.set_max_retry(BRPC_RETRY_TIMES);                                                    \
        res.Clear();                                                                             \
        _stub->rpc_name(&cntl, &req, &res, nullptr);                                             \
        if (cntl.Failed())                                                                       \
            return Status::RpcError("failed to {}: {}", __FUNCTION__, cntl.ErrorText());         \
        if (res.status().code() == selectdb::MetaServiceCode::OK)                                \
            return Status::OK();                                                                 \
        else if (res.status().code() == selectdb::KV_TXN_CONFLICT) {                             \
            duration_ms = retry_times >= 100 ? u(rng) : u2(rng);                                 \
            LOG(WARNING) << "failed to " << __FUNCTION__ << " retry times left:" << retry_times  \
                         << " sleep:" << duration_ms << "ms response:" << res.DebugString();     \
            std::this_thread::sleep_for(std::chrono::milliseconds(duration_ms));                 \
            continue;                                                                            \
        }                                                                                        \
        break;                                                                                   \
    } while (retry_times--);                                                                     \
    return Status::InternalError("failed to {}: {}", __FUNCTION__, res.status().msg());

CloudMetaMgr::CloudMetaMgr() = default;

CloudMetaMgr::~CloudMetaMgr() = default;

Status CloudMetaMgr::open() {
    brpc::ChannelOptions options;
    auto channel = std::make_unique<brpc::Channel>();
    auto endpoint = config::meta_service_endpoint;
    int ret_code = 0;
    if (config::meta_service_use_load_balancer) {
        ret_code = channel->Init(endpoint.c_str(), config::rpc_load_balancer.c_str(), &options);
    } else {
        ret_code = channel->Init(endpoint.c_str(), &options);
    }
    if (ret_code != 0) {
        return Status::InternalError("fail to init brpc channel, endpoint: {}", endpoint);
    }
    _stub = std::make_unique<selectdb::MetaService_Stub>(
            channel.release(), google::protobuf::Service::STUB_OWNS_CHANNEL);
    return Status::OK();
}

Status CloudMetaMgr::get_tablet_meta(int64_t tablet_id, TabletMetaSharedPtr* tablet_meta) {
    VLOG_DEBUG << "send GetTabletRequest, tablet_id: " << tablet_id;
    brpc::Controller cntl;
    cntl.set_timeout_ms(config::meta_service_brpc_timeout_ms);
    selectdb::GetTabletRequest req;
    selectdb::GetTabletResponse resp;
    req.set_cloud_unique_id(config::cloud_unique_id);
    req.set_tablet_id(tablet_id);
    _stub->get_tablet(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) {
        return Status::RpcError("failed to get tablet meta: {}", cntl.ErrorText());
    }
    if (resp.status().code() != selectdb::MetaServiceCode::OK) {
        return Status::InternalError("failed to get tablet meta: {}", resp.status().msg());
    }
    *tablet_meta = std::make_shared<TabletMeta>();
    (*tablet_meta)->init_from_pb(resp.tablet_meta());
    VLOG_DEBUG << "get tablet meta, tablet_id: " << (*tablet_meta)->tablet_id();
    return Status::OK();
}

Status CloudMetaMgr::sync_tablet_rowsets(Tablet* tablet) {
    int tried = 0;

TRY_AGAIN:

    brpc::Controller cntl;
    cntl.set_timeout_ms(config::meta_service_brpc_timeout_ms);
    selectdb::GetRowsetRequest req;
    selectdb::GetRowsetResponse resp;

    int64_t tablet_id = tablet->tablet_id();
    int64_t table_id = tablet->table_id();
    int64_t index_id = tablet->index_id();
    req.set_cloud_unique_id(config::cloud_unique_id);
    auto idx = req.mutable_idx();
    idx->set_tablet_id(tablet_id);
    idx->set_table_id(table_id);
    idx->set_index_id(index_id);
    idx->set_partition_id(tablet->partition_id());
    {
        std::shared_lock rlock(tablet->get_header_lock());
        req.set_start_version(tablet->local_max_version() + 1);
        req.set_base_compaction_cnt(tablet->base_compaction_cnt());
        req.set_cumulative_compaction_cnt(tablet->cumulative_compaction_cnt());
        req.set_cumulative_point(tablet->cumulative_layer_point());
    }
    req.set_end_version(-1);
    VLOG_DEBUG << "send GetRowsetRequest: " << req.ShortDebugString();

    using namespace std::chrono;
    auto start_time = steady_clock::now();

    _stub->get_rowset(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) {
        return Status::RpcError("failed to get rowset meta: {}", cntl.ErrorText());
    }
    if (resp.status().code() == selectdb::MetaServiceCode::TABLET_NOT_FOUND) {
        return Status::NotFound("failed to get rowset meta: {}", resp.status().msg());
    }
    if (resp.status().code() != selectdb::MetaServiceCode::OK) {
        return Status::InternalError("failed to get rowset meta: {}", resp.status().msg());
    }

    int64_t cost = duration_cast<milliseconds>(steady_clock::now() - start_time).count();
    if (cost > 10) {
        LOG(INFO) << "finish get_rowset rpc. rowset_meta.size()=" << resp.rowset_meta().size()
                  << ", cost=" << cost << "ms";
    } else {
        LOG_EVERY_N(INFO, 100) << "finish get_rowset rpc. rowset_meta.size()="
                               << resp.rowset_meta().size() << ", cost=" << cost << "ms";
    }

    int64_t now = duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
    tablet->set_last_sync_time(now);

    if (resp.rowset_meta().empty()) {
        return Status::OK();
    }
    int64_t resp_max_version = (resp.rowset_meta().end() - 1)->end_version();
    {
        auto& stats = resp.stats();
        std::lock_guard wlock(tablet->get_header_lock());

        // ATTN: we are facing following data race
        //
        // resp_base_compaction_cnt=0|base_compaction_cnt=0|resp_cumulative_compaction_cnt=0|cumulative_compaction_cnt=1|resp_max_version=11|max_version=8
        //
        //   BE-compaction-thread                 meta-service                                     BE-query-thread
        //            |                                |                                                |
        //    local   |    commit cumu-compaction      |                                                |
        //   cc_cnt=0 |  --------------------------->  |     sync rowset (long rpc, local cc_cnt=0 )    |   local
        //            |                                |  <-----------------------------------------    |  cc_cnt=0
        //            |                                |  -.                                            |
        //    local   |       done cc_cnt=1            |    \                                           |
        //   cc_cnt=1 |  <---------------------------  |     \                                          |
        //            |                                |      \  returned with resp cc_cnt=0 (snapshot) |
        //            |                                |       '------------------------------------>   |   local
        //            |                                |                                                |  cc_cnt=1
        //            |                                |                                                |
        //            |                                |                                                |  CHECK FAIL
        //            |                                |                                                |  need retry
        // To get rid of just retry syncing tablet
        if (stats.base_compaction_cnt() < tablet->base_compaction_cnt() ||
            stats.cumulative_compaction_cnt() < tablet->cumulative_compaction_cnt() ||
            (resp_max_version < tablet->local_max_version() &&
             stats.base_compaction_cnt() == tablet->base_compaction_cnt() &&
             stats.cumulative_compaction_cnt() == tablet->cumulative_compaction_cnt())) {
            // stale request, ignore
            LOG_WARNING("stale get rowset meta request")
                    .tag("resp_base_compaction_cnt", stats.base_compaction_cnt())
                    .tag("base_compaction_cnt", tablet->base_compaction_cnt())
                    .tag("resp_cumulative_compaction_cnt", stats.cumulative_compaction_cnt())
                    .tag("cumulative_compaction_cnt", tablet->cumulative_compaction_cnt())
                    .tag("resp_max_version", resp_max_version)
                    .tag("max_version", tablet->local_max_version())
                    .tag("tried", tried);
            if (tried++ < 10) goto TRY_AGAIN;
            return Status::OK();
        }
        std::vector<RowsetSharedPtr> rowsets;
        rowsets.reserve(resp.rowset_meta().size());
        for (auto& meta_pb : resp.rowset_meta()) {
            VLOG_DEBUG << "get rowset meta, tablet_id=" << meta_pb.tablet_id() << ", version=["
                       << meta_pb.start_version() << '-' << meta_pb.end_version() << ']';
            if (tablet->version_exists({meta_pb.start_version(), meta_pb.end_version()})) {
                continue;
            }
            auto rs_meta = std::make_shared<RowsetMeta>(table_id, index_id);
            rs_meta->init_from_pb(meta_pb);
            RowsetSharedPtr rowset;
            // schema is nullptr implies using RowsetMeta.tablet_schema
            RowsetFactory::create_rowset(nullptr, tablet->tablet_path(), std::move(rs_meta),
                                         &rowset);
            rowsets.push_back(std::move(rowset));
        }
        if (!rowsets.empty()) {
            // `rowsets.empty()` could happen after doing EMPTY_CUMULATIVE compaction. e.g.:
            //   BE has [0-1][2-11][12-12], [12-12] is delete predicate, cp is 2;
            //   after doing EMPTY_CUMULATIVE compaction, MS cp is 13, get_rowset will return [2-11][12-12].
            bool version_overlap = tablet->local_max_version() >= rowsets.front()->start_version();
            tablet->cloud_add_rowsets(std::move(rowsets), version_overlap);
        }
        tablet->set_base_compaction_cnt(stats.base_compaction_cnt());
        tablet->set_cumulative_compaction_cnt(stats.cumulative_compaction_cnt());
        tablet->set_cumulative_layer_point(stats.cumulative_point());
        tablet->reset_approximate_stats(stats.num_rowsets(), stats.num_segments(), stats.num_rows(),
                                        stats.data_size());
    }
    return Status::OK();
}

Status CloudMetaMgr::prepare_rowset(const RowsetMetaSharedPtr& rs_meta, bool is_tmp,
                                    RowsetMetaSharedPtr* existed_rs_meta) {
    VLOG_DEBUG << "prepare rowset, tablet_id: " << rs_meta->tablet_id()
               << ", rowset_id: " << rs_meta->rowset_id() << ", is_tmp: " << is_tmp;
    selectdb::CreateRowsetRequest req;
    selectdb::CreateRowsetResponse resp;
    req.set_cloud_unique_id(config::cloud_unique_id);
    rs_meta->to_rowset_pb(req.mutable_rowset_meta());
    req.set_temporary(is_tmp);
    int retry_times = config::meta_service_rpc_retry_times;
    do {
        brpc::Controller cntl;
        cntl.set_timeout_ms(config::meta_service_brpc_timeout_ms);
        cntl.set_max_retry(BRPC_RETRY_TIMES);
        _stub->prepare_rowset(&cntl, &req, &resp, nullptr);
        if (cntl.Failed()) {
            return Status::RpcError("failed to prepare rowset: {}", cntl.ErrorText());
        }
        if (resp.status().code() == selectdb::MetaServiceCode::OK) {
            return Status::OK();
        } else if (resp.status().code() == selectdb::MetaServiceCode::ALREADY_EXISTED) {
            if (existed_rs_meta != nullptr && resp.has_existed_rowset_meta()) {
                *existed_rs_meta =
                        std::make_shared<RowsetMeta>(rs_meta->table_id(), rs_meta->index_id());
                (*existed_rs_meta)->init_from_pb(resp.existed_rowset_meta());
            }
            return Status::AlreadyExist("failed to prepare rowset: {}", resp.status().msg());
        } else if (resp.status().code() == selectdb::MetaServiceCode::KV_TXN_CONFLICT) {
            continue;
        }
        break;
    } while (retry_times--);
    return Status::InternalError("failed to prepare rowset: {}", resp.status().msg());
}

Status CloudMetaMgr::commit_rowset(const RowsetMetaSharedPtr& rs_meta, bool is_tmp,
                                   RowsetMetaSharedPtr* existed_rs_meta) {
    VLOG_DEBUG << "commit rowset, tablet_id: " << rs_meta->tablet_id()
               << ", rowset_id: " << rs_meta->rowset_id() << ", is_tmp: " << is_tmp;
    selectdb::CreateRowsetRequest req;
    selectdb::CreateRowsetResponse resp;
    req.set_cloud_unique_id(config::cloud_unique_id);
    rs_meta->to_rowset_pb(req.mutable_rowset_meta());
    req.set_temporary(is_tmp);
    int retry_times = config::meta_service_rpc_retry_times;
    do {
        brpc::Controller cntl;
        cntl.set_timeout_ms(config::meta_service_brpc_timeout_ms);
        cntl.set_max_retry(BRPC_RETRY_TIMES);
        _stub->commit_rowset(&cntl, &req, &resp, nullptr);
        if (cntl.Failed()) {
            return Status::RpcError("failed to commit rowset: {}", cntl.ErrorText());
        }
        if (resp.status().code() == selectdb::MetaServiceCode::OK) {
            return Status::OK();
        } else if (resp.status().code() == selectdb::MetaServiceCode::ALREADY_EXISTED) {
            if (existed_rs_meta != nullptr && resp.has_existed_rowset_meta()) {
                *existed_rs_meta =
                        std::make_shared<RowsetMeta>(rs_meta->table_id(), rs_meta->index_id());
                (*existed_rs_meta)->init_from_pb(resp.existed_rowset_meta());
            }
            return Status::AlreadyExist("failed to commit rowset: {}", resp.status().msg());
        } else if (resp.status().code() == selectdb::MetaServiceCode::KV_TXN_CONFLICT) {
            continue;
        }
        break;
    } while (retry_times--);
    return Status::InternalError("failed to commit rowset: {}", resp.status().msg());
}

Status CloudMetaMgr::commit_txn(StreamLoadContext* ctx, bool is_2pc) {
    VLOG_DEBUG << "commit txn, db_id: " << ctx->db_id << ", txn_id: " << ctx->txn_id
               << ", label: " << ctx->label << ", is_2pc: " << is_2pc;
    selectdb::CommitTxnRequest req;
    selectdb::CommitTxnResponse res;
    req.set_cloud_unique_id(config::cloud_unique_id);
    req.set_db_id(ctx->db_id);
    req.set_txn_id(ctx->txn_id);
    req.set_is_2pc(is_2pc);
    RETRY_RPC(commit_txn);
}

Status CloudMetaMgr::abort_txn(StreamLoadContext* ctx) {
    VLOG_DEBUG << "abort txn, db_id: " << ctx->db_id << ", txn_id: " << ctx->txn_id
               << ", label: " << ctx->label;
    selectdb::AbortTxnRequest req;
    selectdb::AbortTxnResponse res;
    req.set_cloud_unique_id(config::cloud_unique_id);
    if (ctx->db_id > 0 && !ctx->label.empty()) {
        req.set_db_id(ctx->db_id);
        req.set_label(ctx->label);
    } else {
        req.set_txn_id(ctx->txn_id);
    }
    RETRY_RPC(abort_txn);
}

Status CloudMetaMgr::precommit_txn(StreamLoadContext* ctx) {
    VLOG_DEBUG << "precommit txn, db_id: " << ctx->db_id << ", txn_id: " << ctx->txn_id
               << ", label: " << ctx->label;
    selectdb::PrecommitTxnRequest req;
    selectdb::PrecommitTxnResponse res;
    req.set_cloud_unique_id(config::cloud_unique_id);
    req.set_db_id(ctx->db_id);
    req.set_txn_id(ctx->txn_id);
    RETRY_RPC(precommit_txn);
}

Status CloudMetaMgr::get_s3_info(std::vector<std::tuple<std::string, S3Conf>>* s3_infos) {
    brpc::Controller cntl;
    cntl.set_timeout_ms(config::meta_service_brpc_timeout_ms);
    selectdb::GetObjStoreInfoRequest req;
    selectdb::GetObjStoreInfoResponse resp;
    req.set_cloud_unique_id(config::cloud_unique_id);
    _stub->get_obj_store_info(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) {
        return Status::RpcError("failed to get s3 info: {}", cntl.ErrorText());
    }
    if (resp.status().code() != selectdb::MetaServiceCode::OK) {
        return Status::InternalError("failed to get s3 info: {}", resp.status().msg());
    }
    for (auto& obj_store : resp.obj_info()) {
        S3Conf s3_conf;
        s3_conf.ak = obj_store.ak();
        s3_conf.sk = obj_store.sk();
        s3_conf.endpoint = obj_store.endpoint();
        s3_conf.region = obj_store.region();
        s3_conf.bucket = obj_store.bucket();
        s3_conf.prefix = obj_store.prefix();
        s3_infos->emplace_back(obj_store.id(), std::move(s3_conf));
    }
    return Status::OK();
}

Status CloudMetaMgr::prepare_tablet_job(const selectdb::TabletJobInfoPB& job) {
    VLOG_DEBUG << "prepare_tablet_job: " << job.ShortDebugString();
    selectdb::StartTabletJobRequest req;
    selectdb::StartTabletJobResponse res;
    req.mutable_job()->CopyFrom(job);
    req.set_cloud_unique_id(config::cloud_unique_id);
    int retry_times = config::meta_service_rpc_retry_times;
    do {
        brpc::Controller cntl;
        cntl.set_timeout_ms(config::meta_service_brpc_timeout_ms);
        cntl.set_max_retry(BRPC_RETRY_TIMES);
        _stub->start_tablet_job(&cntl, &req, &res, nullptr);
        if (cntl.Failed()) {
            return Status::RpcError("failed to prepare_tablet_job: {}", cntl.ErrorText());
        }
        if (res.status().code() == selectdb::MetaServiceCode::OK) {
            return Status::OK();
        } else if (res.status().code() == selectdb::MetaServiceCode::JOB_ALREADY_SUCCESS) {
            return Status::OLAPInternalError(JOB_ALREADY_SUCCESS);
        } else if (res.status().code() == selectdb::MetaServiceCode::STALE_TABLET_CACHE) {
            return Status::OLAPInternalError(STALE_TABLET_CACHE);
        } else if (res.status().code() == selectdb::MetaServiceCode::TABLET_NOT_FOUND) {
            Status::NotFound("failed to prepare_tablet_job: {}", res.status().msg());
        } else if (res.status().code() == selectdb::KV_TXN_CONFLICT) {
            continue;
        }
        break;
    } while (retry_times--);
    return Status::InternalError("failed to prepare_tablet_job: {}", res.status().msg());
}

Status CloudMetaMgr::commit_tablet_job(const selectdb::TabletJobInfoPB& job,
                                       selectdb::TabletStatsPB* stats) {
    VLOG_DEBUG << "commit_tablet_job: " << job.ShortDebugString();
    selectdb::FinishTabletJobRequest req;
    selectdb::FinishTabletJobResponse res;
    req.mutable_job()->CopyFrom(job);
    req.set_action(selectdb::FinishTabletJobRequest::COMMIT);
    req.set_cloud_unique_id(config::cloud_unique_id);
    int retry_times = config::meta_service_rpc_retry_times;
    do {
        brpc::Controller cntl;
        cntl.set_timeout_ms(config::meta_service_brpc_timeout_ms);
        cntl.set_max_retry(BRPC_RETRY_TIMES);
        _stub->finish_tablet_job(&cntl, &req, &res, nullptr);
        if (cntl.Failed()) {
            return Status::RpcError("failed to commit_tablet_job: {}", cntl.ErrorText());
        }
        if (res.status().code() == selectdb::MetaServiceCode::OK) {
            stats->CopyFrom(res.stats());
            return Status::OK();
        } else if (res.status().code() == selectdb::MetaServiceCode::JOB_ALREADY_SUCCESS) {
            return Status::OLAPInternalError(JOB_ALREADY_SUCCESS);
        } else if (res.status().code() == selectdb::MetaServiceCode::TABLET_NOT_FOUND) {
            Status::NotFound("failed to commit_tablet_job: {}", res.status().msg());
        } else if (res.status().code() == selectdb::KV_TXN_CONFLICT) {
            continue;
        }
        break;
    } while (retry_times--);
    return Status::InternalError("failed to commit_tablet_job: {}", res.status().msg());
}

Status CloudMetaMgr::abort_tablet_job(const selectdb::TabletJobInfoPB& job) {
    VLOG_DEBUG << "abort_tablet_job: " << job.ShortDebugString();
    selectdb::FinishTabletJobRequest req;
    selectdb::FinishTabletJobResponse res;
    req.mutable_job()->CopyFrom(job);
    req.set_action(selectdb::FinishTabletJobRequest::ABORT);
    req.set_cloud_unique_id(config::cloud_unique_id);
    RETRY_RPC(finish_tablet_job);
}

Status CloudMetaMgr::lease_tablet_job(const selectdb::TabletJobInfoPB& job) {
    VLOG_DEBUG << "lease_tablet_job: " << job.ShortDebugString();
    selectdb::FinishTabletJobRequest req;
    selectdb::FinishTabletJobResponse res;
    req.mutable_job()->CopyFrom(job);
    req.set_action(selectdb::FinishTabletJobRequest::LEASE);
    req.set_cloud_unique_id(config::cloud_unique_id);
    RETRY_RPC(finish_tablet_job);
}

Status CloudMetaMgr::update_tablet_schema(int64_t tablet_id, TabletSchemaSPtr tablet_schema) {
    VLOG_DEBUG << "send UpdateTabletSchemaRequest, tablet_id: " << tablet_id;
    brpc::Controller cntl;
    cntl.set_timeout_ms(config::meta_service_brpc_timeout_ms);
    selectdb::UpdateTabletSchemaRequest req;
    selectdb::UpdateTabletSchemaResponse resp;
    req.set_cloud_unique_id(config::cloud_unique_id);
    req.set_tablet_id(tablet_id);

    TabletSchemaPB tablet_schema_pb;
    tablet_schema->to_schema_pb(&tablet_schema_pb);
    req.mutable_tablet_schema()->CopyFrom(tablet_schema_pb);
    _stub->update_tablet_schema(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) {
        return Status::RpcError("failed to update tablet schema: {}", cntl.ErrorText());
    }
    if (resp.status().code() != selectdb::MetaServiceCode::OK) {
        return Status::InternalError("failed to update tablet schema: {}", resp.status().msg());
    }
    VLOG_DEBUG << "succeed to update tablet schema, tablet_id: " << tablet_id;
    return Status::OK();
}

} // namespace doris::cloud

// vim: et tw=100 ts=4 sw=4 cc=80:
