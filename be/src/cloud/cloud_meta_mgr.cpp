#include "cloud/cloud_meta_mgr.h"

#include <brpc/channel.h>
#include <brpc/controller.h>
#include <gen_cpp/olap_file.pb.h>

#include "common/config.h"
#include "common/logging.h"
#include "gen_cpp/selectdb_cloud.pb.h"
#include "olap/rowset/rowset_factory.h"
#include "util/s3_util.h"

namespace doris::cloud {

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

template <typename Res>
static Status check_rpc_response(const Res& res, const brpc::Controller& cntl,
                                 const std::string& msg) {
    if (cntl.Failed()) {
        return Status::RpcError("failed to {} err: {}", msg, cntl.ErrorText());
    }
    if (res.status().code() != selectdb::MetaServiceCode::OK) {
        return Status::InternalError("failed to {} err: {}", msg, res.status().msg());
    }
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
    VLOG_DEBUG << "send GetRowsetRequest: " << req.DebugString();
    _stub->get_rowset(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) {
        return Status::RpcError("failed to get rowset meta: {}", cntl.ErrorText());
    }
    if (resp.status().code() != selectdb::MetaServiceCode::OK) {
        return Status::InternalError("failed to get rowset meta: {}", resp.status().msg());
    }

    if (resp.rowset_meta().empty()) {
        return Status::OK();
    }
    int64_t resp_max_version = (resp.rowset_meta().end() - 1)->end_version();
    {
        std::lock_guard wlock(tablet->get_header_lock());
        if (resp.base_compaction_cnt() < tablet->base_compaction_cnt() ||
            resp.cumulative_compaction_cnt() < tablet->cumulative_compaction_cnt() ||
            resp_max_version < tablet->local_max_version()) {
            // stale request, ignore
            LOG_WARNING("stale get rowset meta request")
                    .tag("resp_base_compaction_cnt", resp.base_compaction_cnt())
                    .tag("base_compaction_cnt", tablet->base_compaction_cnt())
                    .tag("resp_cumulative_compaction_cnt", resp.cumulative_compaction_cnt())
                    .tag("cumulative_compaction_cnt", tablet->cumulative_compaction_cnt())
                    .tag("resp_max_version", resp_max_version)
                    .tag("max_version", tablet->local_max_version());
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
            RowsetFactory::create_rowset(nullptr, tablet->tablet_path(), std::move(rs_meta),
                                         &rowset);
            rowsets.push_back(std::move(rowset));
        }
        bool version_overlap = tablet->local_max_version() >= rowsets.front()->start_version();
        tablet->cloud_add_rowsets(std::move(rowsets), version_overlap);
        // if resp.cumulative_point < 0, resp.cumulative_point could be less than new calculated cumulative_point,
        // should not update compaction_cnt or cumulative_point in this case.
        if (resp.cumulative_point() >= 0) {
            tablet->set_base_compaction_cnt(resp.base_compaction_cnt());
            tablet->set_cumulative_compaction_cnt(resp.cumulative_compaction_cnt());
            tablet->set_cumulative_layer_point(resp.cumulative_point());
        }
    }
    return Status::OK();
}

Status CloudMetaMgr::write_tablet_meta(const TabletMetaSharedPtr& tablet_meta) {
    brpc::Controller cntl;
    cntl.set_timeout_ms(config::meta_service_brpc_timeout_ms);
    selectdb::CreateTabletRequest req;
    selectdb::MetaServiceGenericResponse resp;
    req.set_cloud_unique_id(config::cloud_unique_id);
    tablet_meta->to_meta_pb(req.mutable_tablet_meta());
    _stub->create_tablet(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) {
        return Status::RpcError("failed to write tablet meta: {}", cntl.ErrorText());
    }
    if (resp.status().code() != selectdb::MetaServiceCode::OK) {
        return Status::InternalError("failed to tablet rowset meta: {}", resp.status().msg());
    }
    return Status::OK();
}

Status CloudMetaMgr::prepare_rowset(const RowsetMetaSharedPtr& rs_meta, bool is_tmp) {
    VLOG_DEBUG << "prepare rowset, tablet_id: " << rs_meta->tablet_id()
               << ", rowset_id: " << rs_meta->rowset_id() << ", is_tmp: " << is_tmp;
    brpc::Controller cntl;
    cntl.set_timeout_ms(config::meta_service_brpc_timeout_ms);
    selectdb::CreateRowsetRequest req;
    selectdb::MetaServiceGenericResponse resp;
    req.set_cloud_unique_id(config::cloud_unique_id);
    rs_meta->to_rowset_pb(req.mutable_rowset_meta());
    req.set_temporary(is_tmp);
    _stub->prepare_rowset(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) {
        return Status::RpcError("failed to prepare rowset: {}", cntl.ErrorText());
    }
    if (resp.status().code() == selectdb::MetaServiceCode::OK) {
        return Status::OK();
    } else if (resp.status().code() == selectdb::MetaServiceCode::ROWSET_ALREADY_EXISTED) {
        return Status::AlreadyExist("failed to prepare rowset: {}", resp.status().msg());
    }
    return Status::InternalError("failed to prepare rowset: {}", resp.status().msg());
}

Status CloudMetaMgr::commit_rowset(const RowsetMetaSharedPtr& rs_meta, bool is_tmp) {
    VLOG_DEBUG << "commit rowset, tablet_id: " << rs_meta->tablet_id()
               << ", rowset_id: " << rs_meta->rowset_id() << ", is_tmp: " << is_tmp;
    brpc::Controller cntl;
    cntl.set_timeout_ms(config::meta_service_brpc_timeout_ms);
    selectdb::CreateRowsetRequest req;
    selectdb::MetaServiceGenericResponse resp;
    req.set_cloud_unique_id(config::cloud_unique_id);
    rs_meta->to_rowset_pb(req.mutable_rowset_meta());
    req.set_temporary(is_tmp);
    _stub->commit_rowset(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) {
        return Status::RpcError("failed to commit rowset: {}", cntl.ErrorText());
    }
    if (resp.status().code() == selectdb::MetaServiceCode::OK) {
        return Status::OK();
    } else if (resp.status().code() == selectdb::MetaServiceCode::ROWSET_ALREADY_EXISTED) {
        return Status::AlreadyExist("failed to commit rowset: {}", resp.status().msg());
    }
    return Status::InternalError("failed to commit rowset: {}", resp.status().msg());
}

Status CloudMetaMgr::commit_txn(StreamLoadContext* ctx, bool is_2pc) {
    VLOG_DEBUG << "commit txn, db_id: " << ctx->db_id << ", txn_id: " << ctx->txn_id
               << ", label: " << ctx->label << ", is_2pc: " << is_2pc;
    brpc::Controller cntl;
    cntl.set_timeout_ms(config::meta_service_brpc_timeout_ms);
    selectdb::CommitTxnRequest req;
    selectdb::CommitTxnResponse resp;
    req.set_cloud_unique_id(config::cloud_unique_id);
    req.set_db_id(ctx->db_id);
    req.set_txn_id(ctx->txn_id);
    req.set_is_2pc(is_2pc);
    _stub->commit_txn(&cntl, &req, &resp, nullptr);
    return check_rpc_response(resp, cntl, __FUNCTION__);
}

Status CloudMetaMgr::abort_txn(StreamLoadContext* ctx) {
    VLOG_DEBUG << "abort txn, db_id: " << ctx->db_id << ", txn_id: " << ctx->txn_id
               << ", label: " << ctx->label;
    brpc::Controller cntl;
    cntl.set_timeout_ms(config::meta_service_brpc_timeout_ms);
    selectdb::AbortTxnRequest req;
    selectdb::AbortTxnResponse resp;
    req.set_cloud_unique_id(config::cloud_unique_id);
    if (ctx->db_id > 0 && !ctx->label.empty()) {
        req.set_db_id(ctx->db_id);
        req.set_label(ctx->label);
    } else {
        req.set_txn_id(ctx->txn_id);
    }
    _stub->abort_txn(&cntl, &req, &resp, nullptr);
    return check_rpc_response(resp, cntl, __FUNCTION__);
}

Status CloudMetaMgr::precommit_txn(StreamLoadContext* ctx) {
    VLOG_DEBUG << "precommit txn, db_id: " << ctx->db_id << ", txn_id: " << ctx->txn_id
               << ", label: " << ctx->label;
    brpc::Controller cntl;
    cntl.set_timeout_ms(config::meta_service_brpc_timeout_ms);
    selectdb::PrecommitTxnRequest req;
    selectdb::PrecommitTxnResponse resp;
    req.set_cloud_unique_id(config::cloud_unique_id);
    req.set_db_id(ctx->db_id);
    req.set_txn_id(ctx->txn_id);
    _stub->precommit_txn(&cntl, &req, &resp, nullptr);
    return check_rpc_response(resp, cntl, __FUNCTION__);
}

Status CloudMetaMgr::get_s3_info(std::vector<std::tuple<std::string, S3Conf>>* s3_infos) {
    if (config::test_s3_ak != "ak") {
        // For compatibility reason, will be removed soon.
        S3Conf s3_conf;
        s3_conf.ak = config::test_s3_ak;
        s3_conf.sk = config::test_s3_sk;
        s3_conf.endpoint = config::test_s3_endpoint;
        s3_conf.region = config::test_s3_region;
        s3_conf.bucket = config::test_s3_bucket;
        s3_conf.prefix = config::test_s3_prefix;
        s3_infos->emplace_back(config::test_s3_resource, std::move(s3_conf));
        return Status::OK();
    }

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
    brpc::Controller cntl;
    cntl.set_timeout_ms(config::meta_service_brpc_timeout_ms);
    selectdb::StartTabletJobRequest req;
    selectdb::StartTabletJobResponse res;
    req.mutable_job()->CopyFrom(job);
    req.set_cloud_unique_id(config::cloud_unique_id);
    _stub->start_tablet_job(&cntl, &req, &res, nullptr);
    return check_rpc_response(res, cntl, __FUNCTION__);
}

Status CloudMetaMgr::commit_tablet_job(const selectdb::TabletJobInfoPB& job) {
    brpc::Controller cntl;
    cntl.set_timeout_ms(config::meta_service_brpc_timeout_ms);
    selectdb::FinishTabletJobRequest req;
    selectdb::FinishTabletJobResponse res;
    req.mutable_job()->CopyFrom(job);
    req.set_action(selectdb::FinishTabletJobRequest::COMMIT);
    req.set_cloud_unique_id(config::cloud_unique_id);
    _stub->finish_tablet_job(&cntl, &req, &res, nullptr);
    return check_rpc_response(res, cntl, __FUNCTION__);
}

Status CloudMetaMgr::abort_tablet_job(const selectdb::TabletJobInfoPB& job) {
    brpc::Controller cntl;
    cntl.set_timeout_ms(config::meta_service_brpc_timeout_ms);
    selectdb::FinishTabletJobRequest req;
    selectdb::FinishTabletJobResponse res;
    req.mutable_job()->CopyFrom(job);
    req.set_action(selectdb::FinishTabletJobRequest::ABORT);
    req.set_cloud_unique_id(config::cloud_unique_id);
    _stub->finish_tablet_job(&cntl, &req, &res, nullptr);
    return check_rpc_response(res, cntl, __FUNCTION__);
}

} // namespace doris::cloud

// vim: et tw=100 ts=4 sw=4 cc=80:
