#include "cloud/cloud_meta_mgr.h"

#include <brpc/channel.h>
#include <brpc/controller.h>
#include <gen_cpp/olap_file.pb.h>

#include "common/config.h"
#include "gen_cpp/selectdb_cloud.pb.h"
#include "service/internal_service.h"
#include "util/defer_op.h"

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

Status CloudMetaMgr::get_tablet_meta(int64_t tablet_id, TabletMetaSharedPtr* tablet_meta) {
    VLOG_DEBUG << "send GetTabletRequest, tablet_id: " << tablet_id;
    brpc::Controller cntl;
    selectdb::GetTabletRequest req;
    selectdb::GetTabletResponse resp;
    req.set_cloud_unique_id(config::cloud_unique_id);
    req.set_tablet_id(tablet_id);
    _stub->get_tablet(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) {
        return Status::IOError("failed to get tablet meta: {}", cntl.ErrorText());
    }
    if (resp.status().code() != selectdb::MetaServiceCode::OK) {
        return Status::InternalError("failed to get tablet meta: {}", resp.status().msg());
    }
    *tablet_meta = std::make_shared<TabletMeta>();
    (*tablet_meta)->init_from_pb(resp.tablet_meta());
    VLOG_DEBUG << "get tablet meta, tablet_id: " << (*tablet_meta)->tablet_id();
    return Status::OK();
}

Status CloudMetaMgr::get_rowset_meta(int64_t tablet_id, Version version_range,
                                     std::vector<RowsetMetaSharedPtr>* rs_metas) {
    VLOG_DEBUG << "send GetRowsetRequest, tablet_id: " << tablet_id
               << ", version: " << version_range;
    brpc::Controller cntl;
    selectdb::GetRowsetRequest req;
    selectdb::GetRowsetResponse resp;
    req.set_cloud_unique_id(config::cloud_unique_id);
    req.set_tablet_id(tablet_id);
    req.set_start_version(version_range.first);
    req.set_end_version(version_range.second);
    _stub->get_rowset(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) {
        return Status::IOError("failed to get rowset meta: {}", cntl.ErrorText());
    }
    if (resp.status().code() != selectdb::MetaServiceCode::OK) {
        return Status::InternalError("failed to get rowset meta: {}", resp.status().msg());
    }
    rs_metas->clear();
    rs_metas->reserve(resp.rowset_meta().size());
    for (const auto& meta_pb : resp.rowset_meta()) {
        auto rs_meta = std::make_shared<RowsetMeta>();
        rs_meta->init_from_pb(meta_pb);
        VLOG_DEBUG << "get rowset meta, tablet_id: " << rs_meta->tablet_id()
                   << ", version: " << rs_meta->version();
        rs_metas->push_back(std::move(rs_meta));
    }
    return Status::OK();
}

Status CloudMetaMgr::write_tablet_meta(const TabletMetaSharedPtr& tablet_meta) {
    brpc::Controller cntl;
    selectdb::CreateTabletRequest req;
    selectdb::MetaServiceGenericResponse resp;
    Defer defer([&] { req.release_tablet_meta(); });
    req.set_cloud_unique_id(config::cloud_unique_id);
    TabletMetaPB tablet_meta_pb;
    tablet_meta->to_meta_pb(&tablet_meta_pb);
    req.set_allocated_tablet_meta(&tablet_meta_pb);
    _stub->create_tablet(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) {
        return Status::IOError("failed to write tablet meta: {}", cntl.ErrorText());
    }
    if (resp.status().code() != selectdb::MetaServiceCode::OK) {
        return Status::InternalError("failed to tablet rowset meta: {}", resp.status().msg());
    }
    return Status::OK();
}

Status CloudMetaMgr::write_rowset_meta(const RowsetMetaSharedPtr& rs_meta, bool is_tmp) {
    VLOG_DEBUG << "write rowset meta, tablet_id: " << rs_meta->tablet_id()
               << ", rowset_id: " << rs_meta->rowset_id() << ", is_tmp: " << is_tmp;
    brpc::Controller cntl;
    selectdb::CreateRowsetRequest req;
    selectdb::MetaServiceGenericResponse resp;
    req.set_cloud_unique_id(config::cloud_unique_id);
    *req.mutable_rowset_meta() = rs_meta->get_rowset_pb();
    req.set_temporary(is_tmp);
    _stub->create_rowset(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) {
        return Status::IOError("failed to write rowset meta: {}", cntl.ErrorText());
    }
    if (resp.status().code() != selectdb::MetaServiceCode::OK) {
        return Status::InternalError("failed to write rowset meta: {}", resp.status().msg());
    }
    return Status::OK();
}

Status CloudMetaMgr::commit_txn(StreamLoadContext* ctx, bool is_2pc) {
    VLOG_DEBUG << "commit txn, db_id: " << ctx->db_id << ", txn_id: " << ctx->txn_id
               << ", label: " << ctx->label << ", is_2pc: " << is_2pc;
    brpc::Controller cntl;
    selectdb::CommitTxnRequest req;
    selectdb::CommitTxnResponse resp;
    req.set_cloud_unique_id(config::cloud_unique_id);
    req.set_db_id(ctx->db_id);
    req.set_txn_id(ctx->txn_id);
    req.set_is_2pc(is_2pc);
    _stub->commit_txn(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) {
        return Status::IOError("failed to commit txn: {}", cntl.ErrorText());
    }
    if (resp.status().code() != selectdb::MetaServiceCode::OK) {
        return Status::InternalError("failed to commit txn: {}", resp.status().msg());
    }
    return Status::OK();
}

Status CloudMetaMgr::abort_txn(StreamLoadContext* ctx) {
    VLOG_DEBUG << "abort txn, db_id: " << ctx->db_id << ", txn_id: " << ctx->txn_id
               << ", label: " << ctx->label;
    brpc::Controller cntl;
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
    if (cntl.Failed()) {
        return Status::IOError("failed to abort txn: {}", cntl.ErrorText());
    }
    if (resp.status().code() != selectdb::MetaServiceCode::OK) {
        return Status::InternalError("failed to abort txn: {}", resp.status().msg());
    }
    return Status::OK();
}

Status CloudMetaMgr::precommit_txn(StreamLoadContext* ctx) {
    VLOG_DEBUG << "precommit txn, db_id: " << ctx->db_id << ", txn_id: " << ctx->txn_id
               << ", label: " << ctx->label;
    brpc::Controller cntl;
    selectdb::PrecommitTxnRequest req;
    selectdb::PrecommitTxnResponse resp;
    req.set_cloud_unique_id(config::cloud_unique_id);
    req.set_db_id(ctx->db_id);
    req.set_txn_id(ctx->txn_id);
    _stub->precommit_txn(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) {
        return Status::IOError("failed to precommit txn: {}", cntl.ErrorText());
    }
    if (resp.status().code() != selectdb::MetaServiceCode::OK) {
        return Status::InternalError("failed to precommit txn: {}", resp.status().msg());
    }
    return Status::OK();
}

Status CloudMetaMgr::get_s3_info(const std::string& resource_id, S3Conf* s3_info) {
    s3_info->ak = config::test_s3_ak;
    s3_info->sk = config::test_s3_sk;
    s3_info->endpoint = config::test_s3_endpoint;
    s3_info->region = config::test_s3_region;
    s3_info->bucket = config::test_s3_bucket;
    s3_info->prefix = config::test_s3_prefix;
    return Status::OK();
}

} // namespace doris::cloud
