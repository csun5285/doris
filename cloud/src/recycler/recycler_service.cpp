#include "recycler/recycler_service.h"

#include <brpc/closure_guard.h>
#include <brpc/controller.h>
#include <fmt/format.h>
#include <gen_cpp/selectdb_cloud.pb.h>
#include <google/protobuf/util/json_util.h>

#include "common/config.h"
#include "common/logging.h"
#include "common/util.h"
#include "meta-service/keys.h"
#include "recycler/recycler.h"

namespace selectdb {

extern std::string convert_ms_code_to_http_code(const MetaServiceCode& ret, int& status_code);

RecyclerServiceImpl::RecyclerServiceImpl(std::shared_ptr<TxnKv> txn_kv, Recycler* recycler)
        : txn_kv_(std::move(txn_kv)), recycler_(recycler) {}

RecyclerServiceImpl::~RecyclerServiceImpl() = default;

void RecyclerServiceImpl::recycle_instance(::google::protobuf::RpcController* controller,
                                           const ::selectdb::RecycleInstanceRequest* request,
                                           ::selectdb::RecycleInstanceResponse* response,
                                           ::google::protobuf::Closure* done) {
    auto ctrl = static_cast<brpc::Controller*>(controller);
    LOG(INFO) << "rpc from " << ctrl->remote_side() << " request=" << request->DebugString();
    brpc::ClosureGuard closure_guard(done);
    int ret = 0;
    MetaServiceCode code = MetaServiceCode::OK;
    std::string msg = "OK";
    std::unique_ptr<int, std::function<void(int*)>> defer_status(
            (int*)0x01, [&code, &msg, &response, &ctrl](int*) {
                response->mutable_status()->set_code(code);
                response->mutable_status()->set_msg(msg);
                LOG(INFO) << (code == MetaServiceCode::OK ? "succ to " : "failed to ")
                          << __PRETTY_FUNCTION__ << " " << ctrl->remote_side() << " " << msg;
            });

    std::vector<InstanceInfoPB> instances;
    instances.reserve(request->instance_ids_size());

    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        msg = "failed to create txn";
        return;
    }

    for (auto& id : request->instance_ids()) {
        InstanceKeyInfo key_info {id};
        std::string key;
        instance_key(key_info, &key);
        std::string val;
        ret = txn->get(key, &val);
        if (ret != 0) {
            code = MetaServiceCode::KV_TXN_GET_ERR;
            msg = fmt::format("failed to get instance, instance_id={}", id);
            LOG_WARNING(msg);
            continue;
        }
        InstanceInfoPB instance;
        if (!instance.ParseFromString(val)) {
            code = MetaServiceCode::PROTOBUF_PARSE_ERR;
            msg = fmt::format("malformed instance info, key={}, val={}", hex(key), hex(val));
            LOG_WARNING(msg);
            continue;
        }
        instances.push_back(std::move(instance));
        LOG_INFO("add instance to recycle queue").tag("instance_id", id);
    }

    recycler_->add_pending_instances(std::move(instances));
}

void recycle_copy_jobs(const std::shared_ptr<TxnKv>& txn_kv, const std::string& instance_id,
                       MetaServiceCode& code, std::string& msg) {
    std::unique_ptr<Transaction> txn;
    int ret = txn_kv->create_txn(&txn);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        msg = "failed to create txn";
        return;
    }
    std::string key;
    instance_key({instance_id}, &key);
    std::string val;
    ret = txn->get(key, &val);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_GET_ERR;
        msg = fmt::format("failed to get instance, instance_id={}", instance_id);
        return;
    }
    InstanceInfoPB instance;
    if (!instance.ParseFromString(val)) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        msg = fmt::format("malformed instance info, key={}", hex(key));
        return;
    }
    static std::mutex s_worker_mtx;
    static std::set<std::string> s_worker;
    {
        std::lock_guard lock(s_worker_mtx);
        if (s_worker.size() >= config::recycle_concurrency) { // use another config entry?
            msg = "exceeded the concurrency limit";
            return;
        }
        auto [_, success] = s_worker.insert(instance_id);
        if (!success) {
            msg = "recycle_copy_jobs not yet finished on this instance";
            return;
        }
    }
    auto recycler = std::make_unique<InstanceRecycler>(txn_kv, instance);
    std::thread worker([recycler = std::move(recycler), instance_id] {
        LOG(INFO) << "manually trigger recycle_copy_jobs on instance " << instance_id;
        recycler->recycle_copy_jobs();
        std::lock_guard lock(s_worker_mtx);
        s_worker.erase(instance_id);
    });
    worker.detach();
}

void RecyclerServiceImpl::http(::google::protobuf::RpcController* controller,
                               const ::selectdb::MetaServiceHttpRequest* request,
                               ::selectdb::MetaServiceHttpResponse* response,
                               ::google::protobuf::Closure* done) {
    auto cntl = static_cast<brpc::Controller*>(controller);
    LOG(INFO) << "rpc from " << cntl->remote_side() << " request: " << request->DebugString();
    brpc::ClosureGuard closure_guard(done);
    MetaServiceCode code = MetaServiceCode::OK;
    int status_code = 200;
    std::string msg = "OK";
    std::string req;
    std::string response_body;
    std::string request_body;
    std::unique_ptr<int, std::function<void(int*)>> defer_status(
            (int*)0x01, [&code, &msg, &status_code, &response_body, &cntl, &req](int*) {
                convert_ms_code_to_http_code(code, status_code);
                LOG(INFO) << (code == MetaServiceCode::OK ? "succ to " : "failed to ")
                          << __PRETTY_FUNCTION__ << " " << cntl->remote_side() << " request=\n"
                          << req << "\n ret=" << code << " msg=" << msg;
                cntl->http_response().set_status_code(status_code);
                cntl->response_attachment().append(response_body);
                cntl->response_attachment().append("\n");
            });

    // Prepare input request info
    auto unresolved_path = cntl->http_request().unresolved_path();
    auto uri = cntl->http_request().uri();
    std::stringstream ss;
    ss << "\nuri_path=" << uri.path();
    ss << "\nunresolved_path=" << unresolved_path;
    ss << "\nmethod=" << brpc::HttpMethod2Str(cntl->http_request().method());
    ss << "\nquery strings:";
    for (auto it = uri.QueryBegin(); it != uri.QueryEnd(); ++it) {
        ss << "\n" << it->first << "=" << it->second;
    }
    ss << "\nheaders:";
    for (auto it = cntl->http_request().HeaderBegin(); it != cntl->http_request().HeaderEnd();
         ++it) {
        ss << "\n" << it->first << ":" << it->second;
    }
    req = ss.str();
    ss.clear();
    request_body = cntl->request_attachment().to_string(); // Just copy

    // Auth
    auto token = uri.GetQuery("token");
    if (token == nullptr || *token != config::http_token) {
        msg = "incorrect token, token=" + (token == nullptr ? std::string("(not given)") : *token);
        response_body = "incorrect token";
        status_code = 403;
        return;
    }

    if (unresolved_path == "recycle_instance") {
        RecycleInstanceRequest req;
        auto st = google::protobuf::util::JsonStringToMessage(request_body, &req);
        if (!st.ok()) {
            msg = "failed to RecycleInstanceRequest, error: " + st.message().ToString();
            response_body = msg;
            LOG(WARNING) << msg;
            return;
        }
        RecycleInstanceResponse res;
        recycle_instance(cntl, &req, &res, nullptr);
        code = res.status().code();
        msg = res.status().msg();
        response_body = msg;
        return;
    }

    if (unresolved_path == "recycle_copy_jobs") {
        auto instance_id = uri.GetQuery("instance_id");
        if (instance_id == nullptr || instance_id->empty()) {
            msg = "no instance id";
            response_body = msg;
            status_code = 400;
            return;
        }
        recycle_copy_jobs(txn_kv_, *instance_id, code, msg);
        response_body = msg;
        return;
    }

    status_code = 404;
    msg = "not found";
    response_body = msg;
}

} // namespace selectdb
