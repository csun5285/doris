
// clang-format off
#include "meta_service.h"

#include "common/bvars.h"
#include "common/config.h"
#include "common/encryption_util.h"
#include "common/logging.h"
#include "common/stopwatch.h"
#include "common/sync_point.h"
#include "common/util.h"
#include "meta-service/doris_txn.h"
#include "meta-service/keys.h"
#include "meta-service/meta_service_tablet_stats.h"
#include "meta-service/txn_kv.h"
#include "rate-limiter/rate_limiter.h"

#include "brpc/channel.h"
#include "brpc/closure_guard.h"
#include "brpc/controller.h"
#include "bthread/bthread.h"
#include "google/protobuf/util/json_util.h"
#include "rapidjson/prettywriter.h"
#include "rapidjson/schema.h"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <iomanip>
#include <ios>
#include <limits>
#include <memory>
#include <sstream>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>
// clang-format on

using namespace std::chrono;

namespace selectdb {
/**
 * Nodtifies other metaservice to refresh instance
 */
void notify_refresh_instance(std::shared_ptr<TxnKv> txn_kv, const std::string& instance_id);

static void* run_bthread_work(void* arg) {
    auto f = reinterpret_cast<std::function<void()>*>(arg);
    (*f)();
    delete f;
    return nullptr;
}

MetaServiceImpl::MetaServiceImpl(std::shared_ptr<TxnKv> txn_kv,
                                 std::shared_ptr<ResourceManager> resource_mgr,
                                 std::shared_ptr<RateLimiter> rate_limiter) {
    txn_kv_ = txn_kv;
    resource_mgr_ = resource_mgr;
    rate_limiter_ = rate_limiter;
    rate_limiter_->init(this);
}

MetaServiceImpl::~MetaServiceImpl() = default;

std::string static trim(std::string& str) {
    const std::string drop = "/ \t";
    str.erase(str.find_last_not_of(drop) + 1);
    return str.erase(0, str.find_first_not_of(drop));
}

std::vector<std::string> split(const std::string& str, const char delim) {
    std::vector<std::string> result;
    size_t start = 0;
    size_t pos = str.find(delim);
    while (pos != std::string::npos) {
        if (pos > start) {
            result.push_back(str.substr(start, pos - start));
        }
        start = pos + 1;
        pos = str.find(delim, start);
    }

    if (start < str.length()) result.push_back(str.substr(start));

    return result;
}

// FIXME(gavin): should it be a member function of ResourceManager?
std::string get_instance_id(const std::shared_ptr<ResourceManager>& rc_mgr,
                            const std::string& cloud_unique_id) {
    {
        [[maybe_unused]] std::string tmp_ret;
        TEST_SYNC_POINT_RETURN_WITH_VALUE("get_instance_id", &tmp_ret);
    }

    std::vector<NodeInfo> nodes;
    std::string err = rc_mgr->get_node(cloud_unique_id, &nodes);
    { TEST_SYNC_POINT_CALLBACK("get_instance_id_err", &err); }
    if (!err.empty()) {
        // cache can't find cloud_unique_id, so degraded by parse cloud_unique_id
        // cloud_unique_id encode: ${version}:${instance_id}:${unique_id}
        // check it split by ':' c
        auto vec = split(cloud_unique_id, ':');
        std::stringstream ss;
        for (int i = 0; i < vec.size(); ++i) {
            ss << "idx " << i << "= [" << vec[i] << "] ";
        }
        LOG(INFO) << "degraded to get instance_id, cloud_unique_id: " << cloud_unique_id
                  << "after split: " << ss.str();
        if (vec.size() != 3) {
            LOG(WARNING) << "cloud unique id is not degraded format, failed to check instance "
                            "info, cloud_unique_id="
                         << cloud_unique_id << " , err=" << err;
            return "";
        }
        // version: vec[0], instance_id: vec[1], unique_id: vec[2]
        switch (std::atoi(vec[0].c_str())) {
        case 1:
            // just return instance id;
            return vec[1];
        default:
            LOG(WARNING) << "cloud unique id degraded state, but version not eq configure, "
                            "cloud_unique_id="
                         << cloud_unique_id << ", err=" << err;
            return "";
        }
    }

    std::string instance_id;
    for (auto& i : nodes) {
        if (!instance_id.empty() && instance_id != i.instance_id) {
            LOG(WARNING) << "cloud_unique_id is one-to-many instance_id, "
                         << " cloud_unique_id=" << cloud_unique_id
                         << " current_instance_id=" << instance_id
                         << " later_instance_id=" << i.instance_id;
        }
        instance_id = i.instance_id; // The last wins
    }
    return instance_id;
}

// Return `true` if tablet has been dropped
bool is_dropped_tablet(Transaction* txn, const std::string& instance_id, int64_t index_id,
                       int64_t partition_id) {
    auto key = recycle_index_key({instance_id, index_id});
    std::string val;
    int ret = txn->get(key, &val);
    if (ret == 0) return true; // Ignore kv error
    key = recycle_partition_key({instance_id, partition_id});
    ret = txn->get(key, &val);
    return ret == 0; // Ignore kv error
}

template <class Request>
void begin_rpc(std::string_view func_name, brpc::Controller* ctrl, const Request* req) {
    if constexpr (std::is_same_v<Request, CreateRowsetRequest>) {
        LOG(INFO) << "begin " << func_name << " from " << ctrl->remote_side();
    } else if constexpr (std::is_same_v<Request, CreateTabletsRequest>) {
        LOG(INFO) << "begin " << func_name << " from " << ctrl->remote_side();
    } else if constexpr (std::is_same_v<Request, GetTabletStatsRequest>) {
        LOG(INFO) << "begin " << func_name << " from " << ctrl->remote_side()
                  << " tablet size: " << req->tablet_idx().size();
    } else {
        LOG(INFO) << "begin " << func_name << " from " << ctrl->remote_side()
                  << " request=" << req->ShortDebugString();
    }
}

template <class Response>
void finish_rpc(std::string_view func_name, brpc::Controller* ctrl, Response* res) {
    if constexpr (std::is_same_v<Response, GetTabletResponse>) {
        LOG(INFO) << "finish " << func_name << " from " << ctrl->remote_side()
                  << " status=" << res->status().ShortDebugString();
    } else if constexpr (std::is_same_v<Response, GetRowsetResponse>) {
        if (res->status().code() != MetaServiceCode::OK) {
            res->clear_rowset_meta();
        }
        LOG(INFO) << "finish " << func_name << " from " << ctrl->remote_side()
                  << " status=" << res->status().ShortDebugString();
    } else if constexpr (std::is_same_v<Response, GetCopyFilesResponse>) {
        LOG(INFO) << "finish " << func_name << " from " << ctrl->remote_side()
                  << " status=" << res->status().ShortDebugString();
    } else if constexpr (std::is_same_v<Response, GetClusterResponse>) {
        LOG(INFO) << "finish " << func_name << " from " << ctrl->remote_side()
                  << " status=" << res->status().ShortDebugString();
    } else if constexpr (std::is_same_v<Response, GetObjStoreInfoResponse>) {
        LOG(INFO) << "finish " << func_name << " from " << ctrl->remote_side()
                  << " status=" << res->status().ShortDebugString();
    } else if constexpr (std::is_same_v<Response, CommitTxnResponse>) {
        if (res->status().code() != MetaServiceCode::OK) {
            res->clear_table_ids();
            res->clear_partition_ids();
            res->clear_versions();
        }
        LOG(INFO) << "finish " << func_name << " from " << ctrl->remote_side()
                  << " response=" << res->ShortDebugString();
    } else if constexpr (std::is_same_v<Response, GetTabletStatsResponse>) {
        LOG(INFO) << "finish " << func_name << " from " << ctrl->remote_side()
                  << " status=" << res->status().ShortDebugString()
                  << " tablet size: " << res->tablet_stats().size();
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

void get_tablet_idx(MetaServiceCode& code, std::string& msg, int& ret, Transaction* txn,
                    const std::string& instance_id, int64_t tablet_id, TabletIndexPB& tablet_idx) {
    std::string key, val;
    meta_tablet_idx_key({instance_id, tablet_id}, &key);
    ret = txn->get(key, &val);
    if (ret != 0) {
        code = ret == 1 ? MetaServiceCode::TABLET_NOT_FOUND : MetaServiceCode::KV_TXN_GET_ERR;
        msg = fmt::format("failed to get tablet_idx, ret={} tablet_id={} ", ret, tablet_id);
        return;
    }
    if (!tablet_idx.ParseFromString(val)) [[unlikely]] {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        msg = fmt::format("malformed tablet index value, key={}", hex(key));
        return;
    }
    if (tablet_id != tablet_idx.tablet_id()) [[unlikely]] {
        code = MetaServiceCode::UNDEFINED_ERR;
        msg = "internal error";
        LOG(WARNING) << "unexpected error given_tablet_id=" << tablet_id
                     << " idx_pb_tablet_id=" << tablet_idx.tablet_id();
        return;
    }
}

//TODO: we need move begin/commit etc txn to TxnManager
void MetaServiceImpl::begin_txn(::google::protobuf::RpcController* controller,
                                const ::selectdb::BeginTxnRequest* request,
                                ::selectdb::BeginTxnResponse* response,
                                ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(begin_txn);
    if (!request->has_txn_info()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "invalid argument, missing txn info";
        return;
    }

    auto& txn_info = const_cast<TxnInfoPB&>(request->txn_info());
    std::string label = txn_info.has_label() ? txn_info.label() : "";
    int64_t db_id = txn_info.has_db_id() ? txn_info.db_id() : -1;

    if (label.empty() || db_id < 0 || txn_info.table_ids().empty() || !txn_info.has_timeout_ms()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        ss << "invalid argument, label=" << label << " db_id=" << db_id;
        msg = ss.str();
        return;
    }

    std::string cloud_unique_id = request->has_cloud_unique_id() ? request->cloud_unique_id() : "";
    instance_id = get_instance_id(resource_mgr_, cloud_unique_id);
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        ss << "cannot find instance_id with cloud_unique_id="
           << (cloud_unique_id.empty() ? "(empty)" : cloud_unique_id) << " label=" << label;
        msg = ss.str();
        return;
    }

    RPC_RATE_LIMIT(begin_txn)
    //1. Generate version stamp for txn id
    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        ss << "txn_kv_->create_txn() failed, ret=" << ret << " label=" << label
           << " db_id=" << db_id;
        msg = ss.str();
        return;
    }

    std::string txn_label_key_;
    std::string txn_label_val;

    TxnLabelKeyInfo txn_label_key_info {instance_id, db_id, label};
    txn_label_key(txn_label_key_info, &txn_label_key_);

    ret = txn->get(txn_label_key_, &txn_label_val);
    if (ret < 0) {
        code = MetaServiceCode::KV_TXN_GET_ERR;
        ss << "txn->get failed(), ret=" << ret << " label=" << label;
        msg = ss.str();
        return;
    }

    LOG(INFO) << "txn->get txn_label_key=" << hex(txn_label_key_) << " label=" << label
              << " ret=" << ret;

    //ret == 0 means label has previous txn ids.
    if (ret == 0) {
        txn_label_val = txn_label_val.substr(0, txn_label_val.size() - VERSION_STAMP_LEN);
    }

    //ret > 0, means label not exist previously.
    txn->atomic_set_ver_value(txn_label_key_, txn_label_val);
    LOG(INFO) << "txn->atomic_set_ver_value txn_label_key=" << hex(txn_label_key_);

    TEST_SYNC_POINT_CALLBACK("begin_txn:before:commit_txn:1", &label);
    ret = txn->commit();
    TEST_SYNC_POINT_CALLBACK("begin_txn:after:commit_txn:1", &label);
    if (ret != 0) {
        code = ret == -1 ? MetaServiceCode::KV_TXN_CONFLICT : MetaServiceCode::KV_TXN_COMMIT_ERR;
        ss << "txn->commit failed(), label=" << label << " ret=" << ret;
        msg = ss.str();
        return;
    }
    //2. Get txn id from version stamp
    txn.reset();

    txn_kv_->create_txn(&txn);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        ss << "failed to create txn when get txn id, label=" << label << " ret=" << ret;
        msg = ss.str();
        return;
    }

    txn_label_val.clear();
    ret = txn->get(txn_label_key_, &txn_label_val);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_GET_ERR;
        ss << "txn->get() failed, label=" << label << " ret=" << ret;
        msg = ss.str();
        return;
    }

    LOG(INFO) << "txn->get txn_label_key=" << hex(txn_label_key_) << " label=" << label
              << " ret=" << ret;

    std::string txn_id_str =
            txn_label_val.substr(txn_label_val.size() - VERSION_STAMP_LEN, txn_label_val.size());
    // Generated by TxnKv system
    int64_t txn_id = 0;
    ret = get_txn_id_from_fdb_ts(txn_id_str, &txn_id);
    if (ret != 0) {
        code = MetaServiceCode::TXN_GEN_ID_ERR;
        ss << "get_txn_id_from_fdb_ts() failed, label=" << label << " ret=" << ret;
        msg = ss.str();
        return;
    }

    LOG(INFO) << "get_txn_id_from_fdb_ts() label=" << label << " txn_id=" << txn_id
              << " txn_label_val.size()=" << txn_label_val.size();

    TxnLabelPB txn_label_pb;
    if (txn_label_val.size() > VERSION_STAMP_LEN) {
        //3. Check label
        //txn_label_val.size() > VERSION_STAMP_LEN means label has previous txn ids.

        std::string txn_label_pb_str =
                txn_label_val.substr(0, txn_label_val.size() - VERSION_STAMP_LEN);
        if (!txn_label_pb.ParseFromString(txn_label_pb_str)) {
            code = MetaServiceCode::PROTOBUF_PARSE_ERR;
            ss << "txn_label_pb->ParseFromString() failed, txn_id=" << txn_id << " label=" << label;
            msg = ss.str();
            return;
        }

        // Check if label already used, by following steps
        // 1. get all existing transactions
        // 2. if there is a PREPARE transaction, check if this is a retry request.
        // 3. if there is a non-aborted transaction, throw label already used exception.

        for (auto& cur_txn_id : txn_label_pb.txn_ids()) {
            std::string cur_txn_inf_key;
            std::string cur_txn_inf_val;
            TxnInfoKeyInfo cur_txn_inf_key_info {instance_id, db_id, cur_txn_id};
            txn_info_key(cur_txn_inf_key_info, &cur_txn_inf_key);
            ret = txn->get(cur_txn_inf_key, &cur_txn_inf_val);
            if (ret < 0) {
                code = MetaServiceCode::KV_TXN_GET_ERR;
                ss << "txn->get() failed, cur_txn_id=" << cur_txn_id << " label=" << label
                   << " ret=" << ret;
                msg = ss.str();
                return;
            }

            if (ret == 1) {
                //label_to_idx and txn info inconsistency.
                code = MetaServiceCode::TXN_ID_NOT_FOUND;
                ss << "txn->get() failed, cur_txn_id=" << cur_txn_id << " label=" << label
                   << " ret=" << ret;
                msg = ss.str();
                return;
            }

            TxnInfoPB cur_txn_info;
            if (!cur_txn_info.ParseFromString(cur_txn_inf_val)) {
                code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                ss << "cur_txn_info->ParseFromString() failed, cur_txn_id=" << cur_txn_id
                   << " label=" << label << " ret=" << ret;
                msg = ss.str();
                return;
            }

            VLOG_DEBUG << "cur_txn_info=" << cur_txn_info.ShortDebugString();
            if (cur_txn_info.status() == TxnStatusPB::TXN_STATUS_ABORTED) {
                continue;
            }

            if (cur_txn_info.status() == TxnStatusPB::TXN_STATUS_PREPARED ||
                cur_txn_info.status() == TxnStatusPB::TXN_STATUS_PRECOMMITTED) {
                // clang-format off
                if (cur_txn_info.has_request_id() && txn_info.has_request_id() &&
                    ((cur_txn_info.request_id().hi() == txn_info.request_id().hi()) && 
                     (cur_txn_info.request_id().lo() == txn_info.request_id().lo()))) {

                    response->set_dup_txn_id(cur_txn_info.txn_id());
                    code = MetaServiceCode::TXN_DUPLICATED_REQ;
                    ss << "db_id=" << db_id << " label=" << label << " txn_id=" << cur_txn_info.txn_id() << " dup begin txn request.";
                    msg = ss.str();
                    return;
                }
                // clang-format on
            }
            code = MetaServiceCode::TXN_LABEL_ALREADY_USED;
            ss << "db_id=" << db_id << " label=" << label
               << " already used by txn_id=" << cur_txn_info.txn_id();
            msg = ss.str();
            return;
        }
    }

    // Update txn_info to be put into TxnKv
    // Update txn_id in PB
    txn_info.set_txn_id(txn_id);
    // TODO:
    // check initial status must be TXN_STATUS_PREPARED or TXN_STATUS_UNKNOWN
    txn_info.set_status(TxnStatusPB::TXN_STATUS_PREPARED);

    auto now_time = system_clock::now();
    uint64_t prepare_time = duration_cast<milliseconds>(now_time.time_since_epoch()).count();

    txn_info.set_prepare_time(prepare_time);
    //4. put txn info and db_tbl
    std::string txn_inf_key;
    std::string txn_inf_val;
    TxnInfoKeyInfo txn_inf_key_info {instance_id, db_id, txn_id};
    txn_info_key(txn_inf_key_info, &txn_inf_key);
    if (!txn_info.SerializeToString(&txn_inf_val)) {
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        ss << "failed to serialize txn_info, label=" << label << " txn_id=" << txn_id;
        msg = ss.str();
        return;
    }

    std::string txn_index_key_;
    std::string txn_index_val;
    TxnIndexKeyInfo txn_index_key_info {instance_id, txn_id};
    txn_index_key(txn_index_key_info, &txn_index_key_);
    TxnIndexPB txn_index_pb;
    txn_index_pb.mutable_tablet_index()->set_db_id(db_id);
    if (!txn_index_pb.SerializeToString(&txn_index_val)) {
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        ss << "failed to serialize txn_index_pb "
           << "label=" << label << " txn_id=" << txn_id;
        msg = ss.str();
        return;
    }

    std::string txn_run_key;
    std::string txn_run_val;
    TxnRunningKeyInfo txn_run_key_info {instance_id, db_id, txn_id};
    txn_running_key(txn_run_key_info, &txn_run_key);

    TxnRunningPB running_val_pb;
    running_val_pb.set_timeout_time(prepare_time + txn_info.timeout_ms());
    for (auto i : txn_info.table_ids()) {
        running_val_pb.add_table_ids(i);
    }
    VLOG_DEBUG << "label=" << label << " txn_id=" << txn_id
               << "running_val_pb=" << running_val_pb.ShortDebugString();
    if (!running_val_pb.SerializeToString(&txn_run_val)) {
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        ss << "failed to serialize running_val_pb label=" << label << " txn_id=" << txn_id;
        msg = ss.str();
        return;
    }

    txn_label_pb.add_txn_ids(txn_id);
    VLOG_DEBUG << "label=" << label << " txn_id=" << txn_id
               << "txn_label_pb=" << txn_label_pb.ShortDebugString();
    if (!txn_label_pb.SerializeToString(&txn_label_val)) {
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        ss << "failed to serialize txn_label_pb label=" << label << " txn_id=" << txn_id;
        msg = ss.str();
        return;
    }
    txn->atomic_set_ver_value(txn_label_key_, txn_label_val);
    LOG(INFO) << "txn->atomic_set_ver_value txn_label_key=" << hex(txn_label_key_)
              << " label=" << label << " txn_id=" << txn_id;

    txn->put(txn_inf_key, txn_inf_val);
    txn->put(txn_index_key_, txn_index_val);
    txn->put(txn_run_key, txn_run_val);
    LOG(INFO) << "xxx put txn_info_key=" << hex(txn_inf_key) << " txn_id=" << txn_id;
    LOG(INFO) << "xxx put txn_run_key=" << hex(txn_run_key) << " txn_id=" << txn_id;
    LOG(INFO) << "xxx put txn_index_key=" << hex(txn_index_key_) << " txn_id=" << txn_id;

    ret = txn->commit();
    if (ret != 0) {
        code = ret == -1 ? MetaServiceCode::KV_TXN_CONFLICT : MetaServiceCode::KV_TXN_COMMIT_ERR;
        ss << "failed to commit txn kv, label=" << label << " txn_id=" << txn_id << " ret=" << ret;
        msg = ss.str();
        return;
    }
    TEST_SYNC_POINT_CALLBACK("begin_txn:after:commit_txn:2", &txn_id);
    response->set_txn_id(txn_id);
    return;
}

void MetaServiceImpl::precommit_txn(::google::protobuf::RpcController* controller,
                                    const ::selectdb::PrecommitTxnRequest* request,
                                    ::selectdb::PrecommitTxnResponse* response,
                                    ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(precommit_txn);
    int64_t txn_id = request->has_txn_id() ? request->txn_id() : -1;
    int64_t db_id = request->has_db_id() ? request->db_id() : -1;
    if ((txn_id < 0 && db_id < 0)) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        ss << "invalid argument, "
           << "txn_id=" << txn_id << " db_id=" << db_id;
        msg = ss.str();
        return;
    }

    std::string cloud_unique_id = request->has_cloud_unique_id() ? request->cloud_unique_id() : "";
    instance_id = get_instance_id(resource_mgr_, cloud_unique_id);
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        ss << "cannot find instance_id with cloud_unique_id="
           << (cloud_unique_id.empty() ? "(empty)" : cloud_unique_id) << " txn_id=" << txn_id;
        msg = ss.str();
        return;
    }
    RPC_RATE_LIMIT(precommit_txn);
    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        ss << "txn_kv_->create_txn() failed, ret=" << ret << " txn_id=" << txn_id;
        msg = ss.str();
        return;
    }

    //not provide db_id, we need read from disk.
    if (db_id < 0) {
        std::string txn_index_key_;
        std::string txn_index_val;
        TxnIndexKeyInfo txn_index_key_info {instance_id, txn_id};
        txn_index_key(txn_index_key_info, &txn_index_key_);
        ret = txn->get(txn_index_key_, &txn_index_val);
        if (ret != 0) {
            code = ret > 0 ? MetaServiceCode::TXN_ID_NOT_FOUND : MetaServiceCode::KV_TXN_GET_ERR;
            ss << "failed to get db id with txn_id=" << txn_id << " ret=" << ret;
            msg = ss.str();
            return;
        }
        TxnIndexPB txn_index_pb;
        if (!txn_index_pb.ParseFromString(txn_index_val)) {
            code = MetaServiceCode::PROTOBUF_PARSE_ERR;
            ss << "failed to parse txn_inf"
               << " txn_id=" << txn_id;
            msg = ss.str();
            return;
        }
        DCHECK(txn_index_pb.has_tablet_index() == true);
        DCHECK(txn_index_pb.tablet_index().has_db_id() == true);
        db_id = txn_index_pb.tablet_index().db_id();
        VLOG_DEBUG << " find db_id=" << db_id << " from index";
    } else {
        db_id = request->db_id();
    }

    // Get txn info with db_id and txn_id
    std::string txn_inf_key; // Will be used when saving updated txn
    std::string txn_inf_val; // Will be reused when saving updated txn
    TxnInfoKeyInfo txn_inf_key_info {instance_id, db_id, txn_id};
    txn_info_key(txn_inf_key_info, &txn_inf_key);
    ret = txn->get(txn_inf_key, &txn_inf_val);
    if (ret != 0) {
        code = ret > 0 ? MetaServiceCode::TXN_ID_NOT_FOUND : MetaServiceCode::KV_TXN_GET_ERR;
        ss << "failed to get db id with db_id=" << db_id << " txn_id=" << txn_id << " ret=" << ret;
        msg = ss.str();
        return;
    }

    TxnInfoPB txn_info;
    if (!txn_info.ParseFromString(txn_inf_val)) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        ss << "failed to parse txn_inf db_id=" << db_id << " txn_id=" << txn_id;
        msg = ss.str();
        return;
    }

    DCHECK(txn_info.txn_id() == txn_id);
    if (txn_info.status() == TxnStatusPB::TXN_STATUS_ABORTED) {
        code = MetaServiceCode::TXN_ALREADY_ABORTED;
        ss << "transaction is already aborted: db_id=" << db_id << " txn_id=" << txn_id;
        msg = ss.str();
        return;
    }

    if (txn_info.status() == TxnStatusPB::TXN_STATUS_VISIBLE) {
        code = MetaServiceCode::TXN_ALREADY_VISIBLE;
        ss << "ransaction is already visible: db_id=" << db_id << " txn_id=" << txn_id;
        msg = ss.str();
    }

    if (txn_info.status() == TxnStatusPB::TXN_STATUS_PRECOMMITTED) {
        code = MetaServiceCode::TXN_ALREADY_PRECOMMITED;
        ss << "ransaction is already precommited: db_id=" << db_id << " txn_id=" << txn_id;
        msg = ss.str();
    }

    LOG(INFO) << "before update txn_info=" << txn_info.ShortDebugString();

    // Update txn_info
    txn_info.set_status(TxnStatusPB::TXN_STATUS_PRECOMMITTED);

    auto now_time = system_clock::now();
    uint64_t precommit_time = duration_cast<milliseconds>(now_time.time_since_epoch()).count();
    txn_info.set_precommit_time(precommit_time);
    if (request->has_commit_attachment()) {
        txn_info.mutable_commit_attachment()->CopyFrom(request->commit_attachment());
    }
    LOG(INFO) << "after update txn_info=" << txn_info.ShortDebugString();

    txn_inf_val.clear();
    if (!txn_info.SerializeToString(&txn_inf_val)) {
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        ss << "failed to serialize txn_info when saving, txn_id=" << txn_id;
        msg = ss.str();
        return;
    }

    LOG(INFO) << "xxx put txn_inf_key=" << hex(txn_inf_key) << " txn_id=" << txn_id;
    txn->put(txn_inf_key, txn_inf_val);

    std::string txn_run_key;
    std::string txn_run_val;
    TxnRunningKeyInfo txn_run_key_info {instance_id, db_id, txn_id};
    txn_running_key(txn_run_key_info, &txn_run_key);

    TxnRunningPB running_val_pb;
    running_val_pb.set_timeout_time(precommit_time + txn_info.precommit_timeout_ms());
    if (!running_val_pb.SerializeToString(&txn_run_val)) {
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        ss << "failed to serialize running_val_pb, txn_id=" << txn_id;
        msg = ss.str();
        return;
    }

    LOG(INFO) << "xxx put txn_run_key=" << hex(txn_run_key) << " txn_id=" << txn_id;
    txn->put(txn_run_key, txn_run_val);

    ret = txn->commit();
    if (ret != 0) {
        code = ret == -1 ? MetaServiceCode::KV_TXN_CONFLICT : MetaServiceCode::KV_TXN_COMMIT_ERR;
        ss << "failed to commit txn kv, txn_id=" << txn_id << " ret=" << ret;
        msg = ss.str();
        return;
    }
}

/**
 * 0. Extract txn_id from request
 * 1. Get db id from TxnKv with txn_id
 * 2. Get TxnInfo from TxnKv with db_id and txn_id
 * 3. Get tmp rowset meta, there may be several or hundred of tmp rowsets
 * 4. Get versions of each rowset
 * 5. Put rowset meta, which will be visible to user
 * 6. Put TxnInfo back into TxnKv with updated txn status (committed)
 * 7. Update versions of each partition
 * 8. Remove tmp rowset meta
 *
 * Note: getting version and all changes maded are in a single TxnKv transaction:
 *       step 5, 6, 7, 8
 */
void MetaServiceImpl::commit_txn(::google::protobuf::RpcController* controller,
                                 const ::selectdb::CommitTxnRequest* request,
                                 ::selectdb::CommitTxnResponse* response,
                                 ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(commit_txn);
    if (!request->has_txn_id()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "invalid argument, missing txn id";
        return;
    }

    int64_t txn_id = request->txn_id();

    std::string cloud_unique_id = request->has_cloud_unique_id() ? request->cloud_unique_id() : "";
    instance_id = get_instance_id(resource_mgr_, cloud_unique_id);
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << cloud_unique_id << " txn_id=" << txn_id;
        return;
    }

    RPC_RATE_LIMIT(commit_txn)

    // Create a readonly txn for scan tmp rowset
    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        ss << "filed to create txn, txn_id=" << txn_id;
        msg = ss.str();
        return;
    }

    //Get db id with txn id
    std::string txn_index_key_;
    std::string txn_index_val;
    TxnIndexKeyInfo txn_index_key_info {instance_id, txn_id};
    txn_index_key(txn_index_key_info, &txn_index_key_);
    ret = txn->get(txn_index_key_, &txn_index_val);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_GET_ERR;
        ss << "failed to get db id, txn_id=" << txn_id << " ret=" << ret;
        msg = ss.str();
        return;
    }

    TxnIndexPB txn_index_pb;
    if (!txn_index_pb.ParseFromString(txn_index_val)) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        ss << "failed to parse txn_index_pb, txn_id=" << txn_id;
        msg = ss.str();
        return;
    }

    DCHECK(txn_index_pb.has_tablet_index() == true);
    DCHECK(txn_index_pb.tablet_index().has_db_id() == true);
    int64_t db_id = txn_index_pb.tablet_index().db_id();

    // Get temporary rowsets involved in the txn
    // This is a range scan
    MetaRowsetTmpKeyInfo rs_tmp_key_info0 {instance_id, txn_id, 0};
    MetaRowsetTmpKeyInfo rs_tmp_key_info1 {instance_id, txn_id + 1, 0};
    std::string rs_tmp_key0;
    std::string rs_tmp_key1;
    meta_rowset_tmp_key(rs_tmp_key_info0, &rs_tmp_key0);
    meta_rowset_tmp_key(rs_tmp_key_info1, &rs_tmp_key1);
    // Get rowset meta that should be commited
    //                   tmp_rowset_key -> rowset_meta
    std::vector<std::pair<std::string, doris::RowsetMetaPB>> tmp_rowsets_meta;

    int num_rowsets = 0;
    std::unique_ptr<int, std::function<void(int*)>> defer_log_range(
            (int*)0x01, [rs_tmp_key0, rs_tmp_key1, &num_rowsets, &txn_id](int*) {
                LOG(INFO) << "get tmp rowset meta, txn_id=" << txn_id
                          << " num_rowsets=" << num_rowsets << " range=[" << hex(rs_tmp_key0) << ","
                          << hex(rs_tmp_key1) << ")";
            });

    std::unique_ptr<RangeGetIterator> it;
    do {
        ret = txn->get(rs_tmp_key0, rs_tmp_key1, &it);
        if (ret != 0) {
            code = MetaServiceCode::KV_TXN_GET_ERR;
            ss << "internal error, failed to get tmp rowset while committing, txn_id=" << txn_id
               << " ret=" << ret;
            msg = ss.str();
            return;
        }

        while (it->has_next()) {
            auto [k, v] = it->next();
            LOG(INFO) << "range_get rowset_tmp_key=" << hex(k) << " txn_id=" << txn_id;
            tmp_rowsets_meta.emplace_back();
            if (!tmp_rowsets_meta.back().second.ParseFromArray(v.data(), v.size())) {
                code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                ss << "malformed rowset meta, unable to initialize, txn_id=" << txn_id;
                msg = ss.str();
                ss << " key=" << hex(k);
                LOG(WARNING) << ss.str();
                return;
            }
            // Save keys that will be removed later
            tmp_rowsets_meta.back().first = std::string(k.data(), k.size());
            ++num_rowsets;
            if (!it->has_next()) rs_tmp_key0 = k;
        }
        rs_tmp_key0.push_back('\x00'); // Update to next smallest key for iteration
    } while (it->more());

    VLOG_DEBUG << "txn_id=" << txn_id << " tmp_rowsets_meta.size()=" << tmp_rowsets_meta.size();

    // Create a read/write txn for guarantee consistency
    txn.reset();
    ret = txn_kv_->create_txn(&txn);
    int64_t put_size = 0;
    int64_t del_size = 0;
    int num_put_keys = 0, num_del_keys = 0;
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        ss << "filed to create txn, txn_id=" << txn_id;
        msg = ss.str();
        return;
    }

    // Get txn info with db_id and txn_id
    std::string txn_inf_key; // Will be used when saving updated txn
    std::string txn_inf_val; // Will be reused when saving updated txn
    TxnInfoKeyInfo txn_inf_key_info {instance_id, db_id, txn_id};
    txn_info_key(txn_inf_key_info, &txn_inf_key);
    ret = txn->get(txn_inf_key, &txn_inf_val);
    if (ret != 0) {
        code = ret > 0 ? MetaServiceCode::TXN_ID_NOT_FOUND : MetaServiceCode::KV_TXN_GET_ERR;
        ss << "failed to get txn_info, db_id=" << db_id << " txn_id=" << txn_id << " ret=" << ret;
        msg = ss.str();
        return;
    }

    TxnInfoPB txn_info;
    if (!txn_info.ParseFromString(txn_inf_val)) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        ss << "failed to parse txn_info, db_id=" << db_id << " txn_id=" << txn_id;
        msg = ss.str();
        return;
    }

    // TODO: do more check like txn state, 2PC etc.
    DCHECK(txn_info.txn_id() == txn_id);
    if (txn_info.status() == TxnStatusPB::TXN_STATUS_ABORTED) {
        code = MetaServiceCode::TXN_ALREADY_ABORTED;
        ss << "transaction is already aborted: db_id=" << db_id << " txn_id=" << txn_id;
        msg = ss.str();
        return;
    }

    if (txn_info.status() == TxnStatusPB::TXN_STATUS_VISIBLE) {
        code = MetaServiceCode::TXN_ALREADY_VISIBLE;
        ss << "transaction is already visible: db_id=" << db_id << " txn_id=" << txn_id;
        msg = ss.str();
        response->mutable_txn_info()->CopyFrom(txn_info);
        return;
    }

    if (request->has_is_2pc() && request->is_2pc() && txn_info.status() == TxnStatusPB::TXN_STATUS_PREPARED) {
        code = MetaServiceCode::TXN_INVALID_STATUS;
        ss << "transaction is prepare, not pre-committed: db_id=" << db_id << " txn_id" << txn_id;
        msg = ss.str();
        return;
    }

    LOG(INFO) << "txn_id=" << txn_id << " txn_info=" << txn_info.ShortDebugString();

    // Prepare rowset meta and new_versions
    std::vector<std::pair<std::string, std::string>> rowsets;
    std::map<std::string, uint64_t> new_versions;
    std::map<int64_t, TabletStats> tablet_stats; // tablet_id -> stats
    std::map<int64_t, TabletIndexPB> table_ids;  // tablet_id -> {table/index/partition}_id
    std::map<int64_t, std::vector<int64_t>> table_id_tablet_ids; // table_id -> tablets_ids
    std::map<int64_t, std::vector<int64_t>> table_id_partition_ids;
    rowsets.reserve(tmp_rowsets_meta.size());
    for (auto& [_, i] : tmp_rowsets_meta) {
        int64_t tablet_id = i.tablet_id();
        // Get version for the rowset
        if (table_ids.count(tablet_id) == 0) {
            MetaTabletIdxKeyInfo key_info {instance_id, tablet_id};
            auto [key, val] = std::make_tuple(std::string(""), std::string(""));
            meta_tablet_idx_key(key_info, &key);
            ret = txn->get(key, &val);
            if (ret != 0) { // Must be 0, an existing value
                code = MetaServiceCode::KV_TXN_GET_ERR;
                ss << "failed to get tablet table index ids,"
                   << (ret == 1 ? " not found" : " internal error") << " tablet_id=" << tablet_id
                   << " key=" << hex(key);
                msg = ss.str();
                LOG(INFO) << msg << " ret=" << ret << " txn_id=" << txn_id;
                return;
            }
            if (!table_ids[tablet_id].ParseFromString(val)) {
                code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                ss << "malformed tablet index value tablet_id=" << tablet_id
                   << " txn_id=" << txn_id;
                msg = ss.str();
                return;
            }
            table_id_tablet_ids[table_ids[tablet_id].table_id()].push_back(tablet_id);
            VLOG_DEBUG << "tablet_id:" << tablet_id
                       << " value:" << table_ids[tablet_id].ShortDebugString();
        }

        int64_t table_id = table_ids[tablet_id].table_id();
        int64_t partition_id = i.partition_id();

        VersionKeyInfo ver_key_info {instance_id, db_id, table_id, partition_id};
        std::string ver_key;
        version_key(ver_key_info, &ver_key);
        int64_t version = -1;
        std::string ver_val_str;
        int64_t new_version = -1;
        VersionPB version_pb;
        if (new_versions.count(ver_key) == 0) {
            ret = txn->get(ver_key, &ver_val_str);
            if (ret != 1 && ret != 0) {
                code = MetaServiceCode::KV_TXN_GET_ERR;
                ss << "failed to get version, table_id=" << table_id
                   << "partition_id=" << partition_id << " key=" << hex(ver_key);
                msg = ss.str();
                LOG(INFO) << msg << " txn_id=" << txn_id;
                return;
            }

            if (ret == 1) {
                // Maybe first version
                version = 1;
            } else {
                if (!version_pb.ParseFromString(ver_val_str)) {
                    code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                    ss << "failed to parse ver_val_str"
                       << " txn_id=" << txn_id << " key=" << hex(ver_key);
                    msg = ss.str();
                    return;
                }
                version = version_pb.version();
            }
            new_version = version + 1;
            new_versions.insert({std::move(ver_key), new_version});
            table_id_partition_ids[table_id].push_back(partition_id);
        } else {
            new_version = new_versions[ver_key];
        }

        // Update rowset version
        i.set_start_version(new_version);
        i.set_end_version(new_version);

        std::string key, val;
        MetaRowsetKeyInfo key_info {instance_id, tablet_id, i.end_version()};
        meta_rowset_key(key_info, &key);
        if (!i.SerializeToString(&val)) {
            code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
            ss << "failed to serialize rowset_meta, txn_id=" << txn_id;
            msg = ss.str();
            return;
        }
        rowsets.emplace_back(std::move(key), std::move(val));

        // Accumulate affected rows
        auto& stats = tablet_stats[tablet_id];
        stats.data_size += i.data_disk_size();
        stats.num_rows += i.num_rows();
        ++stats.num_rowsets;
        stats.num_segs += i.num_segments();
    } // for tmp_rowsets_meta

    // process mow table, check lock and remove pending key
    for (auto table_id : request->mow_table_ids()) {
        for (auto partition_id : table_id_partition_ids[table_id]) {
            std::string lock_key =
                    meta_delete_bitmap_update_lock_key({instance_id, table_id, partition_id});
            std::string lock_val;
            ret = txn->get(lock_key, &lock_val);
            LOG(INFO) << "get delete bitmap update lock info, table_id=" << table_id
                      << " key=" << hex(lock_key) << " ret=" << ret;
            if (ret != 0) {
                ss << "failed to get delete bitmap update lock key info, instance_id="
                   << instance_id << " table_id=" << table_id << " key=" << hex(lock_key)
                   << " ret=" << ret;
                msg = ss.str();
                code = MetaServiceCode::KV_TXN_GET_ERR;
                return;
            }
            DeleteBitmapUpdateLockPB lock_info;
            if (!lock_info.ParseFromString(lock_val)) [[unlikely]] {
                code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                msg = "failed to parse DeleteBitmapUpdateLockPB";
                return;
            }
            if (lock_info.lock_id() != request->txn_id()) {
                msg = "lock is expired";
                code = MetaServiceCode::LOCK_EXPIRED;
                return;
            }
            txn->remove(lock_key);
            LOG(INFO) << "xxx remove delete bitmap lock, lock_key=" << hex(lock_key)
                      << " txn_id=" << txn_id;
        }

        for (auto tablet_id : table_id_tablet_ids[table_id]) {
            std::string pending_key = meta_pending_delete_bitmap_key({instance_id, tablet_id});
            txn->remove(pending_key);
            LOG(INFO) << "xxx remove delete bitmap pending key, pending_key=" << hex(pending_key)
                      << " txn_id=" << txn_id;
        }
    }

    // Save rowset meta
    num_put_keys += rowsets.size();
    for (auto& i : rowsets) {
        size_t rowset_size = i.first.size() + i.second.size();
        txn->put(i.first, i.second);
        put_size += rowset_size;
        LOG(INFO) << "xxx put rowset_key=" << hex(i.first) << " txn_id=" << txn_id
                  << " rowset_size=" << rowset_size;
    }

    // Save versions
    num_put_keys += new_versions.size();
    for (auto& i : new_versions) {
        std::string ver_val;
        VersionPB version_pb;
        version_pb.set_version(i.second);
        if (!version_pb.SerializeToString(&ver_val)) {
            code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
            ss << "failed to serialize version_pb when saving, txn_id=" << txn_id;
            msg = ss.str();
            return;
        }

        txn->put(i.first, ver_val);
        put_size += i.first.size() + ver_val.size();
        LOG(INFO) << "xxx put version_key=" << hex(i.first) << " version:" << i.second
                  << " txn_id=" << txn_id;

        std::string_view ver_key = i.first;
        //VersionKeyInfo  {instance_id, db_id, table_id, partition_id}
        ver_key.remove_prefix(1); // Remove key space
        std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
        ret = decode_key(&ver_key, &out);
        if (ret != 0) [[unlikely]] {
            // decode version key error means this is something wrong,
            // we can not continue this txn
            LOG(WARNING) << "failed to decode key, ret=" << ret << " key=" << hex(ver_key);
            code = MetaServiceCode::UNDEFINED_ERR;
            msg = "decode version key error";
            return;
        }

        int64_t table_id = std::get<int64_t>(std::get<0>(out[4]));
        int64_t partition_id = std::get<int64_t>(std::get<0>(out[5]));
        VLOG_DEBUG << " table_id=" << table_id << " partition_id=" << partition_id;

        response->add_table_ids(table_id);
        response->add_partition_ids(partition_id);
        response->add_versions(i.second);
    }

    LOG(INFO) << " before update txn_info=" << txn_info.ShortDebugString();

    // Update txn_info
    txn_info.set_status(TxnStatusPB::TXN_STATUS_VISIBLE);

    auto now_time = system_clock::now();
    uint64_t commit_time = duration_cast<milliseconds>(now_time.time_since_epoch()).count();
    if ((txn_info.prepare_time() + txn_info.timeout_ms()) < commit_time) {
        code = MetaServiceCode::UNDEFINED_ERR;
        msg = fmt::format("txn is expired, not allow to commit txn_id={}", txn_id);
        LOG(INFO) << msg << " prepare_time=" << txn_info.prepare_time()
                  << " timeout_ms=" << txn_info.timeout_ms() << " commit_time=" << commit_time;
        return;
    }
    txn_info.set_commit_time(commit_time);
    txn_info.set_finish_time(commit_time);
    if (request->has_commit_attachment()) {
        txn_info.mutable_commit_attachment()->CopyFrom(request->commit_attachment());
    }
    LOG(INFO) << "after update txn_info=" << txn_info.ShortDebugString();
    txn_inf_val.clear();
    if (!txn_info.SerializeToString(&txn_inf_val)) {
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        ss << "failed to serialize txn_info when saving, txn_id=" << txn_id;
        msg = ss.str();
        return;
    }
    txn->put(txn_inf_key, txn_inf_val);
    put_size += txn_inf_key.size() + txn_inf_val.size();
    ++num_put_keys;
    LOG(INFO) << "xxx put txn_inf_key=" << hex(txn_inf_key) << " txn_id=" << txn_id;

    // Update stats of affected tablet
    std::deque<std::string> kv_pool;
    std::function<void(const StatsTabletKeyInfo&, const TabletStats&)> update_tablet_stats;
    if (config::split_tablet_stats) {
        update_tablet_stats = [&](const StatsTabletKeyInfo& info, const TabletStats& stats) {
            if (stats.num_segs > 0) {
                auto& data_size_key = kv_pool.emplace_back();
                stats_tablet_data_size_key(info, &data_size_key);
                txn->atomic_add(data_size_key, stats.data_size);
                auto& num_rows_key = kv_pool.emplace_back();
                stats_tablet_num_rows_key(info, &num_rows_key);
                txn->atomic_add(num_rows_key, stats.num_rows);
                auto& num_segs_key = kv_pool.emplace_back();
                stats_tablet_num_segs_key(info, &num_segs_key);
                txn->atomic_add(num_segs_key, stats.num_segs);
                put_size += data_size_key.size() + num_rows_key.size() + num_segs_key.size() + 24;
                num_put_keys += 3;
            }
            auto& num_rowsets_key = kv_pool.emplace_back();
            stats_tablet_num_rowsets_key(info, &num_rowsets_key);
            txn->atomic_add(num_rowsets_key, stats.num_rowsets);
            put_size += num_rowsets_key.size() + 8;
            ++num_put_keys;
        };
    } else {
        update_tablet_stats = [&](const StatsTabletKeyInfo& info, const TabletStats& stats) {
            auto& key = kv_pool.emplace_back();
            stats_tablet_key(info, &key);
            auto& val = kv_pool.emplace_back();
            ret = txn->get(key, &val);
            if (ret != 0) {
                code = ret == 1 ? MetaServiceCode::TABLET_NOT_FOUND
                                : MetaServiceCode::KV_TXN_GET_ERR;
                msg = fmt::format("failed to get tablet stats, ret={} tablet_id={}", ret,
                                  std::get<4>(info));
                return;
            }
            TabletStatsPB stats_pb;
            if (!stats_pb.ParseFromString(val)) {
                code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                msg = fmt::format("malformed tablet stats value, key={}", hex(key));
                return;
            }
            stats_pb.set_data_size(stats_pb.data_size() + stats.data_size);
            stats_pb.set_num_rows(stats_pb.num_rows() + stats.num_rows);
            stats_pb.set_num_rowsets(stats_pb.num_rowsets() + stats.num_rowsets);
            stats_pb.set_num_segments(stats_pb.num_segments() + stats.num_segs);
            stats_pb.SerializeToString(&val);
            txn->put(key, val);
            put_size += key.size() + val.size();
            ++num_put_keys;
        };
    }
    for (auto& [tablet_id, stats] : tablet_stats) {
        DCHECK(table_ids.count(tablet_id));
        auto& tablet_idx = table_ids[tablet_id];
        StatsTabletKeyInfo info {instance_id, tablet_idx.table_id(), tablet_idx.index_id(),
                                 tablet_idx.partition_id(), tablet_id};
        update_tablet_stats(info, stats);
        if (code != MetaServiceCode::OK) return;
    }
    // Remove tmp rowset meta
    num_del_keys += tmp_rowsets_meta.size();
    for (auto& [k, _] : tmp_rowsets_meta) {
        txn->remove(k);
        del_size += k.size();
        LOG(INFO) << "xxx remove tmp_rowset_key=" << hex(k) << " txn_id=" << txn_id;
    }

    std::string txn_run_key;
    TxnRunningKeyInfo txn_run_key_info {instance_id, db_id, txn_id};
    txn_running_key(txn_run_key_info, &txn_run_key);
    LOG(INFO) << "xxx remove txn_run_key=" << hex(txn_run_key) << " txn_id=" << txn_id;
    txn->remove(txn_run_key);
    del_size += txn_run_key.size();
    ++num_del_keys;

    std::string recycle_txn_key_;
    std::string recycle_txn_val;
    RecycleTxnKeyInfo recycle_txn_key_info {instance_id, db_id, txn_id};

    recycle_txn_key(recycle_txn_key_info, &recycle_txn_key_);
    RecycleTxnPB recycle_txn_pb;
    recycle_txn_pb.set_creation_time(commit_time);
    recycle_txn_pb.set_label(txn_info.label());

    if (!recycle_txn_pb.SerializeToString(&recycle_txn_val)) {
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        ss << "failed to serialize running_val_pb, txn_id=" << txn_id;
        msg = ss.str();
        return;
    }
    txn->put(recycle_txn_key_, recycle_txn_val);
    put_size += recycle_txn_key_.size() + recycle_txn_val.size();
    ++num_put_keys;

    if (txn_info.load_job_source_type() == LoadJobSourceTypePB::LOAD_JOB_SRC_TYPE_ROUTINE_LOAD_TASK) {
        if (!request->has_commit_attachment()) {
            ss << "failed to get commit attachment from req, db_id=" << db_id << " txn_id=" << txn_id;
            msg = ss.str();
            return;
        }

        TxnCommitAttachmentPB txn_commit_attachment = request->commit_attachment();
        RLTaskTxnCommitAttachmentPB commit_attachment = txn_commit_attachment.rl_task_txn_commit_attachment();
        int64_t job_id = commit_attachment.job_id();

        std::string rl_progress_key;
        std::string rl_progress_val;
        bool prev_progress_existed = true;
        RLJobProgressKeyInfo rl_progress_key_info {instance_id, db_id, job_id};
        rl_job_progress_key_info(rl_progress_key_info, &rl_progress_key);
        ret = txn->get(rl_progress_key, &rl_progress_val);
        if (ret != 0) {
            if (ret > 0) {
                prev_progress_existed = false;
            } else {
                code = MetaServiceCode::KV_TXN_GET_ERR;
                ss << "failed to get txn_info, db_id=" << db_id << " txn_id=" << txn_id << " ret=" << ret;
                msg = ss.str();
                return;
            }
        }

        RoutineLoadProgressPB prev_progress_info;
        if (prev_progress_existed) {
            if (!prev_progress_info.ParseFromString(rl_progress_val)) {
                code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                ss << "failed to parse txn_info, db_id=" << db_id << " txn_id=" << txn_id;
                msg = ss.str();
                return;
            }

            int cal_row_num = 0;
            for (auto const& elem : commit_attachment.progress().partition_to_offset()) {
                if (elem.second >= 0) {
                    auto it = prev_progress_info.partition_to_offset().find(elem.first);
                    if (it != prev_progress_info.partition_to_offset().end() && it->second >= 0) {
                        cal_row_num += elem.second - it->second;
                    } else {
                        cal_row_num += elem.second + 1;
                    }
                }
            }

            LOG(INFO) << " caculated row num " << cal_row_num
                      << " actual row num " << commit_attachment.loaded_rows()
                      << " prev prgress " << prev_progress_info.DebugString();

            if (cal_row_num != commit_attachment.loaded_rows()) {
                if (cal_row_num == 0) {
                    LOG(WARNING) << " repeated to load task in routine load, db_id=" << db_id << " txn_id=" << txn_id
                                 << " caculated row num " << cal_row_num
                                 << " actual row num " << commit_attachment.loaded_rows();
                    return;
                }

                code = MetaServiceCode::ROUTINE_LOAD_DATA_INCONSISTENT;
                ss << " repeated to load task in routine load, db_id=" << db_id << " txn_id=" << txn_id
                   << " caculated row num " << cal_row_num 
                   << " actual row num " << commit_attachment.loaded_rows();
                msg = ss.str();
                return;
            }
        }

        std::string new_progress_val;
        RoutineLoadProgressPB new_progress_info;
        new_progress_info.CopyFrom(commit_attachment.progress());
        for (auto const& elem : prev_progress_info.partition_to_offset()) {
            auto it = new_progress_info.partition_to_offset().find(elem.first);
            if (it == new_progress_info.partition_to_offset().end()) {
                new_progress_info.mutable_partition_to_offset()->insert(elem);
            }
        }

        if (!new_progress_info.SerializeToString(&new_progress_val)) {
            code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
            ss << "failed to serialize new progress val, txn_id=" << txn_info.txn_id();
            msg = ss.str();
            return;
        }
        txn->put(rl_progress_key, new_progress_val);
    }

    LOG(INFO) << "xxx commit_txn put recycle_txn_key key=" << hex(recycle_txn_key_)
              << " txn_id=" << txn_id;
    LOG(INFO) << "commit_txn put_size=" << put_size << " del_size=" << del_size
              << " num_put_keys=" << num_put_keys << " num_del_keys=" << num_del_keys
              << " txn_id=" << txn_id;

    // Finally we are done...
    ret = txn->commit();
    if (ret != 0) {
        code = ret == -1 ? MetaServiceCode::KV_TXN_CONFLICT : MetaServiceCode::KV_TXN_COMMIT_ERR;
        ss << "failed to commit kv txn, txn_id=" << txn_id << " ret=" << ret;
        msg = ss.str();
        return;
    }
    response->mutable_txn_info()->CopyFrom(txn_info);
} // end commit_txn

void MetaServiceImpl::abort_txn(::google::protobuf::RpcController* controller,
                                const ::selectdb::AbortTxnRequest* request,
                                ::selectdb::AbortTxnResponse* response,
                                ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(abort_txn);
    // Get txn id
    int64_t txn_id = request->has_txn_id() ? request->txn_id() : -1;
    std::string label = request->has_label() ? request->label() : "";
    int64_t db_id = request->has_db_id() ? request->db_id() : -1;
    if (txn_id < 0 && (label.empty() || db_id < 0)) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        ss << "invalid txn id and label, db_id=" << db_id << " txn_id=" << txn_id
           << " label=" << label;
        msg = ss.str();
        return;
    }

    std::string cloud_unique_id = request->has_cloud_unique_id() ? request->cloud_unique_id() : "";
    instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        ss << "cannot find instance_id with cloud_unique_id="
           << (cloud_unique_id.empty() ? "(empty)" : cloud_unique_id) << " label=" << label
           << " txn_id=" << txn_id;
        msg = ss.str();
        return;
    }

    RPC_RATE_LIMIT(abort_txn);
    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        ss << "filed to txn_kv_->create_txn(), txn_id=" << txn_id << " label=" << label;
        msg = ss.str();
        return;
    }

    std::string txn_inf_key; // Will be used when saving updated txn
    std::string txn_inf_val; // Will be reused when saving updated txn
    TxnInfoPB txn_info;

    //TODO: split with two function.
    //there two ways to abort txn:
    //1. abort txn by txn id
    //2. abort txn by label and db_id
    if (txn_id > 0) {
        VLOG_DEBUG << "abort_txn by txn_id";
        //abort txn by txn id
        // Get db id with txn id

        std::string txn_index_key_;
        std::string txn_index_val;

        //not provide db_id, we need read from disk.
        if (!request->has_db_id()) {
            TxnIndexKeyInfo txn_index_key_info {instance_id, txn_id};
            txn_index_key(txn_index_key_info, &txn_index_key_);
            ret = txn->get(txn_index_key_, &txn_index_val);
            if (ret != 0) {
                code = ret > 0 ? MetaServiceCode::TXN_ID_NOT_FOUND
                               : MetaServiceCode::KV_TXN_GET_ERR;
                ss << "failed to get db id, txn_id=" << txn_id << " ret=" << ret;
                msg = ss.str();
                return;
            }

            TxnIndexPB txn_index_pb;
            if (!txn_index_pb.ParseFromString(txn_index_val)) {
                code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                ss << "failed to parse txn_index_val"
                   << " txn_id=" << txn_id;
                msg = ss.str();
                return;
            }
            DCHECK(txn_index_pb.has_tablet_index() == true);
            DCHECK(txn_index_pb.tablet_index().has_db_id() == true);
            db_id = txn_index_pb.tablet_index().db_id();
        } else {
            db_id = request->db_id();
        }

        // Get txn info with db_id and txn_id
        TxnInfoKeyInfo txn_inf_key_info {instance_id, db_id, txn_id};
        txn_info_key(txn_inf_key_info, &txn_inf_key);
        ret = txn->get(txn_inf_key, &txn_inf_val);
        if (ret != 0) {
            code = ret > 0 ? MetaServiceCode::TXN_ID_NOT_FOUND : MetaServiceCode::KV_TXN_GET_ERR;
            ss << "failed to get txn_info, db_id=" << db_id << "txn_id=" << txn_id << "ret=" << ret;
            msg = ss.str();
            return;
        }

        if (!txn_info.ParseFromString(txn_inf_val)) {
            code = MetaServiceCode::PROTOBUF_PARSE_ERR;
            ss << "failed to parse txn_info db_id=" << db_id << "txn_id=" << txn_id;
            msg = ss.str();
            return;
        }

        DCHECK(txn_info.txn_id() == txn_id);

        //check state is valid.
        if (txn_info.status() == TxnStatusPB::TXN_STATUS_ABORTED) {
            code = MetaServiceCode::TXN_ALREADY_ABORTED;
            ss << "transaction is already abort db_id=" << db_id << "txn_id=" << txn_id;
            msg = ss.str();
            return;
        }
        if (txn_info.status() == TxnStatusPB::TXN_STATUS_VISIBLE) {
            code = MetaServiceCode::TXN_ALREADY_VISIBLE;
            ss << "transaction is already visible db_id=" << db_id << "txn_id=" << txn_id;
            msg = ss.str();
            return;
        }
    } else {
        VLOG_DEBUG << "abort_txn by db_id and txn label";
        //abort txn by label.
        std::string txn_label_key_;
        std::string txn_label_val;

        TxnLabelKeyInfo txn_label_key_info {instance_id, db_id, label};
        txn_label_key(txn_label_key_info, &txn_label_key_);
        ret = txn->get(txn_label_key_, &txn_label_val);
        if (ret < 0) {
            code = MetaServiceCode::KV_TXN_GET_ERR;
            ss << "txn->get() failed, label=" << label << " ret=" << ret;
            msg = ss.str();
            return;
        }
        //label index not exist
        if (ret > 0) {
            code = MetaServiceCode::TXN_LABEL_NOT_FOUND;
            ss << "label not found, db_id=" << db_id << " label=" << label << " ret=" << ret;
            msg = ss.str();
            return;
        }

        TxnLabelPB txn_label_pb;
        DCHECK(txn_label_val.size() > 10);
        std::string txn_label_pb_str = txn_label_val.substr(0, txn_label_val.size() - 10);
        if (!txn_label_pb.ParseFromString(txn_label_pb_str)) {
            code = MetaServiceCode::PROTOBUF_PARSE_ERR;
            ss << "txn_label_pb->ParseFromString() failed, label=" << label;
            msg = ss.str();
            return;
        }

        int64_t prepare_txn_id = 0;
        //found prepare state txn for abort
        for (auto& cur_txn_id : txn_label_pb.txn_ids()) {
            std::string cur_txn_inf_key;
            std::string cur_txn_inf_val;
            TxnInfoKeyInfo cur_txn_inf_key_info {instance_id, db_id, cur_txn_id};
            txn_info_key(cur_txn_inf_key_info, &cur_txn_inf_key);
            ret = txn->get(cur_txn_inf_key, &cur_txn_inf_val);
            if (ret != 0) {
                code = MetaServiceCode::KV_TXN_GET_ERR;
                std::stringstream ss;
                ss << "txn->get() failed, cur_txn_id=" << cur_txn_id << " ret=" << ret;
                msg = ss.str();
                return;
            }
            // ret == 0
            TxnInfoPB cur_txn_info;
            if (!cur_txn_info.ParseFromString(cur_txn_inf_val)) {
                code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                std::stringstream ss;
                ss << "cur_txn_info->ParseFromString() failed, cur_txn_id=" << cur_txn_id;
                msg = ss.str();
                return;
            }
            VLOG_DEBUG << "cur_txn_info=" << cur_txn_info.ShortDebugString();
            //TODO: 2pc alse need to check TxnStatusPB::TXN_STATUS_PRECOMMITTED
            if ((cur_txn_info.status() == TxnStatusPB::TXN_STATUS_PREPARED) ||
                (cur_txn_info.status() == TxnStatusPB::TXN_STATUS_PRECOMMITTED)) {
                prepare_txn_id = cur_txn_id;
                txn_info = std::move(cur_txn_info);
                txn_inf_key = std::move(cur_txn_inf_key);
                DCHECK_EQ(prepare_txn_id, txn_info.txn_id())
                        << "prepare_txn_id=" << prepare_txn_id << " txn_id=" << txn_info.txn_id();
                break;
            }
        }

        if (prepare_txn_id == 0) {
            code = MetaServiceCode::TXN_INVALID_STATUS;
            std::stringstream ss;
            ss << "running transaction not found, db_id=" << db_id << " label=" << label;
            msg = ss.str();
            return;
        }
    }

    auto now_time = system_clock::now();
    uint64_t finish_time = duration_cast<milliseconds>(now_time.time_since_epoch()).count();

    // Update txn_info
    txn_info.set_status(TxnStatusPB::TXN_STATUS_ABORTED);
    txn_info.set_finish_time(finish_time);
    request->has_reason() ? txn_info.set_reason(request->reason())
                          : txn_info.set_reason("User Abort");

    if (request->has_commit_attachment()) {
        TxnCommitAttachmentPB attachement = request->commit_attachment();
        txn_info.mutable_commit_attachment()->CopyFrom(request->commit_attachment());
    }

    txn_inf_val.clear();
    if (!txn_info.SerializeToString(&txn_inf_val)) {
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        ss << "failed to serialize txn_info when saving, txn_id=" << txn_info.txn_id();
        msg = ss.str();
        return;
    }
    LOG(INFO) << "check watermark conflict, txn_info=" << txn_info.ShortDebugString();
    txn->put(txn_inf_key, txn_inf_val);
    LOG(INFO) << "xxx put txn_inf_key=" << hex(txn_inf_key) << " txn_id=" << txn_info.txn_id();

    std::string txn_run_key;
    TxnRunningKeyInfo txn_run_key_info {instance_id, db_id, txn_info.txn_id()};
    txn_running_key(txn_run_key_info, &txn_run_key);
    txn->remove(txn_run_key);
    LOG(INFO) << "xxx remove txn_run_key=" << hex(txn_run_key) << " txn_id=" << txn_info.txn_id();

    std::string recycle_txn_key_;
    std::string recycle_txn_val;
    RecycleTxnKeyInfo recycle_txn_key_info {instance_id, db_id, txn_info.txn_id()};
    recycle_txn_key(recycle_txn_key_info, &recycle_txn_key_);
    RecycleTxnPB recycle_txn_pb;
    recycle_txn_pb.set_creation_time(finish_time);
    recycle_txn_pb.set_label(txn_info.label());

    if (!recycle_txn_pb.SerializeToString(&recycle_txn_val)) {
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        ss << "failed to serialize running_val_pb, txn_id=" << txn_info.txn_id();
        msg = ss.str();
        return;
    }
    txn->put(recycle_txn_key_, recycle_txn_val);
    LOG(INFO) << "xxx put recycle_txn_key=" << hex(recycle_txn_key_)
              << " txn_id=" << txn_info.txn_id();

    ret = txn->commit();
    if (ret != 0) {
        code = ret == -1 ? MetaServiceCode::KV_TXN_CONFLICT : MetaServiceCode::KV_TXN_COMMIT_ERR;
        ss << "failed to commit kv txn, txn_id=" << txn_info.txn_id() << " ret=" << ret;
        msg = ss.str();
        return;
    }
    response->mutable_txn_info()->CopyFrom(txn_info);
}

void MetaServiceImpl::get_txn(::google::protobuf::RpcController* controller,
                              const ::selectdb::GetTxnRequest* request,
                              ::selectdb::GetTxnResponse* response,
                              ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(get_txn);
    int64_t txn_id = request->has_txn_id() ? request->txn_id() : -1;
    int64_t db_id = request->has_db_id() ? request->db_id() : -1;
    if (txn_id < 0) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        ss << "invalid txn_id, it may be not given or set properly, txn_id=" << txn_id;
        msg = ss.str();
        return;
    }

    std::string cloud_unique_id = request->has_cloud_unique_id() ? request->cloud_unique_id() : "";
    instance_id = get_instance_id(resource_mgr_, cloud_unique_id);
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        ss << "cannot find instance_id with cloud_unique_id="
           << (cloud_unique_id.empty() ? "(empty)" : cloud_unique_id);
        msg = ss.str();
        return;
    }

    RPC_RATE_LIMIT(get_txn)
    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        ss << "failed to create txn, txn_id=" << txn_id;
        msg = ss.str();
        return;
    }

    //not provide db_id, we need read from disk.
    if (db_id < 0) {
        std::string txn_index_key_;
        std::string txn_index_val;
        TxnIndexKeyInfo txn_index_key_info {instance_id, txn_id};
        txn_index_key(txn_index_key_info, &txn_index_key_);
        ret = txn->get(txn_index_key_, &txn_index_val);
        if (ret != 0) {
            code = ret > 0 ? MetaServiceCode::TXN_ID_NOT_FOUND : MetaServiceCode::KV_TXN_GET_ERR;
            ss << "failed to get db id with txn_id=" << txn_id << " ret=" << ret;
            msg = ss.str();
            return;
        }

        TxnIndexPB txn_index_pb;
        if (!txn_index_pb.ParseFromString(txn_index_val)) {
            code = MetaServiceCode::PROTOBUF_PARSE_ERR;
            ss << "failed to parse txn_inf"
               << " txn_id=" << txn_id;
            msg = ss.str();
            return;
        }
        DCHECK(txn_index_pb.has_tablet_index() == true);
        DCHECK(txn_index_pb.tablet_index().has_db_id() == true);
        db_id = txn_index_pb.tablet_index().db_id();
        if (db_id <= 0) {
            ss << "internal error: unexpected db_id " << db_id;
            code = MetaServiceCode::UNDEFINED_ERR;
            msg = ss.str();
            return;
        }
    }

    // Get txn info with db_id and txn_id
    std::string txn_inf_key; // Will be used when saving updated txn
    std::string txn_inf_val; // Will be reused when saving updated txn
    TxnInfoKeyInfo txn_inf_key_info {instance_id, db_id, txn_id};
    txn_info_key(txn_inf_key_info, &txn_inf_key);
    ret = txn->get(txn_inf_key, &txn_inf_val);
    if (ret != 0) {
        code = ret > 0 ? MetaServiceCode::TXN_ID_NOT_FOUND : MetaServiceCode::KV_TXN_GET_ERR;
        ss << "failed to get db id with db_id=" << db_id << " txn_id=" << txn_id << " ret=" << ret;
        msg = ss.str();
        return;
    }

    TxnInfoPB txn_info;
    if (!txn_info.ParseFromString(txn_inf_val)) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        ss << "failed to parse txn_inf db_id=" << db_id << " txn_id=" << txn_id;
        msg = ss.str();
        return;
    }

    VLOG_DEBUG << "txn_info=" << txn_info.ShortDebugString();
    DCHECK(txn_info.txn_id() == txn_id);
    response->mutable_txn_info()->CopyFrom(txn_info);
    return;
}

//To get current max txn id for schema change watermark etc.
void MetaServiceImpl::get_current_max_txn_id(::google::protobuf::RpcController* controller,
                                             const ::selectdb::GetCurrentMaxTxnRequest* request,
                                             ::selectdb::GetCurrentMaxTxnResponse* response,
                                             ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(get_current_max_txn_id);
    // TODO: For auth
    instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << request->cloud_unique_id();
        return;
    }
    RPC_RATE_LIMIT(get_current_max_txn_id)
    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        msg = "failed to create txn";
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        return;
    }

    std::string key = "schema change";
    std::string val;
    ret = txn->get(key, &val);
    if (ret < 0) {
        code = MetaServiceCode::KV_TXN_GET_ERR;
        std::stringstream ss;
        ss << "txn->get() failed,"
           << " ret=" << ret;
        msg = ss.str();
        return;
    }
    int64_t read_version = txn->get_read_version();
    int64_t current_max_txn_id = read_version << 10;
    VLOG_DEBUG << "read_version=" << read_version << " current_max_txn_id=" << current_max_txn_id;
    response->set_current_max_txn_id(current_max_txn_id);
}

void MetaServiceImpl::check_txn_conflict(::google::protobuf::RpcController* controller,
                                         const ::selectdb::CheckTxnConflictRequest* request,
                                         ::selectdb::CheckTxnConflictResponse* response,
                                         ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(check_txn_conflict);
    if (!request->has_db_id() || !request->has_end_txn_id() || (request->table_ids_size() <= 0)) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "invalid db id, end txn id or table_ids.";
        return;
    }
    // TODO: For auth
    std::string cloud_unique_id = request->has_cloud_unique_id() ? request->cloud_unique_id() : "";
    instance_id = get_instance_id(resource_mgr_, cloud_unique_id);
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        ss << "cannot find instance_id with cloud_unique_id="
           << (cloud_unique_id.empty() ? "(empty)" : cloud_unique_id);
        msg = ss.str();
        return;
    }
    RPC_RATE_LIMIT(check_txn_conflict)
    int64_t db_id = request->db_id();
    std::string begin_txn_run_key;
    std::string begin_txn_run_val;
    std::string end_txn_run_key;
    std::string end_txn_run_val;
    TxnRunningKeyInfo begin_txn_run_key_info {instance_id, db_id, 0};
    TxnRunningKeyInfo end_txn_run_key_info {instance_id, db_id, request->end_txn_id()};
    txn_running_key(begin_txn_run_key_info, &begin_txn_run_key);
    txn_running_key(end_txn_run_key_info, &end_txn_run_key);
    LOG(INFO) << "begin_txn_run_key:" << hex(begin_txn_run_key)
              << " end_txn_run_key:" << hex(end_txn_run_key);

    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        msg = "failed to create txn";
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        return;
    }

    //TODO: use set to replace
    std::vector<int64_t> src_table_ids(request->table_ids().begin(), request->table_ids().end());
    std::sort(src_table_ids.begin(), src_table_ids.end());
    std::unique_ptr<RangeGetIterator> it;
    int64_t skip_timeout_txn_cnt = 0;
    int total_iteration_cnt = 0;
    do {
        ret = txn->get(begin_txn_run_key, end_txn_run_key, &it, 1000);
        if (ret != 0) {
            code = MetaServiceCode::KV_TXN_GET_ERR;
            ss << "failed to get txn running info. ret=" << ret;
            msg = ss.str();
            LOG(WARNING) << msg;
            return;
        }

        VLOG_DEBUG << "begin_txn_run_key=" << hex(begin_txn_run_key)
                   << " end_txn_run_val=" << hex(end_txn_run_val)
                   << " it->has_next()=" << it->has_next();

        auto now_time = system_clock::now();
        uint64_t check_time = duration_cast<milliseconds>(now_time.time_since_epoch()).count();
        while (it->has_next()) {
            total_iteration_cnt++;
            auto [k, v] = it->next();
            LOG(INFO) << "check watermark conflict range_get txn_run_key=" << hex(k);
            TxnRunningPB running_val_pb;
            if (!running_val_pb.ParseFromArray(v.data(), v.size())) {
                code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                ss << "malformed txn running info";
                msg = ss.str();
                ss << " key=" << hex(k);
                LOG(WARNING) << ss.str();
                return;
            }

            if (running_val_pb.timeout_time() < check_time) {
                skip_timeout_txn_cnt++;
                break;
            }

            LOG(INFO) << "check watermark conflict range_get txn_run_key=" << hex(k)
                      << " running_val_pb=" << running_val_pb.ShortDebugString();
            std::vector<int64_t> running_table_ids(running_val_pb.table_ids().begin(),
                                                   running_val_pb.table_ids().end());
            std::sort(running_table_ids.begin(), running_table_ids.end());
            std::vector<int64_t> result(std::min(running_table_ids.size(), src_table_ids.size()));
            std::vector<int64_t>::iterator iter = std::set_intersection(
                    src_table_ids.begin(), src_table_ids.end(), running_table_ids.begin(),
                    running_table_ids.end(), result.begin());
            result.resize(iter - result.begin());
            if (result.size() > 0) {
                response->set_finished(false);
                LOG(INFO) << "skip timeout txn count: " << skip_timeout_txn_cnt
                          << " total iteration count: " << total_iteration_cnt;
                return;
            }

            if (!it->has_next()) {
                begin_txn_run_key = k;
            }
        }
        begin_txn_run_key.push_back('\x00'); // Update to next smallest key for iteration
    } while (it->more());
    LOG(INFO) << "skip timeout txn count: " << skip_timeout_txn_cnt
              << " total iteration count: " << total_iteration_cnt;
    response->set_finished(true);
}

void MetaServiceImpl::get_version(::google::protobuf::RpcController* controller,
                                  const ::selectdb::GetVersionRequest* request,
                                  ::selectdb::GetVersionResponse* response,
                                  ::google::protobuf::Closure* done) {
    if (request->batch_mode()) {
        batch_get_version(controller, request, response, done);
        return;
    }

    RPC_PREPROCESS(get_version);
    // TODO(dx): For auth
    std::string cloud_unique_id;
    if (request->has_cloud_unique_id()) {
        cloud_unique_id = request->cloud_unique_id();
    }

    int64_t db_id = request->has_db_id() ? request->db_id() : -1;
    int64_t table_id = request->has_table_id() ? request->table_id() : -1;
    int64_t partition_id = request->has_partition_id() ? request->partition_id() : -1;
    if (db_id == -1 || table_id == -1 || partition_id == -1) {
        msg = "params error, db_id=" + std::to_string(db_id) +
              " table_id=" + std::to_string(table_id) +
              " partition_id=" + std::to_string(partition_id);
        code = MetaServiceCode::INVALID_ARGUMENT;
        LOG(WARNING) << msg;
        return;
    }

    instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << request->cloud_unique_id();
        return;
    }
    RPC_RATE_LIMIT(get_version)
    VersionKeyInfo ver_key_info {instance_id, db_id, table_id, partition_id};
    std::string ver_key;
    version_key(ver_key_info, &ver_key);

    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        msg = "failed to create txn";
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        return;
    }

    std::string ver_val;
    VersionPB version_pb;
    // 0 for success get a key, 1 for key not found, negative for error
    ret = txn->get(ver_key, &ver_val);
    LOG(INFO) << "xxx get version_key=" << hex(ver_key);
    if (ret == 0) {
        if (!version_pb.ParseFromString(ver_val)) {
            code = MetaServiceCode::PROTOBUF_PARSE_ERR;
            msg = "malformed version value";
            return;
        }
        response->set_version(version_pb.version());
        return;
    } else if (ret == 1) {
        msg = "not found";
        code = MetaServiceCode::VERSION_NOT_FOUND;
        return;
    }
    msg = "failed to get txn";
    code = MetaServiceCode::KV_TXN_GET_ERR;
}

void MetaServiceImpl::batch_get_version(::google::protobuf::RpcController* controller,
                                        const ::selectdb::GetVersionRequest* request,
                                        ::selectdb::GetVersionResponse* response,
                                        ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(get_version);

    std::string cloud_unique_id;
    if (request->has_cloud_unique_id()) {
        cloud_unique_id = request->cloud_unique_id();
    }

    int64_t db_id = request->has_db_id() ? request->db_id() : -1;
    if (db_id == -1 || request->table_ids_size() == 0 ||
        request->table_ids_size() != request->partition_ids_size()) {
        msg = "param error, db_id=" + std::to_string(db_id) +
              " num table_ids=" + std::to_string(request->table_ids_size()) +
              " num partition_ids=" + std::to_string(request->partition_ids_size());
        code = MetaServiceCode::INVALID_ARGUMENT;
        LOG(WARNING) << msg;
        return;
    }

    instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << request->cloud_unique_id();
        return;
    }

    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        msg = "failed to create txn";
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        return;
    }

    size_t num_acquired = request->table_ids_size();
    response->mutable_versions()->Reserve(num_acquired);
    response->mutable_table_ids()->CopyFrom(request->table_ids());
    response->mutable_partition_ids()->CopyFrom(request->partition_ids());
    for (size_t i = 0; i < num_acquired; ++i) {
        int64_t table_id = request->table_ids(i);
        int64_t partition_id = request->partition_ids(i);
        VersionKeyInfo ver_key_info {instance_id, db_id, table_id, partition_id};
        std::string ver_key;
        version_key(ver_key_info, &ver_key);

        // TODO(walter) support batch get.
        std::string ver_val;
        ret = txn->get(ver_key, &ver_val, true);
        LOG(INFO) << "xxx get version_key=" << hex(ver_key);
        if (ret == 0) {
            VersionPB version_pb;
            if (!version_pb.ParseFromString(ver_val)) {
                code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                msg = "malformed version value";
                break;
            }
            response->add_versions(version_pb.version());
        } else if (ret == 1) {
            // return -1 if the target version is not exists.
            response->add_versions(-1);
        } else {
            msg = "failed to get txn";
            code = MetaServiceCode::KV_TXN_GET_ERR;
            break;
        }
    }
    if (code != MetaServiceCode::OK) {
        response->clear_partition_ids();
        response->clear_table_ids();
        response->clear_versions();
    }
}

void put_schema_kv(MetaServiceCode& code, std::string& msg, Transaction* txn,
                   const std::string& instance_id, int64_t index_id,
                   const doris::TabletSchemaPB& schema, std::string& schema_key,
                   std::string& schema_val) {
    DCHECK(schema_key.empty());
    DCHECK(schema_val.empty());
    meta_schema_key({instance_id, index_id, schema.schema_version()}, &schema_key);
    if (txn->get(schema_key, &schema_val) == 0) {
        DCHECK([&] {
            auto transform = [](std::string_view type) -> std::string_view {
                if (type == "DECIMALV2") return "DECIMAL";
                if (type == "BITMAP") return "OBJECT";
                return type;
            };
            doris::TabletSchemaPB saved_schema;
            if (!saved_schema.ParseFromString(schema_val)) {
                LOG(WARNING) << "failed to parse from string";
                return false;
            }
            if (saved_schema.column_size() != schema.column_size()) {
                LOG(WARNING) << "saved_schema.column_size()=" << saved_schema.column_size()
                             << " schema.column_size()=" << schema.column_size();
                return false;
            }
            // Sort by column id
            std::sort(saved_schema.mutable_column()->begin(), saved_schema.mutable_column()->end(),
                      [](auto& c1, auto& c2) { return c1.unique_id() < c2.unique_id(); });
            auto& schema_ref = const_cast<doris::TabletSchemaPB&>(schema);
            std::sort(schema_ref.mutable_column()->begin(), schema_ref.mutable_column()->end(),
                      [](auto& c1, auto& c2) { return c1.unique_id() < c2.unique_id(); });
            for (int i = 0; i < saved_schema.column_size(); ++i) {
                auto& saved_column = saved_schema.column(i);
                auto& column = schema.column(i);
                if (saved_column.unique_id() != column.unique_id() ||
                    transform(saved_column.type()) != transform(column.type())) {
                    LOG(WARNING) << "existed column: " << saved_column.DebugString()
                                 << "\nto save column: " << column.DebugString();
                    return false;
                }
            }
            if (saved_schema.index_size() != schema.index_size()) {
                LOG(WARNING) << "saved_schema.index_size()=" << saved_schema.column_size()
                             << " schema.index_size()=" << schema.column_size();
                return false;
            }
            // Sort by index id
            std::sort(saved_schema.mutable_index()->begin(), saved_schema.mutable_index()->end(),
                      [](auto& i1, auto& i2) { return i1.index_id() < i2.index_id(); });
            std::sort(schema_ref.mutable_index()->begin(), schema_ref.mutable_index()->end(),
                      [](auto& i1, auto& i2) { return i1.index_id() < i2.index_id(); });
            for (int i = 0; i < saved_schema.index_size(); ++i) {
                auto& saved_index = saved_schema.index(i);
                auto& index = schema.index(i);
                if (saved_index.index_id() != index.index_id() ||
                    saved_index.index_type() != index.index_type()) {
                    LOG(WARNING) << "existed index: " << saved_index.DebugString()
                                 << "\nto save index: " << index.DebugString();
                    return false;
                }
            }
            return true;
        }()) << hex(schema_key)
             << "\n existed: " <<
                [&]() {
                    doris::TabletSchemaPB saved_schema;
                    saved_schema.ParseFromString(schema_val);
                    return saved_schema.ShortDebugString();
                }()
             << "\n to_save: " << schema.ShortDebugString();
        return; // schema has already been saved
    }
    if (!schema.SerializeToString(&schema_val)) [[unlikely]] {
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        msg = "failed to serialize tablet schema value";
        return;
    }
    txn->put(schema_key, schema_val);
}

void internal_create_tablet(MetaServiceCode& code, std::string& msg, int& ret,
                            doris::TabletMetaPB& tablet_meta, std::shared_ptr<TxnKv> txn_kv,
                            const std::string& instance_id,
                            std::set<std::pair<int64_t, int32_t>>& saved_schema) {
    bool has_first_rowset = tablet_meta.rs_metas_size() > 0;

    // TODO: validate tablet meta, check existence
    int64_t table_id = tablet_meta.table_id();
    int64_t index_id = tablet_meta.index_id();
    int64_t partition_id = tablet_meta.partition_id();
    int64_t tablet_id = tablet_meta.tablet_id();

    if (!tablet_meta.has_schema() && !tablet_meta.has_schema_version()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "tablet_meta must have either schema or schema_version";
        return;
    }

    std::unique_ptr<Transaction> txn;
    ret = txn_kv->create_txn(&txn);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        msg = "failed to init txn";
        return;
    }

    std::string rs_key, rs_val;
    if (has_first_rowset) {
        // Put first rowset if needed
        auto first_rowset = tablet_meta.mutable_rs_metas(0);
        if (config::write_schema_kv) { // detach schema from rowset meta
            first_rowset->set_index_id(index_id);
            first_rowset->set_schema_version(tablet_meta.has_schema_version()
                                                     ? tablet_meta.schema_version()
                                                     : tablet_meta.schema().schema_version());
            first_rowset->set_allocated_tablet_schema(nullptr);
        }
        MetaRowsetKeyInfo rs_key_info {instance_id, tablet_id, first_rowset->end_version()};
        meta_rowset_key(rs_key_info, &rs_key);
        if (!first_rowset->SerializeToString(&rs_val)) {
            code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
            msg = "failed to serialize first rowset meta";
            return;
        }
        txn->put(rs_key, rs_val);
        tablet_meta.clear_rs_metas(); // Strip off rowset meta
    }

    std::string schema_key, schema_val;
    if (tablet_meta.has_schema()) {
        // Temporary hard code to fix wrong column type string generated by FE
        auto fix_column_type = [](doris::TabletSchemaPB* schema) {
            for (auto& column : *schema->mutable_column()) {
                if (column.type() == "DECIMAL128") {
                    column.mutable_type()->push_back('I');
                }
            }
        };
        if (config::write_schema_kv) {
            // detach TabletSchemaPB from TabletMetaPB
            tablet_meta.set_schema_version(tablet_meta.schema().schema_version());
            auto [_, success] = saved_schema.emplace(index_id, tablet_meta.schema_version());
            if (success) { // schema may not be saved
                fix_column_type(tablet_meta.mutable_schema());
                put_schema_kv(code, msg, txn.get(), instance_id, index_id, tablet_meta.schema(),
                              schema_key, schema_val);
                if (code != MetaServiceCode::OK) return;
            }
            tablet_meta.set_allocated_schema(nullptr);
        } else {
            fix_column_type(tablet_meta.mutable_schema());
        }
    }

    MetaTabletKeyInfo key_info {instance_id, table_id, index_id, partition_id, tablet_id};
    std::string key;
    std::string val;
    meta_tablet_key(key_info, &key);
    if (!tablet_meta.SerializeToString(&val)) {
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        msg = "failed to serialize tablet meta";
        return;
    }
    txn->put(key, val);
    LOG(INFO) << "xxx put tablet_key=" << hex(key);

    // Index tablet_id -> table_id, index_id, partition_id
    std::string key1;
    std::string val1;
    MetaTabletIdxKeyInfo key_info1 {instance_id, tablet_id};
    meta_tablet_idx_key(key_info1, &key1);
    TabletIndexPB tablet_table;
    // tablet_table.set_db_id(db_id);
    tablet_table.set_table_id(table_id);
    tablet_table.set_index_id(index_id);
    tablet_table.set_partition_id(partition_id);
    tablet_table.set_tablet_id(tablet_id);
    if (!tablet_table.SerializeToString(&val1)) {
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        msg = "failed to serialize tablet table value";
        return;
    }
    txn->put(key1, val1);
    LOG(INFO) << "put tablet_idx tablet_id=" << tablet_id << " key=" << hex(key1);

    // Create stats info for the tablet
    auto stats_key = stats_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id});
    std::string stats_val;
    TabletStatsPB stats_pb;
    stats_pb.set_num_rowsets(1);
    stats_pb.set_num_segments(0);
    stats_pb.mutable_idx()->set_table_id(table_id);
    stats_pb.mutable_idx()->set_index_id(index_id);
    stats_pb.mutable_idx()->set_partition_id(partition_id);
    stats_pb.mutable_idx()->set_tablet_id(tablet_id);
    stats_pb.set_base_compaction_cnt(0);
    stats_pb.set_cumulative_compaction_cnt(0);
    // set cumulative point to 2 to not compact rowset [0-1]
    stats_pb.set_cumulative_point(2);
    stats_val = stats_pb.SerializeAsString();
    DCHECK(!stats_val.empty());
    txn->put(stats_key, stats_val);
    LOG(INFO) << "put tablet stats, tablet_id=" << tablet_id << " key=" << hex(stats_key);

    ret = txn->commit();
    if (ret != 0) {
        code = ret == -1 ? MetaServiceCode::KV_TXN_CONFLICT : MetaServiceCode::KV_TXN_COMMIT_ERR;
        msg = fmt::format("failed to save tablet meta, ret={}", ret);
        return;
    }
}

void MetaServiceImpl::create_tablets(::google::protobuf::RpcController* controller,
                                     const ::selectdb::CreateTabletsRequest* request,
                                     ::selectdb::CreateTabletsResponse* response,
                                     ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(create_tablets);

    if (request->tablet_metas_size() == 0) {
        msg = "no tablet meta";
        code = MetaServiceCode::INVALID_ARGUMENT;
        return;
    }
    instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << request->cloud_unique_id();
        return;
    }
    RPC_RATE_LIMIT(create_tablets)
    // [index_id, schema_version]
    std::set<std::pair<int64_t, int32_t>> saved_schema;
    for (auto& tablet_meta : request->tablet_metas()) {
        auto& meta = const_cast<doris::TabletMetaPB&>(tablet_meta);
        internal_create_tablet(code, msg, ret, meta, txn_kv_, instance_id, saved_schema);
        if (code != MetaServiceCode::OK) {
            return;
        }
    }
}

void internal_get_tablet(MetaServiceCode& code, std::string& msg, int& ret,
                         const std::string& instance_id, Transaction* txn, int64_t tablet_id,
                         doris::TabletMetaPB* tablet_meta, bool skip_schema) {
    // TODO: validate request
    TabletIndexPB tablet_idx;
    get_tablet_idx(code, msg, ret, txn, instance_id, tablet_id, tablet_idx);
    if (code != MetaServiceCode::OK) return;

    MetaTabletKeyInfo key_info1 {instance_id, tablet_idx.table_id(), tablet_idx.index_id(),
                                 tablet_idx.partition_id(), tablet_id};
    std::string key1, val1;
    meta_tablet_key(key_info1, &key1);
    ret = txn->get(key1, &val1);
    if (ret != 0) {
        code = (ret == 1 ? MetaServiceCode::TABLET_NOT_FOUND : MetaServiceCode::KV_TXN_GET_ERR);
        msg = fmt::format("failed to get tablet, err={}",
                          ret == 1 ? "not found" : "internal error");
        return;
    }

    if (!tablet_meta->ParseFromString(val1)) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        msg = "malformed tablet meta, unable to initialize";
        return;
    }

    if (tablet_meta->has_schema()) { // tablet meta saved before detach schema kv
        tablet_meta->set_schema_version(tablet_meta->schema().schema_version());
    }
    if (!tablet_meta->has_schema() && !skip_schema) {
        if (!tablet_meta->has_schema_version()) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = "tablet_meta must have either schema or schema_version";
            return;
        }
        auto key = meta_schema_key(
                {instance_id, tablet_meta->index_id(), tablet_meta->schema_version()});
        std::string val;
        ret = txn->get(key, &val);
        if (ret != 0) {
            code = MetaServiceCode::KV_TXN_GET_ERR;
            msg = fmt::format("failed to get schema, err={}",
                              ret == 1 ? "not found" : "internal error");
            return;
        }
        if (!tablet_meta->mutable_schema()->ParseFromString(val)) {
            code = MetaServiceCode::PROTOBUF_PARSE_ERR;
            msg = fmt::format("malformed schema value, key={}", key);
            return;
        }
    }
}

void MetaServiceImpl::update_tablet(::google::protobuf::RpcController* controller,
                                    const ::selectdb::UpdateTabletRequest* request,
                                    ::selectdb::UpdateTabletResponse* response,
                                    ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(update_tablet);
    instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(WARNING) << msg << ", cloud_unique_id=" << request->cloud_unique_id();
        return;
    }
    RPC_RATE_LIMIT(update_tablet)
    std::unique_ptr<Transaction> txn;
    if (txn_kv_->create_txn(&txn) != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        msg = "failed to init txn";
        return;
    }
    for (const TabletMetaInfoPB& tablet_meta_info : request->tablet_meta_infos()) {
        doris::TabletMetaPB tablet_meta;
        internal_get_tablet(code, msg, ret, instance_id, txn.get(), tablet_meta_info.tablet_id(),
                            &tablet_meta, true);
        if (code != MetaServiceCode::OK) {
            return;
        }
        if (tablet_meta_info.has_is_in_memory()) {
            tablet_meta.set_is_in_memory(tablet_meta_info.is_in_memory());
        } else if (tablet_meta_info.has_is_persistent()) {
            tablet_meta.set_is_persistent(tablet_meta_info.is_persistent());
        } else if (tablet_meta_info.has_ttl_seconds()) {
            tablet_meta.set_ttl_seconds(tablet_meta_info.ttl_seconds());
        }
        int64_t table_id = tablet_meta.table_id();
        int64_t index_id = tablet_meta.index_id();
        int64_t partition_id = tablet_meta.partition_id();
        int64_t tablet_id = tablet_meta.tablet_id();

        MetaTabletKeyInfo key_info {instance_id, table_id, index_id, partition_id, tablet_id};
        std::string key;
        std::string val;
        meta_tablet_key(key_info, &key);
        if (!tablet_meta.SerializeToString(&val)) {
            code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
            msg = "failed to serialize tablet meta";
            return;
        }
        txn->put(key, val);
        LOG(INFO) << "xxx put tablet_key=" << hex(key);
    }
    if (txn->commit() != 0) {
        code = ret == -1 ? MetaServiceCode::KV_TXN_CONFLICT : MetaServiceCode::KV_TXN_COMMIT_ERR;
        ss << "failed to update tablet meta, ret=" << ret;
        msg = ss.str();
        return;
    }
}

void MetaServiceImpl::update_tablet_schema(::google::protobuf::RpcController* controller,
                                           const ::selectdb::UpdateTabletSchemaRequest* request,
                                           ::selectdb::UpdateTabletSchemaResponse* response,
                                           ::google::protobuf::Closure* done) {
    DCHECK(false) << "should not call update_tablet_schema";
    RPC_PREPROCESS(update_tablet_schema);
    instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(WARNING) << msg << ", cloud_unique_id=" << request->cloud_unique_id();
        return;
    }

    RPC_RATE_LIMIT(update_tablet_schema)

    std::unique_ptr<Transaction> txn;
    if (txn_kv_->create_txn(&txn) != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        msg = "failed to init txn";
        return;
    }

    doris::TabletMetaPB tablet_meta;
    internal_get_tablet(code, msg, ret, instance_id, txn.get(), request->tablet_id(), &tablet_meta,
                        true);
    if (code != MetaServiceCode::OK) {
        return;
    }

    std::string schema_key, schema_val;
    while (request->has_tablet_schema()) {
        if (!config::write_schema_kv) {
            tablet_meta.mutable_schema()->CopyFrom(request->tablet_schema());
            break;
        }
        tablet_meta.set_schema_version(request->tablet_schema().schema_version());
        meta_schema_key({instance_id, tablet_meta.index_id(), tablet_meta.schema_version()},
                        &schema_key);
        if (txn->get(schema_key, &schema_val, true) == 0) {
            break; // schema has already been saved
        }
        if (!request->tablet_schema().SerializeToString(&schema_val)) [[unlikely]] {
            code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
            msg = "failed to serialize tablet schema value";
            return;
        }
        txn->put(schema_key, schema_val);
        break;
    }

    int64_t table_id = tablet_meta.table_id();
    int64_t index_id = tablet_meta.index_id();
    int64_t partition_id = tablet_meta.partition_id();
    int64_t tablet_id = tablet_meta.tablet_id();
    MetaTabletKeyInfo key_info {instance_id, table_id, index_id, partition_id, tablet_id};
    std::string key;
    std::string val;
    meta_tablet_key(key_info, &key);
    if (!tablet_meta.SerializeToString(&val)) {
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        msg = "failed to serialize tablet meta";
        return;
    }
    txn->put(key, val);
    if (txn->commit() != 0) {
        code = ret == -1 ? MetaServiceCode::KV_TXN_CONFLICT : MetaServiceCode::KV_TXN_COMMIT_ERR;
        ss << "failed to update tablet meta, ret=" << ret;
        msg = ss.str();
        return;
    }
}

void MetaServiceImpl::get_tablet(::google::protobuf::RpcController* controller,
                                 const ::selectdb::GetTabletRequest* request,
                                 ::selectdb::GetTabletResponse* response,
                                 ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(get_tablet);
    instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << request->cloud_unique_id();
        return;
    }
    RPC_RATE_LIMIT(get_tablet)
    std::unique_ptr<Transaction> txn;
    if (txn_kv_->create_txn(&txn) != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        msg = "failed to init txn";
        return;
    }
    internal_get_tablet(code, msg, ret, instance_id, txn.get(), request->tablet_id(),
                        response->mutable_tablet_meta(), false);
}

static void set_schema_in_existed_rowset(MetaServiceCode& code, std::string& msg, int& ret,
                                         Transaction* txn, const std::string& instance_id,
                                         doris::RowsetMetaPB& rowset_meta,
                                         doris::RowsetMetaPB& existed_rowset_meta) {
    DCHECK(existed_rowset_meta.has_index_id());
    if (!existed_rowset_meta.has_schema_version()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "rowset_meta must have either schema or schema_version";
        return;
    }
    // Currently, schema version of `existed_rowset_meta` and `rowset_meta` MUST be equal
    DCHECK_EQ(existed_rowset_meta.schema_version(),
              rowset_meta.has_tablet_schema() ? rowset_meta.tablet_schema().schema_version()
                                              : rowset_meta.schema_version());
    if (rowset_meta.has_tablet_schema() &&
        rowset_meta.tablet_schema().schema_version() == existed_rowset_meta.schema_version()) {
        if (existed_rowset_meta.GetArena() &&
            rowset_meta.tablet_schema().GetArena() == existed_rowset_meta.GetArena()) {
            existed_rowset_meta.unsafe_arena_set_allocated_tablet_schema(
                    rowset_meta.mutable_tablet_schema());
        } else {
            existed_rowset_meta.mutable_tablet_schema()->CopyFrom(rowset_meta.tablet_schema());
        }
    } else {
        // get schema from txn kv
        std::string schema_key, schema_val;
        meta_schema_key(
                {instance_id, existed_rowset_meta.index_id(), existed_rowset_meta.schema_version()},
                &schema_key);
        ret = txn->get(schema_key, &schema_val, true);
        if (ret != 0) {
            code = MetaServiceCode::KV_TXN_GET_ERR;
            msg = fmt::format("failed to get schema, schema_version={}: {}",
                              rowset_meta.schema_version(),
                              ret == 1 ? "not found" : "internal error");
            return;
        }
        if (!existed_rowset_meta.mutable_tablet_schema()->ParseFromString(schema_val)) {
            code = MetaServiceCode::PROTOBUF_PARSE_ERR;
            msg = fmt::format("malformed schema value, key={}", schema_key);
            return;
        }
    }
}

/**
 * 0. Construct the corresponding rowset commit_key according to the info in request
 * 1. Check whether this rowset has already been committed through commit_key
 *     a. if has been committed, abort prepare_rowset 
 *     b. else, goto 2
 * 2. Construct recycle rowset kv which contains object path
 * 3. Put recycle rowset kv
 */
void MetaServiceImpl::prepare_rowset(::google::protobuf::RpcController* controller,
                                     const ::selectdb::CreateRowsetRequest* request,
                                     ::selectdb::CreateRowsetResponse* response,
                                     ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(prepare_rowset);
    if (!request->has_rowset_meta()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "no rowset meta";
        return;
    }
    instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << request->cloud_unique_id();
        return;
    }
    auto& rowset_meta = const_cast<doris::RowsetMetaPB&>(request->rowset_meta());
    if (!rowset_meta.has_tablet_schema() && !rowset_meta.has_schema_version()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "rowset_meta must have either schema or schema_version";
        return;
    }
    RPC_RATE_LIMIT(prepare_rowset)
    // temporary == true is for load txn, schema change, compaction
    // temporary == false currently no such situation
    bool temporary = request->has_temporary() ? request->temporary() : false;
    int64_t tablet_id = rowset_meta.tablet_id();
    int64_t end_version = rowset_meta.end_version();
    const auto& rowset_id = rowset_meta.rowset_id_v2();

    std::string commit_key;
    std::string commit_val;

    if (temporary) { // load txn, schema change, compaction
        int64_t txn_id = rowset_meta.txn_id();
        MetaRowsetTmpKeyInfo key_info {instance_id, txn_id, tablet_id};
        meta_rowset_tmp_key(key_info, &commit_key);
    } else { // ?
        MetaRowsetKeyInfo key_info {instance_id, tablet_id, end_version};
        meta_rowset_key(key_info, &commit_key);
    }

    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        msg = "failed to create txn";
        return;
    }

    // Check if commit key already exists.
    ret = txn->get(commit_key, &commit_val);
    if (ret == 0) {
        auto existed_rowset_meta = response->mutable_existed_rowset_meta();
        if (!existed_rowset_meta->ParseFromString(commit_val)) {
            code = MetaServiceCode::PROTOBUF_PARSE_ERR;
            msg = fmt::format("malformed rowset meta value. key={}", hex(commit_key));
            return;
        }
        if (!existed_rowset_meta->has_index_id()) {
            if (rowset_meta.has_index_id()) {
                existed_rowset_meta->set_index_id(rowset_meta.index_id());
            } else {
                TabletIndexPB tablet_idx;
                get_tablet_idx(code, msg, ret, txn.get(), instance_id, rowset_meta.tablet_id(),
                               tablet_idx);
                if (code != MetaServiceCode::OK) return;
                existed_rowset_meta->set_index_id(tablet_idx.index_id());
                rowset_meta.set_index_id(tablet_idx.index_id());
            }
        }
        if (!existed_rowset_meta->has_tablet_schema()) {
            set_schema_in_existed_rowset(code, msg, ret, txn.get(), instance_id, rowset_meta,
                                         *existed_rowset_meta);
            if (code != MetaServiceCode::OK) return;
        } else {
            existed_rowset_meta->set_schema_version(
                    existed_rowset_meta->tablet_schema().schema_version());
        }
        code = MetaServiceCode::ALREADY_EXISTED;
        msg = "rowset already exists";
        return;
    }
    if (ret != 1) {
        code = MetaServiceCode::KV_TXN_GET_ERR;
        msg = "failed to check whether rowset exists";
        return;
    }

    std::string prepare_key;
    std::string prepare_val;
    RecycleRowsetKeyInfo prepare_key_info {instance_id, tablet_id, rowset_id};
    recycle_rowset_key(prepare_key_info, &prepare_key);
    RecycleRowsetPB prepare_rowset;
    using namespace std::chrono;
    int64_t now = duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
    prepare_rowset.set_creation_time(now);
    prepare_rowset.set_expiration(request->rowset_meta().txn_expiration());
    // Schema is useless for PREPARE type recycle rowset, set it to null to reduce storage space
    rowset_meta.set_allocated_tablet_schema(nullptr);
    prepare_rowset.mutable_rowset_meta()->CopyFrom(rowset_meta);
    prepare_rowset.set_type(RecycleRowsetPB::PREPARE);
    prepare_rowset.SerializeToString(&prepare_val);
    DCHECK_GT(prepare_rowset.expiration(), 0);
    txn->put(prepare_key, prepare_val);
    LOG(INFO) << "xxx put" << (temporary ? " tmp " : " ") << "prepare_rowset_key "
              << hex(prepare_key) << " associated commit_rowset_key " << hex(commit_key)
              << " value_size " << prepare_val.size();
    ret = txn->commit();
    if (ret != 0) {
        code = ret == -1 ? MetaServiceCode::KV_TXN_CONFLICT : MetaServiceCode::KV_TXN_COMMIT_ERR;
        ss << "failed to save recycle rowset, ret=" << ret;
        msg = ss.str();
        return;
    }
}

/**
 * 0. Construct the corresponding rowset commit_key and commit_value according
 *    to the info in request
 * 1. Check whether this rowset has already been committed through commit_key
 *     a. if has been committed
 *         1. if committed value is same with commit_value, it may be a redundant
 *            retry request, return ok
 *         2. else, abort commit_rowset 
 *     b. else, goto 2
 * 2. Construct the corresponding rowset prepare_key(recycle rowset)
 * 3. Remove prepare_key and put commit rowset kv
 */
void MetaServiceImpl::commit_rowset(::google::protobuf::RpcController* controller,
                                    const ::selectdb::CreateRowsetRequest* request,
                                    ::selectdb::CreateRowsetResponse* response,
                                    ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(commit_rowset);
    if (!request->has_rowset_meta()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "no rowset meta";
        return;
    }
    instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << request->cloud_unique_id();
        return;
    }
    auto& rowset_meta = const_cast<doris::RowsetMetaPB&>(request->rowset_meta());
    if (!rowset_meta.has_tablet_schema() && !rowset_meta.has_schema_version()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "rowset_meta must have either schema or schema_version";
        return;
    }
    RPC_RATE_LIMIT(commit_rowset)
    // temporary == true is for load txn, schema change, compaction
    // temporary == false currently no such situation
    bool temporary = request->has_temporary() ? request->temporary() : false;
    int64_t tablet_id = rowset_meta.tablet_id();
    int64_t end_version = rowset_meta.end_version();
    const auto& rowset_id = rowset_meta.rowset_id_v2();

    std::string commit_key;
    std::string commit_val;

    if (temporary) { // load txn, schema change, compaction
        int64_t txn_id = rowset_meta.txn_id();
        MetaRowsetTmpKeyInfo key_info {instance_id, txn_id, tablet_id};
        meta_rowset_tmp_key(key_info, &commit_key);
    } else { // ?
        MetaRowsetKeyInfo key_info {instance_id, tablet_id, end_version};
        meta_rowset_key(key_info, &commit_key);
    }

    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        msg = "failed to create txn";
        return;
    }

    // Check if commit key already exists.
    std::string existed_commit_val;
    ret = txn->get(commit_key, &existed_commit_val);
    if (ret == 0) {
        auto existed_rowset_meta = response->mutable_existed_rowset_meta();
        if (!existed_rowset_meta->ParseFromString(existed_commit_val)) {
            code = MetaServiceCode::PROTOBUF_PARSE_ERR;
            msg = fmt::format("malformed rowset meta value. key={}", hex(commit_key));
            return;
        }
        if (existed_rowset_meta->rowset_id_v2() == rowset_meta.rowset_id_v2()) {
            // Same request, return OK
            response->set_allocated_existed_rowset_meta(nullptr);
            return;
        }
        if (!existed_rowset_meta->has_index_id()) {
            if (rowset_meta.has_index_id()) {
                existed_rowset_meta->set_index_id(rowset_meta.index_id());
            } else {
                TabletIndexPB tablet_idx;
                get_tablet_idx(code, msg, ret, txn.get(), instance_id, rowset_meta.tablet_id(),
                               tablet_idx);
                if (code != MetaServiceCode::OK) return;
                existed_rowset_meta->set_index_id(tablet_idx.index_id());
            }
        }
        if (!existed_rowset_meta->has_tablet_schema()) {
            set_schema_in_existed_rowset(code, msg, ret, txn.get(), instance_id, rowset_meta,
                                         *existed_rowset_meta);
            if (code != MetaServiceCode::OK) return;
        } else {
            existed_rowset_meta->set_schema_version(
                    existed_rowset_meta->tablet_schema().schema_version());
        }
        code = MetaServiceCode::ALREADY_EXISTED;
        msg = "rowset already exists";
        return;
    }
    if (ret != 1) {
        code = MetaServiceCode::KV_TXN_GET_ERR;
        msg = "failed to check whether rowset exists";
        return;
    }
    // write schema kv if rowset_meta has schema
    std::string schema_key;
    std::string schema_val;
    if (config::write_schema_kv && rowset_meta.has_tablet_schema()) {
        if (!rowset_meta.has_index_id()) {
            TabletIndexPB tablet_idx;
            get_tablet_idx(code, msg, ret, txn.get(), instance_id, rowset_meta.tablet_id(),
                           tablet_idx);
            if (code != MetaServiceCode::OK) return;
            rowset_meta.set_index_id(tablet_idx.index_id());
        }
        DCHECK(rowset_meta.tablet_schema().has_schema_version());
        DCHECK_GE(rowset_meta.tablet_schema().schema_version(), 0);
        rowset_meta.set_schema_version(rowset_meta.tablet_schema().schema_version());
        put_schema_kv(code, msg, txn.get(), instance_id, rowset_meta.index_id(),
                      rowset_meta.tablet_schema(), schema_key, schema_val);
        if (code != MetaServiceCode::OK) return;
        rowset_meta.set_allocated_tablet_schema(nullptr);
    }

    std::string prepare_key;
    RecycleRowsetKeyInfo prepare_key_info {instance_id, tablet_id, rowset_id};
    recycle_rowset_key(prepare_key_info, &prepare_key);
    DCHECK_GT(rowset_meta.txn_expiration(), 0);
    if (!rowset_meta.SerializeToString(&commit_val)) {
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        msg = "failed to serialize rowset meta";
        return;
    }

    txn->remove(prepare_key);
    txn->put(commit_key, commit_val);
    LOG(INFO) << "xxx put" << (temporary ? " tmp " : " ") << "commit_rowset_key " << hex(commit_key)
              << " delete prepare_rowset_key " << hex(prepare_key) << " value_size "
              << commit_val.size();
    ret = txn->commit();
    if (ret != 0) {
        code = ret == -1 ? MetaServiceCode::KV_TXN_CONFLICT : MetaServiceCode::KV_TXN_COMMIT_ERR;
        ss << "failed to save rowset meta, ret=" << ret;
        msg = ss.str();
        return;
    }
}

void internal_get_rowset(Transaction* txn, int64_t start, int64_t end,
                         const std::string& instance_id, int64_t tablet_id, int& ret,
                         MetaServiceCode& code, std::string& msg,
                         ::selectdb::GetRowsetResponse* response) {
    LOG(INFO) << "get_rowset start=" << start << ", end=" << end;
    MetaRowsetKeyInfo key_info0 {instance_id, tablet_id, start};
    MetaRowsetKeyInfo key_info1 {instance_id, tablet_id, end + 1};
    std::string key0;
    std::string key1;
    meta_rowset_key(key_info0, &key0);
    meta_rowset_key(key_info1, &key1);
    std::unique_ptr<RangeGetIterator> it;

    int num_rowsets = 0;
    std::unique_ptr<int, std::function<void(int*)>> defer_log_range(
            (int*)0x01, [key0, key1, &num_rowsets](int*) {
                LOG(INFO) << "get rowset meta, num_rowsets=" << num_rowsets << " range=["
                          << hex(key0) << "," << hex(key1) << "]";
            });

    std::stringstream ss;
    do {
        ret = txn->get(key0, key1, &it);
        if (ret != 0) {
            code = MetaServiceCode::KV_TXN_GET_ERR;
            ss << "internal error, failed to get rowset, ret=" << ret;
            msg = ss.str();
            LOG(WARNING) << msg;
            return;
        }

        while (it->has_next()) {
            auto [k, v] = it->next();
            auto rs = response->add_rowset_meta();
            if (!rs->ParseFromArray(v.data(), v.size())) {
                code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                msg = "malformed rowset meta, unable to deserialize";
                LOG(WARNING) << msg << " key=" << hex(k);
                ret = -1;
                return;
            }
            ++num_rowsets;
            if (!it->has_next()) key0 = k;
        }
        key0.push_back('\x00'); // Update to next smallest key for iteration
    } while (it->more());
}

std::vector<std::pair<int64_t, int64_t>> calc_sync_versions(int64_t req_bc_cnt, int64_t bc_cnt,
                                                            int64_t req_cc_cnt, int64_t cc_cnt,
                                                            int64_t req_cp, int64_t cp,
                                                            int64_t req_start, int64_t req_end) {
    using Version = std::pair<int64_t, int64_t>;
    // combine `v1` `v2`  to `v1`, return true if success
    static auto combine_if_overlapping = [](Version& v1, Version& v2) -> bool {
        if (v1.second + 1 < v2.first || v2.second + 1 < v1.first) return false;
        v1.first = std::min(v1.first, v2.first);
        v1.second = std::max(v1.second, v2.second);
        return true;
    };
    // [xxx]: compacted versions
    // ^~~~~: cumulative point
    // ^___^: related versions
    std::vector<Version> versions;
    if (req_bc_cnt < bc_cnt) {
        // * for any BC happended
        // BE  [=][=][=][=][=====][=][=]
        //                  ^~~~~ req_cp
        // MS  [xxxxxxxxxx][xxxxxxxxxxxxxx][=======][=][=]
        //                                  ^~~~~~~ ms_cp
        //     ^_________________________^ versions_return: [0, ms_cp - 1]
        versions.emplace_back(0, cp - 1);
    }

    if (req_cc_cnt < cc_cnt) {
        Version cc_version;
        if (req_cp < cp && req_cc_cnt + 1 == cc_cnt) {
            // * only one CC happened and CP changed
            // BE  [=][=][=][=][=====][=][=]
            //                  ^~~~~ req_cp
            // MS  [=][=][=][=][xxxxxxxxxxxxxx][=======][=][=]
            //                                  ^~~~~~~ ms_cp
            //                  ^____________^ related_versions: [req_cp, ms_cp - 1]
            //
            cc_version = {req_cp, cp - 1};
        } else {
            // * more than one CC happened and CP changed
            // BE  [=][=][=][=][=====][=][=]
            //                  ^~~~~ req_cp
            // MS  [=][=][=][=][xxxxxxxxxxxxxx][xxxxxxx][=][=]
            //                                  ^~~~~~~ ms_cp
            //                  ^_____________________^ related_versions: [req_cp, max]
            //
            // * more than one CC happened and CP remain unchanged
            // BE  [=][=][=][=][=====][=][=]
            //                  ^~~~~ req_cp
            // MS  [=][=][=][=][xxxxxxxxxxxxxx][xxxxxxx][=][=]
            //                  ^~~~~~~~~~~~~~ ms_cp
            //                  ^_____________________^ related_versions: [req_cp, max]
            //                                           there may be holes if we don't return all version
            //                                           after ms_cp, however it can be optimized.
            cc_version = {req_cp, std::numeric_limits<int64_t>::max() - 1};
        }
        if (versions.empty() || !combine_if_overlapping(versions.front(), cc_version)) {
            versions.push_back(cc_version);
        }
    }

    Version query_version {req_start, req_end};
    bool combined = false;
    for (auto& v : versions) {
        if ((combined = combine_if_overlapping(v, query_version))) break;
    }
    if (!combined) {
        versions.push_back(query_version);
    }
    std::sort(versions.begin(), versions.end(),
              [](const Version& v1, const Version& v2) { return v1.first < v2.first; });
    return versions;
}

void MetaServiceImpl::get_rowset(::google::protobuf::RpcController* controller,
                                 const ::selectdb::GetRowsetRequest* request,
                                 ::selectdb::GetRowsetResponse* response,
                                 ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(get_rowset);
    instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << request->cloud_unique_id();
        return;
    }
    RPC_RATE_LIMIT(get_rowset)
    int64_t tablet_id = request->idx().has_tablet_id() ? request->idx().tablet_id() : -1;
    if (tablet_id <= 0) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "no valid tablet_id given";
        return;
    }

    if (!request->has_base_compaction_cnt() || !request->has_cumulative_compaction_cnt() ||
        !request->has_cumulative_point()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "no valid compaction_cnt or cumulative_point given";
        return;
    }
    int64_t req_bc_cnt = request->base_compaction_cnt();
    int64_t req_cc_cnt = request->cumulative_compaction_cnt();
    int64_t req_cp = request->cumulative_point();

    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        msg = "failed to create txn";
        return;
    }

    auto& idx = const_cast<TabletIndexPB&>(request->idx());
    // Get tablet id index from kv
    if (!idx.has_table_id() || !idx.has_index_id() || !idx.has_partition_id()) {
        get_tablet_idx(code, msg, ret, txn.get(), instance_id, tablet_id, idx);
        if (code != MetaServiceCode::OK) return;
    }
    // TODO(plat1ko): Judge if tablet has been dropped (in dropped index/partition)

    TabletStatsPB tablet_stat;
    internal_get_tablet_stats(code, msg, ret, txn.get(), instance_id, idx, tablet_stat, true);
    if (code != MetaServiceCode::OK) return;
    VLOG_DEBUG << "tablet_id=" << tablet_id << " stats=" << proto_to_json(tablet_stat);

    int64_t bc_cnt = tablet_stat.base_compaction_cnt();
    int64_t cc_cnt = tablet_stat.cumulative_compaction_cnt();
    int64_t cp = tablet_stat.cumulative_point();

    response->mutable_stats()->CopyFrom(tablet_stat);

    int64_t req_start = request->start_version();
    int64_t req_end = request->end_version();
    req_end = req_end < 0 ? std::numeric_limits<int64_t>::max() - 1 : req_end;

    LOG(INFO) << "req_bc_cnt=" << req_bc_cnt << ", bc_cnt=" << bc_cnt
              << ", req_cc_cnt=" << req_cc_cnt << ", cc_cnt=" << cc_cnt << ", req_cp=" << req_cp
              << ", cp=" << cp;
    //==========================================================================
    //      Find version ranges to be synchronized due to compaction
    //==========================================================================
    if (req_bc_cnt > bc_cnt || req_cc_cnt > cc_cnt || req_cp > cp) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        ss << "no valid compaction_cnt or cumulative_point given. req_bc_cnt=" << req_bc_cnt
           << ", bc_cnt=" << bc_cnt << ", req_cc_cnt=" << req_cc_cnt << ", cc_cnt=" << cc_cnt
           << ", req_cp=" << req_cp << ", cp=" << cp;
        msg = ss.str();
        return;
    }
    auto versions = calc_sync_versions(req_bc_cnt, bc_cnt, req_cc_cnt, cc_cnt, req_cp, cp,
                                       req_start, req_end);
    for (auto [start, end] : versions) {
        internal_get_rowset(txn.get(), start, end, instance_id, tablet_id, ret, code, msg,
                            response);
        if (ret != 0) {
            return;
        }
    }

    // get referenced schema
    std::unordered_map<int32_t, doris::TabletSchemaPB*> version_to_schema;
    for (auto& rowset_meta : *response->mutable_rowset_meta()) {
        if (rowset_meta.has_tablet_schema()) {
            version_to_schema.emplace(rowset_meta.tablet_schema().schema_version(),
                                      rowset_meta.mutable_tablet_schema());
            rowset_meta.set_schema_version(rowset_meta.tablet_schema().schema_version());
        }
        rowset_meta.set_index_id(idx.index_id());
    }
    auto arena = response->GetArena();
    for (auto& rowset_meta : *response->mutable_rowset_meta()) {
        if (rowset_meta.has_tablet_schema()) continue;
        if (!rowset_meta.has_schema_version()) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = fmt::format(
                    "rowset_meta must have either schema or schema_version, "
                    "rowset_version=[{}-{}]",
                    rowset_meta.start_version(), rowset_meta.end_version());
            return;
        }
        if (auto it = version_to_schema.find(rowset_meta.schema_version());
            it != version_to_schema.end()) {
            if (arena != nullptr) {
                rowset_meta.set_allocated_tablet_schema(it->second);
            } else {
                rowset_meta.mutable_tablet_schema()->CopyFrom(*it->second);
            }
        } else {
            auto key = meta_schema_key({instance_id, idx.index_id(), rowset_meta.schema_version()});
            std::string val;
            ret = txn->get(key, &val);
            if (ret != 0) {
                code = MetaServiceCode::KV_TXN_GET_ERR;
                msg = fmt::format(
                        "failed to get schema, schema_version={}, rowset_version=[{}-{}]: {}",
                        rowset_meta.schema_version(), rowset_meta.start_version(),
                        rowset_meta.end_version(), ret == 1 ? "not found" : "internal error");
                return;
            }
            auto schema = rowset_meta.mutable_tablet_schema();
            if (!schema->ParseFromString(val)) {
                code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                msg = fmt::format("malformed schema value, key={}", key);
                return;
            }
            version_to_schema.emplace(rowset_meta.schema_version(), schema);
        }
    }
}

int index_exists(MetaServiceCode& code, std::string& msg, const ::selectdb::IndexRequest* request,
                 const std::string& instance_id, Transaction* txn) {
    const auto& index_ids = request->index_ids();
    if (index_ids.empty() || !request->has_table_id()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty index_ids or table_id";
        return -1;
    }
    MetaTabletKeyInfo info0 {instance_id, request->table_id(), index_ids[0], 0, 0};
    MetaTabletKeyInfo info1 {instance_id, request->table_id(), index_ids[0],
                             std::numeric_limits<int64_t>::max(), 0};
    std::string key0;
    std::string key1;
    meta_tablet_key(info0, &key0);
    meta_tablet_key(info1, &key1);

    std::unique_ptr<RangeGetIterator> it;
    int ret = txn->get(key0, key1, &it, 1);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_GET_ERR;
        msg = "failed to get tablet when checking index existence";
        return -1;
    }
    if (!it->has_next()) {
        return 1;
    }
    return 0;
}

void put_recycle_index_kv(MetaServiceCode& code, std::string& msg, int& ret,
                          const ::selectdb::IndexRequest* request, const std::string& instance_id,
                          Transaction* txn, RecycleIndexPB::Type type) {
    const auto& index_ids = request->index_ids();
    if (index_ids.empty() || !request->has_table_id()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty index_ids or table_id";
        return;
    }

    int64_t creation_time = duration_cast<seconds>(system_clock::now().time_since_epoch()).count();

    std::vector<std::pair<std::string, std::string>> kvs;
    kvs.reserve(index_ids.size());

    for (int64_t index_id : index_ids) {
        std::string key;
        RecycleIndexKeyInfo key_info {instance_id, index_id};
        recycle_index_key(key_info, &key);

        RecycleIndexPB recycle_index;
        recycle_index.set_table_id(request->table_id());
        recycle_index.set_creation_time(creation_time);
        recycle_index.set_expiration(request->expiration());
        recycle_index.set_type(type);
        std::string val = recycle_index.SerializeAsString();

        kvs.emplace_back(std::move(key), std::move(val));
    }
    for (const auto& [k, v] : kvs) {
        txn->put(k, v);
    }
    ret = txn->commit();
    if (ret != 0) {
        code = ret == -1 ? MetaServiceCode::KV_TXN_CONFLICT : MetaServiceCode::KV_TXN_COMMIT_ERR;
        msg = fmt::format("failed to save recycle index kv, ret={}", ret);
        return;
    }
}

void remove_recycle_index_kv(MetaServiceCode& code, std::string& msg, int& ret,
                             const ::selectdb::IndexRequest* request,
                             const std::string& instance_id, Transaction* txn) {
    const auto& index_ids = request->index_ids();
    if (index_ids.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty index_ids";
        return;
    }

    std::vector<std::string> keys;
    keys.reserve(index_ids.size());

    for (int64_t index_id : index_ids) {
        std::string key;
        RecycleIndexKeyInfo key_info {instance_id, index_id};
        recycle_index_key(key_info, &key);
        keys.push_back(std::move(key));
    }
    for (const auto& k : keys) {
        txn->remove(k);
    }
    ret = txn->commit();
    if (ret != 0) {
        code = ret == -1 ? MetaServiceCode::KV_TXN_CONFLICT : MetaServiceCode::KV_TXN_COMMIT_ERR;
        msg = fmt::format("failed to remove recycle index kv, ret={}", ret);
        return;
    }
}

void MetaServiceImpl::prepare_index(::google::protobuf::RpcController* controller,
                                    const ::selectdb::IndexRequest* request,
                                    ::selectdb::IndexResponse* response,
                                    ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(prepare_index);
    instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << request->cloud_unique_id();
        return;
    }
    RPC_RATE_LIMIT(prepare_index)
    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        msg = "failed to create txn";
        return;
    }
    ret = index_exists(code, msg, request, instance_id, txn.get());
    if (ret < 0) {
        return;
    } else if (ret == 0) {
        code = MetaServiceCode::ALREADY_EXISTED;
        msg = "index already existed";
        return;
    }
    put_recycle_index_kv(code, msg, ret, request, instance_id, txn.get(), RecycleIndexPB::PREPARE);
}

void MetaServiceImpl::commit_index(::google::protobuf::RpcController* controller,
                                   const ::selectdb::IndexRequest* request,
                                   ::selectdb::IndexResponse* response,
                                   ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(commit_index);
    instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << request->cloud_unique_id();
        return;
    }
    RPC_RATE_LIMIT(commit_index)
    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        msg = "failed to create txn";
        return;
    }
    remove_recycle_index_kv(code, msg, ret, request, instance_id, txn.get());
}

void MetaServiceImpl::drop_index(::google::protobuf::RpcController* controller,
                                 const ::selectdb::IndexRequest* request,
                                 ::selectdb::IndexResponse* response,
                                 ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(drop_index);
    instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << request->cloud_unique_id();
        return;
    }
    RPC_RATE_LIMIT(drop_index)
    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        msg = "failed to create txn";
        return;
    }
    put_recycle_index_kv(code, msg, ret, request, instance_id, txn.get(), RecycleIndexPB::DROP);
}

int partition_exists(MetaServiceCode& code, std::string& msg,
                     const ::selectdb::PartitionRequest* request, const std::string& instance_id,
                     Transaction* txn) {
    const auto& index_ids = request->index_ids();
    const auto& partition_ids = request->partition_ids();
    if (partition_ids.empty() || index_ids.empty() || !request->has_table_id()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty partition_ids or index_ids or table_id";
        return -1;
    }
    MetaTabletKeyInfo info0 {instance_id, request->table_id(), index_ids[0], partition_ids[0], 0};
    MetaTabletKeyInfo info1 {instance_id, request->table_id(), index_ids[0], partition_ids[0],
                             std::numeric_limits<int64_t>::max()};
    std::string key0;
    std::string key1;
    meta_tablet_key(info0, &key0);
    meta_tablet_key(info1, &key1);

    std::unique_ptr<RangeGetIterator> it;
    int ret = txn->get(key0, key1, &it, 1);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_GET_ERR;
        msg = "failed to get tablet when checking partition existence";
        return -1;
    }
    if (!it->has_next()) {
        return 1;
    }
    return 0;
}

void put_recycle_partition_kv(MetaServiceCode& code, std::string& msg, int& ret,
                              const ::selectdb::PartitionRequest* request,
                              const std::string& instance_id, Transaction* txn,
                              RecyclePartitionPB::Type type) {
    const auto& partition_ids = request->partition_ids();
    const auto& index_ids = request->index_ids();
    if (partition_ids.empty() || index_ids.empty() || !request->has_table_id()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty partition_ids or index_ids or table_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << request->cloud_unique_id();
        return;
    }

    int64_t creation_time = duration_cast<seconds>(system_clock::now().time_since_epoch()).count();

    std::vector<std::pair<std::string, std::string>> kvs;
    kvs.reserve(partition_ids.size());

    for (int64_t partition_id : partition_ids) {
        std::string key;
        RecyclePartKeyInfo key_info {instance_id, partition_id};
        recycle_partition_key(key_info, &key);

        RecyclePartitionPB recycle_partition;
        recycle_partition.set_db_id(request->db_id());
        recycle_partition.set_table_id(request->table_id());
        *recycle_partition.mutable_index_id() = index_ids;
        recycle_partition.set_creation_time(creation_time);
        recycle_partition.set_expiration(request->expiration());
        recycle_partition.set_type(type);
        std::string val = recycle_partition.SerializeAsString();

        kvs.emplace_back(std::move(key), std::move(val));
    }
    for (const auto& [k, v] : kvs) {
        txn->put(k, v);
    }
    ret = txn->commit();
    if (ret != 0) {
        code = ret == -1 ? MetaServiceCode::KV_TXN_CONFLICT : MetaServiceCode::KV_TXN_COMMIT_ERR;
        msg = fmt::format("failed to save recycle partition kv, ret={}", ret);
        return;
    }
}

void remove_recycle_partition_kv(MetaServiceCode& code, std::string& msg, int& ret,
                                 const ::selectdb::PartitionRequest* request,
                                 const std::string& instance_id, Transaction* txn) {
    const auto& partition_ids = request->partition_ids();
    if (partition_ids.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty partition_ids";
        LOG(INFO) << msg << ", cloud_unique_id=" << request->cloud_unique_id();
        return;
    }

    std::vector<std::string> keys;
    keys.reserve(partition_ids.size());

    for (int64_t partition_id : partition_ids) {
        std::string key;
        RecyclePartKeyInfo key_info {instance_id, partition_id};
        recycle_partition_key(key_info, &key);
        keys.push_back(std::move(key));
    }
    for (const auto& k : keys) {
        txn->remove(k);
    }

    ret = txn->commit();
    if (ret != 0) {
        code = ret == -1 ? MetaServiceCode::KV_TXN_CONFLICT : MetaServiceCode::KV_TXN_COMMIT_ERR;
        msg = fmt::format("failed to remove recycle partition kv, ret={}", ret);
        return;
    }
}

void MetaServiceImpl::prepare_partition(::google::protobuf::RpcController* controller,
                                        const ::selectdb::PartitionRequest* request,
                                        ::selectdb::PartitionResponse* response,
                                        ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(prepare_partition);
    instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << request->cloud_unique_id();
        return;
    }
    RPC_RATE_LIMIT(prepare_partition)
    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        msg = "failed to create txn";
        return;
    }
    ret = partition_exists(code, msg, request, instance_id, txn.get());
    if (ret < 0) {
        return;
    } else if (ret == 0) {
        code = MetaServiceCode::ALREADY_EXISTED;
        msg = "index already existed";
        return;
    }
    put_recycle_partition_kv(code, msg, ret, request, instance_id, txn.get(),
                             RecyclePartitionPB::PREPARE);
}

void MetaServiceImpl::commit_partition(::google::protobuf::RpcController* controller,
                                       const ::selectdb::PartitionRequest* request,
                                       ::selectdb::PartitionResponse* response,
                                       ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(commit_partition);
    instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << request->cloud_unique_id();
        return;
    }
    RPC_RATE_LIMIT(commit_partition)
    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        msg = "failed to create txn";
        return;
    }
    remove_recycle_partition_kv(code, msg, ret, request, instance_id, txn.get());
}

void MetaServiceImpl::drop_partition(::google::protobuf::RpcController* controller,
                                     const ::selectdb::PartitionRequest* request,
                                     ::selectdb::PartitionResponse* response,
                                     ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(drop_partition);
    instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << request->cloud_unique_id();
        return;
    }
    RPC_RATE_LIMIT(drop_partition)
    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        msg = "failed to create txn";
        return;
    }
    put_recycle_partition_kv(code, msg, ret, request, instance_id, txn.get(),
                             RecyclePartitionPB::DROP);
}

void MetaServiceImpl::get_tablet_stats(::google::protobuf::RpcController* controller,
                                       const ::selectdb::GetTabletStatsRequest* request,
                                       ::selectdb::GetTabletStatsResponse* response,
                                       ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(get_tablet_stats);
    instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << request->cloud_unique_id();
        return;
    }
    RPC_RATE_LIMIT(get_tablet_stats)

    std::unique_ptr<Transaction> txn;
    for (auto& i : request->tablet_idx()) {
        // FIXME(plat1ko): Get all tablet stats in one txn
        ret = txn_kv_->create_txn(&txn);
        if (ret != 0) {
            code = MetaServiceCode::KV_TXN_CREATE_ERR;
            msg = fmt::format("failed to create txn, tablet_id={}", i.tablet_id());
            return;
        }
        if (!(/* i.has_db_id() && */ i.has_table_id() && i.has_index_id() && i.has_partition_id() &&
              i.has_tablet_id())) {
            get_tablet_idx(code, msg, ret, txn.get(), instance_id, i.tablet_id(),
                           const_cast<TabletIndexPB&>(i));
            if (code != MetaServiceCode::OK) return;
        }
        auto tablet_stats = response->add_tablet_stats();
        internal_get_tablet_stats(code, msg, ret, txn.get(), instance_id, i, *tablet_stats, true);
        if (code != MetaServiceCode::OK) {
            response->clear_tablet_stats();
            break;
        }
#ifdef NDEBUG
        // Force data size >= 0 to reduce the losses caused by bugs
        if (tablet_stats->data_size() < 0) tablet_stats->set_data_size(0);
#endif
    }
    msg = ss.str();
}

static int encrypt_ak_sk_helper(const std::string plain_ak, const std::string plain_sk,
                                EncryptionInfoPB* encryption_info, AkSkPair* cipher_ak_sk_pair,
                                MetaServiceCode& code, std::string& msg) {
    std::string key;
    int64_t key_id;
    int ret = get_newest_encryption_key_for_ak_sk(&key_id, &key);
    {
        TEST_SYNC_POINT_CALLBACK("encrypt_ak_sk:get_encryption_key_ret", &ret);
        TEST_SYNC_POINT_CALLBACK("encrypt_ak_sk:get_encryption_key", &key);
        TEST_SYNC_POINT_CALLBACK("encrypt_ak_sk:get_encryption_key_id", &key_id);
    }
    if (ret != 0) {
        msg = "failed to get encryption key";
        code = MetaServiceCode::ERR_ENCRYPT;
        LOG(WARNING) << msg;
        return -1;
    }
    auto& encryption_method = get_encryption_method_for_ak_sk();
    AkSkPair plain_ak_sk_pair {plain_ak, plain_sk};
    ret = encrypt_ak_sk(plain_ak_sk_pair, encryption_method, key, cipher_ak_sk_pair);
    if (ret != 0) {
        msg = "failed to encrypt";
        code = MetaServiceCode::ERR_ENCRYPT;
        LOG(WARNING) << msg;
        return -1;
    }
    encryption_info->set_key_id(key_id);
    encryption_info->set_encryption_method(std::move(encryption_method));
    return 0;
}

static int decrypt_ak_sk_helper(std::string_view cipher_ak, std::string_view cipher_sk,
                                const EncryptionInfoPB& encryption_info, AkSkPair* plain_ak_sk_pair,
                                MetaServiceCode& code, std::string& msg) {
    int ret = decrypt_ak_sk_helper(cipher_ak, cipher_sk, encryption_info, plain_ak_sk_pair);
    if (ret != 0) {
        msg = "failed to decrypt";
        code = MetaServiceCode::ERR_DECPYPT;
    }
    return ret;
}

int decrypt_instance_info(InstanceInfoPB& instance, const std::string& instance_id,
                          MetaServiceCode& code, std::string& msg,
                          std::shared_ptr<Transaction>& txn) {
    for (auto& obj_info : *instance.mutable_obj_info()) {
        if (obj_info.has_encryption_info()) {
            AkSkPair plain_ak_sk_pair;
            int ret = decrypt_ak_sk_helper(obj_info.ak(), obj_info.sk(), obj_info.encryption_info(),
                                           &plain_ak_sk_pair, code, msg);
            if (ret != 0) return -1;
            obj_info.set_ak(std::move(plain_ak_sk_pair.first));
            obj_info.set_sk(std::move(plain_ak_sk_pair.second));
        }
    }
    if (instance.has_ram_user() && instance.ram_user().has_encryption_info()) {
        auto& ram_user = *instance.mutable_ram_user();
        AkSkPair plain_ak_sk_pair;
        int ret = decrypt_ak_sk_helper(ram_user.ak(), ram_user.sk(), ram_user.encryption_info(),
                                       &plain_ak_sk_pair, code, msg);
        if (ret != 0) return -1;
        ram_user.set_ak(std::move(plain_ak_sk_pair.first));
        ram_user.set_sk(std::move(plain_ak_sk_pair.second));
    }

    std::string val;
    int ret = txn->get(system_meta_service_arn_info_key(), &val);
    if (ret == 1) {
        // For compatibility, use arn_info of config
        RamUserPB iam_user;
        iam_user.set_user_id(config::arn_id);
        iam_user.set_external_id(instance_id);
        iam_user.set_ak(config::arn_ak);
        iam_user.set_sk(config::arn_sk);
        instance.mutable_iam_user()->CopyFrom(iam_user);
    } else if (ret == 0) {
        RamUserPB iam_user;
        if (!iam_user.ParseFromString(val)) {
            code = MetaServiceCode::PROTOBUF_PARSE_ERR;
            msg = "failed to parse RamUserPB";
            LOG(WARNING) << msg;
            return -1;
        }
        AkSkPair plain_ak_sk_pair;
        int ret = decrypt_ak_sk_helper(iam_user.ak(), iam_user.sk(), iam_user.encryption_info(),
                                       &plain_ak_sk_pair, code, msg);
        if (ret != 0) return -1;
        iam_user.set_ak(std::move(plain_ak_sk_pair.first));
        iam_user.set_sk(std::move(plain_ak_sk_pair.second));
        instance.mutable_iam_user()->CopyFrom(iam_user);
    } else {
        code = MetaServiceCode::KV_TXN_GET_ERR;
        msg = "failed to get arn_info_key";
        LOG(WARNING) << msg << " ret=" << ret;
        return -1;
    }

    for (auto& stage : *instance.mutable_stages()) {
        if (stage.has_obj_info() && stage.obj_info().has_encryption_info()) {
            auto& obj_info = *stage.mutable_obj_info();
            AkSkPair plain_ak_sk_pair;
            int ret = decrypt_ak_sk_helper(obj_info.ak(), obj_info.sk(), obj_info.encryption_info(),
                                           &plain_ak_sk_pair, code, msg);
            if (ret != 0) return -1;
            obj_info.set_ak(std::move(plain_ak_sk_pair.first));
            obj_info.set_sk(std::move(plain_ak_sk_pair.second));
        }
    }
    return 0;
}

// TODO: move to separate file
//==============================================================================
// Resources
//==============================================================================

void MetaServiceImpl::get_obj_store_info(google::protobuf::RpcController* controller,
                                         const ::selectdb::GetObjStoreInfoRequest* request,
                                         ::selectdb::GetObjStoreInfoResponse* response,
                                         ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(get_obj_store_info);
    // Prepare data
    std::string cloud_unique_id = request->has_cloud_unique_id() ? request->cloud_unique_id() : "";
    if (cloud_unique_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "cloud unique id not set";
        return;
    }

    instance_id = get_instance_id(resource_mgr_, cloud_unique_id);
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << cloud_unique_id;
        return;
    }
    RPC_RATE_LIMIT(get_obj_store_info)
    InstanceKeyInfo key_info {instance_id};
    std::string key;
    std::string val;
    instance_key(key_info, &key);

    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        msg = "failed to create txn";
        LOG(WARNING) << msg << " ret=" << ret;
        return;
    }
    ret = txn->get(key, &val);
    LOG(INFO) << "get instance_key=" << hex(key);

    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_GET_ERR;
        ss << "failed to get instance, instance_id=" << instance_id << " ret=" << ret;
        msg = ss.str();
        return;
    }

    InstanceInfoPB instance;
    if (!instance.ParseFromString(val)) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        msg = "failed to parse InstanceInfoPB";
        return;
    }
    for (auto& obj_info : *instance.mutable_obj_info()) {
        if (obj_info.has_encryption_info()) {
            AkSkPair plain_ak_sk_pair;
            int ret = decrypt_ak_sk_helper(obj_info.ak(), obj_info.sk(), obj_info.encryption_info(),
                                           &plain_ak_sk_pair, code, msg);
            if (ret != 0) return;
            obj_info.set_ak(std::move(plain_ak_sk_pair.first));
            obj_info.set_sk(std::move(plain_ak_sk_pair.second));
        }
    }
    response->mutable_obj_info()->CopyFrom(instance.obj_info());
}

void MetaServiceImpl::alter_obj_store_info(google::protobuf::RpcController* controller,
                                           const ::selectdb::AlterObjStoreInfoRequest* request,
                                           ::selectdb::AlterObjStoreInfoResponse* response,
                                           ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(alter_obj_store_info);
    // Prepare data
    if (!request->has_obj() || !request->obj().has_ak() || !request->obj().has_sk()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "s3 obj info err " + proto_to_json(*request);
        return;
    }

    auto& obj = request->obj();
    std::string plain_ak = obj.has_ak() ? obj.ak() : "";
    std::string plain_sk = obj.has_sk() ? obj.sk() : "";

    EncryptionInfoPB encryption_info;
    AkSkPair cipher_ak_sk_pair;
    ret = encrypt_ak_sk_helper(plain_ak, plain_sk, &encryption_info, &cipher_ak_sk_pair, code, msg);
    if (ret != 0) {
        return;
    }
    const auto& [ak, sk] = cipher_ak_sk_pair;
    std::string bucket = obj.has_bucket() ? obj.bucket() : "";
    std::string prefix = obj.has_prefix() ? obj.prefix() : "";
    std::string endpoint = obj.has_endpoint() ? obj.endpoint() : "";
    std::string external_endpoint = obj.has_external_endpoint() ? obj.external_endpoint() : "";
    std::string region = obj.has_region() ? obj.region() : "";

    //  obj size > 1k, refuse
    if (obj.ByteSizeLong() > 1024) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "s3 obj info greater than 1k " + proto_to_json(*request);
        return;
    }

    // TODO(dx): check s3 info right

    std::string cloud_unique_id = request->has_cloud_unique_id() ? request->cloud_unique_id() : "";
    if (cloud_unique_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "cloud unique id not set";
        return;
    }

    instance_id = get_instance_id(resource_mgr_, cloud_unique_id);
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << cloud_unique_id;
        return;
    }
    RPC_RATE_LIMIT(alter_obj_store_info)
    InstanceKeyInfo key_info {instance_id};
    std::string key;
    std::string val;
    instance_key(key_info, &key);

    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        msg = "failed to create txn";
        LOG(WARNING) << msg << " ret=" << ret;
        return;
    }
    ret = txn->get(key, &val);
    LOG(INFO) << "get instance_key=" << hex(key);

    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_GET_ERR;
        ss << "failed to get instance, instance_id=" << instance_id << " ret=" << ret;
        msg = ss.str();
        return;
    }

    InstanceInfoPB instance;
    if (!instance.ParseFromString(val)) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        msg = "failed to parse InstanceInfoPB";
        return;
    }

    if (instance.status() != InstanceInfoPB::NORMAL) {
        code = MetaServiceCode::CLUSTER_NOT_FOUND;
        msg = "instance status has been set delete, plz check it";
        return;
    }

    auto now_time = std::chrono::system_clock::now();
    uint64_t time =
            std::chrono::duration_cast<std::chrono::seconds>(now_time.time_since_epoch()).count();

    switch (request->op()) {
    case AlterObjStoreInfoRequest::LEGACY_UPDATE_AK_SK: {
        // get id
        std::string id = request->obj().has_id() ? request->obj().id() : "0";
        int idx = std::stoi(id);
        if (idx < 1 || idx > instance.obj_info().size()) {
            // err
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = "id invalid, please check it";
            return;
        }
        auto& obj_info =
                const_cast<std::decay_t<decltype(instance.obj_info())>&>(instance.obj_info());
        for (auto& it : obj_info) {
            if (std::stoi(it.id()) == idx) {
                if (it.ak() == ak && it.sk() == sk) {
                    // not change, just return ok
                    code = MetaServiceCode::OK;
                    msg = "";
                    return;
                }
                it.set_mtime(time);
                it.set_ak(ak);
                it.set_sk(sk);
                it.mutable_encryption_info()->CopyFrom(encryption_info);
            }
        }
    } break;
    case AlterObjStoreInfoRequest::ADD_OBJ_INFO: {
        if (!obj.has_provider()) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = "s3 conf lease provider info";
            return;
        }
        if (instance.obj_info().size() >= 10) {
            code = MetaServiceCode::UNDEFINED_ERR;
            msg = "this instance history has greater than 10 objs, please new another instance";
            return;
        }
        // ATTN: prefix may be empty
        if (ak.empty() || sk.empty() || bucket.empty() || endpoint.empty() || region.empty()) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = "s3 conf info err, please check it";
            return;
        }

        auto& objs = instance.obj_info();
        for (auto& it : objs) {
            if (bucket == it.bucket() && prefix == it.prefix() && endpoint == it.endpoint() &&
                region == it.region() && ak == it.ak() && sk == it.sk() &&
                obj.provider() == it.provider() && external_endpoint == it.external_endpoint()) {
                // err, anything not changed
                code = MetaServiceCode::INVALID_ARGUMENT;
                msg = "original obj infos has a same conf, please check it";
                return;
            }
        }
        // calc id
        selectdb::ObjectStoreInfoPB last_item;
        last_item.set_ctime(time);
        last_item.set_mtime(time);
        last_item.set_id(std::to_string(instance.obj_info().size() + 1));
        if (obj.has_user_id()) {
            last_item.set_user_id(obj.user_id());
        }
        last_item.set_ak(std::move(cipher_ak_sk_pair.first));
        last_item.set_sk(std::move(cipher_ak_sk_pair.second));
        last_item.mutable_encryption_info()->CopyFrom(encryption_info);
        last_item.set_bucket(bucket);
        // format prefix, such as `/aa/bb/`, `aa/bb//`, `//aa/bb`, `  /aa/bb` -> `aa/bb`
        prefix = trim(prefix);
        last_item.set_prefix(prefix);
        last_item.set_endpoint(endpoint);
        last_item.set_external_endpoint(external_endpoint);
        last_item.set_region(region);
        last_item.set_provider(obj.provider());
        last_item.set_sse_enabled(instance.sse_enabled());
        instance.add_obj_info()->CopyFrom(last_item);
    } break;
    default: {
        code = MetaServiceCode::INVALID_ARGUMENT;
        ss << "invalid request op, op=" << request->op();
        msg = ss.str();
        return;
    }
    }

    LOG(INFO) << "instance " << instance_id << " has " << instance.obj_info().size()
              << " s3 history info, and instance = " << proto_to_json(instance);

    val = instance.SerializeAsString();
    if (val.empty()) {
        msg = "failed to serialize";
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        return;
    }

    txn->put(key, val);
    LOG(INFO) << "put instance_id=" << instance_id << " instance_key=" << hex(key);
    ret = txn->commit();
    if (ret != 0) {
        code = ret == -1 ? MetaServiceCode::KV_TXN_CONFLICT : MetaServiceCode::KV_TXN_COMMIT_ERR;
        msg = fmt::format("failed to commit kv txn, ret={}", ret);
        LOG(WARNING) << msg;
    }
    return;
}

void MetaServiceImpl::update_ak_sk(google::protobuf::RpcController* controller,
                                   const ::selectdb::UpdateAkSkRequest* request,
                                   ::selectdb::UpdateAkSkResponse* response,
                                   ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(update_ak_sk);
    instance_id = request->has_instance_id() ? request->instance_id() : "";
    if (instance_id.empty()) {
        msg = "instance id not set";
        code = MetaServiceCode::INVALID_ARGUMENT;
        return;
    }
    if (!request->has_ram_user() && request->internal_bucket_user().empty()) {
        msg = "nothing to update";
        code = MetaServiceCode::INVALID_ARGUMENT;
        return;
    }
    RPC_RATE_LIMIT(update_ak_sk)

    InstanceKeyInfo key_info {instance_id};
    std::string key;
    std::string val;
    instance_key(key_info, &key);

    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        msg = "failed to create txn";
        LOG(WARNING) << msg << " ret=" << ret;
        return;
    }
    ret = txn->get(key, &val);
    LOG(INFO) << "get instance_key=" << hex(key);

    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_GET_ERR;
        ss << "failed to get instance, instance_id=" << instance_id << " ret=" << ret;
        msg = ss.str();
        return;
    }

    InstanceInfoPB instance;
    if (!instance.ParseFromString(val)) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        msg = "failed to parse InstanceInfoPB";
        return;
    }

    if (instance.status() != InstanceInfoPB::NORMAL) {
        code = MetaServiceCode::CLUSTER_NOT_FOUND;
        msg = "instance status has been set delete, plz check it";
        return;
    }

    auto now_time = std::chrono::system_clock::now();
    uint64_t time =
            std::chrono::duration_cast<std::chrono::seconds>(now_time.time_since_epoch()).count();

    std::stringstream update_record;

    // if has ram_user, encrypt and save it
    if (request->has_ram_user()) {
        if (request->ram_user().user_id().empty() || request->ram_user().ak().empty() ||
            request->ram_user().sk().empty()) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = "ram user info err " + proto_to_json(*request);
            return;
        }
        if (!instance.has_ram_user()) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = "instance doesn't have ram user info";
            return;
        }
        auto& ram_user = request->ram_user();
        EncryptionInfoPB encryption_info;
        AkSkPair cipher_ak_sk_pair;
        ret = encrypt_ak_sk_helper(ram_user.ak(), ram_user.sk(), &encryption_info,
                                   &cipher_ak_sk_pair, code, msg);
        if (ret != 0) {
            return;
        }
        const auto& [ak, sk] = cipher_ak_sk_pair;
        auto& instance_ram_user = *instance.mutable_ram_user();
        if (ram_user.user_id() != instance_ram_user.user_id()) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = "ram user_id err";
            return;
        }
        std::string old_ak = instance_ram_user.ak();
        std::string old_sk = instance_ram_user.sk();
        if (old_ak == ak && old_sk == sk) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = "ak sk eq original, please check it";
            return;
        }
        instance_ram_user.set_ak(std::move(cipher_ak_sk_pair.first));
        instance_ram_user.set_sk(std::move(cipher_ak_sk_pair.second));
        instance_ram_user.mutable_encryption_info()->CopyFrom(encryption_info);
        update_record << "update ram_user's ak sk, instance_id: " << instance_id
                      << " user_id: " << ram_user.user_id() << " old:  cipher ak: " << old_ak
                      << " cipher sk: " << old_sk << " new: cipher ak: " << ak
                      << " cipher sk: " << sk;
    }

    bool has_found_alter_obj_info = false;
    for (auto& alter_bucket_user : request->internal_bucket_user()) {
        if (!alter_bucket_user.has_ak() || !alter_bucket_user.has_sk() ||
            !alter_bucket_user.has_user_id()) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = "s3 bucket info err " + proto_to_json(*request);
            return;
        }
        std::string user_id = alter_bucket_user.user_id();
        EncryptionInfoPB encryption_info;
        AkSkPair cipher_ak_sk_pair;
        ret = encrypt_ak_sk_helper(alter_bucket_user.ak(), alter_bucket_user.sk(), &encryption_info,
                                   &cipher_ak_sk_pair, code, msg);
        if (ret != 0) {
            return;
        }
        const auto& [ak, sk] = cipher_ak_sk_pair;
        auto& obj_info =
                const_cast<std::decay_t<decltype(instance.obj_info())>&>(instance.obj_info());
        for (auto& it : obj_info) {
            std::string old_ak = it.ak();
            std::string old_sk = it.sk();
            if (!it.has_user_id()) {
                has_found_alter_obj_info = true;
                // For compatibility, obj_info without a user_id only allow
                // single internal_bucket_user to modify it.
                if (request->internal_bucket_user_size() != 1) {
                    code = MetaServiceCode::INVALID_ARGUMENT;
                    msg = "fail to update old instance's obj_info, s3 obj info err " +
                          proto_to_json(*request);
                    return;
                }
                if (it.ak() == ak && it.sk() == sk) {
                    code = MetaServiceCode::INVALID_ARGUMENT;
                    msg = "ak sk eq original, please check it";
                    return;
                }
                it.set_mtime(time);
                it.set_user_id(user_id);
                it.set_ak(ak);
                it.set_sk(sk);
                it.mutable_encryption_info()->CopyFrom(encryption_info);
                update_record << "update obj_info's ak sk without user_id, instance_id: "
                              << instance_id << " obj_info_id: " << it.id()
                              << " new user_id: " << user_id << " old:  cipher ak: " << old_ak
                              << " cipher sk: " << old_sk << " new:  cipher ak: " << ak
                              << " cipher sk: " << sk;
                continue;
            }
            if (it.user_id() == user_id) {
                has_found_alter_obj_info = true;
                if (it.ak() == ak && it.sk() == sk) {
                    code = MetaServiceCode::INVALID_ARGUMENT;
                    msg = "ak sk eq original, please check it";
                    return;
                }
                it.set_mtime(time);
                it.set_ak(ak);
                it.set_sk(sk);
                it.mutable_encryption_info()->CopyFrom(encryption_info);
                update_record << "update obj_info's ak sk, instance_id: " << instance_id
                              << " obj_info_id: " << it.id() << " user_id: " << user_id
                              << " old:  cipher ak: " << old_ak << " cipher sk: " << old_sk
                              << " new:  cipher ak: " << ak << " cipher sk: " << sk;
            }
        }
    }

    if (!request->internal_bucket_user().empty() && !has_found_alter_obj_info) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "fail to find the alter obj info";
        return;
    }

    LOG(INFO) << "instance " << instance_id << " has " << instance.obj_info().size()
              << " s3 history info, and instance = " << proto_to_json(instance);

    val = instance.SerializeAsString();
    if (val.empty()) {
        msg = "failed to serialize";
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        return;
    }

    txn->put(key, val);
    LOG(INFO) << "put instance_id=" << instance_id << " instance_key=" << hex(key);
    ret = txn->commit();
    if (ret != 0) {
        code = ret == -1 ? MetaServiceCode::KV_TXN_CONFLICT : MetaServiceCode::KV_TXN_COMMIT_ERR;
        msg = fmt::format("failed to commit kv txn, ret={}", ret);
        LOG(WARNING) << msg;
    }
    LOG(INFO) << update_record.str();
    return;
}

void MetaServiceImpl::create_instance(google::protobuf::RpcController* controller,
                                      const ::selectdb::CreateInstanceRequest* request,
                                      ::selectdb::CreateInstanceResponse* response,
                                      ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(create_instance);
    instance_id = request->instance_id();
    // Prepare data
    auto& obj = request->obj_info();
    std::string plain_ak = obj.has_ak() ? obj.ak() : "";
    std::string plain_sk = obj.has_sk() ? obj.sk() : "";
    std::string bucket = obj.has_bucket() ? obj.bucket() : "";
    std::string prefix = obj.has_prefix() ? obj.prefix() : "";
    // format prefix, such as `/aa/bb/`, `aa/bb//`, `//aa/bb`, `  /aa/bb` -> `aa/bb`
    prefix = trim(prefix);
    std::string endpoint = obj.has_endpoint() ? obj.endpoint() : "";
    std::string external_endpoint = obj.has_external_endpoint() ? obj.external_endpoint() : "";
    std::string region = obj.has_region() ? obj.region() : "";

    // ATTN: prefix may be empty
    if (plain_ak.empty() || plain_sk.empty() || bucket.empty() || endpoint.empty() ||
        region.empty() || !obj.has_provider() || external_endpoint.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "s3 conf info err, please check it";
        return;
    }

    if (request->has_ram_user()) {
        auto& ram_user = request->ram_user();
        std::string ram_user_id = ram_user.has_user_id() ? ram_user.user_id() : "";
        std::string ram_user_ak = ram_user.has_ak() ? ram_user.ak() : "";
        std::string ram_user_sk = ram_user.has_sk() ? ram_user.sk() : "";
        if (ram_user_id.empty() || ram_user_ak.empty() || ram_user_sk.empty()) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = "ram user info err, please check it";
            return;
        }
    }

    EncryptionInfoPB encryption_info;
    AkSkPair cipher_ak_sk_pair;
    ret = encrypt_ak_sk_helper(plain_ak, plain_sk, &encryption_info, &cipher_ak_sk_pair, code, msg);
    if (ret != 0) {
        return;
    }
    InstanceInfoPB instance;
    instance.set_instance_id(instance_id);
    instance.set_user_id(request->has_user_id() ? request->user_id() : "");
    instance.set_name(request->has_name() ? request->name() : "");
    instance.set_status(InstanceInfoPB::NORMAL);
    instance.set_sse_enabled(request->sse_enabled());
    auto obj_info = instance.add_obj_info();
    if (obj.has_user_id()) {
        obj_info->set_user_id(obj.user_id());
    }
    obj_info->set_ak(std::move(cipher_ak_sk_pair.first));
    obj_info->set_sk(std::move(cipher_ak_sk_pair.second));
    obj_info->mutable_encryption_info()->CopyFrom(encryption_info);
    obj_info->set_bucket(bucket);
    obj_info->set_prefix(prefix);
    obj_info->set_endpoint(endpoint);
    obj_info->set_external_endpoint(external_endpoint);
    obj_info->set_region(region);
    obj_info->set_provider(obj.provider());
    std::ostringstream oss;
    // create instance's s3 conf, id = 1
    obj_info->set_id(std::to_string(1));
    auto now_time = std::chrono::system_clock::now();
    uint64_t time =
            std::chrono::duration_cast<std::chrono::seconds>(now_time.time_since_epoch()).count();
    obj_info->set_ctime(time);
    obj_info->set_mtime(time);
    obj_info->set_sse_enabled(instance.sse_enabled());
    if (request->has_ram_user()) {
        auto& ram_user = request->ram_user();
        EncryptionInfoPB encryption_info;
        AkSkPair cipher_ak_sk_pair;
        ret = encrypt_ak_sk_helper(ram_user.ak(), ram_user.sk(), &encryption_info,
                                   &cipher_ak_sk_pair, code, msg);
        if (ret != 0) {
            return;
        }
        RamUserPB new_ram_user;
        new_ram_user.CopyFrom(ram_user);
        new_ram_user.set_ak(std::move(cipher_ak_sk_pair.first));
        new_ram_user.set_sk(std::move(cipher_ak_sk_pair.second));
        new_ram_user.mutable_encryption_info()->CopyFrom(encryption_info);
        instance.mutable_ram_user()->CopyFrom(new_ram_user);
    }

    if (instance.instance_id().empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "instance id not set";
        return;
    }

    InstanceKeyInfo key_info {request->instance_id()};
    std::string key;
    std::string val = instance.SerializeAsString();
    instance_key(key_info, &key);
    if (val.empty()) {
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        msg = "failed to serialize";
        LOG(ERROR) << msg;
        return;
    }

    LOG(INFO) << "xxx instance json=" << proto_to_json(instance);

    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        msg = "failed to create txn";
        LOG(WARNING) << msg << " ret=" << ret;
        return;
    }

    // Check existence before proceeding
    ret = txn->get(key, &val);
    if (ret != 1) {
        std::stringstream ss;
        ss << (ret == 0 ? "instance already existed" : "internal error failed to check instance")
           << ", instance_id=" << request->instance_id();
        code = ret == 0 ? MetaServiceCode::ALREADY_EXISTED : MetaServiceCode::UNDEFINED_ERR;
        msg = ss.str();
        LOG(WARNING) << msg << " ret=" << ret;
        return;
    }

    txn->put(key, val);
    LOG(INFO) << "put instance_id=" << request->instance_id() << " instance_key=" << hex(key);
    ret = txn->commit();
    if (ret != 0) {
        code = ret == -1 ? MetaServiceCode::KV_TXN_CONFLICT : MetaServiceCode::KV_TXN_COMMIT_ERR;
        msg = fmt::format("failed to commit kv txn, ret={}", ret);
        LOG(WARNING) << msg;
    }
}

void MetaServiceImpl::alter_instance(google::protobuf::RpcController* controller,
                                     const ::selectdb::AlterInstanceRequest* request,
                                     ::selectdb::AlterInstanceResponse* response,
                                     ::google::protobuf::Closure* done) {
    StopWatch sw;
    auto ctrl = static_cast<brpc::Controller*>(controller);
    LOG(INFO) << __PRETTY_FUNCTION__ << " rpc from " << ctrl->remote_side()
              << " request=" << request->ShortDebugString();
    brpc::ClosureGuard closure_guard(done);
    MetaServiceCode code = MetaServiceCode::OK;
    std::string msg = "OK";
    [[maybe_unused]] std::stringstream ss;
    std::string instance_id = request->has_instance_id() ? request->instance_id() : "";
    std::unique_ptr<int, std::function<void(int*)>> defer_status(
            (int*)0x01, [&code, &msg, &response, &ctrl, &closure_guard, &sw, &instance_id](int*) {
                response->mutable_status()->set_code(code);
                response->mutable_status()->set_msg(msg);
                LOG(INFO) << (code == MetaServiceCode::OK ? "succ to " : "failed to ")
                          << __PRETTY_FUNCTION__ << " " << ctrl->remote_side() << " " << msg;
                closure_guard.reset(nullptr);
                if (config::use_detailed_metrics && !instance_id.empty()) {
                    g_bvar_ms_alter_instance.put(instance_id, sw.elapsed_us());
                }
            });

    std::pair<MetaServiceCode, std::string> ret;
    switch (request->op()) {
    case AlterInstanceRequest::DROP: {
        ret = alter_instance(request, [&request](InstanceInfoPB* instance) {
            std::string msg;
            // check instance doesn't have any cluster.
            if (instance->clusters_size() != 0) {
                msg = "failed to drop instance, instance has clusters";
                LOG(WARNING) << msg;
                return std::make_pair(MetaServiceCode::INVALID_ARGUMENT, msg);
            }

            instance->set_status(InstanceInfoPB::DELETED);
            instance->set_mtime(
                    duration_cast<seconds>(system_clock::now().time_since_epoch()).count());

            std::string ret = instance->SerializeAsString();
            if (ret.empty()) {
                msg = "failed to serialize";
                LOG(ERROR) << msg;
                return std::make_pair(MetaServiceCode::PROTOBUF_SERIALIZE_ERR, msg);
            }
            LOG(INFO) << "put instance_id=" << request->instance_id()
                      << "drop instance json=" << proto_to_json(*instance);
            return std::make_pair(MetaServiceCode::OK, ret);
        });
    } break;
    case AlterInstanceRequest::RENAME: {
        ret = alter_instance(request, [&request](InstanceInfoPB* instance) {
            std::string msg;
            std::string name = request->has_name() ? request->name() : "";
            if (name.empty()) {
                msg = "rename instance name, but not set";
                LOG(WARNING) << msg;
                return std::make_pair(MetaServiceCode::INVALID_ARGUMENT, msg);
            }
            instance->set_name(name);

            std::string ret = instance->SerializeAsString();
            if (ret.empty()) {
                msg = "failed to serialize";
                LOG(ERROR) << msg;
                return std::make_pair(MetaServiceCode::PROTOBUF_SERIALIZE_ERR, msg);
            }
            LOG(INFO) << "put instance_id=" << request->instance_id()
                      << "rename instance json=" << proto_to_json(*instance);
            return std::make_pair(MetaServiceCode::OK, ret);
        });
    } break;
    case AlterInstanceRequest::ENABLE_SSE: {
        ret = alter_instance(request, [&request](InstanceInfoPB* instance) {
            std::string msg;
            if (instance->sse_enabled()) {
                msg = "failed to enable sse, instance has enabled sse";
                LOG(WARNING) << msg;
                return std::make_pair(MetaServiceCode::INVALID_ARGUMENT, msg);
            }
            instance->set_sse_enabled(true);
            instance->set_mtime(
                    duration_cast<seconds>(system_clock::now().time_since_epoch()).count());

            for (auto& obj_info : *(instance->mutable_obj_info())) {
                obj_info.set_sse_enabled(true);
            }
            std::string ret = instance->SerializeAsString();
            if (ret.empty()) {
                msg = "failed to serialize";
                LOG(ERROR) << msg;
                return std::make_pair(MetaServiceCode::PROTOBUF_SERIALIZE_ERR, msg);
            }
            LOG(INFO) << "put instance_id=" << request->instance_id()
                      << "instance enable sse json=" << proto_to_json(*instance);
            return std::make_pair(MetaServiceCode::OK, ret);
        });
    } break;
    case AlterInstanceRequest::DISABLE_SSE: {
        ret = alter_instance(request, [&request](InstanceInfoPB* instance) {
            std::string msg;
            if (!instance->sse_enabled()) {
                msg = "failed to disable sse, instance has disabled sse";
                LOG(WARNING) << msg;
                return std::make_pair(MetaServiceCode::INVALID_ARGUMENT, msg);
            }
            instance->set_sse_enabled(false);
            instance->set_mtime(
                    duration_cast<seconds>(system_clock::now().time_since_epoch()).count());

            for (auto& obj_info : *(instance->mutable_obj_info())) {
                obj_info.set_sse_enabled(false);
            }
            std::string ret = instance->SerializeAsString();
            if (ret.empty()) {
                msg = "failed to serialize";
                LOG(ERROR) << msg;
                return std::make_pair(MetaServiceCode::PROTOBUF_SERIALIZE_ERR, msg);
            }
            LOG(INFO) << "put instance_id=" << request->instance_id()
                      << "instance disable sse json=" << proto_to_json(*instance);
            return std::make_pair(MetaServiceCode::OK, ret);
        });
    } break;
    case AlterInstanceRequest::REFRESH: {
        ret = resource_mgr_->refresh_instance(request->instance_id());
    } break;
    case AlterInstanceRequest::SET_OVERDUE: {
        ret = alter_instance(request, [&request](InstanceInfoPB* instance) {
            std::string msg;

            if (instance->status() == InstanceInfoPB::DELETED) {
                msg = "can't set deleted instance to overdue, instance_id = " + request->instance_id();
                LOG(WARNING) << msg;
                return std::make_pair(MetaServiceCode::INVALID_ARGUMENT, msg);
            }
            if (instance->status() == InstanceInfoPB::OVERDUE) {
                msg = "the instance has already set  instance to overdue, instance_id = " + request->instance_id();
                LOG(WARNING) << msg;
                return std::make_pair(MetaServiceCode::INVALID_ARGUMENT, msg);
            }
            instance->set_status(InstanceInfoPB::OVERDUE);
            instance->set_mtime(
                    duration_cast<seconds>(system_clock::now().time_since_epoch()).count());

            std::string ret = instance->SerializeAsString();
            if (ret.empty()) {
                msg = "failed to serialize";
                LOG(WARNING) << msg;
                return std::make_pair(MetaServiceCode::PROTOBUF_SERIALIZE_ERR, msg);
            }
            LOG(INFO) << "put instance_id=" << request->instance_id()
                    << "set instance overdue json=" << proto_to_json(*instance);
            return std::make_pair(MetaServiceCode::OK, ret);
        });
    } break;
    case AlterInstanceRequest::SET_NORMAL: {
        ret = alter_instance(request, [&request](InstanceInfoPB* instance) {
            std::string msg;

            if (instance->status() == InstanceInfoPB::DELETED) {
                msg = "can't set deleted instance to normal, instance_id = " + request->instance_id();
                LOG(WARNING) << msg;
                return std::make_pair(MetaServiceCode::INVALID_ARGUMENT, msg);
            }
            if (instance->status() == InstanceInfoPB::NORMAL) {
                msg = "the instance is already normal, instance_id = " + request->instance_id();
                LOG(WARNING) << msg;
                return std::make_pair(MetaServiceCode::INVALID_ARGUMENT, msg);
            }
            instance->set_status(InstanceInfoPB::NORMAL);
            instance->set_mtime(
                    duration_cast<seconds>(system_clock::now().time_since_epoch()).count());

            std::string ret = instance->SerializeAsString();
            if (ret.empty()) {
                msg = "failed to serialize";
                LOG(WARNING) << msg;
                return std::make_pair(MetaServiceCode::PROTOBUF_SERIALIZE_ERR, msg);
            }
            LOG(INFO) << "put instance_id=" << request->instance_id()
                    << "set instance normal json=" << proto_to_json(*instance);
            return std::make_pair(MetaServiceCode::OK, ret);
        });
    } break;
    default: {
        ss << "invalid request op, op=" << request->op();
        ret = std::make_pair(MetaServiceCode::INVALID_ARGUMENT, ss.str());
    }
    }
    code = ret.first;
    msg = ret.second;

    if (request->op() == AlterInstanceRequest::REFRESH) return;

    auto f = new std::function<void()>([instance_id = request->instance_id(), txn_kv = txn_kv_] {
        notify_refresh_instance(txn_kv, instance_id);
    });
    bthread_t bid;
    if (bthread_start_background(&bid, nullptr, run_bthread_work, f) != 0) {
        LOG(WARNING) << "notify refresh instance inplace, instance_id=" << request->instance_id();
        run_bthread_work(f);
    }
}

void MetaServiceImpl::get_instance(google::protobuf::RpcController* controller,
                                     const ::selectdb::GetInstanceRequest* request,
                                     ::selectdb::GetInstanceResponse* response,
                                     ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(get_instance);
    std::string cloud_unique_id = request->has_cloud_unique_id() ? request->cloud_unique_id() : "";
    if (cloud_unique_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "cloud_unique_id must be given";
        return;
    }
    instance_id = get_instance_id(resource_mgr_, cloud_unique_id);
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << cloud_unique_id;
        return;
    }
    RPC_RATE_LIMIT(get_instance);
    InstanceKeyInfo key_info {instance_id};
    std::string key;
    std::string val;
    instance_key(key_info, &key);

    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        msg = "failed to create txn";
        LOG(WARNING) << msg << " ret=" << ret;
        return;
    }
    ret = txn->get(key, &val);
    LOG(INFO) << "get instance_key=" << hex(key);

    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_GET_ERR;
        ss << "failed to get instance, instance_id=" << instance_id << " ret=" << ret;
        msg = ss.str();
        return;
    }

    InstanceInfoPB instance;
    if (!instance.ParseFromString(val)) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        msg = "failed to parse InstanceInfoPB";
        return;
    }

    response->mutable_instance()->CopyFrom(instance);
    return;
}

std::pair<MetaServiceCode, std::string> MetaServiceImpl::alter_instance(
        const selectdb::AlterInstanceRequest* request,
        std::function<std::pair<MetaServiceCode, std::string>(InstanceInfoPB*)> action) {
    int ret = 0;
    MetaServiceCode code = MetaServiceCode::OK;
    std::string msg = "OK";
    std::string instance_id = request->has_instance_id() ? request->instance_id() : "";
    if (instance_id.empty()) {
        msg = "instance id not set";
        LOG(WARNING) << msg;
        return std::make_pair(MetaServiceCode::INVALID_ARGUMENT, msg);
    }

    InstanceKeyInfo key_info {instance_id};
    std::string key;
    std::string val;
    instance_key(key_info, &key);
    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        msg = "failed to create txn";
        LOG(WARNING) << msg << " ret=" << ret;
        return std::make_pair(MetaServiceCode::KV_TXN_CREATE_ERR, msg);
    }

    // Check existence before proceeding
    ret = txn->get(key, &val);
    if (ret != 0) {
        std::stringstream ss;
        ss << (ret == 1 ? "instance not existed" : "internal error failed to check instance")
           << ", instance_id=" << request->instance_id();
        // TODO(dx): fix CLUSTER_NOT_FOUND，VERSION_NOT_FOUND，TXN_LABEL_NOT_FOUND，etc to NOT_FOUND
        code = ret == 1 ? MetaServiceCode::CLUSTER_NOT_FOUND : MetaServiceCode::UNDEFINED_ERR;
        msg = ss.str();
        LOG(WARNING) << msg << " ret=" << ret;
        return std::make_pair(code, msg);
    }
    LOG(INFO) << "alter instance key=" << hex(key);
    InstanceInfoPB instance;
    if (!instance.ParseFromString(val)) {
        msg = "failed to parse InstanceInfoPB";
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        LOG(WARNING) << msg;
        return std::make_pair(code, msg);
    }
    auto r = action(&instance);
    if (r.first != MetaServiceCode::OK) {
        return r;
    }
    val = r.second;
    txn->put(key, val);
    ret = txn->commit();
    if (ret != 0) {
        code = ret == -1 ? MetaServiceCode::KV_TXN_CONFLICT : MetaServiceCode::KV_TXN_COMMIT_ERR;
        msg = fmt::format("failed to commit kv txn, ret={}", ret);
        LOG(WARNING) << msg;
        return std::make_pair(code, msg);
    }
    return std::make_pair(code, msg);
}

std::string_view static print_cluster_status(const ::selectdb::ClusterStatus& status) {
    switch (status) {
        case ClusterStatus::UNKNOWN:
            return "UNKNOWN";
        case ClusterStatus::NORMAL:
            return "NORMAL";
        case ClusterStatus::SUSPENDED:
            return "SUSPENDED";
        case ClusterStatus::TO_RESUME:
            return "TO_RESUME";
        default:
            return "UNKNOWN";
    }
}

void MetaServiceImpl::alter_cluster(google::protobuf::RpcController* controller,
                                    const ::selectdb::AlterClusterRequest* request,
                                    ::selectdb::AlterClusterResponse* response,
                                    ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(alter_cluster);
    std::string cloud_unique_id = request->has_cloud_unique_id() ? request->cloud_unique_id() : "";
    instance_id = request->has_instance_id() ? request->instance_id() : "";
    if (!cloud_unique_id.empty() && instance_id.empty()) {
        instance_id = get_instance_id(resource_mgr_, cloud_unique_id);
        if (instance_id.empty()) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = "empty instance_id";
            LOG(INFO) << msg << ", cloud_unique_id=" << cloud_unique_id;
            return;
        }
    }

    if (instance_id.empty() || !request->has_cluster()) {
        msg = "invalid request instance_id or cluster not given";
        code = MetaServiceCode::INVALID_ARGUMENT;
        return;
    }

    if (!request->has_op()) {
        msg = "op not given";
        code = MetaServiceCode::INVALID_ARGUMENT;
        return;
    }

    LOG(INFO) << "alter cluster instance_id=" << instance_id << " op=" << request->op();
    ClusterInfo cluster;
    cluster.cluster.CopyFrom(request->cluster());

    switch (request->op()) {
    case AlterClusterRequest::ADD_CLUSTER: {
        auto r = resource_mgr_->add_cluster(instance_id, cluster);
        code = r.first;
        msg = r.second;
    } break;
    case AlterClusterRequest::DROP_CLUSTER: {
        auto r = resource_mgr_->drop_cluster(instance_id, cluster);
        code = r.first;
        msg = r.second;
    } break;
    case AlterClusterRequest::UPDATE_CLUSTER_MYSQL_USER_NAME: {
        msg = resource_mgr_->update_cluster(
                instance_id, cluster,
                [&](const ::selectdb::ClusterPB& i) {
                    return i.cluster_id() == cluster.cluster.cluster_id();
                },
                [&](::selectdb::ClusterPB& c, std::set<std::string>& cluster_names) {
                    auto& mysql_user_names = cluster.cluster.mysql_user_name();
                    c.mutable_mysql_user_name()->CopyFrom(mysql_user_names);
                    return "";
                });
    } break;
    case AlterClusterRequest::ADD_NODE: {
        resource_mgr_->check_cluster_params_valid(request->cluster(), &msg, false);
        if (msg != "") {
            LOG(INFO) << msg;
            break;
        }
        std::vector<NodeInfo> to_add;
        std::vector<NodeInfo> to_del;
        for (auto& n : request->cluster().nodes()) {
            NodeInfo node;
            node.instance_id = request->instance_id();
            node.node_info = n;
            node.cluster_id = request->cluster().cluster_id();
            node.cluster_name = request->cluster().cluster_name();
            node.role =
                    (request->cluster().type() == ClusterPB::SQL
                             ? Role::SQL_SERVER
                             : (request->cluster().type() == ClusterPB::COMPUTE ? Role::COMPUTE_NODE
                                                                                : Role::UNDEFINED));
            node.node_info.set_status(NodeStatusPB::NODE_STATUS_RUNNING);
            to_add.emplace_back(std::move(node));
        }
        msg = resource_mgr_->modify_nodes(instance_id, to_add, to_del);
    } break;
    case AlterClusterRequest::DROP_NODE: {
        resource_mgr_->check_cluster_params_valid(request->cluster(), &msg, false);
        if (msg != "") {
            LOG(INFO) << msg;
            break;
        }
        std::vector<NodeInfo> to_add;
        std::vector<NodeInfo> to_del;
        for (auto& n : request->cluster().nodes()) {
            NodeInfo node;
            node.instance_id = request->instance_id();
            node.node_info = n;
            node.cluster_id = request->cluster().cluster_id();
            node.cluster_name = request->cluster().cluster_name();
            node.role =
                    (request->cluster().type() == ClusterPB::SQL
                             ? Role::SQL_SERVER
                             : (request->cluster().type() == ClusterPB::COMPUTE ? Role::COMPUTE_NODE
                                                                                : Role::UNDEFINED));
            to_del.emplace_back(std::move(node));
        }
        msg = resource_mgr_->modify_nodes(instance_id, to_add, to_del);
    } break;
    case AlterClusterRequest::DECOMMISSION_NODE: {
        resource_mgr_->check_cluster_params_valid(request->cluster(), &msg, false);
        if (msg != "") {
            LOG(INFO) << msg;
            break;
        }

        std::string be_unique_id = (request->cluster().nodes())[0].cloud_unique_id();
        std::vector<NodeInfo> nodes;
        std::string err = resource_mgr_->get_node(be_unique_id, &nodes);
        if (!err.empty()) {
            LOG(INFO) << "failed to check instance info, err=" << err;
            msg = err;
            break;
        }

        std::vector<NodeInfo> decomission_nodes;
        for (auto& node : nodes) {
            for (auto req_node : request->cluster().nodes()) {
                bool ip_processed = false;
                if (node.node_info.has_ip() && req_node.has_ip()) {
                    std::string endpoint =
                        node.node_info.ip() + ":" + std::to_string(node.node_info.heartbeat_port());
                    std::string req_endpoint =
                        req_node.ip() + ":" + std::to_string(req_node.heartbeat_port());
                    if (endpoint == req_endpoint) {
                        decomission_nodes.push_back(node);
                        node.node_info.set_status(NodeStatusPB::NODE_STATUS_DECOMMISSIONING);
                    }
                    ip_processed = true;
                }

                if (!ip_processed && node.node_info.has_host() && req_node.has_host()) {
                    std::string endpoint =
                        node.node_info.host() + ":" + std::to_string(node.node_info.heartbeat_port());
                    std::string req_endpoint =
                        req_node.host() + ":" + std::to_string(req_node.heartbeat_port());
                    if (endpoint == req_endpoint) {
                        decomission_nodes.push_back(node);
                        node.node_info.set_status(NodeStatusPB::NODE_STATUS_DECOMMISSIONING);
                    }
                }
            }
        }

        {
            std::vector<NodeInfo> to_add;
            std::vector<NodeInfo>& to_del = decomission_nodes;
            msg = resource_mgr_->modify_nodes(instance_id, to_add, to_del);
        }
        {
            std::vector<NodeInfo>& to_add = decomission_nodes;
            std::vector<NodeInfo> to_del;
            for (auto& node : to_add) {
                node.node_info.set_status(NodeStatusPB::NODE_STATUS_DECOMMISSIONING);
                LOG(INFO) << "decomission node, "
                          << "size: " << to_add.size() << " " << node.node_info.DebugString() << " "
                          << node.cluster_id << " " << node.cluster_name;
            }
            msg = resource_mgr_->modify_nodes(instance_id, to_add, to_del);
        }
    } break;
    case AlterClusterRequest::NOTIFY_DECOMMISSIONED: {
        resource_mgr_->check_cluster_params_valid(request->cluster(), &msg, false);
        if (msg != "") {
            LOG(INFO) << msg;
            break;
        }

        std::string be_unique_id = (request->cluster().nodes())[0].cloud_unique_id();
        std::vector<NodeInfo> nodes;
        std::string err = resource_mgr_->get_node(be_unique_id, &nodes);
        if (!err.empty()) {
            LOG(INFO) << "failed to check instance info, err=" << err;
            msg = err;
            break;
        }

        std::vector<NodeInfo> decomission_nodes;
        for (auto& node : nodes) {
            for (auto req_node : request->cluster().nodes()) {
                bool ip_processed = false;
                if (node.node_info.has_ip() && req_node.has_ip()) {
                    std::string endpoint =
                        node.node_info.ip() + ":" + std::to_string(node.node_info.heartbeat_port());
                    std::string req_endpoint =
                        req_node.ip() + ":" + std::to_string(req_node.heartbeat_port());
                    if (endpoint == req_endpoint) {
                        decomission_nodes.push_back(node);
                    }
                    ip_processed = true;
                }

                if (!ip_processed && node.node_info.has_host() && req_node.has_host()) {
                    std::string endpoint =
                        node.node_info.host() + ":" + std::to_string(node.node_info.heartbeat_port());
                    std::string req_endpoint =
                        req_node.host() + ":" + std::to_string(req_node.heartbeat_port());
                    if (endpoint == req_endpoint) {
                        decomission_nodes.push_back(node);
                    }
                }
            }
        }

        {
            std::vector<NodeInfo> to_add;
            std::vector<NodeInfo>& to_del = decomission_nodes;
            msg = resource_mgr_->modify_nodes(instance_id, to_add, to_del);
        }
        {
            std::vector<NodeInfo>& to_add = decomission_nodes;
            std::vector<NodeInfo> to_del;
            for (auto& node : to_add) {
                node.node_info.set_status(NodeStatusPB::NODE_STATUS_DECOMMISSIONED);
                LOG(INFO) << "notify node decomissioned, "
                          << " size: " << to_add.size() << " " << node.node_info.DebugString()
                          << " " << node.cluster_id << " " << node.cluster_name;
            }
            msg = resource_mgr_->modify_nodes(instance_id, to_add, to_del);
        }
    } break;
    case AlterClusterRequest::RENAME_CLUSTER: {
        msg = resource_mgr_->update_cluster(
                instance_id, cluster,
                [&](const ::selectdb::ClusterPB& i) {
                    return i.cluster_id() == cluster.cluster.cluster_id();
                },
                [&](::selectdb::ClusterPB& c, std::set<std::string>& cluster_names) {
                    std::string msg = "";
                    auto it = cluster_names.find(cluster.cluster.cluster_name());
                    LOG(INFO) << "cluster.cluster.cluster_name(): "
                              << cluster.cluster.cluster_name();
                    for (auto itt : cluster_names) {
                        LOG(INFO) << "itt : " << itt;
                    }
                    if (it != cluster_names.end()) {
                        code = MetaServiceCode::INVALID_ARGUMENT;
                        ss << "failed to rename cluster, a cluster with the same name already "
                              "exists in this instance "
                           << proto_to_json(c);
                        msg = ss.str();
                        return msg;
                    }
                    if (c.cluster_name() == cluster.cluster.cluster_name()) {
                        code = MetaServiceCode::INVALID_ARGUMENT;
                        ss << "failed to rename cluster, name eq original name, original cluster "
                              "is "
                           << proto_to_json(c);
                        msg = ss.str();
                        return msg;
                    }
                    c.set_cluster_name(cluster.cluster.cluster_name());
                    return msg;
                });
    } break;
    case AlterClusterRequest::UPDATE_CLUSTER_ENDPOINT: {
        msg = resource_mgr_->update_cluster(
                instance_id, cluster,
                [&](const ::selectdb::ClusterPB& i) {
                    return i.cluster_id() == cluster.cluster.cluster_id();
                },
                [&](::selectdb::ClusterPB& c, std::set<std::string>& cluster_names) {
                    std::string msg = "";
                    if (!cluster.cluster.has_private_endpoint()
                            || cluster.cluster.private_endpoint().empty()) {
                        code = MetaServiceCode::CLUSTER_ENDPOINT_MISSING;
                        ss << "missing private endpoint";
                        msg = ss.str();
                        return msg;
                    }

                    c.set_public_endpoint(cluster.cluster.public_endpoint());
                    c.set_private_endpoint(cluster.cluster.private_endpoint());

                    return msg;
                });
    } break;
    case AlterClusterRequest::SET_CLUSTER_STATUS: {
        msg = resource_mgr_->update_cluster(
            instance_id, cluster,
            [&](const ::selectdb::ClusterPB& i) {
                return i.cluster_id() == cluster.cluster.cluster_id();
            },
            [&](::selectdb::ClusterPB& c, std::set<std::string>& cluster_names) {
                std::string msg = "";
                if (c.cluster_status() == request->cluster().cluster_status()) {
                    code = MetaServiceCode::INVALID_ARGUMENT;
                    ss << "failed to set cluster status, status eq original status, original cluster is "
                       << print_cluster_status(c.cluster_status());
                    msg = ss.str();
                    return msg;
                }
                // status from -> to
                std::set<std::pair<selectdb::ClusterStatus, selectdb::ClusterStatus>> can_work_directed_edges {
                    {ClusterStatus::UNKNOWN, ClusterStatus::NORMAL},
                    {ClusterStatus::NORMAL, ClusterStatus::SUSPENDED},
                    {ClusterStatus::SUSPENDED, ClusterStatus::TO_RESUME},
                    {ClusterStatus::TO_RESUME, ClusterStatus::NORMAL},
                    {ClusterStatus::SUSPENDED, ClusterStatus::NORMAL},
                };
                auto from = c.cluster_status();
                auto to = request->cluster().cluster_status();
                if (can_work_directed_edges.count({from, to}) == 0) {
                    // can't find a directed edge in set, so refuse it
                    code = MetaServiceCode::INVALID_ARGUMENT;
                    ss << "failed to set cluster status, original cluster is "
                       << print_cluster_status(from) << " and want set " << print_cluster_status(to);
                    msg = ss.str();
                    return msg;
                }
                c.set_cluster_status(request->cluster().cluster_status());
                return msg;
            });
    } break;
    default: {
        code = MetaServiceCode::INVALID_ARGUMENT;
        ss << "invalid request op, op=" << request->op();
        msg = ss.str();
        return;
    }
    }
    if (!msg.empty() && code == MetaServiceCode::OK) {
        code = MetaServiceCode::UNDEFINED_ERR;
    }

    if (code != MetaServiceCode::OK) return;

    auto f = new std::function<void()>([instance_id = request->instance_id(), txn_kv = txn_kv_] {
        notify_refresh_instance(txn_kv, instance_id);
    });
    bthread_t bid;
    if (bthread_start_background(&bid, nullptr, run_bthread_work, f) != 0) {
        LOG(WARNING) << "notify refresh instance inplace, instance_id=" << request->instance_id();
        run_bthread_work(f);
    }
} // alter cluster

void MetaServiceImpl::get_cluster(google::protobuf::RpcController* controller,
                                  const ::selectdb::GetClusterRequest* request,
                                  ::selectdb::GetClusterResponse* response,
                                  ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(get_cluster);
    std::string cloud_unique_id = request->has_cloud_unique_id() ? request->cloud_unique_id() : "";
    std::string cluster_id = request->has_cluster_id() ? request->cluster_id() : "";
    std::string cluster_name = request->has_cluster_name() ? request->cluster_name() : "";
    std::string mysql_user_name = request->has_mysql_user_name() ? request->mysql_user_name() : "";

    if (cloud_unique_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "cloud_unique_id must be given";
        return;
    }

    instance_id = get_instance_id(resource_mgr_, cloud_unique_id);
    if (instance_id.empty()) {
        if (request->has_instance_id()) {
            instance_id = request->instance_id();
            // FIXME(gavin): this mechanism benifits debugging and
            //               administration, is it dangerous?
            LOG(WARNING) << "failed to get instance_id with cloud_unique_id=" << cloud_unique_id
                         << " use the given instance_id=" << instance_id << " instead";
        } else {
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = "empty instance_id";
            LOG(INFO) << msg << ", cloud_unique_id=" << cloud_unique_id;
            return;
        }
    }
    RPC_RATE_LIMIT(get_cluster)
    // ATTN: if the case that multiple conditions are satisfied, just use by this order:
    // cluster_id -> cluster_name -> mysql_user_name
    if (!cluster_id.empty()) {
        cluster_name = "";
        mysql_user_name = "";
    } else if (!cluster_name.empty()) {
        mysql_user_name = "";
    }

    bool get_all_cluster_info = false;
    // if cluster_id、cluster_name、mysql_user_name all empty, get this instance's all cluster info.
    if (cluster_id.empty() && cluster_name.empty() && mysql_user_name.empty()) {
        get_all_cluster_info = true;
    }

    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "failed to get instance_id with cloud_unique_id=" + cloud_unique_id;
        return;
    }

    InstanceKeyInfo key_info {instance_id};
    std::string key;
    std::string val;
    instance_key(key_info, &key);

    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        msg = "failed to create txn";
        LOG(WARNING) << msg << " ret=" << ret;
        return;
    }
    ret = txn->get(key, &val);
    LOG(INFO) << "get instance_key=" << hex(key);

    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_GET_ERR;
        ss << "failed to get instance, instance_id=" << instance_id << " ret=" << ret;
        msg = ss.str();
        return;
    }

    InstanceInfoPB instance;
    if (!instance.ParseFromString(val)) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        msg = "failed to parse InstanceInfoPB";
        return;
    }

    auto get_cluster_mysql_user = [](const ::selectdb::ClusterPB& c,
                                     std::set<std::string>* mysql_users) {
        for (int i = 0; i < c.mysql_user_name_size(); i++) {
            mysql_users->emplace(c.mysql_user_name(i));
        }
    };

    if (get_all_cluster_info) {
        response->mutable_cluster()->CopyFrom(instance.clusters());
        LOG_EVERY_N(INFO, 100) << "get all cluster info, " << msg;
    } else {
        for (int i = 0; i < instance.clusters_size(); ++i) {
            auto& c = instance.clusters(i);
            std::set<std::string> mysql_users;
            get_cluster_mysql_user(c, &mysql_users);
            // The last wins if add_cluster() does not ensure uniqueness of
            // cluster_id and cluster_name respectively
            if ((c.has_cluster_name() && c.cluster_name() == cluster_name) ||
                (c.has_cluster_id() && c.cluster_id() == cluster_id) ||
                mysql_users.count(mysql_user_name)) {
                // just one cluster
                response->add_cluster()->CopyFrom(c);
                LOG_EVERY_N(INFO, 100) << "found a cluster, instance_id=" << instance.instance_id()
                                       << " cluster=" << msg;
            }
        }
    }

    if (response->cluster().size() == 0) {
        ss << "fail to get cluster with " << request->ShortDebugString();
        msg = ss.str();
        std::replace(msg.begin(), msg.end(), '\n', ' ');
        code = MetaServiceCode::CLUSTER_NOT_FOUND;
    }
} // get_cluster

void MetaServiceImpl::create_stage(::google::protobuf::RpcController* controller,
                                   const ::selectdb::CreateStageRequest* request,
                                   ::selectdb::CreateStageResponse* response,
                                   ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(create_stage);
    std::string cloud_unique_id = request->has_cloud_unique_id() ? request->cloud_unique_id() : "";
    if (cloud_unique_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "cloud unique id not set";
        return;
    }

    instance_id = get_instance_id(resource_mgr_, cloud_unique_id);
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << cloud_unique_id;
        return;
    }
    RPC_RATE_LIMIT(create_stage)

    if (!request->has_stage()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "stage not set";
        return;
    }
    auto stage = request->stage();

    if (!stage.has_type()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "stage type not set";
        return;
    }

    if (stage.name().empty() && stage.type() == StagePB::EXTERNAL) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "stage name not set";
        return;
    }
    if (stage.stage_id().empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "stage id not set";
        return;
    }

    if (stage.type() == StagePB::INTERNAL) {
        if (stage.mysql_user_name().empty() || stage.mysql_user_id().empty()) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            ss << "internal stage must have a mysql user name and id must be given, name size="
               << stage.mysql_user_name_size() << " id size=" << stage.mysql_user_id_size();
            msg = ss.str();
            LOG(WARNING) << msg;
            return;
        }
    }

    InstanceKeyInfo key_info {instance_id};
    std::string key;
    std::string val;
    instance_key(key_info, &key);

    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        msg = "failed to create txn";
        LOG(WARNING) << msg << " ret=" << ret;
        return;
    }
    ret = txn->get(key, &val);
    LOG(INFO) << "get instance_key=" << hex(key);

    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_GET_ERR;
        ss << "failed to get instance, instance_id=" << instance_id << " ret=" << ret;
        msg = ss.str();
        return;
    }

    InstanceInfoPB instance;
    if (!instance.ParseFromString(val)) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        msg = "failed to parse InstanceInfoPB";
        return;
    }

    VLOG_DEBUG << "config stages num=" << config::max_num_stages;
    if (instance.stages_size() >= config::max_num_stages) {
        code = MetaServiceCode::UNDEFINED_ERR;
        msg = "this instance has greater than config num stages";
        LOG(WARNING) << "can't create more than config num stages, and instance has "
                     << std::to_string(instance.stages_size());
        return;
    }

    // check if the stage exists
    for (int i = 0; i < instance.stages_size(); ++i) {
        auto& s = instance.stages(i);
        if (stage.type() == StagePB::INTERNAL) {
            // check all internal stage format is right
            if (s.type() == StagePB::INTERNAL && s.mysql_user_id_size() == 0) {
                LOG(WARNING) << "impossible, internal stage must have at least one id instance="
                             << proto_to_json(instance);
            }

            if (s.type() == StagePB::INTERNAL &&
                (s.mysql_user_id(0) == stage.mysql_user_id(0) ||
                 s.mysql_user_name(0) == stage.mysql_user_name(0))) {
                code = MetaServiceCode::ALREADY_EXISTED;
                msg = "stage already exist";
                ss << "stage already exist, req user_name=" << stage.mysql_user_name(0)
                   << " existed user_name=" << s.mysql_user_name(0)
                   << "req user_id=" << stage.mysql_user_id(0)
                   << " existed user_id=" << s.mysql_user_id(0);
                return;
            }
        }

        if (stage.type() == StagePB::EXTERNAL) {
            if (s.name() == stage.name()) {
                code = MetaServiceCode::ALREADY_EXISTED;
                msg = "stage already exist";
                return;
            }
        }

        if (s.stage_id() == stage.stage_id()) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = "stage id is duplicated";
            return;
        }
    }

    if (stage.type() == StagePB::INTERNAL) {
        if (instance.obj_info_size() == 0) {
            LOG(WARNING) << "impossible, instance must have at least one obj_info.";
            code = MetaServiceCode::UNDEFINED_ERR;
            msg = "impossible, instance must have at least one obj_info.";
            return;
        }
        auto& lastest_obj = instance.obj_info()[instance.obj_info_size() - 1];
        // ${obj_prefix}/stage/{username}/{user_id}
        std::string mysql_user_name = stage.mysql_user_name(0);
        std::string prefix = fmt::format("{}/stage/{}/{}", lastest_obj.prefix(), mysql_user_name,
                                         stage.mysql_user_id(0));
        auto as = instance.add_stages();
        as->mutable_obj_info()->set_prefix(prefix);
        as->mutable_obj_info()->set_id(lastest_obj.id());
        as->add_mysql_user_name(mysql_user_name);
        as->add_mysql_user_id(stage.mysql_user_id(0));
        as->set_stage_id(stage.stage_id());
    } else if (stage.type() == StagePB::EXTERNAL) {
        if (!stage.has_obj_info()) {
            instance.add_stages()->CopyFrom(stage);
        } else {
            StagePB tmp_stage;
            tmp_stage.CopyFrom(stage);
            auto obj_info = tmp_stage.mutable_obj_info();
            EncryptionInfoPB encryption_info;
            AkSkPair cipher_ak_sk_pair;
            ret = encrypt_ak_sk_helper(obj_info->ak(), obj_info->sk(), &encryption_info,
                                       &cipher_ak_sk_pair, code, msg);
            if (ret != 0) {
                return;
            }
            obj_info->set_ak(std::move(cipher_ak_sk_pair.first));
            obj_info->set_sk(std::move(cipher_ak_sk_pair.second));
            obj_info->mutable_encryption_info()->CopyFrom(encryption_info);
            instance.add_stages()->CopyFrom(tmp_stage);
        }
    }
    val = instance.SerializeAsString();
    if (val.empty()) {
        msg = "failed to serialize";
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        return;
    }

    txn->put(key, val);
    LOG(INFO) << "put instance_id=" << instance_id << " instance_key=" << hex(key)
              << " json=" << proto_to_json(instance);
    ret = txn->commit();
    if (ret != 0) {
        code = ret == -1 ? MetaServiceCode::KV_TXN_CONFLICT : MetaServiceCode::KV_TXN_COMMIT_ERR;
        msg = fmt::format("failed to commit kv txn, ret={}", ret);
        LOG(WARNING) << msg;
    }
}

void MetaServiceImpl::get_stage(google::protobuf::RpcController* controller,
                                const ::selectdb::GetStageRequest* request,
                                ::selectdb::GetStageResponse* response,
                                ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(get_stage);
    std::string cloud_unique_id = request->has_cloud_unique_id() ? request->cloud_unique_id() : "";
    if (cloud_unique_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "cloud unique id not set";
        return;
    }

    instance_id = get_instance_id(resource_mgr_, cloud_unique_id);
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << cloud_unique_id;
        return;
    }
    RPC_RATE_LIMIT(get_stage)
    if (!request->has_type()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "stage type not set";
        return;
    }
    auto type = request->type();

    InstanceKeyInfo key_info {instance_id};
    std::string key;
    std::string val;
    instance_key(key_info, &key);

    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        msg = "failed to create txn";
        LOG(WARNING) << msg << " ret=" << ret;
        return;
    }
    ret = txn->get(key, &val);
    LOG(INFO) << "get instance_key=" << hex(key);

    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_GET_ERR;
        ss << "failed to get instance, instance_id=" << instance_id << " ret=" << ret;
        msg = ss.str();
        return;
    }

    InstanceInfoPB instance;
    if (!instance.ParseFromString(val)) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        msg = "failed to parse InstanceInfoPB";
        return;
    }

    if (type == StagePB::INTERNAL) {
        auto mysql_user_name = request->has_mysql_user_name() ? request->mysql_user_name() : "";
        if (mysql_user_name.empty()) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = "mysql user name not set";
            return;
        }
        auto mysql_user_id = request->has_mysql_user_id() ? request->mysql_user_id() : "";
        if (mysql_user_id.empty()) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = "mysql user id not set";
            return;
        }

        // check mysql user_name has been created internal stage
        auto& stage = instance.stages();
        bool found = false;
        if (instance.obj_info_size() == 0) {
            LOG(WARNING) << "impossible, instance must have at least one obj_info.";
            code = MetaServiceCode::UNDEFINED_ERR;
            msg = "impossible, instance must have at least one obj_info.";
            return;
        }

        for (auto s : stage) {
            if (s.type() != StagePB::INTERNAL) {
                continue;
            }
            if (s.mysql_user_name().size() == 0 || s.mysql_user_id().size() == 0) {
                LOG(WARNING) << "impossible here, internal stage must have at least one user, "
                                "invalid stage="
                             << proto_to_json(s);
                continue;
            }
            if (s.mysql_user_name(0) == mysql_user_name) {
                StagePB stage_pb;
                // internal stage id is user_id, if user_id not eq internal stage's user_id, del it.
                // let fe create a new internal stage
                if (s.mysql_user_id(0) != mysql_user_id) {
                    LOG(INFO) << "ABA user=" << mysql_user_name
                              << " internal stage original user_id=" << s.mysql_user_id()[0]
                              << " rpc user_id=" << mysql_user_id
                              << " stage info=" << proto_to_json(s);
                    code = MetaServiceCode::STATE_ALREADY_EXISTED_FOR_USER;
                    msg = "aba user, drop stage and create a new one";
                    // response return to be dropped stage id.
                    stage_pb.CopyFrom(s);
                    response->add_stage()->CopyFrom(stage_pb);
                    return;
                }
                // find, use it stage prefix and id
                found = true;
                // get from internal stage
                int idx = stoi(s.obj_info().id());
                if (idx > instance.obj_info().size() || idx < 1) {
                    LOG(WARNING) << "invalid idx: " << idx;
                    code = MetaServiceCode::UNDEFINED_ERR;
                    msg = "impossible, id invalid";
                    return;
                }
                auto& old_obj = instance.obj_info()[idx - 1];

                stage_pb.mutable_obj_info()->set_ak(old_obj.ak());
                stage_pb.mutable_obj_info()->set_sk(old_obj.sk());
                if (old_obj.has_encryption_info()) {
                    AkSkPair plain_ak_sk_pair;
                    int ret = decrypt_ak_sk_helper(old_obj.ak(), old_obj.sk(),
                                                   old_obj.encryption_info(), &plain_ak_sk_pair,
                                                   code, msg);
                    if (ret != 0) return;
                    stage_pb.mutable_obj_info()->set_ak(std::move(plain_ak_sk_pair.first));
                    stage_pb.mutable_obj_info()->set_sk(std::move(plain_ak_sk_pair.second));
                }
                stage_pb.mutable_obj_info()->set_bucket(old_obj.bucket());
                stage_pb.mutable_obj_info()->set_endpoint(old_obj.endpoint());
                stage_pb.mutable_obj_info()->set_external_endpoint(old_obj.external_endpoint());
                stage_pb.mutable_obj_info()->set_region(old_obj.region());
                stage_pb.mutable_obj_info()->set_provider(old_obj.provider());
                stage_pb.mutable_obj_info()->set_prefix(s.obj_info().prefix());
                stage_pb.set_stage_id(s.stage_id());
                stage_pb.set_type(s.type());
                response->add_stage()->CopyFrom(stage_pb);
                return;
            }
        }
        if (!found) {
            LOG(INFO) << "user=" << mysql_user_name
                      << " not have a valid stage, rpc user_id=" << mysql_user_id;
            code = MetaServiceCode::STAGE_NOT_FOUND;
            msg = "stage not found, create a new one";
            return;
        }
    }

    // get all external stages for display, but don't show ak/sk, so there is no need to decrypt ak/sk.
    if (type == StagePB::EXTERNAL && !request->has_stage_name()) {
        for (int i = 0; i < instance.stages_size(); ++i) {
            auto& s = instance.stages(i);
            if (s.type() != StagePB::EXTERNAL) {
                continue;
            }
            response->add_stage()->CopyFrom(s);
        }
        return;
    }

    // get external stage with the specified stage name
    for (int i = 0; i < instance.stages_size(); ++i) {
        auto& s = instance.stages(i);
        if (s.type() == type && s.name() == request->stage_name()) {
            StagePB stage;
            stage.CopyFrom(s);
            if (!stage.has_access_type() || stage.access_type() == StagePB::AKSK) {
                stage.set_access_type(StagePB::AKSK);
                auto obj_info = stage.mutable_obj_info();
                if (obj_info->has_encryption_info()) {
                    AkSkPair plain_ak_sk_pair;
                    int ret = decrypt_ak_sk_helper(obj_info->ak(), obj_info->sk(),
                                                   obj_info->encryption_info(), &plain_ak_sk_pair,
                                                   code, msg);
                    if (ret != 0) return;
                    obj_info->set_ak(std::move(plain_ak_sk_pair.first));
                    obj_info->set_sk(std::move(plain_ak_sk_pair.second));
                }
            } else if (stage.access_type() == StagePB::BUCKET_ACL) {
                if (!instance.has_ram_user()) {
                    ss << "instance does not have ram user";
                    msg = ss.str();
                    code = MetaServiceCode::INVALID_ARGUMENT;
                    return;
                }
                if (instance.ram_user().has_encryption_info()) {
                    AkSkPair plain_ak_sk_pair;
                    int ret = decrypt_ak_sk_helper(
                            instance.ram_user().ak(), instance.ram_user().sk(),
                            instance.ram_user().encryption_info(), &plain_ak_sk_pair, code, msg);
                    if (ret != 0) return;
                    stage.mutable_obj_info()->set_ak(std::move(plain_ak_sk_pair.first));
                    stage.mutable_obj_info()->set_sk(std::move(plain_ak_sk_pair.second));
                } else {
                    stage.mutable_obj_info()->set_ak(instance.ram_user().ak());
                    stage.mutable_obj_info()->set_sk(instance.ram_user().sk());
                }
            } else if (stage.access_type() == StagePB::IAM) {
                std::string val;
                ret = txn->get(system_meta_service_arn_info_key(), &val);
                if (ret == 1) {
                    // For compatibility, use arn_info of config
                    stage.mutable_obj_info()->set_ak(config::arn_ak);
                    stage.mutable_obj_info()->set_sk(config::arn_sk);
                    stage.set_external_id(instance_id);
                } else if (ret == 0) {
                    RamUserPB iam_user;
                    if (!iam_user.ParseFromString(val)) {
                        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                        msg = "failed to parse RamUserPB";
                        return;
                    }
                    AkSkPair plain_ak_sk_pair;
                    int ret = decrypt_ak_sk_helper(iam_user.ak(), iam_user.sk(),
                                                   iam_user.encryption_info(), &plain_ak_sk_pair,
                                                   code, msg);
                    if (ret != 0) return;
                    stage.mutable_obj_info()->set_ak(std::move(plain_ak_sk_pair.first));
                    stage.mutable_obj_info()->set_sk(std::move(plain_ak_sk_pair.second));
                    stage.set_external_id(instance_id);
                } else {
                    code = MetaServiceCode::KV_TXN_GET_ERR;
                    ss << "failed to get arn_info_key, ret=" << ret;
                    msg = ss.str();
                    return;
                }
            }
            response->add_stage()->CopyFrom(stage);
            return;
        }
    }

    ss << "stage not found with " << proto_to_json(*request);
    msg = ss.str();
    code = MetaServiceCode::STAGE_NOT_FOUND;
}

void MetaServiceImpl::drop_stage(google::protobuf::RpcController* controller,
                                 const ::selectdb::DropStageRequest* request,
                                 ::selectdb::DropStageResponse* response,
                                 ::google::protobuf::Closure* done) {
    StopWatch sw;
    auto ctrl = static_cast<brpc::Controller*>(controller);
    LOG(INFO) << "rpc from " << ctrl->remote_side() << " request=" << request->DebugString();
    brpc::ClosureGuard closure_guard(done);
    int ret = 0;
    MetaServiceCode code = MetaServiceCode::OK;
    std::string msg = "OK";
    std::string instance_id;
    bool drop_request = false;
    std::unique_ptr<int, std::function<void(int*)>> defer_status(
            (int*)0x01, [&ret, &code, &msg, &response, &ctrl, &closure_guard, &sw, &instance_id,
                         &drop_request](int*) {
                response->mutable_status()->set_code(code);
                response->mutable_status()->set_msg(msg);
                LOG(INFO) << (ret == 0 ? "succ to " : "failed to ") << __PRETTY_FUNCTION__ << " "
                          << ctrl->remote_side() << " " << msg;
                closure_guard.reset(nullptr);
                if (config::use_detailed_metrics && !instance_id.empty() && !drop_request) {
                    g_bvar_ms_drop_stage.put(instance_id, sw.elapsed_us());
                }
            });

    std::string cloud_unique_id = request->has_cloud_unique_id() ? request->cloud_unique_id() : "";
    if (cloud_unique_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "cloud unique id not set";
        return;
    }

    instance_id = get_instance_id(resource_mgr_, cloud_unique_id);
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << cloud_unique_id;
        return;
    }
    RPC_RATE_LIMIT(drop_stage)

    if (!request->has_type()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "stage type not set";
        return;
    }
    auto type = request->type();

    if (type == StagePB::EXTERNAL && request->stage_name().empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "external stage but not set stage name";
        return;
    }

    if (type == StagePB::INTERNAL && request->mysql_user_id().empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "internal stage but not set user id";
        return;
    }

    InstanceKeyInfo key_info {instance_id};
    std::string key;
    std::string val;
    instance_key(key_info, &key);

    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        msg = "failed to create txn";
        LOG(WARNING) << msg << " ret=" << ret;
        return;
    }

    ret = txn->get(key, &val);
    LOG(INFO) << "get instance_key=" << hex(key);
    std::stringstream ss;
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_GET_ERR;
        ss << "failed to get instance, instance_id=" << instance_id << " ret=" << ret;
        msg = ss.str();
        return;
    }

    InstanceInfoPB instance;
    if (!instance.ParseFromString(val)) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        msg = "failed to parse InstanceInfoPB";
        return;
    }

    StagePB stage;
    int idx = -1;
    for (int i = 0; i < instance.stages_size(); ++i) {
        auto& s = instance.stages(i);
        if ((type == StagePB::INTERNAL && s.type() == StagePB::INTERNAL &&
             s.mysql_user_id(0) == request->mysql_user_id()) ||
            (type == StagePB::EXTERNAL && s.type() == StagePB::EXTERNAL &&
             s.name() == request->stage_name())) {
            idx = i;
            stage = s;
            break;
        }
    }
    if (idx == -1) {
        ss << "stage not found with " << proto_to_json(*request);
        msg = ss.str();
        code = MetaServiceCode::STAGE_NOT_FOUND;
        return;
    }

    auto& stages = const_cast<std::decay_t<decltype(instance.stages())>&>(instance.stages());
    stages.DeleteSubrange(idx, 1); // Remove it
    val = instance.SerializeAsString();
    if (val.empty()) {
        msg = "failed to serialize";
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        return;
    }
    txn->put(key, val);
    LOG(INFO) << "put instance_id=" << instance_id << " instance_key=" << hex(key)
              << " json=" << proto_to_json(instance);

    std::string key1;
    std::string val1;
    if (type == StagePB::INTERNAL) {
        RecycleStageKeyInfo recycle_stage_key_info {instance_id, stage.stage_id()};
        recycle_stage_key(recycle_stage_key_info, &key1);
        RecycleStagePB recycle_stage;
        recycle_stage.set_instance_id(instance_id);
        recycle_stage.set_reason(request->reason());
        recycle_stage.mutable_stage()->CopyFrom(stage);
        val1 = recycle_stage.SerializeAsString();
        if (val1.empty()) {
            msg = "failed to serialize";
            code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
            return;
        }
        txn->put(key1, val1);
    }

    ret = txn->commit();
    if (ret != 0) {
        code = ret == -1 ? MetaServiceCode::KV_TXN_CONFLICT : MetaServiceCode::KV_TXN_COMMIT_ERR;
        msg = fmt::format("failed to commit kv txn, ret={}", ret);
        LOG(WARNING) << msg;
    }
}

void MetaServiceImpl::get_iam(google::protobuf::RpcController* controller,
                              const ::selectdb::GetIamRequest* request,
                              ::selectdb::GetIamResponse* response,
                              ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(get_iam);
    std::string cloud_unique_id = request->has_cloud_unique_id() ? request->cloud_unique_id() : "";
    if (cloud_unique_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "cloud unique id not set";
        return;
    }

    instance_id = get_instance_id(resource_mgr_, cloud_unique_id);
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << cloud_unique_id;
        return;
    }
    RPC_RATE_LIMIT(get_iam)

    InstanceKeyInfo key_info {instance_id};
    std::string key;
    std::string val;
    instance_key(key_info, &key);

    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        msg = "failed to create txn";
        LOG(WARNING) << msg << " ret=" << ret;
        return;
    }
    ret = txn->get(key, &val);
    LOG(INFO) << "get instance_key=" << hex(key);

    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_GET_ERR;
        ss << "failed to get instance, instance_id=" << instance_id << " ret=" << ret;
        msg = ss.str();
        return;
    }

    InstanceInfoPB instance;
    if (!instance.ParseFromString(val)) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        msg = "failed to parse InstanceInfoPB";
        return;
    }

    val.clear();
    ret = txn->get(system_meta_service_arn_info_key(), &val);
    if (ret == 1) {
        // For compatibility, use arn_info of config
        RamUserPB iam_user;
        iam_user.set_user_id(config::arn_id);
        iam_user.set_external_id(instance_id);
        iam_user.set_ak(config::arn_ak);
        iam_user.set_sk(config::arn_sk);
        response->mutable_iam_user()->CopyFrom(iam_user);
    } else if (ret == 0) {
        RamUserPB iam_user;
        if (!iam_user.ParseFromString(val)) {
            code = MetaServiceCode::PROTOBUF_PARSE_ERR;
            msg = "failed to parse RamUserPB";
            return;
        }
        AkSkPair plain_ak_sk_pair;
        int ret = decrypt_ak_sk_helper(iam_user.ak(), iam_user.sk(), iam_user.encryption_info(),
                                       &plain_ak_sk_pair, code, msg);
        if (ret != 0) return;
        iam_user.set_external_id(instance_id);
        iam_user.set_ak(std::move(plain_ak_sk_pair.first));
        iam_user.set_sk(std::move(plain_ak_sk_pair.second));
        response->mutable_iam_user()->CopyFrom(iam_user);
    } else {
        code = MetaServiceCode::KV_TXN_GET_ERR;
        ss << "failed to get arn_info_key, ret=" << ret;
        msg = ss.str();
        return;
    }

    if (instance.has_ram_user()) {
        RamUserPB ram_user;
        ram_user.CopyFrom(instance.ram_user());
        if (ram_user.has_encryption_info()) {
            AkSkPair plain_ak_sk_pair;
            int ret = decrypt_ak_sk_helper(ram_user.ak(), ram_user.sk(), ram_user.encryption_info(),
                                           &plain_ak_sk_pair, code, msg);
            if (ret != 0) return;
            ram_user.set_ak(std::move(plain_ak_sk_pair.first));
            ram_user.set_sk(std::move(plain_ak_sk_pair.second));
        }
        response->mutable_ram_user()->CopyFrom(ram_user);
    }
}

void MetaServiceImpl::alter_iam(google::protobuf::RpcController* controller,
                                const ::selectdb::AlterIamRequest* request,
                                ::selectdb::AlterIamResponse* response,
                                ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(alter_iam);
    std::string arn_id = request->has_account_id() ? request->account_id() : "";
    std::string arn_ak = request->has_ak() ? request->ak() : "";
    std::string arn_sk = request->has_sk() ? request->sk() : "";
    if (arn_id.empty() || arn_ak.empty() || arn_sk.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "invalid argument";
        return;
    }

    RPC_RATE_LIMIT(alter_iam)

    std::string key = system_meta_service_arn_info_key();
    std::string val;
    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        msg = "failed to create txn";
        LOG(WARNING) << msg << " ret=" << ret;
        return;
    }
    ret = txn->get(key, &val);
    if (ret != 0 && ret != 1) {
        code = MetaServiceCode::KV_TXN_GET_ERR;
        ss << "fail to arn_info_key, ret=" << ret;
        msg = ss.str();
        return;
    }

    bool is_add_req = ret == 1;
    EncryptionInfoPB encryption_info;
    AkSkPair cipher_ak_sk_pair;
    ret = encrypt_ak_sk_helper(arn_ak, arn_sk, &encryption_info, &cipher_ak_sk_pair, code, msg);
    if (ret != 0) {
        return;
    }
    const auto& [ak, sk] = cipher_ak_sk_pair;
    RamUserPB iam_user;
    std::string old_ak;
    std::string old_sk;
    if (!is_add_req) {
        if (!iam_user.ParseFromString(val)) {
            code = MetaServiceCode::PROTOBUF_PARSE_ERR;
            ss << "failed to parse RamUserPB";
            msg = ss.str();
            return;
        }

        if (arn_id == iam_user.user_id() && ak == iam_user.ak() && sk == iam_user.sk()) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            ss << "already has the same arn info";
            msg = ss.str();
            return;
        }
        old_ak = iam_user.ak();
        old_sk = iam_user.sk();
    }
    iam_user.set_user_id(arn_id);
    iam_user.set_ak(std::move(cipher_ak_sk_pair.first));
    iam_user.set_sk(std::move(cipher_ak_sk_pair.second));
    iam_user.mutable_encryption_info()->CopyFrom(encryption_info);
    val = iam_user.SerializeAsString();
    if (val.empty()) {
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        ss << "failed to serialize";
        msg = ss.str();
        return;
    }
    txn->put(key, val);
    ret = txn->commit();
    if (ret != 0) {
        code = ret == -1 ? MetaServiceCode::KV_TXN_CONFLICT : MetaServiceCode::KV_TXN_COMMIT_ERR;
        ss << "txn->commit failed() ret=" << ret;
        msg = ss.str();
        return;
    }
    if (is_add_req) {
        LOG(INFO) << "add new iam info, cipher ak: " << ak << " cipher sk: " << sk;
    } else {
        LOG(INFO) << "alter iam info, old:  cipher ak: " << old_ak << " cipher sk" << old_sk
                  << " new: cipher ak: " << ak << " cipher sk:" << sk;
    }
}

void MetaServiceImpl::alter_ram_user(google::protobuf::RpcController* controller,
                                     const ::selectdb::AlterRamUserRequest* request,
                                     ::selectdb::AlterRamUserResponse* response,
                                     ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(alter_ram_user);
    instance_id = request->has_instance_id() ? request->instance_id() : "";
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        return;
    }
    if (!request->has_ram_user() || request->ram_user().user_id().empty() ||
        request->ram_user().ak().empty() || request->ram_user().sk().empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "ram user info err " + proto_to_json(*request);
        return;
    }
    auto& ram_user = request->ram_user();
    RPC_RATE_LIMIT(alter_ram_user)
    InstanceKeyInfo key_info {instance_id};
    std::string key;
    std::string val;
    instance_key(key_info, &key);

    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        msg = "failed to create txn";
        LOG(WARNING) << msg << " ret=" << ret;
        return;
    }
    ret = txn->get(key, &val);
    LOG(INFO) << "get instance_key=" << hex(key);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_GET_ERR;
        ss << "failed to get instance, instance_id=" << instance_id << " ret=" << ret;
        msg = ss.str();
        return;
    }
    InstanceInfoPB instance;
    if (!instance.ParseFromString(val)) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        msg = "failed to parse InstanceInfoPB";
        return;
    }
    if (instance.status() != InstanceInfoPB::NORMAL) {
        code = MetaServiceCode::CLUSTER_NOT_FOUND;
        msg = "instance status has been set delete, plz check it";
        return;
    }
    if (instance.has_ram_user()) {
        LOG(WARNING) << "instance has ram user. instance_id=" << instance_id
                     << ", ram_user_id=" << ram_user.user_id();
    }
    EncryptionInfoPB encryption_info;
    AkSkPair cipher_ak_sk_pair;
    ret = encrypt_ak_sk_helper(ram_user.ak(), ram_user.sk(), &encryption_info, &cipher_ak_sk_pair,
                               code, msg);
    if (ret != 0) {
        return;
    }
    RamUserPB new_ram_user;
    new_ram_user.CopyFrom(ram_user);
    new_ram_user.set_user_id(ram_user.user_id());
    new_ram_user.set_ak(std::move(cipher_ak_sk_pair.first));
    new_ram_user.set_sk(std::move(cipher_ak_sk_pair.second));
    new_ram_user.mutable_encryption_info()->CopyFrom(encryption_info);
    instance.mutable_ram_user()->CopyFrom(new_ram_user);
    val = instance.SerializeAsString();
    if (val.empty()) {
        msg = "failed to serialize";
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        return;
    }
    txn->put(key, val);
    LOG(INFO) << "put instance_id=" << instance_id << " instance_key=" << hex(key);
    ret = txn->commit();
    if (ret != 0) {
        code = ret == -1 ? MetaServiceCode::KV_TXN_CONFLICT : MetaServiceCode::KV_TXN_COMMIT_ERR;
        msg = fmt::format("failed to commit kv txn, ret={}", ret);
        LOG(WARNING) << msg;
    }
}

void MetaServiceImpl::begin_copy(google::protobuf::RpcController* controller,
                                 const ::selectdb::BeginCopyRequest* request,
                                 ::selectdb::BeginCopyResponse* response,
                                 ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(begin_copy);
    std::string cloud_unique_id = request->has_cloud_unique_id() ? request->cloud_unique_id() : "";
    if (cloud_unique_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "cloud unique id not set";
        return;
    }

    instance_id = get_instance_id(resource_mgr_, cloud_unique_id);
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << cloud_unique_id;
        return;
    }
    RPC_RATE_LIMIT(begin_copy)
    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        msg = "failed to create txn";
        LOG(WARNING) << msg << " ret=" << ret;
        return;
    }

    // copy job key
    CopyJobKeyInfo key_info {instance_id, request->stage_id(), request->table_id(),
                             request->copy_id(), request->group_id()};
    std::string key;
    std::string val;
    copy_job_key(key_info, &key);
    // copy job value
    CopyJobPB copy_job;
    copy_job.set_stage_type(request->stage_type());
    copy_job.set_job_status(CopyJobPB::LOADING);
    copy_job.set_start_time_ms(request->start_time_ms());
    copy_job.set_timeout_time_ms(request->timeout_time_ms());

    std::vector<std::pair<std::string, std::string>> copy_files;
    auto& object_files = request->object_files();
    int file_num = 0;
    size_t file_size = 0;
    size_t file_meta_size = 0;
    for (auto i = 0; i < object_files.size(); ++i) {
        auto& file = object_files.at(i);
        // 1. get copy file kv to check if file is loading or loaded
        CopyFileKeyInfo file_key_info {instance_id, request->stage_id(), request->table_id(),
                                       file.relative_path(), file.etag()};
        std::string file_key;
        copy_file_key(file_key_info, &file_key);
        std::string file_val;
        ret = txn->get(file_key, &file_val);
        if (ret == 0) { // found key
            continue;
        } else if (ret < 0) { // error
            code = MetaServiceCode::KV_TXN_GET_ERR;
            msg = "failed to get copy file";
            LOG(WARNING) << msg << " ret=" << ret;
            return;
        }
        // 2. check if reach any limit
        ++file_num;
        file_size += file.size();
        file_meta_size += file.ByteSizeLong();
        if (file_num > 1 &&
            ((request->file_num_limit() > 0 && file_num > request->file_num_limit()) ||
             (request->file_size_limit() > 0 && file_size > request->file_size_limit()) ||
             (request->file_meta_size_limit() > 0 &&
              file_meta_size > request->file_meta_size_limit()))) {
            break;
        }
        // 3. put copy file kv
        CopyFilePB copy_file;
        copy_file.set_copy_id(request->copy_id());
        copy_file.set_group_id(request->group_id());
        std::string copy_file_val = copy_file.SerializeAsString();
        if (copy_file_val.empty()) {
            msg = "failed to serialize";
            code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
            return;
        }
        copy_files.emplace_back(std::move(file_key), std::move(copy_file_val));
        // 3. add file to copy job value
        copy_job.add_object_files()->CopyFrom(file);
        response->add_filtered_object_files()->CopyFrom(file);
    }

    if (file_num == 0) {
        return;
    }

    val = copy_job.SerializeAsString();
    if (val.empty()) {
        msg = "failed to serialize";
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        return;
    }
    // put copy job
    txn->put(key, val);
    LOG(INFO) << "put copy_job_key=" << hex(key);
    // put copy file
    for (const auto& [k, v] : copy_files) {
        txn->put(k, v);
        LOG(INFO) << "put copy_file_key=" << hex(k);
    }

    ret = txn->commit();
    if (ret != 0) {
        code = ret == -1 ? MetaServiceCode::KV_TXN_CONFLICT : MetaServiceCode::KV_TXN_COMMIT_ERR;
        msg = fmt::format("failed to commit kv txn, ret={}", ret);
        LOG(WARNING) << msg;
    }
}

void MetaServiceImpl::finish_copy(google::protobuf::RpcController* controller,
                                  const ::selectdb::FinishCopyRequest* request,
                                  ::selectdb::FinishCopyResponse* response,
                                  ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(finish_copy);
    std::string cloud_unique_id = request->has_cloud_unique_id() ? request->cloud_unique_id() : "";
    if (cloud_unique_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "cloud unique id not set";
        return;
    }

    instance_id = get_instance_id(resource_mgr_, cloud_unique_id);
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << cloud_unique_id;
        return;
    }
    RPC_RATE_LIMIT(finish_copy)

    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        msg = "failed to create txn";
        LOG(WARNING) << msg << " ret=" << ret;
        return;
    }

    // copy job key
    CopyJobKeyInfo key_info {instance_id, request->stage_id(), request->table_id(),
                             request->copy_id(), request->group_id()};
    std::string key;
    std::string val;
    copy_job_key(key_info, &key);
    ret = txn->get(key, &val);
    LOG(INFO) << "get copy_job_key=" << hex(key);

    if (ret == 1) { // not found
        code = MetaServiceCode::COPY_JOB_NOT_FOUND;
        ss << "copy job does not found";
        msg = ss.str();
        return;
    } else if (ret < 0) { // error
        code = MetaServiceCode::KV_TXN_GET_ERR;
        ss << "failed to get copy_job, instance_id=" << instance_id << " ret=" << ret;
        msg = ss.str();
        return;
    }

    CopyJobPB copy_job;
    if (!copy_job.ParseFromString(val)) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        msg = "failed to parse CopyJobPB";
        return;
    }

    std::vector<std::string> copy_files;
    if (request->action() == FinishCopyRequest::COMMIT) {
        // 1. update copy job status from Loading to Finish
        copy_job.set_job_status(CopyJobPB::FINISH);
        if (request->has_finish_time_ms()) {
            copy_job.set_finish_time_ms(request->finish_time_ms());
        }
        val = copy_job.SerializeAsString();
        if (val.empty()) {
            msg = "failed to serialize";
            code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
            return;
        }
        txn->put(key, val);
        LOG(INFO) << "put copy_job_key=" << hex(key);
    } else if (request->action() == FinishCopyRequest::ABORT ||
               request->action() == FinishCopyRequest::REMOVE) {
        // 1. remove copy job kv
        // 2. remove copy file kvs
        txn->remove(key);
        LOG(INFO) << (request->action() == FinishCopyRequest::ABORT ? "abort" : "remove")
                  << " copy_job_key=" << hex(key);
        for (const auto& file : copy_job.object_files()) {
            // copy file key
            CopyFileKeyInfo file_key_info {instance_id, request->stage_id(), request->table_id(),
                                           file.relative_path(), file.etag()};
            std::string file_key;
            copy_file_key(file_key_info, &file_key);
            copy_files.emplace_back(std::move(file_key));
        }
        for (const auto& k : copy_files) {
            txn->remove(k);
            LOG(INFO) << "remove copy_file_key=" << hex(k);
        }
    } else {
        msg = "Unhandled action";
        code = MetaServiceCode::UNDEFINED_ERR;
        return;
    }

    ret = txn->commit();
    if (ret != 0) {
        code = ret == -1 ? MetaServiceCode::KV_TXN_CONFLICT : MetaServiceCode::KV_TXN_COMMIT_ERR;
        msg = fmt::format("failed to commit kv txn, ret={}", ret);
        LOG(WARNING) << msg;
    }
}

void MetaServiceImpl::get_copy_job(google::protobuf::RpcController* controller,
                                   const ::selectdb::GetCopyJobRequest* request,
                                   ::selectdb::GetCopyJobResponse* response,
                                   ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(get_copy_job);
    std::string cloud_unique_id = request->has_cloud_unique_id() ? request->cloud_unique_id() : "";
    if (cloud_unique_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "cloud unique id not set";
        return;
    }

    instance_id = get_instance_id(resource_mgr_, cloud_unique_id);
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << cloud_unique_id;
        return;
    }

    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        msg = "failed to create txn";
        LOG(WARNING) << msg << " ret=" << ret;
        return;
    }

    CopyJobKeyInfo key_info {instance_id, request->stage_id(), request->table_id(),
                             request->copy_id(), request->group_id()};
    std::string key;
    copy_job_key(key_info, &key);
    std::string val;
    ret = txn->get(key, &val);
    if (ret == 1) { // not found key
        return;
    } else if (ret < 0) { // error
        code = MetaServiceCode::KV_TXN_GET_ERR;
        msg = "failed to get copy job";
        LOG(WARNING) << msg << " ret=" << ret;
        return;
    }
    CopyJobPB copy_job;
    if (!copy_job.ParseFromString(val)) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        msg = "failed to parse CopyJobPB";
        return;
    }
    response->mutable_copy_job()->CopyFrom(copy_job);
}

void MetaServiceImpl::get_copy_files(google::protobuf::RpcController* controller,
                                     const ::selectdb::GetCopyFilesRequest* request,
                                     ::selectdb::GetCopyFilesResponse* response,
                                     ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(get_copy_files);
    std::string cloud_unique_id = request->has_cloud_unique_id() ? request->cloud_unique_id() : "";
    if (cloud_unique_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "cloud unique id not set";
        return;
    }

    instance_id = get_instance_id(resource_mgr_, cloud_unique_id);
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << cloud_unique_id;
        return;
    }
    RPC_RATE_LIMIT(get_copy_files)

    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        msg = "failed to create txn";
        LOG(WARNING) << msg << " ret=" << ret;
        return;
    }

    CopyJobKeyInfo key_info0 {instance_id, request->stage_id(), request->table_id(), "", 0};
    CopyJobKeyInfo key_info1 {instance_id, request->stage_id(), request->table_id() + 1, "", 0};
    std::string key0;
    std::string key1;
    copy_job_key(key_info0, &key0);
    copy_job_key(key_info1, &key1);
    std::unique_ptr<RangeGetIterator> it;
    do {
        ret = txn->get(key0, key1, &it);
        if (ret != 0) {
            code = MetaServiceCode::KV_TXN_GET_ERR;
            msg = "failed to get copy jobs";
            LOG(WARNING) << msg << " ret=" << ret;
            return;
        }

        while (it->has_next()) {
            auto [k, v] = it->next();
            if (!it->has_next()) key0 = k;
            CopyJobPB copy_job;
            if (!copy_job.ParseFromArray(v.data(), v.size())) {
                code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                msg = "failed to parse CopyJobPB";
                return;
            }
            // TODO check if job is timeout
            for (const auto& file : copy_job.object_files()) {
                response->add_object_files()->CopyFrom(file);
            }
        }
        key0.push_back('\x00');
    } while (it->more());
}

void MetaServiceImpl::filter_copy_files(google::protobuf::RpcController* controller,
                                        const ::selectdb::FilterCopyFilesRequest* request,
                                        ::selectdb::FilterCopyFilesResponse* response,
                                        ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(filter_copy_files);
    std::string cloud_unique_id = request->has_cloud_unique_id() ? request->cloud_unique_id() : "";
    if (cloud_unique_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "cloud unique id not set";
        return;
    }

    instance_id = get_instance_id(resource_mgr_, cloud_unique_id);
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << cloud_unique_id;
        return;
    }
    RPC_RATE_LIMIT(filter_copy_files)

    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        msg = "failed to create txn";
        LOG(WARNING) << msg << " ret=" << ret;
        return;
    }

    std::vector<ObjectFilePB> filter_files;
    for (auto i = 0; i < request->object_files().size(); ++i) {
        auto& file = request->object_files().at(i);
        // 1. get copy file kv to check if file is loading or loaded
        CopyFileKeyInfo file_key_info {instance_id, request->stage_id(), request->table_id(),
                                       file.relative_path(), file.etag()};
        std::string file_key;
        copy_file_key(file_key_info, &file_key);
        std::string file_val;
        ret = txn->get(file_key, &file_val);
        if (ret == 0) { // found key
            continue;
        } else if (ret < 0) { // error
            code = MetaServiceCode::KV_TXN_GET_ERR;
            msg = "failed to get copy file";
            LOG(WARNING) << msg << " ret=" << ret;
            return;
        } else {
            response->add_object_files()->CopyFrom(file);
        }
    }
}

void MetaServiceImpl::get_cluster_status(google::protobuf::RpcController* controller,
                                         const ::selectdb::GetClusterStatusRequest* request,
                                         GetClusterStatusResponse *response,
                                         ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(get_cluster_status);
    if (request->instance_ids().empty() && request->cloud_unique_ids().empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "cloud_unique_ids or instance_ids must be given, instance_ids.size: "
              + std::to_string(request->instance_ids().size())
              + " cloud_unique_ids.size: " + std::to_string(request->cloud_unique_ids().size());
        return;
    }

    std::vector<std::string> instance_ids;
    instance_ids.reserve(std::max(request->instance_ids().size(), request->cloud_unique_ids().size()));

    // priority use instance_ids
    if (!request->instance_ids().empty()) {
        std::for_each(request->instance_ids().begin(), request->instance_ids().end(), [&](const auto& it) {
            instance_ids.emplace_back(it);
        });
    } else if (!request->cloud_unique_ids().empty()) {
        std::for_each(request->cloud_unique_ids().begin(), request->cloud_unique_ids().end(), [&](const auto& it){
            std::string instance_id = get_instance_id(resource_mgr_, it);
            if (instance_id.empty()) {
                LOG(INFO) << "cant get instance_id from cloud_unique_id : " <<  it;
                return;
            }
            instance_ids.emplace_back(instance_id);
        });
    }

    if (instance_ids.empty()) {
        LOG(INFO) << "can't get valid instanceids";
        return;
    }
    bool has_filter = request->has_status();

    RPC_RATE_LIMIT(get_cluster_status)

    auto get_clusters_info = [this, &request, &response, &has_filter](const std::string& instance_id) {
        InstanceKeyInfo key_info {instance_id};
        std::string key;
        std::string val;
        instance_key(key_info, &key);

        std::unique_ptr<Transaction> txn;
        int ret = txn_kv_->create_txn(&txn);
        if (ret != 0) {
            LOG(WARNING) << "failed to create txn ret=" << ret;
            return;
        }
        ret = txn->get(key, &val);
        LOG(INFO) << "get instance_key=" << hex(key);

        if (ret != 0) {
            LOG(WARNING) << "failed to get instance, instance_id=" << instance_id << " ret=" << ret;
            return;
        }

        InstanceInfoPB instance;
        if (!instance.ParseFromString(val)) {
            LOG(WARNING) << "failed to parse InstanceInfoPB";
            return;
        }
        GetClusterStatusResponse::GetClusterStatusResponseDetail detail;
        detail.set_instance_id(instance_id);
        for (auto &cluster : instance.clusters()) {
            if (cluster.type() != ClusterPB::COMPUTE) {
                continue;
            }
            ClusterPB pb;
            pb.set_cluster_name(cluster.cluster_name());
            pb.set_cluster_id(cluster.cluster_id());
            if (has_filter && request->status() != cluster.cluster_status()) {
                continue;
            }
            // for compatible
            if (cluster.has_cluster_status()) {
                pb.set_cluster_status(cluster.cluster_status());
            } else {
                pb.set_cluster_status(ClusterStatus::NORMAL);
            }
            detail.add_clusters()->CopyFrom(pb);
        }
        if (detail.clusters().size() == 0) {
            return;
        }
        response->add_details()->CopyFrom(detail);
    };

    std::for_each(instance_ids.begin(), instance_ids.end(), get_clusters_info);

    msg = proto_to_json(*response);
}

void notify_refresh_instance(std::shared_ptr<TxnKv> txn_kv, const std::string& instance_id) {
    LOG(INFO) << "begin notify_refresh_instance";
    std::unique_ptr<Transaction> txn;
    int ret = txn_kv->create_txn(&txn);
    if (ret != 0) {
        LOG(WARNING) << "failed to create txn"
                     << " ret=" << ret;
        return;
    }
    std::string key = system_meta_service_registry_key();
    std::string val;
    ret = txn->get(key, &val);
    if (ret != 0) {
        LOG(WARNING) << "failed to get server registry"
                     << " ret=" << ret;
        return;
    }
    std::string self_endpoint;
    if (config::hostname.empty()) {
        self_endpoint = fmt::format("{}:{}", butil::my_ip_cstr(), config::brpc_listen_port);
    } else {
        self_endpoint = fmt::format("{}:{}", config::hostname, config::brpc_listen_port);
    }
    ServiceRegistryPB reg;
    reg.ParseFromString(val);

    brpc::ChannelOptions options;
    options.connection_type = brpc::ConnectionType::CONNECTION_TYPE_SHORT;

    static std::unordered_map<std::string, std::shared_ptr<MetaService_Stub>> stubs;
    static std::mutex mtx;

    std::vector<bthread_t> btids;
    btids.reserve(reg.items_size());
    for (int i = 0; i < reg.items_size(); ++i) {
        ret = 0;
        auto& e = reg.items(i);
        std::string endpoint;
        if (e.has_host()) {
            endpoint = fmt::format("{}:{}", e.host(), e.port());
        } else {
            endpoint = fmt::format("{}:{}", e.ip(), e.port());
        }
        if (endpoint == self_endpoint) continue;

        // Prepare stub
        std::shared_ptr<MetaService_Stub> stub;
        do {
            std::lock_guard l(mtx);
            if (auto it = stubs.find(endpoint); it != stubs.end()) {
                stub = it->second;
                break;
            }
            auto channel = std::make_unique<brpc::Channel>();
            ret = channel->Init(endpoint.c_str(), &options);
            if (ret != 0) {
                LOG(WARNING) << "fail to init brpc channel, endpoint=" << endpoint;
                break;
            }
            stub = std::make_shared<MetaService_Stub>(channel.release(),
                                                      google::protobuf::Service::STUB_OWNS_CHANNEL);
        } while (false);
        if (ret != 0) continue;

        // Issue RPC
        auto f = new std::function<void()>([instance_id, stub, endpoint] {
            int num_try = 0;
            bool succ = false;
            while (num_try++ < 3) {
                brpc::Controller cntl;
                cntl.set_timeout_ms(3000);
                AlterInstanceRequest req;
                AlterInstanceResponse res;
                req.set_instance_id(instance_id);
                req.set_op(AlterInstanceRequest::REFRESH);
                stub->alter_instance(&cntl, &req, &res, nullptr);
                if (cntl.Failed()) {
                    LOG_WARNING("issue refresh instance rpc")
                            .tag("endpoint", endpoint)
                            .tag("num_try", num_try)
                            .tag("code", cntl.ErrorCode())
                            .tag("msg", cntl.ErrorText());
                } else {
                    succ = res.status().code() == MetaServiceCode::OK;
                    LOG(INFO) << (succ ? "succ" : "failed")
                              << " to issue refresh_instance rpc, num_try=" << num_try
                              << " endpoint=" << endpoint << " response=" << proto_to_json(res);
                    if (succ) return;
                }
                bthread_usleep(300000);
            }
            if (succ) return;
            LOG(WARNING) << "failed to refresh finally, it may left the system inconsistent,"
                         << " tired=" << num_try;
        });
        bthread_t bid;
        ret = bthread_start_background(&bid, nullptr, run_bthread_work, f);
        if (ret != 0) continue;
        btids.emplace_back(bid);
    } // for
    for (auto& i : btids) bthread_join(i, nullptr);
    LOG(INFO) << "finish notify_refresh_instance, num_items=" << reg.items_size();
}

void MetaServiceImpl::update_delete_bitmap(google::protobuf::RpcController* controller,
                                           const ::selectdb::UpdateDeleteBitmapRequest* request,
                                           ::selectdb::UpdateDeleteBitmapResponse* response,
                                           ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(update_delete_bitmap);
    std::string cloud_unique_id = request->has_cloud_unique_id() ? request->cloud_unique_id() : "";
    if (cloud_unique_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "cloud unique id not set";
        return;
    }

    instance_id = get_instance_id(resource_mgr_, cloud_unique_id);
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(WARNING) << msg << ", cloud_unique_id=" << request->cloud_unique_id();
        return;
    }
    RPC_RATE_LIMIT(update_delete_bitmap)
    std::unique_ptr<Transaction> txn;
    if (txn_kv_->create_txn(&txn) != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        msg = "failed to init txn";
        return;
    }

    // 1.Check whether the lock expires
    auto table_id = request->table_id();
    auto partition_id = request->partition_id();
    std::string lock_key = meta_delete_bitmap_update_lock_key({instance_id, table_id, partition_id});
    std::string lock_val;
    DeleteBitmapUpdateLockPB lock_info;
    ret = txn->get(lock_key, &lock_val);
    if (ret != 0) {
        ss << "failed to get delete bitmap lock info, instance_id=" << instance_id
           << " table_id=" << table_id << " key=" << hex(lock_key) << " ret=" << ret;
        msg = ss.str();
        code = MetaServiceCode::KV_TXN_GET_ERR;
        return;
    }
    if (!lock_info.ParseFromString(lock_val)) [[unlikely]] {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        msg = "failed to parse DeleteBitmapUpdateLockPB";
        return;
    }
    if (lock_info.lock_id() != request->lock_id()) {
        LOG(WARNING) << "lock is expired, table_id=" << table_id << " key=" << hex(lock_key)
                     << " request lock_id=" << request->lock_id()
                     << " locked lock_id=" << lock_info.lock_id() << " ret=" << ret;
        msg = "lock id not match";
        code = MetaServiceCode::LOCK_EXPIRED;
        return;
    }
    bool found = false;
    for (auto initiator : lock_info.initiators()) {
        if (request->initiator() == initiator) {
            found = true;
            break;
        }
    }
    if (!found) {
        LOG(WARNING) << "cannot found initiator, table_id=" << table_id << " key=" << hex(lock_key)
                     << " request lock_id=" << request->lock_id()
                     << " request initiator=" << request->initiator()
                     << " locked lock_id=" << lock_info.lock_id() << " ret=" << ret;
        msg = "lock initator not exist";
        code = MetaServiceCode::LOCK_EXPIRED;
        return;
    }

    // 2.Check pending delete bitmap
    auto tablet_id = request->tablet_id();
    std::string pending_key = meta_pending_delete_bitmap_key({instance_id, tablet_id});
    std::string pending_val;
    ret = txn->get(pending_key, &pending_val);
    if (ret < 0) {
        ss << "failed to get delete bitmap pending info, instance_id=" << instance_id
           << " tablet_id=" << tablet_id << " key=" << hex(pending_key) << " ret=" << ret;
        msg = ss.str();
        code = MetaServiceCode::KV_TXN_GET_ERR;
        return;
    }

    // delete delete bitmpap of expired txn
    if (ret == 0) {
        PendingDeleteBitmapPB pending_info;
        if (!pending_info.ParseFromString(pending_val)) [[unlikely]] {
            code = MetaServiceCode::PROTOBUF_PARSE_ERR;
            msg = "failed to parse PendingDeleteBitmapPB";
            return;
        }
        for (auto& delete_bitmap_key : pending_info.delete_bitmap_keys()) {
            txn->remove(delete_bitmap_key);
            LOG(INFO) << "xxx remove pending delete bitmap, delete_bitmap_key="
                      << hex(delete_bitmap_key) << " lock_id=" << request->lock_id();
        }
    }

    // 3.Update delete bitmap for curent txn
    int rst_ids_size = request->rowset_ids_size();
    int seg_ids_size = request->segment_ids_size();
    int versions_size = request->versions_size();
    int delete_bitmap_size = request->segment_delete_bitmaps_size();
    if (rst_ids_size != seg_ids_size || seg_ids_size != delete_bitmap_size ||
        delete_bitmap_size != versions_size) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        ss << "delete bitmap size not match. "
           << " rowset_size=" << rst_ids_size << " seg_size=" << seg_ids_size
           << " ver_size=" << versions_size << " delete_bitmap_size=" << delete_bitmap_size;
        msg = ss.str();
        return;
    }

    // lock_id > 0 : load
    // lock_id = -1 : compaction
    // lock_id = -2 : schema change
    bool is_load_op = request->lock_id() > 0;
    PendingDeleteBitmapPB new_pending_info;
    for (size_t i = 0; i < rst_ids_size; ++i) {
        std::string rowset_id = request->rowset_ids(i);
        auto seg_id = request->segment_ids(i);
        auto ver = request->versions(i);
        std::string val = request->segment_delete_bitmaps(i);
        MetaDeleteBitmapInfo key_info {instance_id, tablet_id, rowset_id, ver, seg_id};
        std::string key;
        meta_delete_bitmap_key(key_info, &key);
        txn->put(key, val);
        if (is_load_op) {
            new_pending_info.add_delete_bitmap_keys(key);
        }
        LOG(INFO) << "xxx update delete bitmap put delete_bitmap_key=" << hex(key)
                  << " lock_id=" << request->lock_id() << " value_size: " << val.size();
    }

    // no need to record pending key for compaction or schema change,
    // because delete bitmap will attach to new rowset, just delete new rowset if failed
    if (is_load_op) {
        if (!new_pending_info.SerializeToString(&pending_val)) {
            code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
            msg = "failed to serialize pending delete bitmap";
            return;
        }
        txn->put(pending_key, pending_val);
        LOG(INFO) << "xxx update delete bitmap put pending_key=" << hex(pending_key)
                  << " lock_id=" << request->lock_id() << " value_size: " << pending_val.size();
    }

    ret = txn->commit();
    if (ret != 0) {
        code = ret == -1 ? MetaServiceCode::KV_TXN_CONFLICT : MetaServiceCode::KV_TXN_COMMIT_ERR;
        ss << "failed to update delete bitmap, ret=" << ret;
        msg = ss.str();
        return;
    }
}

void MetaServiceImpl::get_delete_bitmap(google::protobuf::RpcController* controller,
                                        const ::selectdb::GetDeleteBitmapRequest* request,
                                        ::selectdb::GetDeleteBitmapResponse* response,
                                        ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(get_delete_bitmap);
    std::string cloud_unique_id = request->has_cloud_unique_id() ? request->cloud_unique_id() : "";
    if (cloud_unique_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "cloud unique id not set";
        return;
    }

    instance_id = get_instance_id(resource_mgr_, cloud_unique_id);
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(WARNING) << msg << ", cloud_unique_id=" << request->cloud_unique_id();
        return;
    }
    RPC_RATE_LIMIT(get_delete_bitmap)
    auto tablet_id = request->tablet_id();
    auto& rowset_ids = request->rowset_ids();
    auto& begin_versions = request->begin_versions();
    auto& end_versions = request->end_versions();
    if (rowset_ids.size() != begin_versions.size() || rowset_ids.size() != end_versions.size()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        ss << "rowset and version size not match. "
           << " rowset_size=" << rowset_ids.size()
           << " begin_version_size=" << begin_versions.size()
           << " end_version_size=" << end_versions.size();
        msg = ss.str();
        return;
    }

    for (size_t i = 0; i < rowset_ids.size(); i++) {
        std::unique_ptr<Transaction> txn;
        if (txn_kv_->create_txn(&txn) != 0) {
            code = MetaServiceCode::KV_TXN_CREATE_ERR;
            msg = "failed to init txn";
            return;
        }
        MetaDeleteBitmapInfo key_info0 {instance_id, tablet_id, rowset_ids[i], begin_versions[i],
                                        0};
        MetaDeleteBitmapInfo key_info1 {instance_id, tablet_id, rowset_ids[i], end_versions[i],
                                        INT64_MAX};
        std::string key0;
        std::string key1;
        meta_delete_bitmap_key(key_info0, &key0);
        meta_delete_bitmap_key(key_info1, &key1);

        std::unique_ptr<RangeGetIterator> it;
        do {
            ret = txn->get(key0, key1, &it);
            if (ret != 0) {
                code = MetaServiceCode::KV_TXN_GET_ERR;
                ss << "internal error, failed to get delete bitmap, ret=" << ret;
                msg = ss.str();
                return;
            }

            while (it->has_next()) {
                auto [k, v] = it->next();
                auto k1 = k;
                k1.remove_prefix(1);
                std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
                decode_key(&k1, &out);
                // 0x01 "meta" ${instance_id}  "delete_bitmap" ${tablet_id}
                // ${rowset_id0} ${version1} ${segment_id0} -> DeleteBitmapPB
                auto ver = std::get<int64_t>(std::get<0>(out[5]));
                auto seg_id = std::get<int64_t>(std::get<0>(out[6]));
                response->add_rowset_ids(rowset_ids[i]);
                response->add_segment_ids(seg_id);
                response->add_versions(ver);
                response->add_segment_delete_bitmaps(std::string(v));
                if (!it->has_next()) {
                    key0 = k;
                }
            }
            key0.push_back('\x00'); // Update to next smallest key for iteration
        } while (it->more());
    }
}

void MetaServiceImpl::get_delete_bitmap_update_lock(google::protobuf::RpcController* controller,
                                       const ::selectdb::GetDeleteBitmapUpdateLockRequest* request,
                                       ::selectdb::GetDeleteBitmapUpdateLockResponse* response,
                                       ::google::protobuf::Closure* done)  {
    RPC_PREPROCESS(get_delete_bitmap_update_lock);
    std::string cloud_unique_id = request->has_cloud_unique_id() ? request->cloud_unique_id() : "";
    if (cloud_unique_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "cloud unique id not set";
        return;
    }

    instance_id = get_instance_id(resource_mgr_, cloud_unique_id);
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << cloud_unique_id;
        return;
    }

    RPC_RATE_LIMIT(get_delete_bitmap_update_lock)
    std::unique_ptr<Transaction> txn;
    if (txn_kv_->create_txn(&txn) != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        msg = "failed to init txn";
        return;
    }
    auto table_id = request->table_id();
    for (size_t i = 0; i < request->partition_ids_size(); i++) {
        int64_t partition_id = request->partition_ids(i);
        std::string lock_key =
                meta_delete_bitmap_update_lock_key({instance_id, table_id, partition_id});
        std::string lock_val;
        DeleteBitmapUpdateLockPB lock_info;
        ret = txn->get(lock_key, &lock_val);
        if (ret < 0) {
            ss << "failed to get delete bitmap update lock, instance_id=" << instance_id
               << " table_id=" << table_id << " key=" << hex(lock_key) << " ret=" << ret;
            msg = ss.str();
            code = MetaServiceCode::KV_TXN_GET_ERR;
            return;
        }
        using namespace std::chrono;
        int64_t now = duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
        if (ret == 0) {
            if (!lock_info.ParseFromString(lock_val)) [[unlikely]] {
                code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                msg = "failed to parse DeleteBitmapUpdateLockPB";
                return;
            }
            if (lock_info.expiration() > 0 && lock_info.expiration() < now) {
                LOG(INFO) << "delete bitmap lock expired, continue to process. lock_id="
                          << lock_info.lock_id() << " table_id=" << table_id
                          << " partition_id=" << partition_id << " now=" << now;
                lock_info.clear_initiators();
            } else if (lock_info.lock_id() != request->lock_id()) {
                ss << "already be locked. requset lock_id=" << request->lock_id()
                   << " locked by lock_id=" << lock_info.lock_id() << " table_id=" << table_id
                   << " partition_id=" << partition_id << " now=" << now
                   << " expiration=" << lock_info.expiration();
                msg = ss.str();
                code = MetaServiceCode::LOCK_CONFLICT;
                continue;
            }
        }

        lock_info.set_lock_id(request->lock_id());
        lock_info.set_expiration(now + request->expiration());
        bool found = false;
        for (auto initiator : lock_info.initiators()) {
            if (request->initiator() == initiator) {
                found = true;
                break;
            }
        }
        if (!found) {
            lock_info.add_initiators(request->initiator());
        }
        lock_info.SerializeToString(&lock_val);
        if (lock_val.empty()) {
            code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
            msg = "pb serialization error";
            return;
        }
        txn->put(lock_key, lock_val);
        LOG(INFO) << "xxx put lock_key=" << hex(lock_key) << " lock_id=" << request->lock_id()
                  << " initiators_size: " << lock_info.initiators_size();
    }

    ret = txn->commit();
    if (ret != 0) {
        code = ret == -1 ? MetaServiceCode::KV_TXN_CONFLICT : MetaServiceCode::KV_TXN_COMMIT_ERR;
        ss << "failed to get_delete_bitmap_update_lock, ret=" << ret;
        msg = ss.str();
        return;
    }
}

std::pair<MetaServiceCode, std::string> MetaServiceImpl::get_instance_info(
        const std::string& instance_id, const std::string& cloud_unique_id,
        InstanceInfoPB* instance) {
    std::string cloned_instance_id = instance_id;
    if (instance_id.empty()) {
        if (cloud_unique_id.empty()) {
            return {MetaServiceCode::INVALID_ARGUMENT, "empty instance_id and cloud_unique_id"};
        }
        // get instance_id by cloud_unique_id
        cloned_instance_id = get_instance_id(resource_mgr_, cloud_unique_id);
        if (cloned_instance_id.empty()) {
            std::string msg =
                    fmt::format("cannot find instance_id with cloud_unique_id={}", cloud_unique_id);
            return {MetaServiceCode::INVALID_ARGUMENT, std::move(msg)};
        }
    }

    std::unique_ptr<Transaction> txn0;
    int ret_txn = txn_kv_->create_txn(&txn0);
    if (ret_txn != 0) {
        return {MetaServiceCode::KV_TXN_CREATE_ERR, "failed to create txn"};
    }

    std::shared_ptr<Transaction> txn(txn0.release());
    auto [c0, m0] = resource_mgr_->get_instance(txn, cloned_instance_id, instance);
    if (c0 != 0) {
        return {MetaServiceCode::KV_TXN_GET_ERR, "failed to get instance, info=" + m0};
    }

    // maybe do not decrypt ak/sk?
    MetaServiceCode code = MetaServiceCode::OK;
    std::string msg;
    decrypt_instance_info(*instance, cloned_instance_id, code, msg, txn);
    return {code, std::move(msg)};
}

#undef RPC_PREPROCESS
#undef RPC_RATE_LIMIT
} // namespace selectdb
// vim: et ts=4 sw=4 cc=80:
