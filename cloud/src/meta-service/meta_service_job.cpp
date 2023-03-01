
// clang-format off
#include <gen_cpp/selectdb_cloud.pb.h>
#include "common/stopwatch.h"
#include "meta_service.h"

#include "meta-service/keys.h"
#include "common/bvars.h"
#include "common/config.h"
#include "common/logging.h"
#include "common/util.h"
#include "common/sync_point.h"
#include "common/object_pool.h"

#include "brpc/closure_guard.h"
#include "brpc/controller.h"

#include <chrono>
#include <cstddef>
// clang-format on

#define RPC_PREPROCESS(func_name)                                                           \
    StopWatch sw;                                                                           \
    auto ctrl = static_cast<brpc::Controller*>(controller);                                 \
    LOG(INFO) << "begin " #func_name " rpc from " << ctrl->remote_side()                    \
              << " request=" << request->ShortDebugString();                                \
    brpc::ClosureGuard closure_guard(done);                                                 \
    [[maybe_unused]] int ret = 0;                                                           \
    [[maybe_unused]] std::stringstream ss;                                                  \
    [[maybe_unused]] MetaServiceCode code = MetaServiceCode::OK;                            \
    [[maybe_unused]] std::string msg;                                                       \
    [[maybe_unused]] std::string instance_id;                                               \
    [[maybe_unused]] bool drop_request = false;                                             \
    std::unique_ptr<int, std::function<void(int*)>> defer_status((int*)0x01, [&](int*) {    \
        response->mutable_status()->set_code(code);                                         \
        response->mutable_status()->set_msg(msg);                                           \
        LOG(INFO) << "finish " #func_name " from " << ctrl->remote_side() << " ret=" << ret \
                  << " response=" << response->ShortDebugString();                          \
        closure_guard.reset(nullptr);                                                       \
        if (config::use_detailed_metrics && !instance_id.empty() && !drop_request) {        \
            g_bvar_ms_##func_name.put(instance_id, sw.elapsed_us());                        \
        }                                                                                   \
    });

#define RPC_RATE_LIMIT(func_name)                                                           \
    if (config::enable_rate_limit &&                                                        \
            config::use_detailed_metrics && !instance_id.empty()) {                         \
        auto rate_limiter = rate_limiter_->get_rpc_rate_limiter(#func_name);                \
        assert(rate_limiter != nullptr);                                                    \
        std::function<int()> get_bvar_qps = [&] {                                           \
                    return g_bvar_ms_##func_name.get(instance_id)->qps(); };                \
        if (!rate_limiter->get_qps_token(instance_id, get_bvar_qps)) {                      \
            drop_request = true;                                                            \
            code = MetaServiceCode::MAX_QPS_LIMIT;                                          \
            msg = "reach max qps limit";                                                    \
            return;                                                                         \
        }                                                                                   \
    }
// Empty string not is not processed
template <typename T, size_t S>
static inline constexpr size_t get_file_name_offset(const T (&s)[S], size_t i = S - 1) {
    return (s[i] == '/' || s[i] == '\\') ? i + 1 : (i > 0 ? get_file_name_offset(s, i - 1) : 0);
}
#define SS (ss << &__FILE__[get_file_name_offset(__FILE__)] << ":" << __LINE__ << " ")
#define INSTANCE_LOG(severity) (LOG(severity) << '(' << instance_id << ')')

namespace selectdb {

extern std::string get_instance_id(const std::shared_ptr<ResourceManager>& rc_mgr,
                                   const std::string& cloud_unique_id);

void get_tablet_idx(MetaServiceCode& code, std::string& msg, std::stringstream& ss, int& ret,
                    std::unique_ptr<Transaction>& txn, std::string& instance_id, int64_t& table_id,
                    int64_t& index_id, int64_t& partition_id, int64_t& tablet_id) {
    // Get tablet id index from kv
    if (table_id <= 0 || index_id <= 0 || partition_id <= 0) {
        std::string idx_key = meta_tablet_idx_key({instance_id, tablet_id});
        std::string idx_val;
        ret = txn->get(idx_key, &idx_val);
        INSTANCE_LOG(INFO) << "get tablet meta, tablet_id=" << tablet_id << " key=" << hex(idx_key);
        if (ret != 0) {
            SS << "failed to get table id with tablet_id from kv, err="
               << (ret == 1 ? "not found" : "internal error");
            code = ret == 1 ? MetaServiceCode::UNDEFINED_ERR : MetaServiceCode::KV_TXN_GET_ERR;
            msg = ss.str();
            return;
        }
        TabletIndexPB idx_pb;
        if (!idx_pb.ParseFromString(idx_val)) {
            code = MetaServiceCode::PROTOBUF_PARSE_ERR;
            msg = "malformed tablet table value";
            return;
        }
        table_id = idx_pb.table_id();
        index_id = idx_pb.index_id();
        partition_id = idx_pb.partition_id();
        if (tablet_id != idx_pb.tablet_id()) {
            code = MetaServiceCode::UNDEFINED_ERR;
            msg = "internal error";
            INSTANCE_LOG(WARNING) << "unexpected error given_tablet_id=" << tablet_id
                                  << " idx_pb_tablet_id=" << idx_pb.tablet_id();
            return;
        }
    }
}

void start_compaction_job(MetaServiceCode& code, std::string& msg, std::stringstream& ss, int& ret,
                          std::unique_ptr<Transaction>& txn,
                          const ::selectdb::StartTabletJobRequest* request,
                          std::string& instance_id, int64_t& table_id, int64_t& index_id,
                          int64_t& partition_id, int64_t& tablet_id, bool& need_commit,
                          ObjectPool& obj_pool) {
    auto& compaction = request->job().compaction(0);
    if (!compaction.has_id() || compaction.id().empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "no job id specified";
        return;
    }

    // check compaction_cnt to avoid compact on expired tablet cache
    if (!compaction.has_base_compaction_cnt() || !compaction.has_cumulative_compaction_cnt()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "no valid compaction_cnt given";
        return;
    }
    std::string stats_key =
            stats_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id});
    std::string stats_val;
    ret = txn->get(stats_key, &stats_val);
    if (ret != 0) {
        code = ret == 1 ? MetaServiceCode::TABLET_NOT_FOUND : MetaServiceCode::KV_TXN_GET_ERR;
        SS << (ret == 1 ? "not found" : "get kv error") << " when get tablet stats, "
           << " tablet_id=" << tablet_id << " key=" << hex(stats_key) << " ret=" << ret;
        msg = ss.str();
        return;
    }
    TabletStatsPB stats;
    CHECK(stats.ParseFromString(stats_val));
    if (compaction.base_compaction_cnt() < stats.base_compaction_cnt() ||
        compaction.cumulative_compaction_cnt() < stats.cumulative_compaction_cnt()) {
        code = MetaServiceCode::STALE_TABLET_CACHE;
        SS << "could not perform compaction on expired tablet cache."
           << " req_base_compaction_cnt=" << compaction.base_compaction_cnt()
           << ", base_compaction_cnt=" << stats.base_compaction_cnt()
           << ", req_cumulative_compaction_cnt=" << compaction.cumulative_compaction_cnt()
           << ", cumulative_compaction_cnt=" << stats.cumulative_compaction_cnt();
        msg = ss.str();
        return;
    }

    auto& job_key = *obj_pool.add(new std::string(
            job_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id})));
    auto& job_val = *obj_pool.add(new std::string());
    TabletJobInfoPB job_pb;
    ret = txn->get(job_key, &job_val);
    INSTANCE_LOG(INFO) << "get tablet job, tablet_id=" << tablet_id << " key=" << hex(job_key)
                       << " ret=" << ret;
    if (ret < 0) {
        SS << "failed to get tablet job, instance_id=" << instance_id << " tablet_id=" << tablet_id
           << " key=" << hex(job_key) << " ret=" << ret;
        msg = ss.str();
        code = MetaServiceCode::KV_TXN_GET_ERR;
        return;
    }
    TabletCompactionJobPB* recorded_compaction = nullptr;
    while (ret == 0) {
        job_pb.ParseFromString(job_val);
        if (job_pb.compaction().empty()) {
            break;
        }
        for (auto& c : *job_pb.mutable_compaction()) {
            if (c.type() == compaction.type() ||
                (c.type() == TabletCompactionJobPB::CUMULATIVE &&
                 compaction.type() == TabletCompactionJobPB::EMPTY_CUMULATIVE) ||
                (c.type() == TabletCompactionJobPB::EMPTY_CUMULATIVE &&
                 compaction.type() == TabletCompactionJobPB::CUMULATIVE)) {
                recorded_compaction = &c;
                break;
            }
        }
        if (recorded_compaction == nullptr) {
            break; // no recorded compaction with required compaction type
        }

        using namespace std::chrono;
        int64_t now = duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
        if (recorded_compaction->expiration() > 0 && recorded_compaction->expiration() < now) {
            INSTANCE_LOG(INFO) << "got an expired job, continue to process. job="
                               << proto_to_json(*recorded_compaction) << " now=" << now;
            break;
        }
        if (recorded_compaction->lease() > 0 && recorded_compaction->lease() < now) {
            INSTANCE_LOG(INFO) << "got a job exceeding lease, continue to process. job="
                               << proto_to_json(*recorded_compaction) << " now=" << now;
            break;
        }

        SS << "a tablet job has already started instance_id=" << instance_id
           << " tablet_id=" << tablet_id << " job=" << proto_to_json(*recorded_compaction);
        msg = ss.str();
        // TODO(gavin): more condition to check
        if (compaction.id() == recorded_compaction->id()) {
            code = MetaServiceCode::OK; // Idempotency
        } else if (compaction.initiator() == recorded_compaction->initiator()) {
            INSTANCE_LOG(WARNING) << "preempt compaction job of same initiator. job="
                                  << proto_to_json(*recorded_compaction);
            break;
        } else {
            code = MetaServiceCode::JOB_TABLET_BUSY;
        }
        return;
    }
    job_pb.mutable_idx()->CopyFrom(request->job().idx());
    if (recorded_compaction != nullptr) {
        recorded_compaction->CopyFrom(compaction);
    } else {
        job_pb.add_compaction()->CopyFrom(compaction);
    }
    job_pb.SerializeToString(&job_val);
    if (job_val.empty()) {
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        msg = "pb serialization error";
        return;
    }
    INSTANCE_LOG(INFO) << "compaction job to save job=" << proto_to_json(compaction);
    txn->put(job_key, job_val);
    need_commit = true;
}

void start_schema_change_job(MetaServiceCode& code, std::string& msg, std::stringstream& ss,
                             int& ret, std::unique_ptr<Transaction>& txn,
                             const ::selectdb::StartTabletJobRequest* request,
                             std::string& instance_id, int64_t& table_id, int64_t& index_id,
                             int64_t& partition_id, int64_t& tablet_id, bool& need_commit,
                             ObjectPool& obj_pool) {
    auto& schema_change = request->job().schema_change();
    if (!schema_change.has_id() || schema_change.id().empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "no job id specified";
        return;
    }
    if (!schema_change.has_initiator()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "no initiator specified";
        return;
    }

    // check new_tablet state
    int64_t new_tablet_id = schema_change.new_tablet_idx().tablet_id();
    if (new_tablet_id <= 0) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "no valid new_tablet_id given";
        return;
    }
    int64_t new_table_id = schema_change.new_tablet_idx().table_id();
    int64_t new_index_id = schema_change.new_tablet_idx().index_id();
    int64_t new_partition_id = schema_change.new_tablet_idx().partition_id();
    get_tablet_idx(code, msg, ss, ret, txn, instance_id, new_table_id, new_index_id,
                   new_partition_id, new_tablet_id);
    if (code != MetaServiceCode::OK) return;
    MetaTabletKeyInfo new_tablet_key_info {instance_id, new_table_id, new_index_id,
                                           new_partition_id, new_tablet_id};
    std::string new_tablet_key;
    std::string new_tablet_val;
    doris::TabletMetaPB new_tablet_meta;
    meta_tablet_key(new_tablet_key_info, &new_tablet_key);
    ret = txn->get(new_tablet_key, &new_tablet_val);
    if (ret != 0) {
        SS << "failed to get new tablet meta" << (ret == 1 ? " (not found)" : "")
           << " instance_id=" << instance_id << " tablet_id=" << new_tablet_id
           << " key=" << hex(new_tablet_key) << " ret=" << ret;
        msg = ss.str();
        code = ret == 1 ? MetaServiceCode::TABLET_NOT_FOUND : MetaServiceCode::KV_TXN_GET_ERR;
        return;
    }
    if (!new_tablet_meta.ParseFromString(new_tablet_val)) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        msg = "malformed tablet meta";
        return;
    }

    if (!schema_change.is_inverted_index_change()) {
        if (new_tablet_meta.tablet_state() == doris::TabletStatePB::PB_RUNNING) {
            code = MetaServiceCode::JOB_ALREADY_SUCCESS;
            msg = "schema_change job already success";
            return;
        }
        if (!new_tablet_meta.has_tablet_state() ||
            new_tablet_meta.tablet_state() != doris::TabletStatePB::PB_NOTREADY) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = "invalid new tablet state";
            return;
        }
    }

    auto& job_key = *obj_pool.add(new std::string(
            job_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id})));
    auto& job_val = *obj_pool.add(new std::string());
    TabletJobInfoPB job_pb;
    ret = txn->get(job_key, &job_val);
    INSTANCE_LOG(INFO) << "get tablet job, tablet_id=" << tablet_id << " key=" << hex(job_key)
                       << " ret=" << ret;
    if (ret < 0) {
        SS << "failed to get tablet job, instance_id=" << instance_id << " tablet_id=" << tablet_id
           << " key=" << hex(job_key) << " ret=" << ret;
        msg = ss.str();
        code = MetaServiceCode::KV_TXN_GET_ERR;
        return;
    }
    job_pb.mutable_idx()->CopyFrom(request->job().idx());
    // FE can ensure that a tablet does not have more than one schema_change job at the same time,
    // so we can directly preempt previous schema_change job.
    job_pb.mutable_schema_change()->CopyFrom(schema_change);
    job_pb.SerializeToString(&job_val);
    if (job_val.empty()) {
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        msg = "pb serialization error";
        return;
    }
    INSTANCE_LOG(INFO) << "schema_change job to save job=" << proto_to_json(schema_change);
    txn->put(job_key, job_val);
    need_commit = true;
}

void MetaServiceImpl::start_tablet_job(::google::protobuf::RpcController* controller,
                                       const ::selectdb::StartTabletJobRequest* request,
                                       ::selectdb::StartTabletJobResponse* response,
                                       ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(start_tablet_job);
    std::string cloud_unique_id = request->cloud_unique_id();
    instance_id = get_instance_id(resource_mgr_, cloud_unique_id);
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        SS << "cannot find instance_id with cloud_unique_id="
           << (cloud_unique_id.empty() ? "(empty)" : cloud_unique_id);
        msg = ss.str();
        return;
    }
    RPC_RATE_LIMIT(start_tablet_job)
    if (!request->has_job() ||
        (request->job().compaction().empty() && !request->job().has_schema_change())) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "no valid job specified";
        return;
    }

    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        msg = "failed to create txn";
        return;
    }

    int64_t tablet_id = request->job().idx().tablet_id();
    if (tablet_id <= 0) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "no valid tablet_id given";
        return;
    }
    int64_t table_id = request->job().idx().table_id();
    int64_t index_id = request->job().idx().index_id();
    int64_t partition_id = request->job().idx().partition_id();
    get_tablet_idx(code, msg, ss, ret, txn, instance_id, table_id, index_id, partition_id,
                   tablet_id);
    if (code != MetaServiceCode::OK) return;

    ObjectPool obj_pool; // To save KVs that txn may use asynchronously
    bool need_commit = false;
    std::unique_ptr<int, std::function<void(int*)>> defer_commit(
            (int*)0x01, [&ss, &ret, &txn, &code, &msg, &need_commit](int*) {
                if (!need_commit) return;
                ret = txn->commit();
                if (ret != 0) {
                    code = ret == -1 ? MetaServiceCode::KV_TXN_CONFLICT
                                     : MetaServiceCode::KV_TXN_COMMIT_ERR;
                    ss << "failed to commit job kv, ret=" << ret;
                    msg = ss.str();
                    return;
                }
            });

    if (!request->job().compaction().empty()) {
        start_compaction_job(code, msg, ss, ret, txn, request, instance_id, table_id, index_id,
                             partition_id, tablet_id, need_commit, obj_pool);
        return;
    }

    if (request->job().has_schema_change()) {
        start_schema_change_job(code, msg, ss, ret, txn, request, instance_id, table_id, index_id,
                                partition_id, tablet_id, need_commit, obj_pool);
        return;
    }
}

void process_compaction_job(MetaServiceCode& code, std::string& msg, std::stringstream& ss,
                            int& ret, std::unique_ptr<Transaction>& txn,
                            const ::selectdb::FinishTabletJobRequest* request,
                            ::selectdb::FinishTabletJobResponse* response,
                            TabletJobInfoPB& recorded_job, std::string& instance_id,
                            int64_t& table_id, int64_t& index_id, int64_t& partition_id,
                            int64_t& tablet_id, std::string& job_key, bool& need_commit,
                            ObjectPool& obj_pool) {
    //==========================================================================
    //                                check
    //==========================================================================
    if (recorded_job.compaction().empty()) {
        SS << "there is no running compaction, tablet_id=" << tablet_id;
        msg = ss.str();
        code = MetaServiceCode::INVALID_ARGUMENT;
        return;
    }

    auto& compaction = request->job().compaction(0);

    auto recorded_compaction = recorded_job.mutable_compaction()->begin();
    for (; recorded_compaction != recorded_job.mutable_compaction()->end(); ++recorded_compaction) {
        if (recorded_compaction->id() == compaction.id()) break;
    }
    if (recorded_compaction == recorded_job.mutable_compaction()->end()) {
        SS << "unmatched job id, recorded_job=" << proto_to_json(recorded_job)
           << " given_job=" << proto_to_json(compaction);
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = ss.str();
        return;
    }

    using namespace std::chrono;
    int64_t now = duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
    if (recorded_compaction->expiration() > 0 && recorded_compaction->expiration() < now) {
        code = MetaServiceCode::JOB_EXPIRED;
        SS << "expired compaction job, tablet_id=" << tablet_id
           << " job=" << proto_to_json(*recorded_compaction);
        msg = ss.str();
        // FIXME: Just remove or notify to abort?
        // LOG(INFO) << "remove expired job, tablet_id=" << tablet_id << " key=" << hex(job_key);
        return;
    }

    if (request->action() != FinishTabletJobRequest::COMMIT &&
        request->action() != FinishTabletJobRequest::ABORT &&
        request->action() != FinishTabletJobRequest::LEASE) {
        SS << "unsupported action, tablet_id=" << tablet_id << " action=" << request->action();
        msg = ss.str();
        code = MetaServiceCode::INVALID_ARGUMENT;
        return;
    }

    //==========================================================================
    //                               Lease
    //==========================================================================
    if (request->action() == FinishTabletJobRequest::LEASE) {
        if (compaction.lease() <= 0 || recorded_compaction->lease() > compaction.lease()) {
            ss << "invalid lease. recoreded_lease=" << recorded_compaction->lease()
               << " req_lease=" << compaction.lease();
            msg = ss.str();
            code = MetaServiceCode::INVALID_ARGUMENT;
            return;
        }
        recorded_compaction->set_lease(compaction.lease());
        auto& job_val = *obj_pool.add(new std::string());
        recorded_job.SerializeToString(&job_val);
        txn->put(job_key, job_val);
        INSTANCE_LOG(INFO) << "lease tablet compaction job, tablet_id=" << tablet_id
                           << " key=" << hex(job_key);
        need_commit = true;
        return;
    }

    //==========================================================================
    //                               Abort
    //==========================================================================
    if (request->action() == FinishTabletJobRequest::ABORT) {
        // TODO(gavin): mv tmp rowsets to recycle or remove them directly
        recorded_job.mutable_compaction()->erase(recorded_compaction);
        auto& job_val = *obj_pool.add(new std::string());
        recorded_job.SerializeToString(&job_val);
        txn->put(job_key, job_val);
        INSTANCE_LOG(INFO) << "abort tablet compaction job, tablet_id=" << tablet_id
                           << " key=" << hex(job_key);
        need_commit = true;
        return;
    }

    //==========================================================================
    //                               Commit
    //==========================================================================
    //
    // 1. update tablet meta
    // 2. update tablet stats
    // 3. move compaction input rowsets to recycle
    // 4. change tmp rowset to formal rowset
    // 5. remove compaction job
    //
    //==========================================================================
    //                          update talbet meta
    //==========================================================================
    auto& tablet_key = *obj_pool.add(new std::string(
            meta_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id})));
    auto& tablet_val = *obj_pool.add(new std::string());
    ret = txn->get(tablet_key, &tablet_val);
    INSTANCE_LOG(INFO) << "get tablet meta, tablet_id=" << tablet_id << " key=" << hex(tablet_key);
    if (ret != 0) {
        SS << "failed to get tablet meta, tablet_id=" << tablet_id << " key=" << hex(tablet_key)
           << " ret=" << ret;
        code = ret == 1 ? MetaServiceCode::UNDEFINED_ERR : MetaServiceCode::KV_TXN_GET_ERR;
        msg = ss.str();
        return;
    }
    doris::TabletMetaPB tablet_meta;
    tablet_meta.ParseFromString(tablet_val);
    tablet_meta.set_cumulative_layer_point(compaction.output_cumulative_point());
    tablet_val = tablet_meta.SerializeAsString();
    DCHECK(!tablet_val.empty());
    txn->put(tablet_key, tablet_val);
    INSTANCE_LOG(INFO) << "update tablet meta, tablet_id=" << tablet_id
                       << " cumulative_point=" << compaction.output_cumulative_point()
                       << " key=" << hex(tablet_key);

    auto& stats_key = *obj_pool.add(new std::string(
            stats_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id})));
    auto& stats_val = *obj_pool.add(new std::string());
    ret = txn->get(stats_key, &stats_val);
    INSTANCE_LOG(INFO) << "get tablet stats, tablet_id=" << tablet_id << " key=" << hex(stats_key)
                       << " ret=" << ret;
    if (ret != 0) {
        code = ret == 1 ? MetaServiceCode::TABLET_NOT_FOUND : MetaServiceCode::KV_TXN_GET_ERR;
        SS << (ret == 1 ? "not found" : "get kv error") << " tablet_id=" << tablet_id
           << " key=" << hex(stats_key) << " ret=" << ret;
        msg = ss.str();
        return;
    }

    //==========================================================================
    //                          Update tablet stats
    //==========================================================================
    TabletStatsPB stats;
    stats.ParseFromString(stats_val);
    if (compaction.type() == TabletCompactionJobPB::EMPTY_CUMULATIVE) {
        stats.set_cumulative_compaction_cnt(stats.cumulative_compaction_cnt() + 1);
        stats.set_cumulative_point(compaction.output_cumulative_point());
    } else if (compaction.type() == TabletCompactionJobPB::CUMULATIVE) {
        // clang-format off
        stats.set_cumulative_compaction_cnt(stats.cumulative_compaction_cnt() + 1);
        stats.set_cumulative_point(compaction.output_cumulative_point());
        stats.set_num_rows(stats.num_rows() + (compaction.num_output_rows() - compaction.num_input_rows()));
        stats.set_data_size(stats.data_size() + (compaction.size_output_rowsets() - compaction.size_input_rowsets()));
        stats.set_num_rowsets(stats.num_rowsets() + (compaction.num_output_rowsets() - compaction.num_input_rowsets()));
        stats.set_num_segments(stats.num_segments() + (compaction.num_output_segments() - compaction.num_input_segments()));
        stats.set_last_compaction_time(now);
        // clang-format on
    } else if (compaction.type() == TabletCompactionJobPB::BASE) {
        // clang-format off
        stats.set_base_compaction_cnt(stats.base_compaction_cnt() + 1);
        stats.set_num_rows(stats.num_rows() + (compaction.num_output_rows() - compaction.num_input_rows()));
        stats.set_data_size(stats.data_size() + (compaction.size_output_rowsets() - compaction.size_input_rowsets()));
        stats.set_num_rowsets(stats.num_rowsets() + (compaction.num_output_rowsets() - compaction.num_input_rowsets()));
        stats.set_num_segments(stats.num_segments() + (compaction.num_output_segments() - compaction.num_input_segments()));
        stats.set_last_compaction_time(now);
        // clang-format on
    } else {
        msg = "invalid compaction type";
        code = MetaServiceCode::INVALID_ARGUMENT;
        return;
    }
    if (stats.data_size() < 0) {
        INSTANCE_LOG(ERROR) << "buggy data size, tablet_id=" << tablet_id
            << " stats.data_size=" << stats.data_size()
            << " compaction.size_output_rowsets=" << compaction.size_output_rowsets()
            << " compaction.size_input_rowsets= " << compaction.size_input_rowsets();
    }
    DCHECK(stats.data_size() >= 0) << "stats.data_size=" << stats.data_size()
        << " compaction.size_output_rowsets=" << compaction.size_output_rowsets()
        << " compaction.size_input_rowsets=" << compaction.size_input_rowsets();
    response->mutable_stats()->CopyFrom(stats);

    stats_val = stats.SerializeAsString();
    DCHECK(!stats_val.empty());
    txn->put(stats_key, stats_val);
    VLOG_DEBUG << "update tablet stats tablet_id=" << tablet_id << " key=" << hex(stats_key)
               << " stats=" << proto_to_json(stats);
    if (compaction.type() == TabletCompactionJobPB::EMPTY_CUMULATIVE) {
        recorded_job.mutable_compaction()->erase(recorded_compaction);
        auto& job_val = *obj_pool.add(new std::string());
        recorded_job.SerializeToString(&job_val);
        txn->put(job_key, job_val);
        INSTANCE_LOG(INFO) << "remove compaction job, tablet_id=" << tablet_id
                           << " key=" << hex(job_key);
        need_commit = true;
        return;
    }

    //==========================================================================
    //                    Move input rowsets to recycle
    //==========================================================================
    if (compaction.input_versions_size() != 2 || compaction.output_versions_size() != 1 ||
        compaction.output_rowset_ids_size() != 1) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        SS << "invalid input or output versions, input_versions_size="
           << compaction.input_versions_size()
           << " output_versions_size=" << compaction.output_versions_size()
           << " output_rowset_ids_size=" << compaction.output_rowset_ids_size();
        msg = ss.str();
        return;
    }

    auto start = compaction.input_versions(0);
    auto end = compaction.input_versions(1);
    auto& rs_start =
            *obj_pool.add(new std::string(meta_rowset_key({instance_id, tablet_id, start})));
    auto& rs_end =
            *obj_pool.add(new std::string(meta_rowset_key({instance_id, tablet_id, end + 1})));

    std::unique_ptr<RangeGetIterator> it;
    int num_rowsets = 0;
    std::unique_ptr<int, std::function<void(int*)>> defer_log_range(
            (int*)0x01, [&rs_start, &rs_end, &num_rowsets, &instance_id](int*) {
                INSTANCE_LOG(INFO) << "get rowset meta, num_rowsets=" << num_rowsets << " range=["
                                   << hex(rs_start) << "," << hex(rs_end) << "]";
            });

    auto rs_start1 = rs_start;
    do {
        ret = txn->get(rs_start1, rs_end, &it);
        if (ret != 0) {
            code = MetaServiceCode::KV_TXN_GET_ERR;
            SS << "internal error, failed to get rowset range, ret=" << ret
               << " tablet_id=" << tablet_id << " range=[" << hex(rs_start1) << ", << "
               << hex(rs_end) << ")";
            msg = ss.str();
            return;
        }

        while (it->has_next()) {
            auto [k, v] = it->next();

            doris::RowsetMetaPB rs;
            if (!rs.ParseFromArray(v.data(), v.size())) {
                code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                SS << "malformed rowset meta, unable to deserialize, tablet_id=" << tablet_id
                   << " key=" << hex(k);
                msg = ss.str();
                return;
            }

            auto& recycle_key = *obj_pool.add(new std::string(
                    recycle_rowset_key({instance_id, tablet_id, rs.rowset_id_v2()})));
            RecycleRowsetPB recycle_rowset;
            recycle_rowset.set_tablet_id(tablet_id);
            recycle_rowset.set_resource_id(rs.resource_id());
            recycle_rowset.set_creation_time(now);
            auto& recycle_val = *obj_pool.add(new std::string(recycle_rowset.SerializeAsString()));
            txn->put(recycle_key, recycle_val);
            INSTANCE_LOG(INFO) << "put recycle rowset, tablet_id=" << tablet_id
                               << " key=" << hex(recycle_key);

            ++num_rowsets;
            if (!it->has_next()) rs_start1 = k;
        }
        rs_start1.push_back('\x00'); // Update to next smallest key for iteration
    } while (it->more());

    txn->remove(rs_start, rs_end);

    TEST_SYNC_POINT_CALLBACK("process_compaction_job::loop_input_done", &num_rowsets);

    if (num_rowsets < 1) {
        SS << "too few input rowsets, tablet_id=" << tablet_id << " num_rowsets=" << num_rowsets;
        code = MetaServiceCode::UNDEFINED_ERR;
        msg = ss.str();
        recorded_job.mutable_compaction()->erase(recorded_compaction);
        auto& job_val = *obj_pool.add(new std::string());
        recorded_job.SerializeToString(&job_val);
        txn->put(job_key, job_val);
        INSTANCE_LOG(INFO) << "remove compaction job, tablet_id=" << tablet_id
                           << " key=" << hex(job_key);
        need_commit = true;
        TEST_SYNC_POINT_CALLBACK("process_compaction_job::too_few_rowsets", &need_commit);
        return;
    }

    //==========================================================================
    //                Change tmp rowset to formal rowset
    //==========================================================================
    if (compaction.txn_id_size() != 1) {
        SS << "invalid txn_id, txn_id_size=" << compaction.txn_id_size();
        msg = ss.str();
        code = MetaServiceCode::INVALID_ARGUMENT;
        return;
    }
    int64_t txn_id = compaction.txn_id(0);
    auto& rowset_id = compaction.output_rowset_ids(0);
    if (txn_id <= 0 || rowset_id.empty()) {
        SS << "invalid txn_id or rowset_id, tablet_id=" << tablet_id << " txn_id=" << txn_id
           << " rowset_id=" << rowset_id;
        msg = ss.str();
        code = MetaServiceCode::INVALID_ARGUMENT;
        return;
    }
    auto& tmp_rowset_key =
            *obj_pool.add(new std::string(meta_rowset_tmp_key({instance_id, txn_id, tablet_id})));
    auto& tmp_rowset_val = *obj_pool.add(new std::string());
    ret = txn->get(tmp_rowset_key, &tmp_rowset_val);
    if (ret != 0) {
        SS << "failed to get tmp rowset key" << (ret == 1 ? " (not found)" : "")
           << ", tablet_id=" << tablet_id << " tmp_rowset_key=" << hex(tmp_rowset_key);
        msg = ss.str();
        code = ret == 1 ? MetaServiceCode::UNDEFINED_ERR : MetaServiceCode::KV_TXN_GET_ERR;
        return;
    }

    // We don't actually need to parse the rowset meta
    doris::RowsetMetaPB rs_meta;
    rs_meta.ParseFromString(tmp_rowset_val);
    if (rs_meta.txn_id() <= 0) {
        SS << "invalid txn_id in output tmp rowset meta, tablet_id=" << tablet_id
           << " txn_id=" << rs_meta.txn_id();
        msg = ss.str();
        code = MetaServiceCode::INVALID_ARGUMENT;
        return;
    }

    txn->remove(tmp_rowset_key);
    INSTANCE_LOG(INFO) << "remove tmp rowset meta, tablet_id=" << tablet_id
                       << " tmp_rowset_key=" << hex(tmp_rowset_key);

    int64_t version = compaction.output_versions(0);
    auto& rowset_key =
            *obj_pool.add(new std::string(meta_rowset_key({instance_id, tablet_id, version})));
    txn->put(rowset_key, tmp_rowset_val);
    INSTANCE_LOG(INFO) << "put rowset meta, tablet_id=" << tablet_id
                       << " rowset_key=" << hex(rowset_key);

    //==========================================================================
    //                      Remove compaction job
    //==========================================================================
    // TODO(gavin): move deleted job info into recycle or history
    recorded_job.mutable_compaction()->erase(recorded_compaction);
    auto& job_val = *obj_pool.add(new std::string());
    recorded_job.SerializeToString(&job_val);
    txn->put(job_key, job_val);
    INSTANCE_LOG(INFO) << "remove compaction job tabelt_id=" << tablet_id
                       << " key=" << hex(job_key);

    need_commit = true;
}

void process_schema_change_job(MetaServiceCode& code, std::string& msg, std::stringstream& ss,
                               int& ret, std::unique_ptr<Transaction>& txn,
                               const ::selectdb::FinishTabletJobRequest* request,
                               ::selectdb::FinishTabletJobResponse* response,
                               TabletJobInfoPB& recorded_job, std::string& instance_id,
                               int64_t& table_id, int64_t& index_id, int64_t& partition_id,
                               int64_t& tablet_id, std::string& job_key, bool& need_commit,
                               ObjectPool& obj_pool) {
    //==========================================================================
    //                                check
    //==========================================================================
    auto& schema_change = request->job().schema_change();
    int64_t new_tablet_id = schema_change.new_tablet_idx().tablet_id();
    if (new_tablet_id <= 0) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "no valid new_tablet_id given";
        return;
    }
    int64_t new_table_id = schema_change.new_tablet_idx().table_id();
    int64_t new_index_id = schema_change.new_tablet_idx().index_id();
    int64_t new_partition_id = schema_change.new_tablet_idx().partition_id();
    get_tablet_idx(code, msg, ss, ret, txn, instance_id, new_table_id, new_index_id,
                   new_partition_id, new_tablet_id);
    if (code != MetaServiceCode::OK) return;
    MetaTabletKeyInfo new_tablet_key_info {instance_id, new_table_id, new_index_id,
                                           new_partition_id, new_tablet_id};
    auto& new_tablet_key = *obj_pool.add(new std::string());
    auto& new_tablet_val = *obj_pool.add(new std::string());
    doris::TabletMetaPB new_tablet_meta;
    meta_tablet_key(new_tablet_key_info, &new_tablet_key);
    ret = txn->get(new_tablet_key, &new_tablet_val);
    if (ret != 0) {
        SS << "failed to get new tablet meta" << (ret == 1 ? " (not found)" : "")
           << " instance_id=" << instance_id << " tablet_id=" << new_tablet_id
           << " key=" << hex(new_tablet_key) << " ret=" << ret;
        msg = ss.str();
        code = ret == 1 ? MetaServiceCode::TABLET_NOT_FOUND : MetaServiceCode::KV_TXN_GET_ERR;
        return;
    }
    if (!new_tablet_meta.ParseFromString(new_tablet_val)) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        msg = "malformed tablet meta";
        return;
    }

    if (!schema_change.is_inverted_index_change()) {
        if (new_tablet_meta.tablet_state() == doris::TabletStatePB::PB_RUNNING) {
            code = MetaServiceCode::JOB_ALREADY_SUCCESS;
            msg = "schema_change job already success";
            return;
        }
        if (!new_tablet_meta.has_tablet_state() ||
            new_tablet_meta.tablet_state() != doris::TabletStatePB::PB_NOTREADY) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = "invalid new tablet state";
            return;
        }
    }

    if (!recorded_job.has_schema_change()) {
        SS << "there is no running schema_change, tablet_id=" << tablet_id;
        msg = ss.str();
        code = MetaServiceCode::INVALID_ARGUMENT;
        return;
    }
    auto& recorded_schema_change = recorded_job.schema_change();
    using namespace std::chrono;
    int64_t now = duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
    if (recorded_schema_change.expiration() > 0 && recorded_schema_change.expiration() < now) {
        code = MetaServiceCode::JOB_EXPIRED;
        SS << "expired schema_change job, tablet_id=" << tablet_id
           << " job=" << proto_to_json(recorded_schema_change);
        msg = ss.str();
        // FIXME: Just remove or notify to abort?
        // LOG(INFO) << "remove expired job, tablet_id=" << tablet_id << " key=" << hex(job_key);
        return;
    }

    // MUST check initiator to let the retried BE commit this schema_change job.
    if (schema_change.id() != recorded_schema_change.id() ||
        schema_change.initiator() != recorded_schema_change.initiator()) {
        SS << "unmatched job id or initiator, recorded_id=" << recorded_schema_change.id()
           << " given_id=" << schema_change.id()
           << " recorded_job=" << proto_to_json(recorded_schema_change)
           << " given_job=" << proto_to_json(schema_change);
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = ss.str();
        return;
    }

    if (request->action() != FinishTabletJobRequest::COMMIT &&
        request->action() != FinishTabletJobRequest::ABORT) {
        SS << "unsupported action, tablet_id=" << tablet_id << " action=" << request->action();
        msg = ss.str();
        code = MetaServiceCode::INVALID_ARGUMENT;
        return;
    }

    //==========================================================================
    //                               Abort
    //==========================================================================
    if (request->action() == FinishTabletJobRequest::ABORT) {
        // TODO(cyx)
        return;
    }

    //==========================================================================
    //                               Commit
    //==========================================================================
    //
    // 1. update new_tablet meta
    // 2. move rowsets [2-alter_version] in new_tablet to recycle
    // 3. update new_tablet stats
    // 4. change tmp rowset to formal rowset
    // 5. remove schema_change job (unnecessary)
    //
    //==========================================================================
    //                          update tablet meta
    //==========================================================================
    new_tablet_meta.set_tablet_state(doris::TabletStatePB::PB_RUNNING);
    new_tablet_meta.set_cumulative_layer_point(schema_change.output_cumulative_point());
    new_tablet_meta.SerializeToString(&new_tablet_val);
    txn->put(new_tablet_key, new_tablet_val);

    //==========================================================================
    //                move rowsets [2-alter_version] to recycle
    //==========================================================================
    if (!schema_change.has_alter_version()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "invalid alter_version";
        return;
    }
    if (schema_change.alter_version() < 2) { // no need to update stats
        // TODO(cyx): clear schema_change job?
        need_commit = true;
        return;
    }

    int64_t num_remove_rows = 0;
    int64_t size_remove_rowsets = 0;
    int64_t num_remove_rowsets = 0;
    int64_t num_remove_segments = 0;

    auto& rs_start =
            *obj_pool.add(new std::string(meta_rowset_key({instance_id, new_tablet_id, 2})));
    auto& rs_end = *obj_pool.add(new std::string(
            meta_rowset_key({instance_id, new_tablet_id, schema_change.alter_version() + 1})));
    std::unique_ptr<RangeGetIterator> it;
    auto rs_start1 = rs_start;
    do {
        ret = txn->get(rs_start1, rs_end, &it);
        if (ret != 0) {
            code = MetaServiceCode::KV_TXN_GET_ERR;
            SS << "internal error, failed to get rowset range, ret=" << ret
               << " tablet_id=" << new_tablet_id << " range=[" << hex(rs_start1) << ", << "
               << hex(rs_end) << ")";
            msg = ss.str();
            return;
        }

        while (it->has_next()) {
            auto [k, v] = it->next();

            doris::RowsetMetaPB rs;
            if (!rs.ParseFromArray(v.data(), v.size())) {
                code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                SS << "malformed rowset meta, unable to deserialize, tablet_id=" << new_tablet_id
                   << " key=" << hex(k);
                msg = ss.str();
                return;
            }

            num_remove_rows += rs.num_rows();
            size_remove_rowsets += rs.data_disk_size();
            num_remove_rowsets += 1;
            num_remove_segments += rs.num_segments();

            auto& recycle_key = *obj_pool.add(new std::string(
                    recycle_rowset_key({instance_id, new_tablet_id, rs.rowset_id_v2()})));
            RecycleRowsetPB recycle_rowset;
            recycle_rowset.set_tablet_id(new_tablet_id);
            recycle_rowset.set_resource_id(rs.resource_id());
            recycle_rowset.set_creation_time(now);
            auto& recycle_val = *obj_pool.add(new std::string(recycle_rowset.SerializeAsString()));
            txn->put(recycle_key, recycle_val);
            INSTANCE_LOG(INFO) << "put recycle rowset, tablet_id=" << new_tablet_id
                               << " key=" << hex(recycle_key);

            if (!it->has_next()) rs_start1 = k;
        }
        rs_start1.push_back('\x00'); // Update to next smallest key for iteration
    } while (it->more());

    txn->remove(rs_start, rs_end);

    //==========================================================================
    //                        update new_tablet stats
    //==========================================================================
    auto& stats_key = *obj_pool.add(new std::string(stats_tablet_key(
            {instance_id, new_table_id, new_index_id, new_partition_id, new_tablet_id})));
    auto& stats_val = *obj_pool.add(new std::string());
    ret = txn->get(stats_key, &stats_val);
    INSTANCE_LOG(INFO) << "get tablet stats, tablet_id=" << new_tablet_id
                       << " key=" << hex(stats_key) << " ret=" << ret;
    if (ret != 0) {
        code = ret == 1 ? MetaServiceCode::TABLET_NOT_FOUND : MetaServiceCode::KV_TXN_GET_ERR;
        SS << (ret == 1 ? "not found" : "get kv error") << " tablet_id=" << new_tablet_id
           << " key=" << hex(stats_key) << " ret=" << ret;
        msg = ss.str();
        return;
    }
    TabletStatsPB stats;
    stats.ParseFromString(stats_val);
    // clang-format off
    stats.set_cumulative_point(schema_change.output_cumulative_point());
    stats.set_num_rows(stats.num_rows() + (schema_change.num_output_rows() - num_remove_rows));
    stats.set_data_size(stats.data_size() + (schema_change.size_output_rowsets() - size_remove_rowsets));
    stats.set_num_rowsets(stats.num_rowsets() + (schema_change.num_output_rowsets() - num_remove_rowsets));
    stats.set_num_segments(stats.num_segments() + (schema_change.num_output_segments() - num_remove_segments));
    // clang-format on
    response->mutable_stats()->CopyFrom(stats);
    stats.SerializeToString(&stats_val);
    txn->put(stats_key, stats_val);
    VLOG_DEBUG << "update tablet stats tablet_id=" << tablet_id << " key=" << hex(stats_key)
               << " stats=" << proto_to_json(stats);
    //==========================================================================
    //                  change tmp rowset to formal rowset
    //==========================================================================
    if (schema_change.txn_ids().empty() || schema_change.output_versions().empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty txn_ids or output_versions";
        return;
    }
    for (size_t i = 0; i < schema_change.txn_ids().size(); ++i) {
        auto& tmp_rowset_key = *obj_pool.add(new std::string(
                meta_rowset_tmp_key({instance_id, schema_change.txn_ids().at(i), new_tablet_id})));
        auto& tmp_rowset_val = *obj_pool.add(new std::string());
        // FIXME: async get
        ret = txn->get(tmp_rowset_key, &tmp_rowset_val);
        if (ret != 0) {
            SS << "failed to get tmp rowset key" << (ret == 1 ? " (not found)" : "")
               << ", tablet_id=" << new_tablet_id << " tmp_rowset_key=" << hex(tmp_rowset_key);
            msg = ss.str();
            code = ret == 1 ? MetaServiceCode::UNDEFINED_ERR : MetaServiceCode::KV_TXN_GET_ERR;
            return;
        }
        auto& rowset_key = *obj_pool.add(new std::string(meta_rowset_key(
                {instance_id, new_tablet_id, schema_change.output_versions().at(i)})));
        txn->put(rowset_key, tmp_rowset_val);
        txn->remove(tmp_rowset_key);
    }

    //==========================================================================
    //                      remove schema_change job
    //==========================================================================
    recorded_job.clear_schema_change();
    auto& job_val = *obj_pool.add(new std::string());
    recorded_job.SerializeToString(&job_val);
    txn->put(job_key, job_val);
    INSTANCE_LOG(INFO) << "remove schema_change job tablet_id=" << tablet_id
                       << " key=" << hex(job_key);

    need_commit = true;
}

void MetaServiceImpl::finish_tablet_job(::google::protobuf::RpcController* controller,
                                        const ::selectdb::FinishTabletJobRequest* request,
                                        ::selectdb::FinishTabletJobResponse* response,
                                        ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(finish_tablet_job);
    std::string cloud_unique_id = request->cloud_unique_id();
    instance_id = get_instance_id(resource_mgr_, cloud_unique_id);
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        SS << "cannot find instance_id with cloud_unique_id="
           << (cloud_unique_id.empty() ? "(empty)" : cloud_unique_id);
        msg = ss.str();
        LOG(INFO) << msg;
        return;
    }
    RPC_RATE_LIMIT(finish_tablet_job)
    if (!request->has_job() ||
        (request->job().compaction().empty() && !request->job().has_schema_change())) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "no valid job specified";
        return;
    }

    ObjectPool obj_pool; // To save KVs that txn may use asynchronously
    bool need_commit = false;
    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        msg = "failed to create txn";
        return;
    }

    int64_t tablet_id = request->job().idx().tablet_id();
    if (tablet_id <= 0) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "no valid tablet_id given";
        return;
    }
    int64_t table_id = request->job().idx().table_id();
    int64_t index_id = request->job().idx().index_id();
    int64_t partition_id = request->job().idx().partition_id();
    // Get tablet id index from kv
    get_tablet_idx(code, msg, ss, ret, txn, instance_id, table_id, index_id, partition_id,
                   tablet_id);
    if (code != MetaServiceCode::OK) return;

    // TODO(gaivn): remove duplicated code with start_tablet_job()
    // Begin to process finish tablet job
    std::string job_key =
            job_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id});
    std::string job_val;
    ret = txn->get(job_key, &job_val);
    INSTANCE_LOG(INFO) << "get job tablet_id=" << tablet_id << " ret=" << ret
                       << " key=" << hex(job_key);
    if (ret != 0) {
        SS << (ret == 1 ? "job not found," : "internal error,") << " instance_id=" << instance_id
           << " tablet_id=" << tablet_id << " job=" << proto_to_json(request->job());
        msg = ss.str();
        code = ret == 1 ? MetaServiceCode::INVALID_ARGUMENT : MetaServiceCode::KV_TXN_GET_ERR;
        return;
    }
    TabletJobInfoPB recorded_job;
    recorded_job.ParseFromString(job_val);
    VLOG_DEBUG << "get tablet job, tablet_id=" << tablet_id
               << " job=" << proto_to_json(recorded_job);

    std::unique_ptr<int, std::function<void(int*)>> defer_commit(
            (int*)0x01, [&ss, &ret, &txn, &code, &msg, &need_commit](int*) {
                if (!need_commit) return;
                ret = txn->commit();
                if (ret != 0) {
                    code = ret == -1 ? MetaServiceCode::KV_TXN_CONFLICT
                                     : MetaServiceCode::KV_TXN_COMMIT_ERR;
                    ss << "failed to commit job kv, ret=" << ret;
                    msg = ss.str();
                    return;
                }
            });

    // Process compaction commit
    if (!request->job().compaction().empty()) {
        process_compaction_job(code, msg, ss, ret, txn, request, response, recorded_job,
                               instance_id, table_id, index_id, partition_id, tablet_id, job_key,
                               need_commit, obj_pool);
        return;
    }

    // Process schema change commit
    if (request->job().has_schema_change()) {
        process_schema_change_job(code, msg, ss, ret, txn, request, response, recorded_job,
                                  instance_id, table_id, index_id, partition_id, tablet_id, job_key,
                                  need_commit, obj_pool);
        return;
    }
}

#undef RPC_PREPROCESS
#undef RPC_RATE_LIMIT
#undef SS
#undef INSTANCE_LOG
} // namespace selectdb
// vim: et tw=100 ts=4 sw=4 cc=80:
