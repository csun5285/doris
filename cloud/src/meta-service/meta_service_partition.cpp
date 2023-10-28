#include <gen_cpp/selectdb_cloud.pb.h>

#include "common/logging.h"
#include "meta-service/keys.h"
#include "meta-service/meta_service_helper.h"
#include "meta_service.h"

namespace selectdb {

// ATTN: xxx_id MUST NOT be reused
//
//              UNKNOWN
//                 |
//      +----------+---------+
//      |                    |
// (prepare_xxx)         (drop_xxx)
//      |                    |
//      v                    v
//   PREPARED--(drop_xxx)-->DROPPED
//      |                    |
//      |----------+---------+
//      |          |
//      |  (begin_recycle_xxx)
//      |          |
// (commit_xxx)    v
//      |      RECYCLING              RECYCLING --(drop_xxx)-> RECYCLING
//      |          |
//      |  (finish_recycle_xxx)       UNKNOWN --(commit_xxx)-> UNKNOWN
//      |          |                           if xxx exists
//      +----------+
//                 |
//                 v
//              UNKNOWN

// Return 0 if exists, 1 if not exists, otherwise error
static int index_exists(Transaction* txn, const std::string& instance_id,
                        const ::selectdb::IndexRequest* req) {
    auto tablet_key = meta_tablet_key({instance_id, req->table_id(), req->index_ids(0), 0, 0});
    auto tablet_key_end =
            meta_tablet_key({instance_id, req->table_id(), req->index_ids(0), INT64_MAX, 0});
    std::unique_ptr<RangeGetIterator> it;

    int ret = txn->get(tablet_key, tablet_key_end, &it, false, 1);
    if (ret != 0) {
        LOG_WARNING("failed to get kv");
        return ret;
    }
    return it->has_next() ? 0 : 1;
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
        return;
    }
    AnnotateTag tag_instance_id("instance_id", instance_id);

    RPC_RATE_LIMIT(prepare_index)

    if (request->index_ids().empty() || !request->has_table_id()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty index_ids or table_id";
        return;
    }

    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        msg = "failed to create txn";
        return;
    }
    ret = index_exists(txn.get(), instance_id, request);
    // If index has existed, this might be a stale request
    if (ret == 0) {
        code = MetaServiceCode::ALREADY_EXISTED;
        msg = "index already existed";
        return;
    }
    if (ret != 1) {
        code = MetaServiceCode::UNDEFINED_ERR;
        msg = "failed to check index existence";
        return;
    }

    std::string to_save_val;
    {
        RecycleIndexPB pb;
        pb.set_table_id(request->table_id());
        pb.set_creation_time(::time(nullptr));
        pb.set_expiration(request->expiration());
        pb.set_state(RecycleIndexPB::PREPARED);
        pb.SerializeToString(&to_save_val);
    }
    for (auto index_id : request->index_ids()) {
        auto key = recycle_index_key({instance_id, index_id});
        std::string val;
        ret = txn->get(key, &val);
        if (ret == 1) { // UNKNOWN
            LOG_INFO("put recycle index").tag("key", hex(key));
            txn->put(key, to_save_val);
            continue;
        }
        if (ret != 0) {
            code = MetaServiceCode::KV_TXN_GET_ERR;
            msg = "failed to get kv";
            LOG_WARNING(msg);
            return;
        }
        RecycleIndexPB pb;
        if (!pb.ParseFromString(val)) {
            code = MetaServiceCode::PROTOBUF_PARSE_ERR;
            msg = "malformed recycle index value";
            LOG_WARNING(msg).tag("index_id", index_id);
            return;
        }
        if (pb.state() != RecycleIndexPB::PREPARED) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = fmt::format("invalid recycle index state: {}",
                              RecycleIndexPB::State_Name(pb.state()));
            return;
        }
        // else, duplicate request, OK
    }
    ret = txn->commit();
    if (ret != 0) {
        code = ret == -1 ? MetaServiceCode::KV_TXN_CONFLICT : MetaServiceCode::KV_TXN_COMMIT_ERR;
        msg = fmt::format("failed to commit txn: {}", ret);
        return;
    }
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
        return;
    }
    RPC_RATE_LIMIT(commit_index)

    if (request->index_ids().empty() || !request->has_table_id()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty index_ids or table_id";
        return;
    }

    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        msg = "failed to create txn";
        return;
    }

    for (auto index_id : request->index_ids()) {
        auto key = recycle_index_key({instance_id, index_id});
        std::string val;
        ret = txn->get(key, &val);
        if (ret == 1) { // UNKNOWN
            ret = index_exists(txn.get(), instance_id, request);
            // If index has existed, this might be a deplicate request
            if (ret == 0) {
                return; // Index committed, OK
            }
            if (ret != 1) {
                code = MetaServiceCode::UNDEFINED_ERR;
                msg = "failed to check index existence";
                return;
            }
            // Index recycled
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = "index has been recycled";
            return;
        }
        if (ret != 0) {
            code = MetaServiceCode::KV_TXN_GET_ERR;
            msg = "failed to get kv";
            LOG_WARNING(msg);
            return;
        }
        RecycleIndexPB pb;
        if (!pb.ParseFromString(val)) {
            code = MetaServiceCode::PROTOBUF_PARSE_ERR;
            msg = "malformed recycle index value";
            LOG_WARNING(msg).tag("index_id", index_id);
            return;
        }
        if (pb.state() != RecycleIndexPB::PREPARED) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = fmt::format("invalid recycle index state: {}",
                              RecycleIndexPB::State_Name(pb.state()));
            return;
        }
        LOG_INFO("remove recycle index").tag("key", hex(key));
        txn->remove(key);
    }
    ret = txn->commit();
    if (ret != 0) {
        code = ret == -1 ? MetaServiceCode::KV_TXN_CONFLICT : MetaServiceCode::KV_TXN_COMMIT_ERR;
        msg = fmt::format("failed to commit txn: {}", ret);
        return;
    }
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
        return;
    }
    RPC_RATE_LIMIT(drop_index)

    if (request->index_ids().empty() || !request->has_table_id()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty index_ids or table_id";
        return;
    }

    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        msg = "failed to create txn";
        return;
    }

    std::string to_save_val;
    {
        RecycleIndexPB pb;
        pb.set_table_id(request->table_id());
        pb.set_creation_time(::time(nullptr));
        pb.set_expiration(request->expiration());
        pb.set_state(RecycleIndexPB::DROPPED);
        pb.SerializeToString(&to_save_val);
    }
    bool need_commit = false;
    for (auto index_id : request->index_ids()) {
        auto key = recycle_index_key({instance_id, index_id});
        std::string val;
        ret = txn->get(key, &val);
        if (ret == 1) { // UNKNOWN
            LOG_INFO("put recycle index").tag("key", hex(key));
            txn->put(key, to_save_val);
            need_commit = true;
            continue;
        }
        if (ret != 0) {
            code = MetaServiceCode::KV_TXN_GET_ERR;
            msg = "failed to get kv";
            LOG_WARNING(msg);
            return;
        }
        RecycleIndexPB pb;
        if (!pb.ParseFromString(val)) {
            code = MetaServiceCode::PROTOBUF_PARSE_ERR;
            msg = "malformed recycle index value";
            LOG_WARNING(msg).tag("index_id", index_id);
            return;
        }
        switch (pb.state()) {
        case RecycleIndexPB::PREPARED:
            LOG_INFO("put recycle index").tag("key", hex(key));
            txn->put(key, to_save_val);
            need_commit = true;
            break;
        case RecycleIndexPB::DROPPED:
        case RecycleIndexPB::RECYCLING:
            break;
        default:
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = fmt::format("invalid recycle index state: {}",
                              RecycleIndexPB::State_Name(pb.state()));
            return;
        }
    }
    if (!need_commit) return;
    ret = txn->commit();
    if (ret != 0) {
        code = ret == -1 ? MetaServiceCode::KV_TXN_CONFLICT : MetaServiceCode::KV_TXN_COMMIT_ERR;
        msg = fmt::format("failed to commit txn: {}", ret);
        return;
    }
}

// Return 0 if exists, 1 if not exists, otherwise error
static int partition_exists(Transaction* txn, const std::string& instance_id,
                            const ::selectdb::PartitionRequest* req) {
    auto tablet_key = meta_tablet_key(
            {instance_id, req->table_id(), req->index_ids(0), req->partition_ids(0), 0});
    auto tablet_key_end = meta_tablet_key(
            {instance_id, req->table_id(), req->index_ids(0), req->partition_ids(0), INT64_MAX});
    std::unique_ptr<RangeGetIterator> it;

    int ret = txn->get(tablet_key, tablet_key_end, &it, false, 1);
    if (ret != 0) {
        LOG_WARNING("failed to get kv");
        return ret;
    }
    return it->has_next() ? 0 : 1;
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
        return;
    }
    AnnotateTag tag_instance_id("instance_id", instance_id);

    RPC_RATE_LIMIT(prepare_partition)

    if (request->partition_ids().empty() || request->index_ids().empty() ||
        !request->has_table_id()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty partition_ids or index_ids or table_id";
        return;
    }

    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        msg = "failed to create txn";
        return;
    }
    ret = partition_exists(txn.get(), instance_id, request);
    // If index has existed, this might be a stale request
    if (ret == 0) {
        code = MetaServiceCode::ALREADY_EXISTED;
        msg = "index already existed";
        return;
    }
    if (ret != 1) {
        code = MetaServiceCode::UNDEFINED_ERR;
        msg = "failed to check index existence";
        return;
    }

    std::string to_save_val;
    {
        RecyclePartitionPB pb;
        if (request->db_id() > 0) pb.set_db_id(request->db_id());
        pb.set_table_id(request->table_id());
        *pb.mutable_index_id() = request->index_ids();
        pb.set_creation_time(::time(nullptr));
        pb.set_expiration(request->expiration());
        pb.set_state(RecyclePartitionPB::PREPARED);
        pb.SerializeToString(&to_save_val);
    }
    for (auto part_id : request->partition_ids()) {
        auto key = recycle_partition_key({instance_id, part_id});
        std::string val;
        ret = txn->get(key, &val);
        if (ret == 1) { // UNKNOWN
            LOG_INFO("put recycle partition").tag("key", hex(key));
            txn->put(key, to_save_val);
            continue;
        }
        if (ret != 0) {
            code = MetaServiceCode::KV_TXN_GET_ERR;
            msg = "failed to get kv";
            LOG_WARNING(msg);
            return;
        }
        RecyclePartitionPB pb;
        if (!pb.ParseFromString(val)) {
            code = MetaServiceCode::PROTOBUF_PARSE_ERR;
            msg = "malformed recycle partition value";
            LOG_WARNING(msg).tag("partition_id", part_id);
            return;
        }
        if (pb.state() != RecyclePartitionPB::PREPARED) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = fmt::format("invalid recycle index state: {}",
                              RecyclePartitionPB::State_Name(pb.state()));
            return;
        }
        // else, duplicate request, OK
    }
    ret = txn->commit();
    if (ret != 0) {
        code = ret == -1 ? MetaServiceCode::KV_TXN_CONFLICT : MetaServiceCode::KV_TXN_COMMIT_ERR;
        msg = fmt::format("failed to commit txn: {}", ret);
        return;
    }
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
        return;
    }
    RPC_RATE_LIMIT(commit_partition)

    if (request->partition_ids().empty() || !request->has_table_id()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty partition_ids or index_ids or table_id";
        return;
    }

    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        msg = "failed to create txn";
        return;
    }

    for (auto part_id : request->partition_ids()) {
        auto key = recycle_partition_key({instance_id, part_id});
        std::string val;
        ret = txn->get(key, &val);
        if (ret == 1) { // UNKNOWN
            // Compatible with requests without `index_ids`
            if (!request->index_ids().empty()) {
                ret = partition_exists(txn.get(), instance_id, request);
                // If partition has existed, this might be a deplicate request
                if (ret == 0) {
                    return; // Partition committed, OK
                }
                if (ret != 1) {
                    code = MetaServiceCode::UNDEFINED_ERR;
                    msg = "failed to check partition existence";
                    return;
                }
            }
            // Index recycled
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = "partition has been recycled";
            return;
        }
        if (ret != 0) {
            code = MetaServiceCode::KV_TXN_GET_ERR;
            msg = "failed to get kv";
            LOG_WARNING(msg);
            return;
        }
        RecyclePartitionPB pb;
        if (!pb.ParseFromString(val)) {
            code = MetaServiceCode::PROTOBUF_PARSE_ERR;
            msg = "malformed recycle partition value";
            LOG_WARNING(msg).tag("partition_id", part_id);
            return;
        }
        if (pb.state() != RecyclePartitionPB::PREPARED) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = fmt::format("invalid recycle partition state: {}",
                              RecyclePartitionPB::State_Name(pb.state()));
            return;
        }
        LOG_INFO("remove recycle partition").tag("key", hex(key));
        txn->remove(key);
    }
    ret = txn->commit();
    if (ret != 0) {
        code = ret == -1 ? MetaServiceCode::KV_TXN_CONFLICT : MetaServiceCode::KV_TXN_COMMIT_ERR;
        msg = fmt::format("failed to commit txn: {}", ret);
        return;
    }
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
        return;
    }
    RPC_RATE_LIMIT(drop_partition)

    if (request->partition_ids().empty() || request->index_ids().empty() ||
        !request->has_table_id()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty partition_ids or index_ids or table_id";
        return;
    }

    std::unique_ptr<Transaction> txn;
    ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        msg = "failed to create txn";
        return;
    }
    std::string to_save_val;
    {
        RecyclePartitionPB pb;
        if (request->db_id() > 0) pb.set_db_id(request->db_id());
        pb.set_table_id(request->table_id());
        *pb.mutable_index_id() = request->index_ids();
        pb.set_creation_time(::time(nullptr));
        pb.set_expiration(request->expiration());
        pb.set_state(RecyclePartitionPB::DROPPED);
        pb.SerializeToString(&to_save_val);
    }
    bool need_commit = false;
    for (auto part_id : request->partition_ids()) {
        auto key = recycle_partition_key({instance_id, part_id});
        std::string val;
        ret = txn->get(key, &val);
        if (ret == 1) { // UNKNOWN
            LOG_INFO("put recycle partition").tag("key", hex(key));
            txn->put(key, to_save_val);
            need_commit = true;
            continue;
        }
        if (ret != 0) {
            code = MetaServiceCode::KV_TXN_GET_ERR;
            msg = "failed to get kv";
            LOG_WARNING(msg);
            return;
        }
        RecyclePartitionPB pb;
        if (!pb.ParseFromString(val)) {
            code = MetaServiceCode::PROTOBUF_PARSE_ERR;
            msg = "malformed recycle partition value";
            LOG_WARNING(msg).tag("partition_id", part_id);
            return;
        }
        switch (pb.state()) {
        case RecyclePartitionPB::PREPARED:
            LOG_INFO("put recycle partition").tag("key", hex(key));
            txn->put(key, to_save_val);
            need_commit = true;
            break;
        case RecyclePartitionPB::DROPPED:
        case RecyclePartitionPB::RECYCLING:
            break;
        default:
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = fmt::format("invalid recycle partition state: {}",
                              RecyclePartitionPB::State_Name(pb.state()));
            return;
        }
    }
    if (!need_commit) return;
    ret = txn->commit();
    if (ret != 0) {
        code = ret == -1 ? MetaServiceCode::KV_TXN_CONFLICT : MetaServiceCode::KV_TXN_COMMIT_ERR;
        msg = fmt::format("failed to commit txn: {}", ret);
        return;
    }
}

} // namespace selectdb
