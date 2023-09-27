
// clang-format off
#include "meta_service.h"

#include "common/bvars.h"
#include "common/config.h"
#include "common/encryption_util.h"
#include "common/logging.h"
#include "common/stopwatch.h"
#include "common/sync_point.h"
#include "common/string_util.h"
#include "common/util.h"
#include "meta-service/doris_txn.h"
#include "meta-service/keys.h"
#include "meta-service/meta_service_helper.h"
#include "meta-service/meta_service_schema.h"
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

MetaServiceImpl::MetaServiceImpl(std::shared_ptr<TxnKv> txn_kv,
                                 std::shared_ptr<ResourceManager> resource_mgr,
                                 std::shared_ptr<RateLimiter> rate_limiter) {
    txn_kv_ = txn_kv;
    resource_mgr_ = resource_mgr;
    rate_limiter_ = rate_limiter;
    rate_limiter_->init(this);
}

MetaServiceImpl::~MetaServiceImpl() = default;

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

    if (request->db_ids_size() == 0 || request->table_ids_size() == 0 ||
        request->table_ids_size() != request->partition_ids_size() ||
        request->db_ids_size() != request->partition_ids_size()) {
        msg = "param error, num db_ids=" + std::to_string(request->db_ids_size()) +
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

    size_t num_acquired = request->partition_ids_size();
    response->mutable_versions()->Reserve(num_acquired);
    response->mutable_db_ids()->CopyFrom(request->db_ids());
    response->mutable_table_ids()->CopyFrom(request->table_ids());
    response->mutable_partition_ids()->CopyFrom(request->partition_ids());

    while (code == MetaServiceCode::OK &&
           response->versions_size() < response->partition_ids_size()) {
        std::unique_ptr<Transaction> txn;
        ret = txn_kv_->create_txn(&txn);
        if (ret != 0) {
            msg = "failed to create txn";
            code = MetaServiceCode::KV_TXN_CREATE_ERR;
            break;
        }
        for (size_t i = response->versions_size(); i < num_acquired; ++i) {
            int64_t db_id = request->db_ids(i);
            int64_t table_id = request->table_ids(i);
            int64_t partition_id = request->partition_ids(i);
            std::string ver_key = version_key({instance_id, db_id, table_id, partition_id});

            // TODO(walter) support batch get.
            std::string ver_val;
            ret = txn->get(ver_key, &ver_val, true);
            TEST_SYNC_POINT_CALLBACK("batch_get_version_ret", &ret);
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
            } else if (ret == -2) {
                // txn too old, fallback to non-snapshot versions.
                LOG(WARNING) << "batch_get_version execution time exceeds the txn mvcc window, "
                                "fallback to acquire non-snapshot versions, partition_ids_size="
                             << request->partition_ids_size() << ", index=" << i;
                break;
            } else {
                msg = "failed to get txn";
                code = MetaServiceCode::KV_TXN_GET_ERR;
                break;
            }
        }
    }
    if (code != MetaServiceCode::OK) {
        response->clear_partition_ids();
        response->clear_table_ids();
        response->clear_versions();
    }
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
                auto schema_key =
                        meta_schema_key({instance_id, index_id, tablet_meta.schema_version()});
                put_schema_kv(code, msg, txn.get(), schema_key, tablet_meta.schema());
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
        ValueBuf val_buf;
        ret = selectdb::get(txn, key, &val_buf);
        if (ret != 0) {
            code = MetaServiceCode::KV_TXN_GET_ERR;
            msg = fmt::format("failed to get schema, err={}",
                              ret == 1 ? "not found" : "internal error");
            return;
        }
        if (!parse_schema_value(val_buf, tablet_meta->mutable_schema())) {
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
        std::string schema_key = meta_schema_key({instance_id, existed_rowset_meta.index_id(),
                                                  existed_rowset_meta.schema_version()});
        ValueBuf val_buf;
        ret = selectdb::get(txn, schema_key, &val_buf, true);
        if (ret != 0) {
            code = MetaServiceCode::KV_TXN_GET_ERR;
            msg = fmt::format("failed to get schema, schema_version={}: {}",
                              rowset_meta.schema_version(),
                              ret == 1 ? "not found" : "internal error");
            return;
        }
        if (!parse_schema_value(val_buf, existed_rowset_meta.mutable_tablet_schema())) {
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
        auto schema_key = meta_schema_key(
                {instance_id, rowset_meta.index_id(), rowset_meta.schema_version()});
        put_schema_kv(code, msg, txn.get(), schema_key, rowset_meta.tablet_schema());
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
            ValueBuf val_buf;
            ret = selectdb::get(txn.get(), key, &val_buf);
            if (ret != 0) {
                code = MetaServiceCode::KV_TXN_GET_ERR;
                msg = fmt::format(
                        "failed to get schema, schema_version={}, rowset_version=[{}-{}]: {}",
                        rowset_meta.schema_version(), rowset_meta.start_version(),
                        rowset_meta.end_version(), ret == 1 ? "not found" : "internal error");
                return;
            }
            auto schema = rowset_meta.mutable_tablet_schema();
            if (!parse_schema_value(val_buf, schema)) {
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
    int ret = txn->get(key0, key1, &it, true);
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
    int ret = txn->get(key0, key1, &it, true);
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
    std::string lock_key = meta_delete_bitmap_update_lock_key({instance_id, table_id, -1});
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

void MetaServiceImpl::get_delete_bitmap_update_lock(
        google::protobuf::RpcController* controller,
        const ::selectdb::GetDeleteBitmapUpdateLockRequest* request,
        ::selectdb::GetDeleteBitmapUpdateLockResponse* response,
        ::google::protobuf::Closure* done) {
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
    std::string lock_key = meta_delete_bitmap_update_lock_key({instance_id, table_id, -1});
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
                      << lock_info.lock_id() << " table_id=" << table_id << " now=" << now;
            lock_info.clear_initiators();
        } else if (lock_info.lock_id() != request->lock_id()) {
            ss << "already be locked. requset lock_id=" << request->lock_id()
               << " locked by lock_id=" << lock_info.lock_id() << " table_id=" << table_id
               << " now=" << now << " expiration=" << lock_info.expiration();
            msg = ss.str();
            code = MetaServiceCode::LOCK_CONFLICT;
            return;
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

} // namespace selectdb
// vim: et ts=4 sw=4 cc=80:
