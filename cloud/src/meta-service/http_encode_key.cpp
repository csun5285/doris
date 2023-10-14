#include <brpc/uri.h>
#include <fmt/format.h>
#include <gen_cpp/selectdb_cloud.pb.h>

#include <iomanip>
#include <sstream>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/sync_point.h"
#include "common/util.h"
#include "meta-service/doris_txn.h"
#include "meta-service/keys.h"
#include "meta-service/meta_service_http.h"
#include "meta-service/meta_service_schema.h"
#include "meta-service/txn_kv.h"

namespace selectdb {

// Initializes a tuple with give params, bind runtime input params to a tuple
// like structure
// TODO(gavin): add type restriction with static_assert or concept
// clang-format off
template <typename R>
struct KeyInfoSetter {
    // Gets the result with input params
    R get() {
        R r;
        if (params.size() != std::tuple_size_v<typename R::base_type>) { return r; }
        init(r, std::make_index_sequence<std::tuple_size_v<typename R::base_type>>{});
        return r;
    }

    // Iterate over all fields and set with value
    template<typename T, size_t... I>
    void init(T&& t, std::index_sequence<I...>) {
        (set_helper<T, I>(t, params), ...);
    }

    // Set the given element in tuple with index `I`.
    // The trick is using `if constexpr` to choose compilation code statically
    // even though `std::get<I>(t)` is either `string` or `int64_t`.
    template <typename T, size_t I>
    void set_helper(T&& t, const std::vector<const std::string*>& params) {
        using base_type = typename std::remove_reference_t<T>::base_type;
        if constexpr (std::is_same_v<std::tuple_element_t<I, base_type>, std::string>) {
            std::get<I>(t) = *params[I];
        }
        if constexpr (std::is_same_v<std::tuple_element_t<I, base_type>, int64_t>) {
            std::get<I>(t) = std::strtoll(params[I]->c_str(), nullptr, 10);
        }
    }

    std::vector<const std::string*> params;
};
// clang-format on

using param_type = const std::vector<const std::string*>;

template <class ProtoType>
static std::string parse(const ValueBuf& buf) {
    ProtoType pb;
    if (!buf.to_pb(&pb)) return "";
    return proto_to_json(pb);
}

static std::string parse_txn_label(const ValueBuf& buf) {
    std::string value;
    if (buf.iters.size() != 1 || buf.iters[0]->size() != 1) {
        // FIXME(plat1ko): Hard code as we are confident that the `TxnLabel` length
        //  will not exceed the splitting threshold
        return value;
    }
    buf.iters[0]->reset();
    auto [k, v] = buf.iters[0]->next();
    if (v.size() < VERSION_STAMP_LEN) {
        return value;
    }
    int64_t txn_id = 0;
    if (0 != get_txn_id_from_fdb_ts(v.substr(v.size() - VERSION_STAMP_LEN, v.size()), &txn_id)) {
        return value;
    }
    TxnLabelPB pb;
    if (!pb.ParseFromArray(v.data(), v.size() - VERSION_STAMP_LEN)) {
        return value;
    }
    value = proto_to_json(pb);
    value += fmt::format("\ntxn_id={}", txn_id);
    return value;
}

static std::string parse_delete_bitmap(const ValueBuf& buf) {
    std::stringstream ss;
    ss << std::hex << std::setw(2) << std::setfill('0');
    for (auto&& it : buf.iters) {
        it->reset();
        while (it->has_next()) {
            auto [k, v] = it->next();
            for (auto i : v) ss << ((int16_t)i & 0xff);
        }
    }
    return ss.str();
}

static std::string parse_tablet_schema(const ValueBuf& buf) {
    doris::TabletSchemaPB pb;
    if (!parse_schema_value(buf, &pb)) return "";
    return proto_to_json(pb);
}

// See keys.h to get all types of key, e.g: MetaRowsetKeyInfo
// key_type -> {{param1, param2 ...}, encoding_func, value_parsing_func}
// clang-format off
static std::unordered_map<std::string_view,
                          std::tuple<std::vector<std::string_view>, std::function<std::string(param_type&)>, std::function<std::string(const ValueBuf&)>>> param_set {
    {"InstanceKey",                {{"instance_id"},                                                 [](param_type& p) { return instance_key(KeyInfoSetter<InstanceKeyInfo>{p}.get()); }                                      , parse<InstanceInfoPB>}}          ,
    {"TxnLabelKey",                {{"instance_id", "db_id", "label"},                               [](param_type& p) { return txn_label_key(KeyInfoSetter<TxnLabelKeyInfo>{p}.get()); }                                     , parse_txn_label}}                ,
    {"TxnInfoKey",                 {{"instance_id", "db_id", "txn_id"},                              [](param_type& p) { return txn_info_key(KeyInfoSetter<TxnInfoKeyInfo>{p}.get()); }                                       , parse<TxnInfoPB>}}               ,
    {"TxnIndexKey",                {{"instance_id", "txn_id"},                                       [](param_type& p) { return txn_index_key(KeyInfoSetter<TxnIndexKeyInfo>{p}.get()); }                                     , parse<TxnIndexPB>}}              ,
    {"TxnRunningKey",              {{"instance_id", "db_id", "txn_id"},                              [](param_type& p) { return txn_running_key(KeyInfoSetter<TxnRunningKeyInfo>{p}.get()); }                                 , parse<TxnRunningPB>}}            ,
    {"VersionKey",                 {{"instance_id", "db_id", "tbl_id", "partition_id"},              [](param_type& p) { return version_key(KeyInfoSetter<VersionKeyInfo>{p}.get()); }                                        , parse<VersionPB>}}               ,
    {"MetaRowsetKey",              {{"instance_id", "tablet_id", "version"},                         [](param_type& p) { return meta_rowset_key(KeyInfoSetter<MetaRowsetKeyInfo>{p}.get()); }                                 , parse<doris::RowsetMetaPB>}}     ,
    {"MetaRowsetTmpKey",           {{"instance_id", "txn_id", "tablet_id"},                          [](param_type& p) { return meta_rowset_tmp_key(KeyInfoSetter<MetaRowsetTmpKeyInfo>{p}.get()); }                          , parse<doris::RowsetMetaPB>}}     ,
    {"MetaTabletKey",              {{"instance_id", "table_id", "index_id", "part_id", "tablet_id"}, [](param_type& p) { return meta_tablet_key(KeyInfoSetter<MetaTabletKeyInfo>{p}.get()); }                                 , parse<doris::TabletMetaPB>}}     ,
    {"MetaTabletIdxKey",           {{"instance_id", "tablet_id"},                                    [](param_type& p) { return meta_tablet_idx_key(KeyInfoSetter<MetaTabletIdxKeyInfo>{p}.get()); }                          , parse<TabletIndexPB>}}           ,
    {"RecycleIndexKey",            {{"instance_id", "index_id"},                                     [](param_type& p) { return recycle_index_key(KeyInfoSetter<RecycleIndexKeyInfo>{p}.get()); }                             , parse<RecycleIndexPB>}}          ,
    {"RecyclePartKey",             {{"instance_id", "part_id"},                                      [](param_type& p) { return recycle_partition_key(KeyInfoSetter<RecyclePartKeyInfo>{p}.get()); }                          , parse<RecyclePartitionPB>}}      ,
    {"RecycleRowsetKey",           {{"instance_id", "tablet_id", "rowset_id"},                       [](param_type& p) { return recycle_rowset_key(KeyInfoSetter<RecycleRowsetKeyInfo>{p}.get()); }                           , parse<RecycleRowsetPB>}}         ,
    {"RecycleTxnKey",              {{"instance_id", "db_id", "txn_id"},                              [](param_type& p) { return recycle_txn_key(KeyInfoSetter<RecycleTxnKeyInfo>{p}.get()); }                                 , parse<RecycleTxnPB>}}            ,
    {"StatsTabletKey",             {{"instance_id", "table_id", "index_id", "part_id", "tablet_id"}, [](param_type& p) { return stats_tablet_key(KeyInfoSetter<StatsTabletKeyInfo>{p}.get()); }                               , parse<TabletStatsPB>}}           ,
    {"JobTabletKey",               {{"instance_id", "table_id", "index_id", "part_id", "tablet_id"}, [](param_type& p) { return job_tablet_key(KeyInfoSetter<JobTabletKeyInfo>{p}.get()); }                                   , parse<TabletJobInfoPB>}}         ,
    {"CopyJobKey",                 {{"instance_id", "stage_id", "table_id", "copy_id", "group_id"},  [](param_type& p) { return copy_job_key(KeyInfoSetter<CopyJobKeyInfo>{p}.get()); }                                       , parse<CopyJobPB>}}               ,
    {"CopyFileKey",                {{"instance_id", "stage_id", "table_id", "obj_key", "obj_etag"},  [](param_type& p) { return copy_file_key(KeyInfoSetter<CopyFileKeyInfo>{p}.get()); }                                     , parse<CopyFilePB>}}              ,
    {"RecycleStageKey",            {{"instance_id", "stage_id"},                                     [](param_type& p) { return recycle_stage_key(KeyInfoSetter<RecycleStageKeyInfo>{p}.get()); }                             , parse<RecycleStagePB>}}          ,
    {"JobRecycleKey",              {{"instance_id"},                                                 [](param_type& p) { return job_check_key(KeyInfoSetter<JobRecycleKeyInfo>{p}.get()); }                                   , parse<JobRecyclePB>}}            ,
    {"MetaSchemaKey",              {{"instance_id", "index_id", "schema_version"},                   [](param_type& p) { return meta_schema_key(KeyInfoSetter<MetaSchemaKeyInfo>{p}.get()); }                                 , parse_tablet_schema}}            ,
    {"MetaDeleteBitmap",           {{"instance_id", "tablet_id", "rowest_id", "version", "seg_id"},  [](param_type& p) { return meta_delete_bitmap_key(KeyInfoSetter<MetaDeleteBitmapInfo>{p}.get()); }                       , parse_delete_bitmap}}            ,
    {"MetaDeleteBitmapUpdateLock", {{"instance_id", "table_id", "partition_id"},                     [](param_type& p) { return meta_delete_bitmap_update_lock_key( KeyInfoSetter<MetaDeleteBitmapUpdateLockInfo>{p}.get()); }, parse<DeleteBitmapUpdateLockPB>}},
    {"MetaPendingDeleteBitmap",    {{"instance_id", "tablet_id"},                                    [](param_type& p) { return meta_pending_delete_bitmap_key( KeyInfoSetter<MetaPendingDeleteBitmapInfo>{p}.get()); }       , parse<PendingDeleteBitmapPB>}}   ,
    {"RLJobProgressKey",           {{"instance_id", "db_id", "job_id"},                              [](param_type& p) { return rl_job_progress_key_info( KeyInfoSetter<RLJobProgressKeyInfo>{p}.get()); }                    , parse<RoutineLoadProgressPB>}}   ,
    {"MetaServiceRegistryKey",     {std::vector<std::string_view> {},                                [](param_type& p) { return system_meta_service_registry_key(); }                                                         , parse<ServiceRegistryPB>}}       ,
    {"MetaServiceArnInfoKey",      {std::vector<std::string_view> {},                                [](param_type& p) { return system_meta_service_arn_info_key(); }                                                         , parse<RamUserPB>}}               ,
    {"MetaServiceEncryptionKey",   {std::vector<std::string_view> {},                                [](param_type& p) { return system_meta_service_encryption_key_info_key(); }                                              , parse<EncryptionKeyInfoPB>}}     ,
};
// clang-format on

static MetaServiceResponseStatus encode_key(const brpc::URI& uri, std::string& key) {
    MetaServiceResponseStatus status;
    status.set_code(MetaServiceCode::OK);
    std::string_view key_type = http_query(uri, "key_type");
    auto it = param_set.find(key_type);
    if (it == param_set.end()) {
        status.set_code(MetaServiceCode::INVALID_ARGUMENT);
        status.set_msg(fmt::format("key_type not supported: {}",
                                   (key_type.empty() ? "(empty)" : key_type)));
        return status;
    }
    std::remove_cv_t<param_type> params;
    params.reserve(std::get<0>(it->second).size());
    for (auto& i : std::get<0>(it->second)) {
        auto p = uri.GetQuery(i.data());
        if (p == nullptr || p->empty()) {
            status.set_code(MetaServiceCode::INVALID_ARGUMENT);
            status.set_msg(fmt::format("{} is not given or empty", i));
            return status;
        }
        params.emplace_back(p);
    }
    key = std::get<1>(it->second)(params);
    return status;
}

HttpResponse process_http_get_value(TxnKv* txn_kv, const brpc::URI& uri) {
    std::string key;
    if (auto hex_key = http_query(uri, "key"); !hex_key.empty()) {
        key = unhex(hex_key);
    } else { // Encode key from params
        auto st = encode_key(uri, key);
        if (st.code() != MetaServiceCode::OK) {
            return http_json_reply(st);
        }
    }
    std::unique_ptr<Transaction> txn;
    int ret = txn_kv->create_txn(&txn);
    if (ret != 0) [[unlikely]] {
        return http_json_reply(MetaServiceCode::KV_TXN_CREATE_ERR,
                               fmt::format("failed to create txn, ret={}", ret));
    }
    ValueBuf value;
    ret = selectdb::get(txn.get(), key, &value, true);
    if (ret == 1) {
        // FIXME: Key not found err
        return http_json_reply(MetaServiceCode::KV_TXN_GET_ERR,
                               fmt::format("kv not found, key={}", hex(key)));
    }
    if (ret != 0) {
        return http_json_reply(MetaServiceCode::KV_TXN_GET_ERR,
                               fmt::format("failed to get kv, key={}", hex(key)));
    }
    std::string_view key_type = http_query(uri, "key_type");
    auto it = param_set.find(key_type);
    if (it == param_set.end()) {
        return http_json_reply(MetaServiceCode::INVALID_ARGUMENT,
                               fmt::format("key_type not supported: {}",
                                           (key_type.empty() ? "(empty)" : key_type)));
    }
    auto readable_value = std::get<2>(it->second)(value);
    if (readable_value.empty()) [[unlikely]] {
        return http_json_reply(MetaServiceCode::PROTOBUF_PARSE_ERR,
                               fmt::format("failed to parse value, key={}", hex(key)));
    }
    return http_text_reply(MetaServiceCode::OK, "", readable_value);
}

HttpResponse process_http_encode_key(const brpc::URI& uri) {
    std::string key;
    auto st = encode_key(uri, key);
    if (st.code() != MetaServiceCode::OK) {
        return http_json_reply(st);
    }

    // Print to ensure
    bool unicode = !(uri.GetQuery("unicode") != nullptr && *uri.GetQuery("unicode") == "false");

    auto hex_key = hex(key);
    std::string body = prettify_key(hex_key, unicode);
    TEST_SYNC_POINT_CALLBACK("process_http_encode_key::empty_body", &body);
    if (body.empty()) {
        return http_json_reply(MetaServiceCode::INVALID_ARGUMENT,
                               "failed to decode encoded key, key=" + hex_key,
                               "failed to decode key, it may be malformed");
    }

    static auto format_fdb_key = [](const std::string& s) {
        std::stringstream r;
        for (size_t i = 0; i < s.size(); ++i) {
            if (!(i % 2)) r << "\\x";
            r << s[i];
        }
        return r.str();
    };

    return http_text_reply(MetaServiceCode::OK, "", body + format_fdb_key(hex_key) + "\n");
}

} // namespace selectdb
// vim: et tw=80 ts=4 sw=4 cc=80:
