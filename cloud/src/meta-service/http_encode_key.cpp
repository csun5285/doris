#include <brpc/uri.h>
#include <fmt/format.h>

#include <sstream>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/util.h"
#include "common/sync_point.h"
#include "meta-service/keys.h"
#include "meta-service/meta_service_http.h"

namespace selectdb {

// Initializes a tuple with give params, bind runtime input params to a tuple
// like structure
// TODO(gavin): add type restriction with static_assert or concept
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

using param_type = const std::vector<const std::string*>;

// See keys.h to get all types of key, e.g: MetaRowsetKeyInfo
// key_type -> {{param1, param2 ...}, encoding_func}
// clang-format off
static std::unordered_map<std::string_view, std::tuple<std::vector<std::string>, std::function<std::string(param_type&)>>> param_set {
    {"InstanceKey",                {{"instance_id"},                                                 [](param_type& p) { return hex(instance_key(KeyInfoSetter<InstanceKeyInfo>{p}.get())); }}},
    {"TxnLabelKey",                {{"instance_id", "db_id", "label"},                               [](param_type& p) { return hex(txn_label_key(KeyInfoSetter<TxnLabelKeyInfo>{p}.get())); }}},
    {"TxnInfoKey",                 {{"instance_id", "db_id", "txn_id"},                              [](param_type& p) { return hex(txn_info_key(KeyInfoSetter<TxnInfoKeyInfo>{p}.get())); }}},
    {"TxnIndexKey",                {{"instance_id", "txn_id"},                                       [](param_type& p) { return hex(txn_index_key(KeyInfoSetter<TxnIndexKeyInfo>{p}.get())); }}},
    {"TxnRunningKey",              {{"instance_id", "db_id", "txn_id"},                              [](param_type& p) { return hex(txn_running_key(KeyInfoSetter<TxnRunningKeyInfo>{p}.get())); }}},
    {"VersionKey",                 {{"instance_id", "db_id", "tbl_id", "partition_id"},              [](param_type& p) { return hex(version_key(KeyInfoSetter<VersionKeyInfo>{p}.get())); }}},
    {"MetaRowsetKey",              {{"instance_id", "tablet_id", "version"},                         [](param_type& p) { return hex(meta_rowset_key(KeyInfoSetter<MetaRowsetKeyInfo>{p}.get())); }}},
    {"MetaRowsetTmpKey",           {{"instance_id", "txn_id", "tablet_id"},                          [](param_type& p) { return hex(meta_rowset_tmp_key(KeyInfoSetter<MetaRowsetTmpKeyInfo>{p}.get())); }}},
    {"MetaTabletKey",              {{"instance_id", "table_id", "index_id", "part_id", "tablet_id"}, [](param_type& p) { return hex(meta_tablet_key(KeyInfoSetter<MetaTabletKeyInfo>{p}.get())); }}},
    {"MetaTabletIdxKey",           {{"instance_id", "tablet_id"},                                    [](param_type& p) { return hex(meta_tablet_idx_key(KeyInfoSetter<MetaTabletIdxKeyInfo>{p}.get())); }}},
    {"RecycleIndexKey",            {{"instance_id", "index_id"},                                     [](param_type& p) { return hex(recycle_index_key(KeyInfoSetter<RecycleIndexKeyInfo>{p}.get())); }}},
    {"RecyclePartKey",             {{"instance_id", "part_id"},                                      [](param_type& p) { return hex(recycle_partition_key(KeyInfoSetter<RecyclePartKeyInfo>{p}.get())); }}},
    {"RecycleRowsetKey",           {{"instance_id", "tablet_id", "rowset_id"},                       [](param_type& p) { return hex(recycle_rowset_key(KeyInfoSetter<RecycleRowsetKeyInfo>{p}.get())); }}},
    {"RecycleTxnKey",              {{"instance_id", "db_id", "txn_id"},                              [](param_type& p) { return hex(recycle_txn_key(KeyInfoSetter<RecycleTxnKeyInfo>{p}.get())); }}},
    {"StatsTabletKey",             {{"instance_id", "table_id", "index_id", "part_id", "tablet_id"}, [](param_type& p) { return hex(stats_tablet_key(KeyInfoSetter<StatsTabletKeyInfo>{p}.get())); }}},
    {"JobTabletKey",               {{"instance_id", "table_id", "index_id", "part_id", "tablet_id"}, [](param_type& p) { return hex(job_tablet_key(KeyInfoSetter<JobTabletKeyInfo>{p}.get())); }}},
    {"CopyJobKey",                 {{"instance_id", "stage_id", "table_id", "copy_id", "group_id"},  [](param_type& p) { return hex(copy_job_key(KeyInfoSetter<CopyJobKeyInfo>{p}.get())); }}},
    {"CopyFileKey",                {{"instance_id", "stage_id", "table_id", "obj_key", "obj_etag"},  [](param_type& p) { return hex(copy_file_key(KeyInfoSetter<CopyFileKeyInfo>{p}.get())); }}},
    {"RecycleStageKey",            {{"instance_id", "stage_id"},                                     [](param_type& p) { return hex(recycle_stage_key(KeyInfoSetter<RecycleStageKeyInfo>{p}.get())); }}},
    {"JobRecycleKey",              {{"instance_id"},                                                 [](param_type& p) { return hex(job_check_key(KeyInfoSetter<JobRecycleKeyInfo>{p}.get())); }}},
    {"MetaSchemaKey",              {{"instance_id", "index_id", "schema_version"},                   [](param_type& p) { return hex(meta_schema_key(KeyInfoSetter<MetaSchemaKeyInfo>{p}.get())); }}},
    {"MetaDeleteBitmap",           {{"instance_id", "tablet_id", "rowest_id", "version", "seg_id"},  [](param_type& p) { return hex(meta_delete_bitmap_key(KeyInfoSetter<MetaDeleteBitmapInfo>{p}.get())); }}},
    {"MetaDeleteBitmapUpdateLock", {{"instance_id", "table_id", "partition_id"},                     [](param_type& p) { return hex(meta_delete_bitmap_update_lock_key(KeyInfoSetter<MetaDeleteBitmapUpdateLockInfo>{p}.get())); }}},
    {"MetaPendingDeleteBitmap",    {{"instance_id", "tablet_id"},                                    [](param_type& p) { return hex(meta_pending_delete_bitmap_key(KeyInfoSetter<MetaPendingDeleteBitmapInfo>{p}.get())); }}},
    {"RLJobProgressKey",           {{"instance_id", "db_id", "job_id"},                              [](param_type& p) { return hex(rl_job_progress_key_info(KeyInfoSetter<RLJobProgressKeyInfo>{p}.get())); }}},
    {"MetaServiceRegistryKey",     {std::vector<std::string>{},                                      [](param_type& p) { return hex(system_meta_service_registry_key()); }}},
    {"MetaServiceArnInfoKey",      {std::vector<std::string>{},                                      [](param_type& p) { return hex(system_meta_service_arn_info_key()); }}},
    {"MetaServiceEncryptionKey",   {std::vector<std::string>{},                                      [](param_type& p) { return hex(system_meta_service_encryption_key_info_key()); }}},
};
// clang-format on

HttpResponse process_http_encode_key(brpc::URI& uri) {

    std::string_view key_type = http_query(uri, "key_type");
    auto it = param_set.find(key_type);
    if (it == param_set.end()) {
        return http_json_reply(MetaServiceCode::INVALID_ARGUMENT,
                               fmt::format("key_type not supported: {}",
                                           (key_type.empty() ? "(empty)" : key_type)));
    }
    std::remove_cv_t<param_type> params;
    params.reserve(std::get<0>(it->second).size());
    for (auto& i : std::get<0>(it->second)) {
        auto p = uri.GetQuery(i.c_str());
        if (p == nullptr || p->empty()) {
            return http_json_reply(MetaServiceCode::INVALID_ARGUMENT, i + " is not given or empty");
        }
        params.emplace_back(p);
    }

    std::string hex_key = std::get<1>(it->second)(params);

    // Print to ensure
    bool unicode = true;
    if (uri.GetQuery("unicode") != nullptr && *uri.GetQuery("unicode") == "false") {
        unicode = false;
    }

    std::string body = prettify_key(hex_key, unicode);
    TEST_SYNC_POINT_CALLBACK("process_http_encode_key::empty_body", &body);
    if (body.empty()) {
        return http_json_reply(MetaServiceCode::INVALID_ARGUMENT,
                               "failed to decode encoded key, key=" + hex_key,
                               "failed to decode key, it may be malformed");
    }

    static auto format_fdb_key = [](const std::string& s) {
        std::stringstream r;
        for (size_t i = 0; i < s.size(); ++i) { if (!(i % 2)) r << "\\x"; r << s[i]; }
        return r.str();
    };

    return http_text_reply(MetaServiceCode::OK, "", body + format_fdb_key(hex_key) + "\n");
}

} // namespace selectdb
// vim: et tw=80 ts=4 sw=4 cc=80:
