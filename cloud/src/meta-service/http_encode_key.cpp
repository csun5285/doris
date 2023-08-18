#include <brpc/uri.h>
#include <fmt/format.h>

#include <set>
#include <sstream>
#include <string>
#include <unordered_map>

#include "common/util.h"
#include "meta-service/keys.h"
#include "meta-service/meta_service_http.h"

namespace selectdb {

HttpResponse process_http_encode_key(brpc::URI& uri) {
    // See keys.h to get all types of key, e.g: MetaRowsetKey
    // key_type -> {param1, param2 ...}
    static std::unordered_map<std::string_view, std::set<std::string>> param_set {
            {"MetaRowsetKey", {"instance_id", "tablet_id", "version"}},
            {"RecycleIndexKey", {"instance_id", "index_id"}},
            {"RecyclePartKey", {"instance_id", "partition_id"}},
    };

    std::string_view key_type = http_query(uri, "key_type");
    auto it = param_set.find(key_type);
    if (it == param_set.end()) {
        return http_json_reply(MetaServiceCode::INVALID_ARGUMENT,
                               fmt::format("key_type not supported: {}",
                                           (key_type.empty() ? "(empty)" : key_type)));
    }
    for (auto& i : it->second) {
        auto p = uri.GetQuery(i.c_str());
        if (p == nullptr || p->empty()) {
            return http_json_reply(MetaServiceCode::INVALID_ARGUMENT, i + " not given or empty");
        }
    }
    static auto format_fdb_key = [](const std::string& s) {
        std::stringstream r;
        for (size_t i = 0; i < s.size(); ++i) { if (!(i % 2)) r << "\\x"; r << s[i]; }
        return r.str();
    };

    std::string hex_key;

    if (key_type == "MetaRowsetKey") {
        MetaRowsetKeyInfo key_info {*uri.GetQuery("instance_id"),
            std::strtoll(uri.GetQuery("tablet_id")->c_str(), nullptr, 10),
            std::strtoll(uri.GetQuery("version")->c_str(), nullptr, 10)};
        hex_key = hex(meta_rowset_key(key_info));
    }
    if (key_type == "RecyclePartKey") {
        RecyclePartKeyInfo key_info {*uri.GetQuery("instance_id"),
            std::strtoll(uri.GetQuery("partition_id")->c_str(), nullptr, 10)};
        hex_key = hex(recycle_partition_key(key_info));
    }
    if (key_type == "RecycleIndexKey") {
        RecycleIndexKeyInfo key_info {*uri.GetQuery("instance_id"),
            std::strtoll(uri.GetQuery("index_id")->c_str(), nullptr, 10)};
        hex_key = hex(recycle_index_key(key_info));
    }

    // Print to ensure
    bool unicode = true;
    if (uri.GetQuery("unicode") != nullptr && *uri.GetQuery("unicode") == "false") {
        unicode = false;
    }
    std::string body = prettify_key(hex_key, unicode);
    if (body.empty()) {
        return http_json_reply(MetaServiceCode::INVALID_ARGUMENT,
                               "failed to decode encoded key, key=" + hex_key,
                               "failed to decode key, it may be malformed");
    }
    return http_text_reply(MetaServiceCode::OK, "", body + format_fdb_key(hex_key) + "\n");
}

} // namespace selectdb
// vim: et tw=80 ts=4 sw=4 cc=80:
