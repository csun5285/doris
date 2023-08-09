#include "common/util.h"
#include "meta-service/keys.h"

#include "brpc/uri.h"

#include <set>
#include <sstream>
#include <string>
#include <unordered_map>

namespace selectdb {

void process_http_encode_key(std::string& response_body, int& status_code,
        brpc::URI& uri, std::string& msg, bool& keep_raw_body) {
    // See keys.h to get all types of key, e.g: MetaRowsetKey
    // key_type -> {param1, param2 ...}
    static std::unordered_map<std::string, std::set<std::string>> param_set {
        {"MetaRowsetKey", {"instance_id", "tablet_id", "version"}},
        {"RecycleIndexKey", {"instance_id", "index_id"}},
        {"RecyclePartKey", {"instance_id", "partition_id"}},
    };
    auto key_type = uri.GetQuery("key_type");
    if (key_type == nullptr || param_set.count(*key_type) < 1) {
        msg = "key_type not supported: " + (key_type->empty() ? "(empty)" : *key_type);
        response_body = msg;
        status_code = 400;
        return;
    }
    auto param = param_set.find(*key_type)->second;
    for (auto& i : param) {
        auto p = uri.GetQuery(i.c_str());
        if (p == nullptr || p->empty()) {
            msg = i + " not given or empty";
            response_body = msg;
            status_code = 400;
            return;
        }
    }
    static auto format_fdb_key = [](const std::string& s) {
        std::stringstream r;
        for (size_t i = 0; i < s.size(); ++i) { if (!(i % 2)) r << "\\x"; r << s[i]; }
        return r.str();
    };

    std::string hex_key;

    if (*key_type == "MetaRowsetKey") {
        MetaRowsetKeyInfo key_info {*uri.GetQuery("instance_id"),
            std::strtoll(uri.GetQuery("tablet_id")->c_str(), nullptr, 10),
            std::strtoll(uri.GetQuery("version")->c_str(), nullptr, 10)};
        hex_key = hex(meta_rowset_key(key_info));
    }
    if (*key_type == "RecyclePartKey") {
        RecyclePartKeyInfo key_info {*uri.GetQuery("instance_id"),
            std::strtoll(uri.GetQuery("partition_id")->c_str(), nullptr, 10)};
        hex_key = hex(recycle_partition_key(key_info));
    }
    if (*key_type == "RecycleIndexKey") {
        RecycleIndexKeyInfo key_info {*uri.GetQuery("instance_id"),
            std::strtoll(uri.GetQuery("index_id")->c_str(), nullptr, 10)};
        hex_key = hex(recycle_index_key(key_info));
    }

    // Print to ensure
    bool unicode = true;
    if (uri.GetQuery("unicode") != nullptr && *uri.GetQuery("unicode") == "false") {
        unicode = false;
    }
    response_body = prettify_key(hex_key, unicode);
    if (hex_key.empty()) {
        msg = "failed to decode encoded key, key=" + hex_key;
        response_body = "failed to decode key, it may be malformed";
        status_code = 400;
        return;
    } else {
        response_body += format_fdb_key(hex_key) + "\n";
    }
    keep_raw_body = true;
}

} // namespace selectdb
// vim: et tw=80 ts=4 sw=4 cc=80:
