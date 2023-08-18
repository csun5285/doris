#pragma once

#include <brpc/uri.h>

#include <optional>
#include <string>
#include <string_view>

#include "common/util.h"
#include "gen_cpp/selectdb_cloud.pb.h"

namespace selectdb {

struct HttpResponse {
    int status_code;
    std::string msg;
    std::string body;
};

std::tuple<int, std::string_view> convert_ms_code_to_http_code(MetaServiceCode ret);

HttpResponse http_json_reply(MetaServiceCode code, const std::string& msg,
                             std::optional<std::string> body = {});

HttpResponse process_http_encode_key(brpc::URI& uri);

/// Return the query value or an empty string if not exists.
inline static std::string_view http_query(const brpc::URI& uri, const char* name) {
    return uri.GetQuery(name) ? *uri.GetQuery(name) : std::string_view();
}

inline static HttpResponse http_json_reply(const MetaServiceResponseStatus& status,
                                           std::optional<std::string> body = {}) {
    return http_json_reply(status.code(), status.msg(), body);
}

inline static HttpResponse http_json_reply_message(MetaServiceCode code, const std::string& msg,
                                                   const google::protobuf::Message& body) {
    return http_json_reply(code, msg, proto_to_json(body));
}

inline static HttpResponse http_json_reply_message(const MetaServiceResponseStatus& status,
                                                   const google::protobuf::Message& msg) {
    return http_json_reply(status, proto_to_json(msg));
}

inline static HttpResponse http_text_reply(MetaServiceCode code, const std::string& msg,
                                           const std::string& body) {
    auto [status_code, _] = convert_ms_code_to_http_code(code);
    return {status_code, msg, body};
}

inline static HttpResponse http_text_reply(const MetaServiceResponseStatus& status,
                                           const std::string& body) {
    return http_text_reply(status.code(), status.msg(), body);
}

} // namespace selectdb
