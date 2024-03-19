// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "http/action/adjust_s3_rate_limit.h"

#include <string>

#include "http/http_channel.h"
#include "http/http_handler.h"
#include "http/http_request.h"
#include "util/s3_util.h"

namespace doris {
class HttpRequest;

void handle_request(HttpRequest* req) {
    auto parse_param = [&req](std::string param) {
        const auto& value = req->param(param);
        if (value.empty()) {
            auto error_msg = fmt::format("parameter {} not specified in url.", param);
            throw std::runtime_error(error_msg);
        }
        return value;
    };
    auto type = parse_param("type");
    size_t max_speed = std::stoul(parse_param("speed"));
    size_t max_burst = std::stoul(parse_param("burst"));
    size_t limit = std::stoul(parse_param("limit"));
    if (auto st = reset_s3_rate_limiter(string_to_s3_rate_limit_type(type), max_speed, max_burst,
                                        limit);
        !st.ok()) {
        throw std::runtime_error(st.to_string());
    }
}

void AdjustS3RateLimitAction::handle(HttpRequest* req) {
    try {
        handle_request(req);
        auto msg = fmt::format("adjust rate limiter successfully");
        HttpChannel::send_reply(req, msg);
    } catch (const std::exception& e) {
        HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR, e.what());
        LOG(WARNING) << "adjust log level failed, error: " << e.what();
        return;
    }
}

} // namespace doris
