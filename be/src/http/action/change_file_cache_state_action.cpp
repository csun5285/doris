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

#include "http/action/change_file_cache_state_action.h"

#include <fmt/core.h>

#include "io/cache/block/block_file_cache_factory.h"
#include "http/http_channel.h"
#include "http/http_request.h"

namespace doris {
const std::string FILE_CACHE_STATE = "state";

void ChangeFileCacheStateAction::handle(HttpRequest* req) {
    std::string state = req->param(FILE_CACHE_STATE);
    Status st = Status::OK();
    if (state == "close") {
        io::FileCacheFactory::instance().set_read_only();
    } else if (state == "open") {
        st = io::FileCacheFactory::instance().reload_file_cache();
    } else {
        st = Status::InvalidArgument("invalid argument {}={}", FILE_CACHE_STATE, state);
    }
    HttpChannel::send_reply(req, HttpStatus::OK, st.to_json());
}

} // namespace doris
