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

#include "http/action/compaction_action.h"

// IWYU pragma: no_include <bits/chrono.h>
#include <chrono> // IWYU pragma: keep
#include <exception>
#include <future>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <utility>

#include "cloud/olap/storage_engine.h"
#include "cloud/utils.h"
#include "gutil/strings/substitute.h"
#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/http_status.h"
#include "olap/olap_common.h"
#include "olap/base_compaction.h"
#include "olap/cumulative_compaction.h"
#include "olap/cumulative_compaction_policy.h"
#include "olap/cumulative_compaction_time_series_policy.h"
#include "olap/full_compaction.h"
#include "olap/olap_define.h"
#include "cloud/olap/storage_engine.h"
#include "olap/tablet_manager.h"
#include "util/doris_metrics.h"
#include "util/stopwatch.hpp"

namespace doris {
using namespace ErrorCode;

const static std::string HEADER_JSON = "application/json";

CompactionAction::CompactionAction(CompactionActionType ctype, ExecEnv* exec_env,
                                   TPrivilegeHier::type hier, TPrivilegeType::type ptype)
        : HttpHandlerWithAuth(exec_env, hier, ptype), _type(ctype) {}
Status CompactionAction::_check_param(HttpRequest* req, uint64_t* tablet_id) {
    std::string req_tablet_id = req->param(TABLET_ID_KEY);
    if (req_tablet_id == "") {
        return Status::OK();
    }

    try {
        *tablet_id = std::stoull(req_tablet_id);
    } catch (const std::exception& e) {
        return Status::InternalError("convert tablet_id failed, {}", e.what());
    }

    return Status::OK();
}

// for viewing the compaction status
Status CompactionAction::_handle_show_compaction(HttpRequest* req, std::string* json_result) {
    uint64_t tablet_id = 0;
    RETURN_NOT_OK_STATUS_WITH_WARN(_check_param(req, &tablet_id), "check param failed");

#ifdef CLOUD_MODE
    TabletSharedPtr tablet;
    RETURN_IF_ERROR(cloud::tablet_mgr()->get_tablet(tablet_id, &tablet));
#else
    TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id);
    if (tablet == nullptr) {
        return Status::NotFound("Tablet not found. tablet_id={}", tablet_id);
    }
#endif

    tablet->get_compaction_status(json_result);
    return Status::OK();
}

Status CompactionAction::_handle_run_compaction(HttpRequest* req, std::string* json_result) {
    // 1. param check
    // check req_tablet_id is not empty
    uint64_t tablet_id = 0;
    RETURN_NOT_OK_STATUS_WITH_WARN(_check_param(req, &tablet_id), "check param failed");

    // check compaction_type equals 'base' or 'cumulative'
    auto& compaction_type = req->param(PARAM_COMPACTION_TYPE);
    if (compaction_type != PARAM_COMPACTION_BASE &&
        compaction_type != PARAM_COMPACTION_CUMULATIVE &&
        compaction_type != PARAM_COMPACTION_FULL) {
        return Status::NotSupported("The compaction type '{}' is not supported", compaction_type);
    }

    // 2. fetch the tablet by tablet_id
#ifdef CLOUD_MODE
    TabletSharedPtr tablet;
    RETURN_IF_ERROR(cloud::tablet_mgr()->get_tablet(tablet_id, &tablet));
#else
    TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id);
    if (tablet == nullptr) {
        return Status::NotFound("Tablet not found. tablet_id={}", tablet_id);
    }
#endif

    // 3. submit compaction task
    RETURN_IF_ERROR(StorageEngine::instance()->submit_compaction_task(
            tablet, compaction_type == PARAM_COMPACTION_BASE
                            ? CompactionType::BASE_COMPACTION
                            : CompactionType::CUMULATIVE_COMPACTION));

    LOG(INFO) << "Manual compaction task is successfully triggered";
    *json_result =
            "{\"status\": \"Success\", \"msg\": \"compaction task is successfully triggered.\"}";

    return Status::OK();
}

Status CompactionAction::_handle_run_status_compaction(HttpRequest* req, std::string* json_result) {
    uint64_t tablet_id = 0;

    // check req_tablet_id is not empty
    RETURN_NOT_OK_STATUS_WITH_WARN(_check_param(req, &tablet_id), "check param failed");

    if (tablet_id == 0) {
        // overall compaction status
        RETURN_IF_ERROR(StorageEngine::instance()->get_compaction_status_json(json_result));
        return Status::OK();
    } else {
        std::string json_template = R"({
            "status" : "Success",
            "run_status" : $0,
            "msg" : "$1",
            "tablet_id" : $2,
            "compact_type" : "$3"
        })";

        std::string msg = "compaction task for this tablet is not running";
        std::string compaction_type;
        bool run_status = false;

        if (StorageEngine::instance()->has_cumu_compaction(tablet_id)) {
            msg = "compaction task for this tablet is running";
            compaction_type = "cumulative";
            run_status = true;
            *json_result =
                    strings::Substitute(json_template, run_status, msg, tablet_id, compaction_type);
            return Status::OK();
        }

        if (StorageEngine::instance()->has_base_compaction(tablet_id)) {
            msg = "compaction task for this tablet is running";
            compaction_type = "base";
            run_status = true;
            *json_result =
                    strings::Substitute(json_template, run_status, msg, tablet_id, compaction_type);
            return Status::OK();
        }
        // not running any compaction
        *json_result =
                strings::Substitute(json_template, run_status, msg, tablet_id, compaction_type);
        return Status::OK();
    }
}

void CompactionAction::handle(HttpRequest* req) {
    req->add_output_header(HttpHeaders::CONTENT_TYPE, HEADER_JSON.c_str());

    if (_type == CompactionActionType::SHOW_INFO) {
        std::string json_result;
        Status st = _handle_show_compaction(req, &json_result);
        if (!st.ok()) {
            HttpChannel::send_reply(req, HttpStatus::OK, st.to_json());
        } else {
            HttpChannel::send_reply(req, HttpStatus::OK, json_result);
        }
    } else if (_type == CompactionActionType::RUN_COMPACTION) {
        std::string json_result;
        Status st = _handle_run_compaction(req, &json_result);
        if (!st.ok()) {
            HttpChannel::send_reply(req, HttpStatus::OK, st.to_json());
        } else {
            HttpChannel::send_reply(req, HttpStatus::OK, json_result);
        }
    } else {
        std::string json_result;
        Status st = _handle_run_status_compaction(req, &json_result);
        if (!st.ok()) {
            HttpChannel::send_reply(req, HttpStatus::OK, st.to_json());
        } else {
            HttpChannel::send_reply(req, HttpStatus::OK, json_result);
        }
    }
}

} // end namespace doris
