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
#include "olap/base_compaction.h"
#include "olap/cumulative_compaction.h"
#include "olap/cumulative_compaction_policy.h"
#include "olap/cumulative_compaction_time_series_policy.h"
#include "olap/full_compaction.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/single_replica_compaction.h"
#include "olap/tablet_manager.h"
#include "util/doris_metrics.h"
#include "util/stopwatch.hpp"

namespace doris {
using namespace ErrorCode;

const static std::string HEADER_JSON = "application/json";

CompactionAction::CompactionAction(CompactionActionType ctype, ExecEnv* exec_env,
                                   TPrivilegeHier::type hier, TPrivilegeType::type ptype)
        : HttpHandlerWithAuth(exec_env, hier, ptype), _type(ctype) {}
Status CompactionAction::_check_param(HttpRequest* req, uint64_t* tablet_id, uint64_t* table_id) {
    // req tablet id and table id, we have to set only one of them.
    std::string req_tablet_id = req->param(TABLET_ID_KEY);
    std::string req_table_id = req->param(TABLE_ID_KEY);
    if (req_tablet_id == "") {
        if (req_table_id == "") {
            // both tablet id and table id are empty, return error.
            return Status::InternalError(
                    "tablet id and table id can not be empty at the same time!");
        } else {
            try {
                *table_id = std::stoull(req_table_id);
            } catch (const std::exception& e) {
                return Status::InternalError("convert table_id failed, {}", e.what());
            }
            return Status::OK();
        }
    } else {
        if (req_table_id == "") {
            try {
                *tablet_id = std::stoull(req_tablet_id);
            } catch (const std::exception& e) {
                return Status::InternalError("convert tablet_id failed, {}", e.what());
            }
            return Status::OK();
        } else {
            // both tablet id and table id are not empty, return err.
            return Status::InternalError("tablet id and table id can not be set at the same time!");
        }
    }
}

/// retrieve specific id from req
Status CompactionAction::_check_param(HttpRequest* req, uint64_t* id_param,
                                      const std::string param_name) {
    std::string req_id_param = req->param(param_name);
    if (req_id_param != "") {
        try {
            *id_param = std::stoull(req_id_param);
        } catch (const std::exception& e) {
            return Status::InternalError("convert {} failed, {}", param_name, e.what());
        }
    }

    return Status::OK();
}

// for viewing the compaction status
Status CompactionAction::_handle_show_compaction(HttpRequest* req, std::string* json_result) {
    uint64_t tablet_id = 0;
    RETURN_NOT_OK_STATUS_WITH_WARN(_check_param(req, &tablet_id, TABLET_ID_KEY),
                                   "check param failed");
    if (tablet_id == 0) {
        return Status::InternalError("check param failed: missing tablet_id");
    }

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
    // check req_tablet_id or req_table_id is not empty and can not be set together.
    uint64_t tablet_id = 0;
    uint64_t table_id = 0;
    RETURN_NOT_OK_STATUS_WITH_WARN(_check_param(req, &tablet_id, &table_id), "check param failed");

    // check compaction_type equals 'base' or 'cumulative'
    auto& compaction_type = req->param(PARAM_COMPACTION_TYPE);
    if (compaction_type != PARAM_COMPACTION_BASE &&
        compaction_type != PARAM_COMPACTION_CUMULATIVE &&
        compaction_type != PARAM_COMPACTION_FULL) {
        return Status::NotSupported("The compaction type '{}' is not supported", compaction_type);
    }

// <<<<<<< ours
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
            tablet, compaction_type == PARAM_COMPACTION_BASE ? CompactionType::BASE_COMPACTION
                    : compaction_type == PARAM_COMPACTION_CUMULATIVE
                            ? CompactionType::CUMULATIVE_COMPACTION
                            : CompactionType::FULL_COMPACTION));

    // =======
    //     if (tablet_id == 0 && table_id != 0) {
    //         std::vector<TabletSharedPtr> tablet_vec =
    //                 StorageEngine::instance()->tablet_manager()->get_all_tablet(
    //                         [table_id](Tablet* tablet) -> bool {
    //                             return tablet->get_table_id() == table_id;
    //                         });
    //         for (const auto& tablet : tablet_vec) {
    //             StorageEngine::instance()->submit_compaction_task(
    //                     tablet, CompactionType::FULL_COMPACTION, false);
    //         }
    //     } else {
    //         // 2. fetch the tablet by tablet_id
    //         TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id);
    //         if (tablet == nullptr) {
    //             return Status::NotFound("Tablet not found. tablet_id={}", tablet_id);
    //         }
    //
    //         // 3. execute compaction task
    //         std::packaged_task<Status()> task([this, tablet, compaction_type]() {
    //             return _execute_compaction_callback(tablet, compaction_type);
    //         });
    //         std::future<Status> future_obj = task.get_future();
    //         std::thread(std::move(task)).detach();
    //
    //         // 4. wait for result for 2 seconds by async
    //         std::future_status status = future_obj.wait_for(std::chrono::seconds(2));
    //         if (status == std::future_status::ready) {
    //             // fetch execute result
    //             Status olap_status = future_obj.get();
    //             if (!olap_status.ok()) {
    //                 return olap_status;
    //             }
    //         } else {
    //             LOG(INFO) << "Manual compaction task is timeout for waiting "
    //                       << (status == std::future_status::timeout);
    //         }
    //     }
    // >>>>>>> theirs
    LOG(INFO) << "Manual compaction task is successfully triggered";
    *json_result =
            "{\"status\": \"Success\", \"msg\": \"compaction task is successfully triggered. Table "
            "id: " +
            std::to_string(table_id) + ". Tablet id: " + std::to_string(tablet_id) + "\"}";
    return Status::OK();
}

Status CompactionAction::_handle_run_status_compaction(HttpRequest* req, std::string* json_result) {
    uint64_t tablet_id = 0;
    RETURN_NOT_OK_STATUS_WITH_WARN(_check_param(req, &tablet_id, TABLET_ID_KEY),
                                   "check param failed");

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

        if (StorageEngine::instance()->has_full_compaction(tablet_id)) {
            msg = "compaction task for this tablet is running";
            compaction_type = "full";
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

#ifndef CLOUD_MODE
Status CompactionAction::_execute_compaction_callback(TabletSharedPtr tablet,
                                                      const std::string& compaction_type,
                                                      bool fetch_from_remote) {
    MonotonicStopWatch timer;
    timer.start();

    std::shared_ptr<CumulativeCompactionPolicy> cumulative_compaction_policy =
            CumulativeCompactionPolicyFactory::create_cumulative_compaction_policy(
                    tablet->tablet_meta()->compaction_policy());
    if (tablet->get_cumulative_compaction_policy() == nullptr) {
        tablet->set_cumulative_compaction_policy(cumulative_compaction_policy);
    }
    Status res = Status::OK();
    if (compaction_type == PARAM_COMPACTION_BASE) {
        BaseCompaction base_compaction(tablet);
        res = base_compaction.compact();
        if (!res) {
            if (res.is<BE_NO_SUITABLE_VERSION>()) {
                // Ignore this error code.
                VLOG_NOTICE << "failed to init base compaction due to no suitable version, tablet="
                            << tablet->full_name();
            } else {
                DorisMetrics::instance()->base_compaction_request_failed->increment(1);
                LOG(WARNING) << "failed to init base compaction. res=" << res
                             << ", tablet=" << tablet->full_name();
            }
        }
    } else if (compaction_type == PARAM_COMPACTION_CUMULATIVE) {
        if (fetch_from_remote) {
            SingleReplicaCompaction single_compaction(tablet,
                                                      CompactionType::CUMULATIVE_COMPACTION);
            res = single_compaction.compact();
            if (!res) {
                LOG(WARNING) << "failed to do single compaction. res=" << res
                             << ", table=" << tablet->tablet_id();
            }
        } else {
            CumulativeCompaction cumulative_compaction(tablet);
            res = cumulative_compaction.compact();
            if (!res) {
                if (res.is<CUMULATIVE_NO_SUITABLE_VERSION>()) {
                    // Ignore this error code.
                    VLOG_NOTICE
                            << "failed to init cumulative compaction due to no suitable version,"
                            << "tablet=" << tablet->tablet_id();
                } else {
                    DorisMetrics::instance()->cumulative_compaction_request_failed->increment(1);
                    LOG(WARNING) << "failed to do cumulative compaction. res=" << res
                                 << ", table=" << tablet->tablet_id();
                }
            }
        }
    } else if (compaction_type == PARAM_COMPACTION_FULL) {
        FullCompaction full_compaction(tablet);
        res = full_compaction.compact();
        if (!res) {
            if (res.is<FULL_NO_SUITABLE_VERSION>()) {
                // Ignore this error code.
                VLOG_NOTICE << "failed to init full compaction due to no suitable version,"
                            << "tablet=" << tablet->full_name();
            } else {
                LOG(WARNING) << "failed to do full compaction. res=" << res
                             << ", table=" << tablet->full_name();
            }
        }
    }
    timer.stop();
    LOG(INFO) << "Manual compaction task finish, status=" << res
              << ", compaction_use_time=" << timer.elapsed_time() / 1000000 << "ms";
    return res;
}
#endif

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
