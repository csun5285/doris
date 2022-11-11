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

#include "olap/task/engine_alter_tablet_task.h"

#include "cloud/cloud_schema_change.h"
#include "olap/schema_change.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/thread_context.h"

namespace doris {

EngineAlterTabletTask::EngineAlterTabletTask(const TAlterTabletReqV2& request)
        : _alter_tablet_req(request) {
    _mem_tracker = std::make_shared<MemTrackerLimiter>(
            config::memory_limitation_per_thread_for_schema_change_bytes,
            fmt::format("EngineAlterTabletTask#baseTabletId={}:newTabletId={}",
                        std::to_string(_alter_tablet_req.base_tablet_id),
                        std::to_string(_alter_tablet_req.new_tablet_id)),
            StorageEngine::instance()->schema_change_mem_tracker());
}

Status EngineAlterTabletTask::execute() {
    SCOPED_ATTACH_TASK(_mem_tracker, ThreadContext::TaskType::STORAGE);
    DorisMetrics::instance()->create_rollup_requests_total->increment(1);

    Status res;
#ifdef CLOUD_MODE
    DCHECK(_alter_tablet_req.__isset.job_id);
    cloud::CloudSchemaChange cloud_sc(std::to_string(_alter_tablet_req.job_id),
                                      _alter_tablet_req.expiration);
    res = cloud_sc.process_alter_tablet(_alter_tablet_req);
#else
    res = SchemaChangeHandler::process_alter_tablet_v2(_alter_tablet_req);
#endif
    if (!res.ok()) {
        DorisMetrics::instance()->create_rollup_requests_failed->increment(1);
        return res;
    }
    return res;
} // execute


EngineAlterInvertedIndexTask::EngineAlterInvertedIndexTask(const TAlterInvertedIndexReq& alter_inverted_index_request)
        : _alter_inverted_index_req(alter_inverted_index_request) {
    _mem_tracker = std::make_shared<MemTrackerLimiter>(
            config::memory_limitation_per_thread_for_schema_change_bytes,
            fmt::format("EngineAlterInvertedIndexTask#tabletId={}",
                        std::to_string(_alter_inverted_index_req.tablet_id)),
            StorageEngine::instance()->schema_change_mem_tracker());
}

Status EngineAlterInvertedIndexTask::execute() {
    // SCOPED_ATTACH_TASK(_mem_tracker, ThreadContext::TaskType::STORAGE);
    DorisMetrics::instance()->create_rollup_requests_total->increment(1);

    Status res;
#ifdef CLOUD_MODE
    DCHECK(_alter_inverted_index_req.__isset.job_id);
    cloud::CloudSchemaChange cloud_sc(std::to_string(_alter_inverted_index_req.job_id),
                                    _alter_inverted_index_req.expiration);
    res = cloud_sc.process_alter_inverted_index(_alter_inverted_index_req);
#else
    res = SchemaChangeHandler::process_alter_inverted_index(_alter_inverted_index_req);
#endif
    if (!res.ok()) {
        LOG(WARNING) << "failed to do alter inverted index task. res=" << res
                     << " tablet_id=" << _alter_inverted_index_req.tablet_id
                     << ", schema_hash=" << _alter_inverted_index_req.schema_hash;
        DorisMetrics::instance()->create_rollup_requests_failed->increment(1);
        return res;
    }

    LOG(INFO) << "success to create new alter inverted index. res=" << res
              << " tablet_id=" << _alter_inverted_index_req.tablet_id 
              << ", schema_hash="<< _alter_inverted_index_req.schema_hash;
    return res;
} // execute

} // namespace doris
