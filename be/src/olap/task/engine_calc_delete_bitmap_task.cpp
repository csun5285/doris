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

#include "olap/task/engine_calc_delete_bitmap_task.h"

#include "cloud/meta_mgr.h"
#include "cloud/utils.h"
#include "common/status.h"
#include "olap/olap_common.h"
#include "olap/rowset/rowset.h"
#include "olap/tablet_meta.h"
#include "util/defer_op.h"

namespace doris {

EngineCalcDeleteBitmapTask::EngineCalcDeleteBitmapTask(
        TCalcDeleteBitmapRequest& cal_delete_bitmap_req, std::vector<TTabletId>* error_tablet_ids,
        std::vector<TTabletId>* succ_tablet_ids)
        : _cal_delete_bitmap_req(cal_delete_bitmap_req),
          _error_tablet_ids(error_tablet_ids),
          _succ_tablet_ids(succ_tablet_ids) {}

void EngineCalcDeleteBitmapTask::add_error_tablet_id(int64_t tablet_id, const Status& err) {
    std::lock_guard<std::mutex> lck(_mutex);
    _error_tablet_ids->push_back(tablet_id);
    if (_res.ok() || _res.is<ErrorCode::DELETE_BITMAP_LOCK_ERROR>()) {
        _res = err;
    }
}

void EngineCalcDeleteBitmapTask::add_succ_tablet_id(int64_t tablet_id) {
    std::lock_guard<std::mutex> lck(_mutex);
    _succ_tablet_ids->push_back(tablet_id);
}

Status EngineCalcDeleteBitmapTask::finish() {
    int64_t transaction_id = _cal_delete_bitmap_req.transaction_id;
    OlapStopWatch watch;
    VLOG_NOTICE << "begin to calculate delete bitmap. transaction_id=" << transaction_id;
    std::unique_ptr<ThreadPoolToken> token =
            StorageEngine::instance()->calc_tablet_delete_bitmap_task_thread_pool()->new_token(
                    ThreadPool::ExecutionMode::CONCURRENT);

    for (auto& partition : _cal_delete_bitmap_req.partitions) {
        int64_t version = partition.version;
        for (auto tablet_id : partition.tablet_ids) {
            auto tablet_calc_delete_bitmap_ptr = std::make_shared<TabletCalcDeleteBitmapTask>(
                    this, tablet_id, transaction_id, version);
            auto submit_st = token->submit_func([=]() { tablet_calc_delete_bitmap_ptr->handle(); });
            CHECK(submit_st.ok());
        }
    }
    // wait for all finished
    token->wait();

    LOG(INFO) << "finish to calculate delete bitmap on transaction."
              << "transaction_id=" << transaction_id << ", cost(us): " << watch.get_elapse_time_us()
              << ", error_tablet_size=" << _error_tablet_ids->size()
              << ", res=" << _res.to_string();
    return _res;
}

TabletCalcDeleteBitmapTask::TabletCalcDeleteBitmapTask(EngineCalcDeleteBitmapTask* engine_task,
                                                       int64_t tablet_id, int64_t transaction_id,
                                                       int64_t version)
        : _engine_calc_delete_bitmap_task(engine_task),
          _tablet_id(tablet_id),
          _transaction_id(transaction_id),
          _version(version) {}

void TabletCalcDeleteBitmapTask::handle() {
    TabletSharedPtr tablet;
    int64_t t1 = MonotonicMicros();
    cloud::tablet_mgr()->get_tablet(_tablet_id, &tablet);
    auto get_tablet_time_us = MonotonicMicros() - t1;
    if (tablet == nullptr) {
        LOG(WARNING) << "can't get tablet when calculate delete bitmap. tablet_id=" << _tablet_id;
        auto error_st = Status::Error<ErrorCode::PUSH_TABLE_NOT_EXIST>(
                "can't get tablet when calculate delete bitmap. tablet_id={}", _tablet_id);
        _engine_calc_delete_bitmap_task->add_error_tablet_id(_tablet_id, error_st);
        return;
    }
    int64_t max_version = tablet->max_version().second;
    // version is continuous when retry calculating delete bitmap
    // or calculating bitmap after compaction, so check continuity first to avoid unnecessary sync
    int64_t t2 = MonotonicMicros();
    if (_version != max_version + 1) {
        auto sync_st = tablet->cloud_sync_rowsets();
        if (sync_st.is<ErrorCode::INVALID_TABLET_STATE>()) [[unlikely]] {
            _engine_calc_delete_bitmap_task->add_succ_tablet_id(_tablet_id);
            LOG(INFO) << "tablet is under alter process, delete bitmap will be calculated "
                         "later, "
                         "tablet_id: "
                      << _tablet_id << " txn_id: " << _transaction_id
                      << ", request_version=" << _version;
            return;
        }
        if (!sync_st.ok()) {
            LOG(WARNING) << "failed to sync rowsets. tablet_id=" << _tablet_id
                         << ", txn_id=" << _transaction_id << ", status=" << sync_st;
            _engine_calc_delete_bitmap_task->add_error_tablet_id(_tablet_id, sync_st);
            return;
        }
    }
    auto sync_rowset_time_us = MonotonicMicros() - t2;
    max_version = tablet->max_version().second;
    if (_version != max_version + 1) {
        LOG(WARNING) << "version not continuous, current max version=" << max_version
                     << ", request_version=" << _version << " tablet_id=" << _tablet_id;
        auto error_st =
                Status::Error<ErrorCode::DELETE_BITMAP_LOCK_ERROR, false>("version not continuous");
        _engine_calc_delete_bitmap_task->add_error_tablet_id(_tablet_id, error_st);
        return;
    }
    RowsetSharedPtr rowset;
    DeleteBitmapPtr delete_bitmap;
    RowsetIdUnorderedSet rowset_ids;
    std::shared_ptr<PartialUpdateInfo> partial_update_info;
    int64_t txn_expiration;
    Status status = StorageEngine::instance()->delete_bitmap_txn_manager()->get_tablet_txn_info(
            _transaction_id, _tablet_id, &rowset, &delete_bitmap, &rowset_ids, &txn_expiration,
            &partial_update_info);
    if (status != Status::OK()) {
        LOG(WARNING) << "failed to get tablet txn info. tablet_id=" << _tablet_id
                     << ", txn_id=" << _transaction_id << ", status=" << status;
        _engine_calc_delete_bitmap_task->add_error_tablet_id(_tablet_id, status);
        return;
    }

    int64_t t3 = MonotonicMicros();
    rowset->set_version(Version(_version, _version));
    TabletTxnInfo txn_info;
    txn_info.rowset = rowset;
    txn_info.delete_bitmap = delete_bitmap;
    txn_info.rowset_ids = rowset_ids;
    txn_info.partial_update_info = partial_update_info;
    status = tablet->update_delete_bitmap(&txn_info, _transaction_id, txn_expiration);
    auto update_delete_bitmap_time_us = MonotonicMicros() - t3;

    if (status != Status::OK()) {
        LOG(WARNING) << "failed to calculate delete bitmap. rowset_id=" << rowset->rowset_id()
                     << ", tablet_id=" << _tablet_id << ", txn_id=" << _transaction_id
                     << ", status=" << status;
        _engine_calc_delete_bitmap_task->add_error_tablet_id(_tablet_id, status);
        return;
    }

    _engine_calc_delete_bitmap_task->add_succ_tablet_id(_tablet_id);
    LOG(INFO) << "calculate delete bitmap successfully on tablet"
              << ", table_id=" << _tablet_id << ", tablet=" << tablet->full_name()
              << ", transaction_id=" << _transaction_id << ", num_rows=" << rowset->num_rows()
              << ", get_tablet_time_us=" << get_tablet_time_us
              << ", sync_rowset_time_us=" << sync_rowset_time_us
              << ", update_delete_bitmap_time_us=" << update_delete_bitmap_time_us
              << ", res=" << status;
}
} // namespace doris
