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

void EngineCalcDeleteBitmapTask::add_error_tablet_id(int64_t tablet_id) {
    std::lock_guard<std::mutex> lck(_tablet_ids_mutex);
    _error_tablet_ids->push_back(tablet_id);
}

void EngineCalcDeleteBitmapTask::add_succ_tablet_id(int64_t tablet_id) {
    std::lock_guard<std::mutex> lck(_tablet_ids_mutex);
    _succ_tablet_ids->push_back(tablet_id);
}

void EngineCalcDeleteBitmapTask::wait() {
    std::unique_lock<std::mutex> lock(_tablet_finish_sleep_mutex);
    _tablet_finish_sleep_cond.wait_for(lock, std::chrono::milliseconds(10));
}

void EngineCalcDeleteBitmapTask::notify() {
    std::unique_lock<std::mutex> lock(_tablet_finish_sleep_mutex);
    _tablet_finish_sleep_cond.notify_one();
}

Status EngineCalcDeleteBitmapTask::finish() {
    Status res = Status::OK();
    int64_t transaction_id = _cal_delete_bitmap_req.transaction_id;
    OlapStopWatch watch;
    VLOG_NOTICE << "begin to calculate delete bitmap. transaction_id=" << transaction_id;

    std::atomic<int64_t> total_task_num(0);
    for (auto& partition : _cal_delete_bitmap_req.partitions) {
        int64_t version = partition.version;
        for (auto tablet_id : partition.tablet_ids) {
            TabletSharedPtr tablet;
            cloud::tablet_mgr()->get_tablet(tablet_id, &tablet);
            if (tablet == nullptr) {
                LOG(WARNING) << "can't get tablet when calculate delete bitmap. tablet_id="
                             << tablet_id;
                _error_tablet_ids->push_back(tablet_id);
                res = Status::Error<ErrorCode::PUSH_TABLE_NOT_EXIST>(
                        "can't get tablet when calculate delete bitmap. tablet_id={}", tablet_id);
                break;
            }

            auto st = tablet->cloud_sync_rowsets();
            if (!st.ok() && !st.is<ErrorCode::INVALID_TABLET_STATE>()) {
                return st;
            }
            if (st.is<ErrorCode::INVALID_TABLET_STATE>()) [[unlikely]] {
                add_succ_tablet_id(tablet->tablet_id());
                LOG(INFO)
                        << "tablet is under alter process, delete bitmap will be calculated later, "
                           "tablet_id: "
                        << tablet->tablet_id() << " txn_id: " << transaction_id
                        << ", request_version=" << version;
                continue;
            }
            int64_t max_version = tablet->max_version().second;
            if (version != max_version + 1) {
                _error_tablet_ids->push_back(tablet_id);
                res = Status::Error<ErrorCode::PUBLISH_VERSION_NOT_CONTINUOUS>(
                        "version not continuous");
                LOG(WARNING) << "version not continuous, current max version=" << max_version
                             << ", request_version=" << version
                             << " tablet_id=" << tablet->tablet_id();
                break;
            }

            total_task_num.fetch_add(1);
            auto tablet_calc_delete_bitmap_ptr = std::make_shared<TabletCalcDeleteBitmapTask>(
                    this, tablet, transaction_id, version, &total_task_num);
            auto submit_st =
                    StorageEngine::instance()
                            ->calc_tablet_delete_bitmap_task_thread_pool()
                            ->submit_func([=]() { tablet_calc_delete_bitmap_ptr->handle(); });
            CHECK(submit_st.ok());
        }
    }
    // wait for all finished
    while (total_task_num.load() != 0) {
        wait();
    }

    LOG(INFO) << "finish to calculate delete bitmap on transaction."
              << "transaction_id=" << transaction_id << ", cost(us): " << watch.get_elapse_time_us()
              << ", error_tablet_size=" << _error_tablet_ids->size() << ", res=" << res.to_string();
    return res;
}

TabletCalcDeleteBitmapTask::TabletCalcDeleteBitmapTask(EngineCalcDeleteBitmapTask* engine_task,
                                                       TabletSharedPtr tablet,
                                                       int64_t transaction_id, int64_t version,
                                                       std::atomic<int64_t>* total_task_num)
        : _engine_calc_delete_bitmap_task(engine_task),
          _tablet(tablet),
          _transaction_id(transaction_id),
          _version(version),
          _total_task_num(total_task_num) {}

void TabletCalcDeleteBitmapTask::handle() {
    Defer defer {[&] {
        if (_total_task_num->fetch_sub(1) == 1) {
            _engine_calc_delete_bitmap_task->notify();
        }
    }};
    RowsetSharedPtr rowset;
    DeleteBitmapPtr delete_bitmap;
    RowsetIdUnorderedSet rowset_ids;
    std::shared_ptr<PartialUpdateInfo> partial_update_info;
    int64_t txn_expiration;
    Status status = StorageEngine::instance()->delete_bitmap_txn_manager()->get_tablet_txn_info(
            _transaction_id, _tablet->tablet_id(), &rowset, &delete_bitmap, &rowset_ids,
            &txn_expiration, &partial_update_info);
    if (status != Status::OK()) {
        LOG(WARNING) << "failed to get tablet txn info. tablet_id=" << _tablet->tablet_id()
                     << ", txn_id=" << _transaction_id << ", status=" << status;
        _engine_calc_delete_bitmap_task->add_error_tablet_id(_tablet->tablet_id());
        return;
    }

    rowset->set_version(Version(_version, _version));
    std::unique_ptr<RowsetWriter> rowset_writer;
    _tablet->create_transient_rowset_writer(rowset, &rowset_writer, partial_update_info,
                                            txn_expiration);
    status = _tablet->update_delete_bitmap(rowset, rowset_ids, delete_bitmap, _transaction_id,
                                           rowset_writer.get());

    if (status != Status::OK()) {
        LOG(WARNING) << "failed to calculate delete bitmap. rowset_id=" << rowset->rowset_id()
                     << ", tablet_id=" << _tablet->tablet_id() << ", txn_id=" << _transaction_id
                     << ", status=" << status;
        _engine_calc_delete_bitmap_task->add_error_tablet_id(_tablet->tablet_id());
        return;
    }

    if (partial_update_info && partial_update_info->is_partial_update &&
        rowset_writer->num_rows() > 0) {
        // build rowset writer and merge transient rowset
        rowset_writer->flush();
        RowsetSharedPtr transient_rowset;
        auto st = rowset_writer->build(transient_rowset);
        if (!st.ok()) {
            LOG(WARNING) << "failed to build rowset calculate delete bitmap."
                         << " rowset_id=" << rowset->rowset_id()
                         << ", tablet_id=" << _tablet->tablet_id() << ", txn_id=" << _transaction_id
                         << ", status=" << st;
            return;
        }
        rowset->merge_rowset_meta(transient_rowset->rowset_meta());
        const auto& rowset_meta = rowset->rowset_meta();
        status = cloud::meta_mgr()->update_tmp_rowset(*rowset_meta);
        if (!status.ok()) {
            LOG(WARNING) << "failed to update the committed rowset. rowset_id="
                         << rowset->rowset_id() << ", tablet_id=" << _tablet->tablet_id()
                         << ", txn_id=" << _transaction_id << ", status=" << status;
            _engine_calc_delete_bitmap_task->add_error_tablet_id(_tablet->tablet_id());
            return;
        }
        // erase segment cache cause we will add a segment to rowset
        SegmentLoader::instance()->erase_segments(rowset->rowset_id());
    }

    _engine_calc_delete_bitmap_task->add_succ_tablet_id(_tablet->tablet_id());
    LOG(INFO) << "calculate delete bitmap successfully on tablet"
              << ", table_id=" << _tablet->table_id() << ", tablet=" << _tablet->full_name()
              << ", transaction_id=" << _transaction_id << ", num_rows=" << rowset->num_rows()
              << ", res=" << status;
}
} // namespace doris
