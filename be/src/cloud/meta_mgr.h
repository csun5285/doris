#pragma once

#include <vector>

#include "common/status.h"
#include "util/s3_util.h"

namespace selectdb {
class TabletJobInfoPB;
class StartTabletJobResponse;
class FinishTabletJobResponse;
} // namespace selectdb

namespace doris {
class StreamLoadContext;
class Tablet;
class TabletMeta;
class RowsetMeta;
class TabletSchema;
class DeleteBitmap;

namespace cloud {

class MetaMgr {
public:
    virtual ~MetaMgr() = default;

    virtual Status open() { return Status::OK(); }

    virtual Status get_tablet_meta(int64_t tablet_id, std::shared_ptr<TabletMeta>* tablet_meta) = 0;

    // If `warmup_delta_data` is true, download the new version rowset data in background
    virtual Status sync_tablet_rowsets(Tablet* tablet, bool warmup_delta_data = false) = 0;

    virtual Status prepare_rowset(const RowsetMeta* rs_meta,
                                  std::shared_ptr<RowsetMeta>* existed_rs_meta = nullptr) = 0;

    virtual Status commit_rowset(const RowsetMeta* rs_meta,
                                 std::shared_ptr<RowsetMeta>* existed_rs_meta = nullptr) = 0;

    virtual Status update_tmp_rowset(const RowsetMeta& rs_meta) = 0;

    virtual Status commit_txn(StreamLoadContext* ctx, bool is_2pc) = 0;

    virtual Status abort_txn(StreamLoadContext* ctx) = 0;

    virtual Status precommit_txn(StreamLoadContext* ctx) = 0;

    virtual Status get_s3_info(std::vector<std::tuple<std::string, S3Conf>>* s3_infos) = 0;

    virtual Status prepare_tablet_job(const selectdb::TabletJobInfoPB& job,
                                      selectdb::StartTabletJobResponse* res) = 0;

    virtual Status commit_tablet_job(const selectdb::TabletJobInfoPB& job,
                                     selectdb::FinishTabletJobResponse* res) = 0;

    virtual Status abort_tablet_job(const selectdb::TabletJobInfoPB& job) = 0;

    virtual Status lease_tablet_job(const selectdb::TabletJobInfoPB& job) = 0;

    virtual Status update_delete_bitmap(const Tablet* tablet, int64_t lock_id, int64_t initiator,
                                        DeleteBitmap* delete_bitmap) = 0;

    virtual Status get_delete_bitmap_update_lock(const Tablet* tablet, int64_t lock_id,
                                                 int64_t initiator) = 0;

    virtual Status update_tablet_schema(int64_t tablet_id, const TabletSchema* tablet_schema) = 0;
};

} // namespace cloud
} // namespace doris
