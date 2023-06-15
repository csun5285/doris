#pragma once

#include "cloud/meta_mgr.h"

namespace selectdb {
class MetaService_Stub;
} // namespace selectdb

namespace doris::cloud {

class CloudMetaMgr final : public MetaMgr {
public:
    CloudMetaMgr();

    ~CloudMetaMgr() override;

    Status open() override;

    Status get_tablet_meta(int64_t tablet_id, TabletMetaSharedPtr* tablet_meta) override;

    Status sync_tablet_rowsets(Tablet* tablet, bool need_download_data_async = false) override;

    Status prepare_rowset(const RowsetMetaSharedPtr& rs_meta, bool is_tmp,
                          RowsetMetaSharedPtr* existed_rs_meta = nullptr) override;

    Status commit_rowset(const RowsetMetaSharedPtr& rs_meta, bool is_tmp,
                         RowsetMetaSharedPtr* existed_rs_meta = nullptr) override;

    Status commit_txn(StreamLoadContext* ctx, bool is_2pc) override;

    Status abort_txn(StreamLoadContext* ctx) override;

    Status precommit_txn(StreamLoadContext* ctx) override;

    Status get_s3_info(std::vector<std::tuple<std::string, S3Conf>>* s3_infos) override;

    Status prepare_tablet_job(const selectdb::TabletJobInfoPB& job) override;

    Status commit_tablet_job(const selectdb::TabletJobInfoPB& job,
                             selectdb::TabletStatsPB* stats) override;

    Status abort_tablet_job(const selectdb::TabletJobInfoPB& job) override;

    Status lease_tablet_job(const selectdb::TabletJobInfoPB& job) override;

    Status update_tablet_schema(int64_t tablet_id, TabletSchemaSPtr tablet_schema) override;

    Status update_delete_bitmap(const Tablet* tablet, int64_t lock_id, int64_t initiator,
                                DeleteBitmapPtr delete_bitmap) override;

    Status get_delete_bitmap_update_lock(const Tablet* tablet, int64_t lock_id,
                                         int64_t initiator) override;

private:
    Status sync_tablet_delete_bitmap(const Tablet* tablet, int64_t old_max_version,
                                     std::vector<RowsetMetaPB> rowset_metas,
                                     DeleteBitmapPtr delete_bitmap);
    std::unique_ptr<selectdb::MetaService_Stub> _stub;
};

} // namespace doris::cloud
