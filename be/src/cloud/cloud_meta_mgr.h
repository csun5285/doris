#pragma once

#include <ranges>

#include "cloud/meta_mgr.h"
#include "olap/rowset/rowset_meta.h"

namespace doris::cloud {

class CloudMetaMgr final : public MetaMgr {
public:
    CloudMetaMgr();

    ~CloudMetaMgr() override;

    Status open() override;

    Status get_tablet_meta(int64_t tablet_id, std::shared_ptr<TabletMeta>* tablet_meta) override;

    Status sync_tablet_rowsets(Tablet* tablet, bool warmup_delta_data = false) override;

    Status prepare_rowset(const RowsetMeta* rs_meta,
                          std::shared_ptr<RowsetMeta>* existed_rs_meta = nullptr) override;

    Status commit_rowset(const RowsetMeta* rs_meta,
                         std::shared_ptr<RowsetMeta>* existed_rs_meta = nullptr) override;

    Status update_tmp_rowset(const RowsetMeta& rs_meta) override;

    Status commit_txn(StreamLoadContext* ctx, bool is_2pc) override;

    Status abort_txn(StreamLoadContext* ctx) override;

    Status precommit_txn(StreamLoadContext* ctx) override;

    Status get_s3_info(std::vector<std::tuple<std::string, S3Conf>>* s3_infos) override;

    Status prepare_tablet_job(const selectdb::TabletJobInfoPB& job,
                              selectdb::StartTabletJobResponse* res) override;

    Status commit_tablet_job(const selectdb::TabletJobInfoPB& job,
                             selectdb::FinishTabletJobResponse* res) override;

    Status abort_tablet_job(const selectdb::TabletJobInfoPB& job) override;

    Status lease_tablet_job(const selectdb::TabletJobInfoPB& job) override;

    Status update_tablet_schema(int64_t tablet_id, const TabletSchema* tablet_schema) override;

    Status update_delete_bitmap(const Tablet* tablet, int64_t lock_id, int64_t initiator,
                                DeleteBitmap* delete_bitmap) override;

    Status get_delete_bitmap_update_lock(const Tablet* tablet, int64_t lock_id,
                                         int64_t initiator) override;

    bool sync_tablet_delete_bitmap_by_cache(Tablet* tablet, int64_t old_max_version,
                                            std::ranges::range auto&& rs_metas,
                                            DeleteBitmap* delete_bitmap);

private:
    Status sync_tablet_delete_bitmap(
            Tablet* tablet, int64_t old_max_version,
            const google::protobuf::RepeatedPtrField<RowsetMetaPB>& rs_metas,
            const selectdb::TabletStatsPB& stas, const selectdb::TabletIndexPB& idx,
            DeleteBitmap* delete_bitmap);
};

} // namespace doris::cloud
