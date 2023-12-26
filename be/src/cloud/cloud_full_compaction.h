#pragma once

#include <memory>

#include "olap/full_compaction.h"

namespace doris {

class CloudFullCompaction : public FullCompaction {
public:
    CloudFullCompaction(TabletSharedPtr tablet);
    ~CloudFullCompaction() override;

    Status prepare_compact() override;
    Status execute_compact_impl() override;

    void do_lease();

protected:
    Status pick_rowsets_to_compact() override;

    std::string compaction_name() const override { return "CloudFullCompaction"; }

    Status modify_rowsets(const Merger::Statistics* stats = nullptr) override;
    void garbage_collection() override;

private:
    Status _cloud_full_compaction_update_delete_bitmap(int64_t initiator);
    Status _cloud_full_compaction_calc_delete_bitmap(const RowsetSharedPtr& rowset,
                                                     const int64_t& cur_version,
                                                     const DeleteBitmapPtr& delete_bitmap);

    std::string _uuid;
    int64_t _input_segments = 0;
    // Snapshot values when pick input rowsets
    int64_t _base_compaction_cnt = 0;
    int64_t _cumulative_compaction_cnt = 0;
};

} // namespace doris
