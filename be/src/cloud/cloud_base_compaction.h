#pragma once

#include <memory>

#include "olap/base_compaction.h"

namespace doris {

class CloudBaseCompaction : public BaseCompaction {
public:
    CloudBaseCompaction(TabletSharedPtr tablet);
    ~CloudBaseCompaction() override;

    Status prepare_compact() override;
    Status execute_compact_impl() override;

    void do_lease();

protected:
    Status pick_rowsets_to_compact() override;

    std::string compaction_name() const override { return "CloudBaseCompaction"; }

    Status modify_rowsets(const Merger::Statistics* stats = nullptr) override;
    void garbage_collection() override;

private:
    std::string _uuid;
    int64_t _input_segments = 0;
    // Snapshot values when pick input rowsets
    int64_t _base_compaction_cnt = 0;
    int64_t _cumulative_compaction_cnt = 0;
};

} // namespace doris
