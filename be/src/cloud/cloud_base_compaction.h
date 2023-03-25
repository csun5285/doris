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
    std::string compaction_name() const override { return "CloudBaseCompaction"; }

    Status update_tablet_meta(const Merger::Statistics* stats = nullptr) override;
    void garbage_collection() override;

private:
    std::string _uuid;
    int64_t _input_segments = 0;
};

std::vector<std::shared_ptr<CloudBaseCompaction>> get_base_compactions();
void push_base_compaction(std::shared_ptr<CloudBaseCompaction> compaction);
void pop_base_compaction(CloudBaseCompaction* compaction);

} // namespace doris
