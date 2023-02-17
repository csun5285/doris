#pragma once

#include <memory>

#include "common/status.h"
#include "olap/lru_cache.h"
#include "olap/tablet.h"

namespace doris::cloud {

class CloudTabletMgr {
public:
    CloudTabletMgr();
    ~CloudTabletMgr();

    Status get_tablet(int64_t tablet_id, TabletSharedPtr* tablet);

    void erase_tablet(int64_t tablet_id);

    void vacuum_stale_rowsets();

    // MUST add tablet to vacuum set if it has stale rowsets.
    void add_to_vacuum_set(int64_t tablet_id);

    // Return weak ptr of all cached tablets.
    // We return weak ptr to avoid extend lifetime of tablets that are no longer cached.
    static std::vector<std::weak_ptr<Tablet>> get_weak_tablets();

    static uint64_t get_rowset_nums();
    static uint64_t get_segment_nums();

    void sync_tablets();

    /**
     * Gets top N tablets that are considered to be compacted first
     *
     * @param n max number of tablets to get, all of them are comapction enabled
     * @param filter_out a filter takes a tablet and return bool to check
     *                   whether skipping the tablet, true for skip
     * @param tablets output param
     * @param max_score output param, max score of existed tablets
     * @return status of this call
     */
    Status get_topn_tablets_to_compact(int n, CompactionType compaction_type,
                                       const std::function<bool(Tablet*)>& filter_out,
                                       std::vector<TabletSharedPtr>* tablets, int64_t* max_score);

private:
    std::unique_ptr<Cache> _cache;

    std::mutex _vacuum_set_mtx;
    // record the id of tablets with stale rowsets,
    // we scan tablets in this set periodically to reclaim expired stale rowsets
    std::unordered_set<int64_t> _vacuum_set;
};

} // namespace doris::cloud
