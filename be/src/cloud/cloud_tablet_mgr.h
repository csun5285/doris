#pragma once

#include <atomic>
#include <memory>

#include "common/status.h"
#include "olap/lru_cache.h"
#include "olap/olap_common.h"
#include "olap/rowset/rowset.h"
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
    std::vector<std::weak_ptr<Tablet>> get_weak_tablets();

    uint64_t get_rowset_nums();
    uint64_t get_segment_nums();

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

    class TabletMap;
    std::shared_ptr<TabletMap> _tablet_map;

    std::mutex _vacuum_set_mtx;
    // record the id of tablets with stale rowsets,
    // we scan tablets in this set periodically to reclaim expired stale rowsets
    std::unordered_set<int64_t> _vacuum_set;

public:
    // When sync rowsets from ms, if it has overlap rowsets, the new rowsets data need to be downloaded
    // and after downloading, remove the old rowset and add the new rowsets into version path.
    void sumbit_overlap_rowsets(int64_t tablet_id, RowsetSharedPtr to_add,
                                std::vector<RowsetSharedPtr> to_delete);

private:
    // used for map<Version, Rowset*>
    struct VersionCmp {
        bool operator()(const Version& v1, const Version& v2) const {
            return v1.second < v2.second;
        }
    };
    struct OverlapRowsetsMgr {
        // When adding overlap rowsets, we need to prepare to download its data
        // and remove its overlap rowset after downloading done
        std::mutex mtx;
        std::condition_variable cond;
        std::unordered_map<int64_t, std::map<Version, RowsetSharedPtr, VersionCmp>>
                tablet_id_to_prepare_overlap_rowsets;
        std::unordered_map<int64_t, std::map<Version, std::vector<RowsetSharedPtr>, VersionCmp>>
                tablet_id_to_new_version_to_overlap_rowset;
        std::thread prepare_overlap_rowsets_thread;
        RowsetId downloading_rowset_id;
        bool closed {false};
        void handle_overlap_rowsets();
        std::thread consumer;
    };
    static constexpr size_t mgr_size = 32;
    std::array<OverlapRowsetsMgr, mgr_size> _ovarlap_rowsets_mgrs;
};

} // namespace doris::cloud
