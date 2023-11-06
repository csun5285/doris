#pragma once

#include <gen_cpp/selectdb_cloud.pb.h>

namespace selectdb {
class Transaction;

// Detached tablet stats
struct TabletStats {
    int64_t data_size = 0;
    int64_t num_rows = 0;
    int64_t num_rowsets = 0;
    int64_t num_segs = 0;
};

// Get tablet stats and detached tablet stats via `txn`. If an error occurs, `code` will be set to non OK.
// NOTE: this function returns original `TabletStatsPB` and detached tablet stats val stored in kv store,
//  MUST call `merge_tablet_stats(stats, detached_stats)` to get the real tablet stats.
void internal_get_tablet_stats(MetaServiceCode& code, std::string& msg, Transaction* txn,
                               const std::string& instance_id, const TabletIndexPB& idx,
                               TabletStatsPB& stats, TabletStats& detached_stats,
                               bool snapshot = false);

// Merge `detached_stats` `stats` to `stats`.
void merge_tablet_stats(TabletStatsPB& stats, const TabletStats& detached_stats);

// Get merged tablet stats via `txn`. If an error occurs, `code` will be set to non OK.
void internal_get_tablet_stats(MetaServiceCode& code, std::string& msg, Transaction* txn,
                               const std::string& instance_id, const TabletIndexPB& idx,
                               TabletStatsPB& stats, bool snapshot = false);

} // namespace selectdb
