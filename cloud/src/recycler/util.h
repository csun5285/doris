#pragma once
#include <fmt/core.h>

#include <string>

#include "gen_cpp/selectdb_cloud.pb.h"

namespace selectdb {
class TxnKv;

/**
 * Get all instances, include DELETED instance
 * @return 0 for success, otherwise error
 */
int get_all_instances(TxnKv* txn_kv, std::vector<InstanceInfoPB>& res);

/**
 *
 * @return 0 for success
 */
int prepare_instance_recycle_job(TxnKv* txn_kv, std::string_view key,
                                 const std::string& instance_id, const std::string& ip_port,
                                 int64_t interval_ms);

void finish_instance_recycle_job(TxnKv* txn_kv, std::string_view key,
                                 const std::string& instance_id, const std::string& ip_port,
                                 bool success, int64_t ctime_ms);

/**
 *
 * @return 0 for success, 1 if job should be aborted, negative for other errors
 */
int lease_instance_recycle_job(TxnKv* txn_kv, std::string_view key, const std::string& instance_id,
                               const std::string& ip_port);

inline std::string segment_path(int64_t tablet_id, const std::string& rowset_id,
                                int64_t segment_id) {
    return fmt::format("data/{}/{}_{}.dat", tablet_id, rowset_id, segment_id);
}

inline std::string inverted_index_path(int64_t tablet_id, const std::string& rowset_id,
                                       int64_t segment_id, int64_t index_id) {
    return fmt::format("data/{}/{}_{}_{}.idx", tablet_id, rowset_id, segment_id, index_id);
}

inline std::string rowset_path_prefix(int64_t tablet_id, const std::string& rowset_id) {
    return fmt::format("data/{}/{}_", tablet_id, rowset_id);
}

inline std::string tablet_path_prefix(int64_t tablet_id) {
    return fmt::format("data/{}/", tablet_id);
}

} // namespace selectdb
