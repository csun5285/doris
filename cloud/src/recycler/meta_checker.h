
#pragma once

#include <mysql/mysql.h>

#include <atomic>
#include <condition_variable>
#include <deque>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include "gen_cpp/selectdb_cloud.pb.h"
#include "recycler/white_black_list.h"

namespace selectdb {
class TxnKv;

struct StatInfo {
    int64_t check_fe_tablet_num = 0;
    int64_t check_fdb_tablet_idx_num = 0;
    int64_t check_fdb_tablet_meta_num = 0;
    int64_t check_fdb_partition_version_num = 0;
};

class MetaChecker {
public:
    explicit MetaChecker(std::shared_ptr<TxnKv> txn_kv);
    void do_check(const std::string& host, const std::string& port, const std::string& user,
                  const std::string& password, const std::string& instance_id, std::string& msg);
    bool check_fe_meta_by_fdb(MYSQL* conn);
    bool check_fdb_by_fe_meta(MYSQL* conn);

private:
    std::shared_ptr<TxnKv> txn_kv_;
    StatInfo stat_info_;
    std::string instance_id_;
};

} // namespace selectdb
