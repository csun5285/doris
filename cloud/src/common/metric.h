#pragma once

#include "common/bvars.h"
#include "meta-service/txn_kv.h"
#include <rapidjson/document.h>
#include <rapidjson/encodings.h>
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace selectdb {

// The format of the output is shown in "test/fdb_metric_example.json"
static const std::string FDB_STATUS_KEY = "\xff\xff/status/json";

static std::string get_fdb_status(std::shared_ptr<TxnKv> txn_kv) {
    std::unique_ptr<Transaction> txn;
    int ret = txn_kv->create_txn(&txn);
    if (ret != 0) {
        LOG(WARNING) << "failed to create_txn, ret=" << ret;
        return "";
    }
    std::string status_val;
    ret = txn->get(FDB_STATUS_KEY, &status_val);
    if (ret < 0) {
        LOG(WARNING) << "failed to get FDB_STATUS_KEY, ret=" << ret;
        return "";
    }
    return status_val;
}

// The format of fdb status details:
//
// Configuration:
//   Redundancy mode        - double
//   Storage engine         - ssd-2
//   Coordinators           - 3
//   Usable Regions         - 1

// Cluster:
//   FoundationDB processes - 15
//   Zones                  - 3
//   Machines               - 3
//   Memory availability    - 2.9 GB per process on machine with least available
//                            >>>>> (WARNING: 4.0 GB recommended) <<<<<
//   Retransmissions rate   - 3 Hz
//   Fault Tolerance        - 1 machines
//   Server time            - 02/16/23 16:48:14

// Data:
//   Replication health     - Healthy
//   Moving data            - 0.000 GB
//   Sum of key-value sizes - 4.317 GB
//   Disk space used        - 11.493 GB

// Operating space:
//   Storage server         - 462.8 GB free on most full server
//   Log server             - 462.8 GB free on most full server

// Workload:
//   Read rate              - 84 Hz
//   Write rate             - 4 Hz
//   Transactions started   - 222 Hz
//   Transactions committed - 4 Hz
//   Conflict rate          - 0 Hz

// Backup and DR:
//   Running backups        - 0
//   Running DRs            - 0

static void export_fdb_status_details(const std::string& status_str) {
    using namespace rapidjson;
    Document document;
    try {
        document.Parse(status_str.c_str());
    } catch (std::exception &e) {
        LOG(WARNING) << "fail to parse status str, err: " << e.what();
        return;
    }
    if (!document.HasMember("cluster")) {
        LOG(WARNING) << "err fdb status details";
        return;
    }
    auto get_value = [&](const std::vector<std::string>& v) -> int64_t {
        if (v.empty()) return BVAR_FDB_INVALID_VALUE;
        auto node = document.FindMember("cluster");
        for (const auto& name: v) {
            if (!node->value.HasMember(name.c_str())) return BVAR_FDB_INVALID_VALUE;
            node = node->value.FindMember(name.c_str());
        }
        if (node->value.IsInt64()) return node->value.GetInt64();
        if (node->value.IsDouble()) return static_cast<int64_t>(node->value.GetDouble());
        if (node->value.IsObject()) return node->value.MemberCount();
        return BVAR_FDB_INVALID_VALUE;
    };
    // Configuration
    g_bvar_fdb_configuration_coordinators_count.set_value(get_value({"configuration", "coordinators_count"}));
    g_bvar_fdb_configuration_usable_regions.set_value(get_value({"configuration", "usable_regions"}));

    // Cluster
    g_bvar_fdb_process_count.set_value(get_value({"processes"}));
    g_bvar_fdb_machines_count.set_value(get_value({"machines"}));
    g_bvar_fdb_fault_tolerance_count.set_value(get_value({"fault_tolerance", "max_zone_failures_without_losing_data"}));

    // Data/Operating space
    g_bvar_fdb_data_total_disk_used_bytes.set_value(get_value({"data", "total_disk_used_bytes"}));
    g_bvar_fdb_data_total_kv_size_bytes.set_value(get_value({"data", "total_kv_size_bytes"}));
    g_bvar_fdb_data_log_server_space_bytes.set_value(get_value({"data", "least_operating_space_bytes_log_server"}));
    g_bvar_fdb_data_storage_server_space_bytes.set_value(get_value({"data", "least_operating_space_bytes_storage_server"}));
    g_bvar_fdb_data_moving_data_highest_priority.set_value(get_value({"data", "moving_data", "highest_priority"}));
    g_bvar_fdb_data_moving_data_in_flight_bytes.set_value(get_value({"data", "moving_data", "in_flight_bytes"}));
    g_bvar_fdb_data_moving_data_in_queue_bytes.set_value(get_value({"data", "moving_data", "in_queue_bytes"}));
    g_bvar_fdb_data_moving_total_written_bytes.set_value(get_value({"data", "moving_data", "total_written_bytes"}));

    // Workload
    g_bvar_fdb_workload_read_rate_hz.set_value(get_value({"workload", "operations", "reads", "hz"}));
    g_bvar_fdb_workload_write_rate_hz.set_value(get_value({"workload", "operations", "writes", "hz"}));
    g_bvar_fdb_workload_transactions_started_hz.set_value(get_value({"workload", "transactions", "started", "hz"}));
    g_bvar_fdb_workload_conflict_rate_hz.set_value(get_value({"workload", "transactions", "conflicted", "hz"}));
    g_bvar_fdb_workload_transactions_committed_hz.set_value(get_value({"workload", "transactions", "committed", "hz"}));

    // Backup and DR

    // Client Count
    g_bvar_fdb_client_count.set_value(get_value({"clients", "count"}));
}

class FdbMetricExporter {
public:
    FdbMetricExporter(std::shared_ptr<TxnKv> txn_kv)
        : txn_kv_(std::move(txn_kv)), running_(false) {
        thread_.reset(new std::thread([this] {
            while(running_.load()) {
                std::string fdb_status = get_fdb_status(txn_kv_);
                export_fdb_status_details(fdb_status);
                std::this_thread::sleep_for(std::chrono::seconds(5));
            }
 
        }));
    }
    ~FdbMetricExporter() = default;

    int start() {
        if (txn_kv_ == nullptr) return -1;
        running_ = true;
        return 0;
    }

    void stop() {
        running_ = false;
        if (thread_ != nullptr) {
            thread_->join();
        }
    }
private:
    std::shared_ptr<TxnKv> txn_kv_;
    std::unique_ptr<std::thread> thread_;
    std::atomic<bool> running_;
};

} // namespace selectdb