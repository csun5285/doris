
#include "common/bvars.h"

#include <cstdint>

// meta-service's bvars
BvarLatencyRecorderWithTag g_bvar_ms_begin_txn("ms", "begin_txn");
BvarLatencyRecorderWithTag g_bvar_ms_precommit_txn("ms", "precommit_txn");
BvarLatencyRecorderWithTag g_bvar_ms_commit_txn("ms", "commit_txn");
BvarLatencyRecorderWithTag g_bvar_ms_abort_txn("ms", "abort_txn");
BvarLatencyRecorderWithTag g_bvar_ms_get_txn("ms", "get_txn");
BvarLatencyRecorderWithTag g_bvar_ms_get_current_max_txn_id("ms", "get_current_max_txn_id");
BvarLatencyRecorderWithTag g_bvar_ms_check_txn_conflict("ms", "check_txn_conflict");
BvarLatencyRecorderWithTag g_bvar_ms_get_version("ms", "get_version");
BvarLatencyRecorderWithTag g_bvar_ms_create_tablets("ms", "create_tablets");
BvarLatencyRecorderWithTag g_bvar_ms_update_tablet("ms", "update_tablet");
BvarLatencyRecorderWithTag g_bvar_ms_update_tablet_schema("ms", "update_tablet_schema");
BvarLatencyRecorderWithTag g_bvar_ms_get_tablet("ms", "get_tablet");
BvarLatencyRecorderWithTag g_bvar_ms_prepare_rowset("ms", "prepare_rowset");
BvarLatencyRecorderWithTag g_bvar_ms_commit_rowset("ms", "commit_rowset");
BvarLatencyRecorderWithTag g_bvar_ms_get_rowset("ms", "get_rowset");
BvarLatencyRecorderWithTag g_bvar_ms_drop_index("ms", "drop_index");
BvarLatencyRecorderWithTag g_bvar_ms_prepare_index("ms", "prepare_index");
BvarLatencyRecorderWithTag g_bvar_ms_commit_index("ms", "commit_index");
BvarLatencyRecorderWithTag g_bvar_ms_prepare_partition("ms", "prepare_partition");
BvarLatencyRecorderWithTag g_bvar_ms_commit_partition("ms", "commit_partition");
BvarLatencyRecorderWithTag g_bvar_ms_drop_partition("ms", "drop_partition");
BvarLatencyRecorderWithTag g_bvar_ms_get_tablet_stats("ms", "get_tablet_stats");
BvarLatencyRecorderWithTag g_bvar_ms_get_obj_store_info("ms", "get_obj_store_info");
BvarLatencyRecorderWithTag g_bvar_ms_alter_obj_store_info("ms", "alter_obj_store_info");
BvarLatencyRecorderWithTag g_bvar_ms_create_instance("ms", "create_instance");
BvarLatencyRecorderWithTag g_bvar_ms_alter_instance("ms", "alter_instance");
BvarLatencyRecorderWithTag g_bvar_ms_alter_cluster("ms", "alter_cluster");
BvarLatencyRecorderWithTag g_bvar_ms_get_cluster("ms", "get_cluster");
BvarLatencyRecorderWithTag g_bvar_ms_create_stage("ms", "create_stage");
BvarLatencyRecorderWithTag g_bvar_ms_get_stage("ms", "get_stage");
BvarLatencyRecorderWithTag g_bvar_ms_drop_stage("ms", "drop_stage");
BvarLatencyRecorderWithTag g_bvar_ms_get_iam("ms", "get_iam");
BvarLatencyRecorderWithTag g_bvar_ms_alter_ram_user("ms", "alter_ram_user");
BvarLatencyRecorderWithTag g_bvar_ms_begin_copy("ms", "begin_copy");
BvarLatencyRecorderWithTag g_bvar_ms_finish_copy("ms", "finish_copy");
BvarLatencyRecorderWithTag g_bvar_ms_get_copy_job("ms", "get_copy_job");
BvarLatencyRecorderWithTag g_bvar_ms_get_copy_files("ms", "get_copy_files");

BvarLatencyRecorderWithTag g_bvar_ms_start_tablet_job("ms", "start_tablet_job");
BvarLatencyRecorderWithTag g_bvar_ms_finish_tablet_job("ms", "finish_tablet_job");

// txn_kv's bvars
bvar::LatencyRecorder g_bvar_txn_kv_get("txn_kv", "get");
bvar::LatencyRecorder g_bvar_txn_kv_range_get("txn_kv", "range_get");
bvar::LatencyRecorder g_bvar_txn_kv_put("txn_kv", "put");
bvar::LatencyRecorder g_bvar_txn_kv_commit("txn_kv", "commit");
bvar::LatencyRecorder g_bvar_txn_kv_atomic_set_ver_key("txn_kv", "atomic_set_ver_key");
bvar::LatencyRecorder g_bvar_txn_kv_atomic_set_ver_value("txn_kv", "atomic_set_ver_value");
bvar::LatencyRecorder g_bvar_txn_kv_atomic_add("txn_kv", "atomic_add");
bvar::LatencyRecorder g_bvar_txn_kv_remove("txn_kv", "remove");
bvar::LatencyRecorder g_bvar_txn_kv_range_remove("txn_kv", "range_remove");
bvar::LatencyRecorder g_bvar_txn_kv_get_read_version("txn_kv", "get_read_version");
bvar::LatencyRecorder g_bvar_txn_kv_get_committed_version("txn_kv", "get_committed_version");

bvar::Adder<int64_t> g_bvar_txn_kv_commit_error_counter;
bvar::Window<bvar::Adder<int64_t> > g_bvar_txn_kv_commit_error_counter_minute("txn_kv", "commit_error", &g_bvar_txn_kv_commit_error_counter, 60);

bvar::Adder<int64_t> g_bvar_txn_kv_commit_conflict_counter;
bvar::Window<bvar::Adder<int64_t> > g_bvar_txn_kv_commit_conflict_counter_minute("txn_kv", "commit_conflict", &g_bvar_txn_kv_commit_conflict_counter, 60);