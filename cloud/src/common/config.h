// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Convert to KV formart with Vim
// %s/.*(\([a-z_A-Z]\+\),.*"\([^"]*\)".*/\1 = \2/gc

#pragma once

#include "configbase.h"

namespace selectdb::config {

CONF_Int32(brpc_listen_port, "5000");
CONF_Int32(brpc_num_threads, "-1");
CONF_String(fdb_cluster, "xxx:yyy@127.0.0.1:4500");
CONF_String(fdb_cluster_file_path, "./conf/fdb.cluster");
CONF_String(http_token, "greedisgood9999");
// use volatile mem kv for test. MUST NOT be `true` in production environment.
CONF_Bool(use_mem_kv, "false");
CONF_Int32(meta_server_register_interval_ms, "20000");
CONF_Int32(meta_server_lease_ms, "60000");


CONF_Int64(brpc_max_body_size, "3147483648")
CONF_Int64(brpc_socket_max_unwritten_bytes, "1073741824");

// logging
CONF_String(log_dir, "./log/");
CONF_String(log_level, "info"); // info warn error fatal
CONF_Int64(log_size_mb, "1024");
CONF_Int32(log_filenum_quota, "10");
CONF_Int32(warn_log_filenum_quota, "1");
CONF_Bool(log_immediate_flush, "false");
CONF_Strings(log_verbose_modules, ""); // Comma seprated list: a.*,b.*
CONF_Int32(log_verbose_level, "5");

// recycler config
CONF_mInt64(recycle_interval_seconds, "3600");
CONF_mInt64(retention_seconds, "259200"); // 72h
CONF_Int32(recycle_concurrency, "16");
CONF_Bool(recycle_standalone_mode, "false");
CONF_Int32(recycle_job_lease_expired_ms, "60000");

CONF_String(test_s3_ak, "ak");
CONF_String(test_s3_sk, "sk");
CONF_String(test_s3_endpoint, "endpoint");
CONF_String(test_s3_region, "region");
CONF_String(test_s3_bucket, "bucket");
// CONF_Int64(a, "1073741824");
// CONF_Bool(b, "true");

// txn config
CONF_Int32(stream_load_default_timeout_second, "600");
CONF_Int32(stream_load_default_precommit_timeout_second, "3600");
CONF_Int32(label_keep_max_second, "259200"); //3 * 24 * 3600 seconds
CONF_Int32(expired_txn_scan_key_nums, "1000");

// Maximum number of version of a tablet. If the version num of a tablet exceed limit,
// the load process will reject new incoming load job of this tablet.
// This is to avoid too many version num.
CONF_Int64(max_tablet_version_num, "2000");

// metrics config
CONF_Bool(use_detailed_metrics, "true");

// stage num config
CONF_Int32(max_num_stages, "40");

// qps limit config

// limit by each warehouse each rpc
CONF_Int64(default_max_qps_limit, "1000000");
// limit by each warehouse specific rpc
CONF_String(specific_max_qps_limit, "get_cluster:5000000;begin_txn:5000000");
CONF_Bool(enable_rate_limit, "true");
CONF_Int64(bvar_qps_update_second, "5");

} // namespace selectdb::config
