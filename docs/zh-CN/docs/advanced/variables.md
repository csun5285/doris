---
{
    "title": "������",
    "language": "zh-CN"
}
---

<!-- 
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# ������

���������������������������������������������variables������

Doris ������������������ MySQL ��������������������������������������������������������� MySQL ������������������������������������ MySQL ������������������������������

## ���������������������

### ������

������������ `SHOW VARIABLES [LIKE 'xxx'];` ���������������������������������������

```sql
SHOW VARIABLES;
SHOW VARIABLES LIKE '%time_zone%';
```

### ������

���������������������������������������������������������������

������������ 1.1 ������������������������������������������������������������������������������������������������������������������������
������ 1.1 ������������������������������������������������������������������������������������������������������������������������������������

������������������������������ `SET var_name=xxx;` ������������������������

```sql
SET exec_mem_limit = 137438953472;
SET forward_to_master = true;
SET time_zone = "Asia/Shanghai";
```

��������������������� `SET GLOBAL var_name=xxx;` ���������������

```sql
SET GLOBAL exec_mem_limit = 137438953472
```

> ���1��������� ADMIN ������������������������������������������

������������������������������������������������������������������

- `time_zone`
- `wait_timeout`
- `sql_mode`
- `enable_profile`
- `query_timeout`
- `insert_timeout`<version since="dev"></version>
- `exec_mem_limit`
- `batch_size`
- `allow_partition_column_nullable`
- `insert_visible_timeout_ms`
- `enable_fold_constant_by_be`

���������������������������������������

- `default_rowset_type`
- `default_password_lifetime`
- `password_history`
- `validate_password_policy`

������������������������������������������������������

```sql
SET exec_mem_limit = 10 * 1024 * 1024 * 1024;
SET forward_to_master = concat('tr', 'u', 'e');
```

### ������������������������������

������������������������������������������������������������������������������������ ������������SET_VAR������������������������������������������������������������������������������������

```sql
SELECT /*+ SET_VAR(exec_mem_limit = 8589934592) */ name FROM people ORDER BY name;
SELECT /*+ SET_VAR(query_timeout = 1, enable_partition_cache=true) */ sleep(3);
```

���������������������/*+ ������������������������������SELECT���������

## ���������������

> ������
> 
> ��������������� `docs/generate-config-and-variable-doc.sh` ���������������
> 
> ������������������������ `fe/fe-core/src/main/java/org/apache/doris/qe/SessionVariable.java` ��� `fe/fe-core/src/main/java/org/apache/doris/qe/GlobalVariable.java` ���������������������

### `SQL_AUTO_IS_NULL`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `allow_partition_column_nullable`

待补充

类型：`boolean`

默认值：`true`

只读变量：`false`

仅全局变量：`false`

### `auto_broadcast_join_threshold`

待补充

类型：`double`

默认值：`0.8`

只读变量：`false`

仅全局变量：`false`

### `auto_increment_increment`

待补充

类型：`int`

默认值：`1`

只读变量：`false`

仅全局变量：`false`

### `autocommit`

待补充

类型：`boolean`

默认值：`true`

只读变量：`false`

仅全局变量：`false`

### `batch_size`

待补充

类型：`int`

默认值：`4064`

只读变量：`false`

仅全局变量：`false`

### `character_set_client`

待补充

类型：`String`

默认值：`utf8`

只读变量：`false`

仅全局变量：`false`

### `character_set_connection`

待补充

类型：`String`

默认值：`utf8`

只读变量：`false`

仅全局变量：`false`

### `character_set_results`

待补充

类型：`String`

默认值：`utf8`

只读变量：`false`

仅全局变量：`false`

### `character_set_server`

待补充

类型：`String`

默认值：`utf8`

只读变量：`false`

仅全局变量：`false`

### `cloud_cluster`

待补充

类型：`String`

默认值：``

只读变量：`false`

仅全局变量：`false`

### `codegen_level`

待补充

类型：`int`

默认值：`0`

只读变量：`false`

仅全局变量：`false`

### `collation_connection`

待补充

类型：`String`

默认值：`utf8_general_ci`

只读变量：`false`

仅全局变量：`false`

### `collation_database`

待补充

类型：`String`

默认值：`utf8_general_ci`

只读变量：`false`

仅全局变量：`false`

### `collation_server`

待补充

类型：`String`

默认值：`utf8_general_ci`

只读变量：`false`

仅全局变量：`false`

### `cpu_resource_limit`

待补充

类型：`int`

默认值：`-1`

只读变量：`false`

仅全局变量：`false`

### `default_password_lifetime`

待补充

类型：`int`

默认值：`0`

只读变量：`false`

仅全局变量：`true`

### `default_rowset_type`

待补充

类型：`String`

默认值：`beta`

只读变量：`false`

仅全局变量：`true`

### `default_storage_engine`

待补充

类型：`String`

默认值：`olap`

只读变量：`false`

仅全局变量：`false`

### `default_tmp_storage_engine`

待补充

类型：`String`

默认值：`olap`

只读变量：`false`

仅全局变量：`false`

### `delete_without_partition`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `disable_colocate_plan`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `disable_file_cache`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `disable_streaming_preaggregations`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `div_precision_increment`

待补充

类型：`int`

默认值：`4`

只读变量：`false`

仅全局变量：`false`

### `drop_table_if_ctas_failed`

待补充

类型：`boolean`

默认值：`true`

只读变量：`false`

仅全局变量：`false`

### `dry_run_query`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `dump_nereids_memo`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `experimental_enable_agg_state`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `enable_analyze_complex_type_column`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `enable_bucket_shuffle_join`

待补充

类型：`boolean`

默认值：`true`

只读变量：`false`

仅全局变量：`false`

### `enable_cbo_statistics`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `enable_colocate_scan`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `enable_common_expr_pushdown`

待补充

类型：`boolean`

默认值：`true`

只读变量：`false`

仅全局变量：`false`

### `enable_count_on_index_pushdown`

是否启用count_on_index pushdown。

类型：`boolean`

默认值：`true`

只读变量：`false`

仅全局变量：`false`

### `enable_cte_materialize`

待补充

类型：`boolean`

默认值：`true`

只读变量：`false`

仅全局变量：`false`

### `enable_dphyp_optimizer`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `enable_dphyp_trace`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `enable_eliminate_sort_node`

待补充

类型：`boolean`

默认值：`true`

只读变量：`false`

仅全局变量：`false`

### `enable_exchange_node_parallel_merge`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `enable_fallback_to_original_planner`

待补充

类型：`boolean`

默认值：`true`

只读变量：`false`

仅全局变量：`false`

### `enable_file_cache`

是否启用file cache。该变量只有在be.conf中enable_file_cache=true时才有效，如果be.conf中enable_file_cache=false，该BE节点的file cache处于禁用状态。

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `enable_fold_nondeterministic_fn`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `enable_function_pushdown`

待补充

类型：`boolean`

默认值：`true`

只读变量：`false`

仅全局变量：`false`

### `enable_insert_group_commit`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `enable_insert_strict`

待补充

类型：`boolean`

默认值：`true`

只读变量：`false`

仅全局变量：`false`

### `enable_inverted_index_query`

是否启用inverted index query。

类型：`boolean`

默认值：`true`

只读变量：`false`

仅全局变量：`false`

### `enable_local_exchange`

待补充

类型：`boolean`

默认值：`true`

只读变量：`false`

仅全局变量：`false`

### `enable_memtable_on_sink_node`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `enable_minidump`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `enable_multi_cluster_sync_load`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `enable_nereids_dml`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `enable_nereids_timeout`

待补充

类型：`boolean`

默认值：`true`

只读变量：`false`

仅全局变量：`false`

### `enable_new_shuffle_hash_method`

待补充

类型：`boolean`

默认值：`true`

只读变量：`false`

仅全局变量：`false`

### `enable_odbc_transcation`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `enable_orc_lazy_materialization`

控制 orc reader 是否启用延迟物化技术。默认为 true。

类型：`boolean`

默认值：`true`

只读变量：`false`

仅全局变量：`false`

### `enable_page_cache`

控制是否启用page cache。默认为 true。

类型：`boolean`

默认值：`true`

只读变量：`false`

仅全局变量：`false`

### `enable_parallel_outfile`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `enable_parquet_lazy_materialization`

控制 parquet reader 是否启用延迟物化技术。默认为 true。

类型：`boolean`

默认值：`true`

只读变量：`false`

仅全局变量：`false`

### `enable_partition_cache`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `enable_profile`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `enable_push_down_no_group_agg`

待补充

类型：`boolean`

默认值：`true`

只读变量：`false`

仅全局变量：`false`

### `enable_runtime_filter_prune`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `enable_scan_node_run_serial`

是否开启ScanNode串行读，以避免limit较小的情况下的读放大，可以提高查询的并发能力

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `enable_share_hash_table_for_broadcast_join`

待补充

类型：`boolean`

默认值：`true`

只读变量：`false`

仅全局变量：`false`

### `enable_single_distinct_column_opt`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `experimental_enable_single_replica_insert`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `enable_spilling`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `enable_sql_cache`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `enable_strict_consistency_dml`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `enable_strong_consistency_read`

用以开启强一致读。Doris 默认支持同一个会话内的强一致性，即同一个会话内对数据的变更操作是实时可见的。如需要会话间的强一致读，则需将此变量设置为true。

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `enable_two_phase_read_opt`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `enable_unicode_name_support`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `enable_unified_load`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `enable_unique_key_partial_update`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `enable_vectorized_engine`

待补充

类型：`boolean`

默认值：`true`

只读变量：`false`

仅全局变量：`false`

### `event_scheduler`

待补充

类型：`String`

默认值：`OFF`

只读变量：`false`

仅全局变量：`false`

### `exec_mem_limit`

待补充

类型：`long`

默认值：`2147483648`

只读变量：`false`

仅全局变量：`false`

### `external_agg_bytes_threshold`

待补充

类型：`long`

默认值：`0`

只读变量：`false`

仅全局变量：`false`

### `external_agg_partition_bits`

待补充

类型：`int`

默认值：`8`

只读变量：`false`

仅全局变量：`false`

### `external_sort_bytes_threshold`

待补充

类型：`long`

默认值：`0`

只读变量：`false`

仅全局变量：`false`

### `external_table_analyze_part_num`

收集外表统计信息行数时选取的采样分区数，默认-1表示全部分区

类型：`int`

默认值：`-1`

只读变量：`false`

仅全局变量：`false`

### `extract_wide_range_expr`

待补充

类型：`boolean`

默认值：`true`

只读变量：`false`

仅全局变量：`false`

### `file_cache_base_path`

指定block file cache在BE上的存储路径，默认 'random'，随机选择BE配置的存储路径。

类型：`String`

默认值：`random`

只读变量：`false`

仅全局变量：`false`

### `file_split_size`

待补充

类型：`long`

默认值：`0`

只读变量：`false`

仅全局变量：`false`

### `forbid_unknown_col_stats`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `forward_to_master`

待补充

类型：`boolean`

默认值：`true`

只读变量：`false`

仅全局变量：`false`

### `fragment_transmission_compression_codec`

待补充

类型：`String`

默认值：`lz4`

只读变量：`false`

仅全局变量：`false`

### `group_by_and_having_use_alias_first`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `group_concat_max_len`

待补充

类型：`long`

默认值：`2147483646`

只读变量：`false`

仅全局变量：`false`

### `have_query_cache`

待补充

类型：`boolean`

默认值：`false`

只读变量：`true`

仅全局变量：`false`

### `init_connect`

待补充

类型：`String`

默认值：``

只读变量：`false`

仅全局变量：`true`

### `inline_cte_referenced_threshold`

待补充

类型：`int`

默认值：`1`

只读变量：`false`

仅全局变量：`false`

### `insert_timeout`

待补充

类型：`int`

默认值：`14400`

只读变量：`false`

仅全局变量：`false`

### `insert_visible_timeout_ms`

待补充

类型：`long`

默认值：`10000`

只读变量：`false`

仅全局变量：`false`

### `interactive_timeout`

待补充

类型：`int`

默认值：`3600`

只读变量：`false`

仅全局变量：`false`

### `internal_session`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `inverted_index_conjunction_opt_threshold`

在match_all中求取多个倒排索引的交集时,如果最大的倒排索引中的总数是最小倒排索引中的总数的整数倍,则使用跳表来优化交集操作。

类型：`int`

默认值：`1000`

只读变量：`false`

仅全局变量：`false`

### `jdbc_clickhouse_query_final`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `language`

待补充

类型：`String`

默认值：`/selectdb_cloud/share/english/`

只读变量：`true`

仅全局变量：`false`

### `license`

待补充

类型：`String`

默认值：`Apache License, Version 2.0`

只读变量：`true`

仅全局变量：`false`

### `lower_case_table_names`

待补充

类型：`int`

默认值：`0`

只读变量：`true`

仅全局变量：`false`

### `max_allowed_packet`

待补充

类型：`int`

默认值：`1048576`

只读变量：`false`

仅全局变量：`false`

### `max_execution_time`

待补充

类型：`int`

默认值：`-1`

只读变量：`false`

仅全局变量：`false`

### `max_filter_ratio`

待补充

类型：`double`

默认值：`0.0`

只读变量：`false`

仅全局变量：`false`

### `max_instance_num`

待补充

类型：`int`

默认值：`64`

只读变量：`false`

仅全局变量：`false`

### `max_pushdown_conditions_per_column`

待补充

类型：`int`

默认值：`-1`

只读变量：`false`

仅全局变量：`false`

### `max_scan_key_num`

待补充

类型：`int`

默认值：`-1`

只读变量：`false`

仅全局变量：`false`

### `max_table_count_use_cascades_join_reorder`

待补充

类型：`int`

默认值：`10`

只读变量：`false`

仅全局变量：`false`

### `memo_max_group_expression_size`

待补充

类型：`int`

默认值：`10000`

只读变量：`false`

仅全局变量：`false`

### `nereids_trace_event_mode`

待补充

类型：`String`

默认值：`all`

只读变量：`false`

仅全局变量：`false`

### `net_buffer_length`

待补充

类型：`int`

默认值：`16384`

只读变量：`true`

仅全局变量：`false`

### `net_read_timeout`

待补充

类型：`int`

默认值：`60`

只读变量：`false`

仅全局变量：`false`

### `net_write_timeout`

待补充

类型：`int`

默认值：`60`

只读变量：`false`

仅全局变量：`false`

### `parallel_exchange_instance_num`

待补充

类型：`int`

默认值：`-1`

只读变量：`false`

仅全局变量：`false`

### `parallel_fragment_exec_instance_num`

待补充

类型：`int`

默认值：`1`

只读变量：`false`

仅全局变量：`false`

### `parallel_pipeline_task_num`

待补充

类型：`int`

默认值：`0`

只读变量：`false`

仅全局变量：`false`

### `parallel_sync_analyze_task_num`

待补充

类型：`int`

默认值：`2`

只读变量：`false`

仅全局变量：`false`

### `partition_pruning_expand_threshold`

待补充

类型：`int`

默认值：`10`

只读变量：`false`

仅全局变量：`false`

### `partitioned_hash_agg_rows_threshold`

待补充

类型：`int`

默认值：`0`

只读变量：`false`

仅全局变量：`false`

### `partitioned_hash_join_rows_threshold`

待补充

类型：`int`

默认值：`0`

只读变量：`false`

仅全局变量：`false`

### `password_history`

待补充

类型：`int`

默认值：`0`

只读变量：`false`

仅全局变量：`true`

### `performance_schema`

待补充

类型：`String`

默认值：`OFF`

只读变量：`true`

仅全局变量：`false`

### `plan_nereids_dump`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `prefer_join_method`

待补充

类型：`String`

默认值：`broadcast`

只读变量：`false`

仅全局变量：`false`

### `profiling`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `query_cache_size`

待补充

类型：`long`

默认值：`1048576`

只读变量：`false`

仅全局变量：`true`

### `query_cache_type`

待补充

类型：`int`

默认值：`0`

只读变量：`false`

仅全局变量：`false`

### `query_timeout`

待补充

类型：`int`

默认值：`900`

只读变量：`false`

仅全局变量：`false`

### `repeat_max_num`

待补充

类型：`int`

默认值：`10000`

只读变量：`false`

仅全局变量：`false`

### `resource_group`

待补充

类型：`String`

默认值：``

只读变量：`false`

仅全局变量：`false`

### `rewrite_count_distinct_to_bitmap_hll`

待补充

类型：`boolean`

默认值：`true`

只读变量：`false`

仅全局变量：`false`

### `round_precise_decimalv2_value`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `scan_queue_mem_limit`

待补充

类型：`long`

默认值：`107374182`

只读变量：`false`

仅全局变量：`false`

### `send_batch_parallelism`

待补充

类型：`int`

默认值：`1`

只读变量：`false`

仅全局变量：`false`

### `session_context`

待补充

类型：`String`

默认值：``

只读变量：`false`

仅全局变量：`false`

### `show_hidden_columns`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `show_user_default_role`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `skip_delete_bitmap`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `skip_delete_predicate`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `skip_delete_sign`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `skip_storage_engine_merge`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `sql_mode`

待补充

类型：`long`

默认值：`1`

只读变量：`false`

仅全局变量：`false`

### `sql_quote_show_create`

待补充

类型：`boolean`

默认值：`true`

只读变量：`false`

仅全局变量：`false`

### `sql_safe_updates`

待补充

类型：`int`

默认值：`0`

只读变量：`false`

仅全局变量：`false`

### `storage_engine`

待补充

类型：`String`

默认值：`olap`

只读变量：`false`

仅全局变量：`false`

### `strict_mode`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `system_time_zone`

待补充

类型：`String`

默认值：`Asia/Shanghai`

只读变量：`true`

仅全局变量：`false`

### `time_zone`

待补充

类型：`String`

默认值：`Asia/Shanghai`

只读变量：`false`

仅全局变量：`false`

### `topn_opt_limit_threshold`

待补充

类型：`long`

默认值：`1024`

只读变量：`false`

仅全局变量：`false`

### `trace_nereids`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `transaction_isolation`

待补充

类型：`String`

默认值：`REPEATABLE-READ`

只读变量：`false`

仅全局变量：`false`

### `transaction_read_only`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `trim_tailing_spaces_for_external_table_query`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `truncate_char_or_varchar_columns`

是否按照表的 schema 来截断 char 或者 varchar 列。默认为 false。
因为外表会存在表的 schema 中 char 或者 varchar 列的最大长度和底层 parquet 或者 orc 文件中的 schema 不一致的情况。此时开启改选项，会按照表的 schema 中的最大长度进行截断。

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `tx_isolation`

待补充

类型：`String`

默认值：`REPEATABLE-READ`

只读变量：`false`

仅全局变量：`false`

### `tx_read_only`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `use_fix_replica`

待补充

类型：`int`

默认值：`-1`

只读变量：`false`

仅全局变量：`false`

### `use_rf_default`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `use_v2_rollup`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `version`

待补充

类型：`String`

默认值：`5.7.99`

只读变量：`true`

仅全局变量：`false`

### `version_comment`

待补充

类型：`String`

默认值：`SelectDB Core version: 0.0.0`

只读变量：`true`

仅全局变量：`false`

### `wait_timeout`

待补充

类型：`int`

默认值：`28800`

只读变量：`false`

仅全局变量：`false`

### `workload_group`

待补充

类型：`String`

默认值：``

只读变量：`false`

仅全局变量：`false`



