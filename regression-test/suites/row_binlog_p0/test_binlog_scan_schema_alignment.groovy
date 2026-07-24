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

suite("test_binlog_scan_schema_alignment", "nonConcurrent,p0") {
    sql "DROP TABLE IF EXISTS binlog_scan_schema_alignment"
    sql """
        CREATE TABLE binlog_scan_schema_alignment (
            k1 INT,
            k2 INT,
            v1 INT,
            v2 INT
        ) ENGINE = OLAP
        UNIQUE KEY(k1, k2)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true"
        )
    """

    def knownStats = """
        ('row_count'='1', 'ndv'='1', 'num_nulls'='0',
         'min_value'='1', 'max_value'='1', 'data_size'='4')
    """
    sql "ALTER TABLE binlog_scan_schema_alignment MODIFY COLUMN k1 SET STATS ${knownStats}"
    sql "ALTER TABLE binlog_scan_schema_alignment MODIFY COLUMN k2 SET STATS ${knownStats}"
    sql "ALTER TABLE binlog_scan_schema_alignment MODIFY COLUMN v1 SET STATS ${knownStats}"

    sql "SET forbid_unknown_col_stats = true"
    try {
        sql """
            SELECT v1
            FROM binlog_scan_schema_alignment@incr("incrementType" = "MIN_DELTA")
        """
        sql """
            SELECT v1 + k1 AS x, (v1 + k1) + 1 AS y
            FROM binlog_scan_schema_alignment@incr("incrementType" = "MIN_DELTA")
        """
    } finally {
        sql "SET forbid_unknown_col_stats = false"
    }
}
