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

// A parallel scan narrows each segment to the rows its key ranges may hit before splitting it
// among scanners, and the scanners that read parts of one segment share its index results.
// Every query here must return the same rows as a serial scan.
suite("test_parallel_scan_key_prune") {
    sql """ DROP TABLE IF EXISTS test_parallel_scan_key_prune """
    sql """
        CREATE TABLE test_parallel_scan_key_prune (
            k1 INT NOT NULL,
            k2 INT NOT NULL,
            v1 INT,
            v2 VARCHAR(32)
        ) ENGINE=OLAP
        DUPLICATE KEY(k1, k2)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "bloom_filter_columns" = "v1",
            "disable_auto_compaction" = "true"
        )
    """
    // One load of sorted keys: every k1 holds 100 rows, k2 runs 0..99 within it.
    sql """
        INSERT INTO test_parallel_scan_key_prune
        SELECT CAST(number DIV 100 AS INT), CAST(number % 100 AS INT),
               CAST(number % 1000 AS INT), CONCAT('v', CAST(number AS STRING))
        FROM numbers("number" = "200000")
    """
    sql """ sync """

    // Split the segment among many scanners.
    sql """ set enable_parallel_scan = true """
    sql """ set parallel_scan_min_rows_per_scanner = 1024 """
    sql """ set parallel_scan_max_scanners_count = 16 """

    // A key prefix range.
    order_qt_prefix_range """
        SELECT count(*), sum(v1), min(k2), max(k2)
        FROM test_parallel_scan_key_prune WHERE k1 BETWEEN 500 AND 520
    """
    // A range on the full key.
    order_qt_full_key_range """
        SELECT k1, k2, v1 FROM test_parallel_scan_key_prune
        WHERE k1 = 700 AND k2 BETWEEN 10 AND 20
    """
    // Several key ranges.
    order_qt_in_small """
        SELECT k1, count(*), sum(v1) FROM test_parallel_scan_key_prune
        WHERE k1 IN (1, 300, 1999) GROUP BY k1
    """
    // More values than max_scan_key_num: one range over all of them, spread-out values matched
    // against each page zone map.
    order_qt_in_large """
        SELECT k1, count(*), sum(v1) FROM test_parallel_scan_key_prune
        WHERE k1 IN (0, 40, 80, 120, 160, 200, 240, 280, 320, 360, 400, 440, 480, 520, 560, 600,
                     640, 680, 720, 760, 800, 840, 880, 920, 960, 1000, 1040, 1080, 1120, 1160,
                     1200, 1240, 1280, 1320, 1360, 1400, 1440, 1480, 1520, 1560, 1600, 1640, 1680,
                     1720, 1760, 1800, 1840, 1880, 1920, 1960)
        GROUP BY k1
    """
    // The product of two IN lists exceeds max_scan_key_num, so the second becomes a range.
    order_qt_in_two_columns """
        SELECT k1, k2, v1 FROM test_parallel_scan_key_prune
        WHERE k1 IN (10, 20) AND k2 IN (1, 5, 9, 13, 17, 21, 25, 29, 33, 37, 41, 45, 49, 53, 57,
                                        61, 65, 69, 73, 77, 81, 85, 89, 93, 97)
    """
    // A bloom filter predicate on a value column inside a key range.
    order_qt_bloom_filter """
        SELECT count(*), min(k1), max(k1) FROM test_parallel_scan_key_prune
        WHERE v1 IN (7, 77, 777) AND k1 BETWEEN 100 AND 1500
    """
    // Key ranges that miss every row.
    order_qt_no_rows """
        SELECT count(*) FROM test_parallel_scan_key_prune WHERE k1 BETWEEN 5000 AND 6000
    """
    // A LIMIT without ORDER BY does not look up the key ranges before splitting.
    order_qt_limit """
        SELECT count(*) FROM (
            SELECT k1 FROM test_parallel_scan_key_prune WHERE k1 >= 1000 LIMIT 10
        ) t
    """
}
