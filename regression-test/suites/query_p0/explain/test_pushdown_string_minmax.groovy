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

// MIN/MAX push-down no longer looks at the declared column length. The storage layer decides per
// segment: a zone map bound cut to 512 bytes is not a value the column holds, so those segments
// read the rows instead.
suite("test_pushdown_string_minmax") {
    sql "set enable_nereids_planner = true"
    sql "set enable_fallback_to_original_planner = false"

    def longValue = "z" * 600
    def exactValue = "d" * 512

    // A VARCHAR wider than 512 used to be excluded by its declared length alone, even when every
    // value in it was short.
    sql "DROP TABLE IF EXISTS test_string_minmax_wide"
    sql """
        CREATE TABLE test_string_minmax_wide (
            `id` INT NOT NULL,
            `v` VARCHAR(65533) NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """
    sql """ INSERT INTO test_string_minmax_wide VALUES (1, "aaa"), (2, "zzz") """
    explain {
        sql "select min(v), max(v) from test_string_minmax_wide"
        contains "pushAggOp=MINMAX"
    }
    def r = sql "select min(v), max(v) from test_string_minmax_wide"
    assertEquals("aaa", r[0][0])
    assertEquals("zzz", r[0][1])

    // A value past the 512-byte cut: the plan still pushes down, and the storage layer falls back
    // per segment so the answer stays a value the table holds.
    sql """ INSERT INTO test_string_minmax_wide VALUES (3, "${longValue}") """
    explain {
        sql "select min(v), max(v) from test_string_minmax_wide"
        contains "pushAggOp=MINMAX"
    }
    r = sql "select min(v), max(v) from test_string_minmax_wide"
    assertEquals("aaa", r[0][0])
    assertEquals(longValue, r[0][1])

    // A VARCHAR filled to exactly 512 bytes is cut as well. FE pushed this down before too,
    // because the declared length is not over 512, and the raised bound answered MAX with a value
    // that was never inserted.
    sql "DROP TABLE IF EXISTS test_string_minmax_512"
    sql """
        CREATE TABLE test_string_minmax_512 (
            `id` INT NOT NULL,
            `v` VARCHAR(512) NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """
    sql """ INSERT INTO test_string_minmax_512 VALUES (1, "aaa"), (2, "${exactValue}") """
    r = sql "select max(v) from test_string_minmax_512"
    assertEquals(exactValue, r[0][0])
    assertEquals(1, sql("select count(*) from test_string_minmax_512 where v = '${exactValue}'")[0][0])

    // The switch now covers every string type, and it is on by default. Turning it off stops
    // string MIN/MAX from being pushed down at all.
    sql "DROP TABLE IF EXISTS test_string_minmax_str"
    sql """
        CREATE TABLE test_string_minmax_str (
            `id` INT NOT NULL,
            `v` STRING NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """
    sql """ INSERT INTO test_string_minmax_str VALUES (1, "aaa"), (2, "${longValue}") """

    sql "set enable_pushdown_string_minmax = false"
    explain {
        sql "select min(v), max(v) from test_string_minmax_str"
        contains "pushAggOp=NONE"
    }

    sql "set enable_pushdown_string_minmax = true"
    explain {
        sql "select min(v), max(v) from test_string_minmax_str"
        contains "pushAggOp=MINMAX"
    }
    r = sql "select min(v), max(v) from test_string_minmax_str"
    assertEquals("aaa", r[0][0])
    assertEquals(longValue, r[0][1])

    sql "DROP TABLE IF EXISTS test_string_minmax_str"
    sql "DROP TABLE IF EXISTS test_string_minmax_512"
    sql "DROP TABLE IF EXISTS test_string_minmax_wide"
}
