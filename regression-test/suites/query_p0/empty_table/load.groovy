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

// The cases is copied from https://github.com/trinodb/trino/tree/master
// /testing/trino-product-tests/src/main/resources/sql-tests/testcases
// and modified by Doris.

suite("load") {
    def tables=["empty"]

    for (String table in tables) {
        sql """ DROP TABLE IF EXISTS $table """
    }

    for (String table in tables) {
        sql new File("""${context.file.parent}/ddl/${table}.sql""").text
    }

    // Empty tables shoud not trigger scanning
    sql """drop table if exists t1 force"""
    sql """
CREATE TABLE `t1` (
  `id` int(11) NULL,
  `name` varchar(255) NULL,
  `score` int(11) SUM NULL
) ENGINE=OLAP
AGGREGATE KEY(`id`, `name`)
COMMENT 'OLAP'
DISTRIBUTED BY HASH(`id`) BUCKETS 1
PROPERTIES (
"persistent" = "false"
);
    """

    String res = sql """explain select * from t1"""
    // """
    // +---------------------------------------------------------------------------------------+
    // | Explain String                                                                        |
    // +---------------------------------------------------------------------------------------+
    // | PLAN FRAGMENT 0                                                                       |
    // |   OUTPUT EXPRS:                                                                       |
    // |     `default_cluster:db1`.`t1`.`id`                                                   |
    // |     `default_cluster:db1`.`t1`.`name`                                                 |
    // |     `default_cluster:db1`.`t1`.`score`                                                |
    // |   PARTITION: HASH_PARTITIONED: `default_cluster:db1`.`t1`.`id`                        |
    // |                                                                                       |
    // |   VRESULT SINK                                                                        |
    // |                                                                                       |
    // |   0:VOlapScanNode                                                                     |
    // |      TABLE: default_cluster:db1.t1(t1), PREAGGREGATION: OFF. Reason: No AggregateInfo |
    // |      partitions=0/1, tablets=0/0, tabletList=                                         |
    // |      cardinality=0, avgRowSize=24.0, numNodes=1                                       |
    // +---------------------------------------------------------------------------------------+
    // """
    // println(res)
    assertTrue(res.contains("partitions=0/1, tablets=0/0, tabletList="))
    sql """drop table if exists t1 force"""
}
