import java.util.stream.Collectors

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

suite("test_analyze_partition_first_load") {
    String db = "regression_test_statistics"
    String tbl = "partition_first_load_test"
    sql """
        DROP DATABASE IF EXISTS `${db}`
    """

    sql """
        CREATE DATABASE `${db}`
    """

    sql """
        USE `${db}`
    """

    sql """
        DROP TABLE IF EXISTS partition_test
    """

    // Test partititon load data for the first time.
    sql """
     CREATE TABLE `$tbl` (
      `id` INT NOT NULL,
      `name` VARCHAR(25) NOT NULL,
      `comment` VARCHAR(152) NULL
      ) ENGINE=OLAP
      DUPLICATE KEY(`id`)
      COMMENT 'OLAP'
      PARTITION BY RANGE(`id`)
      (PARTITION p1 VALUES [("0"), ("100")),
       PARTITION p2 VALUES [("100"), ("200")),
       PARTITION p3 VALUES [("200"), ("300")))
      DISTRIBUTED BY HASH(`id`) BUCKETS 1
      PROPERTIES (
       "replication_num" = "1");
    """

    sql """analyze table `$tbl` with sync"""
    sql """insert into `$tbl` values (1, '1', '1')"""
    def partition_result = sql """show table stats `$tbl`"""
    assertEquals(partition_result[0][6], "true")
    assertEquals(partition_result[0][0], "1")
    sql """analyze table `$tbl` with sync"""
    partition_result = sql """show table stats `$tbl`"""
    assertEquals(partition_result[0][6], "false")
    sql """insert into `$tbl` values (101, '1', '1')"""
    partition_result = sql """show table stats `$tbl`"""
    assertEquals(partition_result[0][6], "true")
    sql """analyze table `$tbl`(id) with sync"""
    partition_result = sql """show table stats `$tbl`"""
    assertEquals(partition_result[0][6], "false")
    sql """insert into `$tbl` values (102, '1', '1')"""
    partition_result = sql """show table stats `$tbl`"""
    assertEquals(partition_result[0][6], "false")

    streamLoad {
        table "$tbl"
        set 'column_separator', '\t'
        set 'columns', 'id, name, comment'
        file 'test_partition_first_load.csv'
        time 10000 // limit inflight 10s
    }

    partition_result = sql """show table stats `$tbl`"""
    assertEquals(partition_result[0][6], "true")
    sql """analyze table `$tbl`(id) with sync"""
    partition_result = sql """show table stats `$tbl`"""
    assertEquals(partition_result[0][6], "false")
}
