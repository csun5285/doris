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

suite("test_cloud_partition_visible_update", "load") {
    // define a sql table
    def testTable1 = "tbl_test_cloud_partition_visible_update_01"

    try {
        sql "DROP TABLE IF EXISTS ${testTable1}"

        sql """
            CREATE TABLE `${testTable1}` (
            `sequenceid` varchar(64) NULL COMMENT 'auto change 2019-09-15T17:32:40Z[Etc/UCT]',
            `partnercode` varchar(100) NULL COMMENT 'partnercode',
            `cookieEnabled` text NULL COMMENT 'auto change 2019-09-15T17:32:40Z[Etc/UCT]'
            ) ENGINE = OLAP UNIQUE KEY(`sequenceid`, `partnercode`) 
            COMMENT 'OLAP' 
            DISTRIBUTED BY HASH(`partnercode`) 
            BUCKETS 10;
        """

        def partition_infos = sql """
        show partitions from `${testTable1}`
        """
        def origin_partition_visible_time = partition_infos[0][3]

        sql " INSERT INTO ${testTable1} (partnercode, cookieEnabled, sequenceid) VALUES ('partnercode_1098', 'coock1', 'seq1'); "

        partition_infos = sql """
        show partitions from `${testTable1}`
        """

        def current_partition_visible_time = partition_infos[0][3]
        logger.info("origin partition visible time: " + origin_partition_visible_time + " current: " + current_partition_visible_time)
        assertTrue(origin_partition_visible_time != partition_infos)
    } finally {
        try_sql("DROP TABLE IF EXISTS ${testTable1}")
    }
}
