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

suite("test_unique_table_insert", "load") {
    // define a sql table
    def testTable1 = "tbl_test_unique_table_insert01"
    def testTable2 = "tbl_test_unique_table_insert02"

    try {
        sql "DROP TABLE IF EXISTS ${testTable1}"
        sql "DROP TABLE IF EXISTS ${testTable2}"

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

        sql """
            CREATE TABLE `${testTable2}` (
            `partnercode` varchar(100) NULL COMMENT 'partnercode',
            `cookieEnabled` text NULL COMMENT 'auto change 2019-09-15T17:32:40Z[Etc/UCT]',
            `sequenceid` varchar(64) NULL COMMENT 'auto change 2019-09-15T17:32:40Z[Etc/UCT]'
            ) ENGINE = OLAP DUPLICATE KEY(`partnercode`) 
            COMMENT 'OLAP' 
            DISTRIBUTED BY HASH(`partnercode`) 
            BUCKETS 10;
        """
        sql " INSERT INTO ${testTable2} (partnercode, cookieEnabled, sequenceid) VALUES ('partnercode_1098', 'coock1', 'seq1'); "
        sql " insert into ${testTable1} select * from ${testTable2}; "
        qt_sql """ select * from ${testTable1}; """
        sql " update ${testTable1} set cookieEnabled = 'coock2' where sequenceid like 'partnercode_1098'; "
        qt_sql """ select * from ${testTable1}; """
    } finally {
        try_sql("DROP TABLE IF EXISTS ${testTable1}")
        try_sql("DROP TABLE IF EXISTS ${testTable2}")
    }
}
