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
import java.util.Date
import java.text.SimpleDateFormat

suite("truncate_table", "p0") {
    sql "show tables"

    def tableName = "test_truncate_table"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `k1` int(11) NULL,
            `k2` tinyint(4) NULL,
            `k3` smallint(6) NULL,
            `k4` bigint(20) NULL,
            `k5` largeint(40) NULL,
            `k6` float NULL,
            `k7` double NULL,
            `k8` decimal(9, 0) NULL,
            `k9` char(10) NULL,
            `k10` varchar(1024) NULL,
            `k11` text NULL,
            `k12` date NULL,
            `k13` datetime NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`k1`, `k2`)
        COMMENT 'OLAP'
        PARTITION BY RANGE(`k1`)
        (PARTITION partition_a values less than ("0"))
        DISTRIBUTED BY HASH(`k1`) BUCKETS 3
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """

    for (int i = 1; i <= 3600; ++i) {
        sql """ ALTER TABLE ${tableName} ADD PARTITION p${i} VALUES LESS THAN ("${i}"); """
    }

    sql """ insert into ${tableName} values (0,2,3,4,5,6.6,7.7,8.8,   'abc','def','ghiaaaaaa','2020-10-10',"2020-10-10 11:12:59") """
    sql """ insert into ${tableName} values (101,2,3,4,5,6.6,7.7,8.8, 'abc','def','ghiaaaaaa','2020-10-10',"2020-10-10 11:12:59") """
    sql """ insert into ${tableName} values (1001,2,3,4,5,6.6,7.7,8.8,'abc','def','ghiaaaaaa','2020-10-10',"2020-10-10 11:12:59") """
    sql """ insert into ${tableName} values (2020,2,3,4,5,6.6,7.7,8.8,'abc','def','ghiaaaaaa','2020-10-10',"2020-10-10 11:12:59") """
    sql """ insert into ${tableName} values (3111,2,3,4,5,6.6,7.7,8.8,'abc','def','ghiaaaaaa','2020-10-10',"2020-10-10 11:12:59") """

    qt_select1 """ select count(*) from ${tableName} """

    sql """ truncate table ${tableName} """

    qt_select2 """ select count(*) from ${tableName} """

    sql """ insert into ${tableName} values (0,2,3,4,5,6.6,7.7,8.8,   'abc','def','ghiaaaaaa','2020-10-10',"2020-10-10 11:12:59") """
    sql """ insert into ${tableName} values (101,2,3,4,5,6.6,7.7,8.8, 'abc','def','ghiaaaaaa','2020-10-10',"2020-10-10 11:12:59") """
    sql """ insert into ${tableName} values (1001,2,3,4,5,6.6,7.7,8.8,'abc','def','ghiaaaaaa','2020-10-10',"2020-10-10 11:12:59") """
    sql """ insert into ${tableName} values (2020,2,3,4,5,6.6,7.7,8.8,'abc','def','ghiaaaaaa','2020-10-10',"2020-10-10 11:12:59") """
    sql """ insert into ${tableName} values (3111,2,3,4,5,6.6,7.7,8.8,'abc','def','ghiaaaaaa','2020-10-10',"2020-10-10 11:12:59") """

    qt_select3 """ select count(*) from ${tableName} """
}

