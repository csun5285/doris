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

suite("test_match_null_empty", "p0") {


    def load_data = { tableName ->
        streamLoad {
            table "${tableName}"
            // set http request header params
            set 'label', "${tableName}" + UUID.randomUUID().toString()
            set 'read_json_by_line', 'true'
            set 'format', 'json'
            file 'documents-1000.json' // import json file
            time 10000 // limit inflight 10s
            // if declared a check callback, the default check condition will ignore.
            // So you must check all condition
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
            }
        }
    }

    def testTable = "httplogs"
    sql "DROP TABLE IF EXISTS ${testTable}"
    sql """
        CREATE TABLE IF NOT EXISTS ${testTable} (
            `@timestamp` int(11) NULL,
            `clientip` varchar(20) NULL,
            `request` text NULL,
            `status` int(11) NULL,
            `size` int(11) NULL,
            INDEX size_idx (`size`) USING INVERTED COMMENT '',
            INDEX status_idx (`status`) USING INVERTED COMMENT '',
            INDEX clientip_idx (`clientip`) USING INVERTED COMMENT '',
            INDEX request_idx (`request`) USING INVERTED PROPERTIES("parser"="english") COMMENT ''
        ) ENGINE=OLAP
        DUPLICATE KEY(`@timestamp`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`@timestamp`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    load_data.call(testTable)
    sql """ insert into ${testTable} values (100, '10.16.10.6', 'GET /api/v1 HTTP', 200, 200); """
    sql """ insert into ${testTable} values (100, NULL, 'GET /api/v1 HTTP', 200, 200); """
    sql """ insert into ${testTable} values (100, '10.16.10.6', NULL, 200, 200); """
    sql """ insert into ${testTable} values (100, '10.16.10.6', 'GET /api/v1 HTTP', NULL, 200); """
    sql """ insert into ${testTable} values (100, '', '', 200, 200); """

    sql """set enable_no_need_read_data_opt = false"""
    def result2 = sql """ select count() from ${testTable} where request match 'GET' and request like '%GET%'; """
    logger.info("result2 is {}", result2)

    sql """set enable_no_need_read_data_opt = true"""
    def result1 = sql """ select count() from ${testTable} where request match 'GET' and request like '%GET%'; """
    logger.info("result1 is {}", result1)


    assertEquals(result1, result2)
}
