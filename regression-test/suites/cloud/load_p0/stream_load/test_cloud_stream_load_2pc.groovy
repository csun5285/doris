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

suite("test_cloud_stream_load_2pc", "p0") {
    // test common case
    def tableName3 = "test_all"

    sql """ drop table if exists ${tableName3} """

    sql """
    CREATE TABLE IF NOT EXISTS ${tableName3} (
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
    DISTRIBUTED BY HASH(`k1`) BUCKETS 3;
    """

    long txnId = -1;
    // 2pc precommit
    streamLoad {
        table "${tableName3}"

        set 'column_separator', ','
        set 'two_phase_commit', 'true'

        file 'all_types.csv'
        time 10000 // limit inflight 10s
        isCloud true

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(2500, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
            txnId = json.TxnId
        }
    }
    sql "sync"
    order_qt_all11 "SELECT count(*) FROM ${tableName3}" // 0
    order_qt_all12 "SELECT count(*) FROM ${tableName3} where k1 <= 10"  // 0

    // 2pc commit
    streamLoad {
        table "${tableName3}"

        set 'column_separator', ','
        set 'txn_id', txnId.toString()
        set 'txn_operation', 'commit'

        file 'all_types.csv'
        twoPhaseCommit true
        isCloud true

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load 2pc commit result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.status.toLowerCase())
        }
    }

    order_qt_all11 "SELECT count(*) FROM ${tableName3}" // 2500
    order_qt_all12 "SELECT count(*) FROM ${tableName3} where k1 <= 10"  // 11
}

