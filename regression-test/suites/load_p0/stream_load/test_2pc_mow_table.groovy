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

import org.apache.http.client.methods.RequestBuilder;

suite("test_2pc_mow_table", "p0") {
    def tables = [
                  "two_pc_mow_tbl_basic",
                  "two_pc_mow_tbl_array",
                 ]

    def columns = [ 
                    "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18",
                    "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18",
                  ]

    def files = [
                  "basic_data.csv",
                  "basic_array_data.csv"
                ]

    InetSocketAddress address = context.config.feHttpInetSocketAddress
    String user = context.config.feHttpUser
    String password = context.config.feHttpPassword
    String db = context.config.getDbNameByFile(context.file)

    sql """ DROP TABLE IF EXISTS ${tables[0]} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tables[0]} (
            k00 INT             NOT NULL,
            k01 DATE            NULL,
            k02 BOOLEAN         NULL,
            k03 TINYINT         NULL,
            k04 SMALLINT        NULL,
            k05 INT             NULL,
            k06 BIGINT          NULL,
            k07 LARGEINT        NULL,
            k08 FLOAT           NULL,
            k09 DOUBLE          NULL,
            k10 DECIMAL(9,1)    NULL,
            k11 DECIMALV3(9,1)  NULL,
            k12 DATETIME        NULL,
            k13 DATEV2          NULL,
            k14 DATETIMEV2      NULL,
            k15 CHAR            NULL,
            k16 VARCHAR         NULL,
            k17 STRING          NULL,
            k18 JSON            NULL,
            kd01 BOOLEAN         NOT NULL DEFAULT "TRUE",
            kd02 TINYINT         NOT NULL DEFAULT "1",
            kd03 SMALLINT        NOT NULL DEFAULT "2",
            kd04 INT             NOT NULL DEFAULT "3",
            kd05 BIGINT          NOT NULL DEFAULT "4",
            kd06 LARGEINT        NOT NULL DEFAULT "5",
            kd07 FLOAT           NOT NULL DEFAULT "6.0",
            kd08 DOUBLE          NOT NULL DEFAULT "7.0",
            kd09 DECIMAL         NOT NULL DEFAULT "888888888",
            kd10 DECIMALV3       NOT NULL DEFAULT "999999999",
            kd11 DATE            NOT NULL DEFAULT "2023-08-24",
            kd12 DATETIME        NOT NULL DEFAULT "2023-08-24 12:00:00",
            kd13 DATEV2          NOT NULL DEFAULT "2023-08-24",
            kd14 DATETIMEV2      NOT NULL DEFAULT "2023-08-24 12:00:00",
            kd15 CHAR(300)            NOT NULL DEFAULT "我能吞下玻璃而不伤身体",
            kd16 VARCHAR(300)         NOT NULL DEFAULT "我能吞下玻璃而不伤身体",
            kd17 STRING          NOT NULL DEFAULT "我能吞下玻璃而不伤身体",
            kd18 JSON            NULL,

            INDEX idx_inverted_k104 (`k05`) USING INVERTED,
            INDEX idx_inverted_k110 (`k11`) USING INVERTED,
            INDEX idx_inverted_k113 (`k13`) USING INVERTED,
            INDEX idx_inverted_k114 (`k14`) USING INVERTED,
            INDEX idx_inverted_k117 (`k17`) USING INVERTED PROPERTIES("parser" = "english"),
            INDEX idx_bitmap_k104 (`k05`) USING BITMAP,
            INDEX idx_bitmap_k110 (`k11`) USING BITMAP,
            INDEX idx_bitmap_k113 (`k13`) USING BITMAP,
            INDEX idx_bitmap_k114 (`k14`) USING BITMAP,
            INDEX idx_bitmap_k117 (`k17`) USING BITMAP,
            INDEX idx_ngrambf_k115 (`k15`) USING NGRAM_BF PROPERTIES("gram_size"="3", "bf_size"="256"),
            INDEX idx_ngrambf_k116 (`k16`) USING NGRAM_BF PROPERTIES("gram_size"="3", "bf_size"="256"),
            INDEX idx_ngrambf_k117 (`k17`) USING NGRAM_BF PROPERTIES("gram_size"="3", "bf_size"="256")
        )
        UNIQUE KEY(k00,k01)
        PARTITION BY RANGE(k01)
        (
            PARTITION p1 VALUES [('2023-08-01'), ('2023-08-11')),
            PARTITION p2 VALUES [('2023-08-11'), ('2023-08-21')),
            PARTITION p3 VALUES [('2023-08-21'), ('2023-09-01'))
        )
        DISTRIBUTED BY HASH(k00) BUCKETS 32
        PROPERTIES (
            "bloom_filter_columns"="k05",
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """ DROP TABLE IF EXISTS ${tables[1]} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tables[1]} (
            k00 INT                    NOT NULL,
            k01 array<BOOLEAN>         NULL,
            k02 array<TINYINT>         NULL,
            k03 array<SMALLINT>        NULL,
            k04 array<INT>             NULL,
            k05 array<BIGINT>          NULL,
            k06 array<LARGEINT>        NULL,
            k07 array<FLOAT>           NULL,
            k08 array<DOUBLE>          NULL,
            k09 array<DECIMAL>         NULL,
            k10 array<DECIMALV3>       NULL,
            k11 array<DATE>            NULL,
            k12 array<DATETIME>        NULL,
            k13 array<DATEV2>          NULL,
            k14 array<DATETIMEV2>      NULL,
            k15 array<CHAR>            NULL,
            k16 array<VARCHAR>         NULL,
            k17 array<STRING>          NULL,
            kd01 array<BOOLEAN>         NOT NULL DEFAULT "[]",
            kd02 array<TINYINT>         NOT NULL DEFAULT "[]",
            kd03 array<SMALLINT>        NOT NULL DEFAULT "[]",
            kd04 array<INT>             NOT NULL DEFAULT "[]",
            kd05 array<BIGINT>          NOT NULL DEFAULT "[]",
            kd06 array<LARGEINT>        NOT NULL DEFAULT "[]",
            kd07 array<FLOAT>           NOT NULL DEFAULT "[]",
            kd08 array<DOUBLE>          NOT NULL DEFAULT "[]",
            kd09 array<DECIMAL>         NOT NULL DEFAULT "[]",
            kd10 array<DECIMALV3>       NOT NULL DEFAULT "[]",
            kd11 array<DATE>            NOT NULL DEFAULT "[]",
            kd12 array<DATETIME>        NOT NULL DEFAULT "[]",
            kd13 array<DATEV2>          NOT NULL DEFAULT "[]",
            kd14 array<DATETIMEV2>      NOT NULL DEFAULT "[]",
            kd15 array<CHAR>            NOT NULL DEFAULT "[]",
            kd16 array<VARCHAR>         NOT NULL DEFAULT "[]",
            kd17 array<STRING>          NOT NULL DEFAULT "[]"
        )
        UNIQUE KEY(k00)
        DISTRIBUTED BY HASH(k00) BUCKETS 32
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    // two_phase_commit
    def do_streamload_2pc = { txn_id, txn_operation, tableName->
        HttpClients.createDefault().withCloseable { client ->
            RequestBuilder requestBuilder = RequestBuilder.put("http://${address.hostString}:${address.port}/api/${db}/${tableName}/_stream_load_2pc")
            String encoding = Base64.getEncoder()
                .encodeToString((user + ":" + (password == null ? "" : password)).getBytes("UTF-8"))
            requestBuilder.setHeader("Authorization", "Basic ${encoding}")
            requestBuilder.setHeader("Expect", "100-Continue")
            requestBuilder.setHeader("txn_id", "${txn_id}")
            requestBuilder.setHeader("txn_operation", "${txn_operation}")

            String backendStreamLoadUri = null
            client.execute(requestBuilder.build()).withCloseable { resp ->
                resp.withCloseable {
                    String body = EntityUtils.toString(resp.getEntity())
                    def respCode = resp.getStatusLine().getStatusCode()
                    // should redirect to backend
                    if (respCode != 307) {
                        throw new IllegalStateException("Expect frontend stream load response code is 307, " +
                                "but meet ${respCode}\nbody: ${body}")
                    }
                    backendStreamLoadUri = resp.getFirstHeader("location").getValue()
                }
            }

            requestBuilder.setUri(backendStreamLoadUri)
            try{
                client.execute(requestBuilder.build()).withCloseable { resp ->
                    resp.withCloseable {
                        String body = EntityUtils.toString(resp.getEntity())
                        def respCode = resp.getStatusLine().getStatusCode()
                        if (respCode != 200) {
                            throw new IllegalStateException("Expect backend stream load response code is 200, " +
                                    "but meet ${respCode}\nbody: ${body}")
                        }
                    }
                }
            } catch (Throwable t) {
                log.info("StreamLoad Exception: ", t)
            }
        }
    }

    def i = 0
    try {
        for (String tableName in tables) {
            String txnId
            streamLoad {
                table tableName
                set 'column_separator', '|'
                set 'columns', columns[i]
                set 'two_phase_commit', 'true'
                file files[i]
                time 10000 // limit inflight 10s

                check { result, exception, startTime, endTime ->
                    if (exception != null) {
                        throw exception
                    }
                    log.info("Stream load result: ${result}".toString())
                    def json = parseJson(result)
                    txnId = json.TxnId
                    assertEquals("success", json.Status.toLowerCase())
                    assertEquals(20, json.NumberTotalRows)
                    assertEquals(20, json.NumberLoadedRows)
                    assertEquals(0, json.NumberFilteredRows)
                    assertEquals(0, json.NumberUnselectedRows)
                }
            }

            if (i <= 3) {
                qt_sql_2pc "select * from ${tableName} order by k00,k01"
            } else {
                qt_sql_2pc "select * from ${tableName} order by k00"
            }

            do_streamload_2pc.call(txnId, "abort", tableName)

            if (i <= 3) {
                qt_sql_2pc_abort "select * from ${tableName} order by k00,k01"
            } else {
                qt_sql_2pc_abort "select * from ${tableName} order by k00"
            }

            streamLoad {
                table tableName
                set 'column_separator', '|'
                set 'columns', columns[i]
                set 'two_phase_commit', 'true'
                file files[i]
                time 10000 // limit inflight 10s

                check { result, exception, startTime, endTime ->
                    if (exception != null) {
                        throw exception
                    }
                    log.info("Stream load result: ${result}".toString())
                    def json = parseJson(result)
                    txnId = json.TxnId
                    assertEquals("success", json.Status.toLowerCase())
                    assertEquals(20, json.NumberTotalRows)
                    assertEquals(20, json.NumberLoadedRows)
                    assertEquals(0, json.NumberFilteredRows)
                    assertEquals(0, json.NumberUnselectedRows)
                }
            }

            do_streamload_2pc.call(txnId, "commit", tableName)

            def count = 0
            while (true) {
                def res
                if (i <= 3) {
                    res = sql "select count(*) from ${tableName}"
                } else {
                    res = sql "select count(*) from ${tableName}"
                }
                if (res[0][0] > 0) {
                    break
                }
                if (count >= 60) {
                    log.error("stream load commit can not visible for long time")
                    assertEquals(20, res[0][0])
                    break
                }
                sleep(1000)
                count++
            }
            
            if (i <= 3) {
                qt_sql_2pc_commit "select * from ${tableName} order by k00,k01"
            } else {
                qt_sql_2pc_commit "select * from ${tableName} order by k00"
            }

            i++
        }
    } finally {
        for (String tableName in tables) {
            sql "DROP TABLE IF EXISTS ${tableName} FORCE"
        }
    }
}