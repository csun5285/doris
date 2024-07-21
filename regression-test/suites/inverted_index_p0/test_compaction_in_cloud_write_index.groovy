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

import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_compaction_in_cloud_write_index") {
    def tableName = "test_compaction_in_cloud_write_index"

    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

    sql """
    CREATE TABLE ${tableName} (
    `@timestamp` int(11) NULL COMMENT "",
    `clientip` varchar(20) NULL COMMENT "",
    `request` text NULL COMMENT "",
    `status` int(11) NULL COMMENT "",
    `size` int(11) NULL COMMENT ""
    ) ENGINE=OLAP
    DUPLICATE KEY(`@timestamp`)
    COMMENT "OLAP"
    DISTRIBUTED BY RANDOM BUCKETS 1
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );
    """

    def load_httplogs_data = {table_name, label, read_flag, format_flag, file_name, ignore_failure=false,
                        expected_succ_rows = -1, load_to_single_tablet = 'true' ->
        
        // load the json data
        streamLoad {
            table "${table_name}"
            
            // set http request header params
            set 'label', label + "_" + UUID.randomUUID().toString()
            set 'read_json_by_line', read_flag
            set 'format', format_flag
            file file_name // import json file
            time 10000 // limit inflight 10s
            if (expected_succ_rows >= 0) {
                set 'max_filter_ratio', '1'
            }

            // if declared a check callback, the default check condition will ignore.
            // So you must check all condition
            check { result, exception, startTime, endTime ->
                if (ignore_failure && expected_succ_rows < 0) { return }
                    if (exception != null) {
                        throw exception
                    }
                    log.info("Stream load result: ${result}".toString())
                    def json = parseJson(result)
                    assertEquals("success", json.Status.toLowerCase())
                    if (expected_succ_rows >= 0) {
                        assertEquals(json.NumberLoadedRows, expected_succ_rows)
                    } else {
                        assertEquals(json.NumberTotalRows, json.NumberLoadedRows + json.NumberUnselectedRows)
                        assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
                }
            }
        }
    }

    def set_be_config = { key, value ->

        for (String backend_id: backendId_to_backendIP.keySet()) {
            def (code, out, err) = update_be_config(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), key, value)
            logger.info("update config: code=" + code + ", out=" + out + ", err=" + err)
        }
    }

    load_httplogs_data.call(test_compaction_in_cloud_write_index, "1", 'true', 'json', 'documents-1000.json')
    load_httplogs_data.call(test_compaction_in_cloud_write_index, "2", 'true', 'json', 'documents-1000.json')
    load_httplogs_data.call(test_compaction_in_cloud_write_index, "3", 'true', 'json', 'documents-1000.json')
    load_httplogs_data.call(test_compaction_in_cloud_write_index, "4", 'true', 'json', 'documents-1000.json')
    load_httplogs_data.call(test_compaction_in_cloud_write_index, "5", 'true', 'json', 'documents-1000.json')
    load_httplogs_data.call(test_compaction_in_cloud_write_index, "6", 'true', 'json', 'documents-1000.json')
    set_be_config.call("vertical_compaction_max_segment_size", "1024000")

    //TabletId,ReplicaId,BackendId,SchemaHash,Version,LstSuccessVersion,LstFailedVersion,LstFailedTime,LocalDataSize,RemoteDataSize,RowCount,State,LstConsistencyCheckTime,CheckVersion,VersionCount,QueryHits,PathHash,MetaUrl,CompactionStatus
    def tablets = sql_return_maparray """ show tablets from ${tableName}; """

    // trigger compactions for all tablets in ${tableName}
    for (def tablet in tablets) {
        String tablet_id = tablet.TabletId
        backend_id = tablet.BackendId
        (code, out, err) = be_run_full_compaction(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
        logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)

        assertEquals(code, 0)
        def compactJson = parseJson(out.trim())
        if (compactJson.status.toLowerCase() == "fail") {
            logger.info("Compaction was done automatically!")
        }
    }

    // wait for all compactions done
    for (def tablet in tablets) {
        boolean running = true
        do {
            Thread.sleep(1000)
            String tablet_id = tablet.TabletId
            backend_id = tablet.BackendId
            (code, out, err) = be_get_compaction_status(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
            logger.info("Get compaction status: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            def compactionStatus = parseJson(out.trim())
            assertEquals("success", compactionStatus.status.toLowerCase())
            running = compactionStatus.run_status
        } while (running)
    }

}
