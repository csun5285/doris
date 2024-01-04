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

suite("test_time_series_compaction_polciy", "p0") {
    def tableName = "test_time_series_compaction_polciy"
    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

    def trigger_cumulative_compaction_on_tablets = { String[][] tablets ->
        for (String[] tablet : tablets) {
            String tablet_id = tablet[0]
            backend_id = tablet[2]
            times = 1
            
            String compactionStatus;
            do{
                def (code, out, err) = be_run_cumulative_compaction(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
                logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
                ++times
                sleep(1000)
                compactionStatus = parseJson(out.trim()).status.toLowerCase();
            } while (compactionStatus!="success" && times<=3)
            if (compactionStatus!="success") {
                assertTrue(compactionStatus.contains("2000"))
                continue;
            }
            assertEquals("success", compactionStatus)
        }
    }

    def wait_cumulative_compaction_done = { String[][] tablets ->
        for (String[] tablet in tablets) {
            boolean running = true
            do {
                Thread.sleep(1000)
                String tablet_id = tablet[0]
                backend_id = tablet[2]
                (code, out, err) = be_get_compaction_status(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
                logger.info("Get compaction status: code=" + code + ", out=" + out + ", err=" + err)
                assertEquals(code, 0)
                def compactionStatus = parseJson(out.trim())
                assertEquals("success", compactionStatus.status.toLowerCase())
                running = compactionStatus.run_status
            } while (running)
        }
    }

    def get_rowset_count = {String[][] tablets ->
        int rowsetCount = 0
        for (String[] tablet in tablets) {
            String tablet_id = tablet[0]
            def compactionStatusUrlIndex = 18
            (code, out, err) = curl("GET", tablet[compactionStatusUrlIndex])
            logger.info("Show tablet status: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            def tabletJson = parseJson(out.trim())
            assert tabletJson.rowsets instanceof List
            rowsetCount +=((List<String>) tabletJson.rowsets).size()
        }
        return rowsetCount
    }

    String backend_id;
    backend_id = backendId_to_backendIP.keySet()[0]
    def (code, out, err) = show_be_config(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id))
    
    logger.info("Show config: code=" + code + ", out=" + out + ", err=" + err)
    assertEquals(code, 0)

    sql """ DROP TABLE IF EXISTS ${tableName}; """
    sql """
        CREATE TABLE ${tableName} (
            `id` int(11) NULL,
            `name` varchar(255) NULL,
            `hobbies` text NULL,
            `score` int(11) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 2
        PROPERTIES ( "replication_num" = "1", "disable_auto_compaction" = "true", "compaction_policy" = "time_series");
    """
    // insert 16 lines, BUCKETS = 2
    sql """ INSERT INTO ${tableName} VALUES (1, "andy", "andy love apple", 100); """
    sql """ INSERT INTO ${tableName} VALUES (1, "bason", "bason hate pear", 99); """
    sql """ INSERT INTO ${tableName} VALUES (1, "andy", "andy love apple", 100); """
    sql """ INSERT INTO ${tableName} VALUES (1, "bason", "bason hate pear", 99); """
    sql """ INSERT INTO ${tableName} VALUES (1, "andy", "andy love apple", 100); """
    sql """ INSERT INTO ${tableName} VALUES (100, "andy", "andy love apple", 100); """
    sql """ INSERT INTO ${tableName} VALUES (100, "bason", "bason hate pear", 99); """
    sql """ INSERT INTO ${tableName} VALUES (100, "andy", "andy love apple", 100); """
    sql """ INSERT INTO ${tableName} VALUES (100, "bason", "bason hate pear", 99); """
    sql """ INSERT INTO ${tableName} VALUES (100, "andy", "andy love apple", 100); """
    sql """ INSERT INTO ${tableName} VALUES (1, "andy", "andy love apple", 100); """
    sql """ INSERT INTO ${tableName} VALUES (1, "bason", "bason hate pear", 99); """
    sql """ INSERT INTO ${tableName} VALUES (1, "andy", "andy love apple", 100); """
    sql """ INSERT INTO ${tableName} VALUES (1, "bason", "bason hate pear", 99); """
    sql """ INSERT INTO ${tableName} VALUES (1, "andy", "andy love apple", 100); """
    sql """ INSERT INTO ${tableName} VALUES (100, "andy", "andy love apple", 100); """
    
    qt_sql """ select count() from ${tableName} """

    //TabletId,ReplicaId,BackendId,SchemaHash,Version,LstSuccessVersion,LstFailedVersion,LstFailedTime,LocalDataSize,RemoteDataSize,RowCount,State,LstConsistencyCheckTime,CheckVersion,VersionCount,PathHash,MetaUrl,CompactionStatus
    String[][] tablets = sql """ show tablets from ${tableName}; """

    // BUCKETS = 2
    // before cumulative compaction, there are 17 * 2 = 34 rowsets.
    int rowsetCount = get_rowset_count.call(tablets);
    assert (rowsetCount == 34)

    // trigger cumulative compactions for all tablets in table
    trigger_cumulative_compaction_on_tablets.call(tablets)

    // wait for cumulative compaction done
    wait_cumulative_compaction_done.call(tablets)

    // after cumulative compaction, there is only 26 rowset.
    // 5 consecutive empty versions are merged into one empty version
    // 34 - 2*4 = 26
    rowsetCount = get_rowset_count.call(tablets);
    assert (rowsetCount == 26)

    // trigger cumulative compactions for all tablets in ${tableName}
    trigger_cumulative_compaction_on_tablets.call(tablets)

    // wait for cumulative compaction done
    wait_cumulative_compaction_done.call(tablets)

    // after cumulative compaction, there is only 22 rowset.
    // 26 - 4 = 22
    rowsetCount = get_rowset_count.call(tablets);
    assert (rowsetCount == 22)

    qt_sql """ select count() from ${tableName}"""
}
