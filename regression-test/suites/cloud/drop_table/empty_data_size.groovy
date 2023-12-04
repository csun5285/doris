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
import groovy.json.JsonOutput
import groovy.json.JsonSlurper

/**
*   @Params url is "/xxx", data is request body
*   @Return response body
*/
def http_get(url, data = null) {
    def dst = "http://"+ context.config.feHttpAddress
    def conn = new URL(dst + url).openConnection()
    conn.setRequestMethod("GET")
    conn.setRequestProperty("Content-Type", "application/json")
    conn.setRequestProperty("Authorization", "Basic cm9vdDo=")
    if (data) {
        // 
        logger.info("query body: " + data)
        conn.doOutput = true
        def writer = new OutputStreamWriter(conn.outputStream)
        writer.write(data)
        writer.flush()
        writer.close()
    }
    return conn.content.text
}

suite("empty_data_size") {
    def url= "/metrics/"

    def dbName = "regression_test_cloud_drop_table"
    def oldTblName = "table_emtpy_data"
    def newTblName = "new_table_empty_data"

    sql """ DROP TABLE IF EXISTS ${oldTblName} FORCE"""
    sql """ DROP TABLE IF EXISTS ${newTblName} FORCE"""

    def oldMetricsStr = """doris_fe_table_data_size{db_name="${dbName}", table_name="${oldTblName}"}"""
    def newMetricsStr = """doris_fe_table_data_size{db_name="${dbName}", table_name="${newTblName}"}"""
    // data size metrics of the new table should present
    def long start = System.currentTimeMillis()
    def boolean containsOldTable = true
    def boolean containsNewTable = true
    def long current = -1

    while (containsOldTable && current - start < 600000) {
        def resJson = http_get(url)
        containsOldTable = resJson.contains(oldMetricsStr)
        containsNewTable = resJson.contains(newMetricsStr)
        current = System.currentTimeMillis()
        sleep(10000)
    }
    assertTrue(current > 0)
    assertFalse(containsOldTable)
    assertFalse(containsNewTable)

    sql """
        CREATE TABLE IF NOT EXISTS ${oldTblName} (
        `test_varchar` varchar(150) NULL,
        `test_datetime` datetime NULL,
        `test_default_timestamp` datetime DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=OLAP
        UNIQUE KEY(`test_varchar`)
        DISTRIBUTED BY HASH(`test_varchar`) BUCKETS 3
    """

    sql """ INSERT INTO ${oldTblName}(test_varchar, test_datetime) VALUES ('test1','2022-04-27 16:00:33'),('test2','2022-04-27 16:00:54') """

    start = System.currentTimeMillis()
    containsOldTable = false
    current = -1
    while (!containsOldTable && current - start < 600000) {
        def resJson = http_get(url)
        containsOldTable = resJson.contains(oldMetricsStr)
        current = System.currentTimeMillis()
        sleep(10000)
    }
    assertTrue(current > 0)
    assertTrue(containsOldTable)

    sql """ alter table ${oldTblName} rename ${newTblName} """
    start = System.currentTimeMillis()
    containsOldTable = true
    containsNewTable = false
    current = -1

    while (containsOldTable && current - start < 600000) {
        def resJson = http_get(url)
        // def res = new JsonSlurper().parseText(resJson)
        containsOldTable = resJson.contains(oldMetricsStr)
        containsNewTable = resJson.contains(newMetricsStr)
        current = System.currentTimeMillis()
        sleep(10000)
    }
    assertTrue(current > 0)
    assertFalse(containsOldTable)
    assertTrue(containsNewTable)

    sql """ DROP TABLE ${newTblName} FORCE"""

    // data size metrics of the new table should disappear 
    start = System.currentTimeMillis()
    containsOldTable = true
    containsNewTable = true
    current = -1
    while (containsNewTable && current - start < 600000) {
        def resJson = http_get(url)
        containsOldTable = resJson.contains(oldMetricsStr)
        containsNewTable = resJson.contains(newMetricsStr)
        current = System.currentTimeMillis()
        sleep(10000)
    }
    assertTrue(current > 0)
    assertFalse(containsOldTable)
    assertFalse(containsNewTable)

    sql """ DROP TABLE IF EXISTS ${oldTblName} FORCE """
    sql """ DROP TABLE IF EXISTS ${newTblName} FORCE """
}
