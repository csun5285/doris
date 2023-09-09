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
def http_post(url, data = null) {
    def dst = "http://"+ context.config.feHttpAddress
    def conn = new URL(dst + url).openConnection()
    conn.setRequestMethod("POST")
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

def SUCCESS_MSG = "success"
def SUCCESS_CODE = 0

class Stmt {
    String stmt
}
suite("empty_data_size") {
    def url= "/metrics/"

    def tableName = "table_emtpy_data"

    sql """ DROP TABLE IF EXISTS ${tableName} """

    sql """
    CREATE TABLE IF NOT EXISTS ${tableName} (
      `test_varchar` varchar(150) NULL,
      `test_datetime` datetime NULL,
      `test_default_timestamp` datetime DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=OLAP
    UNIQUE KEY(`test_varchar`)
    DISTRIBUTED BY HASH(`test_varchar`) BUCKETS 3
    """

    sql """ INSERT INTO ${tableName}(test_varchar, test_datetime) VALUES ('test1','2022-04-27 16:00:33'),('test2','2022-04-27 16:00:54') """

    // ensure fe will get tablet info from ms
    sleep(65000)

    def resJson = http_post(url)
    // def res = new JsonSlurper().parseText(resJson)
    assertTrue(resJson.contains("db_name=\"regression_test_cloud_drop_table\", table_name=\"table_emtpy_data\""))

    sql """ DROP TABLE ${tableName} """
    resJson = http_post(url)
    assertTrue(!resJson.contains("db_name=\"regression_test_cloud_drop_table\", table_name=\"table_emtpy_data\""))

    sql """ DROP TABLE IF EXISTS ${tableName} FORCE """
}