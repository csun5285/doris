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
    String stmt;
    Boolean enable_nereids_planner = true;
}
suite("test_readonly_stmt") {
    result = sql """ SELECT DATABASE(); """
    def url= "/api/show/" + result[0][0]


    def tableName = "table_readonly"
    def tableName2 = "table_notread"

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

    // test select
    def stmt1 = """ 
    SELECT * FROM ${tableName}
    """
    def stmt1_json = JsonOutput.toJson(new Stmt(stmt: stmt1));

    def resJson = http_post(url, stmt1_json)
    def obj = new JsonSlurper().parseText(resJson)

    logger.info("stmt1 msg is {}", obj.msg)
    logger.info("stmt1 code is {}", obj.code)
    assertEquals(obj.code, 1)

    // test show databases
    sql """ show databases """
    def stmt2 = """ show databases """
    def stmt2_json = JsonOutput.toJson(new Stmt(stmt: stmt2));

    resJson = http_post(url, stmt2_json)
    obj = new JsonSlurper().parseText(resJson)

    logger.info("stmt2 msg is {}", obj.msg)
    logger.info("stmt2 code is {}", obj.code)
    logger.info("stmt2 data size is {}", obj.data.data.size)
    logger.info("stmt2 data is {}", obj.data.data)

    assertEquals(obj.code, 0)

    // test show columns
    def ret = sql """ SHOW FULL COLUMNS FROM ${tableName} """
    logger.info("stmt3 from mysql is {}", ret)
    def stmt3 = """ SHOW FULL COLUMNS FROM ${tableName} """
    def stmt3_json = JsonOutput.toJson(new Stmt(stmt: stmt3));

    resJson = http_post(url, stmt3_json)
    obj = new JsonSlurper().parseText(resJson)

    logger.info("stmt3 msg is {}", obj.msg)
    logger.info("stmt3 code is {}", obj.code)
    logger.info("stmt3 data is {}", obj.data.data)

    assertEquals(obj.code, 0)

    sql """
    CREATE TABLE IF NOT EXISTS ${tableName2} (
      `test_varchar` varchar(150) NULL,
      `test_datetime` datetime NULL,
      `test_default_timestamp` datetime DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=OLAP
    UNIQUE KEY(`test_varchar`)
    DISTRIBUTED BY HASH(`test_varchar`) BUCKETS 3
    """

    def stmt4 = """ SHOW DATABASES like 'regression%' """
    def stmt4_json = JsonOutput.toJson(new Stmt(stmt: stmt4));

    resJson = http_post(url, stmt4_json)
    obj = new JsonSlurper().parseText(resJson)

    logger.info("stmt4 msg is {}", obj.msg)
    logger.info("stmt4 code is {}", obj.code)
    logger.info("stmt4 data is {}", obj.data.data)

    assertEquals(obj.code, 0)

    def stmt5 = """ explain  select * from ${tableName} """
    def stmt5_json = JsonOutput.toJson(new Stmt(stmt: stmt5));

    resJson = http_post(url, stmt5_json)
    obj = new JsonSlurper().parseText(resJson)
    logger.info("stmt5 res is {}", obj)
    assertEquals(0, obj.code)

    sql """ SET enable_nereids_planner=true; """
    sql """
      explain select * from ${tableName}
    """

    def stmt6 = """ explain  select * from ${tableName} """
    def stmt6_json = JsonOutput.toJson(new Stmt(stmt: stmt6, enable_nereids_planner: false));

    resJson = http_post(url, stmt6_json)
    obj = new JsonSlurper().parseText(resJson)
    logger.info("stmt6 res is {}", obj)
    assertEquals(0, obj.code)
}
