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

suite("check_meta") {
    def token = "greedisgood9999"
    def instanceId = context.config.instanceId;
    def cloudUniqueId = context.config.cloudUniqueId;
    def caseStartTime = System.currentTimeMillis()
    def errMsg = "OK"
    def status = 200

    def checkMeta = {
        def metaCheckApi = { checkFunc ->
            httpTest {
                endpoint context.config.recycleServiceHttpAddress
                uri "/RecyclerService/http/check_meta?token=$token&instance_id=$instanceId&host=127.0.0.1&port=9742&user=root&password="
                op "get"
                check checkFunc
            }
        }
        metaCheckApi.call() {
            respCode, body ->
                logger.info("http cli result: ${body} ${respCode}")
                errMsg = body
                status = respCode
        }
    }

    def start = System.currentTimeMillis()
    def now = -1;
    do {
        checkMeta()
        if (status != 200 && errMsg == "meta leak err") {
            sleep(10000);
        }
        now = System.currentTimeMillis()
        logger.info("status {}, errMsg {} start {} now {}", status, errMsg, start, now)
    } while(status != 200 && errMsg == "meta leak err" && (now - start < 3600 * 1000))

    List<List<Object>> dbRes = sql "show databases"
    for (dbRow : dbRes) {
        db = dbRow[0]
        if (db == "__internal_schema" || db == "information_schema") {
            continue
        }

        if (db.contains("external_table")) {
            continue
        }
        List<List<Object>> tableRes = sql """ show tables from ${db} """
        for (tableRow : tableRes) {
            table = tableRow[0]
            logger.info("select count database: {}, table {}", db, table)
            sql """ select count(*) from ${db}.${table} """
        }
    }
}
