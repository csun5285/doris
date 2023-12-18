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

suite("test_retry_e-230") {
    def curlBeDebugPoint = { endpoint, uri ->
        StringBuilder strBuilder = new StringBuilder()
        strBuilder.append("""curl http://""" + "${endpoint}" + "${uri}")
        String command = strBuilder.toString()
        logger.info("inject debug point command=" + command)
        def process = command.toString().execute()
        def code = process.waitFor()
        def err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
        def out = process.getText()
        logger.info("inject debug point : code=" + code + ", out=" + out + ", err=" + err)
        assertEquals(code, 0)
    }


    def curlFeDebugPoint = { endpoint, user, passwd, uri ->
        StringBuilder strBuilder = new StringBuilder()
        strBuilder.append("""curl  -X POST -u${user}:${passwd} http://""" + "${endpoint}" + "${uri}")
        String command = strBuilder.toString()
        logger.info("inject debug point command=" + command)
        def process = command.toString().execute()
        def code = process.waitFor()
        def err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
        def out = process.getText()
        logger.info("inject debug point : code=" + code + ", out=" + out + ", err=" + err)
        assertEquals(code, 0)
    }

    List<String> ipList = new ArrayList<>()
    List<String> hbPortList = new ArrayList<>()
    List<String> httpPortList = new ArrayList<>()
    List<String> beUniqueIdList = new ArrayList<>()

    String[] bes = context.config.multiClusterBes.split(',');
    println("the value is " + context.config.multiClusterBes);
    for(String values : bes) {
        println("the value is " + values);
        String[] beInfo = values.split(':');
        ipList.add(beInfo[0]);
        hbPortList.add(beInfo[1]);
        httpPortList.add(beInfo[2]);
        beUniqueIdList.add(beInfo[3]);
    }

    println("the ip is " + ipList);
    println("the heartbeat port is " + hbPortList);
    println("the http port is " + httpPortList);
    println("the be unique id is " + beUniqueIdList);
    
    def tbl = 'test_retry_e_230_tbl'
    sql """ DROP TABLE IF EXISTS ${tbl} """

    try {
        // set be debug point
        // curl "127.0.0.1:11101/api/injection_point/set/Tablet::cloud_capture_rs_readers?behavior=return_error&code=-230"
        for (def i = 0; i < ipList.size(); i++) {
            curlBeDebugPoint.call("${ipList[i]}:${httpPortList[i]}", "/api/injection_point/set/Tablet::cloud_capture_rs_readers?behavior=return_error&code=-230")
        }

        sql """
            CREATE TABLE ${tbl} (
            `k1` int(11) NULL,
            `k2` int(11) NULL
            )
            DUPLICATE KEY(`k1`, `k2`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES (
            "replication_num"="1"
            );
            """
        for (def i = 1; i <= 5; i++) {
            sql "INSERT INTO ${tbl} VALUES (${i}, ${10 * i})"
        }

        // curl fe debug point
        curlFeDebugPoint.call(context.config.feHttpAddress, context.config.jdbcUser, context.config.jdbcPassword, "/api/debug_point/add/StmtExecutor.retry.longtime")

        def futrue1 = thread {
            Thread.sleep(3000)
            for (def i = 0; i < ipList.size(); i++) {
                curlBeDebugPoint.call("${ipList[i]}:${httpPortList[i]}", "/api/injection_point/clear/Tablet::cloud_capture_rs_readers")
            }
        }

        def begin = System.currentTimeMillis();
        def futrue2 = thread {
            def result = try_sql """select * from ${tbl}"""
        }

        futrue1.get()
        futrue2.get()
        def cost = System.currentTimeMillis() - begin;
        log.info("time cost: {}", cost)
        // fe StmtExecutor retry time, at most 25 * 1.5s + 25 * 2.5s
        assertTrue(cost > 3000 && cost < 100000)
    } finally {
        for (def i = 0; i < ipList.size(); i++) {
            curlBeDebugPoint.call("${ipList[i]}:${httpPortList[i]}", "/api/injection_point/clear/Tablet::cloud_capture_rs_readers")
        } 
        curlFeDebugPoint.call(context.config.feHttpAddress, context.config.jdbcUser, context.config.jdbcPassword, "/api/debug_point/remove/StmtExecutor.retry.longtime")
        sql """ DROP TABLE IF EXISTS ${tbl} """
    }
}
