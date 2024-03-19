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

suite("load_four_step") {

    // Import multiple times, use unique key, use seq and delete, select some to delete, and then import
    // Map[tableName, rowCount]
    def tables = [nation: [25, 15], customer: [15000000, 7500000], lineitem: [600037902, 480038598], orders: [150000000, 120000000], part: [20000000, 10000000], partsupp: [80000000, 64000000], region: [5, 3], supplier: [1000000, 500000]]
    def s3BucketName = getS3BucketName()
    def s3WithProperties = """WITH S3 (
        |"AWS_ACCESS_KEY" = "${getS3AK()}",
        |"AWS_SECRET_KEY" = "${getS3SK()}",
        |"AWS_ENDPOINT" = "${getS3Endpoint()}",
        |"AWS_REGION" = "${getS3Region()}")
        |PROPERTIES(
        |"exec_mem_limit" = "8589934592",
        |"load_parallelism" = "3")""".stripMargin()
    
    


    def uniqueID1 = Math.abs(UUID.randomUUID().hashCode()).toString()
    def uniqueID2 = Math.abs(UUID.randomUUID().hashCode()).toString()
    def uniqueID3 = Math.abs(UUID.randomUUID().hashCode()).toString()
    tables.each { table, rows ->
        sql """ DROP TABLE IF EXISTS $table """
        // create table if not exists
        sql new File("""${context.file.parentFile.parent}/ddl/${table}_sequence.sql""").text
        def loadLabel1 = table + "_" + uniqueID1
        // load data from cos
        def loadSql1 = new File("""${context.file.parentFile.parent}/ddl/${table}_load_sequence.sql""").text.replaceAll("\\\$\\{s3BucketName\\}", s3BucketName)
        loadSql1 = loadSql1.replaceAll("\\\$\\{loadLabel\\}", loadLabel1) + s3WithProperties
        sql loadSql1
        // check load state
        while (true) {
            def stateResult1 = sql "show load where Label = '${loadLabel1}'"
            logger.info("load result is ${stateResult1}")
            def loadState1 = stateResult1[stateResult1.size() - 1][2].toString()
            if ("CANCELLED".equalsIgnoreCase(loadState1)) {
                throw new IllegalStateException("load ${loadLabel1} failed.")
            } else if ("FINISHED".equalsIgnoreCase(loadState1)) {
                sql 'sync'
                for (int i = 1; i <= 5; i++) {
                    def loadRowCount = sql "select count(1) from ${table}"
                    logger.info("select ${table} numbers: ${loadRowCount[0][0]}".toString())
                    assertTrue(loadRowCount[0][0] == rows[0])
                }
                def loadLabel2 = table + "_" + uniqueID2
                def loadSql2 = new File("""${context.file.parentFile.parent}/ddl/${table}_load_sequence.sql""").text.replaceAll("\\\$\\{s3BucketName\\}", s3BucketName)
                loadSql2 = loadSql2.replaceAll("\\\$\\{loadLabel\\}", loadLabel2) + s3WithProperties
                sql loadSql2

                while (true) {
                    def stateResult2 = sql "show load where Label = '${loadLabel2}'"
                    logger.info("load result is ${stateResult2}")
                    def loadState2 = stateResult2[stateResult2.size() - 1][2].toString()
                    if ("CANCELLED".equalsIgnoreCase(loadState2)) {
                        throw new IllegalStateException("load ${loadLabel2} failed.")
                    } else if ("FINISHED".equalsIgnoreCase(loadState2)) {
                        sql 'sync'
                        for (int i = 1; i <= 5; i++) {
                            def loadRowCount = sql "select count(1) from ${table}"
                            logger.info("select ${table} numbers: ${loadRowCount[0][0]}".toString())
                            assertTrue(loadRowCount[0][0] == rows[0])
                        }
                        break;
                    }
                    sleep(5000)
                }

                sql new File("""${context.file.parentFile.parent}/ddl/${table}_part_delete.sql""").text
                sql 'sync'
                for (int i = 1; i <= 5; i++) {
                    def loadRowCount = sql "select count(1) from ${table}"
                    logger.info("select ${table} numbers: ${loadRowCount[0][0]}".toString())
                    assertTrue(loadRowCount[0][0] == rows[1])
                }

                def loadLabel3 = table + "_" + uniqueID3
                def loadSql3 = new File("""${context.file.parentFile.parent}/ddl/${table}_load_sequence.sql""").text.replaceAll("\\\$\\{s3BucketName\\}", s3BucketName)
                loadSql3 = loadSql3.replaceAll("\\\$\\{loadLabel\\}", loadLabel3) + s3WithProperties
                sql loadSql3

                while (true) {
                    def stateResult3 = sql "show load where Label = '${loadLabel3}'"
                    logger.info("load result is ${stateResult3}")
                    def loadState3 = stateResult3[stateResult3.size() - 1][2].toString()
                    if ("CANCELLED".equalsIgnoreCase(loadState3)) {
                        throw new IllegalStateException("load ${loadLabel3} failed.")
                    } else if ("FINISHED".equalsIgnoreCase(loadState3)) {
                        sql 'sync'
                        for (int i = 1; i <= 5; i++) {
                            def loadRowCount = sql "select count(1) from ${table}"
                            logger.info("select ${table} numbers: ${loadRowCount[0][0]}".toString())
                            assertTrue(loadRowCount[0][0] == rows[0])
                        }
                        break;
                    }
                    sleep(5000)
                }
                break
            }
            sleep(5000)
        }
        sql """SET query_timeout = 1800"""
        sql """ ANALYZE TABLE $table WITH SYNC """
    }
}
