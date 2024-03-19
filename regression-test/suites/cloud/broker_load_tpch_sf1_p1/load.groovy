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

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.DistributedFileSystem
import java.net.URI
import org.apache.hadoop.fs.Path
import org.apache.hadoop.util.Progressable
import java.io.BufferedWriter
import java.io.OutputStreamWriter

suite("load") {
    def externalStageName = "regression_test_tpch"
    def prefix = "tpch/sf1"
    def s3BucketName = getS3BucketName()

    // tpch_sf1_p1 is writted to test unique key table merge correctly.
    // It creates unique key table and sets bucket num to 1 in order to make sure that
    // many rowsets will be created during loading and then the merge process will be triggered.

    def tables = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]

    Configuration configuration = new Configuration();
    configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
    FileSystem hdfs = FileSystem.get(new URI(getHdfsFs()), configuration);

    for (String table in tables) {
        if (table == "orders" || table == "lineitem" || table == "partsupp" ) {
            for (int i = 1; i < 11; i++) {
                String file = table + ".tbl." + i.toString()

                String cmd = "wget " + getS3BucketName() + "." + getS3Endpoint() + "/regression/tpch/sf1/" + file
                logger.info("cmd: " + cmd)
                def sout = new StringBuilder()
                def serr = new StringBuilder()
                def proc = cmd.execute()
                proc.consumeProcessOutput(sout, serr)
                proc.waitForOrKill(10000)
                println "out> $sout\n err> $serr"

                hdfs.copyFromLocalFile(new Path("./${file}"), new Path("/broker/${file}"));

                cmd = "rm " + System.getProperty("user.dir") + "/" + file
                logger.info("cmd: " + cmd)
                sout = new StringBuilder()
                serr = new StringBuilder()
                proc = cmd.execute()
                proc.consumeProcessOutput(sout, serr)
                proc.waitForOrKill(1000)
                println "out> $sout\n err> $serr"
            }
        } else {
            String file = table + ".tbl"
            String cmd = "wget " + getS3BucketName() + "." + getS3Endpoint() + "/regression/tpch/sf1/" + file
            logger.info("cmd: " + cmd)

            def sout = new StringBuilder()
            def serr = new StringBuilder()
            def proc = cmd.execute()
            proc.consumeProcessOutput(sout, serr)
            proc.waitForOrKill(1000)
            println "out> $sout\n err> $serr"

            hdfs.copyFromLocalFile(new Path("./${file}"), new Path("/broker/${file}"));

            cmd = "rm " + System.getProperty("user.dir") + "/" + file
            logger.info("cmd: " + cmd)
            sout = new StringBuilder()
            serr = new StringBuilder()
            proc = cmd.execute()
            proc.consumeProcessOutput(sout, serr)
            proc.waitForOrKill(1000)
            println "out> $sout\n err> $serr"
        }
    }

    def tableList = [customer: 150000, lineitem: 6001215, nation: 25, orders: 1500000, part: 200000, partsupp: 800000, region: 5, supplier: 10000]
    def hdfsFs = getHdfsFs()
    def hdfsUser = getHdfsUser()
    
    

    def uniqueID = Math.abs(UUID.randomUUID().hashCode()).toString()
    tableList.each { table, rows ->
        // create table if not exists
        sql new File("""${context.file.parent}/ddl/${table}.sql""").text
        // check row count
        def rowCount = sql "select count(*) from ${table}"
        if (rowCount[0][0] != rows) {
            def loadLabel = table + "_" + uniqueID
            sql new File("""${context.file.parent}/ddl/${table}_delete.sql""").text
            // load data from cos
            def loadSql = new File("""${context.file.parent}/ddl/${table}_load.sql""").text.replaceAll("\\\$\\{hdfsFs\\}", hdfsFs)
            loadSql = loadSql.replaceAll("\\\$\\{hdfsUser\\}", hdfsUser)
            loadSql = loadSql.replaceAll("\\\$\\{loadLabel\\}", loadLabel)
            sql loadSql

            // check load state
            while (true) {
                def stateResult = sql "show load where Label = '${loadLabel}'"
                def loadState = stateResult[stateResult.size() - 1][2].toString()
                if ("CANCELLED".equalsIgnoreCase(loadState)) {
                    throw new IllegalStateException("load ${loadLabel} failed.")
                } else if ("FINISHED".equalsIgnoreCase(loadState)) {
                    break
                }
                sleep(5000)
            }
        }
    }
}
