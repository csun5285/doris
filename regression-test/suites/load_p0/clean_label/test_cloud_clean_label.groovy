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

suite("test_cloud_clean_label") {
    // define a sql table
    def tableName = "test_cloud_clean_label"
    def dbName = context.config.getDbNameByFile(context.file)

    try {
        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
              `k1` INT(11) NULL COMMENT "",
              `k2` INT(11) NULL COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
            )
        """

        def label1 = "test_cloud_clean_label1" + UUID.randomUUID().toString().replaceAll("-", "")

        sql "insert into ${tableName} with label ${label1} select 1, 2;"

        qt_select "select * from ${tableName} order by k1"

        test {
            sql "insert into ${tableName} with label ${label1} select 1, 2;"
            exception "errCode = 2, detailMessage = Label"
        }

        sql "clean label ${label1} from ${dbName};"

        sql "insert into ${tableName} with label ${label1} select 1, 2;"

        qt_select "select * from ${tableName} order by k1"

    } finally {
        try_sql("DROP TABLE IF EXISTS ${tableName}")
    }
}
