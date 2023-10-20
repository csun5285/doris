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

suite("test_alter_table_reorder_column") {
    def tbName1 = "alter_table_reorder_column"

    def getJobState = { tableName ->
        def jobStateResult = sql """  SHOW ALTER TABLE COLUMN WHERE IndexName='${tableName}' ORDER BY createtime DESC LIMIT 1 """
        return jobStateResult[0][9]
    }

    sql "DROP TABLE IF EXISTS ${tbName1}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbName1} (
                k1 INT,
                k2 INT,
                value1 INT
            )
            DUPLICATE KEY (k1, k2)
            DISTRIBUTED BY HASH(k1) BUCKETS 1 properties("replication_num" = "1", "light_schema_change" = "true");
        """
    sql """ insert into ${tbName1} values (1, 2, 10), (1 ,5, 10), (4, 1, 10), (3, 2, 10); """

    Thread.sleep(1000)
    sql """
            ALTER TABLE ${tbName1} ORDER BY(k2,k1,value1)
    """

    int max_try_secs = 60
    while (max_try_secs--) {
        String res = getJobState(tbName1)
        if (res == "FINISHED") {
            sleep(3000)
            break
        } else {
            Thread.sleep(2000)
            if (max_try_secs < 1) {
                println "test timeout," + "state:" + res
                assertEquals("FINISHED",res)
            }
        }
    }

    qt_sql """ insert into ${tbName1} values (1, 1, 10), (1 ,4, 10), (1, 6, 10), (2, 2, 10); """

    order_qt_sql1 """ select * from ${tbName1} """

    order_qt_sql2 """ select * from ${tbName1} where k2 > 2 """

    order_qt_sql3 """ select * from ${tbName1} where k2 < 2 """

    order_qt_sql4 """ select * from ${tbName1} where k1 > 2 """

    order_qt_sql5 """ select * from ${tbName1} where k1 < 2 """
}
