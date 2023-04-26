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

suite("test_parquet", "p0") {
     def externalStageName = "regression_test_customer"
     def filePrefix = "copy_into/parquet"
     def tableName = "parquet_test"

     sql """
         create stage if not exists ${externalStageName} 
         properties ('endpoint' = '${getS3Endpoint()}' ,
         'region' = '${getS3Region()}' ,
         'bucket' = '${getS3BucketName()}' ,
         'prefix' = 'regression' ,
         'ak' = '${getS3AK()}' ,
         'sk' = '${getS3SK()}' ,
         'provider' = '${getProvider()}',
         'access_type' = 'aksk',
         'default.file.column_separator' = "|");
     """

     def dropTable =  """ DROP TABLE IF EXISTS ${tableName}; """
     def createTable = """
         CREATE TABLE ${tableName} (
                 p_partkey     int NOT NULL DEFAULT "1",
                 p_name        VARCHAR(55) NOT NULL DEFAULT "2",
                 p_mfgr        VARCHAR(25) NOT NULL DEFAULT "3",
                 p_brand       VARCHAR(10) NOT NULL DEFAULT "4",
                 p_type        VARCHAR(25) NOT NULL DEFAULT "5",
                 p_size        int NOT NULL DEFAULT "6"
                 )ENGINE=OLAP
         DUPLICATE KEY(`p_partkey`)
         COMMENT "OLAP"
         DISTRIBUTED BY HASH(`p_partkey`) BUCKETS 3;
      """

     def tartgetColumnsList = [ 
                          """""",
                          """""",
                          """""",
                          """(p_partkey, p_name, p_mfgr, p_brand, p_type, p_size)""",
                          """(p_partkey, p_name, p_mfgr, p_brand, p_type, p_size)""",
                          """(p_size, p_type, p_brand, p_mfgr, p_name, p_partkey)""",
                          """(p_partkey, p_name, p_mfgr, p_brand)""",
                          """(p_partkey, p_name, p_size)""",
                          """(p_partkey, p_name, p_mfgr, p_brand, p_type, p_size)""",
                          """(p_partkey, p_name, p_mfgr, p_brand, p_type, p_size)""",
                        ]

    // parquet file contains 9 columns
     def selectColumnsList = [ 
                          """p_partkey, p_name, p_mfgr, p_brand, p_type, p_size""",
                          """p_partkey, p_name, p_mfgr, p_brand, not_exist, p_size""",
                          """*""",
                          """p_partkey, p_name, p_mfgr, p_brand, p_type, p_size""",
                          """p_partkey, p_name, p_mfgr, p_brand, not_exist, p_size""",
                          """p_size, p_type, p_brand, p_mfgr, p_name, p_partkey""",
                          """p_partkey, p_name, p_mfgr, p_brand """,
                          """p_partkey, p_name, greatest(cast(p_partkey as int), cast(p_size as int))""",
                          """p_partkey + 1,  p_name, p_mfgr, p_brand, p_type, p_size * 2""",
                          """p_partkey, p_name, p_mfgr, p_brand, p_type, p_size""",
                        ]

     def whereExprs = [
                        "",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "where p_partkey>10",
                        ]

     def loadRows = [
                     100490,
                     0,
                     0, // success in copy into
                     100490,
                     0,
                     100490,
                     100490,
                     100490,
                     100490,
                     100480,
                     ]

    // "errCode = 2, detailMessage = some columns not found in the file"
    // "errCode = 2, detailMessage = failed to find default value expr for slot: not_exist"
     def errorMsgs = [
                     "",
                     "errCode = 2, detailMessage = Unknown column 'not_exist' in 'table list'",
                     "errCode = 2, detailMessage = Column count doesn't match value count", // success in copy into
                     "",
                     "errCode = 2, detailMessage =",
                     "",
                     "",
                     "",
                     "",
                     "",
                     ]


     // set fe configuration
     sql "ADMIN SET FRONTEND CONFIG ('max_bytes_per_broker_scanner' = '161061273600')"

     def do_insert_into = {table, targetColumns, selectColumns, stageName, prefix, whereExpr ->
         try {
             sql """
             insert into $table $targetColumns
             select $selectColumns from stage('stage_name'='${stageName}', 'file_pattern'='${prefix}/part.parquet', 'format'='parquet') 
             $whereExpr
             """
         } catch (Exception e) {
             return e.getMessage()
         }
     }

     def result;
     for (int i = 0; i < tartgetColumnsList.size(); i++) {
         sql "$dropTable"
         sql "$createTable"
         result = do_insert_into.call(tableName, tartgetColumnsList[i], selectColumnsList[i],
                                    externalStageName, filePrefix, whereExprs[i])
         logger.info("i: " + i + ", insert result: " + result)
         if (errorMsgs[i].isEmpty()) {
             assertTrue(result.size() == 1)
             assertTrue(result[0].size() == 1)
             assertTrue(loadRows[i] == result[0][0])
         } else {
             assertTrue(result.contains(errorMsgs[i]))
         }
     }
}

