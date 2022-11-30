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


suite("test_array_index"){
    // prepare test table


    def timeout = 60000
    def delta_time = 1000
    def alter_res = "null"
    def useTime = 0

    def indexTblName = "array_test"

    sql "DROP TABLE IF EXISTS ${indexTblName}"
    // create 1 replica table
    sql """
	CREATE TABLE IF NOT EXISTS ${indexTblName}(
		`id`int(11)NULL,
		`c_array` array<varchar(20)> NULL,
		INDEX c_array_idx(`c_array`) USING INVERTED PROPERTIES("parser"="english") COMMENT 'c_array index'
	) ENGINE=OLAP
	DUPLICATE KEY(`id`)
	COMMENT 'OLAP'
	DISTRIBUTED BY HASH(`id`) BUCKETS 1
	PROPERTIES(
 		"replication_allocation" = "tag.location.default: 1",
		"persistent"="false"
	);
    """
    
    // set enable_vectorized_engine=true
    sql """ SET enable_vectorized_engine=true; """
    def var_result = sql "show variables"
    logger.info("show variales result: " + var_result )

    sql "INSERT INTO $indexTblName VALUES (1, ['i','love','china']), (2, ['i','love','north korea']), (3, NULL);"
    sql "INSERT INTO $indexTblName VALUES (4, NULL);"
    qt_sql "SELECT * FROM $indexTblName WHERE c_array MATCH 'china';"
    qt_sql "SELECT * FROM $indexTblName WHERE c_array MATCH 'love';"
    qt_sql "SELECT * FROM $indexTblName WHERE c_array MATCH 'north';"
}
