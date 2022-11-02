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


suite("test_index", "add_drop_index"){
    // prepare test table


    def timeout = 60000
    def delta_time = 1000
    def alter_res = "null"
    def useTime = 0
    def wait_for_latest_op_on_table_finish = { table_name, OpTimeout ->
        for(int t = delta_time; t <= OpTimeout; t += delta_time){
            alter_res = sql """SHOW ALTER TABLE COLUMN WHERE TableName = "${table_name}" ORDER BY CreateTime DESC LIMIT 1;"""
            alter_res = alter_res.toString()
            if(alter_res.contains("FINISHED")) {
                 break
            }
            useTime = t
            sleep(delta_time)
        }
        assertTrue(useTime <= OpTimeout)
    }

    def indexTbName1 = "test_add_drop_inverted_index"

    sql "DROP TABLE IF EXISTS ${indexTbName1}"
    // create 1 replica table
    sql """
            CREATE TABLE IF NOT EXISTS ${indexTbName1} (
                name varchar(50),
                age int NOT NULL,
                grade int NOT NULL,
                registDate datetime NULL,
                studentInfo char(100),
                tearchComment string
            )
            DUPLICATE KEY(`name`)
            DISTRIBUTED BY HASH(`name`) BUCKETS 10
            properties("replication_num" = "1");
    """
    

    // set enable_vectorized_engine=true
    sql """ SET enable_vectorized_engine=true; """
    def var_result = sql "show variables"
    logger.info("show variales result: " + var_result )

    // case1: create index for int colume
    // case1.0 create index
    sql "create index age_idx on ${indexTbName1}(age);"
    wait_for_latest_op_on_table_finish(indexTbName1, timeout)

    def show_result = sql "show index from ${indexTbName1}"
    logger.info("show index from " + indexTbName1 + " result: " + show_result)
    assertEquals(show_result[0][2], "age_idx")
    
    // case1.1 create duplicate same index for one colume with same name
    def create_dup_index_result = "fail"
    try {
        sql "create index age_idx on ${indexTbName1}(`age`)"
        create_dup_index_result = "success"
    } catch(Exception ex) {
        logger.info("create same duplicate and same name index,  result: " + ex)
    }
    assertEquals(create_dup_index_result, "fail")
    // case1.2 create duplicate same index for one colume with different name
    try {
        sql "create index age_idx_diff on ${indexTbName1}(`age`)"
        create_dup_index_result = "success"
    } catch(Exception ex) {
        logger.info("create same duplicate with different name index,  result: " + ex)
    }
    assertEquals(create_dup_index_result, "fail")
    // case1.3 create duplicate different index for one colume with same name
    try {
        sql "create index age_idx_diff on ${indexTbName1}(`age`) using bitmap"
        create_dup_index_result = "success"
    } catch(Exception ex) {
        logger.info("create different duplicate and different name index,  result: " + ex)
    }
    assertEquals(create_dup_index_result, "fail")
    
    // case1.4 drop index
    def drop_result = sql "drop index age_idx on ${indexTbName1}"
    logger.info("drop index age_idx on " + indexTbName1 + "; result: " + drop_result)
    wait_for_latest_op_on_table_finish(indexTbName1, timeout)
    show_result = sql "show index from ${indexTbName1}"
    assertEquals(show_result.size(), 0)
    
    // case1.5 drop index again, expect drop fail
    def drop_index_twice_result = "fail"
    try {
        drop_result = sql "drop index age_idx on ${indexTbName1}"
        drop_index_twice_result = "success"
    } catch(Exception ex) {
        logger.info("drop index again, result: " + ex)
    }
    assertEquals(drop_index_twice_result, "fail")


    // case2: create index for date colume
    // case2.0 create index with which index_name has been used in age colume
    sleep(30000)
    def create_index_with_used_name_result = "fail"
    try {
        sql "create index age_idx on ${indexTbName1}(age);"
        sql "create index age_idx on ${indexTbName1}(`registDate`)"
        create_index_with_used_name_result = "success"
    } catch(Exception ex) {
        logger.info("expect create index with used index name, result: " + ex)
        sleep(3000)
        sql "drop index age_idx on ${indexTbName1}"
    }
    assertEquals(create_index_with_used_name_result, "fail")
    // case2.1 create index for date colume
    sleep(3000)
    sql "create index date_idx on ${indexTbName1}(`registDate`)"
    wait_for_latest_op_on_table_finish(indexTbName1, timeout)
    show_result = sql "show index from ${indexTbName1}"
    assertEquals(show_result.size(), 1)
    sql "drop index date_idx on ${indexTbName1}"
    wait_for_latest_op_on_table_finish(indexTbName1, timeout)

    // case3: create string inverted index for int colume
    def create_string_index_on_int_colume_result = "fail"
    try {
        syntax_error = sql "create index age_idx on ${indexTbName1}(`age`) USING INVERTED PROPERTIES("parser"="standard")"
        create_string_index_on_int_colume_result = "success"
    } catch(Exception ex) {
        logger.info("sql: create index age_idx on" + indexTbName1 + "(`age`) USING INVERTED PROPERTIES(\"parser\"=\"standard\")")
        logger.info("create string inverted index for int colume, result: " + ex)
    }
    assertEquals(create_string_index_on_int_colume_result, "fail")

    // case4: create default inverted index for varchar coulume
    sleep(10000)
    sql "create index name_idx on ${indexTbName1}(`name`)"
    wait_for_latest_op_on_table_finish(indexTbName1, timeout)
    show_result = sql "show index from ${indexTbName1}"
    assertTrue(show_result[0][2] == "name_idx" && show_result[0][10] == "BITMAP")
    logger.info("create index name_idx for " + indexTbName1 + "(`name`)")
    logger.info("show index result: " + show_result)
    sql "drop index name_idx on ${indexTbName1}"
    wait_for_latest_op_on_table_finish(indexTbName1, timeout)

    // case5: create none inverted index for char colume
    sql "create index name_idx on ${indexTbName1}(`name`) USING INVERTED PROPERTIES(\"parser\"=\"none\")"
    wait_for_latest_op_on_table_finish(indexTbName1, timeout)
    show_result = sql "show index from ${indexTbName1}"
    assertEquals(show_result[0][10], "INVERTED")
    logger.info("create index name_idx for " + indexTbName1 + "(`name`) USING INVERTED PROPERTIES(\"parser\"=\"none\")")
    logger.info("show index result: " + show_result)
    sql "drop index name_idx on ${indexTbName1}"
    wait_for_latest_op_on_table_finish(indexTbName1, timeout)

    // case5：create simple inverted index for char colume
    sql "create index studentInfo_idx on ${indexTbName1}(`studentInfo`) USING INVERTED PROPERTIES(\"parser\"=\"english\")"
    wait_for_latest_op_on_table_finish(indexTbName1, timeout)
    show_result = sql "show index from ${indexTbName1}"
    assertTrue(show_result[0][2] == "studentInfo_idx" && show_result[0][10] == "INVERTED")
    logger.info("create index studentInfo_idx for " + indexTbName1 + "(`studentInfo`) USING INVERTED PROPERTIES(\"parser\"=\"english\")")
    logger.info("show index result: " + show_result)
    sql "drop index studentInfo_idx on ${indexTbName1}"
    wait_for_latest_op_on_table_finish(indexTbName1, timeout)

    // case6: create standard inverted index for text colume
    sql "create index tearchComment_idx on ${indexTbName1}(`tearchComment`) USING INVERTED PROPERTIES(\"parser\"=\"standard\")"
    wait_for_latest_op_on_table_finish(indexTbName1, timeout)
    show_result = sql "show index from ${indexTbName1}"
    assertTrue(show_result[0][2] == "tearchComment_idx" && show_result[0][10] == "INVERTED")
    logger.info("create index tearchComment_idx for " + indexTbName1 + "(`tearchComment`) USING INVERTED PROPERTIES(\"parser\"=\"standard\")")
    logger.info("show index result: " + show_result)
    sql "drop index tearchComment_idx on ${indexTbName1}"
    wait_for_latest_op_on_table_finish(indexTbName1, timeout)

    // case7: drop not exist index
    def drop_no_exist_index_result = "fail"
    try {
        sql "drop index no_exist_idx on ${indexTbName1}"
        drop_no_exist_index_result = "success"
    } catch(Exception ex) {
       logger.info("drop not exist index: result " + ex)
    }
    assertEquals(drop_no_exist_index_result, "fail")

    // case8: create，drop, create index
    // case8.0: create, drop, create same index with same name
    sql "create index tearchComment_idx on ${indexTbName1}(`tearchComment`) USING INVERTED PROPERTIES(\"parser\"=\"standard\")"
    wait_for_latest_op_on_table_finish(indexTbName1, timeout)
    sql "drop index tearchComment_idx on ${indexTbName1}"
    wait_for_latest_op_on_table_finish(indexTbName1, timeout)
    sql "create index tearchComment_idx on ${indexTbName1}(`tearchComment`) USING INVERTED PROPERTIES(\"parser\"=\"standard\")"
    wait_for_latest_op_on_table_finish(indexTbName1, timeout)
    show_result = sql "show index from ${indexTbName1}"
    assertTrue(show_result[0][2] == "tearchComment_idx" && show_result[0][10] == "INVERTED")
    sql "drop index tearchComment_idx on ${indexTbName1}"
    wait_for_latest_op_on_table_finish(indexTbName1, timeout)

    // case8.1: create, drop, create other index with same name
    sql "create index tearchComment_idx on ${indexTbName1}(`tearchComment`) USING INVERTED PROPERTIES(\"parser\"=\"standard\")"
    wait_for_latest_op_on_table_finish(indexTbName1, timeout)
    sql "drop index tearchComment_idx on ${indexTbName1}"
    wait_for_latest_op_on_table_finish(indexTbName1, timeout)
    sql "create index tearchComment_idx on ${indexTbName1}(`tearchComment`) USING INVERTED PROPERTIES(\"parser\"=\"none\")"
    wait_for_latest_op_on_table_finish(indexTbName1, timeout)
    show_result = sql "show index from ${indexTbName1}"
    assertTrue(show_result[0][2] == "tearchComment_idx" && show_result[0][10] == "INVERTED")
    sql "drop index tearchComment_idx on ${indexTbName1}"
    wait_for_latest_op_on_table_finish(indexTbName1, timeout)

    // case8.2: create, drop, create same index with other name
    sql "create index tearchComment_idx on ${indexTbName1}(`tearchComment`) USING INVERTED PROPERTIES(\"parser\"=\"standard\")"
    wait_for_latest_op_on_table_finish(indexTbName1, timeout)
    sql "drop index tearchComment_idx on ${indexTbName1}"
    wait_for_latest_op_on_table_finish(indexTbName1, timeout)
    sql "create index tearchComment_idx_2 on ${indexTbName1}(`tearchComment`) USING INVERTED PROPERTIES(\"parser\"=\"standard\")"
    wait_for_latest_op_on_table_finish(indexTbName1, timeout)
    show_result = sql "show index from ${indexTbName1}"
    assertTrue(show_result[0][2] == "tearchComment_idx_2" && show_result[0][10] == "INVERTED")
    sql "drop index tearchComment_idx_2 on ${indexTbName1}"
    wait_for_latest_op_on_table_finish(indexTbName1, timeout)

}


suite("add_drop_index_with_data", "add_drop_index_with_data"){
    // prepare test table


    def timeout = 60000
    def delta_time = 1000
    def alter_res = "null"
    def useTime = 0
    def wait_for_latest_op_on_table_finish = { table_name, OpTimeout ->
        for(int t = delta_time; t <= OpTimeout; t += delta_time){
            alter_res = sql """SHOW ALTER TABLE COLUMN WHERE TableName = "${table_name}" ORDER BY CreateTime DESC LIMIT 1;"""
            alter_res = alter_res.toString()
            if(alter_res.contains("FINISHED")) {
                 break
            }
            useTime = t
            sleep(delta_time)
        }
        assertTrue(useTime <= OpTimeout)
    }

    def indexTbName1 = "test_add_drop_inverted_index2"

    sql "DROP TABLE IF EXISTS ${indexTbName1}"
    // create 1 replica table
    sql """
            CREATE TABLE IF NOT EXISTS ${indexTbName1} (
                `id` int(11) NULL,
                `name` text NULL,
                `description` text NULL,
                INDEX idx_id (`id`) USING INVERTED COMMENT '',
                INDEX idx_name (`name`) USING INVERTED PROPERTIES("parser"="none") COMMENT ''
            )
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 10
            properties("replication_num" = "1");
    """

    // set enable_vectorized_engine=true
    sql """ SET enable_vectorized_engine=true; """
    def var_result = sql "show variables"
    logger.info("show variales result: " + var_result )

    // show index of create table
    def show_result = sql "show index from ${indexTbName1}"
    logger.info("show index from " + indexTbName1 + " result: " + show_result)
    assertEquals(show_result[0][2], "idx_id")
    assertEquals(show_result[1][2], "idx_name")

    // insert data
    sql "insert into ${indexTbName1} values (1, 'name1', 'desc 1'), (2, 'name2', 'desc 2')"

    // query all rows
    def select_result = sql "select * from ${indexTbName1}"
    assertEquals(select_result.size(), 2)
    assertEquals(select_result[0][0], 1)
    assertEquals(select_result[0][1], "name1")
    assertEquals(select_result[0][2], "desc 1")
    assertEquals(select_result[1][0], 2)
    assertEquals(select_result[1][1], "name2")
    assertEquals(select_result[1][2], "desc 2")

    // query rows where name='name1'
    select_result = sql "select * from ${indexTbName1} where name='name1'"
    assertEquals(select_result.size(), 1)
    assertEquals(select_result[0][0], 1)
    assertEquals(select_result[0][1], "name1")
    assertEquals(select_result[0][2], "desc 1")

    // query rows where name='name2'
    select_result = sql "select * from ${indexTbName1} where name='name2'"
    assertEquals(select_result.size(), 1)
    assertEquals(select_result[0][0], 2)
    assertEquals(select_result[0][1], "name2")
    assertEquals(select_result[0][2], "desc 2")

    // query rows where description match 'desc', should fail without index
    def success = false
    try {
        sql "select * from ${indexTbName1} where description match 'desc'"
        success = true
    } catch(Exception ex) {
        logger.info("sql exception: " + ex)
    }
    assertEquals(success, false)

    // add index on column description
    sql "create index idx_desc on ${indexTbName1}(description) USING INVERTED PROPERTIES(\"parser\"=\"standard\");"
    wait_for_latest_op_on_table_finish(indexTbName1, timeout)

    // show index after add index
    show_result = sql "show index from ${indexTbName1}"
    logger.info("show index from " + indexTbName1 + " result: " + show_result)
    assertEquals(show_result[0][2], "idx_id")
    assertEquals(show_result[1][2], "idx_name")
    assertEquals(show_result[2][2], "idx_desc")

    // query rows where description match 'desc'
    select_result = sql "select * from ${indexTbName1} where description match 'desc'"
    assertEquals(select_result.size(), 2)
    assertEquals(select_result[0][0], 1)
    assertEquals(select_result[0][1], "name1")
    assertEquals(select_result[0][2], "desc 1")
    assertEquals(select_result[1][0], 2)
    assertEquals(select_result[1][1], "name2")
    assertEquals(select_result[1][2], "desc 2")

    // query rows where description match_all 'desc 1'
    select_result = sql "select * from ${indexTbName1} where description match_all 'desc 1'"
    assertEquals(select_result.size(), 1)
    assertEquals(select_result[0][0], 1)
    assertEquals(select_result[0][1], "name1")
    assertEquals(select_result[0][2], "desc 1")

    // query rows where description match_all 'desc 2'
    select_result = sql "select * from ${indexTbName1} where description match_all 'desc 2'"
    assertEquals(select_result.size(), 1)
    assertEquals(select_result[0][0], 2)
    assertEquals(select_result[0][1], "name2")
    assertEquals(select_result[0][2], "desc 2")

    // drop index
    // add index on column description
    sql "drop index idx_desc on ${indexTbName1}"
    wait_for_latest_op_on_table_finish(indexTbName1, timeout)

    // query rows where description match 'desc', should fail without index
    success = false
    try {
        sql "select * from ${indexTbName1} where description match 'desc'"
        success = true
    } catch(Exception ex) {
        logger.info("sql exception: " + ex)
    }
    assertEquals(success, false)

    // query rows where name='name1'
    select_result = sql "select * from ${indexTbName1} where name='name1'"
    assertEquals(select_result.size(), 1)
    assertEquals(select_result[0][0], 1)
    assertEquals(select_result[0][1], "name1")
    assertEquals(select_result[0][2], "desc 1")

    // query rows where name='name2'
    select_result = sql "select * from ${indexTbName1} where name='name2'"
    assertEquals(select_result.size(), 1)
    assertEquals(select_result[0][0], 2)
    assertEquals(select_result[0][1], "name2")
    assertEquals(select_result[0][2], "desc 2")

    // add index on column description
    sql "create index idx_desc on ${indexTbName1}(description) USING INVERTED PROPERTIES(\"parser\"=\"standard\");;"
    wait_for_latest_op_on_table_finish(indexTbName1, timeout)

    // query rows where description match 'desc'
    select_result = sql "select * from ${indexTbName1} where description match 'desc'"
    assertEquals(select_result.size(), 2)
    assertEquals(select_result[0][0], 1)
    assertEquals(select_result[0][1], "name1")
    assertEquals(select_result[0][2], "desc 1")
    assertEquals(select_result[1][0], 2)
    assertEquals(select_result[1][1], "name2")
    assertEquals(select_result[1][2], "desc 2")

    // query rows where description match_all 'desc 1'
    select_result = sql "select * from ${indexTbName1} where description match_all 'desc 1'"
    assertEquals(select_result.size(), 1)
    assertEquals(select_result[0][0], 1)
    assertEquals(select_result[0][1], "name1")
    assertEquals(select_result[0][2], "desc 1")

    // query rows where description match_all 'desc 2'
    select_result = sql "select * from ${indexTbName1} where description match_all 'desc 2'"
    assertEquals(select_result.size(), 1)
    assertEquals(select_result[0][0], 2)
    assertEquals(select_result[0][1], "name2")
    assertEquals(select_result[0][2], "desc 2")
}
