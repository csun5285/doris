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

suite("regression_test_select_variant_without_cast", "variant_type_select"){

    def create_table = { table_name ->
        sql "DROP TABLE IF EXISTS ${table_name}"
        sql """
            CREATE TABLE IF NOT EXISTS ${table_name} (
                k bigint,
                v variant
            )
            DUPLICATE KEY(`k`)
            DISTRIBUTED BY HASH(k) BUCKETS 5
            properties("replication_num" = "1", "disable_auto_compaction" = "true");
        """
    }

    def set_be_config = { key, value ->
        String backend_id;
        def backendId_to_backendIP = [:]
        def backendId_to_backendHttpPort = [:]
        getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

        backend_id = backendId_to_backendIP.keySet()[0]
        def (code, out, err) = update_be_config(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), key, value)
        logger.info("update config: code=" + code + ", out=" + out + ", err=" + err)
    }

    try {
        // select with cast
        table_name = "select_data"
        create_table.call(table_name)
        sql "set enable_two_phase_read_opt = false;"
        set_be_config.call("ratio_of_defaults_as_sparse_column", "1")
        sql """insert into  ${table_name} values(10000, '{"a": 3000, "b" : [123, {"xx" : 1}], "c" : {"c" : 456, "d" : null, "e" : 7.111}}')"""
        sql """insert into  ${table_name} values(10001, '{"a": 3000, "b" : "string", "c" : {"c" : 789, "d" : null, "e" : 3.20}}')"""
        sql """insert into  ${table_name} values(10002, '{"a": 3000, "b" : 2.8, "c" : {"c" : 123, "e" : 3.19}}')"""
        sql """insert into  ${table_name} values(10003, '{"a": 3500, "b" : [123, {"xx" : 1}], "c" : {"c" : 456, "d" : null, "e" : 7.112}}')"""
        sql """insert into  ${table_name} values(10004, '{"a": 3500, "c" : {"e" : 6.828}}')"""
        sql """insert into  ${table_name} values(10005, '{"a": 3500, "c" : {"c" : 445}}')"""
        sql """insert into  ${table_name} values(10006, '{"a": 45000, "b" : [123, {"xx" : 1}], "c" : {"c" : 456, "d" : null, "e" : 7.113}}')"""
        sql """insert into  ${table_name} values(10007, '{"a": 45000, "c" : {"c" : 59999, "e" : 7.29}}')"""
        // todo: fix insert error
        // sql """insert into  ${table_name} values(10008, '{"f" : [1, 2, 3]}')"""
        sql """insert into  ${table_name} values(10009, '{"a": 45000, "h": "string_1"}')"""
        sql """insert into  ${table_name} values(10010, '{"a": 45000, "h": "string_2"}')"""
        qt_sql_1_0 """select cast(v:c.c as int) from ${table_name} order by k"""
        // todo: fix cast lost info
        qt_sql_1_1 """select cast(v:b as json) from ${table_name} order by k"""
        qt_sql_1_2 """select cast(v:c.c as int) from ${table_name} order by cast(v:c.c as int)"""
        qt_sql_1_3 """select cast(v:c.e as double) from ${table_name} where cast(v:a as int) > 3000 order by k"""
        // qt_sql_1_4 """select cast(v:f as array) from ${table_name}"""
        qt_sql_1_5 """select cast(v:c.e as double) from ${table_name} where cast(v:c.e as double) > 7.0 order by k"""
        qt_sql_1_6 """select sum(cast(v:a as int)) as sum_a from ${table_name} group by cast(v:a as int) order by sum_a"""
        qt_sql_1_7 """select cast(v:c.e as double) from ${table_name} where cast(v:c.e as double) > 5.0 order by cast(v:c.e as double)"""
        qt_sql_1_8 """select count(cast(v:c.c as int)) as count_c from ${table_name} group by cast(v:a as int) order by count_c"""
        qt_sql_1_9 """select max(cast(v:c.e as double)) as max_e from ${table_name} group by cast(v:a as int) order by max_e"""
        // qt_sql_1_10 """select * from ${table_name} where cast(v:a as int) > 30000 order by k"""
        // todo: support for delete predicate 
        // qt_sql_1_11 """delete from ${table_name} where cast(v:a as int) > 30000"""
        // qt_sql_1_12 """select cast(v:h as string) from ${table_name} where cast(v:a as int) > 30000 order by k"""
        sql "truncate table ${table_name}"

        // select without cast
        table_name = "select_data_without_cast"
        create_table.call(table_name)
        sql "set enable_two_phase_read_opt = false;"
        set_be_config.call("ratio_of_defaults_as_sparse_column", "1")
        sql """insert into  ${table_name} values(10000, '{"a": 3000, "b" : [123, {"xx" : 1}], "c" : {"c" : 456, "d" : null, "e" : 7.111}}')"""
        sql """insert into  ${table_name} values(10001, '{"a": 3000, "b" : "string", "c" : {"c" : 789, "d" : null, "e" : 3.20}}')"""
        sql """insert into  ${table_name} values(10002, '{"a": 3000, "b" : 2.8, "c" : {"c" : 123, "e" : 3.19}}')"""
        sql """insert into  ${table_name} values(10003, '{"a": 3500, "b" : [123, {"xx" : 1}], "c" : {"c" : 456, "d" : null, "e" : 7.112}}')"""
        sql """insert into  ${table_name} values(10004, '{"a": 3500, "c" : {"e" : 6.828}}')"""
        sql """insert into  ${table_name} values(10005, '{"a": 3500, "c" : {"c" : 445}}')"""
        sql """insert into  ${table_name} values(10006, '{"a": 45000, "b" : [123, {"xx" : 1}], "c" : {"c" : 456, "d" : null, "e" : 7.113}}')"""
        sql """insert into  ${table_name} values(10007, '{"a": 45000, "c" : {"c" : 59999, "e" : 7.29}}')"""
        // sql """insert into  ${table_name} values(10008, '{"f" : [1, 2, 3]}')"""
        sql """insert into  ${table_name} values(10009, '{"a": 45000, "h": "string_1"}')"""
        sql """insert into  ${table_name} values(10010, '{"a": 45000, "h": "string_2"}')"""
        Thread.sleep(12000)
        qt_sql_2_0 """select v:c.c from ${table_name} order by k"""
        // todo(lhy): fix cast lost info
        qt_sql_2_1 """select v:b from ${table_name} order by k"""
        qt_sql_2_2 """select v:c.c from ${table_name} order by v:c.c"""
        qt_sql_2_3 """select v:c.e from ${table_name} where v:a > 3000 order by k"""
        // qt_sql_2_4 """select v:f from ${table_name}"""
        qt_sql_2_5 """select v:c.e  from ${table_name} where v:c.e > 7.0 order by k"""
        qt_sql_2_6 """select sum(v:a) as sum_a from ${table_name} group by v:a order by sum_a"""
        qt_sql_2_7 """select v:c.e from ${table_name} where v:c.e > 5.0 order by v:c.e"""
        qt_sql_2_8 """select count(v:c.c) as count_c from ${table_name} group by v:a order by count_c"""
        qt_sql_2_9 """select max(v:c.e) as max_e from ${table_name} group by v:a order by max_e"""
        qt_sql_2_10 """select * from ${table_name} where v:a >= 30000 order by k"""
        // qt_sql_1_11 """delete from ${table_name} where v:a > 30000"""
        // qt_sql_1_12 """select v:h from ${table_name} where v:a > 30000 order by k"""
        sql "truncate table ${table_name}"


        // test where, order, agg ...
        set_be_config.call("ratio_of_defaults_as_sparse_column", "0.95")
        table_name = "github_events"
        sql """DROP TABLE IF EXISTS ${table_name}"""
        sql """
            CREATE TABLE IF NOT EXISTS ${table_name} (
                k bigint,
                v variant
            )
            DUPLICATE KEY(`k`)
            DISTRIBUTED BY HASH(k) BUCKETS 4 
            properties("replication_num" = "1", "disable_auto_compaction" = "false");
        """
        // 2022
        load_json_data.call(table_name, """${getS3Url() + '/regression/gharchive.m/2022-11-07-16.json'}""")
        load_json_data.call(table_name, """${getS3Url() + '/regression/gharchive.m/2022-11-07-10.json'}""")
        load_json_data.call(table_name, """${getS3Url() + '/regression/gharchive.m/2022-11-07-22.json'}""")
        load_json_data.call(table_name, """${getS3Url() + '/regression/gharchive.m/2022-11-07-23.json'}""")
        Thread.sleep(12000)

    } finally {
        // reset flags
        set_be_config.call("ratio_of_defaults_as_sparse_column", "0.95")
    }
}
