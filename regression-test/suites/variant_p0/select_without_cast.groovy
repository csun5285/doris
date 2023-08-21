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
            DISTRIBUTED BY HASH(k) BUCKETS 1
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

    def load_json_data = {table_name, file_name ->
        // load the json data
        streamLoad {
            table "${table_name}"

            // set http request header params
            set 'read_json_by_line', 'true' 
            set 'format', 'json' 
            set 'max_filter_ratio', '0.1'
            file file_name // import json file
            time 10000 // limit inflight 10s

            // if declared a check callback, the default check condition will ignore.
            // So you must check all condition

            check { result, exception, startTime, endTime ->
                if (exception != null) {
                        throw exception
                }
                logger.info("Stream load ${file_name} result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                // assertEquals(json.NumberTotalRows, json.NumberLoadedRows + json.NumberUnselectedRows)
                assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
            }
        }
    }

    try {
        // select with cast
        table_name = "select_data"
        create_table.call(table_name)
        sql """insert into  ${table_name} values(10000, '{"a": 3000, "b" : [123, {"xx" : 1}]}')"""
        sql """insert into  ${table_name} values(10001, '{"a": 3000, "b" : "string"}')"""
        sql """insert into  ${table_name} values(10002, '{"a": 3000, "c" : {"c" : 123, "e" : 3.19}}')"""
        sql """insert into  ${table_name} values(10003, '{"a": 3500, "c" : {"c" : 456, "d" : null, "e" : 7.112}}')"""
        sql """insert into  ${table_name} values(10004, '{"a": 3500, "c" : {"e" : 6.828}}')"""
        sql """insert into  ${table_name} values(10005, '{"a": 3500, "c" : {"c" : 445}}')"""
        // todo: fix insert error
        // sql """insert into  ${table_name} values(10008, '{"f" : [1, 2, 3]}')"""
        sql """insert into  ${table_name} values(10006, '{"a": 45000, "h": "string_1"}')"""
        sql """insert into  ${table_name} values(10007, '{"a": 45000, "h": "string_2"}')"""

        sql "set enable_two_phase_read_opt = false;"
        qt_sql_1_0 """select cast(v:c.c as int) from ${table_name} order by k"""
        // // todo: fix cast lost info
        qt_sql_1_1 """select cast(v:b as json) from ${table_name} order by k"""
        qt_sql_1_2 """select cast(v:c.e as double) from ${table_name} where cast(v:c.c as int) > 123 order by k"""
        // qt_sql_1_3 """select cast(v:f as array) from ${table_name}"""
        qt_sql_1_4 """select cast(v:c.e as double) from ${table_name} where cast(v:c.e as double) > 7.0 order by k"""
        qt_sql_1_5 """select sum(cast(v:a as int)) as sum_a from ${table_name} group by cast(v:a as int) order by sum_a"""
        qt_sql_1_6 """select cast(v:c.e as double) from ${table_name} where cast(v:c.e as double) > 5.0 order by cast(v:c.e as double)"""
        qt_sql_1_7 """select count(cast(v:c.c as int)) as count_c from ${table_name} group by cast(v:a as int) order by count_c"""
        qt_sql_1_8 """select max(cast(v:c.e as double)) as max_e from ${table_name} group by cast(v:a as int) order by max_e"""
        qt_sql_1_9 """select * from ${table_name} where cast(v:a as int) > 30000 order by k"""
        // todo: support for delete predicate 
        // qt_sql_1_10 """delete from ${table_name} where cast(v:a as int) > 30000"""
        qt_sql_1_11 """select * from ${table_name} where cast(v:h as string) = "string_2" """
        qt_sql_1_12 """select cast(v:c as string) from ${table_name} where cast(v:a as int) = 3500 order by k"""
        // sql "truncate table ${table_name}"

        // select without cast
        table_name = "select_data_without_cast"
        create_table.call(table_name)
        sql """insert into  ${table_name} values(10000, '{"a": 3000, "b" : [123, {"xx" : 1}]}')"""
        sql """insert into  ${table_name} values(10001, '{"a": 3000, "b" : "string"}')"""
        sql """insert into  ${table_name} values(10002, '{"a": 3000, "c" : {"c" : 123, "e" : 3.19}}')"""
        sql """insert into  ${table_name} values(10003, '{"a": 3500, "c" : {"c" : 456, "d" : null, "e" : 7.112}}')"""
        sql """insert into  ${table_name} values(10004, '{"a": 3500, "c" : {"e" : 6.828}}')"""
        sql """insert into  ${table_name} values(10005, '{"a": 3500, "c" : {"c" : 445}}')"""
        // todo: fix insert error
        // sql """insert into  ${table_name} values(10008, '{"f" : [1, 2, 3]}')"""
        sql """insert into  ${table_name} values(10006, '{"a": 45000, "h": "string_1"}')"""
        sql """insert into  ${table_name} values(10007, '{"a": 45000, "h": "string_2"}')"""
    
        Thread.sleep(12000)
        sql "set enable_two_phase_read_opt = false;"
        qt_sql_2_0 """select v:c.c from ${table_name} order by k"""
        // // todo: fix cast lost info
        qt_sql_2_1 """select v:b from ${table_name} order by k"""
        qt_sql_2_2 """select v:c.e from ${table_name} where v:c.c > 123 order by k"""
        // qt_sql_2_3 """select cast(v:f as array) from ${table_name}"""
        qt_sql_2_4 """select v:c.e from ${table_name} where v:c.e > 7.0 order by k"""
        qt_sql_2_5 """select sum(v:a) as sum_a from ${table_name} group by v:a order by sum_a"""
        qt_sql_2_6 """select v:c.e from ${table_name} where v:c.e > 5.0 order by v:c.e"""
        qt_sql_2_7 """select count(v:c.c) as count_c from ${table_name} group by v:a order by count_c"""
        qt_sql_2_8 """select max(v:c.e) as max_e from ${table_name} group by v:a order by max_e"""
        qt_sql_2_9 """select * from ${table_name} where v:a > 30000 order by k"""
        // todo: support for delete predicate 
        // qt_sql_2_10 """delete from ${table_name} where cast(v:a as int) > 30000"""
        qt_sql_2_11 """select * from ${table_name} where v:h = "string_2" """
        qt_sql_2_12 """select v:c from ${table_name} where v:a = 3500 order by k"""
        // sql "truncate table ${table_name}"

        set_be_config.call("ratio_of_defaults_as_sparse_column", "0.75")
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
        // 2015
        load_json_data.call(table_name, """${getS3Url() + '/regression/gharchive.m/2015-01-01-0.json'}""")
        load_json_data.call(table_name, """${getS3Url() + '/regression/gharchive.m/2015-01-01-1.json'}""")
        load_json_data.call(table_name, """${getS3Url() + '/regression/gharchive.m/2015-01-01-2.json'}""")
        load_json_data.call(table_name, """${getS3Url() + '/regression/gharchive.m/2015-01-01-3.json'}""")
        // 2022
        load_json_data.call(table_name, """${getS3Url() + '/regression/gharchive.m/2022-11-07-16.json'}""")
        load_json_data.call(table_name, """${getS3Url() + '/regression/gharchive.m/2022-11-07-10.json'}""")
        load_json_data.call(table_name, """${getS3Url() + '/regression/gharchive.m/2022-11-07-22.json'}""")
        load_json_data.call(table_name, """${getS3Url() + '/regression/gharchive.m/2022-11-07-23.json'}""")
        Thread.sleep(12000)

        // v:repo.id   int
        // v:repo.name text
        // todo: sometime result is empty
        qt_sql_3 """select v:repo from ${table_name} where v:repo.id = 562172658"""
        // todo: result is empty, but select v:repo is not
        qt_sql_3_1 """select v from ${table_name} where v:repo.id = 562172658"""
        
        qt_sql_3_2 """select * from ${table_name} order by v:repo.id desc limit 1"""
        qt_sql_3_3 """select v:type, count(*) from ${table_name} group by v:type order by v:type"""
        qt_sql_3_4 """SELECT v:payload.action, count() FROM github_events  GROUP BY v:payload.action ORDER BY v:payload.action"""
        qt_sql_3_5 """SELECT v:repo.name FROM github_events ORDER BY v:repo.name LIMIT 5"""
        qt_sql_3_6 """SELECT v:repo.name, count() AS stars FROM github_events GROUP BY v:repo.name ORDER BY stars DESC, v:repo.name, 1 LIMIT 5"""
        qt_sql_3_7 """
            SELECT 
                concat('https://github.com/', v:repo.name, '/pull/') AS URL,
                count(distinct v:actor.login) AS authors
                FROM github_events
                GROUP BY v:repo.name, v:payload.issue.`number` 
                ORDER BY authors DESC, URL ASC
                LIMIT 5
        """
        qt_sql_3_8 """
            SELECT
                lower(split_part(v:repo.name, '/', 1)) AS org,
                count() AS stars
            FROM github_events
            GROUP BY org
            ORDER BY stars DESC, 1
            LIMIT 1;
        """
        qt_sql_3_9 """
            SELECT count(distinct v:actor.login) FROM github_events
        """

        qt_sql_3_10 """
            SELECT
                repo_name,
                sum(num_star) AS num_stars,
                sum(num_comment) AS num_comments
            FROM
            (
                SELECT
                    v:repo.name as repo_name,
                    CASE WHEN v:type = 'WatchEvent' THEN 1 ELSE 0 END AS num_star,
                    CASE WHEN lower(v:payload.comment.body) LIKE '%spark%' THEN 1 ELSE 0 END AS num_comment
                FROM github_events
            ) t
            GROUP BY repo_name
            HAVING num_comments > 0
            ORDER BY num_stars DESC,num_comments DESC,repo_name ASC
            LIMIT 5
        """

        

        qt_sql_3_11 """
            SELECT
                repo_name,
                total_stars,
                round(spark_stars / total_stars, 2) AS ratio
                FROM
                (
                    SELECT
                        v:repo.name as repo_name,
                        count(distinct v:actor.login) AS total_stars
                    FROM github_events
                    GROUP BY repo_name
                    HAVING total_stars >= 10
                ) t1
                JOIN
                (
                    SELECT
                        count(distinct v:actor.login) AS spark_stars
                    FROM github_events
                ) t2
                ORDER BY ratio DESC, repo_name
                LIMIT 5
        """

        qt_sql_3_12 """
            SELECT
                sum(forks) AS forks,
                sum(stars) AS stars,
                round(sum(stars) / sum(forks), 2) AS ratio
            FROM
            (
                SELECT
                    sum(fork) AS forks,
                    sum(star) AS stars
                FROM
                (
                    SELECT
                        v:repo.name as repo_name,
                        CASE WHEN v:type = 'ForkEvent' THEN 1 ELSE 0 END AS fork,
                        CASE WHEN v:type = 'WatchEvent' THEN 1 ELSE 0 END AS star
                    FROM github_events
                ) t
                GROUP BY repo_name
                HAVING stars > 10
            ) t2
        """



    } finally {
        // reset flags
        set_be_config.call("ratio_of_defaults_as_sparse_column", "0.95")
    }
}
