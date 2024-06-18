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

suite("test_json_keys", "p0") {
    // json_keys
    qt_sql_json_keys """SELECT json_keys('{"k1":"v31","k2":300}')"""
    qt_sql_json_keys """SELECT json_keys('{"a.b.c":{"k1.a1":"v31", "k2": 300},"a":"niu"}')"""
    qt_sql_json_keys """SELECT json_keys('{"a":{"k1.a1":"v31", "k2": 300},"b":"niu"}','\$.a')"""
    qt_sql_json_keys """SELECT json_keys('abc','\$.k1')"""
    qt_sql_json_keys """SELECT json_keys('["a", "b", "c"]', '\$.k2')"""
    qt_sql_json_keys """SELECT json_keys('["a", "b", "c"]')"""
    qt_sql_json_keys """SELECT json_keys('["a", "b", "c"]', '\$[1]')"""

    // error keys
    test {
        sql """ SELECT JSON_KEYS('{"a": {}, "a.b.c": {"c": 30}}', ''); """
        exception("Invalid Json Path for value")
    }

    test {
        sql """ SELECT JSON_KEYS('{"a": {}, "a.b.c": {"c": 30}}', 'a.b.c'); """
        exception("Invalid Json Path for value")
    }

    // make table with path
    sql """
        CREATE TABLE IF NOT EXISTS json_keys_table (
            id INT,
            j JSONB,
            p STRING
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 10
        PROPERTIES("replication_num" = "1");
        """

    sql """ insert into json_keys_table values (1, '{"a.b.c":{"k1.a1":"v31", "k2": 300}, "a": {}}', '\$.a'), (2, '{"a.b.c":{"k1.a1":"v31", "k2": 300}}', '\$.a.b.c'), (3, '{"a.b.c":{"k1.a1":"v31", "k2": 300}, "a": {"k1.a1": 1}}', '\$.a'), (4, '["a", "b"]', '\$.a'); """
    qt_select_json_keys """SELECT j, p, json_keys(j, p) FROM json_keys_table ORDER BY id"""
}

