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

suite("test_external_catalog_icebergv2", "p2") {
    String enabled = context.config.otherConfigs.get("enableExternalHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String extHiveHmsHost = context.config.otherConfigs.get("extHiveHmsHost")
        String extHiveHmsPort = context.config.otherConfigs.get("extHiveHmsPort")
        String catalog_name = "test_external_catalog_iceberg"

        sql """drop catalog if exists ${catalog_name};"""
        sql """
            create catalog if not exists ${catalog_name} properties (
                'type'='hms',
                'hive.metastore.uris' = 'thrift://${extHiveHmsHost}:${extHiveHmsPort}'
            );
        """

        sql """switch ${catalog_name};"""
        // test parquet format format
        def q01 = {
            qt_q01 """ select count(1) as c from customer_small """
            qt_q02 """ select c_custkey from customer_small group by c_custkey order by c_custkey limit 4 """
            qt_q03 """ select count(1) from orders_small """
            qt_q04 """ select count(1) from customer_small where c_name = 'Customer#000000005' or c_name = 'Customer#000000006' """
            qt_q05 """ select * from customer_small order by c_custkey limit 3 """
            qt_q06 """ select o_orderkey from orders_small where o_orderkey > 652566 order by o_orderkey limit 3 """
            qt_q07 """ select o_totalprice from orders_small where o_custkey < 3357 order by o_custkey limit 3 """
            qt_q08 """ select count(1) as c from customer """
        }
        // test time travel stmt
        def q02 = {
            qt_q09 """ select c_custkey from customer for time as of '2022-12-27 10:21:36' order by c_custkey limit 3 """
            qt_q10 """ select c_custkey from customer for time as of '2022-12-28 10:21:36' order by c_custkey desc limit 3 """
            qt_q11 """ select c_custkey from customer for version as of 906874575350293177 order by c_custkey limit 3 """
            qt_q12 """ select c_custkey from customer for version as of 6352416983354893547 order by c_custkey desc limit 3 """
        }
        // in predicate
        def q03 = {
            qt_q13 """ select c_custkey from customer_small where c_custkey in (1, 2, 4, 7) order by c_custkey """
            qt_q14 """ select c_name from customer_small where c_name in ('Customer#000000004', 'Customer#000000007') order by c_custkey """
        }
        sql """ use `tpch_1000_icebergv2`; """
        q01()
        q02()
        q03()
    }
}
