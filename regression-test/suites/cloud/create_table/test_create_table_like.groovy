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

suite("test_create_table_like") {
    sql "drop table if exists lineorder"

    sql """
        CREATE TABLE IF NOT EXISTS lineorder (
            lo_orderdate int(11) NOT NULL COMMENT "",
            lo_orderkey bigint(20) NOT NULL COMMENT "",
            lo_linenumber bigint(20) NOT NULL COMMENT "",
            lo_custkey int(11) NOT NULL COMMENT "",
            lo_partkey int(11) NOT NULL COMMENT "",
            lo_suppkey int(11) NOT NULL COMMENT "",
            lo_orderpriority varchar(64) NOT NULL COMMENT "",
            lo_shippriority int(11) NOT NULL COMMENT "",
            lo_quantity bigint(20) NOT NULL COMMENT "",
            lo_extendedprice bigint(20) NOT NULL COMMENT "",
            lo_ordtotalprice bigint(20) NOT NULL COMMENT "",
            lo_discount bigint(20) NOT NULL COMMENT "",
            lo_revenue bigint(20) NOT NULL COMMENT "",
            lo_supplycost bigint(20) NOT NULL COMMENT "",
            lo_tax bigint(20) NOT NULL COMMENT "",
            lo_commitdate bigint(20) NOT NULL COMMENT "",
            lo_shipmode varchar(64) NOT NULL COMMENT "" )
        ENGINE=OLAP
        UNIQUE KEY(lo_orderdate, lo_orderkey, lo_linenumber)
        COMMENT "OLAP"
        PARTITION BY RANGE(lo_orderdate) (
        PARTITION p1992 VALUES [("-2147483648"), ("19930101")),
        PARTITION p1993 VALUES [("19930101"), ("19940101")),
        PARTITION p1994 VALUES [("19940101"), ("19950101")),
        PARTITION p1995 VALUES [("19950101"), ("19960101")),
        PARTITION p1996 VALUES [("19960101"), ("19970101")),
        PARTITION p1997 VALUES [("19970101"), ("19980101")),
        PARTITION p1998 VALUES [("19980101"), ("19990101")))
        DISTRIBUTED BY HASH(lo_orderkey) BUCKETS 48;
        """

    sql "drop table if exists lineorder_1"

    sql """
        CREATE TABLE lineorder_1 like lineorder;
        """

    sql "drop table if exists dy_par"

    sql """
        CREATE TABLE dy_par ( k1 date NOT NULL, k2 varchar(20) NOT NULL, k3 int sum NOT NULL ) 
        AGGREGATE KEY(k1,k2) 
        PARTITION BY RANGE(k1) ( ) 
        DISTRIBUTED BY HASH(k1) BUCKETS 3 
        PROPERTIES (  
            "dynamic_partition.enable"="true", 
            "dynamic_partition.end"="3", 
            "dynamic_partition.buckets"="10", 
            "dynamic_partition.start"="-3", 
            "dynamic_partition.prefix"="p", 
            "dynamic_partition.time_unit"="DAY", 
            "dynamic_partition.create_history_partition"="true")
        """

    sql "drop table if exists dy_par_1"

    sql "create table dy_par_1 like dy_par"
}
