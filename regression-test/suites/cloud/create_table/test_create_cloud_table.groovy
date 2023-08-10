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

suite("test_create_cloud_table") {
    sql "ADMIN SET FRONTEND CONFIG ('ignore_unsupported_properties_in_cloud_mode' = 'false')"

    sql "drop table if exists table_1"
    sql """
        CREATE TABLE table_1 ( k1 date NOT NULL, k2 varchar(20) NOT NULL, k3 int sum NOT NULL ) 
        AGGREGATE KEY(k1,k2) 
        DISTRIBUTED BY HASH(k1) BUCKETS 3 
        """
    List<List<Object>> result  = sql "show tables like 'table_1'"
    logger.info("${result}")
    assertEquals(result.size(), 1)

    sql "drop table table_1"

    sql "drop table if exists table_2"
    test {
        sql """
        CREATE TABLE table_2 ( k1 date NOT NULL, k2 varchar(20) NOT NULL, k3 int sum NOT NULL ) 
        AGGREGATE KEY(k1,k2) 
        DISTRIBUTED BY HASH(k1) BUCKETS 3 
        """
        // check exception message contains
        exception "errCode = 2,"
    }
    sql "drop table if exists table_2"

    sql "drop table if exists table_p1"
    test {
        sql """
        CREATE TABLE table_p1 ( k1 int(11) NOT NULL, k2 varchar(20) NOT NULL, k3 int sum NOT NULL ) 
        AGGREGATE KEY(k1, k2)
        PARTITION BY RANGE(k1) (
        PARTITION p1992 VALUES [("-2147483648"), ("19930101")),
        PARTITION p1993 VALUES [("19930101"), ("19940101")),
        PARTITION p1994 VALUES [("19940101"), ("19950101")),
        PARTITION p1995 VALUES [("19950101"), ("19960101")),
        PARTITION p1996 VALUES [("19960101"), ("19970101")),
        PARTITION p1997 VALUES [("19970101"), ("19980101")),
        PARTITION p1998 VALUES [("19980101"), ("19990101")))
        DISTRIBUTED BY HASH(k1) BUCKETS 3 
        """
        // check exception message contains
        exception "errCode = 2,"
    }
    sql "drop table if exists table_p1"

    sql "ADMIN SET FRONTEND CONFIG ('ignore_unsupported_properties_in_cloud_mode' = 'true')"

    sql "drop table if exists table_3"
    sql """
        CREATE TABLE table_3 ( k1 date NOT NULL, k2 varchar(20) NOT NULL, k3 int sum NOT NULL ) 
        AGGREGATE KEY(k1,k2) 
        DISTRIBUTED BY HASH(k1) BUCKETS 3 
        """
    result  = sql "show tables like 'table_3'"
    logger.info("${result}")
    assertEquals(result.size(), 1)
    sql "drop table table_3"

    sql "drop table if exists table_4"
    test {
        sql """
        CREATE TABLE table_4 ( k1 date NOT NULL, k2 varchar(20) NOT NULL, k3 int sum NOT NULL ) 
        AGGREGATE KEY(k1,k2) 
        DISTRIBUTED BY HASH(k1) BUCKETS 3 
        PROPERTIES (
            "compression"="zstd")
        """
    }

    result  = sql "show tables like 'table_4'"
    logger.info("${result}")
    assertEquals(result.size(), 1)
    sql "drop table table_4"

    sql "drop table if exists table_p2"
    test {
        sql """
        CREATE TABLE table_p2 ( k1 int(11) NOT NULL, k2 varchar(20) NOT NULL, k3 int sum NOT NULL ) 
        AGGREGATE KEY(k1, k2)
        PARTITION BY RANGE(k1) (
        PARTITION p1992 VALUES [("-2147483648"), ("19930101")),
        PARTITION p1993 VALUES [("19930101"), ("19940101")),
        PARTITION p1994 VALUES [("19940101"), ("19950101")),
        PARTITION p1995 VALUES [("19950101"), ("19960101")),
        PARTITION p1996 VALUES [("19960101"), ("19970101")),
        PARTITION p1997 VALUES [("19970101"), ("19980101")),
        PARTITION p1998 VALUES [("19980101"), ("19990101")))
        DISTRIBUTED BY HASH(k1) BUCKETS 3 
        """
        // check exception message contains
    }

    result  = sql "show tables like 'table_p2'"
    logger.info("${result}")
    assertEquals(result.size(), 1)

    sql "drop table if exists table_p2"
}
