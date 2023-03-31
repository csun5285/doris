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

suite("test_disable_routine_load") {
    try {
        def tableName = "test_disable_routine_load"
        sql """ DROP TABLE IF EXISTS ${tableName}; """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName}
            (
                siteid INT DEFAULT '10',
                citycode SMALLINT,
                username VARCHAR(32) DEFAULT '',
                pv BIGINT SUM DEFAULT '0'
            )
            AGGREGATE KEY(siteid, citycode, username)
            DISTRIBUTED BY HASH(siteid) BUCKETS 1;
        """

        sql """
            CREATE ROUTINE LOAD disable_routine_load_test6 ON 
            ${tableName} COLUMNS TERMINATED BY "|",
            COLUMNS(siteid,citycode,username,pv)
            PROPERTIES(
            "desired_concurrent_number"="1",
            "max_batch_rows"="200000",
            "max_batch_size"="104857600")
            FROM KAFKA(
            "kafka_broker_list"="broker1:9092,broker2:9092,broker3:9092",
            "kafka_topic"="doris",
            "property.group.id"="gid6",
            "property.clinet.id"="cid6",
            "property.kafka_default_offsets"="OFFSET_BEGINNING");
            """

    } catch (Exception e) {
        assertTrue(e.getMessage().contains("Unsupported operation"), e.getMessage())
    }
}
