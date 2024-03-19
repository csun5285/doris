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

import org.codehaus.groovy.runtime.IOGroovyMethods;
import java.util.Random;
import org.apache.commons.lang.RandomStringUtils;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.UUID;
import java.time.format.DateTimeFormatter;

suite("test_pk_uk_index_change", "inverted_index") {
    def timeout = 60000
    def delta_time = 1000
    def alter_res = "null"
    def useTime = 0

    def wait_for_latest_op_on_table_finish = { table_name, OpTimeout ->
        for(int t = delta_time; t <= OpTimeout; t += delta_time){
            alter_res = sql """SHOW ALTER TABLE COLUMN WHERE TableName = "${table_name}" ORDER BY CreateTime DESC LIMIT 1;"""
            alter_res = alter_res.toString()
            if(alter_res.contains("FINISHED")) {
                sleep(3000) // wait change table state to normal
                logger.info(table_name + " latest alter job finished, detail: " + alter_res)
                break
            }
            useTime = t
            sleep(delta_time)
        }
        assertTrue(useTime <= OpTimeout, "wait_for_latest_op_on_table_finish timeout")
    }

    def wait_for_build_index_on_partition_finish = { table_name, OpTimeout ->
        for(int t = delta_time; t <= OpTimeout; t += delta_time){
            alter_res = sql """SHOW BUILD INDEX WHERE TableName = "${table_name}";"""
            def expected_finished_num = alter_res.size();
            def finished_num = 0;
            for (int i = 0; i < expected_finished_num; i++) {
                logger.info(table_name + " build index job state: " + alter_res[i][7] + i
                            + " expected_finished_num=" + expected_finished_num)
                if (alter_res[i][7] == "FINISHED") {
                    ++finished_num;
                }
            }
            if (finished_num == expected_finished_num) {
                logger.info(table_name + " all build index jobs finished, detail: " + alter_res)
                break
            }
            useTime = t
            sleep(delta_time)
        }
        assertTrue(useTime <= OpTimeout, "wait_for_latest_build_index_on_partition_finish timeout")
    }
    
    def tableNamePk = "primary_key_pk_uk"
    def tableNameUk = "unique_key_pk_uk"

    onFinish {
        try_sql("DROP TABLE IF EXISTS ${tableNamePk}")
        try_sql("DROP TABLE IF EXISTS ${tableNameUk}")
    }

    sql """ DROP TABLE IF EXISTS ${tableNamePk} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableNamePk} (
        L_ORDERKEY    INTEGER NOT NULL,
        L_PARTKEY     INTEGER NOT NULL,
        L_SUPPKEY     INTEGER NOT NULL,
        L_LINENUMBER  INTEGER NOT NULL,
        L_QUANTITY    DECIMAL(15,2) NOT NULL,
        L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,
        L_DISCOUNT    DECIMAL(15,2) NOT NULL,
        L_TAX         DECIMAL(15,2) NOT NULL,
        L_RETURNFLAG  CHAR(1) NOT NULL,
        L_LINESTATUS  CHAR(1) NOT NULL,
        L_SHIPDATE    DATE NOT NULL,
        L_COMMITDATE  DATE NOT NULL,
        L_RECEIPTDATE DATE NOT NULL,
        L_SHIPINSTRUCT CHAR(60) NOT NULL,
        L_SHIPMODE     CHAR(60) NOT NULL,
        L_COMMENT      VARCHAR(60) NOT NULL,
        INDEX L_ORDERKEY_idx(L_ORDERKEY) USING INVERTED COMMENT 'L_ORDERKEY index',
        INDEX L_PARTKEY_idx(L_PARTKEY) USING INVERTED COMMENT 'L_PARTKEY index',
        INDEX L_SUPPKEY_idx(L_SUPPKEY) USING INVERTED COMMENT 'L_SUPPKEY index',
        INDEX L_LINENUMBER_idx(L_LINENUMBER) USING INVERTED COMMENT 'L_LINENUMBER index',
        INDEX L_QUANTITY_idx(L_QUANTITY) USING INVERTED COMMENT 'L_QUANTITY index',
        INDEX L_RETURNFLAG_idx(L_RETURNFLAG) USING INVERTED COMMENT 'L_RETURNFLAG index',
        INDEX L_SHIPDATE_idx(L_SHIPDATE) USING INVERTED COMMENT 'L_SHIPDATE index'
        )
        UNIQUE KEY(L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER)
        DISTRIBUTED BY HASH(L_ORDERKEY) BUCKETS 1
        PROPERTIES (
        "replication_num" = "1",
        "enable_unique_key_merge_on_write" = "true"
        )
    """

    sql """ DROP TABLE IF EXISTS ${tableNameUk} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableNameUk} (
        L_ORDERKEY    INTEGER NOT NULL,
        L_PARTKEY     INTEGER NOT NULL,
        L_SUPPKEY     INTEGER NOT NULL,
        L_LINENUMBER  INTEGER NOT NULL,
        L_QUANTITY    DECIMAL(15,2) NOT NULL,
        L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,
        L_DISCOUNT    DECIMAL(15,2) NOT NULL,
        L_TAX         DECIMAL(15,2) NOT NULL,
        L_RETURNFLAG  CHAR(1) NOT NULL,
        L_LINESTATUS  CHAR(1) NOT NULL,
        L_SHIPDATE    DATE NOT NULL,
        L_COMMITDATE  DATE NOT NULL,
        L_RECEIPTDATE DATE NOT NULL,
        L_SHIPINSTRUCT CHAR(60) NOT NULL,
        L_SHIPMODE     CHAR(60) NOT NULL,
        L_COMMENT      VARCHAR(60) NOT NULL
        )
        UNIQUE KEY(L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER)
        DISTRIBUTED BY HASH(L_ORDERKEY) BUCKETS 1
        PROPERTIES (
        "replication_num" = "1",
        "enable_unique_key_merge_on_write" = "false"
        )       
    """

    Random rd = new Random()
    def order_key = rd.nextInt(1000)
    def part_key = rd.nextInt(1000)
    def sub_key = 13
    def line_num = 29
    def decimal = rd.nextInt(1000) + 0.11
    def city = RandomStringUtils.randomAlphabetic(10)
    def name = UUID.randomUUID().toString()
    def date = DateTimeFormatter.ofPattern("yyyy-MM-dd").format(LocalDateTime.now())
    for (int idx = 0; idx < 5; idx++) {
        order_key = rd.nextInt(10)
        part_key = rd.nextInt(10)
        city = RandomStringUtils.randomAlphabetic(10)
        name = UUID.randomUUID().toString()
        sql """ INSERT INTO ${tableNamePk} VALUES
            ($order_key, $part_key, $sub_key, $line_num, 
            $decimal, $decimal, $decimal, $decimal, '1', '1', '$date', '$date', '$date', '$name', '$name', '$city')
            """
        sql """ INSERT INTO ${tableNameUk} VALUES
            ($order_key, $part_key, $sub_key, $line_num,
            $decimal, $decimal, $decimal, $decimal, '1', '1', '$date', '$date', '$date', '$name', '$name', '$city')
            """

        order_key = rd.nextInt(10)
        part_key = rd.nextInt(10)
        city = RandomStringUtils.randomAlphabetic(10)
        name = UUID.randomUUID().toString()
        sql """ INSERT INTO ${tableNamePk} VALUES
            ($order_key, $part_key, $sub_key, $line_num, 
            $decimal, $decimal, $decimal, $decimal, '1', '1', '$date', '$date', '$date', '$name', '$name', '$city')
            """
        sql """ INSERT INTO ${tableNameUk} VALUES
            ($order_key, $part_key, $sub_key, $line_num, 
            $decimal, $decimal, $decimal, $decimal, '1', '1', '$date', '$date', '$date', '$name', '$name', '$city')
            """

        order_key = rd.nextInt(10)
        part_key = rd.nextInt(10)
        city = RandomStringUtils.randomAlphabetic(10)
        name = UUID.randomUUID().toString()
        sql """ INSERT INTO ${tableNamePk} VALUES
            ($order_key, $part_key, $sub_key, $line_num, 
            $decimal, $decimal, $decimal, $decimal, '1', '1', '$date', '$date', '$date', '$name', '$name', '$city')
            """
        sql """ INSERT INTO ${tableNameUk} VALUES
            ($order_key, $part_key, $sub_key, $line_num,
            $decimal, $decimal, $decimal, $decimal, '1', '1', '$date', '$date', '$date', '$name', '$name', '$city')
            """

        order_key = rd.nextInt(10)
        part_key = rd.nextInt(10)
        city = RandomStringUtils.randomAlphabetic(10)
        name = UUID.randomUUID().toString()
        sql """ INSERT INTO ${tableNamePk} VALUES
            ($order_key, $part_key, $sub_key, $line_num, 
            $decimal, $decimal, $decimal, $decimal, '1', '1', '$date', '$date', '$date', '$name', '$name', '$city')
            """
        sql """ INSERT INTO ${tableNameUk} VALUES
            ($order_key, $part_key, $sub_key, $line_num, 
            $decimal, $decimal, $decimal, $decimal, '1', '1', '$date', '$date', '$date', '$name', '$name', '$city')
            """
        
        order_key = rd.nextInt(10)
        part_key = rd.nextInt(10)
        city = RandomStringUtils.randomAlphabetic(10)
        name = UUID.randomUUID().toString()
        sql """ INSERT INTO ${tableNamePk} VALUES
            ($order_key, $part_key, $sub_key, $line_num, 
            $decimal, $decimal, $decimal, $decimal, '1', '1', '$date', '$date', '$date', '$name', '$name', '$city')
            """
        sql """ INSERT INTO ${tableNameUk} VALUES
            ($order_key, $part_key, $sub_key, $line_num, 
            $decimal, $decimal, $decimal, $decimal, '1', '1', '$date', '$date', '$date', '$name', '$name', '$city')
            """

        // insert batch key 
        order_key = rd.nextInt(10)
        part_key = rd.nextInt(10)
        city = RandomStringUtils.randomAlphabetic(10)
        name = UUID.randomUUID().toString()
        sql """ INSERT INTO ${tableNamePk} VALUES
            ($order_key, $part_key, $sub_key, $line_num, $decimal, $decimal, $decimal, $decimal, '1', '1', '$date', '$date', '$date', '$name', '$name', '$city'),
            ($order_key, $part_key, $sub_key, $line_num, $decimal, $decimal, $decimal, $decimal, '1', '1', '$date', '$date', '$date', '$name', '$name', '$city'),
            ($order_key, $part_key, $sub_key, $line_num, $decimal, $decimal, $decimal, $decimal, '1', '1', '$date', '$date', '$date', '$name', '$name', '$city'),
            ($order_key, $part_key, $sub_key, $line_num, $decimal, $decimal, $decimal, $decimal, '1', '1', '$date', '$date', '$date', '$name', '$name', '$city')
        """
        sql """ INSERT INTO ${tableNameUk} VALUES
            ($order_key, $part_key, $sub_key, $line_num, $decimal, $decimal, $decimal, $decimal, '1', '1', '$date', '$date', '$date', '$name', '$name', '$city'),
            ($order_key, $part_key, $sub_key, $line_num, $decimal, $decimal, $decimal, $decimal, '1', '1', '$date', '$date', '$date', '$name', '$name', '$city'),
            ($order_key, $part_key, $sub_key, $line_num, $decimal, $decimal, $decimal, $decimal, '1', '1', '$date', '$date', '$date', '$name', '$name', '$city'),
            ($order_key, $part_key, $sub_key, $line_num, $decimal, $decimal, $decimal, $decimal, '1', '1', '$date', '$date', '$date', '$name', '$name', '$city')
        """

        if (idx > 0) {
            // alter add inverted index
            sql """ ALTER TABLE ${tableNamePk}
                    ADD INDEX L_ORDERKEY_idx (L_ORDERKEY) USING INVERTED COMMENT 'L_ORDERKEY index';
            """

            wait_for_latest_op_on_table_finish(tableNamePk, timeout)

            // build inverted index
            sql """ BUILD INDEX L_ORDERKEY_idx ON ${tableNamePk}; """
            wait_for_build_index_on_partition_finish(tableNamePk, timeout)
        }

        sql "sync"

        // count(*)
        def result0 = sql """ SELECT count(*) FROM ${tableNamePk}; """
        def result1 = sql """ SELECT count(*) FROM ${tableNameUk}; """
        logger.info("result:" + result0[0][0] + "|" + result1[0][0])
        assertTrue(result0[0]==result1[0])
        if (result0[0][0]!=result1[0][0]) {
            logger.info("result:" + result0[0][0] + "|" + result1[0][0])
        }

        result0 = sql """ SELECT
                            l_returnflag,
                            l_linestatus,
                            sum(l_quantity)                                       AS sum_qty,
                            sum(l_extendedprice)                                  AS sum_base_price,
                            sum(l_extendedprice * (1 - l_discount))               AS sum_disc_price,
                            sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
                            avg(l_quantity)                                       AS avg_qty,
                            avg(l_extendedprice)                                  AS avg_price,
                            avg(l_discount)                                       AS avg_disc,
                            count(*)                                              AS count_order
                            FROM
                            ${tableNamePk}
                            GROUP BY
                            l_returnflag,
                            l_linestatus
                            ORDER BY
                            l_returnflag,
                            l_linestatus
                        """
        result1 = sql """ SELECT
                            l_returnflag,
                            l_linestatus,
                            sum(l_quantity)                                       AS sum_qty,
                            sum(l_extendedprice)                                  AS sum_base_price,
                            sum(l_extendedprice * (1 - l_discount))               AS sum_disc_price,
                            sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
                            avg(l_quantity)                                       AS avg_qty,
                            avg(l_extendedprice)                                  AS avg_price,
                            avg(l_discount)                                       AS avg_disc,
                            count(*)                                              AS count_order
                            FROM
                            ${tableNameUk}
                            GROUP BY
                            l_returnflag,
                            l_linestatus
                            ORDER BY
                            l_returnflag,
                            l_linestatus
                        """  
        assertTrue(result0.size()==result1.size())
        for (int i = 0; i < result0.size(); ++i) {
            for (j = 0; j < result0[0].size(); j++) {
                logger.info("result: " + result0[i][j] + "|" + result1[i][j])
                assertTrue(result0[i][j]==result1[i][j])
            }
        }       

        // delete
        if (idx % 10 == 0) {
            order_key = rd.nextInt(10)
            part_key = rd.nextInt(10)
            result0 = sql """ SELECT count(*) FROM ${tableNamePk} where L_ORDERKEY < $order_key and L_PARTKEY < $part_key; """
            result1 = sql """ SELECT count(*) FROM ${tableNameUk} where L_ORDERKEY < $order_key and L_PARTKEY < $part_key"""
            logger.info("result:" + result0[0][0] + "|" + result1[0][0])
            sql "DELETE FROM ${tableNamePk} where L_ORDERKEY < $order_key and L_PARTKEY < $part_key"
            sql "DELETE FROM ${tableNameUk} where L_ORDERKEY < $order_key and L_PARTKEY < $part_key"
        }

        // drop inverted index
        sql """ DROP INDEX L_ORDERKEY_idx ON ${tableNamePk}; """
        wait_for_latest_op_on_table_finish(tableNamePk, timeout)
        wait_for_build_index_on_partition_finish(tableNamePk, timeout)
    }
}

