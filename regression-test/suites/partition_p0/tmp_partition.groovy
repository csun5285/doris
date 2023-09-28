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
suite("tmp_partition") {
    sql "drop table if exists tmp_part_table"

    sql """
        CREATE TABLE IF NOT EXISTS tmp_part_table
        ( k1 DATETIME NOT NULL,
          k2 varchar(20) NOT NULL,
          k3 int sum NOT NULL 
        )
        AGGREGATE KEY(k1,k2)
        PARTITION BY RANGE(k1) (
            PARTITION p20230925 VALUES [('2023-09-25 00:00:00'), ('2023-09-26 00:00:00')),
            PARTITION p20230926 VALUES [('2023-09-26 00:00:00'), ('2023-09-27 00:00:00'))
        )
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        """
    sql """insert into tmp_part_table values ('2023-09-25 01:00:00', 'abc', 1) """

    sql """ ALTER TABLE tmp_part_table ADD TEMPORARY PARTITION tp20230925 VALUES [("2023-09-25 00:00:00"), ("2023-09-26 00:00:00"))  """
    sql """ insert into tmp_part_table TEMPORARY PARTITION(tp20230925) values ('2023-09-25 02:00:00', 'xyz', 2) """

    sql """ ALTER TABLE tmp_part_table REPLACE PARTITION (p20230925) WITH TEMPORARY PARTITION (tp20230925) """

    qt_select """ select * from tmp_part_table """
}
