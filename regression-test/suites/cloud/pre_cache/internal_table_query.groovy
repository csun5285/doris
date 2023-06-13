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
import java.time.LocalDate

suite("test_cloud_dynamic_partition") {
    // insert multi values with different insert day
    try_sql """
insert into __internal_schema.selectdb_cache_hotspot 
values("lightman_cluster_id0", 10003, 11002, 11003, 11414, "${LocalDate.now().toString()}", 
"regression_test.selectdb_cache_hotspot", "selectdb_cache_hotspot", "p20230529", "lightman_cluster_name0", 1234567, 1, 1, "2023-05-29 12:38:02");
    """

    try_sql """
insert into __internal_schema.selectdb_cache_hotspot 
values("lightman_cluster_id0", 10003, 11002, 11003, 11414, "${LocalDate.now().minusDays(1).toString()}", 
"regression_test.selectdb_cache_hotspot", "selectdb_cache_hotspot", "p20230529", "lightman_cluster_name0", 123456, 1, 1, "2023-05-29 12:38:02");
    """

    try_sql """
insert into __internal_schema.selectdb_cache_hotspot 
values("lightman_cluster_id0", 10003, 11002, 11003, 11414, "${LocalDate.now().minusDays(2).toString()}", 
"regression_test.selectdb_cache_hotspot", "selectdb_cache_hotspot", "p20230529", "lightman_cluster_name0", 12345, 1, 1, "2023-05-29 12:38:02");
    """

    try_sql """
insert into __internal_schema.selectdb_cache_hotspot 
values("lightman_cluster_id0", 10003, 11002, 11003, 11414, "${LocalDate.now().minusDays(3).toString()}", 
"regression_test.selectdb_cache_hotspot", "selectdb_cache_hotspot", "p20230529", "lightman_cluster_name0", 1234, 2, 1, "2023-05-29 12:38:02");
    """

    // insert value with different backend id, to test if it would sum all the table's file cache size under one cluster
    try_sql """
insert into __internal_schema.selectdb_cache_hotspot 
values("lightman_cluster_id0", 10004, 11102, 11103, 111414, "${LocalDate.now().toString()}", 
"regression_test.selectdb_cache_hotspot", "selectdb_cache_hotspot", "p20230529", "lightman_cluster_name0", 3456, 1, 1, "2023-05-29 12:38:02");
    """

    try_sql """
insert into __internal_schema.selectdb_cache_hotspot 
values("lightman_cluster_id0", 10004, 11002, 11003, 112414, "${LocalDate.now().toString()}", 
"regression_test.selectdb_cache_hotspot", "selectdb_cache_hotspot", "p20230602", "lightman_cluster_name0", 6789, 1, 1, "2023-05-29 12:38:02");
    """

    // insert value with different table
    try_sql """
insert into __internal_schema.selectdb_cache_hotspot 
values("lightman_cluster_id0", 10005, 110020, 110030, 114140, "${LocalDate.now().toString()}", 
"regression_test.selectdb_cache_hotspot_1", "selectdb_cache_hotspot_1", "p20230529", "lightman_cluster_name0", 2456, 1, 1, "2023-05-29 12:38:02");
    """

    // insert value with different qpd within different partition in one table
    try_sql """
insert into __internal_schema.selectdb_cache_hotspot 
values("lightman_cluster_id0", 10003, 11002, 11003, 114514, "${LocalDate.now().toString()}", 
"regression_test.selectdb_cache_hotspot", "selectdb_cache_hotspot", "p20230601", "lightman_cluster_name0", 8654, 5, 1, "2023-05-29 12:38:02");
    """

    // insert value with different cluster id
    try_sql """
insert into __internal_schema.selectdb_cache_hotspot 
values("lightman_cluster_id1", 10003, 11002, 11003, 11414, "${LocalDate.now().toString()}", 
"regression_test.selectdb_cache_hotspot", "selectdb_cache_hotspot", "p20230529", "lightman_cluster_name1", 1996, 1, 1, "2023-05-29 12:38:02");
    """

    qt_sql """
    show cache hotspot '/';
    """

    qt_sql """
    show cache hotspot '/lightman_cluster_name0';
    """

    qt_sql """
    show cache hotspot '/lightman_cluster_name0/regression_test.selectdb_cache_hotspot';
    """
    
    // to test if it returns the latest partition's data
    
    qt_sql """
SELECT file_cache_size FROM (SELECT *, row_number() OVER (PARTITION BY cluster_id, backend_id, table_id, index_id, partition_id ORDER BY insert_day DESC) AS rn
FROM __internal_schema.selectdb_cache_hotspot) t WHERE rn = 1;
    """
    
    
    // test if 
    try_sql """
        drop table if exists sample_table force;
    """
    try_sql """
        CREATE TABLE IF NOT EXISTS sample_table
        (
            id INT,
            V1 VARCHAR(2048) REPLACE
        ) PARTITION BY RANGE (id) (
            PARTITION plessThan1 VALUES LESS THAN ("0"),
            PARTITION plessThan2 VALUES LESS THAN ("100")
        ) DISTRIBUTED BY HASH(id) BUCKETS 1;
    """

    sql """
    insert into sample_table values(1, "123"),(-1, "123"),(-2,"222"),(3,"456");
    """
    sql """
    select * from sample_table;
    """
    // get higher qpd
    try_sql """
    select * from sample_table;
    """
    try_sql """
    select * from sample_table;
    """
    try_sql """
    select * from sample_table;
    """
    try_sql """
    select * from sample_table;
    """
    try_sql """
    select * from sample_table;
    """
    // sleep 5 min for internal table to fetch information from be
    sleep(330000)
    def clusters = sql " SHOW CLUSTERS; "
    assertTrue(!clusters.isEmpty())
    def validCluster = clusters[0][0]
    
    List<List<Object>> hotspots = sql """
    show cache hotspot '/';
    """
    String clusterId;
    for (List<Object> infos: hotspots) {
        if (infos[2].contains("sample_table")) {
            clusterId = infos[0]
        }
    }

    hotspots = sql """
    show cache hotspot '/${clusterId}/regression_test_cloud_pre_cache.sample_table';
    """
    assertTrue(hotspots.size() == 2)
}
