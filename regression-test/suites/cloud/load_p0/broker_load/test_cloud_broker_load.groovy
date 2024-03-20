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

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.DistributedFileSystem
import java.net.URI
import org.apache.hadoop.fs.Path
import org.apache.hadoop.util.Progressable
import java.io.BufferedWriter
import java.io.OutputStreamWriter

suite("test_cloud_broker_load", "external,external_docker,external_docker_hive,hive") {
    Configuration configuration = new Configuration();
    configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
    FileSystem hdfs = FileSystem.get(new URI(getHdfsFs()), configuration);
    hdfs.copyFromLocalFile(new Path("regression-test/data/cloud/load_p0/broker_load/all_types.csv"), new Path("/broker/all_types.csv"));

    // test common case
    def tableName3 = "test_broker_all_type"

    sql """ drop table if exists ${tableName3} """

    sql """
    CREATE TABLE IF NOT EXISTS ${tableName3} (
      `k1` int(11) NULL,
      `k2` tinyint(4) NULL,
      `k3` smallint(6) NULL,
      `k4` bigint(20) NULL,
      `k5` largeint(40) NULL,
      `k6` float NULL,
      `k7` double NULL,
      `k8` decimal(9, 0) NULL,
      `k9` char(10) NULL,
      `k10` varchar(1024) NULL,
      `k11` text NULL,
      `k12` date NULL,
      `k13` datetime NULL
    ) ENGINE=OLAP
    DISTRIBUTED BY HASH(`k1`) BUCKETS 3
    """

    long timestamp = System.currentTimeMillis()
    String job_name = "broker_load_test_" + String.valueOf(timestamp);
    sql """
    LOAD LABEL ${job_name}
    (
        DATA INFILE("${hdfsFs}/broker/all_types.csv")
        INTO TABLE `test_broker_all_type`
        COLUMNS TERMINATED BY ","            (k1,k2,k3,k4,k5,k6,k7,k8,k9,k10,k11,k12,k13)
    ) 
    with HDFS (
        "fs.defaultFS"="${hdfsFs}",
        "hadoop.username"="${hdfsUser}"
    )
    PROPERTIES
    (
        "timeout"="1200",
        "max_filter_ratio"="0.1"
    );
    """

    sleep(10000)
    order_qt_all11 "SELECT count(*) FROM ${tableName3}" // 2500
    order_qt_all12 "SELECT count(*) FROM ${tableName3} where k1 <= 10"  // 11
}

