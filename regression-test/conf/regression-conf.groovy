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

/* ******* Do not commit this file unless you know what you are doing ******* */

// **Note**: default db will be create if not exist
defaultDb = "regression_test"

jdbcUrl = "jdbc:mysql://127.0.0.1:8902/?"
jdbcUser = "root"
jdbcPassword = ""

feHttpAddress = "127.0.0.1:8900"
feHttpUser = "root"
feHttpPassword = ""

beHttpAddress = "127.0.0.1"
instanceId = "gavin-debug"
cloudUniqueId = ""
metaServiceHttpAddress = "127.0.0.1:5000"
recycleServiceHttpAddress = "127.0.0.1:5100"
feCloudHttpAddress = "127.0.0.1:8904"

// set DORIS_HOME by system properties
// e.g. java -DDORIS_HOME=./
suitePath = "/mnt/disk1/gavinchou/debug/regression/regression-test/suites"
dataPath = "/mnt/disk1/gavinchou/debug/regression/regression-test/data"
sf1DataPath = "/mnt/disk1/gavinchou/debug/regression/data"

// will test <group>/<suite>.groovy
// empty group will test all group
testGroups = ""
// empty suite will test all suite
testSuites = ""
// empty directories will test all directories
testDirectories = ""

// this groups will not be executed
excludeGroups = ""

// this suites will not be executed
excludeSuites = "test_clean_label, \
test_alter_user,test_explain_tpch_sf_1_q4,test_explain_tpch_sf_1_q20, \
test_explain_tpch_sf_1_q21,test_explain_tpch_sf_1_q13,test_explain_tpch_sf_1_q22, \
test_ctl,redundant_conjuncts,test_disable_management_cluster"
// this directories will not be executed
excludeDirectories = "backup_restore,compaction, cold_heat_separation, javaudf_p0, \
tpcds_sf1000_p2,primary_index,github_events_p2,,schema_change_p0, \
schema_change, cloud/smoke, cloud/multi_cluster, load_p0/broker_load, \
tpch_sf1_p1/tpch_sf1/explain"

excludeSuites = ""
excludeDirectories = ""

customConf1 = "test_custom_conf_value"

// for test csv with header
enableHdfs=false // set to true if hdfs is ready
hdfsFs = "hdfs://127.0.0.1:9000"
hdfsUser = "doris-test"
hdfsPasswd = ""
brokerName = "broker_name"

// s3Endpoint = "cos.ap-beijing.myqcloud.com"
// s3BucketName = "doris-build-1308700295"
// ak = "AKIDAE2aqpY0B7oFPIvHMBj01lFSO3RYOxFH"
// sk = "nJYWDepkQqzrWv3uWsxlJ0ScV7SXLs88"

s3Endpoint = "cos.ap-hongkong.myqcloud.com"
s3BucketName = "doris-build-hk-1308700295"
ak = "AKIDPhmeKo46sPhkZ9USWQEqGTSDsW5Xr6IL"
sk = "xmT2Uoz0vgGkbKr6A4mGWcWCiBVcYJSV"
s3Region = "ap-hongkong"
s3Provider = "COS"

// enableJdbcTest：开启 jdbc 外表测试，需要启动 MySQL 和 Postgresql 的 container。
// mysql_57_port 和 pg_14_port 分别对应 MySQL 和 Postgresql 的对外端口，默认为 3316 和 5442。
// enableHiveTest：开启 hive 外表测试，需要启动 hive 的 container。
// hms_port 对应 hive metastore 的对外端口，默认为 9183。
enableEsTest=true
es_6_port=19200
es_7_port=29200
es_8_port=39200
