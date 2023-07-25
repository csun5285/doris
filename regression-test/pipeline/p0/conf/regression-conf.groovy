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

<<<<<<< HEAD
jdbcUrl = "jdbc:mysql://172.30.32.17:9030/?useLocalSessionState=true"
jdbcUser = "root"
jdbcPassword = ""

feHttpAddress = "172.30.32.17:8030"
=======
jdbcUrl = "jdbc:mysql://172.19.0.2:9131/?useLocalSessionState=true&allowLoadLocalInfile=true"
targetJdbcUrl = "jdbc:mysql://172.19.0.2:9131/?useLocalSessionState=true&allowLoadLocalInfile=true"
jdbcUser = "root"
jdbcPassword = ""

feSourceThriftAddress = "127.0.0.1:9020"
feTargetThriftAddress = "127.0.0.1:9020"
feSyncerUser = "root"
feSyncerPassword = ""

feHttpAddress = "172.19.0.2:8131"
>>>>>>> 2.0.0-rc01
feHttpUser = "root"
feHttpPassword = ""

beHttpAddress = "172.30.32.17:8040"
instanceId = "selectdb-cloud"
cloudUniqueId = "selectdb-cloud_fe"
metaServiceHttpAddress = "172.30.32.17:5000"
recycleServiceHttpAddress = "172.30.32.17:6000"
feCloudHttpAddress = "172.30.32.17:18030"

// set DORIS_HOME by system properties
// e.g. java -DDORIS_HOME=./
suitePath = "${DORIS_HOME}/regression-test/suites"
dataPath = "${DORIS_HOME}/regression-test/data"
sf1DataPath = "/data"

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
<<<<<<< HEAD
excludeSuites = "test_insert_nested_array,test_clean_label, \
tpch_sf1_q8_nereids,test_alter_user,test_explain_tpch_sf_1_q4,test_explain_tpch_sf_1_q20, \
test_explain_tpch_sf_1_q21,test_explain_tpch_sf_1_q13,test_explain_tpch_sf_1_q22,test_ctas, \
test_ctl,redundant_conjuncts,test_dynamic_partition,test_array_show_create,test_disable_management_cluster"
=======
excludeSuites = "test_broker_load,test_spark_load,test_analyze_stats_p1,test_refresh_mtmv"
>>>>>>> 2.0.0-rc01
// this directories will not be executed
excludeDirectories = "backup_restore,compaction, cold_heat_separation, dynamic_table, javaudf_p0, primary_key,\
tpcds_sf1000_p2,primary_index,github_events_p2,nereids_syntax_p0,schema_change_p0, \
tpch_sf1_p1/tpch_sf1/nereids,schema_change, cloud/smoke, cloud/recycler, cloud/multi_cluster, cloud/compaction, cloud/cache, load_p0/broker_load, \
tpch_sf1_p1/tpch_sf1/explain,cloud/abnormal"

customConf1 = "test_custom_conf_value"

// for test csv with header
enableHdfs=false // set to true if hdfs is ready
hdfsFs = "hdfs://127.0.0.1:9000"
hdfsUser = "doris-test"
hdfsPasswd = ""
brokerName = "broker_name"

// broker load test config
<<<<<<< HEAD
// enableBrokerLoad=true
=======
enableBrokerLoad=true
>>>>>>> 2.0.0-rc01

// cacheDataPath = "/data/regression/"
s3Endpoint = "cos.ap-hongkong.myqcloud.com"
s3BucketName = "doris-build-hk-1308700295"
s3Region = "ap-hongkong"

<<<<<<< HEAD
=======
// hive catalog test config
// To enable jdbc test, you need first start hive container.
// See `docker/thirdparties/start-thirdparties-docker.sh`
enableHiveTest=false
hms_port=7141

cacheDataPath = "/data/regression/"

s3Endpoint = "cos.ap-hongkong.myqcloud.com"
s3BucketName = "doris-build-hk-1308700295"
s3Region = "ap-hongkong"

>>>>>>> 2.0.0-rc01
max_failure_num=50
