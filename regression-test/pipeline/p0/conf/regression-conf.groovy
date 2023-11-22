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

jdbcUrl = "jdbc:mysql://172.30.32.17:9030/?useLocalSessionState=true&allowLoadLocalInfile=true"
targetJdbcUrl = "jdbc:mysql://172.30.32.17:9030/?useLocalSessionState=true&allowLoadLocalInfile=true"
jdbcUser = "root"
jdbcPassword = ""

feSourceThriftAddress = "127.0.0.1:9020"
feTargetThriftAddress = "127.0.0.1:9020"
feSyncerUser = "root"
feSyncerPassword = ""

feHttpAddress = "172.30.32.17:8030"
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
excludeSuites = "test_alter_database_property, test_alter_table_property, test_bitmap_filter, test_es_query_nereids, \
    test_partial_update_schema_change, test_show_create_catalog, explode, q67_ignore_temporarily, test_analyze_stats_p1, \
    test_bitmap_filter, test_clean_label, test_cloud_dynamic_partition, test_concat_extreme_input, test_create_cloud_table, \
    test_date_function, test_dup_table_auto_inc_col, test_external_es, test_nereids_row_policy, test_overdue, test_point_query, \
    test_profile, test_refresh_mtmv, test_sql_depth, test_transactional_hive, test_unicode_name, push_filter_through_ptopn, \
    test_bitmap_int, test_jsonb_load_and_function, test_dynamic_table, test_full_compaction, test_full_compaction_by_table_id"
// this directories will not be executed
excludeDirectories = "backup_restore, ccr_syncer_p0, ccr_syncer_p1, cloud/abnormal, cloud/cache, \
    cloud/compaction, cloud/limit_optimize, cloud/multi_cluster, cloud/recycler, cloud/smoke, cloud/stage_tvf, \
    cold_heat_separation, connector_p0, export_p0, javaudf_p0, mtmv_p0, mv_p0/ssb, nereids_p0/javaudf, nereids_p0/outfile, \
    nereids_tpcds_shape_sf100_p0, nereids_tpch_shape_sf1000_p0, nereids_tpch_shape_sf500_p0, tpcds_sf1000_p2, usercases/TB2, \
    usercases/dbgen, usercases/MYZS1"

customConf1 = "test_custom_conf_value"

// for test csv with header
enableHdfs=false // set to true if hdfs is ready
hdfsFs = "hdfs://127.0.0.1:9000"
hdfsUser = "doris-test"
hdfsPasswd = ""
brokerName = "broker_name"

// broker load test config
enableBrokerLoad=true

// cacheDataPath = "/data/regression/"
s3Endpoint = "cos.ap-hongkong.myqcloud.com"
s3BucketName = "doris-build-hk-1308700295"
s3Region = "ap-hongkong"

// hive catalog test config
// To enable jdbc test, you need first start hive container.
// See `docker/thirdparties/start-thirdparties-docker.sh`
enableHiveTest=false
hms_port=7141

enableEsTest=false
es_6_port=19200
es_7_port=29200
es_8_port=39200

cacheDataPath = "/data/regression/"

s3Endpoint = "cos.ap-hongkong.myqcloud.com"
s3BucketName = "doris-build-hk-1308700295"
s3Region = "ap-hongkong"

max_failure_num=50

externalEnvIp="127.0.0.1"
