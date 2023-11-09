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

feSourceThriftAddress = "127.0.0.1:9020"
feTargetThriftAddress = "127.0.0.1:9020"
syncerAddress = "127.0.0.1:9190"
feSyncerUser = "root"
feSyncerPassword = ""

feHttpAddress = "127.0.0.1:8030"
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
suitePath = "${DORIS_HOME}/regression-test/suites"
dataPath = "${DORIS_HOME}/regression-test/data"
pluginPath = "${DORIS_HOME}/regression-test/plugins"
realDataPath = "${DORIS_HOME}/regression-test/realdata"

// docker image
image = ""
dockerEndDeleteFiles = false
dorisComposePath = "${DORIS_HOME}/docker/runtime/doris-compose/doris-compose.py"
// do run docker test because pipeline not support build image now
excludeDockerTest = true

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

// broker load test config
enableBrokerLoad=true
ak=""
sk=""

// jdbc connector test config
// To enable jdbc test, you need first start mysql/pg container.
// See `docker/thirdparties/start-thirdparties-docker.sh`
enableJdbcTest=false
mysql_57_port=3316
pg_14_port=5442
oracle_11_port=1521
sqlserver_2022_port=1433
clickhouse_22_port=8123
doris_port=9030
mariadb_10_port=3326

// hive catalog test config
// To enable hive/paimon test, you need first start hive container.
// See `docker/thirdparties/start-thirdparties-docker.sh`
enableHiveTest=false
enablePaimonTest=false
hms_port=9183
hdfs_port=8120
hiveServerPort=10000

// kafka test config
// to enable kafka test, you need firstly to start kafka container
// See `docker/thirdparties/start-thirdparties-docker.sh`
enableKafkaTest=false
kafka_port=19193

// elasticsearch catalog test config
// See `docker/thirdparties/start-thirdparties-docker.sh`
enableEsTest=false
es_6_port=19200
es_7_port=29200
es_8_port=39200

// stage iam test
stageIamEndpoint = ""
stageIamRegion = ""
stageIamBucket = ""

// used for oss and obs, which does not support external id. Create role (or agency) and use for all instances.
stageIamRole = ""
stageIamArn = ""

//hive  catalog test config for bigdata
enableExternalHiveTest = false
extHiveHmsHost = "***.**.**.**"
extHiveHmsPort = 7004
extHdfsPort = 4007
extHiveServerPort= 7001
extHiveHmsUser = "****"
extHiveHmsPassword= "***********"

//paimon catalog test config for bigdata
enableExternalPaimonTest = false

//mysql jdbc connector test config for bigdata
enableExternalMysqlTest = false
extMysqlHost = "***.**.**.**"
extMysqlPort = 3306
extMysqlUser = "****"
extMysqlPassword = "***********"

//postgresql jdbc connector test config for bigdata
enableExternalPgTest = false
extPgHost = "***.**.**.*"
extPgPort = 5432
extPgUser = "****"
extPgPassword = "***********"

// elasticsearch external test config for bigdata
enableExternalEsTest = false
extEsHost = "***********"
extEsPort = 9200
extEsUser = "*******"
extEsPassword = "***********"

<<<<<<< HEAD
// used for cos and s3, which support external id.
// this is policy arn for s3
stageIamPolicy = "smoke_test_policy"
stageIamAk = ""
stageIamSk = ""

// used for cos, which sdk does not return arn, so construct arn by user id.
stageIamUserId = ""
=======
enableObjStorageTest=false
enableMaxComputeTest=false
aliYunAk="***********"
dlfUid="***********"
aliYunSk="***********"
hwYunAk="***********"
hwYunSk="***********"

s3Endpoint = "cos.ap-hongkong.myqcloud.com"
s3BucketName = "doris-build-hk-1308700295"
s3Region = "ap-hongkong"

// iceberg rest catalog config
iceberg_rest_uri_port=18181

>>>>>>> 2.0.3-rc01
// If the failure suite num exceeds this config
// all following suite will be skipped to fast quit the run.
// <=0 means no limit.
max_failure_num=0

// used for exporting test
s3ExportBucketName = ""

externalEnvIp="127.0.0.1"
