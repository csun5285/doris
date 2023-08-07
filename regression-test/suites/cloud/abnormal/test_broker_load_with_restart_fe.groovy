suite("test_broker_load_with_restart_fe") {
    def clusterMap = loadClusterMap(getConf("clusterFile"))

    // create table
    def tableName = 'test_broker_load_with_restart_fe'
    def uniqueID = Math.abs(UUID.randomUUID().hashCode()).toString()
    def loadLabel = tableName + "_" + uniqueID

    /* clusterMap demo
    {
        "user": "ubuntu",
        "password": "Cfplhys2022@",
        "cluster_mode": "cloud",
        "java_home": "/home/ubuntu/jdk1.8.0_131",
        "fe": {
            "http_port": "18047",
            "rpc_port": "9037",
            "query_port": "9047",
            "edit_log_port": "9027",
            "cloud_http_port": "",
            "cloud_unique_id": "selectdb_cloud_dev_specific_abnormal_asan_fe",
            "node": [
                {
                    "ip": "172.21.16.8",
                    "is_master": "true",
                    "install_path": "/mnt/disk1/selectdb-cloud-dev-specific-abnormal-asan/fe"
                }
            ]
        },
        "be": {
            "cluster": [
                {
                    "be_port": "9077",
                    "brpc_port": "8077",
                    "webserver_port": "8057",
                    "heartbeat_service_port": "9067",
                    "cloud_unique_id": "selectdb_cloud_dev_specific_abnormalasan_id_cluster0",
                    "node": [
                        {
                            "ip": "172.21.16.8",
                            "install_path": "/mnt/disk1/selectdb-cloud-dev-specific-abnormal-asan/cluster0/be"
                        },
                        {
                            "ip": "172.21.16.40",
                            "install_path": "/mnt/disk1/selectdb-cloud-dev-specific-abnormal-asan/cluster0/be"
                        },
                        {
                            "ip": "172.21.16.21",
                            "install_path": "/mnt/disk1/selectdb-cloud-dev-specific-abnormal-asan/cluster0/be"
                        }
                    ]
                }
            ]
        },
        "meta_service": {
            "brpc_listen_port": "5017",
            "node": [
                {
                    "ip": "172.21.16.8",
                    "install_path": "/mnt/disk1/selectdb-cloud-dev-specific-abnormal-asan/meta-service"
                },
                {
                    "ip": "172.21.16.40",
                    "install_path": "/mnt/disk1/selectdb-cloud-dev-specific-abnormal-asan/meta-service"
                },
                {
                    "ip": "172.21.16.21",
                    "install_path": "/mnt/disk1/selectdb-cloud-dev-specific-abnormal-asan/meta-service"
                }
            ]
        },
        "recycler": {
            "brpc_listen_port": "6017",
            "node": [
                {
                    "ip": "172.21.16.8",
                    "install_path": "/mnt/disk1/selectdb-cloud-dev-specific-abnormal-asan/recycler"
                },
                {
                    "ip": "172.21.16.40",
                    "install_path": "/mnt/disk1/selectdb-cloud-dev-specific-abnormal-asan/recycler"
                },
                {
                    "ip": "172.21.16.21",
                    "install_path": "/mnt/disk1/selectdb-cloud-dev-specific-abnormal-asan/recycler"
                }
            ]
        },
        "object_storage": {
            "ak": "AKIDAE2aqpY0B7oFPIvHMBj01lFSO3RYOxFH",
            "sk": "nJYWDepkQqzrWv3uWsxlJ0ScV7SXLs88",
            "provider": "COS",
            "region": "ap-beijing",
            "endpoint": "cos.ap-beijing.myqcloud.com",
            "bucket": "doris-build-1308700295",
            "prefix": "selectdb-cloud-dev-specific-abnormal-asan-pipeline-0330"
        },
        "monitor": {
            "inconsistent_webhook": "",
            "default_webhook": ""
        }
    }
    */

    logger.debug("clusterMap:${clusterMap}");
    checkProcessAlive(clusterMap["fe"]["node"][0]["ip"], "fe", clusterMap["fe"]["node"][0]["install_path"])
    checkProcessAlive(clusterMap["be"]["cluster"][0]["node"][0]["ip"], "be", clusterMap["be"]["cluster"][0]["node"][0]["install_path"])
    checkProcessAlive(clusterMap["meta_service"]["node"][0]["ip"], "ms", clusterMap["meta_service"]["node"][0]["install_path"])

    sql """ DROP TABLE IF EXISTS ${tableName} FORCE"""
    sql """
        CREATE TABLE IF NOT EXISTS `${tableName}` (
            C_CUSTKEY     INTEGER NOT NULL,
            C_NAME        VARCHAR(25) NOT NULL,
            C_ADDRESS     VARCHAR(40) NOT NULL,
            C_NATIONKEY   INTEGER NOT NULL,
            C_PHONE       CHAR(15) NOT NULL,
            C_ACCTBAL     DECIMAL(15,2)   NOT NULL,
            C_MKTSEGMENT  CHAR(10) NOT NULL,
            C_COMMENT     VARCHAR(117) NOT NULL
        )
        DUPLICATE KEY(C_CUSTKEY, C_NAME)
        DISTRIBUTED BY HASH(C_CUSTKEY) BUCKETS 32
        ;
    """

    sql """
        LOAD LABEL ${loadLabel}
        (
            DATA INFILE('s3://${s3BucketName}/regression/tpch/sf100/customer.tbl')
            INTO TABLE ${tableName}
            COLUMNS TERMINATED BY "|"
            (c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment, temp)
        )
        WITH S3
        (
            'AWS_REGION' = '${getS3Region()}',
            'AWS_ENDPOINT' = '${getS3Endpoint()}',
            'AWS_ACCESS_KEY' = '${getS3AK()}',
            'AWS_SECRET_KEY' = '${getS3SK()}'
        )
        PROPERTIES
        (
            'exec_mem_limit' = '8589934592',
            'load_parallelism' = '1',
            'timeout' = '3600'
        )
    """

    checkBrokerLoadLoading(loadLabel)

    restartProcess(clusterMap["fe"]["node"][0]["ip"], "fe", clusterMap["fe"]["node"][0]["install_path"])
    resetConnection()
    checkBrokerLoadFinished(loadLabel)

    rowCount = sql "select count(*) from ${tableName}"
    logger.info("rowCount:{}", rowCount)
    assertEquals(rowCount[0][0], 15000000)
    sql """ DROP TABLE IF EXISTS ${tableName} FORCE"""
}
