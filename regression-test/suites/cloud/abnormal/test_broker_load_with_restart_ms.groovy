suite("test_broker_load_with_restart_ms") {
    checkClusterDir();
    // create table
    def tableName = 'test_broker_load_with_restart_ms'
    def uniqueID = Math.abs(UUID.randomUUID().hashCode()).toString()
    def loadLabel = tableName + "_" + uniqueID
    def feDir = "${context.config.clusterDir}/fe"
    def beDir = "${context.config.clusterDir}/cluster0/be"
    def msDir = "${context.config.clusterDir}/meta-service"

    String nodeIp = context.config.feHttpAddress.split(':')[0].trim()

    def clusterMap = [
        fe : [[ ip : nodeIp, path : feDir]],
        be : [[ ip : nodeIp, path: beDir]],
        ms : [[ ip : nodeIp, path: msDir]]
    ]

    logger.info("clusterMap:${clusterMap}");
    checkProcessAlive(clusterMap["fe"][0]["ip"], "fe", clusterMap["fe"][0]["path"]);
    checkProcessAlive(clusterMap["be"][0]["ip"], "be", clusterMap["be"][0]["path"]);
    checkProcessAlive(clusterMap["ms"][0]["ip"], "ms", clusterMap["ms"][0]["path"]);

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
        PROPERTIES (
            "replication_num" = "1"
        );
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
    // restart fe
    restartProcess(clusterMap["ms"][0]["ip"], "ms", clusterMap["ms"][0]["path"])
    resetConnection()
    checkBrokerLoadFinished(loadLabel)

    rowCount = sql "select count(*) from ${tableName}"
    logger.info("rowCount:{}", rowCount)
    assertEquals(rowCount[0][0], 15000000)
    sql """ DROP TABLE IF EXISTS ${tableName} FORCE"""
}
