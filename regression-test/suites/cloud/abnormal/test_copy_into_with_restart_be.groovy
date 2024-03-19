suite("test_copy_into_with_restart_be") {
    def clusterMap = loadClusterMap(getConf("clusterFile"))
    // create table
    def tableName = 'test_copy_into_with_restart_be'
    def externalStageName = "test_copy_into_with_restart_be"
    def uniqueID = Math.abs(UUID.randomUUID().hashCode()).toString()

    logger.debug("clusterMap:${clusterMap}")
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

    sql """drop stage if exists ${externalStageName}"""

    // create external stage
    sql """
        create stage if not exists ${externalStageName} properties(
        'endpoint' = '${getS3Endpoint()}' ,
        'region' = '${getS3Region()}' ,
        'bucket' = '${getS3BucketName()}' ,
        'prefix' = 'regression' ,
        'ak' = '${getS3AK()}' ,
        'sk' = '${getS3SK()}' ,
        'provider' = '${getProvider()}',
        'access_type' = 'aksk',
        'default.file.column_separator' = "|"
        );
    """

    
    
    def result = sql """
        copy into ${tableName} from @${externalStageName}('tpch/sf100/customer.tbl')
        properties (
            'file.type' = 'csv',
            'file.column_separator' = '|',
            'copy.async' = 'true');
        """

    String loadLabel = result[0][0]
    loadLabel = loadLabel.replace('-', '_')
    logger.info("copy into result: {}, loadLabel", result, loadLabel);

    checkCopyIntoLoading(loadLabel)

    restartProcess(clusterMap["be"]["cluster"][0]["node"][0]["ip"], "be", clusterMap["be"]["cluster"][0]["node"][0]["install_path"])
    checkCopyIntoFinished(loadLabel)

    rowCount = sql "select count(*) from ${tableName}"
    logger.info("rowCount:{}", rowCount)
    assertEquals(rowCount[0][0], 15000000)
    sql """ DROP TABLE IF EXISTS ${tableName} FORCE"""
}
