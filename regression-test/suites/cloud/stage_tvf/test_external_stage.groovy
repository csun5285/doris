suite("test_external_stage") {
    def tableName = "customer_external_stage"
    def externalStageName = "regression_test_tpch"
    def prefix = "tpch/sf1"

    try {
        sql """ DROP TABLE IF EXISTS ${tableName}; """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
            C_CUSTKEY     INTEGER NOT NULL,
            C_NAME        VARCHAR(25) NOT NULL,
            C_ADDRESS     VARCHAR(40) NOT NULL,
            C_NATIONKEY   INTEGER NOT NULL,
            C_PHONE       CHAR(15) NOT NULL,
            C_ACCTBAL     DECIMAL(15,2)   NOT NULL,
            C_MKTSEGMENT  CHAR(10) NOT NULL,
            C_COMMENT     VARCHAR(117) NOT NULL
            )
            DUPLICATE KEY(C_CUSTKEY)
            DISTRIBUTED BY HASH(C_CUSTKEY) BUCKETS 1
        """

        sql """
            create stage if not exists ${externalStageName} 
            properties ('endpoint' = '${getS3Endpoint()}' ,
            'region' = '${getS3Region()}' ,
            'bucket' = '${getS3BucketName()}' ,
            'prefix' = 'regression' ,
            'ak' = '${getS3AK()}' ,
            'sk' = '${getS3SK()}' ,
            'provider' = '${getProvider()}', 
            'access_type' = 'aksk',
            'default.file.column_separator' = "|");
        """

        def result = sql " insert into ${tableName} select * from stage('stage_name'='${externalStageName}', 'file_pattern'='${prefix}/customer.csv.gz', 'format' = 'csv', 'compress' = 'gz'); "
        logger.info("insert result: " + result)
        qt_sql " SELECT COUNT(*) FROM ${tableName}; "

        try {
            result = sql " insert into ${tableName} select * from stage('stage_name'='${externalStageName}', 'file_pattern'='${prefix}/customer.csv.gz'); "
            assertTrue(false, "should throw exception")
        } catch (Exception e) {
            logger.info("exception: " + e.getMessage())
        }
        qt_sql " SELECT COUNT(*) FROM ${tableName}; "

        // insert into with force
        label = "test_external_stage_" + System.currentTimeMillis()
        result = sql " insert into ${tableName} with label ${label} select * from stage('stage_name'='${externalStageName}', 'file_pattern'='${prefix}/customer.csv.gz', 'format' = 'csv', 'compress' = 'gz'); "
        logger.info("insert result: " + result)
        qt_sql " SELECT COUNT(*) FROM ${tableName}; "

        // show load
        result = sql "show load where label='${label}' and state = 'finished'"
        logger.info("show load result: " + result)
        assertTrue(result.size == 1)

        // insert with invalid data
        try {
            result = sql " insert into ${tableName} select * from stage('stage_name'='${externalStageName}', 'file_pattern'='${prefix}/supplier.csv.gz', 'format' = 'csv', 'compress' = 'gz'); "
            assertTrue(false, "should throw exception")
        } catch (Exception e) {
            logger.info("exception: " + e.getMessage())
        }
    } finally {
        try_sql("DROP TABLE IF EXISTS ${tableName}")
    }
}
