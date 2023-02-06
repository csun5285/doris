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

        def result = sql " copy into ${tableName} from @${externalStageName}('${prefix}/customer.csv.gz') properties ('file.type' = 'csv', 'file.compression' = 'gz', 'copy.async' = 'false'); "
        logger.info("copy result: " + result)
        assertTrue(result.size() == 1)
        assertTrue(result[0].size() == 8)
        assertTrue(result[0][1].equals("FINISHED"), "Finish copy into, state=" + result[0][1] + ", expected state=FINISHED")
        qt_sql " SELECT COUNT(*) FROM ${tableName}; "

        result = sql " copy into ${tableName} from @${externalStageName}('${prefix}/customer.csv.gz') properties ('copy.async' = 'false'); "
        logger.info("copy result: " + result)
        assertTrue(result.size() == 1)
        assertTrue(result[0].size() == 8)
        assertTrue(result[0][1].equals("CANCELLED"), "Finish copy into, state=" + result[0][1] + ", expected state=CANCELLED")
        qt_sql " SELECT COUNT(*) FROM ${tableName}; "

        // copy into with force
        result = sql " copy into ${tableName} from @${externalStageName}('${prefix}/customer.csv.gz') properties ('file.type' = 'csv', 'file.compression' = 'gz', 'copy.async' = 'false', 'copy.force'='true'); "
        logger.info("copy result: " + result)
        assertTrue(result.size() == 1)
        assertTrue(result[0].size() == 8)
        assertTrue(result[0][1].equals("FINISHED"))
        qt_sql " SELECT COUNT(*) FROM ${tableName}; "

        // show copy
        result = sql "show copy where id='${result[0][0]}' and tablename ='${tableName}' and files like '${prefix}/customer.csv.gz' and state='finished'"
        logger.info("show copy result: " + result)
        assertTrue(result.size == 1)

        // copy with invalid data
        result = sql " copy into ${tableName} from @${externalStageName}('${prefix}/supplier.csv.gz') properties ('file.type' = 'csv', 'file.compression' = 'gz', 'copy.async' = 'false'); "
        logger.info("copy result: " + result)
        assertTrue(result.size() == 1)
        assertTrue(result[0].size() == 8)
        assertTrue(result[0][1].equals('CANCELLED'))
        assertTrue(result[0][3].contains('quality not good enough to cancel'))
    } finally {
        try_sql("DROP TABLE IF EXISTS ${tableName}")
    }
}
