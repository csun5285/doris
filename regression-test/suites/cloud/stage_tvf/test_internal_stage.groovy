import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_internal_stage") {
    // Internal and external stage cross use
    def tableNamExternal = "customer_internal_stage"
    def externalStageName = "internal_external_stage_cross_use"
    def token = "greedisgood9999"
    def instanceId = context.config.instanceId
    def cloudUniqueId = context.config.cloudUniqueId
    try {
        sql """ DROP TABLE IF EXISTS ${tableNamExternal}; """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableNamExternal} (
            C_CUSTKEY     INTEGER NOT NULL,
            C_NAME        VARCHAR(25) NOT NULL,
            C_ADDRESS     VARCHAR(40) NOT NULL,
            C_NATIONKEY   INTEGER NOT NULL,
            C_PHONE       CHAR(15) NOT NULL,
            C_ACCTBAL     DECIMAL(15,2)   NOT NULL,
            C_MKTSEGMENT  CHAR(10) NOT NULL,
            C_COMMENT     VARCHAR(117) NOT NULL
            )
            UNIQUE KEY(C_CUSTKEY)
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
    } finally {
        try_sql("DROP TABLE IF EXISTS ${tableNamExternal}")
    }

    def tableName = "customer_internal_stage"
    def tableName2 = "customer_internal_stage2"
    def sizeLimitTable = "test_size_limit"

    def uploadFile = { remoteFilePath, localFilePath ->
        StringBuilder strBuilder = new StringBuilder()
        strBuilder.append("""curl -u """ + context.config.feCloudHttpUser + ":" + context.config.feCloudHttpPassword)
        strBuilder.append(""" -H fileName:""" + remoteFilePath)
        strBuilder.append(""" -T """ + localFilePath)
        strBuilder.append(""" -L http://""" + context.config.feCloudHttpAddress + """/copy/upload""")

        String command = strBuilder.toString()
        logger.info("upload command=" + command)
        def process = command.toString().execute()
        def code = process.waitFor()
        def err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
        def out = process.getText()
        logger.info("Request FE Config: code=" + code + ", out=" + out + ", err=" + err)
        assertEquals(code, 0)
    }

    def createTable = {
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
        sql """ DROP TABLE IF EXISTS ${tableName2}; """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName2} (
            C_CUSTKEY     INTEGER NOT NULL,
            C_NAME        VARCHAR(25) NOT NULL,
            C_ADDRESS     VARCHAR(40) NOT NULL,
            C_NATIONKEY   INTEGER NOT NULL,
            C_PHONE       CHAR(15) NOT NULL,
            C_ACCTBAL     DECIMAL(15,2) NULL,
            C_MKTSEGMENT  CHAR(10) NOT NULL,
            C_COMMENT     VARCHAR(117) NOT NULL
            )
            DUPLICATE KEY(C_CUSTKEY)
            DISTRIBUTED BY HASH(C_CUSTKEY) BUCKETS 1
        """
    }

    def waitInternalStageFilesDeleted = { fileName ->
        def retry = 10
        do {
            Thread.sleep(2000)
            if (checkRecycleInternalStage(token, instanceId, cloudUniqueId, fileName)) {
                Thread.sleep(2000) // wait for copy job kv is deleted
                return
            }
        } while (retry--)
        assertTrue(false, "Internal stage file is not deleted")
    }

    def getCloudConf = {
        result = sql """ ADMIN SHOW FRONTEND CONFIG """
        for (def r : result) {
            assertTrue(r.size() > 2)
            if (r[0] == "cloud_delete_loaded_internal_stage_files") {
                return (r[1] == "true")
            }
        }
        return false
    }

    boolean cloud_delete_loaded_internal_stage_files = getCloudConf()
    logger.info("cloud_delete_loaded_internal_stage_files=" + cloud_delete_loaded_internal_stage_files)

    try {
        def fileName = "internal_customer.csv"
        def remoteFileName = fileName + "stage_tvf_test_internal_stage"
        def filePath = "${context.config.dataPath}/cloud/copy_into/" + fileName
        uploadFile(remoteFileName, filePath)

        createTable()
        def result = sql " insert into ${tableName} select * from stage('stage_name'='~', 'file_pattern'='${remoteFileName}', 'format' = 'csv', 'column_separator' = '|'); "
        logger.info("insert result: " + result)
        qt_sql " SELECT COUNT(*) FROM ${tableName}; "

        if (cloud_delete_loaded_internal_stage_files) {
            // check file is deleted
            waitInternalStageFilesDeleted(remoteFileName)
            // check copy job and file keys are deleted
            uploadFile(remoteFileName, filePath)
            result = sql " insert into ${tableName} select * from stage('stage_name'='~', 'file_pattern'='${remoteFileName}', 'format' = 'csv', 'column_separator' = '|'); "
            logger.info("insert result: " + result)
            // check file is deleted
            waitInternalStageFilesDeleted(remoteFileName)
        }

        // insert with invalid file
        // line 5: str cast to int, 'C_ACCTBAL' is NULL in ${tableName}, NOT NULL in ${tableName2}
        // line 6: empty str
        // line 7: add a | in the end
        // line 8: add two | in the end
        fileName = "internal_customer_partial_error.csv"
        filePath = "${context.config.dataPath}/cloud/copy_into/" + fileName
        remoteFileName = fileName + "stage_tvf_test_internal_stage"
        uploadFile(remoteFileName, filePath)

        def sqls = [
                " insert into ${tableName} select * from stage('stage_name'='~', 'file_pattern'='${remoteFileName}', 'format' = 'csv', 'column_separator' = '|'); ",
                // force, strict_mode
                " insert /*+ set_var('strict_mode'='true') */ into ${tableName} with label test_internal_stage_" + System.currentTimeMillis() + " select * from stage('stage_name'='~', 'file_pattern'='${remoteFileName}', 'format' = 'csv', 'column_separator' = '|'); ",
                // 'max_filter_ratio'='0.1'
                " insert /*+ set_var('max_filter_ratio'='0.1') */ into ${tableName} select * from stage('stage_name'='~', 'file_pattern'='${remoteFileName}', 'format' = 'csv', 'column_separator' = '|'); ",
                // force, strict_mode
                // TODO expected fail
                // " insert /*+ set_var('strict_mode'='true') */ into ${tableName2} with label test_internal_stage_" + (System.currentTimeMillis() + 10) + " select * from stage('stage_name'='~', 'file_pattern'='${fileName}', 'format' = 'csv', 'column_separator' = '|'); ",
                " insert into ${tableName2} select * from stage('stage_name'='~', 'file_pattern'='${remoteFileName}', 'format' = 'csv', 'column_separator' = '|'); "
        ]

        def state = [
                'CANCELLED',
                'CANCELLED',
                'FINISHED',
                // 'CANCELLED',
                'FINISHED'
        ]

        def rows = [
                [],
                [],
                [9, 1, 0],
                // [],
                [10, 0, 0]
        ]

        def session_sqls = [
                [],
                [" set strict_mode = true "],
                [" set strict_mode = false ", " set max_filter_ratio = 0.1 "],
                // [" set strict_mode = true ", " set max_filter_ratio = 0 "],
                [" set strict_mode = false ", " set max_filter_ratio = 0 "]
        ]

        createTable()
        for (int i = 0; i < sqls.size(); i++) {
            try {
                /*for (def session_sql : session_sqls[i]) {
                    sql """ ${session_sql} """
                }*/
                result = sql "${sqls[i]}"
                logger.info("insert result: " + result + ", i=" + i)
                assertTrue(state[i].equals("FINISHED"), "real state=" + state[i] + ", expected state=FINISHED")
                assertTrue(result.size() == 1, "size=" + result.size() )
                assertTrue(result[0].size() == 1, "result0 size=" + result[0].size())
                assertTrue(rows[i][0].equals(result[0][0]))
                if (cloud_delete_loaded_internal_stage_files) {
                    waitInternalStageFilesDeleted(remoteFileName)
                    uploadFile(remoteFileName, filePath)
                }
            } catch (Exception e) {
                assertTrue(state[i].equals("CANCELLED"), "i=" + i + ", real exception: " + e.getMessage())
                logger.info("i=" + i + ", real exception: " + e.getMessage())
            }
        }

        // test size limit
        try_sql("DROP TABLE IF EXISTS ${sizeLimitTable}")
        sql """
        CREATE TABLE ${sizeLimitTable} (
        id INT,
        name varchar(50),
        score INT
        )
        DUPLICATE KEY(id, name)
        DISTRIBUTED BY HASH(id) BUCKETS 1;
        """
        filePath = "${context.config.dataPath}/cloud/stage_tvf/size_limit.csv"
        def prefix = "test_internal_stage_size_limit_" + System.currentTimeMillis() + "/"
        for (int i = 0; i < 5; i++) {
            uploadFile(prefix + i + ".csv", filePath)
        }
        rows = [2, 2, 1]
        for (int i = 0; i < 3; i++) {
            result = sql " insert into ${sizeLimitTable} select * from stage('stage_name'='~', 'file_pattern'='${prefix}*', 'format'='csv', 'size_limit'='20') "
            logger.info("insert result: " + result)
            assertTrue(result.size() == 1, "size=" + result.size() )
            assertTrue(result[0].size() == 1, "result0 size=" + result[0].size())
            assertTrue(rows[i].equals(result[0][0]))
            if (cloud_delete_loaded_internal_stage_files) {
                if (i == 0) {
                    waitInternalStageFilesDeleted(prefix + "0.csv")
                    waitInternalStageFilesDeleted(prefix + "1.csv")
                } else if (i == 1) {
                    waitInternalStageFilesDeleted(prefix + "2.csv")
                    waitInternalStageFilesDeleted(prefix + "3.csv")
                } else {
                    waitInternalStageFilesDeleted(prefix + "4.csv")
                }
            }
        }
    } finally {
        try_sql("DROP TABLE IF EXISTS ${tableName}")
        try_sql("DROP TABLE IF EXISTS ${tableName2}")
        try_sql("DROP TABLE IF EXISTS ${sizeLimitTable}")
    }
}