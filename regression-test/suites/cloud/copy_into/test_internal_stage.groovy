import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_internal_stage_copy_into") {
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
        //try_sql("DROP TABLE IF EXISTS ${tableNamExternal}")
    }

    def tableName = "customer_internal_stage"
    def tableName2 = "customer_internal_stage2"

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
        def filePath = "${context.config.dataPath}/cloud/copy_into/" + fileName
        def remoteFileName = fileName + "test_internal_stage"
        uploadFile(remoteFileName, filePath)

        createTable()
        def result = sql " copy into ${tableName} from @~('${remoteFileName}') properties ('file.type' = 'csv', 'file.column_separator' = '|', 'copy.async' = 'false'); "
        logger.info("copy result: " + result)
        assertTrue(result.size() == 1)
        assertTrue(result[0].size() == 8)
        assertTrue(result[0][1].equals("FINISHED"), "Finish copy into, state=" + result[0][1] + ", expected state=FINISHED")
        qt_sql " SELECT COUNT(*) FROM ${tableName}; "

        result = sql " copy into ${tableName} from @~('${remoteFileName}') properties ('file.type' = 'csv', 'file.column_separator' = '|', 'copy.async' = 'false'); "
        logger.info("copy result: " + result)
        assertTrue(result.size() == 1)
        assertTrue(result[0].size() == 8)
        assertTrue(result[0][1].equals("CANCELLED"), "Finish copy into, state=" + result[0][1] + ", expected state=CANCELLED")
        qt_sql " SELECT COUNT(*) FROM ${tableName}; "

        if (cloud_delete_loaded_internal_stage_files) {
            // check file is deleted
            waitInternalStageFilesDeleted(remoteFileName)
            // check copy job and file keys are deleted
            uploadFile(remoteFileName, filePath)
            result = sql " copy into ${tableName} from @~('${remoteFileName}') properties ('file.type' = 'csv', 'file.column_separator' = '|', 'copy.async' = 'false'); "
            logger.info("copy result: " + result)
            assertTrue(result.size() == 1)
            assertTrue(result[0].size() == 8)
            assertTrue(result[0][1].equals("FINISHED"), "Finish copy into, state=" + result[0][1] + ", expected state=FINISHED")
            // check file is deleted
            waitInternalStageFilesDeleted(remoteFileName)
        }

        // copy with invalid file
        // line 5: str cast to int, 'C_ACCTBAL' is NULL in ${tableName}, NOT NULL in ${tableName2}
        // line 6: empty str
        // line 7: add a | in the end
        // line 8: add two | in the end
        fileName = "internal_customer_partial_error.csv"
        filePath = "${context.config.dataPath}/cloud/copy_into/" + fileName
        remoteFileName = fileName + "test_internal_stage"
        uploadFile(remoteFileName, filePath)

        def sqls = [
                " copy into ${tableName} from @~('${remoteFileName}') properties ('file.type' = 'csv', 'file.column_separator' = '|', 'copy.async' = 'false'); ",
                " copy into ${tableName} from @~('${remoteFileName}') properties ('file.type' = 'csv', 'file.column_separator' = '|', 'copy.async' = 'false', 'copy.force'='true', 'copy.strict_mode'='true'); ",
                " copy into ${tableName} from @~('${remoteFileName}') properties ('file.type' = 'csv', 'file.column_separator' = '|', 'copy.async' = 'false', 'copy.on_error'='max_filter_ratio_0.3'); ",
                " copy into ${tableName2} from @~('${remoteFileName}') properties ('file.type' = 'csv', 'file.column_separator' = '|', 'copy.async' = 'false', 'copy.force'='true', 'copy.strict_mode'='true'); ",
                " copy into ${tableName2} from ( select \$1, \$2, \$3, \$4, \$5, \$6, \$7, \$8 from @~('${remoteFileName}') ) properties ('file.type' = 'csv', 'file.column_separator' = '|', 'copy.async' = 'false'); "
        ]

        def state = [
                'CANCELLED',
                'CANCELLED',
                'FINISHED',
                'CANCELLED',
                'FINISHED'
        ]

        def rows = [
                [],
                [],
                ['7', '3', '0'],
                [],
                ['10', '0', '0']
        ]

        createTable()
        for (int i = 0; i < sqls.size(); i++) {
            uploadFile(remoteFileName, filePath)
            result = sql "${sqls[i]}"
            logger.info("copy result: " + result)
            assertTrue(result.size() == 1)
            assertTrue(result[0].size() == 8)
            assertTrue(result[0][1].equals(state[i]), "Finish copy into, state=" + result[0][1])
            if (state[i].equals('CANCELLED')) {
                assertTrue(result[0][3].contains('quality not good enough to cancel'), "Finish copy into, msg=" + result[0][3])
            } else if (state[i].equals('FINISHED')) {
                assertTrue(result[0][4].equals(rows[i][0]), "Finish copy into, loaded rows=" + result[0][4])
                assertTrue(result[0][5].equals(rows[i][1]), "Finish copy into, filter rows=" + result[0][5])
                assertTrue(result[0][6].equals(rows[i][2]), "Finish copy into, unselected rows=" + result[0][6])
                qt_sql "select * from ${tableName} order by C_CUSTKEY ASC"
                qt_sql "select * from ${tableName2} order by C_CUSTKEY ASC"
                if (cloud_delete_loaded_internal_stage_files) {
                    waitInternalStageFilesDeleted(remoteFileName)
                }
            }
        }
    } finally {
        //try_sql("DROP TABLE IF EXISTS ${tableName}")
        //try_sql("DROP TABLE IF EXISTS ${tableName2}")
    }

    def endpoint = context.config.feHttpAddress.split(':')
    assertTrue(endpoint.size() == 2)

    // get fe metrics
    def count = 0
    while (true) {
        def upload_total = get_be_metric(endpoint[0], endpoint[1], "doris_fe_http_copy_into_upload_request_total")
        logger.info("fe metrics, doris_fe_http_copy_into_upload_request_total: " + upload_total)
        if (upload_total > 0) {
            break;
        }
        if (count >= 60) {
            assertTrue(upload_total > 0)
            break;
        }
        count++
        sleep(1000)
    }
}
