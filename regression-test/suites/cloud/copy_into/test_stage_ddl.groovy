import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_stage_ddl") {
    def stageName = "test_stage_ddl_" + new Random().nextInt(Integer.MAX_VALUE)
    def user = "test_stage_ddl"
    def filePath = "${context.config.dataPath}/cloud/copy_into/internal_customer.csv"
    def sk = getS3SK()

    def stageExists = { stage ->
        def result = sql " SHOW STAGES; "
        for (int i = 0; i < result.size(); i++) {
            if (result[i][0] == stageName) {
                return true
            }
        }
        return false
    }

    def createStage = { ifNotExists, success, errorMsg ->
        try {
            def result = sql "create stage " + (ifNotExists ? "if not exists " : "") + """ ${stageName} 
            properties ('endpoint' = '${getS3Endpoint()}' ,
            'region' = '${getS3Region()}' ,
            'bucket' = '${getS3BucketName()}' ,
            'prefix' = 'regression' ,
            'ak' = '${getS3AK()}' ,
            'sk' = '${sk}' ,
            'access_type' = 'aksk',
            'provider' = '${getProvider()}');
            """
            assertTrue(success, "Expected create stage success")
            assertTrue(stageExists(stageName), "Expect stage exists")
        } catch (Exception e) {
            if (success || errorMsg == null || errorMsg.isEmpty()) {
                logger.error("create stage exception, " + e.getMessage())
            }
            assertFalse(success, "Expected create stage fail")
            assertTrue(e.getMessage().contains(errorMsg), "Expected error=" + errorMsg + ", real error=" + e.getMessage())
        }
    }

    def dropStage = { ifExists, success, errorMsg ->
        try {
            sql " DROP STAGE " + (ifExists ? "IF EXISTS " : "") + "${stageName}; "
            assertFalse(stageExists(stageName))
            assertTrue(success)
        } catch(Exception e) {
            if (success) {
                logger.error("drop stage exception, " + e.getMessage())
            }
            assertFalse(success)
            assertTrue(e.getMessage().containsIgnoreCase(errorMsg))
        }
    }

    def uploadFile = { userName, password, remoteFilePath, localFilePath ->
        StringBuilder strBuilder = new StringBuilder()
        strBuilder.append("""curl -u """ + userName + ":" + password)
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

    try {
        // drop stage
        dropStage(true, true, null)

        // create stage -> success
        createStage(false, true, null)
        // create stage without "IF NOT EXISTS" -> fail
        createStage(false, false, "stage already exist")
        // create stage with "IF NOT EXISTS" -> success
        createStage(true, true, null)

        // drop stage -> success
        dropStage(false, true, null)
        // drop stage with "IF EXISTS" -> success
        dropStage(true, true, null)
        // drop stage without "IF EXISTS" -> fail
        dropStage(false, false, "Stage does not exists")

        // create stage with invalid AKSK, error msg is 'AccessForbidden'(OSS) or '403'(other Object Storage)
        sk = "test"
        createStage(true, false, "")
        sk = getS3SK()

        /**------ drop internal stage ------**/
        // create user
        sql """create user ${user} identified by '12345'"""
        // upload file
        def remote = user
        uploadFile(user, "12345", remote, filePath)
        // drop user
        sql """DROP USER ${user}"""
        // create user again
        sql """create user ${user} identified by '12345'"""
        // create table and grant
        sql """
        CREATE TABLE IF NOT EXISTS regression_test.test (
        id INT,
        name varchar(50),
        score INT)
        DUPLICATE KEY(id, name)
        DISTRIBUTED BY HASH(id) BUCKETS 1;
        """
        sql """
        GRANT LOAD_PRIV ON *.*.* TO '${user}'@'%';
        """
        // copy into
        def result = connect(user=user, password='12345', url=context.config.jdbcUrl) {
            def result1 = sql """COPY INTO regression_test.test FROM @~('${remote}') properties('copy.async'='false')"""
            assertTrue(result1.size() == 1)
            assertTrue(result1[0].size() == 8)
            assertTrue(result1[0][1].equals("CANCELLED"), "Finish copy into, state=" + result1[0][1] + ", expected state=CANCELLED")
            assertTrue(result1[0][3].contains("matched 0 files"), "Finish copy into, msg=" + result1[0][3])
        }
    } finally {
        try_sql("DROP TABLE test")
        try_sql("DROP STAGE IF EXISTS ${stageName}")
        try_sql("DROP USER ${user}")
    }
}
