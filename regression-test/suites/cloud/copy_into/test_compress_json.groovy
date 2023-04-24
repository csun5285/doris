import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_compress_json") {
    def tableName = "test_compress_json"
    def localFileDir = "${context.config.dataPath}/cloud/copy_into/"

    def upload_file = {localPath, remotePath ->
        StringBuilder strBuilder = new StringBuilder()
        strBuilder.append("""curl -u """ + context.config.feCloudHttpUser + ":" + context.config.feCloudHttpPassword)
        strBuilder.append(""" -H fileName:""" + remotePath)
        strBuilder.append(""" -T """ + localPath)
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

    def copy_into = {remotePath, compressionType ->
        def result = sql " copy into ${tableName} from @~('${remotePath}') properties ('file.type' = 'json', 'file.compression' = '${compressionType}', 'copy.async' = 'false'); "
        logger.info("copy result: " + result)
        assertTrue(result.size() == 1)
        assertTrue(result[0].size() == 8)
        assertTrue(result[0][1].equals("FINISHED"), "Finish copy into, state=" + result[0][1] + ", expected state=FINISHED")
        qt_sql " SELECT COUNT(*) FROM ${tableName}; "
        qt_sql " SELECT * FROM ${tableName} order by id, name, score asc; "
    }

    try {
        sql """ DROP TABLE IF EXISTS ${tableName}; """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
            id INT,
            name varchar(20),
            score INT
            )
            DUPLICATE KEY(id, name)
            DISTRIBUTED BY HASH(id) BUCKETS 1;
        """

        // Be must compile with WITH_LZO to use lzo
        String[] compressionTypes = new String[]{"gz", "bz2", /*"lzo",*/ "lz4", "deflate"}
        for (final def compressionType in compressionTypes) {
            def fileName = "test_compress_json.json." + compressionType
            def remoteFileName = "test_compress_json_" + +new Random().nextInt() + ".json." + compressionType
            upload_file(localFileDir + fileName, remoteFileName)
            copy_into(remoteFileName, compressionType)
        }
    } finally {
        //try_sql("DROP TABLE IF EXISTS ${tableName}")
    }
}