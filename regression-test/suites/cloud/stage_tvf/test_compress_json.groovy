import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_compress_json") {
    def tableName = "test_compress_json"
    def localFileDir = "${context.config.dataPath}/cloud/stage_tvf/"

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

    def insert_into = {remotePath, compressionType ->
        sql " insert into ${tableName} select * from stage('stage_name'='~', 'file_pattern'='${remotePath}', 'format' = 'json', 'read_json_by_line' = 'true', 'compress' = '${compressionType}'); "
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
            insert_into(remoteFileName, compressionType)
        }
    } finally {
        try_sql("DROP TABLE IF EXISTS ${tableName}")
    }
}