import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_strict_mode") {
    // Internal and external stage cross use
    def tableNamExternal = "customer_internal_stage"
    def token = "greedisgood9999"
    def instanceId = context.config.instanceId
    def cloudUniqueId = context.config.cloudUniqueId
    def tableName = "test_strict_mode_tbl"

    def uploadFile = { name, path ->
        StringBuilder strBuilder = new StringBuilder()
        strBuilder.append("""curl -u """ + context.config.feCloudHttpUser + ":" + context.config.feCloudHttpPassword)
        strBuilder.append(""" -H fileName:""" + name)
        strBuilder.append(""" -T """ + path)
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

    sql """ DROP TABLE IF EXISTS ${tableName}; """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
          `name` varchar(128) NULL,
          `age` int(11) NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`name`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`name`) BUCKETS 1
    """

    def fileName = "test_strict_mode.json"
    def filePath = "${context.config.dataPath}/cloud/copy_into/" + fileName
    uploadFile(fileName, filePath)

    def result = sql " copy into ${tableName} from @~('${fileName}') properties ('file.type' = 'json', 'copy.async' = 'false', 'copy.strict_mode' = 'true'); "
    logger.info("copy result: " + result)
    assertTrue(result.size() == 1)
    assertTrue(result[0].size() == 8)
    assertTrue(result[0][1].equals("CANCELLED"), "Finish copy into, state=" + result[0][1] + ", expected state=CANCELLED")
    assertTrue(result[0][2].equals("ETL_QUALITY_UNSATISFIED"))

    result = sql " copy into ${tableName} from @~('${fileName}') properties ('file.type' = 'json', 'copy.async' = 'false', 'copy.strict_mode' = 'true', 'copy.use_delete_sign'='true'); "
    logger.info("copy result: " + result)
    assertTrue(result.size() == 1)
    assertTrue(result[0].size() == 8)
    assertTrue(result[0][1].equals("CANCELLED"), "Finish copy into, state=" + result[0][1] + ", expected state=CANCELLED")
    assertTrue(result[0][2].equals("ETL_QUALITY_UNSATISFIED"))

    result = sql " copy into ${tableName} from @~('${fileName}') properties ('file.type' = 'json', 'copy.async' = 'false', 'copy.strict_mode' = 'true', 'copy.use_delete_sign'='false'); "
    logger.info("copy result: " + result)
    assertTrue(result.size() == 1)
    assertTrue(result[0].size() == 8)
    assertTrue(result[0][1].equals("CANCELLED"), "Finish copy into, state=" + result[0][1] + ", expected state=CANCELLED")
    assertTrue(result[0][2].equals("ETL_QUALITY_UNSATISFIED"))

    result = sql " copy into ${tableName} from @~('${fileName}') properties ('file.type' = 'json', 'copy.async' = 'false', 'copy.strict_mode' = 'false', 'copy.use_delete_sign'='true'); "
    logger.info("copy result: " + result)
    assertTrue(result.size() == 1)
    assertTrue(result[0].size() == 8)
    assertTrue(result[0][1].equals("FINISHED"), "Finish copy into, state=" + result[0][1] + ", expected state=FINISHED")
    assertTrue(result[0][4].equals("2"), "load rows=" + result[0][4] + ", expected_rows=2")
}