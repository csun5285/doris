import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_csv") {
    // Internal and external stage cross use
    def tableNamExternal = "customer_internal_stage"
    def token = "greedisgood9999"
    def instanceId = context.config.instanceId
    def cloudUniqueId = context.config.cloudUniqueId
    def tableName = "customer_internal_stage_csv"
    def tableName2 = "customer_internal_stage2"

    def uploadFile = { remoteFilePath, localFilePath ->
        StringBuilder strBuilder = new StringBuilder()
        strBuilder.append("""curl -v -u """ + context.config.feCloudHttpUser + ":" + context.config.feCloudHttpPassword)
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

    // csv 5 col, table 3 col, load 5 col
    sql """ DROP TABLE IF EXISTS ${tableName}; """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
           col1       INTEGER NOT NULL,
           col2       VARCHAR(25) NOT NULL,
           col3       VARCHAR(40) NOT NULL,
        )
        DUPLICATE KEY(col1)
        DISTRIBUTED BY HASH(col1) BUCKETS 1
    """

    def fileName = "test_csv_col.csv"
    def filePath = "${context.config.dataPath}/cloud/copy_into/" + fileName
    def remoteFileName = fileName + "test_internal_stage"
    uploadFile(remoteFileName, filePath)

    def result = sql " copy into ${tableName} from (select * from @~('${remoteFileName}')) properties ('file.type' = 'csv', 'file.column_separator' = ',', 'copy.async' = 'false'); "
    logger.info("copy result: " + result)
    assertTrue(result.size() == 1)
    assertTrue(result[0].size() == 8)
    assertTrue(result[0][1].equals("CANCELLED"), "Finish copy into, state=" + result[0][1] + ", expected state=CANCELLED")

    // csv 5 col, table 3 col, load 3 col
    result = sql " copy into ${tableName} from (select \$1, \$2, \$4 from @~('${remoteFileName}')) properties ('file.type' = 'csv', 'file.column_separator' = ',', 'copy.async' = 'false'); "
    logger.info("copy result: " + result)
    assertTrue(result.size() == 1)
    assertTrue(result[0].size() == 8)
    assertTrue(result[0][1].equals("FINISHED"), "Finish copy into, state=" + result[0][1] + ", expected state=FINISHED")
    qt_sql " SELECT * FROM ${tableName}; "

    // csv 5 col, table 3 col, load 2 col
    sql """ DROP TABLE IF EXISTS ${tableName}; """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
           col1       INTEGER NOT NULL,
           col2       VARCHAR(25) NULL,
           col3       VARCHAR(40) NOT NULL,
        )
        DUPLICATE KEY(col1)
        DISTRIBUTED BY HASH(col1) BUCKETS 1
    """

    fileName = "test_csv_col.csv"
    filePath = "${context.config.dataPath}/cloud/copy_into/" + fileName
    remoteFileName = fileName + "test_internal_stage"
    uploadFile(remoteFileName, filePath)

    result = sql " copy into ${tableName} (col1, col3) from (select \$1, \$4 from @~('${remoteFileName}')) properties ('file.type' = 'csv', 'file.column_separator' = ',', 'copy.async' = 'false'); "
    logger.info("copy result: " + result)
    assertTrue(result.size() == 1)
    assertTrue(result[0].size() == 8)
    assertTrue(result[0][1].equals("FINISHED"), "Finish copy into, state=" + result[0][1] + ", expected state=FINISHED")
    qt_sql " SELECT * FROM ${tableName}; "

    /*
    // case3 csv 5 col, table 7 col, load 5 col
    sql """ DROP TABLE IF EXISTS ${tableName}; """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
           col1       INTEGER NOT NULL,
           col2       VARCHAR(25) NOT NULL,
           col3       VARCHAR(40) NOT NULL,
           col4       INTEGER NOT NULL,
           col5       CHAR(15)  NULL,
           col6       INTEGER NOT NULL,
           col7       INTEGER NULL,
        )
        DUPLICATE KEY(col1)
        DISTRIBUTED BY HASH(col1) BUCKETS 1
    """

    fileName = "test_csv_col.csv"
    filePath = "${context.config.dataPath}/cloud/copy_into/" + fileName
    remoteFileName = fileName + "test_internal_stage"
    uploadFile(remoteFileName, filePath)

    result = sql " copy into ${tableName} (col1, col2, col3, col4, col5) from @~('${remoteFileName}') properties ('file.type' = 'csv', 'file.column_separator' = ',', 'copy.async' = 'false'); "
    logger.info("copy result: " + result)
    assertTrue(result.size() == 1)
    assertTrue(result[0].size() == 8)
    assertTrue(result[0][1].equals("FINISHED"), "Finish copy into, state=" + result[0][1] + ", expected state=FINISHED")
    qt_sql " SELECT COUNT(*) FROM ${tableName}; "
    */
}
