import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_cancel_immediately") {
    def tableName = "cancel_immediately_customer"

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
    def remoteFileName = fileName + "test"
    uploadFile(remoteFileName, filePath)

    def result = sql " copy into ${tableName} from (select \$1, \$2, \$4 from @~('${remoteFileName}')) properties ('file.type' = 'csv', 'file.column_separator' = ',', 'copy.async' = 'false'); "
    logger.info("copy result: " + result)
    assertEquals(result.size(), 1)
    assertEquals(result[0].size(), 8)
    qt_sql " SELECT * FROM ${tableName}; "

    result = sql " copy into ${tableName} from (select \$1, \$2, \$4 from @~('${remoteFileName}')) properties ('file.type' = 'csv', 'file.column_separator' = ',', 'copy.async' = 'false'); "
    logger.info("copy result: " + result)
    def id = result[0][0]
    assertEquals(result[0][1], "CANCELLED")
    result = sql "SHOW COPY WHERE id = \'${id}\'"
    logger.info("show copy result: " + result)
    def createTime = result[0][9].toString()
    logger.info("createTime: " + createTime)
    def createSec = createTime.split(":")[2].toInteger()
    logger.info("createSec: " + createSec)
    def endTime = result[0][13].toString()
    logger.info("cendTime: " + endTime)
    def endSec = endTime.split(":")[2].toInteger()
    if (endSec == 00) {
        endSec = 60
    }
    logger.info("endSec: " + endSec)
    // Allow 2 second difference.
    assertTrue(createSec + 2 >= endSec)
}
