import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_delete_sign") {
    def tableName = "test_delete_on"
    def filePathDir = "${context.config.dataPath}/cloud/copy_into/"
    def filePath = filePathDir + "test_delete_on_0.csv"
    def deleteFilePath = "${context.config.dataPath}/cloud/copy_into/" + "test_delete_on_1.csv"
    def deleteFilePrefix = "test_delete_on_"

    def createTable = {
        sql """ DROP TABLE IF EXISTS ${tableName}; """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
            id INT not null default '-1',
            name varchar(20) not null default 'fuzzy',
            score INT default '0'
            )
           UNIQUE KEY(id)
           DISTRIBUTED BY HASH(id) BUCKETS 1
           """
    }

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

    def copyInto = { copySql ->
        def result = sql """ ${copySql} """
        logger.info("copy result: " + result)
        assertTrue(result.size() == 1)
        assertTrue(result[0].size() == 8)
        assertTrue(result[0][1].equals("FINISHED"), "Finish copy into, state=" + result[0][1] + ", expected state=FINISHED")
        qt_sql " SELECT COUNT(*) FROM ${tableName}; "
        qt_sql "select * from ${tableName} order by id, name, score asc;"
    }

    def copyIntoWithException = { copySql ->
        try {
            def result = sql """ ${copySql} """
            assertTrue(false, "should throw exception, result=" + result)
        } catch (Exception e) {
            // logger.info("catch exception", e)
            assertTrue(e.getMessage().contains("Column count") && e.getMessage().contains("doesn't match value count"))
        }
    }

    // copy from csv
    def copy_prefix = """copy into ${tableName} from """;
    def properties = """properties ('file.type' = 'csv', 'file.column_separator' = ',', 'copy.async' = 'false', 'copy.use_delete_sign' = 'true')"""
    def deleteOnSql0 = copy_prefix + """ @~('${deleteFilePrefix}%d')""" + properties
    def deleteOnSql1 = copy_prefix + " (select from @~('${deleteFilePrefix}%d') )" + properties
    def deleteOnSql2 = copy_prefix + ' (select `$1`, $2, $3, $4 from' + """ @~('${deleteFilePrefix}%d') )""" + properties
    def deleteOnSql3 = copy_prefix + ' (select `$1`, $2, $3, $4 = 1 from' + """ @~('${deleteFilePrefix}%d') )""" + properties
    def deleteOnSql4 = copy_prefix + ' (select `$1`, $2, $3, $4 = 0 from' + """ @~('${deleteFilePrefix}%d') )""" + properties
    def deleteOnSql5 = copy_prefix + ' (select `$1`, $2, $4, $3 > 50 from' + """ @~('${deleteFilePrefix}%d') )""" + properties
    def deleteOnSql6 = copy_prefix + ' (select `$1`, $2, $3, $4 from' + """ @~('${deleteFilePrefix}%d') """ + ' where $3 > 40)' + properties
    def sqls = [deleteOnSql0, deleteOnSql1, deleteOnSql2, deleteOnSql3, deleteOnSql4, deleteOnSql5, deleteOnSql6]

    for (int i = 0; i < sqls.size(); i++) {
        for (int j = 0; j < 2; j++) {
            if (j == 0) {
                sql """ SET show_hidden_columns=false """
            } else {
                sql """ SET show_hidden_columns=true """
            }
            int index = j * sqls.size() + i
            def deleteSql = sqls[i]
            try {
                createTable()
                def fileName = "test_delete_on_0_" + index + ".csv"
                uploadFile(fileName, filePath)
                def sql = """ copy into ${tableName} from @~('${fileName}') properties ('file.type' = 'csv', 'file.column_separator' = ',', 'copy.async' = 'false');"""
                copyInto(sql)

                uploadFile(deleteFilePrefix + index, deleteFilePath);
                deleteSql = String.format(deleteSql, index)
                copyInto(deleteSql)
            } finally {
                //try_sql("DROP TABLE IF EXISTS ${tableName}")
            }
        }
    }

    // copy from invalid csv
    try {
        createTable()
        uploadFile("test_delete_on_error.csv", filePath)
        def deleteSql = copy_prefix + """ @~('test_delete_on_error.csv') """ + ' ' + properties
        def result = sql """ ${deleteSql}"""
        logger.info("copy result: " + result)
        assertTrue(result.size() == 1)
        assertTrue(result[0].size() == 8)
        assertTrue(result[0][1].equals("CANCELLED"), "Finish copy into, state=" + result[0][1] + ", expected state=CANCELLED")
        assertTrue(result[0][2].equals("ETL_QUALITY_UNSATISFIED"))
    } finally {
        //try_sql("DROP TABLE IF EXISTS ${tableName}")
    }

    // copy from json, parquet, orc
    sql """ SET show_hidden_columns=false """
    def fileTypes = ["json", "parquet", "orc"]
    for (final def fileType in fileTypes) {
        try {
            def fileName = "test_delete_on." + fileType
            filePath = filePathDir + fileName
            uploadFile(fileName, filePath)

            // 1.1 load data
            createTable()
            def sql = copy_prefix + """ @~('${fileName}') properties ('file.type' = '${fileType}', 'copy.async' = 'false', 'copy.force' = 'true', 'copy.on_error'='max_filter_ratio_0.25')"""
            copyInto(sql)
            // 1.2 load data with delete sign
            sql = copy_prefix + """ @~('${fileName}') properties ('file.type' = '${fileType}', 'copy.async' = 'false', 'copy.use_delete_sign' = 'true', 'copy.on_error'='max_filter_ratio_0.4', 'copy.force'= 'true')"""
            copyInto(sql)
            // 1.3 load data with columns and delete sign
            sql = """copy into ${tableName} (id, name) from (select id, name from @~('${fileName}')) properties ('file.type' = '${fileType}', 'copy.async' = 'false', 'copy.use_delete_sign' = 'true', 'copy.on_error'='max_filter_ratio_0.4', 'copy.force' = 'true')"""
            copyInto(sql)

            // 2.1 load data with columns and delete sign
            createTable()
            sql = """copy into ${tableName} (id, name) from (select id, name from @~('${fileName}')) properties ('file.type' = '${fileType}', 'copy.async' = 'false', 'copy.use_delete_sign' = 'true', 'copy.on_error'='max_filter_ratio_0.4', 'copy.force' = 'true')"""
            copyInto(sql)

            // 3.1 load data
            createTable()
            sql = copy_prefix + """ @~('${fileName}') properties ('file.type' = '${fileType}', 'copy.async' = 'false', 'copy.on_error'='max_filter_ratio_0.25', 'copy.force' = 'true')"""
            copyInto(sql)
            // 3.2 load data with columns and delete sign
            sql = """copy into ${tableName} (id, name) from (select id, name from @~('${fileName}')) properties ('file.type' = '${fileType}', 'copy.async' = 'false', 'copy.use_delete_sign' = 'true', 'copy.on_error'='max_filter_ratio_0.4', 'copy.force' = 'true')"""
            copyInto(sql)

            // 4.1 load data with unmatched columns
            createTable()
            sql = """copy into ${tableName} (id, name) from (select id, name, score from @~('${fileName}')) properties ('file.type' = '${fileType}', 'copy.async' = 'false', 'copy.use_delete_sign' = 'true', 'copy.on_error'='max_filter_ratio_0.4', 'copy.force' = 'true')"""
            copyIntoWithException(sql)
            // 4.2 load data with unmatched columns
            sql = """copy into ${tableName} (id, name, score) from (select id, name from @~('${fileName}')) properties ('file.type' = '${fileType}', 'copy.async' = 'false', 'copy.use_delete_sign' = 'true', 'copy.on_error'='max_filter_ratio_0.4', 'copy.force' = 'true')"""
            copyIntoWithException(sql)

            // 4.3 load data with unmatched columns
            sql = """copy into ${tableName} (id, name) from (select id, name, __DORIS_DELETE_SIGN__ from @~('${fileName}')) properties ('file.type' = '${fileType}', 'copy.async' = 'false', 'copy.use_delete_sign' = 'true', 'copy.on_error'='max_filter_ratio_0.4', 'copy.force' = 'true')"""
            copyIntoWithException(sql)
            // 4.4 load data with unmatched columns
            sql = """copy into ${tableName} (id, name) from (select id, __DORIS_DELETE_SIGN__, name from @~('${fileName}')) properties ('file.type' = '${fileType}', 'copy.async' = 'false', 'copy.use_delete_sign' = 'true', 'copy.on_error'='max_filter_ratio_0.4', 'copy.force' = 'true')"""
            copyIntoWithException(sql)
            // 4.5 load data with unmatched columns
            sql = """copy into ${tableName} (id, __DORIS_DELETE_SIGN__, name) from (select id, name from @~('${fileName}')) properties ('file.type' = '${fileType}', 'copy.async' = 'false', 'copy.use_delete_sign' = 'true', 'copy.on_error'='max_filter_ratio_0.4', 'copy.force' = 'true')"""
            copyIntoWithException(sql)
            // 4.6 load data with unmatched columns
            sql = """copy into ${tableName} (id, name, __DORIS_DELETE_SIGN__) from (select id, name from @~('${fileName}')) properties ('file.type' = '${fileType}', 'copy.async' = 'false', 'copy.use_delete_sign' = 'true', 'copy.on_error'='max_filter_ratio_0.4', 'copy.force' = 'true')"""
            copyIntoWithException(sql)

            // 5.1 load data with transfer delete sign
            sql = """copy into ${tableName} (id, name, score, __DORIS_DELETE_SIGN__) from (select id, name, score, __DORIS_DELETE_SIGN__ = 0 from @~('${fileName}')) properties ('file.type' = '${fileType}', 'copy.async' = 'false', 'copy.use_delete_sign' = 'true', 'copy.on_error'='max_filter_ratio_0.4', 'copy.force' = 'true')"""
            copyInto(sql)
            // 5.2 load data with transfer delete sign (ANNT: should this success?)
            sql = """copy into ${tableName} from (select id, name, score, score <= 40 from @~('${fileName}')) properties ('file.type' = '${fileType}', 'copy.async' = 'false', 'copy.use_delete_sign' = 'true', 'copy.on_error'='max_filter_ratio_0.4', 'copy.force' = 'true')"""
            copyIntoWithException(sql)

            // 6.1 load data with __DORIS_DELETE_SIGN__ column and copy.use_delete_sign = false
            sql = """copy into ${tableName} (id, name, score, __DORIS_DELETE_SIGN__) from (select id, name, score, __DORIS_DELETE_SIGN__ from @~('${fileName}')) properties ('file.type' = '${fileType}', 'copy.async' = 'false', 'copy.on_error'='max_filter_ratio_0.4', 'copy.force' = 'true')"""
            copyInto(sql)
        } finally {
            //try_sql("DROP TABLE IF EXISTS ${tableName}")
        }
    }

    // copy from csv without delete_sign column
    {
        sql """ truncate table ${tableName}; """
        filePath = filePathDir + "test_delete_on_0.csv"

        def fileName = "test_delete_on_0_" + sqls.size() + ".csv"
        uploadFile(fileName, filePath)
        def sql = copy_prefix + ' (select `$1`, $2, $3, 0 from' + """ @~('${fileName}') )""" + properties
        copyInto(sql)

        fileName = "test_delete_on_0_" + (sqls.size() + 1) + ".csv"
        uploadFile(fileName, filePath)
        sql = copy_prefix + ' (select `$1`, $2, $3, 1 from' + """ @~('${fileName}') )""" + properties
        copyInto(sql)
    }
}
