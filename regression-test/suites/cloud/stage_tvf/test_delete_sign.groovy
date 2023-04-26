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

    def insertInto = {id, insertSql ->
        def result = sql """ ${insertSql} """
        logger.info(id + ", insert result: " + result)
        qt_sql " SELECT COUNT(*) FROM ${tableName}; "
        qt_sql "select * from ${tableName} order by id, name, score asc;"
    }

    def insertIntoWithException = { insertSql, exp ->
        try {
            def result = sql """ ${insertSql} """
            assertTrue(false, "should throw exception, result=" + result)
        } catch (Exception e) {
            // logger.info("catch exception", e)
            assertTrue(e.getMessage().contains(exp), "real message=" + e.getMessage())
        }
    }

    // insert from csv
    def insert_prefix = """insert into ${tableName} (id, name, score, __DORIS_DELETE_SIGN__) """;
    def properties = """ 'format' = 'csv', 'column_separator' = ',')"""
    def deleteOnSql0 = insert_prefix + """ select * from stage('stage_name'='~', 'file_pattern'='${deleteFilePrefix}%d', """ + properties
    def deleteOnSql1 = insert_prefix + """ select * from stage('stage_name'='~', 'file_pattern'='${deleteFilePrefix}%d', """ + properties
    def deleteOnSql2 = insert_prefix + """ select c1, c2, c3, c4 from stage('stage_name'='~', 'file_pattern'='${deleteFilePrefix}%d', """ + properties
    def deleteOnSql3 = insert_prefix + """ select c1, c2, c3, c4 = 1 from stage('stage_name'='~', 'file_pattern'='${deleteFilePrefix}%d', """ + properties
    def deleteOnSql4 = insert_prefix + """ select c1, c2, c3, c4 = 0 from stage('stage_name'='~', 'file_pattern'='${deleteFilePrefix}%d', """ + properties
    def deleteOnSql5 = insert_prefix + """ select c1, c2, c4, c3 > 50 from stage('stage_name'='~', 'file_pattern'='${deleteFilePrefix}%d', """ + properties
    def deleteOnSql6 = insert_prefix + """ select c1, c2, c3, c4 from stage('stage_name'='~', 'file_pattern'='${deleteFilePrefix}%d', """  + properties + ' where c3 > 40'
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
                def sql = """ insert into ${tableName} select * from stage('stage_name'='~', 'file_pattern'='${fileName}', 'format' = 'csv', 'column_separator' = ',');"""
                insertInto("0.1", sql)

                uploadFile(deleteFilePrefix + index, deleteFilePath);
                deleteSql = String.format(deleteSql, index)
                insertInto("0.2", deleteSql)
            } finally {
                try_sql("DROP TABLE IF EXISTS ${tableName}")
            }
        }
    }

    // insert from invalid csv
    try {
        createTable()
        uploadFile("test_delete_on_error.csv", filePath)
        def deleteSql = insert_prefix + """ select c1, c2, c3, c4 from stage('stage_name'='~', 'file_pattern'='test_delete_on_error.csv', """ + properties
        insertIntoWithException(deleteSql, "errCode = 2, detailMessage = Unknown column 'c4' in 'table list'")
    } finally {
        try_sql("DROP TABLE IF EXISTS ${tableName}")
    }

    // insert from json, parquet, orc
    sql """ SET show_hidden_columns=false """
    def fileTypes = ["json", "parquet", "orc"]
    for (final def fileType in fileTypes) {
        try {
            def fileName = "test_delete_on." + fileType
            filePath = filePathDir + fileName
            uploadFile(fileName, filePath)

            // 1.1 load data
            createTable()
            sql """ set max_filter_ratio = 0.25 """
            def label = "test_delete_sign_" + System.currentTimeMillis()
            def sql0 = """ insert into ${tableName} with label ${label} select id, name, score from stage('stage_name'='~', 'file_pattern'='${fileName}', 'format' = '${fileType}', 'read_json_by_line'='true')"""
            insertInto("1.1", sql0)
            // 1.2 load data with delete sign
            sql " set max_filter_ratio = 0.4 "
            label = "test_delete_sign_" + (System.currentTimeMillis() + 1)
            sql0 = """insert into ${tableName} with label ${label} (id, name, score, __DORIS_DELETE_SIGN__) select * from stage('stage_name'='~', 'file_pattern'='${fileName}', 'format' = '${fileType}', 'read_json_by_line'='true')"""
            insertInto("1.2", sql0)
            // 1.3 load data with columns and delete sign
            label = "test_delete_sign_" + (System.currentTimeMillis() + 2)
            sql0 = """insert into ${tableName} with label ${label} (id, name, __DORIS_DELETE_SIGN__) select id, name, __DORIS_DELETE_SIGN__ from stage('stage_name'='~', 'file_pattern'='${fileName}', 'format' = '${fileType}', 'read_json_by_line'='true')"""
            insertInto("1.3", sql0)

            // 2.1 load data with columns and delete sign
            createTable()
            label = "test_delete_sign_" + (System.currentTimeMillis() + 3)
            sql0 = """insert into ${tableName} with label ${label} (id, name, __DORIS_DELETE_SIGN__) select id, name, __DORIS_DELETE_SIGN__ from stage('stage_name'='~', 'file_pattern'='${fileName}', 'format' = '${fileType}', 'read_json_by_line'='true')"""
            insertInto("2.1", sql0)

            // 3.1 load data
            createTable()
            sql " set max_filter_ratio = 0.25 "
            label = "test_delete_sign_" + (System.currentTimeMillis() + 4)
            sql0 = """insert into ${tableName} with label ${label} (id, name, score) select id, name, score from stage('stage_name'='~', 'file_pattern'='${fileName}', 'format' = '${fileType}', 'read_json_by_line'='true')"""
            insertInto("3.1", sql0)
            // 3.2 load data with columns and delete sign
            sql " set max_filter_ratio = 0.4 "
            label = "test_delete_sign_" + (System.currentTimeMillis() + 5)
            sql0 = """insert into ${tableName} with label ${label} (id, name, __DORIS_DELETE_SIGN__) select id, name, __DORIS_DELETE_SIGN__ from stage('stage_name'='~', 'file_pattern'='${fileName}', 'format' = '${fileType}', 'read_json_by_line'='true')"""
            insertInto("3.2", sql0)

            // 4.1 load data with unmatched columns
            createTable()
            label = "test_delete_sign_" + (System.currentTimeMillis() + 6)
            sql0 = """insert into ${tableName} with label ${label} (id, name, __DORIS_DELETE_SIGN__)  select id, name, score, __DORIS_DELETE_SIGN__ from stage('stage_name'='~', 'file_pattern'='${fileName}', 'format' = '${fileType}', 'read_json_by_line'='true')"""
            insertIntoWithException(sql0, "Column count doesn't match value count")
            // 4.2 load data with unmatched columns
            label = "test_delete_sign_" + (System.currentTimeMillis() + 7)
            sql0 = """insert into ${tableName} with label ${label} (id, name, score, __DORIS_DELETE_SIGN__) select id, name, __DORIS_DELETE_SIGN__ from stage('stage_name'='~', 'file_pattern'='${fileName}', 'format' = '${fileType}', 'read_json_by_line'='true')"""
            insertIntoWithException(sql0, "Column count doesn't match value count")

            // 4.3 load data with unmatched columns
            label = "test_delete_sign_" + (System.currentTimeMillis() + 8)
            sql0 = """insert into ${tableName} with label ${label} (id, name) select id, name, __DORIS_DELETE_SIGN__ from stage('stage_name'='~', 'file_pattern'='${fileName}', 'format' = '${fileType}', 'read_json_by_line'='true')"""
            insertIntoWithException(sql0, "Column count doesn't match value count")
            // 4.4 load data with unmatched columns
            label = "test_delete_sign_" + (System.currentTimeMillis() + 9)
            sql0 = """insert into ${tableName} with label ${label} (id, name)  select id, __DORIS_DELETE_SIGN__, name from stage('stage_name'='~', 'file_pattern'='${fileName}', 'format' = '${fileType}', 'read_json_by_line'='true')"""
            insertIntoWithException(sql0, "Column count doesn't match value count")
            // 4.5 load data with unmatched columns
            label = "test_delete_sign_" + (System.currentTimeMillis() + 10)
            sql0 = """insert into ${tableName} with label ${label} (id, __DORIS_DELETE_SIGN__, name)  select id, name from stage('stage_name'='~', 'file_pattern'='${fileName}', 'format' = '${fileType}', 'read_json_by_line'='true')"""
            insertIntoWithException(sql0, "Column count doesn't match value count")
            // 4.6 load data with unmatched columns
            label = "test_delete_sign_" + (System.currentTimeMillis() + 11)
            sql0 = """insert into ${tableName} with label ${label} (id, name, __DORIS_DELETE_SIGN__)  select id, name from stage('stage_name'='~', 'file_pattern'='${fileName}', 'format' = '${fileType}', 'read_json_by_line'='true')"""
            insertIntoWithException(sql0, "Column count doesn't match value count")

            // 5.1 load data with transfer delete sign
            label = "test_delete_sign_" + (System.currentTimeMillis() + 12)
            sql0 = """insert into ${tableName} with label ${label} (id, name, score, __DORIS_DELETE_SIGN__)  select id, name, score, __DORIS_DELETE_SIGN__ = 0 from stage('stage_name'='~', 'file_pattern'='${fileName}', 'format' = '${fileType}', 'read_json_by_line'='true')"""
            insertInto("5.1", sql0)
            // 5.2 load data with transfer delete sign (ANNT: should this success?)
            label = "test_delete_sign_" + (System.currentTimeMillis() + 13)
            sql0 = """insert into ${tableName} with label ${label} select id, name, score, score <= 40 from stage('stage_name'='~', 'file_pattern'='${fileName}', 'format' = '${fileType}', 'read_json_by_line'='true')"""
            insertIntoWithException(sql0, "Column count doesn't match value count")

            // 6.1 load data with __DORIS_DELETE_SIGN__ column and copy.use_delete_sign = false
            sql0 = """insert into ${tableName} (id, name, score, __DORIS_DELETE_SIGN__) select id, name, score, __DORIS_DELETE_SIGN__ from stage('stage_name'='~', 'file_pattern'='${fileName}', 'format' = '${fileType}', 'read_json_by_line'='true')"""
            insertInto("6.1", sql0)
        } finally {
            try_sql("DROP TABLE IF EXISTS ${tableName}")
        }
    }
}
