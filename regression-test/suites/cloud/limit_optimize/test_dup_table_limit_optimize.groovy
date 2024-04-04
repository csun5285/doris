import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_dup_table_limit_optimize") {
    def tableName = "test_dup_table_limit_optimize"
    sql """ DROP TABLE IF EXISTS ${tableName}; """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
        k1 int(11) NOT NULL COMMENT "",
        k2 int(11) NOT NULL COMMENT ""
        )
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1;
    """

    for (int i = 0; i < 20; i++) {
        sql """insert into ${tableName} values (${i}, ${i*10})"""
    }

    qt_sql """ select * from ${tableName} limit 1 """
    qt_sql """ select k1 from ${tableName} limit 1 """
    qt_sql """ select k2 as b from ${tableName} limit 1 """
    qt_sql """ select * from ${tableName} limit 5 """
    qt_sql """ select * from ${tableName} limit 10 """
    qt_sql """ select * from ${tableName} limit 20 """
    qt_sql """ select * from ${tableName} """

    sql """ delete from ${tableName} where k1 = 0"""
    sql """ delete from ${tableName} where k1 = 3"""
    sql """ delete from ${tableName} where k1 = 5"""
    sql """ delete from ${tableName} where k1 = 7"""
    sql """ delete from ${tableName} where k1 = 11"""
    sql """ delete from ${tableName} where k1 = 17"""

    qt_sql """ select * from ${tableName} limit 1 """
    qt_sql """ select k1 from ${tableName} limit 1 """
    qt_sql """ select k2 as b from ${tableName} limit 1 """
    qt_sql """ select * from ${tableName} limit 5 """
    qt_sql """ select * from ${tableName} limit 10 """
    qt_sql """ select * from ${tableName} limit 20 """
    qt_sql """ select * from ${tableName} """

    def updateBeConf = { backend_ip, backend_http_port, key, value ->
        String command = "curl -X POST http://${backend_ip}:${backend_http_port}/api/update_config?${key}=${value}"
        logger.info(command)
        process = command.execute()
        code = process.waitFor()
        assertEquals(code, 0)
    }

    String[][] backends = sql """ show backends """
    assertTrue(backends.size() > 0)
    def backendIdToBackendIP = [:]
    def backendIdToBackendHttpPort = [:]
    for (String[] backend in backends) {
        backendIdToBackendIP.put(backend[0], backend[2])
        backendIdToBackendHttpPort.put(backend[0], backend[5])
    }

    def backenId_to_org_write_buffer_size = [:]
    for (String backend_id in backendIdToBackendIP.keySet()) {
        StringBuilder showConfigCommand = new StringBuilder();
        showConfigCommand.append("curl -X GET http://")
        showConfigCommand.append(backendIdToBackendIP.get(backend_id))
        showConfigCommand.append(":")
        showConfigCommand.append(backendIdToBackendHttpPort.get(backend_id))
        showConfigCommand.append("/api/show_config")
        logger.info(showConfigCommand.toString())
        def process = showConfigCommand.toString().execute()
        int code = process.waitFor()
        String err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
        String out = process.getText()
        logger.info("Show config: code=" + code + ", out=" + out + ", err=" + err)
        assertEquals(code, 0)
        def configList = parseJson(out.trim())
        assert configList instanceof List

        int write_buffer_size = 209715200;

        for (Object ele in (List) configList) {
            assert ele instanceof List<String>
            if (((List<String>) ele)[0] == "write_buffer_size") {
                write_buffer_size = Integer.parseInt(((List<String>) ele)[2])
                logger.info("debu: " + write_buffer_size);
                backenId_to_org_write_buffer_size.put(backend_id, write_buffer_size);
            }
        }
    }
    
    try {
        for (String backend_id in backendIdToBackendIP.keySet()) {
            updateBeConf(backendIdToBackendIP.get(backend_id), backendIdToBackendHttpPort.get(backend_id),
                            "write_buffer_size", "2097152");
        }
        tableName = "customer"
        def externalStageName = "limit_optimize"

        sql """ DROP TABLE IF EXISTS ${tableName}; """

        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
            C_CUSTKEY     INTEGER NOT NULL,
            C_NAME        VARCHAR(40) NOT NULL,
            C_ADDRESS     VARCHAR(40) NOT NULL,
            C_NATIONKEY   INTEGER NOT NULL,
            C_PHONE       CHAR(15) NULL,
            C_ACCTBAL     DECIMAL(15,2)   NOT NULL,
            C_MKTSEGMENT  CHAR(10) NOT NULL,
            C_COMMENT     VARCHAR(117) NULL
            )
            DUPLICATE KEY(C_CUSTKEY)
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

        def result = sql """
                    copy into ${tableName}
                    from
                    @${externalStageName}('tpch/sf1/${tableName}.tbl') properties (
                        'file.type' = 'csv',
                        'file.column_separator' = '|',
                        'copy.async' = 'false'
                    );
                    """

        logger.info("copy result: " + result)
        assertTrue(result.size() == 1)
        assertTrue(result[0].size() == 8)
        assertTrue(result[0][1].equals("FINISHED"), "Finish copy into, state=" + result[0][1] + ", expected state=FINISHED")
        sql """select * from ${tableName} limit 1"""
        result = sql """select count(*) from ${tableName}"""
        assertTrue(result.size() == 1)
        assertTrue(result[0].size() == 1)
        assertTrue(result[0][0] == 150000)
    } finally {
        for (String backend_id in backendIdToBackendIP.keySet()) {
            updateBeConf(backendIdToBackendIP.get(backend_id), backendIdToBackendHttpPort.get(backend_id),
                            "write_buffer_size", backenId_to_org_write_buffer_size.get(backend_id));
        }
    }


}