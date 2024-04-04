import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_stale_rowset") {
    sql """ use @regression_cluster_name1 """
    //cloud show backneds: BackendId,Cluster,IP,HeartbeatPort,BePort,HttpPort,BrpcPort,LastStartTime,LastHeartbeat,Alive,SystemDecommissioned,ClusterDecommissioned,TabletNum,DataUsedCapacity,AvailCapacity,TotalCapacity,UsedPct,MaxDiskUsedPct,Tag,ErrMsg,Version,Status
    //doris show backends: BackendId  Host  HeartbeatPort  BePort  HttpPort  BrpcPort  ArrowFlightSqlPort  LastStartTime  LastHeartbeat  Alive  SystemDecommissioned  TabletNum  DataUsedCapacity  TrashUsedCapcacity  AvailCapacity  TotalCapacity  UsedPct  MaxDiskUsedPct  RemoteUsedCapacity  Tag  ErrMsg  Version  Status  HeartbeatFailureCounter  NodeRole
    def backends = sql_return_maparray "show backends;"
    assertTrue(backends.size() > 0)
    String backend_id;
    def backendIdToBackendIP = [:]
    def backendIdToBackendHttpPort = [:]
    def backendIdToBackendBrpcPort = [:]
    String host = ''
    for (def backend in backends) {
        if (backend.keySet().contains('Host')) {
            host = backend.Host
        } else {
            host = backend.IP
        }
        def cloud_tag = parseJson(backend.Tag)
        if (backend.Alive.equals("true") && cloud_tag.cloud_cluster_name.contains("regression_cluster_name1")) {
            backendIdToBackendIP.put(backend.BackendId, host)
            backendIdToBackendHttpPort.put(backend.BackendId, backend.HttpPort)
            backendIdToBackendBrpcPort.put(backend.BackendId, backend.BrpcPort)
        }
    }
    String backendId = backendIdToBackendIP.keySet()[0]
    def url = backendIdToBackendIP.get(backendId) + ":" + backendIdToBackendHttpPort.get(backendId) + """/api/clear_file_cache"""
    logger.info(url)
    def clearFileCache = { check_func ->
        httpTest {
            endpoint ""
            uri url
            op "post"
            body "{\"sync\"=\"true\"}"
            check check_func
        }
    }

    clearFileCache.call() {
        respCode, body -> {}
    }

    backend_id = backendIdToBackendIP.keySet()[0]
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

    boolean disableAutoCompaction = true
    for (Object ele in (List) configList) {
        assert ele instanceof List<String>
        if (((List<String>) ele)[0] == "disable_auto_compaction") {
            disableAutoCompaction = Boolean.parseBoolean(((List<String>) ele)[2])
        }
    }
    def tables = [nation: 25]
    // def tables = [nation: 15000000]
    def tableName = "nation"
    def s3BucketName = getS3BucketName()
    def s3WithProperties = """WITH S3 (
        |"AWS_ACCESS_KEY" = "${getS3AK()}",
        |"AWS_SECRET_KEY" = "${getS3SK()}",
        |"AWS_ENDPOINT" = "${getS3Endpoint()}",
        |"AWS_REGION" = "${getS3Region()}")
        |PROPERTIES(
        |"exec_mem_limit" = "8589934592",
        |"load_parallelism" = "3")""".stripMargin()
    
    

    def table = "nation"
    sql new File("""${context.file.parent}/../ddl/${table}_delete.sql""").text
    // create table if not exists
    sql new File("""${context.file.parent}/../ddl/${table}.sql""").text

    def load_nation_once =  { 
        def uniqueID = Math.abs(UUID.randomUUID().hashCode()).toString()
        def loadLabel = table + "_" + uniqueID
        // load data from cos
        def loadSql = new File("""${context.file.parent}/../ddl/${table}_load.sql""").text.replaceAll("\\\$\\{s3BucketName\\}", s3BucketName)
        loadSql = loadSql.replaceAll("\\\$\\{loadLabel\\}", loadLabel) + s3WithProperties
        sql loadSql

        // check load state
        while (true) {
            def stateResult = sql "show load where Label = '${loadLabel}'"
            def loadState = stateResult[stateResult.size() - 1][2].toString()
            if ("CANCELLED".equalsIgnoreCase(loadState)) {
                throw new IllegalStateException("load ${loadLabel} failed.")
            } else if ("FINISHED".equalsIgnoreCase(loadState)) {
                break
            }
            sleep(5000)
        }
    }
    def getCurCacheSize = {
        backendIdToCacheSize = [:]
        for (String[] backend in backends) {
            if (backend[8].equals("true") && backend[18].contains("regression_cluster_name1")) {
                StringBuilder sb = new StringBuilder();
                sb.append("curl http://")
                sb.append(backendIdToBackendIP.get(backend[0]))
                sb.append(":")
                sb.append(backendIdToBackendBrpcPort.get(backend[0]))
                sb.append("/vars/*file_cache_cache_size")
                String command = sb.toString()
                logger.info(command);
                process = command.execute()
                code = process.waitFor()
                err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
                out = process.getText()
                logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
                assertEquals(code, 0)
                String[] str = out.split(':')
                assertEquals(str.length, 2)
                logger.info(str[1].trim())
                backendIdToCacheSize.put(backend[0], Long.parseLong(str[1].trim()))
            }
        }
        return backendIdToCacheSize
    }
    load_nation_once()
    load_nation_once()
    load_nation_once()
    load_nation_once()
    load_nation_once()
    sleep(30000);
    def backendIdToAfterLoadCacheSize = getCurCacheSize()
    for (String[] backend in backends) {
        if (backend[8].equals("true") && backend[18].contains("regression_cluster_name1")) {
            logger.info(backend[0] + " size: " + backendIdToAfterLoadCacheSize.get(backend[0]))
        }
    }
    sql """
    select count(*) from ${tableName};
    """

    String[][] tablets = sql """ show tablets from ${tableName}; """

    // trigger compactions for all tablets in ${tableName}
    for (String[] tablet in tablets) {
        String tablet_id = tablet[0]
        backend_id = tablet[2]
        StringBuilder sb = new StringBuilder();
        sb.append("curl -X POST http://")
        sb.append(backendIdToBackendIP.get(backend_id))
        sb.append(":")
        sb.append(backendIdToBackendHttpPort.get(backend_id))
        sb.append("/api/compaction/run?tablet_id=")
        sb.append(tablet_id)
        sb.append("&compact_type=cumulative")

        String command = sb.toString()
        process = command.execute()
        code = process.waitFor()
        err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
        out = process.getText()
        logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
        assertEquals(code, 0)
        def compactJson = parseJson(out.trim())
        if (compactJson.status.toLowerCase() == "fail") {
            assertEquals(disableAutoCompaction, false)
            logger.info("Compaction was done automatically!")
        }
        if (disableAutoCompaction) {
            assertEquals("success", compactJson.status.toLowerCase())
        }
    }

    // wait for all compactions done
    for (String[] tablet in tablets) {
        boolean running = true
        do {
            Thread.sleep(1000)
            String tablet_id = tablet[0]
            backend_id = tablet[2]
            StringBuilder sb = new StringBuilder();
            sb.append("curl -X GET http://")
            sb.append(backendIdToBackendIP.get(backend_id))
            sb.append(":")
            sb.append(backendIdToBackendHttpPort.get(backend_id))
            sb.append("/api/compaction/run_status?tablet_id=")
            sb.append(tablet_id)

            String command = sb.toString()
            logger.info(command)
            process = command.execute()
            code = process.waitFor()
            err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
            out = process.getText()
            logger.info("Get compaction status: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            def compactionStatus = parseJson(out.trim())
            assertEquals("success", compactionStatus.status.toLowerCase())
            running = compactionStatus.run_status
        } while (running)
    }

    sql """
    select count(*) from ${tableName};
    """

    sleep(60000);
    def backendIdToAfterCompactionCacheSize = getCurCacheSize()
    for (String[] backend in backends) {
        if (backend[8].equals("true") && backend[18].contains("regression_cluster_name1")) {
            assertTrue(backendIdToAfterLoadCacheSize.get(backend[0]) >
                backendIdToAfterCompactionCacheSize.get(backend[0]))
        }
    }
    sql new File("""${context.file.parent}/../ddl/${table}_delete.sql""").text
}
