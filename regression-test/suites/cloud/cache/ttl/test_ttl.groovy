import org.codehaus.groovy.runtime.IOGroovyMethods

// 1. clear file cache
// 2. load customer once
// 3. check ttl size and total size whether equal
// 4. select some data for checking whether the datas all in cache or not
// 5. wait for ttl data timeout
// 6. check the ttl size is equal to zero, but total size is constant
// 7. select some data for checking whether the datas still all in cache or not
suite("test_ttl") {
    sql """ use @regression_cluster_name1 """
    def ttlProperties = """ PROPERTIES("file_cache_ttl_seconds"="180") """
    //doris show backends: BackendId  Host  HeartbeatPort  BePort  HttpPort  BrpcPort  ArrowFlightSqlPort  LastStartTime  LastHeartbeat  Alive  SystemDecommissioned  TabletNum  DataUsedCapacity  TrashUsedCapcacity  AvailCapacity  TotalCapacity  UsedPct  MaxDiskUsedPct  RemoteUsedCapacity  Tag  ErrMsg  Version  Status  HeartbeatFailureCounter  NodeRole
    def backends = sql_return_maparray "show backends;"
    assertTrue(backends.size() > 0)
    String backend_id;
    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    def backendId_to_backendBrpcPort = [:]
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
    assertEquals(backendIdToBackendIP.size(), 1)

    backendId = backendIdToBackendIP.keySet()[0]
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

    def tables = [customer_ttl: 15000000]
    def s3BucketName = getS3BucketName()
    def s3WithProperties = """WITH S3 (
        |"AWS_ACCESS_KEY" = "${getS3AK()}",
        |"AWS_SECRET_KEY" = "${getS3SK()}",
        |"AWS_ENDPOINT" = "${getS3Endpoint()}",
        |"AWS_REGION" = "${getS3Region()}")
        |PROPERTIES(
        |"exec_mem_limit" = "8589934592",
        |"load_parallelism" = "3")""".stripMargin()
    
    
    sql new File("""${context.file.parent}/../ddl/customer_ttl_delete.sql""").text
    def load_customer_once =  { String table ->
        def uniqueID = Math.abs(UUID.randomUUID().hashCode()).toString()
        sql (new File("""${context.file.parent}/../ddl/${table}.sql""").text + ttlProperties)
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

    def getMetricsMethod = { check_func ->
        httpTest {
            endpoint backendIdToBackendIP.get(backendId) + ":" + backendIdToBackendBrpcPort.get(backendId)
            uri "/brpc_metrics"
            op "get"
            check check_func
        }
    }

    clearFileCache.call() {
        respCode, body -> {}
    }

    load_customer_once("customer_ttl")
    sleep(30000) // 30s
    long total_cache_size = 0
    getMetricsMethod.call() {
        respCode, body ->
            assertEquals("${respCode}".toString(), "200")
            String out = "${body}".toString()
            def strs = out.split('\n')
            Boolean flag1 = false;
            Boolean flag2 = false;
            long ttl_cache_size = 0;
            for (String line in strs) {
                if (flag1 && flag2) break;
                if (line.contains("ttl_cache_size")) {
                    if (line.startsWith("#")) {
                        continue
                    }
                    def i = line.indexOf(' ')
                    ttl_cache_size = line.substring(i).toLong()
                    flag1 = true
                }
                if (line.contains("file_cache_cache_size")) {
                    if (line.startsWith("#")) {
                        continue
                    }
                    def i = line.indexOf(' ')
                    total_cache_size = line.substring(i).toLong()
                    flag2 = true
                }
            }
            assertTrue(flag1 && flag2)
            assertEquals(ttl_cache_size, total_cache_size)
    }
    long s3_read_count = 0
    getMetricsMethod.call() {
        respCode, body ->
            assertEquals("${respCode}".toString(), "200")
            String out = "${body}".toString()
            def strs = out.split('\n')
            Boolean flag = false;
            for (String line in strs) {
                if (line.contains("cached_remote_reader_s3_read")) {
                    if (line.startsWith("#")) {
                        continue
                    }
                    def i = line.indexOf(' ')
                    s3_read_count = line.substring(i).toLong()
                    flag = true
                    break
                }
            }
            assertTrue(flag)
    }

    sql """ select * from customer_ttl limit 10 """
    sleep(10000)
    getMetricsMethod.call() {
        respCode, body ->
            assertEquals("${respCode}".toString(), "200")
            String out = "${body}".toString()
            def strs = out.split('\n')
            Boolean flag = false;
            long read_at_count = 0;
            for (String line in strs) {
                if (line.contains("cached_remote_reader_s3_read")) {
                    if (line.startsWith("#")) {
                        continue
                    }
                    def i = line.indexOf(' ')
                    read_at_count = line.substring(i).toLong()
                    assertEquals(s3_read_count, read_at_count)
                    flag = true
                    break
                }
            }
            assertTrue(flag)
    }
    sleep(180000)
    getMetricsMethod.call() {
        respCode, body ->
            assertEquals("${respCode}".toString(), "200")
            String out = "${body}".toString()
            def strs = out.split('\n')
            Boolean flag1 = false;
            for (String line in strs) {
                if (flag1) break;
                if (line.contains("ttl_cache_size")) {
                    if (line.startsWith("#")) {
                        continue
                    }
                    def i = line.indexOf(' ')
                    assertEquals(line.substring(i).toLong(), 0)
                    flag1 = true
                }
            }
            assertTrue(flag1)
    }
    sql """ select * from customer_ttl limit 10 """
    sleep(10000)
    getMetricsMethod.call() {
        respCode, body ->
            assertEquals("${respCode}".toString(), "200")
            String out = "${body}".toString()
            def strs = out.split('\n')
            Boolean flag = false;
            long read_at_count = 0;
            for (String line in strs) {
                if (line.contains("cached_remote_reader_s3_read")) {
                    if (line.startsWith("#")) {
                        continue
                    }
                    def i = line.indexOf(' ')
                    read_at_count = line.substring(i).toLong()
                    assertEquals(s3_read_count, read_at_count)
                    flag = true
                    break
                }
            }
            assertTrue(flag)
    }
}
