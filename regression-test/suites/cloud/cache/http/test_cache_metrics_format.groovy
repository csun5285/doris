import org.codehaus.groovy.runtime.IOGroovyMethods

// 1. call clear http
// 2. load customer once
// 3. check ttl size and total size whether equal 
// 4. call async clear http 
// 5. check total size whether equal to zero
suite("test_cache_metrics_format") {
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
        if (backend.Alive.equals("true") && cloud_tag.cloud_cluster_name.contains("regression_cluster_name1")) {
            backendIdToBackendIP.put(backend.BackendId, host)
            backendIdToBackendHttpPort.put(backend.BackendId, backend.HttpPort)
            backendIdToBackendBrpcPort.put(backend.BackendId, backend.BrpcPort)
        }
    }
    assertEquals(backendIdToBackendIP.size(), 1)
    backendId = backendIdToBackendIP.keySet()[0]

    def getMetricsMethod = { check_func ->
        httpTest {
            endpoint backendIdToBackendIP.get(backendId) + ":" + backendIdToBackendHttpPort.get(backendId)
            uri "/metrics"
            op "get"
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

    load_customer_once("customer_ttl")
    for (int k = 0; k < 100; k++) {
        sql " select count(*) from customer_ttl "
    }
    sleep(10000)
    def regex = ~/doris_be_num_io_bytes_read_total\{table_id="\d+"\} \d+/
    getMetricsMethod.call() {
        respCode, body ->
            assertEquals("${respCode}".toString(), "200")
            String out = "${body}".toString()
            def strs = out.split('\n')
            Boolean flag1 = false;
            for (String line in strs) {
                if (flag1) break;
                if (line.contains("doris_be_num_io_bytes_read_total")) {
                    if (line.startsWith("#")) {
                        continue
                    }
                    assertTrue(line ==~ regex)
                    flag1 = true
                }
            }
            assertTrue(flag1)
    }

    sql new File("""${context.file.parent}/../ddl/customer_ttl_delete.sql""").text
}
