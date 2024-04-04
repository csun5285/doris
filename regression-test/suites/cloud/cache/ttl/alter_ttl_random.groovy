import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_ttl_random") {
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
    sql (new File("""${context.file.parent}/../ddl/customer_ttl.sql""").text + ttlProperties)
    def load_customer_once =  { String table ->
        def uniqueID = Math.abs(UUID.randomUUID().hashCode()).toString()
        def loadLabel = table + "_" + uniqueID
        // load data from cos
        def loadSql = new File("""${context.file.parent}/../ddl/customer_ttl_load.sql""").text.replaceAll("\\\$\\{s3BucketName\\}", s3BucketName)
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

    clearFileCache.call() {
        respCode, body -> {}
    }

    load_customer_once("customer_ttl")

    Random random = new Random()
    for (int j = 0; j < 40; j++) {
        long number = random.nextLong() % 10l;
        if (number < 5) {
            sql """ select * from customer_ttl limit 10"""
        } else if (number >= 5 && number < 8) {
            load_customer_once("customer_ttl")
        } else {
            def alterTtlSeconds = random.nextLong()
            if (alterTtlSeconds < 0) {
                alterTtlSeconds *= -1
            }
            sql """ ALTER TABLE customer_ttl SET ("file_cache_ttl_seconds"="${alterTtlSeconds}") """
            sleep(40000)
        }
    }
}
