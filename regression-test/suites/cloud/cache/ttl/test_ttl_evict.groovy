import org.codehaus.groovy.runtime.IOGroovyMethods

// 1. clear file cache
// 2. load 19.5G ttl table data into cache (cache capacity is 20G)
// 3. check ttl size and total size
// 4. load 1.3G normal table data into cache (just little datas will be cached)
// 5. select some data from normal table, and it will read from s3
// 6. select some data from ttl table, and it will not read from s3
// 7. wait for ttl data timeout
// 8. drop the normal table and load again. All normal table datas will be cached this time.
// 9. select some data from normal table to check whether all datas are cached
suite("test_ttl_evict") {
    sql """ use @regression_cluster_name1 """
    sql """ set global enable_auto_analyze = false; """
    def ttlProperties = """ PROPERTIES("file_cache_ttl_seconds"="420") """
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

    def getMetricsMethod = { check_func ->
        httpTest {
            endpoint backendIdToBackendIP.get(backendId) + ":" + backendIdToBackendBrpcPort.get(backendId)
            uri "/brpc_metrics"
            op "get"
            check check_func
        }
    }

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
    sql new File("""${context.file.parent}/../ddl/customer_delete.sql""").text
    def load_customer_ttl_once =  { String table ->
        def uniqueID = Math.abs(UUID.randomUUID().hashCode()).toString()
        // def table = "customer"
        // create table if not exists
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

    def load_customer_once =  { String table ->
        def uniqueID = Math.abs(UUID.randomUUID().hashCode()).toString()
        // def table = "customer"
        // create table if not exists
        sql new File("""${context.file.parent}/../ddl/${table}.sql""").text
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

    clearFileCache.call() {
        respCode, body -> {}
    }

    // one customer table would take about 1.3GB, the total cache size is 20GB
    // the following would take 19.5G all
    load_customer_ttl_once("customer_ttl")
    load_customer_ttl_once("customer_ttl")
    load_customer_ttl_once("customer_ttl")
    load_customer_ttl_once("customer_ttl")
    load_customer_ttl_once("customer_ttl")
    load_customer_ttl_once("customer_ttl")
    load_customer_ttl_once("customer_ttl")
    load_customer_ttl_once("customer_ttl")
    load_customer_ttl_once("customer_ttl")
    load_customer_ttl_once("customer_ttl")
    load_customer_ttl_once("customer_ttl")
    load_customer_ttl_once("customer_ttl")
    load_customer_ttl_once("customer_ttl")
    load_customer_ttl_once("customer_ttl")
    load_customer_ttl_once("customer_ttl")

    // The max ttl cache size is 90% cache capacity
    long total_cache_size = 0
    sleep(30000)
    getMetricsMethod.call() {
        respCode, body ->
            assertEquals("${respCode}".toString(), "200")
            String out = "${body}".toString()
            def strs = out.split('\n')
            Boolean flag1 = false;
            long ttl_cache_size = 0;
            for (String line in strs) {
                if (flag1) break;
                if (line.contains("ttl_cache_size")) {
                    if (line.startsWith("#")) {
                        continue
                    }
                    def i = line.indexOf(' ')
                    ttl_cache_size = line.substring(i).toLong()
                    logger.info("current ttl_cache_size " + ttl_cache_size);
                    assertTrue(ttl_cache_size <= 19327352832)
                    flag1 = true
                }
            }
            assertTrue(flag1)
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

    // will not cache all data
    load_customer_once("customer")
    logger.info("current s3 read count " + s3_read_count);

    sql """ select * from customer limit 10 """
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
                    logger.info("new s3 read count " + read_at_count);
                    assertEquals(s3_read_count, read_at_count)
                    s3_read_count = read_at_count;
                    flag = true
                    break
                }
            }
            assertTrue(flag)
    }

    for (int j = 0; j < 60; j++) {
        sleep(10000)
        boolean flag = false;
        getMetricsMethod.call() {
        respCode, body ->
            assertEquals("${respCode}".toString(), "200")
            String out = "${body}".toString()
            def strs = out.split('\n')
            long ttl_cache_size = 0;
            for (String line in strs) {
                if (line.contains("ttl_cache_size")) {
                    if (line.startsWith("#")) {
                        continue
                    }
                    def i = line.indexOf(' ')
                    ttl_cache_size = line.substring(i).toLong()
                    if (ttl_cache_size == 0) {
                        flag = true
                    }
                    break
                }
            }
        }
        if (flag) break;
    }

    sql new File("""${context.file.parent}/../ddl/customer_delete.sql""").text
}
