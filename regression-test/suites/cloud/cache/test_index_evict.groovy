import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_index_evict") {
    sql """ use @regression_cluster_name1 """
    def backends = sql_return_maparray "show backends;"
    String backendId;
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
    def clea_cache_url = backendIdToBackendIP.get(backendId) + ":" + backendIdToBackendHttpPort.get(backendId) + """/api/clear_file_cache"""
    logger.info(clea_cache_url)
    def clearFileCache = { check_func ->
        httpTest {
            endpoint ""
            uri clea_cache_url
            op "post"
            body "{\"sync\"=\"true\"}"
            check check_func
        }
    }

    def update_config_url = backendIdToBackendIP.get(backendId) + ":" + backendIdToBackendHttpPort.get(backendId) + """/api/update_config?file_cache_convert_all_write_data_into_index=true"""
    def updateConfig = { check_func ->
        httpTest {
            endpoint ""
            uri update_config_url
            op "post"
            body ""
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
    // set fe configuration
    sql "ADMIN SET FRONTEND CONFIG ('max_bytes_per_broker_scanner' = '161061273600')"
    sql new File("""${context.file.parent}/ddl/customer_delete.sql""").text

    def load_customer_once =  { String table ->
        def uniqueID = Math.abs(UUID.randomUUID().hashCode()).toString()
        // def table = "customer"
        // create table if not exists
        sql new File("""${context.file.parent}/ddl/${table}.sql""").text
        def loadLabel = table + "_" + uniqueID
        // load data from cos
        def loadSql = new File("""${context.file.parent}/ddl/${table}_load.sql""").text.replaceAll("\\\$\\{s3BucketName\\}", s3BucketName)
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

    updateConfig.call() {
        respCode, body -> {}
    }

    // one customer table would take about 1.3GB, the total cache size is 20GB
    // the following would take 19.5G all
    load_customer_once("customer")
    load_customer_once("customer")
    load_customer_once("customer")
    load_customer_once("customer")
    load_customer_once("customer")
    load_customer_once("customer")
    load_customer_once("customer")
    load_customer_once("customer")
    load_customer_once("customer")
    load_customer_once("customer")
    load_customer_once("customer")
    load_customer_once("customer")
    load_customer_once("customer")
    load_customer_once("customer")
    load_customer_once("customer")

    long index_queue_cache_size = 0
    sleep(30000)
    getMetricsMethod.call() {
        respCode, body ->
            assertEquals("${respCode}".toString(), "200")
            String out = "${body}".toString()
            def strs = out.split('\n')
            for (String line in strs) {
                if (line.startsWith("#")) {
                    continue
                }
                if (line.contains("index_queue_cache_size")) {
                    def i = line.indexOf(' ')
                    index_queue_cache_size = line.substring(i).toLong()
                }
            }
    }

    load_customer_once("customer")

    sleep(30000)
    getMetricsMethod.call() {
        respCode, body ->
            assertEquals("${respCode}".toString(), "200")
            String out = "${body}".toString()
            def strs = out.split('\n')
            for (String line in strs) {
                if (line.startsWith("#")) {
                    continue
                }
                if (line.contains("index_queue_cache_size")) {
                    def i = line.indexOf(' ')
                    cur_index_queue_cache_size = line.substring(i).toLong()
                    assertTrue(cur_index_queue_cache_size > 20937965568)
                }
            }
    }

    update_config_url = backendIdToBackendIP.get(backendId) + ":" + backendIdToBackendHttpPort.get(backendId) + """/api/update_config?file_cache_convert_all_write_data_into_index=false"""
    updateConfig.call() {
        respCode, body -> {}
    }
}
