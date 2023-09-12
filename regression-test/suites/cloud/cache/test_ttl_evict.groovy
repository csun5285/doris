import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_ttl_evict") {
    def tables = [customer: 15000000]
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
    // one customer table would take about 1.3GB, the total cache size is 20GB
    // the following would take 15.6G all
    load_customer_once("customer_ttl")
    load_customer_once("customer_ttl")
    load_customer_once("customer_ttl")
    load_customer_once("customer_ttl")
    load_customer_once("customer_ttl")
    load_customer_once("customer_ttl")
    load_customer_once("customer_ttl")
    load_customer_once("customer_ttl")
    load_customer_once("customer_ttl")
    load_customer_once("customer_ttl")
    load_customer_once("customer_ttl")
    load_customer_once("customer_ttl")
    load_customer_once("customer_ttl")

    // check the ttl cache size inside file cache

    String[][] backends = sql """ show backends; """
    assertTrue(backends.size() > 0)
    String backendId;
    def backendIdToBackendIP = [:]
    def backendIdToBackendBrpcPort = [:]
    for (String[] backend in backends) {
        if (backend[8].equals("true")) {
            backendIdToBackendIP.put(backend[0], backend[1])
            backendIdToBackendBrpcPort.put(backend[0], backend[5])
        }
    }

    backendId = backendIdToBackendIP.keySet()[0]
    def getMetricsMethod = { check_func ->
        httpTest {
            endpoint backendIdToBackendIP.get(backendId) + ":" + backendIdToBackendBrpcPort.get(backendId)
            uri "/brpc_metrics"
            op "get"
            check check_func
        }
    }
    long ttl_cache_size = 0

    // wait 30s for the report
    sleep(30000)

    getMetricsMethod.call() {
        respCode, body ->
            logger.info("test ttl expired resp Code {}", "${respCode}".toString())
            assertEquals("${respCode}".toString(), "200")
            String out = "${body}".toString()
            def strs = out.split('\n')
            for (String line in strs) {
                if (line.contains("ttl_cache_size")) {
                    if (line.startsWith("#")) {
                        continue
                    }
                    def i = line.indexOf(' ')
                    ttl_cache_size = line.substring(i).toLong()
                    logger.info("test ttl evict origin cache size {}", ttl_cache_size)
                    break;
                }
            }
    }

    load_customer_once("customer")
    load_customer_once("customer")
    load_customer_once("customer")
    load_customer_once("customer")
    load_customer_once("customer")
    load_customer_once("customer")
    load_customer_once("customer")

    // 检查ttl cache有没有被驱逐
    logger.info("test if ttl cache still remains")
    getMetricsMethod.call() {
        respCode, body ->
            logger.info("test ttl expired resp Code {}", "${respCode}".toString())
            assertEquals("${respCode}".toString(), "200")
            String out = "${body}".toString()
            def strs = out.split('\n')
            boolean flag = false
            for (String line in strs) {
                if (line.contains("ttl_cache_size")) {
                    if (line.startsWith("#")) {
                        continue
                    }
                    def i = line.indexOf(' ')
                    Long ttl_cache_size2 = line.substring(i).toLong()
                    logger.info("test ttl evict after cache size {}", ttl_cache_size2)
                    assertEquals(ttl_cache_size, ttl_cache_size2)
                    flag = true
                    break;
                }
            }
            assertTrue(flag)
    }

}