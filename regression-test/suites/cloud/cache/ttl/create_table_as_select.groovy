import org.codehaus.groovy.runtime.IOGroovyMethods

suite("create_table_as_select") {
    sql """ use @regression_cluster_name1 """
    String[][] backends = sql """ show backends """
    String backendId;
    def backendIdToBackendIP = [:]
    def backendIdToBackendHttpPort = [:]
    def backendIdToBackendBrpcPort = [:]
    for (String[] backend in backends) {
        if (backend[8].equals("true") && backend[18].contains("regression_cluster_name1")) {
            backendIdToBackendIP.put(backend[0], backend[1])
            backendIdToBackendHttpPort.put(backend[0], backend[4])
            backendIdToBackendBrpcPort.put(backend[0], backend[5])
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
    sql """ DROP TABLE IF EXISTS customer_ttl_as_select """
    sql """
        CREATE TABLE IF NOT EXISTS customer_ttl (
        C_CUSTKEY     INTEGER NOT NULL,
        C_NAME        VARCHAR(25) NOT NULL,
        C_ADDRESS     VARCHAR(40) NOT NULL,
        C_NATIONKEY   INTEGER NOT NULL,
        C_PHONE       CHAR(15) NOT NULL,
        C_ACCTBAL     DECIMAL(15,2)   NOT NULL,
        C_MKTSEGMENT  CHAR(10) NOT NULL,
        C_COMMENT     VARCHAR(117) NOT NULL
        )
        DUPLICATE KEY(C_CUSTKEY, C_NAME)
        DISTRIBUTED BY HASH(C_CUSTKEY) BUCKETS 32
        PROPERTIES("file_cache_ttl_seconds"="180")
    """

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

    def uniqueID = Math.abs(UUID.randomUUID().hashCode()).toString()
    def loadLabel = "customer_ttl_load_" + uniqueID
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
    sql """ CREATE TABLE customer_ttl_as_select DUPLICATE KEY(C_CUSTKEY, C_NAME) 
            DISTRIBUTED BY HASH(C_CUSTKEY) BUCKETS 32 
            PROPERTIES("file_cache_ttl_seconds"="120") as select * from customer_ttl"""

    sleep(30000) // 30s
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
                    long cur_cache_size = line.substring(i).toLong();
                    assertTrue(Math.abs(2* total_cache_size - cur_cache_size) < 10000)
                    total_cache_size = cur_cache_size
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

    sql """ select * from customer_ttl_as_select limit 10 """
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
    sleep(150000)
    getMetricsMethod.call() {
        respCode, body ->
            assertEquals("${respCode}".toString(), "200")
            String out = "${body}".toString()
            def strs = out.split('\n')
            Boolean flag1 = false;
            Boolean flag2 = false;
            for (String line in strs) {
                if (flag1 && flag2) break;
                if (line.contains("ttl_cache_size")) {
                    if (line.startsWith("#")) {
                        continue
                    }
                    def i = line.indexOf(' ')
                    assertEquals(line.substring(i).toLong(), 0)
                    flag1 = true
                }
                if (line.contains("file_cache_cache_size")) {
                    if (line.startsWith("#")) {
                        continue
                    }
                    def i = line.indexOf(' ')
                    assertEquals(total_cache_size, line.substring(i).toLong())
                    flag2 = true
                }
            }
            assertTrue(flag1 && flag2)
    }
    sql """ select * from customer_ttl_as_select limit 10 """
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
    sql new File("""${context.file.parent}/../ddl/customer_ttl_delete.sql""").text
    sql """ DROP TABLE IF EXISTS customer_ttl_as_select """
}
