import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_warm_up_table") {
    sql """ SET GLOBAL enable_auto_analyze = false """
    def getJobState = { jobId ->
         def jobStateResult = sql """  SHOW WARM UP JOB WHERE ID = ${jobId} """
         return jobStateResult[0][2]
    }

    def token = context.config.metaServiceToken;
    def instance_id = context.config.multiClusterInstance

    List<String> ipList = new ArrayList<>();
    List<String> hbPortList = new ArrayList<>()
    List<String> httpPortList = new ArrayList<>()
    List<String> brpcPortList = new ArrayList<>()
    List<String> beUniqueIdList = new ArrayList<>()

    String[] bes = context.config.multiClusterBes.split(',');
    println("the value is " + context.config.multiClusterBes);
    int num = 0
    for(String values : bes) {
        if (num++ == 2) break;
        println("the value is " + values);
        String[] beInfo = values.split(':');
        ipList.add(beInfo[0]);
        hbPortList.add(beInfo[1]);
        httpPortList.add(beInfo[2]);
        beUniqueIdList.add(beInfo[3]);
        brpcPortList.add(beInfo[4]);
    }

    println("the ip is " + ipList);
    println("the heartbeat port is " + hbPortList);
    println("the http port is " + httpPortList);
    println("the be unique id is " + beUniqueIdList);
    println("the brpc port is " + brpcPortList);

    for (unique_id : beUniqueIdList) {
        resp = get_cluster.call(unique_id);
        for (cluster : resp) {
            if (cluster.type == "COMPUTE") {
                drop_cluster.call(cluster.cluster_name, cluster.cluster_id);
            }
        }
    }
    sleep(20000)

    List<List<Object>> result  = sql "show clusters"
    assertEquals(result.size(), 0);

    add_cluster.call(beUniqueIdList[0], ipList[0], hbPortList[0],
                     "regression_cluster_name0", "lightman_cluster_id0");
    add_cluster.call(beUniqueIdList[1], ipList[1], hbPortList[1],
                     "regression_cluster_name1", "lightman_cluster_id1");
    sleep(20000)

    result  = sql "show clusters"
    assertEquals(result.size(), 2);

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

    sql "use @regression_cluster_name0"

    def table = "customer"
    sql new File("""${context.file.parent}/../ddl/${table}_delete.sql""").text
    // create table if not exists
    sql new File("""${context.file.parent}/../ddl/${table}.sql""").text
    sleep(10000)

    def load_customer_once =  { 
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

    def clearFileCache = { ip, port ->
        httpTest {
            endpoint ""
            uri ip + ":" + port + """/api/clear_file_cache"""
            op "post"
            body "{\"sync\"=\"true\"}"
        }
    }

    def getMetricsMethod = { ip, port, check_func ->
        httpTest {
            endpoint ip + ":" + port
            uri "/brpc_metrics"
            op "get"
            check check_func
        }
    }

    clearFileCache.call(ipList[0], httpPortList[0]);
    clearFileCache.call(ipList[1], httpPortList[1]);
    
    load_customer_once()
    load_customer_once()
    load_customer_once()
    load_customer_once()
    load_customer_once()
    load_customer_once()
    load_customer_once()
    load_customer_once()
    load_customer_once()
    load_customer_once()
    load_customer_once()
    load_customer_once()
    load_customer_once()

    def jobId = sql "warm up cluster regression_cluster_name1 with table customer;"
    try {
        sql "warm up cluster regression_cluster_name1 with table customer;"
        assertTrue(false)
    } catch (Exception e) {
        assertTrue(true)
    }
    int retryTime = 120
    int i = 0
    for (; i < retryTime; i++) {
        sleep(1000)
        def status = getJobState(jobId[0][0])
        logger.info(status)
        if (status.equals("CANCELLED")) {
            assertTrue(false);
        }
        if (status.equals("FINISHED")) {
            break;
        }
    }
    if (i == retryTime) {
        sql "cancel warm up job where id = ${jobId[0][0]}"
        assertTrue(false);
    }

    long s3_read_count = 0
    getMetricsMethod.call(ipList[1], brpcPortList[1]) {
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
                    def j = line.indexOf(' ')
                    s3_read_count = line.substring(j).toLong()
                    flag = true
                    break
                }
            }
            assertTrue(flag)
    }
    sql "use @regression_cluster_name1"
    qt_sql2 """
    select count(*) from ${table};
    """

    getMetricsMethod.call(ipList[1], brpcPortList[1]) {
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
                    def j = line.indexOf(' ')
                    assertEquals(s3_read_count, line.substring(j).toLong())
                    flag = true
                    break
                }
            }
            assertTrue(flag)
    }

    try {
        sql "warm up cluster regression_cluster_name2 with table customer;"
        assertTrue(false)
    } catch (Exception e) {
        assertTrue(true)
    }

    sql new File("""${context.file.parent}/../ddl/${table}_delete.sql""").text
    try {
        sql "warm up cluster regression_cluster_name1 with table customer;"
        assertTrue(false)
    } catch (Exception e) {
        assertTrue(true)
    }
}
