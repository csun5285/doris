import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_warmup_show_stmt_3") {
    def table = "customer"
    sql new File("""${context.file.parent}/../ddl/${table}_delete.sql""").text
    sql new File("""${context.file.parent}/../ddl/supplier_delete.sql""").text
    // create table if not exists
    sql new File("""${context.file.parent}/../ddl/${table}.sql""").text

    sql """ TRUNCATE TABLE __internal_schema.selectdb_cache_hotspot; """
    sleep(30000)

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
                     "regression_cluster_name0", "regression_cluster_id0");
    add_cluster.call(beUniqueIdList[1], ipList[1], hbPortList[1],
                     "regression_cluster_name1", "regression_cluster_id1");
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
    
    sql "use @regression_cluster_name0"
    load_customer_once()
    sql "select count(*) from customer"
    def thread = Thread.start {
        Random random = new Random()
        for (int i = 0; i < 40; i++) {
            long number = random.nextLong() % 10l;
            if (number < 8) {
                sql "select count(*) from customer"
            } else {
                load_customer_once()
            }
            sleep(5000)
        }
    }
    for (int i = 0; i < 3; i++) {
        sleep(40000)
        result = sql """ show cache hotspot "/regression_cluster_name0/regression_test_cloud_multi_cluster_warm_up_hotspot.customer" """
        assertTrue(result.size() > 0);
    }
    thread.join()
    sleep(40000)
    result = sql """ show cache hotspot "/regression_cluster_name0/regression_test_cloud_multi_cluster_warm_up_hotspot.customer" """
    assertTrue(result.size() > 0);
}
