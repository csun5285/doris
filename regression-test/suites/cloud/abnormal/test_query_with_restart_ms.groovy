suite("test_query_with_restart_ms", "abnormal") {
    checkClusterDir();
    // create table
    def tableName = 'test_query_with_restart_ms'
    def feDir = "${context.config.clusterDir}/fe"
    def beDir = "${context.config.clusterDir}/cluster0/be"
    def msDir = "${context.config.clusterDir}/meta-service"

    String nodeIp = context.config.feHttpAddress.split(':')[0].trim()

    // by default, we need deploy fe/be/ms in the same node
    def clusterMap = [
        fe : [[ ip : nodeIp, path : feDir]],
        be : [[ ip : nodeIp, path: beDir]],
        ms : [[ ip : nodeIp, path: msDir]]
    ]
    logger.info("clusterMap:${clusterMap}");
    checkProcessAlive(clusterMap["fe"][0]["ip"], "fe", clusterMap["fe"][0]["path"]);
    checkProcessAlive(clusterMap["be"][0]["ip"], "be", clusterMap["be"][0]["path"]);
    checkProcessAlive(clusterMap["ms"][0]["ip"], "ms", clusterMap["ms"][0]["path"]);

    sql """ DROP TABLE IF EXISTS ${tableName} FORCE"""
    sql """
        CREATE TABLE `${tableName}` (
        id INT,
        name varchar(20),
        score INT
        )
        DUPLICATE KEY(id, name)
        DISTRIBUTED BY HASH(id) BUCKETS 3;
    """

    sql """insert into `${tableName}` values (1, "selectdb", 100)"""
    sql """insert into `${tableName}` values (2, "selectdb", 100)"""
    sql """insert into `${tableName}` values (3, "selectdb", 100)"""
    def result2 = connect('root') {
        // execute sql with root user
        sql 'select 50 + 50'
    }

    lazyCheckThread {
        def result
        int tryTimes = 60
        while (tryTimes-- > 0) {
            result = try_sql "select count(*) from ${tableName}"
            assertEquals(result[0][0], 3)
            logger.info("query result: {}, times {}", result, tryTimes);
            sleep(1000)
        }
    }

    sleep(20 * 1000)
    // restart ms
    restartProcess(clusterMap["ms"][0]["ip"], "ms", clusterMap["ms"][0]["path"])
    resetConnection()
    sleep(40 * 1000)
    
    sql """ DROP TABLE IF EXISTS ${tableName} FORCE"""

    lazyCheckFutures.clear()
}
