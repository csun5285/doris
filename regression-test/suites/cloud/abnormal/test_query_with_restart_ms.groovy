suite("test_query_with_restart_ms", "abnormal") {
    def clusterMap = loadClusterMap(Config.clusterFile)
    // create table
    def tableName = 'test_query_with_restart_ms'

    logger.debug("clusterMap:${clusterMap}");
    checkProcessAlive(clusterMap["fe"]["node"][0]["ip"], "fe", clusterMap["fe"]["node"][0]["install_path"])
    checkProcessAlive(clusterMap["be"]["node"][0]["ip"], "be", clusterMap["be"]["node"][0]["install_path"])

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
    restartProcess(clusterMap["meta_service"]["node"][0]["ip"], "be", clusterMap["meta_service"]["node"][0]["install_path"])
    resetConnection()
    sleep(40 * 1000)
    
    sql """ DROP TABLE IF EXISTS ${tableName} FORCE"""

    lazyCheckFutures.clear()
}
