import org.codehaus.groovy.runtime.IOGroovyMethods
import groovy.json.JsonOutput

suite("test_auto_suspend_fe_metrics") {
        // Parse url
    String url = getServerPrepareJdbcUrl(context.config.jdbcUrl, "noexist")

    def check_is_master_fe = {
        // check if executed on master fe
        // observer fe will forward the insert statements to master and forward does not support prepare statement
        def fes = sql_return_maparray "show frontends"
        logger.info("frontends: ${fes}")
        def is_master_fe = true
        for (def fe : fes) {
            if (url.contains(fe.Host + ":")) {
                if (fe.IsMaster == "false") {
                    is_master_fe = false
                }
                break
            }
        }
        logger.info("is master fe: ${is_master_fe}")
        return is_master_fe
    }
    
    def is_master_fe = check_is_master_fe()
    if (!is_master_fe) {
        logger.info("not master not run")
        return
    }

    def getRestApi = {
        StringBuilder strBuilder = new StringBuilder()
        strBuilder.append("""curl -u """ + context.config.feHttpUser + ":" + context.config.feHttpPassword)
        strBuilder.append(""" http://""" + context.config.feHttpAddress + """/rest/v2/manager/cluster/cluster_info/cloud_cluster_status""")

        String command = strBuilder.toString()
        logger.info("get rest cluster info command=" + command)
        def process = command.toString().execute()
        def code = process.waitFor()
        def err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
        def out = process.getText()
        logger.info("Request FE Resp: code=" + code + ", out=" + out + ", err=" + err)
        assertEquals(code, 0)
        def jsonResp = parseJson(out)
        // assert cluster id > 0, must have cluster
        assertTrue(jsonResp.data.size() > 0)
        return clusterInfo = jsonResp.data
    }

    // case 1, after be start, lastFragmentUpdateTime must > 0
    // so bes must alive
    def ret = getRestApi.call()
    def beMaps = [:]
    for (final def r in ret) {
        def clusterId = r.key
        def clusterInfos = r.value
        for (final def v in clusterInfos) {
            beMaps.put(v.host + ":" + v.heartbeatPort, v.lastFragmentUpdateTime)
            assertTrue(v.lastFragmentUpdateTime > 0)
        }
    }
    logger.info("result1 ret={}, beMaps={}", ret, beMaps)
    
    def result = sql_return_maparray "SHOW CLUSTERS;"
    def clusterName1 = null
    for (final def r in result) {
        clusterName1 = r.cluster
        break;
    }
    assertNotNull(clusterName1)
    def tbl = 'test_auto_suspend_fe_metrics_tbl'
    sql """ DROP TABLE IF EXISTS ${tbl} """

    sql """
        CREATE TABLE ${tbl} (
        `k1` int(11) NULL,
        `k2` int(11) NULL
        )
        DUPLICATE KEY(`k1`, `k2`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 1
        PROPERTIES (
        "replication_num"="1"
        );
    """

    sql """USE @${clusterName1}"""

    def checkFunc = { map ->
        sleep(10 * 1000)
        ret = getRestApi.call() 
        for (final def r in ret) {
            def clusterId = r.key
            def clusterInfos = r.value
            for (final def v in clusterInfos) {
                assertTrue(v.lastFragmentUpdateTime >= map.get(v.host + ":" + v.heartbeatPort))
                map.put(v.host + ":" + v.heartbeatPort, v.lastFragmentUpdateTime)
            }
        }
    }

    // case2, pipeline off, insert, select
    sql """set enable_pipeline_engine = false"""
    sql """insert into ${tbl} values (1, 10)"""
    checkFunc(beMaps)
    logger.info("result2, beMaps={}", beMaps)
    sql """ select * from ${tbl}"""
    checkFunc(beMaps)
    logger.info("result3, beMaps={}", beMaps)

    // case3, pipeline on, insert, select
    sql """set enable_pipeline_engine = true"""
    sql """insert into ${tbl} values (1, 10)"""
    checkFunc(beMaps)
    logger.info("result4, beMaps={}", beMaps)
    sql """ select * from ${tbl}"""
    checkFunc(beMaps)
    logger.info("result5, beMaps={}", beMaps)

    // case4, schema change
    sql """ALTER TABLE ${tbl} ADD COLUMN new_col INT KEY DEFAULT "0" AFTER k2"""
    checkFunc(beMaps)
}