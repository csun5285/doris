import org.codehaus.groovy.runtime.IOGroovyMethods

suite("file_size_test") {
    sql """ use @regression_cluster_name1 """
    def table1 = "test_file_size"

    sql "drop table if exists ${table1}"

    sql """
CREATE TABLE IF NOT EXISTS `${table1}` (
  `siteid` int(11) NULL COMMENT "",
  `citycode` int(11) NULL COMMENT "",
  `userid` int(11) NULL COMMENT "",
  `pv` int(11) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`siteid`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`siteid`) BUCKETS 1
"""

    sql """insert into ${table1} values
        (9,10,11,12),
        (9,10,11,12),
        (21,null,23,null),
        (1,2,3,4),
        (1,2,3,4),
        (13,14,15,16),
        (13,21,22,16),
        (13,14,15,16),
        (13,21,22,16),
        (17,18,19,20),
        (17,18,19,20),
        (null,21,null,23),
        (22,null,24,25),
        (26,27,null,29),
        (5,6,7,8),
        (5,6,7,8)
"""

    long origin_count = 0;
    def backends = sql_return_maparray "show backends;"
    assertTrue(backends.size() > 0)
    String backendId;
    def backendIdToBackendIP = [:]
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
            backendIdToBackendBrpcPort.put(backend.BackendId, backend.BrpcPort)
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
    getMetricsMethod.call() {
        respCode, body ->
            logger.info("test file size resp Code {}", "${respCode}".toString())
            assertEquals("${respCode}".toString(), "200")
            String out = "${body}".toString()
            def strs = out.split('\n')
            Boolean flag = false;
            for (String line in strs) {
                if (line.contains("s3_file_system_file_size")) {
                    if (line.startsWith("#")) {
                        continue
                    }
                    def i = line.indexOf(' ')
                    origin_count = line.substring(i).toLong()
                    logger.info("test file size origin count {}", origin_count)
                }
            }
    }

    sql """
    select count(*) from ${table1}
    """

    logger.info("test if file size count is zero")
    getMetricsMethod.call() {
        respCode, body ->
            logger.info("test file size resp Code {}", "${respCode}".toString())
            assertEquals("${respCode}".toString(), "200")
            String out = "${body}".toString()
            def strs = out.split('\n')
            Boolean flag = false;
            for (String line in strs) {
                if (line.contains("s3_file_system_file_size")) {
                    if (line.startsWith("#")) {
                        continue
                    }
                    def i = line.indexOf(' ')
                    def count = line.substring(i).toLong()
                    logger.info("test file size count {}", count)
                    assertEquals(count, origin_count)
                    flag = true;
                    break;
                }
            }
            assertTrue(flag);
    }
    sql "drop table if exists ${table1}"
}
