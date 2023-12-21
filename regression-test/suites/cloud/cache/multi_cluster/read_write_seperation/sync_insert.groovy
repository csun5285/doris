import org.codehaus.groovy.runtime.IOGroovyMethods

suite("sync_insert") {
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

    sql "use @regression_cluster_name0"

    def table1 = "test_dup_tab_basic_int_tab_nullable"
    sql """ drop table if exists ${table1} """
    sql """ set enable_multi_cluster_sync_load = true """

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
    sleep(10000) // wait for rebalance
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
    sleep(30000)

    sql "use @regression_cluster_name1"

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
                    def i = line.indexOf(' ')
                    s3_read_count = line.substring(i).toLong()
                    flag = true
                    break
                }
            }
            assertTrue(flag)
    }

    qt_sql1 "select siteid,citycode,userid,pv from ${table1} where siteid = 21 "

    qt_sql2 "select siteid,citycode,userid,pv from ${table1} where siteid is null "

    qt_sql3 "select siteid,citycode,userid,pv from ${table1} where siteid is not null order by siteid,citycode,userid,pv"

    qt_sql4 "select siteid,citycode,userid,pv from ${table1} where citycode is null "

    qt_sql5 "select siteid,citycode,userid,pv from ${table1} where citycode is not null order by siteid,citycode,userid,pv"

    qt_sql6 "select siteid from ${table1} order by siteid"

    qt_sql7 "select citycode from ${table1} order by citycode"

    qt_sql8 "select siteid,citycode from ${table1} order by siteid,citycode"

    qt_sql9 "select userid, citycode from ${table1} order by userid,citycode"

    qt_sql10 "select siteid from ${table1} where siteid!=13 order by siteid"

    qt_sql11 "select siteid from ${table1} where siteid=13"

    qt_sql12 "select citycode from ${table1} where citycode=18"

    qt_sql13 "select citycode from ${table1} where citycode!=18 order by citycode"

    qt_sql14 "select siteid,citycode from ${table1} where siteid=13 order by siteid,citycode"

    qt_sql15 "select citycode,siteid from ${table1} where siteid=13 order by citycode,siteid"

    qt_sql16 "select citycode,siteid from ${table1} where siteid!=13 order by citycode,siteid"

    qt_sql17 "select siteid from ${table1} where siteid!=13 order by siteid"

    qt_sql18 "select siteid,citycode from ${table1} where citycode=18 order by siteid,citycode"

    qt_sql19 "select citycode from ${table1} where citycode=18 order by citycode"


    qt_sql20 "select siteid,citycode from ${table1} where citycode!=18 order by siteid,citycode"

    qt_sql21 "select citycode,siteid from ${table1} where citycode!=18 order by citycode,siteid"

    qt_sql22 "select citycode from ${table1} where citycode!=18 order by citycode"

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
                    def i = line.indexOf(' ')
                    assertEquals(s3_read_count, line.substring(i).toLong())
                    flag = true
                    break
                }
            }
            assertTrue(flag)
    }
    sql "drop table if exists ${table1}"
}
