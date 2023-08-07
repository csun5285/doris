import org.codehaus.groovy.runtime.IOGroovyMethods

suite("sync_insert") {
    def token = context.config.metaServiceToken;
    def instance_id = context.config.multiClusterInstance

    List<String> ipList = new ArrayList<>();
    List<String> hbPortList = new ArrayList<>()
    List<String> httpPortList = new ArrayList<>()
    List<String> brpcPortList = new ArrayList<>()
    List<String> beUniqueIdList = new ArrayList<>()

    String[] bes = context.config.multiClusterBes.split(',');
    assertEquals(bes.length, 2)
    println("the value is " + context.config.multiClusterBes);
    for(String values : bes) {
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

    def getCurCacheSize = {
        backendIdToCacheSize = [:]
        for (int i = 0; i < ipList.size(); i++) {
            StringBuilder sb = new StringBuilder();
            sb.append("curl http://")
            sb.append(ipList[i])
            sb.append(":")
            sb.append(brpcPortList[i])
            sb.append("/vars/*file_cache_cache_size")
            String command = sb.toString()
            logger.info(command);
            process = command.execute()
            code = process.waitFor()
            err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
            out = process.getText()
            logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            String[] str = out.split(':')
            assertEquals(str.length, 2)
            logger.info(str[1].trim())
            backendIdToCacheSize.put(beUniqueIdList[i], Long.parseLong(str[1].trim()))
        }
        return backendIdToCacheSize
    }
    def originalSize = getCurCacheSize();

    sql "use @regression_cluster_name0"

    def table1 = "test_dup_tab_basic_int_tab_nullable"

    sql "drop table if exists ${table1}"

    sql """ set enable_multi_cluster_sync_load=true """

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
    sleep(10000)
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
    def afterLoadSize = getCurCacheSize();

    assertEquals(afterLoadSize.get(beUniqueIdList[0]) - originalSize.get(beUniqueIdList[0]),
                afterLoadSize.get(beUniqueIdList[1]) - originalSize.get(beUniqueIdList[1]));

    sql "use @regression_cluster_name1"

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

}
