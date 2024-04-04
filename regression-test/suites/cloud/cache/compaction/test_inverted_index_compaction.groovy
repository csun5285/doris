import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_inverted_index_compaction"){
    sql """ use @regression_cluster_name1 """
    //BackendId,Cluster,IP,HeartbeatPort,BePort,HttpPort,BrpcPort,LastStartTime,LastHeartbeat,Alive,SystemDecommissioned,ClusterDecommissioned,TabletNum,DataUsedCapacity,AvailCapacity,TotalCapacity,UsedPct,MaxDiskUsedPct,Tag,ErrMsg,Version,Status
    //doris show backends: BackendId  Host  HeartbeatPort  BePort  HttpPort  BrpcPort  ArrowFlightSqlPort  LastStartTime  LastHeartbeat  Alive  SystemDecommissioned  TabletNum  DataUsedCapacity  TrashUsedCapcacity  AvailCapacity  TotalCapacity  UsedPct  MaxDiskUsedPct  RemoteUsedCapacity  Tag  ErrMsg  Version  Status  HeartbeatFailureCounter  NodeRole
    def backends = sql_return_maparray "show backends;"
    assertTrue(backends.size() > 0)
    String backend_id;
    def backendIdToBackendIP = [:]
    def backendIdToBackendHttpPort = [:]
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
            backendIdToBackendHttpPort.put(backend.BackendId, backend.HttpPort)
            backendIdToBackendBrpcPort.put(backend.BackendId, backend.BrpcPort)
        }
    }
    String backendId = backendIdToBackendIP.keySet()[0]
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

    clearFileCache.call() {
        respCode, body -> {}
    }

    def getCurCacheSize = {
        backendIdToCacheSize = [:]
        StringBuilder sb = new StringBuilder();
        sb.append("curl http://")
        sb.append(backendIdToBackendIP.get(backendId))
        sb.append(":")
        sb.append(backendIdToBackendBrpcPort.get(backendId))
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
        backendIdToCacheSize.put(backendId, Long.parseLong(str[1].trim()))
        return backendIdToCacheSize
    }

    def indexTbName1 = "test_inverted_index_compcation"

    sql "DROP TABLE IF EXISTS ${indexTbName1}"

    // create table with different index
        sql """
            CREATE TABLE IF NOT EXISTS ${indexTbName1} (
                name varchar(50),
                age int NOT NULL,
                grade varchar(30) NOT NULL,
                registDate datetime NULL,
                studentInfo char(100),
                tearchComment string,
                selfComment text,
                fatherName varchar(50),
                matherName varchar(50),
                otherinfo varchar(100),
                INDEX name_idx(name) USING INVERTED COMMENT 'name index',
                INDEX age_idx(age) USING INVERTED COMMENT 'age index',
                INDEX grade_idx(grade) USING INVERTED PROPERTIES("parser"="none") COMMENT 'grade index',
                INDEX tearchComment_idx(tearchComment) USING INVERTED PROPERTIES("parser"="english") COMMENT 'tearchComment index',
                INDEX studentInfo_idx(studentInfo) USING INVERTED PROPERTIES("parser"="standard") COMMENT 'studentInfo index',
                INDEX selfComment_idx(selfComment) USING INVERTED PROPERTIES("parser"="standard") COMMENT 'studentInfo index',
                INDEX fatherName_idx(fatherName) USING INVERTED PROPERTIES("parser"="standard") COMMENT ' fatherName index'
            )
            DUPLICATE KEY(`name`)
            DISTRIBUTED BY HASH(`name`) BUCKETS 1
            ;
    """

    for (int i = 0; i < 5; i++) {
        // insert data
        sql """ insert into ${indexTbName1} VALUES
            ("zhang san", 10, "grade 5", "2017-10-01", "tall:120cm, weight: 35kg, hobbies: sing, dancing", "Like cultural and recreational activities", "Class activists", "zhang yi", "chen san", "buy dancing book"),
            ("zhang san yi", 11, "grade 5", "2017-10-01", "tall:120cm, weight: 35kg, hobbies: reading book", "A quiet little boy", "learn makes me happy", "zhang yi", "chen san", "buy"),
            ("li si", 9, "grade 4", "2018-10-01",  "tall:100cm, weight: 30kg, hobbies: playing ball", "A naughty boy", "i just want go outside", "li er", "wan jiu", ""),
            ("san zhang", 10, "grade 5", "2017-10-01", "tall:100cm, weight: 30kg, hobbies:", "", "", "", "", ""),
            ("li sisi", 11, "grade 6", "2016-10-01", "tall:150cm, weight: 40kg, hobbies: sing, dancing, running", "good at handiwork and beaty", "", "li ba", "li liuliu", "")
        """
    }

    sleep(30000);
    def backendIdToAfterLoadCacheSize = getCurCacheSize()
    for (String[] backend in backends) {
        logger.info(backend[0] + " size: " + backendIdToAfterLoadCacheSize.get(backend[0]))
    }

    qt_sql1 """ select count(*) from ${indexTbName1} """

    String[][] tablets = sql """ show tablets from ${indexTbName1}; """

    for (String[] tablet in tablets) {
        String tablet_id = tablet[0]
        backend_id = tablet[2]
        StringBuilder sb = new StringBuilder();
        sb.append("curl -X POST http://")
        sb.append(backendIdToBackendIP.get(backend_id))
        sb.append(":")
        sb.append(backendIdToBackendHttpPort.get(backend_id))
        sb.append("/api/compaction/run?tablet_id=")
        sb.append(tablet_id)
        sb.append("&compact_type=cumulative")

        String command = sb.toString()
        process = command.execute()
        code = process.waitFor()
        err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
        out = process.getText()
        logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
        assertEquals(code, 0)
    }

    // wait for all compactions done
    for (String[] tablet in tablets) {
        boolean running = true
        do {
            Thread.sleep(1000)
            String tablet_id = tablet[0]
            backend_id = tablet[2]
            StringBuilder sb = new StringBuilder();
            sb.append("curl -X GET http://")
            sb.append(backendIdToBackendIP.get(backend_id))
            sb.append(":")
            sb.append(backendIdToBackendHttpPort.get(backend_id))
            sb.append("/api/compaction/run_status?tablet_id=")
            sb.append(tablet_id)

            String command = sb.toString()
            logger.info(command)
            process = command.execute()
            code = process.waitFor()
            err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
            out = process.getText()
            logger.info("Get compaction status: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            def compactionStatus = parseJson(out.trim())
            assertEquals("success", compactionStatus.status.toLowerCase())
            running = compactionStatus.run_status
        } while (running)
    }

    qt_sql2 """
    select count(*) from ${indexTbName1};
    """

    sleep(60000);
    def backendIdToAfterCompactionCacheSize = getCurCacheSize()
    assertTrue(backendIdToAfterLoadCacheSize.get(backendId) >
        backendIdToAfterCompactionCacheSize.get(backendId))

    // case1. test <
    // case1.0: test only <
    qt_sql "select * from ${indexTbName1} where name<'' order by name "
    qt_sql "select * from ${indexTbName1} where age<0 order by name"
    qt_sql "select * from ${indexTbName1} where grade<'' order by name"
    qt_sql "select * from ${indexTbName1} where studentInfo<'' order by name"
    qt_sql "select * from ${indexTbName1} where selfComment<'' order by name "
    qt_sql "select * from ${indexTbName1} where tearchComment<'' order by name "
    qt_sql "select * from ${indexTbName1} where fatherName<'' order by name"

    qt_sql """ select * from ${indexTbName1} where name<"" order by name """
    qt_sql """ select * from ${indexTbName1} where age<0 order by name """
    qt_sql """ select * from ${indexTbName1} where grade<"" order by name"""
    qt_sql """ select * from ${indexTbName1} where studentInfo<"" order by name"""
    qt_sql """ select * from ${indexTbName1} where selfComment<"" order by name"""
    qt_sql """ select * from ${indexTbName1} where tearchComment<"" order by name"""
    qt_sql """ select * from ${indexTbName1} where fatherName<"" order by name"""
    // case1.1: test only < some condition
    qt_sql "select * from ${indexTbName1} where name<'zhang'order by name"
    qt_sql "select * from ${indexTbName1} where age<8 order by name"
    qt_sql "select * from ${indexTbName1} where grade<'grade 5'order by name"
    qt_sql "select * from ${indexTbName1} where studentInfo<'tall:120cm, weight: 35kg,' order by name"
    qt_sql "select * from ${indexTbName1} where selfComment<'i like' order by name"
    qt_sql "select * from ${indexTbName1} where tearchComment<'A' order by name"
    qt_sql "select * from ${indexTbName1} where fatherName< 'zhang yi' order by name"
    // case1.1 test index colume and common colume mix select
    qt_sql """ select * from ${indexTbName1} where name<'zhang' and registDate="2017-10-01" order by name"""
    qt_sql """ select * from ${indexTbName1} where age<8 and registDate="2017-10-01" order by name"""
    qt_sql """ select * from ${indexTbName1} where grade<'grade 5' and registDate="2017-10-01" order by name"""
    qt_sql """ select * from ${indexTbName1} where studentInfo<"tall:120cm, weight: 35kg," and registDate="2017-10-01" order by name """
    qt_sql """ select * from ${indexTbName1} where selfComment<'i like' and registDate="2017-10-01" order by name"""
    qt_sql """ select * from ${indexTbName1} where tearchComment<'A' and registDate="2017-10-01" order by name"""
    qt_sql """ select * from ${indexTbName1} where fatherName< 'zhang yi' and registDate="2017-10-01" order by name"""
    // case1.1 test index colume or common colume mix select
    qt_sql """ select * from ${indexTbName1} where name<'zhang' or registDate="2017-10-01" order by name"""
    qt_sql """ select * from ${indexTbName1} where age<8 or registDate="2017-10-01" order by name"""
    qt_sql """ select * from ${indexTbName1} where grade<'grade 5' or registDate="2017-10-01" order by name"""
    qt_sql """ select * from ${indexTbName1} where studentInfo<"tall:120cm, weight: 35kg," or registDate="2017-10-01" order by name"""
    qt_sql """ select * from ${indexTbName1} where selfComment<'i like' or registDate="2017-10-01" order by name"""
    qt_sql """ select * from ${indexTbName1} where tearchComment<'A' or registDate="2017-10-01" order by name"""
    qt_sql """ select * from ${indexTbName1} where fatherName< 'zhang yi' or registDate="2017-10-01" order by name"""
    // case1.2 test different index mix select
    // case1.2.0 data index colume and string index mix select;
    qt_sql """ select * from ${indexTbName1} where age<10 and name<"zhang san" order by name"""
    qt_sql """ select * from ${indexTbName1} where age<10 and grade<'grade 5' order by name"""
    qt_sql """ select * from ${indexTbName1} where age<10 and tearchComment<"A quiet little boy" order by name"""
    qt_sql """ select * from ${indexTbName1} where age<10 and studentInfo<"tall:120cm, weight: 35kg," order by name"""
    qt_sql """ select * from ${indexTbName1} where age<10 and fatherName< 'zhang yi' order by name"""
    qt_sql """ select * from ${indexTbName1} where age<10 and selfComment<'i like' order by name"""
    // case1.2.1 data index colume or string index mix select;
    qt_sql """ select * from ${indexTbName1} where age<10 or name<"zhang san" order by name"""
    qt_sql """ select * from ${indexTbName1} where age<10 or grade<'grade 5' order by name"""
    qt_sql """ select * from ${indexTbName1} where age<10 or tearchComment<"A quiet little boy" order by name"""
    qt_sql """ select * from ${indexTbName1} where age<10 or studentInfo<"tall:120cm, weight: 35kg," order by name"""
    qt_sql """ select * from ${indexTbName1} where age<10 or fatherName< 'zhang yi' order by name"""
    qt_sql """ select * from ${indexTbName1} where age<10 or selfComment<'i like'order by name"""
    // case1.2.2 mutiple  index colume mix select;
    qt_sql """
        select * from ${indexTbName1} where age<10 and grade<'grade 5' and fatherName< 'zhang yi' or studentInfo<"tall:120cm, weight: 35kg," order by name
        """
    qt_sql """
        select * from ${indexTbName1} where selfComment<'i like' or grade<'grade 5' and fatherName< 'zhang yi' or studentInfo<"tall:120cm, weight: 35kg," order by name
        """
    sql "DROP TABLE IF EXISTS ${indexTbName1}"
}
