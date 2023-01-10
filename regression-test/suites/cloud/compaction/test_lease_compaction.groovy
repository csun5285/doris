import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_lease_compaction") {
    def tableName = "t1"

    //BackendId,Cluster,IP,HeartbeatPort,BePort,HttpPort,BrpcPort,LastStartTime,LastHeartbeat,Alive,SystemDecommissioned,ClusterDecommissioned,TabletNum,DataUsedCapacity,AvailCapacity,TotalCapacity,UsedPct,MaxDiskUsedPct,RemoteUsedCapacity,Tag,ErrMsg,Version,Status
    String[][] backends = sql """ show backends; """
    assertTrue(backends.size() > 0)
    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    def cluster_to_backendId = [:]
    for (String[] backend in backends) {
        backendId_to_backendIP.put(backend[0], backend[2])
        backendId_to_backendHttpPort.put(backend[0], backend[5])
        def tagJson = parseJson(backend[19])
        if (!cluster_to_backendId.containsKey(tagJson.cloud_cluster_name)) {
            cluster_to_backendId.put(tagJson.cloud_cluster_name, backend[0])
        }
    }
    assertTrue(cluster_to_backendId.size() >= 2)
    def cluster0 = cluster_to_backendId.keySet()[0]
    def cluster1 = cluster_to_backendId.keySet()[1]
    def backend_id0 = cluster_to_backendId.get(cluster0)
    def backend_id1 = cluster_to_backendId.get(cluster1)
    
    def updateBeConf = { backend_ip, backend_http_port, key, value ->
        String command = "curl -X POST http://${backend_ip}:${backend_http_port}/api/update_config?${key}=${value}"
        logger.info(command)
        process = command.execute()
        code = process.waitFor()
        assertEquals(code, 0)
    }
    def injectionPoint = { backend_ip, backend_http_port, args ->
        String command = "curl -X GET http://${backend_ip}:${backend_http_port}/api/injection_point/${args}"
        logger.info(command)
        process = command.execute()
        code = process.waitFor()
        assertEquals(0, code)
        out = process.getText()
        assertEquals("OK", out)
    }

    try {
        updateBeConf(backendId_to_backendIP.get(backend_id0), backendId_to_backendHttpPort.get(backend_id0),
            "disable_auto_compaction", "true");
        updateBeConf(backendId_to_backendIP.get(backend_id1), backendId_to_backendHttpPort.get(backend_id1),
            "disable_auto_compaction", "true");

        sql """ DROP TABLE IF EXISTS ${tableName}; """
        sql """
            CREATE TABLE ${tableName} (
                `id` int(11) NULL,
                `name` varchar(255) NULL,
                `score` int(11) SUM NULL
            ) ENGINE=OLAP
            AGGREGATE KEY(`id`, `name`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`id`) BUCKETS 1;
        """

        //TabletId,ReplicaId,BackendId,SchemaHash,Version,LstSuccessVersion,LstFailedVersion,LstFailedTime,LocalDataSize,RemoteDataSize,RowCount,State,LstConsistencyCheckTime,CheckVersion,VersionCount,PathHash,MetaUrl,CompactionStatus
        def tablet = (sql """ show tablets from ${tableName}; """)[0]

        def triggerCompaction = { backend_id, compact_type ->
            // trigger compactions for all tablets in ${tableName}
            String tablet_id = tablet[0]
            StringBuilder sb = new StringBuilder();
            sb.append("curl -X POST http://")
            sb.append(backendId_to_backendIP.get(backend_id))
            sb.append(":")
            sb.append(backendId_to_backendHttpPort.get(backend_id))
            sb.append("/api/compaction/run?tablet_id=")
            sb.append(tablet_id)
            sb.append("&compact_type=${compact_type}")

            String command = sb.toString()
            logger.info(command)
            process = command.execute()
            code = process.waitFor()
            err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
            out = process.getText()
            logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            return out
        } 
        def waitForCompaction = { backend_id ->
            // wait for all compactions done
            boolean running = true
            do {
                Thread.sleep(1000)
                String tablet_id = tablet[0]
                StringBuilder sb = new StringBuilder();
                sb.append("curl -X GET http://")
                sb.append(backendId_to_backendIP.get(backend_id))
                sb.append(":")
                sb.append(backendId_to_backendHttpPort.get(backend_id))
                sb.append("/api/compaction/run_status?tablet_id=")
                sb.append(tablet_id)

                String command = sb.toString()
                logger.info(command)
                process = command.execute()
                code = process.waitFor()
                out = process.getText()
                logger.info("Get compaction status: code=" + code + ", out=" + out)
                assertEquals(code, 0)
                def compactionStatus = parseJson(out.trim())
                assertEquals("success", compactionStatus.status.toLowerCase())
                running = compactionStatus.run_status
            } while (running)
        }

        //======================================================================
        // Test rejecting compaction job when lease not expired
        //======================================================================
        // cluster0 set lease_compaction_interval_seconds=1, then lease expiration will be 4s
        updateBeConf(backendId_to_backendIP.get(backend_id0), backendId_to_backendHttpPort.get(backend_id0),
            "lease_compaction_interval_seconds", "1");
        // make cluster0 compaction time exceed 8s to longer than lease expiration
        injectionPoint(backendId_to_backendIP.get(backend_id0), backendId_to_backendHttpPort.get(backend_id0),
            "set/Compaction::do_compaction?behavior=sleep&duration=8000");

        sql """ use @`${cluster0}`; """
        sql """ INSERT INTO ${tableName} VALUES (1, "a", 100); """
        sql """ INSERT INTO ${tableName} VALUES (1, "a", 100); """
        sql """ INSERT INTO ${tableName} VALUES (1, "a", 100); """
        sql """ INSERT INTO ${tableName} VALUES (1, "a", 100); """
        sql """ INSERT INTO ${tableName} VALUES (1, "a", 100); """
        sql """ INSERT INTO ${tableName} VALUES (1, "a", 100); """

        // cluster0 trigger cumu compaction
        assertTrue(triggerCompaction(backend_id0, "cumulative").contains("Success"));
        // cluster1 trigger cumu compaction failed
        assertTrue(triggerCompaction(backend_id1, "cumulative").contains("already started"));
        // wait cluster0 compaction success
        waitForCompaction(backend_id0);

        //======================================================================
        // Test preempting compaction job when lease expired
        //======================================================================
        // cluster0 disable lease
        injectionPoint(backendId_to_backendIP.get(backend_id0), backendId_to_backendHttpPort.get(backend_id0),
            "set/CloudCumulativeCompaction::do_lease?behavior=return");        
        sql """ INSERT INTO ${tableName} VALUES (1, "a", 100); """
        sql """ INSERT INTO ${tableName} VALUES (1, "a", 100); """
        sql """ INSERT INTO ${tableName} VALUES (1, "a", 100); """
        sql """ INSERT INTO ${tableName} VALUES (1, "a", 100); """
        sql """ INSERT INTO ${tableName} VALUES (1, "a", 100); """
        sql """ INSERT INTO ${tableName} VALUES (1, "a", 100); """

        // cluster0 trigger cumu compaction
        assertTrue(triggerCompaction(backend_id0, "cumulative").contains("Success"));
        // cluster1 trigger cumu compaction. Retry for 10s to wait for the cluster0 lease to expire
        def retry_times = 10
        do { 
            Thread.sleep(1000);
            if (triggerCompaction(backend_id1, "cumulative").contains("Success")) {
                break;
            }
        } while (--retry_times);
        assertTrue(retry_times > 0);
        // wait cluster1 compaction success
        waitForCompaction(backend_id1);
    } finally {
        try_sql("DROP TABLE IF EXISTS ${tableName}")
        injectionPoint(backendId_to_backendIP.get(backend_id0), backendId_to_backendHttpPort.get(backend_id0),
            "clear/all");
        injectionPoint(backendId_to_backendIP.get(backend_id1), backendId_to_backendHttpPort.get(backend_id1),
            "clear/all");
        updateBeConf(backendId_to_backendIP.get(backend_id0), backendId_to_backendHttpPort.get(backend_id0),
            "lease_compaction_interval_seconds", "20");
        updateBeConf(backendId_to_backendIP.get(backend_id0), backendId_to_backendHttpPort.get(backend_id0),
            "disable_auto_compaction", "false");
        updateBeConf(backendId_to_backendIP.get(backend_id1), backendId_to_backendHttpPort.get(backend_id1),
            "disable_auto_compaction", "false");
    }
}
