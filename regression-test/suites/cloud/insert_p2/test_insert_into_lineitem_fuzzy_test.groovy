import java.util.concurrent.TimeUnit

import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.common.IOUtils;
import net.schmizz.sshj.connection.channel.direct.Session;
import net.schmizz.sshj.connection.channel.direct.Session.Command

import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReadWriteLock
import java.util.concurrent.locks.ReentrantReadWriteLock;

enum STATE {
    RESTART_FE(1),
    RESTART_BE(2),
    private int value

    STATE(int value) {
        this.value = value
    }

    int getValue() {
        return value
    }
}

String[] getFiles(String dirName) {
    File[] datas = new File(dirName).listFiles()
    String[] array = new String[datas.length];
    for (int i = 0; i < datas.length; i++) {
        array[i] = datas[i].getPath();
    }
    Arrays.sort(array);
    return array;
}

suite("test_insert_into_lineitem_fuzzy_test") {
    def insert_table = "test_insert_into_lineitem_fuzzy_test_sf1"
    def stream_load_table = "test_stream_load_lineitem_fuzzy_test_sf1"
    def dir = "${context.config.dataPath}/cloud/insert_p2/lineitem"
    def batch = 100;
    def count = 0;
    def total = 0;
    def feNum = 0;
    int beNum = 0;
    String[] file_array = null;

    def checkProcessName = { String processName ->

        if (processName in ["fe", "be", "ms"]) {
            return
        }
        throw new Exception("invalid process name: " + processName)
    }

    def executeCommand = { String nodeIp, String commandStr ->
        final SSHClient ssh = new SSHClient()
        ssh.loadKnownHosts()
        ssh.connect(nodeIp)
        Session session = null
        try {
            logger.info("user.name:{}", System.getProperty("user.name"))
            ssh.authPublickey(System.getProperty("user.name"))
            session = ssh.startSession()
            logger.info("commandStr:${commandStr}")
            final Command cmd = session.exec(commandStr)
            cmd.join(30, TimeUnit.SECONDS)
            def code = cmd.getExitStatus()
            def out = IOUtils.readFully(cmd.getInputStream()).toString()
            def err = IOUtils.readFully(cmd.getErrorStream()).toString()
            def errMsg = cmd.getExitErrorMessage()
            logger.info("code:${code}, out:${out}, err:${err}, errMsg:${errMsg}")
            assertEquals(0, code)
        } finally {
            try {
                if (session != null) {
                    session.close()
                }
            } catch (IOException e) {
                // Do Nothing
            }
            ssh.disconnect()
        }
        return
    }

    def checkProcessAlive = { String nodeIp, String processName, String installPath /* param */ ->
        logger.info("checkProcessAlive(): nodeIp=${nodeIp} installPath=${installPath} processName=${processName}")
        checkProcessName(processName)

        def commandStr = "invalid command"
        if (processName == "fe") {
            commandStr = "bash -c \"ps aux | grep ${installPath}/log/fe.gc.log | grep -v grep\""
        }

        if (processName == "be") {
            commandStr = "bash -c \"ps aux | grep ${installPath}/lib/doris_be | grep -v grep\""
        }

        if (processName == "ms") {
            commandStr = "bash -c \"ps aux | grep '${installPath}/lib/selectdb_cloud --meta-service' | grep -v grep\""
        }

        executeCommand(nodeIp, commandStr)
        return
    }

    def stopProcess = { String nodeIp, String processName, String installPath /* param */ ->

        logger.info("stopProcess(): nodeIp=${nodeIp} installPath=${installPath} processName=${processName}")
        String commandStr
        if (processName == "ms") {
            commandStr = "bash -c \"${installPath}/bin/stop.sh\""
        } else {
            commandStr = "bash -c \"${installPath}/bin/stop_${processName}.sh\""
        }

        executeCommand(nodeIp, commandStr)
        return
    }

    def startProcess = { String nodeIp, String processName, String installPath /* param */ ->
        checkProcessName(processName);

        logger.info("startProcess(): nodeIp=${nodeIp} installPath=${installPath} processName=${processName}");

        String commandStr
        if (processName == "ms") {
            commandStr = "bash -c \"${installPath}/bin/start.sh  --meta-service --daemon\"";
        } else {
            commandStr = "bash -c \"${installPath}/bin/start_${processName}.sh --daemon\"";
        }

        executeCommand(nodeIp, commandStr)
        return;
    }


    def restartProcess = { String nodeIp, String processName, String installPath /* param */ ->
        logger.info("restartProcess(): nodeIp=${nodeIp} installPath=${installPath} processName=${processName}")
        stopProcess(nodeIp, processName, installPath)
        Thread.sleep(1000)
        startProcess(nodeIp, processName, installPath)

        int tryTimes = 3
        while (tryTimes-- > 0) {
            try {
                checkProcessAlive(nodeIp, processName, installPath)
                break
            } catch (Exception e) {
                logger.info("checkProcessAlive failed, tryTimes=${tryTimes}")
                if (tryTimes <= 0) {
                    throw e
                }
            } finally {
                // sleep 5 seconds for wait qe service ready
                Thread.sleep(5000)
            }
        }
    }


    def getRowCount = { expectedRowCount, table_name ->
        def retry = 0
        while (retry < 30) {
            Thread.sleep(2000)
            try {
                def rowCount = sql "select count(*) from ${table_name}"
                logger.info("rowCount: " + rowCount + ", retry: " + retry)
                if (rowCount[0][0] >= expectedRowCount) {
                    break
                }
            } catch (Exception e) {
                logger.info("select count get exception", e);
            }
            retry++
        }
    }

    def create_stream_load_table = {
        sql """ drop table if exists ${stream_load_table}; """

        sql """
   CREATE TABLE ${stream_load_table} (
    l_shipdate    DATEV2 NOT NULL,
    l_orderkey    bigint NOT NULL,
    l_linenumber  int not null,
    l_partkey     int NOT NULL,
    l_suppkey     int not null,
    l_quantity    decimalv3(15, 2) NOT NULL,
    l_extendedprice  decimalv3(15, 2) NOT NULL,
    l_discount    decimalv3(15, 2) NOT NULL,
    l_tax         decimalv3(15, 2) NOT NULL,
    l_returnflag  VARCHAR(1) NOT NULL,
    l_linestatus  VARCHAR(1) NOT NULL,
    l_commitdate  DATEV2 NOT NULL,
    l_receiptdate DATEV2 NOT NULL,
    l_shipinstruct VARCHAR(25) NOT NULL,
    l_shipmode     VARCHAR(10) NOT NULL,
    l_comment      VARCHAR(44) NOT NULL
)ENGINE=OLAP
DUPLICATE KEY(`l_shipdate`, `l_orderkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 96
PROPERTIES (
    "colocate_with" = "lineitem_orders"
);
        """
    }

    def do_stream_load = {
        create_stream_load_table()
        for (int i = 0; i < file_array.length; i++) {
            def filePath = file_array[i];
            logger.info("stream load file:" + filePath)
            streamLoad {
                table stream_load_table
                set 'column_separator', '|'
                set 'columns', 'l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag,l_linestatus, l_shipdate,l_commitdate,l_receiptdate,l_shipinstruct,l_shipmode,l_comment,temp'
                file """${filePath}"""
                check { result, exception, startTime, endTime ->
                    if (exception != null) {
                        throw exception
                    }
                    log.info("Stream load result: ${result}".toString())
                    def json = parseJson(result)
                    assertEquals("success", json.Status.toLowerCase())
                    assertEquals(json.NumberTotalRows, json.NumberLoadedRows)
                    assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
                }
            }
        }

        while (true) {
            try {
                qt_sql """ select count(*) from ${stream_load_table}; """
                break
            } catch (Exception e) {
                Thread.sleep(1000)
                log.info("exception:", e)
            }
        }

        while (true) {
            try {
                qt_sql """ select l_orderkey from ${stream_load_table} where l_orderkey >=0 and l_orderkey <=6000000 order by l_shipdate asc; """
                break
            } catch (Exception e) {
                Thread.sleep(1000)
                log.info("exception:", e)
            }
        }
    }

    def create_insert_table = {
        // create table
        sql """ drop table if exists ${insert_table}; """

        sql """
   CREATE TABLE ${insert_table} (
    l_shipdate    DATEV2 NOT NULL,
    l_orderkey    bigint NOT NULL,
    l_linenumber  int not null,
    l_partkey     int NOT NULL,
    l_suppkey     int not null,
    l_quantity    decimalv3(15, 2) NOT NULL,
    l_extendedprice  decimalv3(15, 2) NOT NULL,
    l_discount    decimalv3(15, 2) NOT NULL,
    l_tax         decimalv3(15, 2) NOT NULL,
    l_returnflag  VARCHAR(1) NOT NULL,
    l_linestatus  VARCHAR(1) NOT NULL,
    l_commitdate  DATEV2 NOT NULL,
    l_receiptdate DATEV2 NOT NULL,
    l_shipinstruct VARCHAR(25) NOT NULL,
    l_shipmode     VARCHAR(10) NOT NULL,
    l_comment      VARCHAR(44) NOT NULL
)ENGINE=OLAP
DUPLICATE KEY(`l_shipdate`, `l_orderkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 96
PROPERTIES (
    "colocate_with" = "lineitem_orders"
);
        """
        sql """ set enable_insert_group_commit = true; """
    }

    def do_inset_into = {
        create_insert_table()
        def feDir = "/mnt/disk1/huanghaibin/hhb/selectdb-core/output/fe"
        def beDir = "/mnt/disk1/huanghaibin/hhb/selectdb-core/output/be"
        logger.info("fedir:" + feDir)
        logger.info("beDir:" + beDir)

        String feIp1 = "172.21.16.12"
        String beIp1 = "172.21.16.12"


        def clusterMap = [
                fe: [[ip: feIp1, path: feDir]],
                be: [[ip: beIp1, path: beDir]]
        ]

        logger.info("clusterMap:${clusterMap}");
        checkProcessAlive(clusterMap["fe"][0]["ip"], "fe", clusterMap["fe"][0]["path"]);
        checkProcessAlive(clusterMap["be"][0]["ip"], "be", clusterMap["be"][0]["path"]);


        for (int k = 0; k < file_array.length; k++) {
            logger.info("insert into file:" + file_array[k])

            switch (k) {
                case STATE.RESTART_FE.value:
                    Thread thread = new Thread(new Runnable() {
                        @Override
                        void run() {
                            restartProcess(clusterMap["fe"][feNum]["ip"], "fe", clusterMap["fe"][feNum]["path"])
                        }
                    });
                    thread.setDaemon(true);
                    thread.start();
                    break
                case STATE.RESTART_BE.value:
                    Thread thread = new Thread(new Runnable() {
                        @Override
                        void run() {
                            logger.info("restart beNum:" + beNum)
                            restartProcess(clusterMap["be"][beNum]["ip"], "be", clusterMap["be"][beNum]["path"]);
                        }
                    });
                    thread.setDaemon(true);
                    thread.start();
                    break
            }

            //read and insert
            BufferedReader reader;
            try {
                reader = new BufferedReader(new FileReader(file_array[k]));
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            }

            String s = null;
            StringBuilder sb = null;
            count = 0;
            while (true) {
                try {
                    if (count == batch) {
                        sb.append(";");
                        String exp = sb.toString();
                        while (true) {
                            try {
                                def result = sql exp;
                                logger.info("result:" + result);
                                break
                            } catch (Exception e) {
                                logger.info("got exception:" + e)
                                resetConnection();
                            }
                        }
                        count = 0;
                    }
                    s = reader.readLine();
                    if (s != null) {
                        if (count == 0) {
                            sb = new StringBuilder();
                            sb.append("insert into ${insert_table} (l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag,l_linestatus, l_shipdate,l_commitdate,l_receiptdate,l_shipinstruct,l_shipmode,l_comment)VALUES");
                        }
                        if (count > 0) {
                            sb.append(",");
                        }
                        String[] array = s.split("\\|");
                        sb.append("(");
                        for (int i = 0; i < array.length; i++) {
                            sb.append("\"" + array[i] + "\"");
                            if (i != array.length - 1) {
                                sb.append(",");
                            }
                        }
                        sb.append(")");
                        count++;
                        total++;
                    } else if (count > 0) {
                        sb.append(";");
                        String exp = sb.toString();
                        while (true) {
                            try {
                                def result = sql exp;
                                logger.info("result:" + result);
                                break
                            } catch (Exception e) {
                                logger.info("got exception:" + e)
                                resetConnection();
                            }
                        }
                        break;
                    } else {
                        break;
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            if (reader != null) {
                reader.close();
            }
        }
        logger.info("total: " + total)
        getRowCount(total, insert_table)

        while (true) {
            try {
                qt_sql """select count(*) from ${insert_table};"""
                break
            } catch (Exception e) {
                Thread.sleep(1000)
                log.info("exception:", e)
            }
        }

        while (true) {
            try {
                qt_sql """ select l_orderkey from ${insert_table} where l_orderkey >=0 and l_orderkey <=6000000 order by l_shipdate asc; """
                break
            } catch (Exception e) {
                Thread.sleep(1000)
                log.info("exception:", e)
            }
        }
    }

    try {
        file_array = getFiles(dir)
        if (!context.outputFile.exists()) {
            do_stream_load()
        } else {
            do_inset_into()
        }
    } finally {

    }

}

