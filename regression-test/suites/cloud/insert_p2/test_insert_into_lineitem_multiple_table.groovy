import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

String[] getFiles(String dirName) {
    File[] datas = new File(dirName).listFiles()
    String[] array = new String[datas.length];
    for (int i = 0; i < datas.length; i++) {
        array[i] = datas[i].getPath();
    }
    Arrays.sort(array);
    return array;
}

suite("test_insert_into_lineitem_multiple_table") {
    def insert_table_base = "test_insert_into_lineitem_multiple_table"
    def stream_load_table_base = "test_stream_load_lineitem_multiple_table"
    def dir = "${context.config.dataPath}/cloud/insert_p2/lineitem"
    def batch = 100;
    def total = 0;
    ReadWriteLock rwLock = new ReentrantReadWriteLock();
    Lock wlock = rwLock.writeLock();
    String[] file_array = null;

    def getRowCount = { expectedRowCount, table_name ->
        def retry = 0
        while (retry < 60) {
            sleep(5000)
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

    def create_stream_load_table = { table_name ->
        sql """ drop table if exists ${table_name}; """

        sql """
   CREATE TABLE ${table_name} (
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
        for (int i = 0; i < file_array.length; i++) {
            def filePath = file_array[i];
            logger.info("stream load file:" + filePath)
            def table_name = stream_load_table_base + "_" + i;
            create_stream_load_table(table_name)
            streamLoad {
                table table_name
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

            try {
                qt_sql """ select count(*) from ${table_name}; """
            } catch (Exception e) {
                log.info("exception:", e)
            }

            try {
                qt_sql """ select l_orderkey from ${table_name} where l_orderkey >=0 and l_orderkey <=6000000 order by l_orderkey asc; """
            } catch (Exception e) {
                log.info("exception:", e)
            }
        }

    }

    def create_insert_table = { table_name ->
        // create table
        sql """ drop table if exists ${table_name}; """

        sql """
   CREATE TABLE ${table_name} (
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

    def process_insert_into = { file_name, table_name ->
        logger.info("file:" + file_name)
        //read and insert
        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader(file_name));
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        sql """ set enable_insert_group_commit = true; """

        String s = null;
        StringBuilder sb = null;
        int c = 0;
        int t = 0;
        while (true) {
            try {
                if (c == batch) {
                    sb.append(";");
                    String exp = sb.toString();
                    while (true) {
                        try {
                            def result = insert_into_sql(exp, c);
                            logger.info("result:" + result);
                            break
                        } catch (Exception e) {
                            logger.info("got exception:" + e)
                        }
                    }
                    c = 0;
                }
                s = reader.readLine();
                if (s != null) {
                    if (c == 0) {
                        sb = new StringBuilder();
                        sb.append("insert into ${table_name} (l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag,l_linestatus, l_shipdate,l_commitdate,l_receiptdate,l_shipinstruct,l_shipmode,l_comment)VALUES");
                    }
                    if (c > 0) {
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
                    c++;
                    t++;
                } else if (c > 0) {
                    sb.append(";");
                    String exp = sb.toString();
                    while (true) {
                        try {
                            def result = insert_into_sql(exp, c);
                            logger.info("result:" + result);
                            break
                        } catch (Exception e) {
                            logger.info("got exception:" + e)
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
        {
            logger.info("t: " + t)
            wlock.lock()
            total += t;
            wlock.unlock()
        }

        if (reader != null) {
            reader.close();
        }
        getRowCount(t, table_name)
    }

    def do_insert_into = {
        def threads = []
        for (int k = 0; k < file_array.length; k++) {
            String file_name = file_array[k]
            String table_name = insert_table_base + "_" + k;
            create_insert_table(table_name)
            logger.info("insert into file:" + file_name)
            threads.add(Thread.startDaemon {
                process_insert_into(file_name, table_name)
            })
        }
        for (Thread th in threads) {
            th.join()
        }

        for (int k = 0; k < file_array.length; k++) {
            String table_name = insert_table_base + "_" + k;

            try {
                qt_sql """ select count(*) from ${table_name}; """
            } catch (Exception e) {
                log.info("exception:", e)
            }

            try {
                qt_sql """ select l_orderkey from ${table_name} where l_orderkey >=0 and l_orderkey <=6000000 order by l_orderkey asc; """
            } catch (Exception e) {
                log.info("exception:", e)
            }

        }
    }

    try {
        File file = new File(dir)
        if (file.exists() && file.isDirectory()) {
            file_array = getFiles(dir)
            if (!context.outputFile.exists()) {
                do_stream_load()
            } else {
                do_insert_into()
            }
        }
    } finally {

    }


}