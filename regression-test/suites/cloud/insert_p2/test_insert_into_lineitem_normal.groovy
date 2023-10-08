String[] getFiles(String dirName) {
    File[] datas = new File(dirName).listFiles()
    String[] array = new String[datas.length];
    for (int i = 0; i < datas.length; i++) {
        array[i] = datas[i].getPath();
    }
    Arrays.sort(array);
    return array;
}

suite("test_insert_into_lineitem_normal") {
    def insert_table = "test_insert_into_lineitem_sf1"
    def stream_load_table = "test_stream_load_lineitem_sf1"
    def dir = "${context.config.dataPath}/cloud/insert_p2/lineitem"
    def batch = 100;
    def count = 0;
    def total = 0;
    def round = 2;
    String[] file_array = null;

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
        int finish_round = 0;
        while (finish_round < round) {
            for (String fileName : file_array) {
                logger.info("stream load file:" + fileName)
                streamLoad {
                    table stream_load_table
                    set 'column_separator', '|'
                    set 'columns', 'l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag,l_linestatus, l_shipdate,l_commitdate,l_receiptdate,l_shipinstruct,l_shipmode,l_comment,temp'
                    file """${fileName}"""
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
                    qt_sql """ select l_orderkey from ${stream_load_table} where l_orderkey >=0 and l_orderkey <=6000000 order by l_orderkey asc; """
                    break
                } catch (Exception e) {
                    Thread.sleep(1000)
                    log.info("exception:", e)
                }
            }
            finish_round++;
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
        int finish_round = 0;
        while (finish_round < round) {
            for (String file : file_array) {
                logger.info("insert into file: " + file)
                BufferedReader reader;
                try {
                    reader = new BufferedReader(new FileReader(file));
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
                    qt_sql """ select l_orderkey from ${insert_table} where l_orderkey >=0 and l_orderkey <=6000000 order by l_orderkey asc; """
                    break
                } catch (Exception e) {
                    Thread.sleep(1000)
                    log.info("exception:", e)
                }
            }
            finish_round++;
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