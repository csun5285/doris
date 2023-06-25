suite("not_null_check"){
    def tableName = 'table_null_check'
    sql """ DROP TABLE IF EXISTS ${tableName} """

    sql """
    CREATE TABLE IF NOT EXISTS ${tableName} (
      `test_varchar` varchar(150) NULL,
      `test_datetime` datetime NULL,
      `test_default_timestamp` datetime DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=OLAP
    UNIQUE KEY(`test_varchar`)
    DISTRIBUTED BY HASH(`test_varchar`) BUCKETS 3
    """

    sql """ INSERT INTO ${tableName}(test_varchar, test_datetime) VALUES ('test1','2022-04-27 16:00:33'),('test2','2022-04-27 16:00:54') """

    try {
        sql """ SELECT /*+ SET_VAR(parallel_fragment_exec_instance_num=8 */ * from ${tableName}; """
        assertTrue(false. "There is an error in this statment, it can't be parsed correctly")
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("Encountered: Unknown last token with id:"), e.getMessage())
    }
}