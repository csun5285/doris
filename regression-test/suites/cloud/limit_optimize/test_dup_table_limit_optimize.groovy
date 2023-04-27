suite("test_dup_table_limit_optimize") {
    def tableName = "test_dup_table_limit_optimize"
    sql """ DROP TABLE IF EXISTS ${tableName}; """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
        k1 int(11) NOT NULL COMMENT "",
        k2 int(11) NOT NULL COMMENT ""
        )
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1;
    """

    for (int i = 0; i < 20; i++) {
        sql """insert into ${tableName} values (${i}, ${i*10})"""
    }

    qt_sql """ select * from ${tableName} limit 1 """
    qt_sql """ select k1 from ${tableName} limit 1 """
    qt_sql """ select k2 as b from ${tableName} limit 1 """
    qt_sql """ select * from ${tableName} limit 5 """
    qt_sql """ select * from ${tableName} limit 10 """
    qt_sql """ select * from ${tableName} limit 20 """
    qt_sql """ select * from ${tableName} """

    sql """ delete from ${tableName} where k1 = 0"""
    sql """ delete from ${tableName} where k1 = 3"""
    sql """ delete from ${tableName} where k1 = 5"""
    sql """ delete from ${tableName} where k1 = 7"""
    sql """ delete from ${tableName} where k1 = 11"""
    sql """ delete from ${tableName} where k1 = 17"""

    qt_sql """ select * from ${tableName} limit 1 """
    qt_sql """ select k1 from ${tableName} limit 1 """
    qt_sql """ select k2 as b from ${tableName} limit 1 """
    qt_sql """ select * from ${tableName} limit 5 """
    qt_sql """ select * from ${tableName} limit 10 """
    qt_sql """ select * from ${tableName} limit 20 """
    qt_sql """ select * from ${tableName} """  
    
}