suite("test_copy_with_select") {
    def tableName = "customer_copy_with_select"
    def externalStageName = "regression_test_tpch"
    def prefix = "tpch/sf1"

    sql """
        create stage if not exists ${externalStageName} 
        properties ('endpoint' = '${getS3Endpoint()}' ,
        'region' = '${getS3Region()}' ,
        'bucket' = '${getS3BucketName()}' ,
        'prefix' = 'regression' ,
        'ak' = '${getS3AK()}' ,
        'sk' = '${getS3SK()}' ,
        'provider' = '${getProvider()}',
        'access_type' = 'aksk',
        'default.file.column_separator' = "|");
    """

    def sql_prefix = """ insert into ${tableName} """
    def sql_stage = """  from stage('stage_name'='${externalStageName}', 'file_pattern'='${prefix}/customer.csv.gz', 'format' = 'csv', 'compress' = 'gz') """

    def sqls = [
            'select c1, c2, c3, c4, c5, c6, c7, c8 ' + sql_stage,
            'select c1, c3, c2, c4, c5, c6, c7, c8 ' + sql_stage,
            'select c1, c2, c3, c4, c5, c6, c7, NULL ' + sql_stage,
            'select c1, c2, c3, c4, NULL, c6, c7, NULL ' + sql_stage,
            'select c1, c2, c3, c4, c5, c6, c7, c8 ' + sql_stage + ' where c1 > 2000',
            'select c1 + 20000, c2, c3, c4, c5, c6, c7, c8 ' + sql_stage,
            'select c1 + 30000, c2, c3, c4, c5, c6, c7, c8 ' + sql_stage + ' where c1 > 3000',
            'select c1, c2, c3, c4, c5, c6, c7, substring(c8, 2) ' + sql_stage
    ]

    for (String selectSql: sqls) {
        try {
            sql """ DROP TABLE IF EXISTS ${tableName}; """
            sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
            C_CUSTKEY     INTEGER NOT NULL,
            C_NAME        VARCHAR(40) NOT NULL,
            C_ADDRESS     VARCHAR(40) NOT NULL,
            C_NATIONKEY   INTEGER NOT NULL,
            C_PHONE       CHAR(15) NULL,
            C_ACCTBAL     DECIMAL(15,2)   NOT NULL,
            C_MKTSEGMENT  CHAR(10) NOT NULL,
            C_COMMENT     VARCHAR(117) NULL
            )
            UNIQUE KEY(C_CUSTKEY)
            DISTRIBUTED BY HASH(C_CUSTKEY) BUCKETS 1
            """

            sql """ ${sql_prefix} ${selectSql} """
            qt_sql " SELECT COUNT(*) FROM ${tableName}; "
            qt_sql "select * from ${tableName} order by C_CUSTKEY, C_NAME, C_ADDRESS, C_NATIONKEY, C_PHONE, C_ACCTBAL, C_MKTSEGMENT, C_COMMENT limit 20;"

        } finally {
            try_sql("DROP TABLE IF EXISTS ${tableName}")
        }
    }
}