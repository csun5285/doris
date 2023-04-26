suite("test_dynamic_table") {
    def tableName = "es_nested"
    def externalStageName = "dynamic_table"
    def prefix = "copy_into_dynamic_table"

    try {
        sql """ DROP TABLE IF EXISTS ${tableName}; """
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
                qid bigint,
                creationDate datetime,
                `answers.date` array<datetime>,
                `title` string,
		        ...
        )
        DUPLICATE KEY(`qid`)
        DISTRIBUTED BY RANDOM BUCKETS 5 
        properties("replication_num" = "1");
        """

        sql """
            create stage if not exists ${externalStageName} 
            properties ('endpoint' = '${getS3Endpoint()}' ,
            'region' = '${getS3Region()}' ,
            'bucket' = '${getS3BucketName()}' ,
            'ak' = '${getS3AK()}' ,
            'sk' = '${getS3SK()}' ,
            'provider' = '${getProvider()}' , 
            'access_type' = 'aksk',
            'default.file.type' = "json");
        """

        def result = sql " insert into ${tableName} select * from stage( 'stage_name'='${externalStageName}', 'file_pattern'='${prefix}/es_nested.json', 'format'='json', 'read_json_by_line'='true') "
        logger.info("insert result: " + result)
        qt_sql " SELECT COUNT(*) FROM ${tableName}; "

        result = sql " insert into ${tableName} select * from stage( 'stage_name'='${externalStageName}', 'file_pattern'='${prefix}/es_nested.json', 'format'='json', 'read_json_by_line'='true') "
        logger.info("insert result: " + result)
        qt_sql " SELECT COUNT(*) FROM ${tableName}; "
    } finally {
        try_sql("DROP TABLE IF EXISTS ${tableName}")
    }
}