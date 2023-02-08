suite("test_dynamic_table") {
    def tableName1 = "es_nested"
    def tableName2 = "github_events"
    def externalStageName = "dynamic_table"
    def prefix = "copy_into_dynamic_table"

    try {
        sql """ DROP TABLE IF EXISTS ${tableName1}; """
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName1} (
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

        sql """ DROP TABLE IF EXISTS ${tableName2}; """
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName2} (
                id bigint,
		        ...
        )
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 10
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

        // es_nested
        def result = sql " copy into ${tableName1} from @${externalStageName}('${prefix}/es_nested.json') properties ('file.type' = 'json', 'copy.async' = 'false'); "
        logger.info("copy result: " + result)
        assertTrue(result.size() == 1)
        assertTrue(result[0].size() == 8)
        assertTrue(result[0][1].equals("FINISHED"), "Finish copy into, state=" + result[0][1] + ", expected state=FINISHED")
        qt_sql " SELECT COUNT(*) FROM ${tableName1}; "

        result = sql " copy into ${tableName1} from @${externalStageName}('${prefix}/es_nested.json') properties ('file.type' = 'json', 'copy.async' = 'false'); "
        logger.info("copy result: " + result)
        assertTrue(result.size() == 1)
        assertTrue(result[0].size() == 8)
        assertTrue(result[0][1].equals("CANCELLED"), "Finish copy into, state=" + result[0][1] + ", expected state=CANCELLED")
        qt_sql " SELECT COUNT(*) FROM ${tableName1}; "

        // github_events
        result = sql " copy into ${tableName2} from @${externalStageName}('${prefix}/*.json.10000') properties ('file.type' = 'json', 'copy.async' = 'false', 'copy.load_parallelism' = '5'); "
        logger.info("copy result: " + result)
        assertTrue(result.size() == 1)
        assertTrue(result[0].size() == 8)
        assertTrue(result[0][1].equals("FINISHED"), "Finish copy into, state=" + result[0][1] + ", expected state=FINISHED")
        qt_sql " SELECT COUNT(*) FROM ${tableName2}; "
        qt_sql " SELECT COUNT(`actor.id`) FROM ${tableName2}; "
        qt_sql " SELECT COUNT(`type`) FROM ${tableName2}; "
        qt_sql " SELECT COUNT(`payload.push_id`) FROM ${tableName2}; "
        qt_sql " SELECT COUNT(`payload.review.user.login`) FROM ${tableName2}; "
    } finally {
        try_sql("DROP TABLE IF EXISTS ${tableName1}")
        try_sql("DROP TABLE IF EXISTS ${tableName2}")
    }
}