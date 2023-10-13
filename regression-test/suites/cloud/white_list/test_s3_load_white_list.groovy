
suite("test_s3_load_white_list") {
    def tableName = "test_s3_load_white_list_table"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
                k1 INT NOT NULL,
                value1 varchar(16) NOT NULL
            )
        DUPLICATE KEY (k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1;
    """


    def s3_load = {endPoint ->
        sql """
        LOAD LABEL ${tableName}
        (
            DATA INFILE("s3://dummy_bucket/dummy_file.txt")
            INTO TABLE ${tableName}
            COLUMNS TERMINATED BY ","
        )
        WITH S3
        (
            "AWS_ENDPOINT" = "${endPoint}",
            "AWS_ACCESS_KEY" = "AWS_ACCESS_KEY",
            "AWS_SECRET_KEY"="AWS_SECRET_KEY",
            "AWS_REGION" = "AWS_REGION"
        )
        PROPERTIES
        (
            "timeout" = "3600"
        );
        """
    }
    def noPass = "is not in s3 load endpoint white list"

    try {
        // default has no white list
        try {
            s3_load("anything")
        } catch (Exception e) {
            log.info(e.getMessage())
            assertFalse(e.getMessage().contains(noPass))
        }

        // set white list in [a, b, c]
        try {
            sql """ ADMIN SET FRONTEND CONFIG ("s3_load_endpoint_white_list" = "a, b, c"); """
            s3_load("d")
        } catch (Exception e) {
            log.info(e.getMessage())
            assertTrue(e.getMessage().contains(noPass))
        }

        // set white list in [test1, test2]
        try {
            sql """ ADMIN SET FRONTEND CONFIG ("s3_load_endpoint_white_list" = "test1, test2"); """
            s3_load("test1")
        } catch (Exception e) {
            log.info(e.getMessage())
            assertFalse(e.getMessage().contains(noPass))
        }

        try {
            s3_load("test2")
        } catch (Exception e) {
            log.info(e.getMessage())
            assertFalse(e.getMessage().contains(noPass))
        }

        try {
            s3_load("test3")
        } catch (Exception e) {
            log.info(e.getMessage())
            assertTrue(e.getMessage().contains(noPass))
        }

        // set no white list
        try {
            sql """ ADMIN SET FRONTEND CONFIG ("s3_load_endpoint_white_list" = ""); """
            s3_load("test3")
        } catch (Exception e) {
            log.info(e.getMessage())
            assertFalse(e.getMessage().contains(noPass))
        }
    } finally {
        sql """ ADMIN SET FRONTEND CONFIG ("s3_load_endpoint_white_list" = ""); """
    }

}