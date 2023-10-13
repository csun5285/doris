suite("test_jdbc_driver_url_white_list") {
    def catalog_name = "test_jdbc_driver_url"
    def create_jdbc = {driver_url ->
        sql """DROP CATALOG if exists ${catalog_name};"""
        sql """
            CREATE CATALOG ${catalog_name} PROPERTIES (
                "type"="jdbc",
                "user"="root",
                "password"="123456",
                "jdbc_url" = "jdbc:mysql:tmp",
                "driver_url" = "${driver_url}",
                "driver_class" = "com.mysql.jdbc.Driver"
            );
        """
    }
    def noPass = "is not in jdbc driver url white list"

    try {
        // default has no white list
        try {
            create_jdbc("anything")
        } catch (Exception e) {
            log.info(e.getMessage())
            assertFalse(e.getMessage().contains(noPass))
        }

        // set white list in [a, b, c]
        try {
            sql """ ADMIN SET FRONTEND CONFIG ("jdbc_driver_url_white_list" = "a, b, c"); """
            create_jdbc("d")
        } catch (Exception e) {
            log.info(e.getMessage())
            assertTrue(e.getMessage().contains(noPass))
        }

        // set white list in [test1, test2]
        try {
            sql """ ADMIN SET FRONTEND CONFIG ("jdbc_driver_url_white_list" = "test1, test2"); """
            create_jdbc("test1")
        } catch (Exception e) {
            log.info(e.getMessage())
            assertFalse(e.getMessage().contains(noPass))
        }

        try {
            create_jdbc("test2")
        } catch (Exception e) {
            log.info(e.getMessage())
            assertFalse(e.getMessage().contains(noPass))
        }

        try {
            create_jdbc("test3")
        } catch (Exception e) {
            log.info(e.getMessage())
            assertTrue(e.getMessage().contains(noPass))
        }

        // set no white list
        try {
            sql """ ADMIN SET FRONTEND CONFIG ("jdbc_driver_url_white_list" = ""); """
            create_jdbc("test3")
        } catch (Exception e) {
            log.info(e.getMessage())
            assertFalse(e.getMessage().contains(noPass))
        }
    } finally {
        sql """ ADMIN SET FRONTEND CONFIG ("jdbc_driver_url_white_list" = ""); """
    }

}