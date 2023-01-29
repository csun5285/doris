suite("test_disable_query_information_schema", "cloud_auth") {
    def user1 = "information_schema_user"
    sql """drop user if exists ${user1}"""

    sql """create user ${user1} identified by '12345' default role 'admin'"""

    try {
        result = connect(user = "${user1}", password = '12345', url = context.config.jdbcUrl) {
             sql """
                select * from information_schema.backends;
             """
        }
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("Unsupported operation"), e.getMessage())
    }

    try {
        result = connect(user = "${user1}", password = '12345', url = context.config.jdbcUrl) {
             sql """
                select BackendId,IP from (select * from information_schema.backends as t1) as t2;
             """
        }
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("Unsupported operation"), e.getMessage())
    }

    try {
        result = connect(user = "${user1}", password = '12345', url = context.config.jdbcUrl) {
             sql """
                select * from information_schema.rowsets;
             """
        }
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("Unsupported operation"), e.getMessage())
    }

    sql """drop user if exists ${user1}"""
}
