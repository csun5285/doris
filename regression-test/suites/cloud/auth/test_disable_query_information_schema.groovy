suite("test_disable_query_information_schema", "cloud_auth") {
    def user1 = "information_schema_user"
    sql """drop user if exists ${user1}"""

    sql """create user ${user1} identified by '12345' default role 'admin'"""

    sql "sync"
    
    try {
        result = connect(user = "${user1}", password = '12345', url = context.config.jdbcUrl) {
             sql "set enable_nereids_planner = false"
             sql """
                select * from information_schema.rowsets;
             """
        }
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("Unsupported operation"), e.getMessage())
    }

    try {
        result = connect(user = "${user1}", password = '12345', url = context.config.jdbcUrl) {
             sql "set enable_nereids_planner = true"
             sql """
                select * from information_schema.rowsets;
             """
        }
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("Unsupported operation"), e.getMessage())
    }

    sql """drop user if exists ${user1}"""
}
