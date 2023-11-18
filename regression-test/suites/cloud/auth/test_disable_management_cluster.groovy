suite("test_disable_management_cluster", "cloud_auth") {
    def host = "127.0.0.1"
    def heart_port = 10086
    def edit_log_port = 10000
    def user1 = "regression_test_cloud_user1"
    sql """drop user if exists ${user1}"""

    // 1. change user
    // ${user1} admin role
    sql """create user ${user1} identified by 'Cloud12345' default role 'admin'"""

    sql "sync"

    try {
        result = connect(user = "${user1}", password = 'Cloud12345', url = context.config.jdbcUrl) {
             sql """
                ALTER SYSTEM ADD BACKEND "${host}:${heart_port}"
             """
        }
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("Unsupported operation"), e.getMessage())
    }

    try {
        result = connect(user = "${user1}", password = 'Cloud12345', url = context.config.jdbcUrl) {
             sql """
                ALTER SYSTEM ADD FOLLOWER "${host}:${edit_log_port}"
             """
        }
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("Unsupported operation"), e.getMessage())
    }

    try {
        result = connect(user = "${user1}", password = 'Cloud12345', url = context.config.jdbcUrl) {
             sql """
                ALTER SYSTEM DROP BACKEND "${host}:${heart_port}"
             """
        }
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("Unsupported operation"), e.getMessage())
    }

    try {
        result = connect(user = "${user1}", password = 'Cloud12345', url = context.config.jdbcUrl) {
             sql """
                ALTER SYSTEM DROP FOLLOWER "${host}:${edit_log_port}"
             """
        }
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("Unsupported operation"), e.getMessage())
    }
    sql """drop user if exists ${user1}"""
}
