suite("test_disable_show_system_info_not_root", "cloud_auth") {
    // root
    def spb = sql("show proc '/backends'")
    assertTrue(spb.size() != 0)
    def sbs = sql("show backends")
    assertTrue(sbs.size() != 0)
    def spf = sql("show proc '/frontends'")
    assertTrue(spf.size() != 0)
    def sfs = sql("show frontends")
    assertTrue(sfs.size() != 0)

    def role= 'admin'
    def user = 'acloud_auth_test_user_jackma'
    def dbName = 'cloud_auth_test_database'
    try_sql("DROP USER if exists ${user}")
    sql """DROP DATABASE IF EXISTS ${dbName}"""
    sql """CREATE DATABASE ${dbName}"""
    sql """CREATE USER '${user}' IDENTIFIED BY 'Cloud123456' DEFAULT ROLE '${role}'"""
    sql "sync"
    def result1 = connect(user=user, password='Cloud123456', url=context.config.jdbcUrl) {
        try_sql """show backends"""
    }
    // failed: ERROR 1227 (42000): errCode = 2, detailMessage = Access denied; you need (at least one of) the OPERATOR privilege(s) for this operation
    assertEquals(result1, null)

    try {
        result1 = connect(user=user, password='Cloud123456', url=context.config.jdbcUrl) {
            sql """show proc '/current_query_stmts';"""
        }
    } catch (Exception e) {
        // assertTrue(e.getMessage().contains("Unsupported operation"), e.getMessage())
        assertTrue(false);
    }

    sql """DROP USER if exists ${user}"""
    sql """DROP DATABASE ${dbName}"""
}