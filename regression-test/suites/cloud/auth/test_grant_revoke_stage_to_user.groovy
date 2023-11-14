suite("test_grant_revoke_stage_to_user", "cloud_auth") {
    def user1 = "regression_test_user1"
    def stage1 = "test_stage_1"
    def role = "admin"

    try_sql("DROP USER if exists ${user1}")
    sql """CREATE USER '${user1}' IDENTIFIED BY 'Cloud123456' DEFAULT ROLE '${role}'"""

    def succ1 = try_sql """
        GRANT USAGE_PRIV ON STAGE ${stage1} TO ${user1};
    """
    // OK
    assertEquals(succ1.size(), 1)

    sql "sync"

    def result1 = connect(user=user1, password='Cloud123456', url=context.config.jdbcUrl) {
        def sg = try_sql """show grants"""
        assertEquals(sg.size(), 1)
    }

    def succ3 = try_sql """
        REVOKE USAGE_PRIV ON STAGE ${stage1} FROM ${user1};
    """
    assertEquals(succ3.size(), 1)

    def succ4 = try_sql """
        DROP USER ${user1}
    """
    assertEquals(succ3.size(), 1)
}
