suite("test_show_grant", "cloud_auth") {
    def user1 = "selectdb_cloud_test_test_show_grant"
    sql """drop user if exists ${user1}"""
    try {
        if (context.config.jdbcUser.equals("root")) {
            result1 = sql """show all grants"""
            boolean found = false
            for (def res : result1) {
                if (res[0] == """'root'@'%'""") {
                    found = true
                    break
                }
            }
            assertTrue(found, "root user must found in show")
        }


        // create user
        sql """create user ${user1} IDENTIFIED BY 'A12345678_';"""
        sql """GRANT Usage_priv ON CLUSTER test_cluster TO ${user1}"""
        sql """GRANT Usage_priv ON STAGE test_stage TO ${user1}"""
        sql """GRANT Grant_priv,Select_priv,Load_priv,Alter_priv,Create_priv,Drop_priv ON *.*.* TO ${user1}"""
        result2 = connect(user = "${user1}", password = 'A12345678_', url = context.config.jdbcUrl) {
            sql """show all grants"""
        }
        boolean found = false
        for (def res : result2) {
            if (res[0] == """'root'@'%'""") {
                found = true
                break
            }
        }
        assertFalse(found, "root user must found in show")

        for (def res : result2) {
            if (res[0] == """'${user1}'@'%'""") {
                log.info("found it ${res}")
                found = true
                assertTrue(res[9].contains("Cluster_Usage_priv"))
                assertTrue(res[10].contains("Stage_Usage_priv"))
            }
        }
        assertTrue(found, "${user1} user must found in show")
       
    } finally {
        sql """drop user if exists ${user1}"""
    }
}

