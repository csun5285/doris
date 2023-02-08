suite("test_grant_revoke_cluster_to_user", "cloud_auth") {
    def role = "admin"
    def user1 = "regression_test_cloud_user1"
    def user2 = "regression_test_cloud_user2"

    sql """drop user if exists ${user1}"""
    sql """drop user if exists ${user2}"""

    def getCluster = { cluster ->
        def result = sql " SHOW CLUSTERS; "
        for (int i = 0; i < result.size(); i++) {
            if (result[i][0] == cluster) {
                return result[i]
            }
        }
        return null
    }

    // 1. change user
    // ${user1} admin role
    sql """create user ${user1} identified by '12345' default role 'admin'"""
    order_qt_show_user1_grants1 """show grants for '${user1}'"""

    // ${user2} not admin role
    sql """create user ${user2} identified by '12345'"""
    // for use default_cluster:regression_test
    sql """grant select_priv on *.*.* to ${user2}"""
    order_qt_show_user2_grants2 """show grants for '${user2}'"""


    // 2. grant cluster
    def cluster1 = "clusterA"
    def result

    // admin role user can grant cluster to use
    result = connect(user = "${user1}", password = '12345', url = context.config.jdbcUrl) {
            sql """GRANT USAGE_PRIV ON CLUSTER '${cluster1}' TO '${user1}'"""
    }

    // general user can't grant cluster to use
    try {
        result = connect(user = "${user2}", password = '12345', url = context.config.jdbcUrl) {
             sql """GRANT USAGE_PRIV ON CLUSTER '${cluster1}' TO '${user1}'"""
        }
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("Access denied; you need (at least one of) the GRANT/ROVOKE privilege(s) for this operation"), e.getMessage())
    }

    def clusters = sql " SHOW CLUSTERS; "
    assertTrue(!clusters.isEmpty())
    def validCluster = clusters[0][0]

    // default cluster
    sql """SET PROPERTY FOR '${user1}' 'default_cloud_cluster' = '${validCluster}'"""
    sql """SET PROPERTY FOR '${user2}' 'default_cloud_cluster' = '${validCluster}'"""
    def show_cluster_1 = getCluster(validCluster)

    assertTrue(show_cluster_1[2].contains(user2), "Expect contain users regression_test_cloud_user2")

    // grant GRANT_PRIV to general user, he can grant cluster to other user.
    sql """grant GRANT_PRIV on *.*.* to ${user2}"""

    result = connect(user = "${user2}", password = '12345', url = context.config.jdbcUrl) {
            sql """GRANT USAGE_PRIV ON CLUSTER '${cluster1}' TO '${user2}'"""
    }
    order_qt_show_user3_grants3 """show grants for '${user2}'"""

    sql """GRANT USAGE_PRIV ON CLUSTER '${validCluster}' TO '${user2}'"""
    show_cluster_2 = connect(user = "${user2}", password = '12345', url = context.config.jdbcUrl) {
            getCluster(validCluster)
    }

    assertTrue(show_cluster_2[2].equals(user2), "Expect just only have user regression_test_cloud_user2")
    sql """REVOKE USAGE_PRIV ON CLUSTER '${validCluster}' FROM '${user2}'"""

    // 3. revoke cluster
    // admin role user can revoke cluster
    result = connect(user = "${user1}", password = '12345', url = context.config.jdbcUrl) {
            sql """REVOKE USAGE_PRIV ON CLUSTER '${cluster1}' FROM '${user1}'"""
    }

    // revoke GRANT_PRIV from general user, he can not revoke cluster to other user.
    sql """revoke GRANT_PRIV on *.*.* from ${user2}"""

    // general user can't revoke cluster
    try {
        result = connect(user = "${user2}", password = '12345', url = context.config.jdbcUrl) {
             sql """REVOKE USAGE_PRIV ON CLUSTER '${cluster1}' FROM '${user2}'"""
        }
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("Access denied; you need (at least one of) the GRANT/ROVOKE privilege(s) for this operation"), e.getMessage())
    }

    order_qt_show_user4_grants4 """show grants for '${user1}'"""

    order_qt_show_user5_grants5 """show grants for '${user2}'"""
}
