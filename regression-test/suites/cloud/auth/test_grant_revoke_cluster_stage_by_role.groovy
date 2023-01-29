suite("test_grant_revoke_cluster_stage_to_role", "cloud_auth") {
    def roleName = "testRole"
    def user1 = "test_grant_revoke_cluster_stage_to_user1"

    sql """drop user if exists ${user1}"""
    sql """
         drop role if exists ${roleName}
        """

    // 1. create role
    sql """
        create role ${roleName}
        """

    // grant cluster and stage usage_priv to role
    sql """
        grant usage_priv on cluster 'clusterA' to role "${roleName}";
        """

    sql """
        grant usage_priv on stage 'stageA' to role "${roleName}";
        """

    sql """
        create user "${user1}" default role "${roleName}"
        """

    order_qt_show_role1 """
            show grants for $user1
        """

    // grant * to role
    sql """
        grant usage_priv on cluster * to role "${roleName}";
        """

    sql """
        grant usage_priv on stage * to role "${roleName}";
        """

    order_qt_show_role2 """
            show grants for $user1
        """

    // revoke cluster and stage usage_priv from role
    sql """
        revoke usage_priv on cluster 'clusterA' from role "${roleName}";
        """

    sql """
        revoke usage_priv on stage 'stageA' from role "${roleName}";
        """

    order_qt_show_role3 """
            show grants for $user1
        """

    // revoke * from role
    sql """
        revoke usage_priv on cluster * from role "${roleName}";
        """

    sql """
        revoke usage_priv on stage * from role "${roleName}";
        """

    order_qt_show_role4 """
            show grants for $user1
        """

    sql """
        drop user ${user1}
        """

    sql """
        drop role ${roleName}
        """
    
    order_qt_show_roles5 """
        show roles
    """
}

