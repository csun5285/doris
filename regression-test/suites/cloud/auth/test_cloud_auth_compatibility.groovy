import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_cloud_auth_compatibility", "cloud_auth") {
    def user = "selectdb_cloud_test_cloud_auth_compatibility_not_drop"
    def pwd = 'Cloud12345'
    def dbName = 'selectdb_cloud_auth_not_drop_db'
    def tableName = 'userinfo'
    def clusterName = 'selectdbCloudTestCloudAuthCompatibilityCluster'
    def stageName = 'selectdbCloudTestCloudAuthCompatibilityStage'
    // not drop user, db, table for test compatibility
    sql """CREATE DATABASE IF NOT EXISTS ${dbName}"""
    sql """USE ${dbName}"""
    sql """
        CREATE TABLE IF NOT EXISTS ${dbName}.${tableName} (
        `id` int(11) COMMENT "",
        `username` varchar(256) NULL,
        `userid` varchar(256) NULL
        )
        UNIQUE KEY(id)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 1;
    """

    def getUserId = {
        // get userId
        def fileName = "internal_customer.csv"
        def filePath = "${context.config.dataPath}/cloud/smoke/copy_into/" + fileName

        StringBuilder strBuilder = new StringBuilder()
        strBuilder.append("""curl -u """ + "${user}" + ":" + "${pwd}")
        strBuilder.append(""" -H fileName:justGetuserId""")
        strBuilder.append(""" -T """ + filePath)
        strBuilder.append(""" -L http://""" + context.config.feCloudHttpAddress + """/copy/upload --connect-timeout 5 -vv""")
        String command = strBuilder.toString()
        logger.info("upload command=" + command)
        def process = command.toString().execute()
        def code = process.waitFor()
        def err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())))
        def out = process.getText()
        logger.info("Http data api Request FE Config: code=" + code + ", out=" + out + ", err=" + err)
        location = null
        err.eachLine {if(it.startsWith("< Location")) {
            location = it
        }}
        if (null == location) {
            assertTrue(false)
        }
        // https://gavin-test-bj-1308700295.cos.ap-beijing.myqcloud.com/dx-test/stage/selectdb_cloud_test_cloud_auth_compatibility_not_drop/572e03b2-e608-427f-95d6-773423a51dd0/justGetuserId?q-sign-algorithm=sha1&q-ak=AKIDsZHqgyhDSRBpDONtHPHua6MRUN0Wnpci&q-sign-time=1708569555%3B1708573155&
        // q-key-time=1708569555%3B1708573155&q-header-list=host&q-url-param-list=&q-signature=1d6e30d9f225de9f90870b495c3707f81e1d4747

        // get userid by regex
        log.info("location="+location)
        def pattern = /selectdb_cloud_test_cloud_auth_compatibility_not_drop\/([a-f0-9-]+)\/justGetuserId/
        def matcher = (location =~ pattern)
        def userid = null
        if (matcher.find()) {
            userid = matcher.group(1)
        } 
        assertNotNull(userid, "can't get userid")
        userid
    }

    // check has been created ${user}
    def userCreated = true

    try {
        def result = sql """SHOW GRANTS FOR ${user}"""
    } catch (Exception e) {
        // User: 'default_cluster:${user}'@'%' does not exist
        assertTrue(e.getMessage().contains("does not exist"), e.getMessage())
        def result = sql """SELECT * FROM ${dbName}.${tableName} WHERE id = 1"""
        logger.info("result="+result)
        if (result.size() == 0) {
            userCreated = false
        }
    }

    if (!userCreated) {
        sql """CREATE USER IF NOT EXISTS '${user}'"""
        sql """SET PASSWORD FOR '${user}' = PASSWORD('${pwd}')"""
        sql """GRANT USAGE_PRIV ON CLUSTER '${clusterName}' TO '${user}'"""
        sql """GRANT USAGE_PRIV ON STAGE '${stageName}' TO '${user}'"""
        def userId = getUserId()
        sql """INSERT INTO ${dbName}.${tableName} VALUES (1, "${user}", "${userId}")"""
    }

    // check some info
    def result = sql_return_maparray """SHOW GRANTS FOR ${user}"""
    assertEquals("[${clusterName}: Cluster_Usage_priv ]" as String, result.CloudClusterPrivs as String)
    assertEquals("[${stageName}: Stage_Usage_priv ]" as String, result.CloudStagePrivs as String)
    def userId = getUserId()
    result = sql_return_maparray """SELECT * FROM ${dbName}.${tableName} WHERE id = 1"""
    assertEquals(result[0].username as String, user as String)
    assertEquals(result[0].userid, userId)
}

