import org.codehaus.groovy.runtime.IOGroovyMethods
suite("test_profile_not_check_auth", "cloud_auth") {
    def getRestApiProfile = {
        StringBuilder strBuilder = new StringBuilder()
        strBuilder.append("""curl http://""" + context.config.feHttpAddress + """/api/profile?query_id=aaaa""")

        String command = strBuilder.toString()
        logger.info("get rest profile command=" + command)
        def process = command.toString().execute()
        def code = process.waitFor()
        def err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
        def out = process.getText()
        logger.info("Request FE Resp: code=" + code + ", out=" + out + ", err=" + err)
        assertEquals(code, 0)
        // assert return not contain "Unauthorized"
        assertTrue(!out.contains("Unauthorized"))
    }

    getRestApiProfile.call()
}
