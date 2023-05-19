import org.codehaus.groovy.runtime.IOGroovyMethods
import groovy.json.JsonOutput

suite("test_auto_suspend_fe_metrics") {
    def getRestApi = {
        StringBuilder strBuilder = new StringBuilder()
        strBuilder.append("""curl -u """ + context.config.feHttpUser + ":" + context.config.feHttpPassword)
        strBuilder.append(""" http://""" + context.config.feHttpAddress + """/rest/v2/manager/cluster/cluster_info/cloud_cluster_status""")

        String command = strBuilder.toString()
        logger.info("get rest cluster info command=" + command)
        def process = command.toString().execute()
        def code = process.waitFor()
        def err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
        def out = process.getText()
        logger.info("Request FE Resp: code=" + code + ", out=" + out + ", err=" + err)
        assertEquals(code, 0)
        def jsonResp = parseJson(out)
        // assert cluster id > 0, must have cluster
        assertTrue(jsonResp.data.size() > 0)
    }

    getRestApi.call()

}