suite("test_show_version") {
    def result = sql """show variables like "version_comment";"""
    def re = /SelectDB Core version: (\d+\.\d+(\.\d+){0,2}(-[0-9a-fA-F]+)?) \(based on Apache Doris (\d+\.\d+(\.\d+){0,2})\)/
    log.info('show version result: ' + result[0][1])
    log.info('matching pattern: ' + re)
    assertTrue(result[0][1].matches(re))
}
