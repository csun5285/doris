suite("test_show_version") {
    def result = sql """show variables like "version_comment";"""
    def strs = result[0][1].split(' ')
    assertEquals(strs.length, 4)
    assertEquals(strs[0], "SelectDB")
    assertEquals(strs[1], "Core")
    assertEquals(strs[2], "version:")
    def version = strs[3].split('.')
    def reg = /[0-9]+\.\d+\.\d+/
    assertTrue(strs[3].matches(reg))
}
