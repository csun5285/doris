import org.codehaus.groovy.runtime.IOGroovyMethods

suite("alter_ttl_error") {
    sql """ use @regression_cluster_name1 """
    def ttlProperties = """ PROPERTIES("file_cache_ttl_seconds"="-1") """
    String[][] backends = sql """ show backends """
    String backendId;
    def backendIdToBackendIP = [:]
    def backendIdToBackendHttpPort = [:]
    def backendIdToBackendBrpcPort = [:]
    for (String[] backend in backends) {
        if (backend[8].equals("true") && backend[18].contains("regression_cluster_name1")) {
            backendIdToBackendIP.put(backend[0], backend[1])
            backendIdToBackendHttpPort.put(backend[0], backend[4])
            backendIdToBackendBrpcPort.put(backend[0], backend[5])
        }
    }
    assertEquals(backendIdToBackendIP.size(), 1)

    sql new File("""${context.file.parent}/../ddl/customer_ttl_delete.sql""").text


    try {
        sql (new File("""${context.file.parent}/../ddl/customer_ttl.sql""").text + ttlProperties)
        assertTrue(false)
    } catch (Exception e) {
        assertTrue(true)
    }

    ttlProperties = """ PROPERTIES("file_cache_ttl_seconds"="abcasfaf") """
    try {
        sql (new File("""${context.file.parent}/../ddl/customer_ttl.sql""").text + ttlProperties)
        assertTrue(false)
    } catch (Exception e) {
        assertTrue(true)
    }

    ttlProperties = """ PROPERTIES("file_cache_ttl_seconds"="9223372036854775810") """
    try {
        sql (new File("""${context.file.parent}/../ddl/customer_ttl.sql""").text + ttlProperties)
        assertTrue(false)
    } catch (Exception e) {
        assertTrue(true)
    }

    ttlProperties = """ PROPERTIES("file_cache_ttl_seconds"="3600") """
    sql (new File("""${context.file.parent}/../ddl/customer_ttl.sql""").text + ttlProperties)

    try {
        sql """ ALTER TABLE customer_ttl SET ("file_cache_ttl_seconds"="-2414") """
        assertTrue(false)
    } catch (Exception e) {
        assertTrue(true)
    }

    try {
        sql """ ALTER TABLE customer_ttl SET ("file_cache_ttl_seconds"="abs-") """
        assertTrue(false)
    } catch (Exception e) {
        assertTrue(true)
    }

    try {
        sql """ ALTER TABLE customer_ttl SET ("file_cache_ttl_seconds"="9223372036854775810") """
        assertTrue(false)
    } catch (Exception e) {
        assertTrue(true)
    }
}
