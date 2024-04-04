import org.codehaus.groovy.runtime.IOGroovyMethods

suite("alter_ttl_error") {
    sql """ use @regression_cluster_name1 """
    def ttlProperties = """ PROPERTIES("file_cache_ttl_seconds"="-1") """
    //doris show backends: BackendId  Host  HeartbeatPort  BePort  HttpPort  BrpcPort  ArrowFlightSqlPort  LastStartTime  LastHeartbeat  Alive  SystemDecommissioned  TabletNum  DataUsedCapacity  TrashUsedCapcacity  AvailCapacity  TotalCapacity  UsedPct  MaxDiskUsedPct  RemoteUsedCapacity  Tag  ErrMsg  Version  Status  HeartbeatFailureCounter  NodeRole
    def backends = sql_return_maparray "show backends;"
    assertTrue(backends.size() > 0)
    String backend_id;
    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    def backendId_to_backendBrpcPort = [:]
    String host = ''
    for (def backend in backends) {
        if (backend.keySet().contains('Host')) {
            host = backend.Host
        } else {
            host = backend.IP
        }
        def cloud_tag = parseJson(backend.Tag)
        if (backend.Alive.equals("true") && cloud_tag.cloud_cluster_name.contains("regression_cluster_name1")) {
            backendIdToBackendIP.put(backend.BackendId, host)
            backendIdToBackendHttpPort.put(backend.BackendId, backend.HttpPort)
            backendIdToBackendBrpcPort.put(backend.BackendId, backend.BrpcPort)
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
