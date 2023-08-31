import groovy.json.JsonOutput
suite("cloud_cluster_test", "cloud_cluster") {
    def token = "greedisgood9999"
    def instance_id = "instance_id_deadbeef_cluster"
    def name = "user_1"
    def user_id = "10000"

    // create instance
    /*
        curl -X GET '127.0.0.1:5000/MetaService/http/create_instance?token=greedisgood9999' -d '{
            "instance_id": "instance_id_deadbeef",
            "name": "user_1",
            "user_id": "10000",
            "obj_info": {
                "ak": "test-ak1",
                "sk": "test-sk1",
                "bucket": "test-bucket",
                "prefix": "test-prefix",
                "endpoint": "test-endpoint",
                "region": "test-region",
                "provider" : "BOS",
                "external_endpoint" : "endpoint"
            }
        }'
     */

    def jsonOutput = new JsonOutput()
    def s3 = [ak: "test-ak1",
              sk : "test-sk1",
              bucket : "test-bucket",
              prefix: "test-prefix",
              endpoint: "test-endpoint",
              region: "test-region",
              provider : "BOS",
              'external_endpoint': "test-external-endpoint"]
    def map = [instance_id: "${instance_id}", name: "${name}", user_id: "${user_id}", obj_info: s3]
    def js = jsonOutput.toJson(map)

    def create_instance_api = { request_body, check_func ->
        httpTest {
            endpoint context.config.metaServiceHttpAddress
            uri "/MetaService/http/create_instance?token=$token"
            body request_body
            check check_func
        }
    }

    create_instance_api.call(js) {
        respCode, body ->
            log.info("http cli result: ${body} ${respCode}".toString())
            def json = parseJson(body)
            assertTrue(json.code.equalsIgnoreCase("OK") || json.code.equalsIgnoreCase("ALREADY_EXISTED"))
    }

    def clusterName = "cluster_name1"
    def clusterId = "cluster_id1"
    def opType = "COMPUTE"
    def cloudUniqueId = "cloud_unique_id_compute_node0"
    def ip = "172.0.0.10"
    def heartbeatPort = 9050
    def ip2 = "172.0.0.11"
    def clusterName2 = "cluster_name2"
    def clusterId2 = "cluster_id2"
    def nodeMap = [cloud_unique_id: "${cloudUniqueId}", ip: "${ip}", heartbeat_port: "${heartbeatPort}"]
    def nodeList = [nodeMap]
    def clusterMap = [cluster_name: "${clusterName}", cluster_id:"${clusterId}", nodes:nodeList]
    def instance = [instance_id: "${instance_id}", op:"${opType}", cluster: clusterMap]


    // add no be cluster
    /*
        curl -X GET http://127.0.0.1:5000/MetaService/http/add_cluster?token=greedisgood9999 -d '{
            "instance_id": "instance_id_deadbeef",
            "cluster": {
                "cluster_name": "cluster_name1",
                "cluster_id": "cluster_id1",
                "type": "COMPUTE",
                "nodes": []
            }
        }'
     */
    nodeList = []
    clusterMap = [cluster_name: "${clusterName}", cluster_id:"${clusterId}", type:"COMPUTE", nodes:nodeList]
    instance = [instance_id: "${instance_id}", cluster: clusterMap]
    jsonOutput = new JsonOutput()
    js = jsonOutput.toJson(instance)

    def add_cluster_api = { request_body, check_func ->
        httpTest {
            endpoint context.config.metaServiceHttpAddress
            uri "/MetaService/http/add_cluster?token=$token"
            body request_body
            check check_func
        }
    }

    add_cluster_api.call(js) {
        respCode, body ->
            log.info("http cli result: ${body} ${respCode}".toString())
            def json = parseJson(body)
            assertTrue(json.code.equalsIgnoreCase("OK") || json.code.equalsIgnoreCase("ALREADY_EXISTED"))
    }

    jsonOutput = new JsonOutput()
    js = jsonOutput.toJson(instance)

    // drop cluster
    /*
        curl -X GET http://127.0.0.1:5000/MetaService/http/drop_cluster?token=greedisgood9999 -d '{
            "instance_id": "instance_id_deadbeef",
            "cluster": {
                "cluster_name": "cluster_name1",
                "cluster_id": "cluster_id1"
            }
        }'
     */

    def drop_cluster_api = { request_body, check_func ->
        httpTest {
            endpoint context.config.metaServiceHttpAddress
            uri "/MetaService/http/drop_cluster?token=$token"
            body request_body
            check check_func
        }
    }

    drop_cluster_api.call(js) {
        respCode, body ->
            log.info("http cli result: ${body} ${respCode}".toString())
            def json = parseJson(body)
            assertTrue(json.code.equalsIgnoreCase("OK") || json.code.equalsIgnoreCase("INTERANAL_ERROR"))
    }

    nodeMap = [cloud_unique_id: "${cloudUniqueId}", ip: "${ip}", heartbeat_port: "${heartbeatPort}"]
    nodeList = [nodeMap]
    clusterMap = [cluster_name: "${clusterName}", cluster_id:"${clusterId}", type:"COMPUTE", nodes:nodeList]
    instance = [instance_id: "${instance_id}", cluster: clusterMap]
    jsonOutput = new JsonOutput()
    js = jsonOutput.toJson(instance)
    def js1 = js
    // add_cluster has one node
    /*
         curl '127.0.0.1:5000/MetaService/http/add_cluster?token=greedisgood9999' -d '{
             "instance_id":"instance_id_deadbeef",
             "cluster":{
                 "cluster_name":"cluster_name1",
                 "cluster_id":"cluster_id1",
                 "type" : "COMPUTE",
                 "nodes":[
                     {
                         "cloud_unique_id":"cloud_unique_id_compute_node0",
                         "ip":"172.0.0.10",
                         "heartbeat_port":9050
                     }
                 ]
             }
         }'
     */

    add_cluster_api.call(js) {
        respCode, body ->
            log.info("http cli result: ${body} ${respCode}".toString())
            def json = parseJson(body)
            assertTrue(json.code.equalsIgnoreCase("OK") || json.code.equalsIgnoreCase("INTERANAL_ERROR"))
    }

    // get instance's s3 info
    /*
        curl '127.0.0.1:5000/MetaService/http/get_obj_store_info?token=greedisgood9999' -d '{
             "cloud_unique_id":"cloud_unique_id_compute_node0"
         }'
     */
    def get_obj_store_info_api_body = [cloud_unique_id:"${cloudUniqueId}"]
    jsonOutput = new JsonOutput()
    js = jsonOutput.toJson(get_obj_store_info_api_body)
    def get_obj_store_info_api = { request_body, check_func ->
        httpTest {
            endpoint context.config.metaServiceHttpAddress
            uri "/MetaService/http/get_obj_store_info?token=$token"
            body request_body
            check check_func
        }
    }

    get_obj_store_info_api.call(js) {
        respCode, body ->
            log.info("http cli result: ${body} ${respCode}".toString())
            def json = parseJson(body)
            assertTrue(json.code.equalsIgnoreCase("OK") || json.code.equalsIgnoreCase("INVALID_ARGUMENT"))
    }

    // update instance's s3 info
    /*
        curl '127.0.0.1:5000/MetaService/http/update_ak_sk?token=greedisgood9999' -d '{
            "cloud_unique_id": "cloud_unique_id_compute_node0",
            "obj": {
                "id": "1",
                "ak": "test-ak1-updated",
                "sk": "test-sk1-updated"
            }
        }'
     */
    def update_ak_sk_api_body = [cloud_unique_id:"${cloudUniqueId}", obj:[id:"1", ak:"test-ak1-updated", sk:"test-sk1-updated"]]
    jsonOutput = new JsonOutput()
    js = jsonOutput.toJson(update_ak_sk_api_body)
    def update_ak_sk_api = { request_body, check_func ->
        httpTest {
            endpoint context.config.metaServiceHttpAddress
            uri "/MetaService/http/update_ak_sk?token=$token"
            body request_body
            check check_func
        }
    }

    update_ak_sk_api.call(js) {
        respCode, body ->
            log.info("http cli result: ${body} ${respCode}".toString())
            def json = parseJson(body)
            assertTrue(json.code.equalsIgnoreCase("OK") || json.code.equalsIgnoreCase("INVALID_ARGUMENT"))
    }

    // add s3 info to instance
    /*
        curl '127.0.0.1:5000/MetaService/http/add_obj_info?token=greedisgood9999' -d '{
            "cloud_unique_id": "cloud_unique_id_compute_node0",
            "obj": {
                "ak": "test-ak2",
                "sk": "test-sk2",
                "bucket": "test-bucket",
                "prefix": "test-prefix",
                "endpoint": "test-endpoint",
                "region": "test-region"
            }
        }'
     */

    def add_obj_info_api_body = [cloud_unique_id:"${cloudUniqueId}",
                                 obj:[ak:"test-ak2", sk:"test-sk2", bucket:"test-bucket",
                                      prefix: "test-prefix", endpoint: "test-endpoint", region:"test-region"]]
    jsonOutput = new JsonOutput()
    js = jsonOutput.toJson(add_obj_info_api_body)
    def add_obj_info_api = { request_body, check_func ->
        httpTest {
            endpoint context.config.metaServiceHttpAddress
            uri "/MetaService/http/add_obj_info?token=$token"
            body request_body
            check check_func
        }
    }

    add_obj_info_api.call(js) {
        respCode, body ->
            log.info("http cli result: ${body} ${respCode}".toString())
            def json = parseJson(body)
            assertTrue(json.code.equalsIgnoreCase("OK") || json.code.equalsIgnoreCase("INVALID_ARGUMENT"))
    }

    // add again, failed

    add_cluster_api.call(js1) {
        respCode, body ->
            log.info("http cli result: ${body} ${respCode}".toString())
            def json = parseJson(body)
            assertTrue(json.code.equalsIgnoreCase("ALREADY_EXISTED"))
    }

    nodeList = [nodeMap]
    instance = [instance_id: "${instance_id}", cluster: clusterMap]
    jsonOutput = new JsonOutput()
    js = jsonOutput.toJson(instance)

    /*
        curl '127.0.0.1:5000/MetaService/http/add_cluster?token=greedisgood9999' -d '{
             "instance_id":"instance_id_deadbeef",
             "cluster":{
                 "cluster_name":"cluster_name1",
                 "cluster_id":"cluster_id1",
                 "type" : "COMPUTE",
                 "nodes":[
                     {
                         "cloud_unique_id":"cloud_unique_id_compute_node0",
                         "ip":"172.0.0.10",
                         "heartbeat_port":9050
                     }
                 ]
             }
         }'
     */
    // use a new instance_id add a cloud_unique_id node has been used, failed
    // err: try to add a existing cluster id, existing_cluster_id=cluster_id1
    add_cluster_api.call(js) {
        respCode, body ->
            log.info("http cli result: ${body} ${respCode}".toString())
            def json = parseJson(body)
            assertTrue(json.code.equalsIgnoreCase("ALREADY_EXISTED"))
            assertTrue(json.msg.startsWith("try to add a existing cluster id"))
    }

    // get_cluster by cluster name
    /*
        curl '127.0.0.1:5000/MetaService/http/get_cluster?token=greedisgood9999' -d '{
            "cluster_name": "cluster_name1",
            "cloud_unique_id": "cloud_unique_id_compute_node0"
        }'
     */
    def get_cluster_by_name = [cluster_name: "${clusterName}", cloud_unique_id: "${cloudUniqueId}"]
    jsonOutput = new JsonOutput()
    js = jsonOutput.toJson(get_cluster_by_name)

    def get_cluster_api = { request_body, check_func ->
        httpTest {
            endpoint context.config.metaServiceHttpAddress
            uri "/MetaService/http/get_cluster?token=$token"
            body request_body
            check check_func
        }
    }

    get_cluster_api.call(js) {
        respCode, body ->
            def json = parseJson(body)
            log.info("http cli result: ${body} ${respCode} ${json}".toString())
            assertTrue(json.code.equalsIgnoreCase("OK") || json.code.equalsIgnoreCase("NOT_FOUND"))
            if (json.code.equalsIgnoreCase("OK")) {
                assertTrue(json.msg.equalsIgnoreCase(""))
            }
    }

    // get_cluster by cluster id
    /*
        curl '127.0.0.1:5000/MetaService/http/get_cluster?token=greedisgood9999' -d '{
            "cluster_id": "cluster_id1",
            "cloud_unique_id": "cloud_unique_id_compute_node0"
        }'
    */
    def get_cluster_by_id = [cluster_id: "${clusterId}", cloud_unique_id: "${cloudUniqueId}"]
    jsonOutput = new JsonOutput()
    js = jsonOutput.toJson(get_cluster_by_id)

    get_cluster_api.call(js) {
        respCode, body ->
            def json = parseJson(body)
            log.info("http cli result: ${body} ${respCode} ${json}".toString())
            assertTrue(json.code.equalsIgnoreCase("OK") || json.code.equalsIgnoreCase("NOT_FOUND"))
            if (json.code.equalsIgnoreCase("OK")) {
                assertTrue(json.msg.equalsIgnoreCase(""))
            }
    }

    // add default name to cluster
    /*
        curl '127.0.0.1:5000/MetaService/http/update_cluster_mysql_user_name?token=greedisgood9999' -d '{
            "instance_id":"instance_id_deadbeef",
            "cluster":{
                "cluster_name":"cluster_name1",
                "cluster_id":"cluster_id1",
                "mysql_user_name": [
                    "jack",
                    "root"
                ]
            }
        }'
     */
    def default_name_cluster = [cluster_name:"$clusterName", cluster_id:"$clusterId", mysql_user_name:["jack", "root"]]
    def default_name_cluster_body = [instance_id:"$instance_id", cluster: default_name_cluster]
    jsonOutput = new JsonOutput()
    js = jsonOutput.toJson(default_name_cluster_body)
    def set_default_user_to_cluster_api = { request_body, check_func ->
        httpTest {
            endpoint context.config.metaServiceHttpAddress
            uri "/MetaService/http/update_cluster_mysql_user_name?token=$token"
            body request_body
            check check_func
        }
    }

    set_default_user_to_cluster_api.call(js) {
        respCode, body ->
            def json = parseJson(body)
            log.info("http cli result: ${body} ${respCode} ${json}".toString())
            assertTrue(json.code.equalsIgnoreCase("OK"))
    }

    // get_cluster by cluster id
    /*
        curl '127.0.0.1:5000/MetaService/http/get_cluster?token=greedisgood9999' -d '{
            "mysql_user_name": "jack",
            "cloud_unique_id": "cloud_unique_id_compute_node0"
        }'
    */
    def get_cluster_by_mysql_user_id = [mysql_user_name: "jack", cloud_unique_id: "${cloudUniqueId}"]
    jsonOutput = new JsonOutput()
    js = jsonOutput.toJson(get_cluster_by_mysql_user_id)

    get_cluster_api.call(js) {
        respCode, body ->
            def json = parseJson(body)
            log.info("http cli result: ${body} ${respCode} ${json}".toString())
            assertTrue(json.code.equalsIgnoreCase("OK") || json.code.equalsIgnoreCase("NOT_FOUND"))
            if (json.code.equalsIgnoreCase("OK")) {
                assertTrue(json.msg.equalsIgnoreCase(""))
            }
    }

    // add nodes
    /*
        curl '127.0.0.1:5000/MetaService/http/add_node?token=greedisgood9999' -d '{
            "instance_id": "instance_id_deadbeef",
            "cluster": {
                "cluster_name": "cluster_name1",
                "cluster_id": "cluster_id1",
                "type": "COMPUTE",
                "nodes": [
                    {
                        "cloud_unique_id": "cloud_unique_id_compute_node1",
                        "ip": "172.0.0.11",
                        "heartbeat_port": 9050
                    },
                    {
                        "cloud_unique_id": "cloud_unique_id_compute_node2",
                        "ip": "172.0.0.12",
                        "heartbeat_port": 9050
                    }
                ]
            }
        }'
     */
    def node1 = [cloud_unique_id: "cloud_unique_id_compute_node1", ip :"172.0.0.11", heartbeat_port: 9050]
    def node2 = [cloud_unique_id: "cloud_unique_id_compute_node2", ip :"172.0.0.12", heartbeat_port: 9050]
    def add_nodes = [node1, node2]
    def add_nodes_cluster = [cluster_name: "${clusterName}", cluster_id: "${clusterId}", type: "COMPUTE", nodes: add_nodes]
    def add_nodes_body = [instance_id: "${instance_id}", cluster: add_nodes_cluster]
    jsonOutput = new JsonOutput()
    js = jsonOutput.toJson(add_nodes_body)

    def add_node_api = { request_body, check_func ->
        httpTest {
            endpoint context.config.metaServiceHttpAddress
            uri "/MetaService/http/add_node?token=$token"
            body request_body
            check check_func
        }
    }

    add_node_api.call(js) {
        respCode, body ->
            def json = parseJson(body)
            log.info("http cli result: ${body} ${respCode} ${json}".toString())
            assertTrue(json.code.equalsIgnoreCase("OK") || json.code.equalsIgnoreCase("INTERANAL_ERROR"))
            if (!json.code.equalsIgnoreCase("OK")) {
                assertTrue(json.msg.startsWith("cloud_unique_id is already occupied by an instance"))
            }
    }

    // get cluster
    jsonOutput = new JsonOutput()
    js = jsonOutput.toJson(get_cluster_by_mysql_user_id)

    get_cluster_api.call(js) {
        respCode, body ->
            def json = parseJson(body)
            log.info("http cli result: ${body} ${respCode} ${json}".toString())
            assertTrue(json.code.equalsIgnoreCase("OK"))
    }

    // drop nodes
    /*
        curl '127.0.0.1:5000/MetaService/http/drop_node?token=greedisgood9999' -d '{
            "instance_id": "instance_id_deadbeef",
            "cluster": {
                "cluster_name": "cluster_name1",
                "cluster_id": "cluster_id1",
                "type": "COMPUTE",
                "nodes": [
                    {
                        "cloud_unique_id": "cloud_unique_id_compute_node1",
                        "ip": "172.0.0.11",
                        "heartbeat_port": 9050
                    },
                    {
                        "cloud_unique_id": "cloud_unique_id_compute_node2",
                        "ip": "172.0.0.12",
                        "heartbeat_port": 9050
                    }
                ]
            }
        }'
     */
    def del_nodes = [node1, node2]
    def del_nodes_cluster = [cluster_name: "${clusterName}", cluster_id: "${clusterId}", type: "COMPUTE", nodes: del_nodes]
    def del_nodes_body = [instance_id: "${instance_id}", cluster: del_nodes_cluster]
    jsonOutput = new JsonOutput()
    js = jsonOutput.toJson(del_nodes_body)
    def drop_node_api = { request_body, check_func ->
        httpTest {
            endpoint context.config.metaServiceHttpAddress
            uri "/MetaService/http/drop_node?token=$token"
            body request_body
            check check_func
        }
    }

    drop_node_api.call(js) {
        respCode, body ->
            def json = parseJson(body)
            log.info("http cli result: ${body} ${respCode} ${json}".toString())
            assertTrue(json.code.equalsIgnoreCase("OK") || json.code.equalsIgnoreCase("INTERANAL_ERROR"))
            if (!json.code.equalsIgnoreCase("OK")) {
                assertTrue(json.msg.startsWith("cloud_unique_id is already occupied by an instance"))
            }
    }

    // rename cluster
    /*
         curl '127.0.0.1:5000/MetaService/http/rename_cluster?token=greedisgood9999' -d '{
             "instance_id":"instance_id_deadbeef",
             "cluster":{
                 "cluster_name":"cluster_name1_renamed",
                 "cluster_id":"cluster_id1"
             }
         }'
     */
    def rename_node_api = { request_body, check_func ->
        httpTest {
            endpoint context.config.metaServiceHttpAddress
            uri "/MetaService/http/rename_cluster?token=$token"
            body request_body
            check check_func
        }
    }

    clusterMap = [cluster_name: "cluster_name1_renamed", cluster_id:"${clusterId}"]
    instance = [instance_id: "${instance_id}", cluster: clusterMap]
    jsonOutput = new JsonOutput()
    js = jsonOutput.toJson(instance)
    rename_node_api.call(js) {
        respCode, body ->
            def json = parseJson(body)
            log.info("http cli result: ${body} ${respCode} ${json}".toString())
            assertTrue(json.code.equalsIgnoreCase("OK") || json.code.equalsIgnoreCase("INTERANAL_ERROR"))
            if (!json.code.equalsIgnoreCase("OK")) {
                assertTrue(json.msg.startsWith("cloud_unique_id is already occupied by an instance"))
            }
    }

    clusterMap = [cluster_name: "${clusterName}", cluster_id:"${clusterId}"]
    instance = [instance_id: "${instance_id}", cluster: clusterMap]
    jsonOutput = new JsonOutput()
    js = jsonOutput.toJson(instance)
    rename_node_api.call(js) {
        respCode, body ->
            def json = parseJson(body)
            log.info("http cli result: ${body} ${respCode} ${json}".toString())
            assertTrue(json.code.equalsIgnoreCase("OK") || json.code.equalsIgnoreCase("INTERANAL_ERROR"))
            if (!json.code.equalsIgnoreCase("OK")) {
                assertTrue(json.msg.startsWith("cloud_unique_id is already occupied by an instance"))
            }
    }

    nodeMap1 = [cloud_unique_id: "${cloudUniqueId}", ip: "${ip2}", heartbeat_port: "${heartbeatPort}"]
    nodeList1 = [nodeMap1]
    clusterMap1 = [cluster_name: "${clusterName2}", cluster_id:"${clusterId2}", type:"COMPUTE", nodes:nodeList1]
    instance = [instance_id: "${instance_id}", cluster: clusterMap1]
    jsonOutput = new JsonOutput()
    js2 = jsonOutput.toJson(instance)
    // add_cluster has one node
    /*
         curl '127.0.0.1:5000/MetaService/http/add_cluster?token=greedisgood9999' -d '{
             "instance_id":"instance_id_deadbeef",
             "cluster":{
                 "cluster_name":"cluster_name2",
                 "cluster_id":"cluster_id2",
                 "type" : "COMPUTE",
                 "nodes":[
                     {
                         "cloud_unique_id":"cloud_unique_id_compute_node0",
                         "ip":"172.0.0.11",
                         "heartbeat_port":9050
                     }
                 ]
             }
         }'
     */

    add_cluster_api.call(js2) {
        respCode, body ->
            log.info("http cli result: ${body} ${respCode}".toString())
            def json = parseJson(body)
            assertTrue(json.code.equalsIgnoreCase("OK") || json.code.equalsIgnoreCase("INTERANAL_ERROR"))
    }

    // failed, failed to rename cluster, a cluster with the same name already exists in this instance
    clusterMap = [cluster_name: "${clusterName}", cluster_id:"${clusterId2}"]
    instance = [instance_id: "${instance_id}", cluster: clusterMap]
    jsonOutput = new JsonOutput()
    js = jsonOutput.toJson(instance)
    rename_node_api.call(js) {
        respCode, body ->
            def json = parseJson(body)
            log.info("http cli result: ${body} ${respCode} ${json}".toString())
            assertTrue(json.code.equalsIgnoreCase("INVALID_ARGUMENT"))
    }

    // get cluster status
    /*
         curl '127.0.0.1:5000/MetaService/http/get_cluster_status?token=greedisgood9999' -d '{
            "instance_ids":["instance_id_deadbeef"]
         }'
     */
    def get_cluster_status = { request_body, check_func ->
        httpTest {
            endpoint context.config.metaServiceHttpAddress
            uri "/MetaService/http/get_cluster_status?token=$token"
            body request_body
            check check_func
        }
    }

    // set cluster status
    /*
         curl '127.0.0.1:5000/MetaService/http/set_cluster_status?token=greedisgood9999' -d '{
            "instance_id": "instance_id_deadbeef",
            "cluster": {
                "cluster_id": "test_cluster_1_id1",
                "cluster_status":"STOPPED"
            }
         }'
     */
    def set_cluster_status = { request_body, check_func ->
        httpTest {
            endpoint context.config.metaServiceHttpAddress
            uri "/MetaService/http/set_cluster_status?token=$token"
            body request_body
            check check_func
        }
    }

    def getClusterInstance = [instance_ids: ["${instance_id}"]]
    jsonOutput = new JsonOutput()
    js = jsonOutput.toJson(getClusterInstance)
    get_cluster_status.call(js) {
        respCode, body ->
            def json = parseJson(body)
            log.info("http cli result: ${body} ${respCode} ${json}".toString())
            assertTrue(json.code.equalsIgnoreCase("OK"))
    }

    clusterMap = [cluster_id:"${clusterId2}", cluster_status:"SUSPENDED"]
    instance = [instance_id: "${instance_id}", cluster: clusterMap]
    jsonOutput = new JsonOutput()
    js = jsonOutput.toJson(instance)
    set_cluster_status.call(js) {
        respCode, body ->
            def json = parseJson(body)
            log.info("http cli result: ${body} ${respCode} ${json}".toString())
            assertTrue(json.code.equalsIgnoreCase("OK"))
    }

    // failed to set cluster status, status eq original status, original cluster is NORMAL
    set_cluster_status.call(js) {
        respCode, body ->
            def json = parseJson(body)
            log.info("http cli result: ${body} ${respCode} ${json}".toString())
            assertTrue(json.code.equalsIgnoreCase("INVALID_ARGUMENT"))
    }


    // failed to set cluster status, original cluster is SUSPENDED and want set UNKNOWN
    clusterMap = [cluster_id:"${clusterId2}", cluster_status:"UNKNOWN"]
    instance = [instance_id: "${instance_id}", cluster: clusterMap]
    jsonOutput = new JsonOutput()
    js = jsonOutput.toJson(instance)
    set_cluster_status.call(js) {
        respCode, body ->
            def json = parseJson(body)
            log.info("http cli result: ${body} ${respCode} ${json}".toString())
            assertTrue(json.code.equalsIgnoreCase("INVALID_ARGUMENT"))
    }

    // drop cluster
    /*
         curl '127.0.0.1:5000/MetaService/http/drop_cluster?token=greedisgood9999' -d '{
             "instance_id":"instance_id_deadbeef",
             "cluster":{
                 "cluster_name":"cluster_name1",
                 "cluster_id":"cluster_id1"
             }
         }'
     */
    clusterMap = [cluster_name: "${clusterName}", cluster_id:"${clusterId}"]
    instance = [instance_id: "${instance_id}", cluster: clusterMap]
    jsonOutput = new JsonOutput()
    js = jsonOutput.toJson(instance)
    drop_cluster_api.call(js) {
        respCode, body ->
            log.info("http cli result: ${body} ${respCode}".toString())
            def json = parseJson(body)
            assertTrue(json.code.equalsIgnoreCase("OK") || json.code.equalsIgnoreCase("INTERANAL_ERROR"))
    }

    // drop cluster_id2
    clusterMap = [cluster_id:"${clusterId2}"]
    instance = [instance_id: "${instance_id}", cluster: clusterMap]
    jsonOutput = new JsonOutput()
    js = jsonOutput.toJson(instance)
    drop_cluster_api.call(js) {
        respCode, body ->
            log.info("http cli result: ${body} ${respCode}".toString())
            def json = parseJson(body)
            assertTrue(json.code.equalsIgnoreCase("OK") || json.code.equalsIgnoreCase("INTERANAL_ERROR"))
    }

    // drop not exist cluster, falied
    /*
         curl '127.0.0.1:5000/MetaService/http/drop_cluster?token=greedisgood9999' -d '{
             "instance_id":"instance_id_deadbeef",
             "cluster":{
                 "cluster_name":"not_exist_cluster_name",
                 "cluster_id":"not_exist_cluster_name"
             }
         }'
     */
    clusterMap = [cluster_name: "not_exist_cluster_name", cluster_id:"not_exist_cluster_id", nodes:nodeList]
    instance = [instance_id: "${instance_id}", cluster: clusterMap]
    jsonOutput = new JsonOutput()
    js = jsonOutput.toJson(instance)
    drop_cluster_api.call(js) {
        respCode, body ->
            log.info("http cli result: ${body} ${respCode}".toString())
            def json = parseJson(body)
            assertTrue(json.code.equalsIgnoreCase("NOT_FOUND"))
    }

    // after drop, get cluster again, failed
    instance = [cloud_unique_id: "${cloudUniqueId}", cluster_id: "${clusterId}"]
    jsonOutput = new JsonOutput()
    js = jsonOutput.toJson(instance)
    get_cluster_api.call(js) {
        respCode, body ->
            def json = parseJson(body)
            log.info("http cli result: ${body} ${respCode} ${json}".toString())
            assertTrue(json.code.equalsIgnoreCase("INVALID_ARGUMENT"))
    }

    // add node to another cluster
    /*
        curl '127.0.0.1:5000/MetaService/http/add_cluster?token=greedisgood9999' -d '{
             "instance_id":"instance_id_deadbeef",
             "cluster":{
                 "cluster_name":"cluster_name2",
                 "cluster_id":"cluster_name2",
                 "type" : "COMPUTE",
                 "nodes":[
                     {
                         "cloud_unique_id":"cloud_unique_id_compute_node0",
                         "ip":"172.0.0.10",
                         "heartbeat_port":9050
                     }
                 ]
             }
         }'
     */
    clusterName = "cluster_name2"
    clusterId = "cluster_id2"
    nodeMap = [cloud_unique_id: "${cloudUniqueId}", ip: "${ip}", heartbeat_port: "${heartbeatPort}"]
    nodeList = [nodeMap]
    clusterMap = [cluster_name: "${clusterName}", cluster_id:"${clusterId}", nodes:nodeList]
    instance = [instance_id: "${instance_id}", op:"${opType}", cluster: clusterMap]
    jsonOutput = new JsonOutput()
    js = jsonOutput.toJson(instance)
    add_cluster_api.call(js) {
        respCode, body ->
            log.info("http cli result: ${body} ${respCode}".toString())
            def json = parseJson(body)
            assertTrue(json.code.equalsIgnoreCase("INVALID_ARGUMENT") || json.code.equalsIgnoreCase("OK"))
    }

    // drop cluster
    /*
        curl -X GET http://127.0.0.1:5000/MetaService/http/drop_cluster?token=greedisgood9999 -d '{
            "instance_id": "instance_id_deadbeef",
            "cluster": {
                "cluster_name": "cluster_name2",
                "cluster_id": "cluster_id2"
            }
        }'
     */
    clusterMap = [cluster_name: "cluster_name2", cluster_id:"cluster_id2"]
    instance = [instance_id: "${instance_id}", cluster: clusterMap]
    jsonOutput = new JsonOutput()
    js = jsonOutput.toJson(instance)
    drop_cluster_api.call(js) {
        respCode, body ->
            log.info("http cli result: ${body} ${respCode}".toString())
            def json = parseJson(body)
            assertTrue(json.code.equalsIgnoreCase("NOT_FOUND"))
    }

    // add a fe cluster
    /*
        curl '127.0.0.1:5000/MetaService/http/add_cluster?token=greedisgood9999' -d '{
             "instance_id":"instance_id_deadbeef",
             "cluster":{
                 "cluster_name":"RESERVED_CLUSTER_NAME_FOR_SQL_SERVER",
                 "cluster_id":"RESERVED_CLUSTER_ID_FOR_SQL_SERVER",
                 "type" : "SQL",
                 "nodes":[
                     {
                         "cloud_unique_id":"cloud_unique_id_sql_node0",
                         "ip":"172.0.0.10",
                         "edit_log_port":9030,
                         "node_type":"FE_MASTER"
                     },
                     {
                         "cloud_unique_id":"cloud_unique_id_sql_node0",
                         "ip":"172.0.0.11",
                         "edit_log_port":9030,
                         "node_type":"FE_OBSERVER"
                     },
                     {
                         "cloud_unique_id":"cloud_unique_id_sql_node0",
                         "ip":"172.0.0.12",
                         "edit_log_port":9030,
                         "node_type":"FE_OBSERVER"
                     }
                 ]
             }
         }'
     */
    def fe_node0 = [cloud_unique_id:"cloud_unique_id_sql_node0",
                    ip:"172.0.0.10", edit_log_port:9030, node_type:"FE_MASTER"]
    def fe_node1 = [cloud_unique_id:"cloud_unique_id_sql_node0",
                    ip:"172.0.0.11", edit_log_port:9030, node_type:"FE_OBSERVER"]
    def fe_node2 = [cloud_unique_id:"cloud_unique_id_sql_node0",
                    ip:"172.0.0.12", edit_log_port:9030, node_type:"FE_OBSERVER"]
    clusterMap = [cluster_name: "RESERVED_CLUSTER_NAME_FOR_SQL_SERVER_TEST",
                  cluster_id:"RESERVED_CLUSTER_NAME_FOR_SQL_SERVER_TEST",
                  type:"SQL", nodes: [fe_node0, fe_node1, fe_node2]]
    instance = [instance_id: "${instance_id}", cluster: clusterMap]
    jsonOutput = new JsonOutput()
    js = jsonOutput.toJson(instance)

    add_cluster_api.call(js) {
        respCode, body ->
            log.info("http cli result: ${body} ${respCode}".toString())
            def json = parseJson(body)
            assertTrue(json.code.equalsIgnoreCase("OK") || json.code.equalsIgnoreCase("ALREADY_EXISTED"))
    }

    // cluster is SQL type, must have only one master node, now master count: 0
    clusterMap = [cluster_name: "RESERVED_CLUSTER_NAME_FOR_SQL_SERVER_TEST",
                  cluster_id:"RESERVED_CLUSTER_ID_FOR_SQL_SERVER_TEST",
                  type:"SQL", nodes: [fe_node1, fe_node2]]
    instance = [instance_id: "${instance_id}", cluster: clusterMap]
    jsonOutput = new JsonOutput()
    js = jsonOutput.toJson(instance)
    add_cluster_api.call(js) {
        respCode, body ->
            log.info("http cli result: ${body} ${respCode}".toString())
            def json = parseJson(body)
            assertTrue(json.code.equalsIgnoreCase("INVALID_ARGUMENT"))
            assertTrue(json.msg.startsWith("cluster is SQL type"))
    }

    // drop fe cluster
    /*
        curl -X GET http://127.0.0.1:5000/MetaService/http/drop_cluster?token=greedisgood9999 -d '{
            "instance_id": "instance_id_deadbeef",
            "cluster": {
                "cluster_name": "RESERVED_CLUSTER_NAME_FOR_SQL_SERVER_TEST",
                "cluster_id": "RESERVED_CLUSTER_NAME_FOR_SQL_SERVER_TEST"
            }
        }'
     */

    clusterMap = [cluster_name: "RESERVED_CLUSTER_NAME_FOR_SQL_SERVER_TEST", cluster_id:"RESERVED_CLUSTER_NAME_FOR_SQL_SERVER_TEST"]
    instance = [instance_id: "${instance_id}", cluster: clusterMap]
    jsonOutput = new JsonOutput()
    js = jsonOutput.toJson(instance)
    drop_cluster_api.call(js) {
        respCode, body ->
            log.info("http cli result: ${body} ${respCode}".toString())
            def json = parseJson(body)
            assertTrue(json.code.equalsIgnoreCase("OK") || json.code.equalsIgnoreCase("INTERANAL_ERROR"))
    }
}
