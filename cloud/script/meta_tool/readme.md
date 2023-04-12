# cloud meta service decode tool 介绍
1. 可以线下debug使用，直接根据meta service的log中的key，打印出其保存在fdb的value数据。不用拼接http请求，而且某些key没有相应的http接口可用

## 环境准备：
1. python的fdb
   1. pip3 install -I foundationdb==7.1.11
   2. 其中7.1.11是fdb的安装版本
2. python生成的proto对应的文件
3. 在selectdb代码库的中proto存放的目录生成python对应的proto文件格式，例如selectdb_cloud.proto生成selectdb_cloud_pb2.py
4. 先进入proto目录，执行下面
   1. ```
      python -m grpc_tools.protoc -I. --python_out=. *.proto
      ```
   2. 如果出错可能需要安装，具体google下，应该容易解决
      1. ```
         pip3 install grpcio-tools
         pip3 install grpc-tools
         ```

## 使用方法：
1. 使用只需要修改decode.py中的三个参数（get_key、proto_directory、type)
   1. get_key: 从meta_service log中的复制出来的key
      1. ```
         举例： meta_service.INFO 中一条log 如下
         I0411 16:06:00.420356 4107823 meta_service.cpp:4953] get instance_key=0110696e7374616e636500011072656772657373696f6e5f696e7374616e63652d64782d313231390001
         get_key = 0110696e7374616e636500011072656772657373696f6e5f696e7374616e63652d64782d313231390001
         ```
   2. proto_directory: selectdb代码库的中proto存放的目录
   3. type: get_key对应的pb类型
      1. ```
         查看meta service代码，可知道instance_key存入fdb的value，是InstanceInfoPB
         因此 type = 'InstanceInfoPB'
         ```
         
## 结果展示：
```
python decode.py
k = b'\x01\x10instance\x00\x01\x10regression_instance-dx-1219\x00\x01'
v = b'\n\rgavin-user-id\x12\x1bregression_instance-dx-1219\x1a regression_instance-dx-1219-name:\x8b\x01\n"RESERVED_CLUSTER_ID_FOR_SQL_SERVER\x12$RESERVED_CLUSTER_NAME_FOR_SQL_SERVER\x18\x00*=\n\x1fregression-cloud-unique-id-fe-1\x1a\t127.0.0.1(\x9a\x9d\x80\x9d\x060\x9a\x9d\x80\x9d\x06P\xc7^X\x01:{\n\x16regression_cluster_id2\x12&regression_cluster_name2-changed-again\x18\x01*7\n\x1bregression-cloud-unique-id0\x1a\t127.0.0.1(\x96\x85\x86\x9d\x060\x96\x85\x86\x9d\x06@\x96n:m\n\x16regression_cluster_id3\x12\x18regression_cluster_name3\x18\x01*7\n\x1bregression-cloud-unique-id0\x1a\t127.0.0.1(\x96\x85\x86\x9d\x060\x96\x85\x86\x9d\x06@\xe6}:w\n\x1bregression_test_cluster_id0\x12\x1dregression_test_cluster_name0\x18\x01*7\n\x1bregression-cloud-unique-id0\x1a\t127.0.0.1(\xba\xcb\xbf\xa1\x060\xba\xcb\xbf\xa1\x06@\xdeVB\xa7\x01\x08\x9a\x9d\x80\x9d\x06\x10\x9a\x9d\x80\x9d\x06\x1a\x011"$AKIDsZHqgyhDSRBpDONtHPHua6MRUN0Wnpci* 0kOmumPnwpSr2ye6KSQ9cpmS4XN4VtDJ2\x18gavin-test-bj-1308700295:\x07dx-testB\x1bcos.ap-beijing.myqcloud.comJ\nap-beijingP\x02Z\x00J\xf6\x01\x08\x01\x1a\x14regression_test_tpch2\x91\x01"$AKIDAE2aqpY0B7oFPIvHMBj01lFSO3RYOxFH* nJYWDepkQqzrWv3uWsxlJ0ScV7SXLs882\x16doris-build-1308700295:\nregressionB\x1bcos.ap-beijing.myqcloud.comJ\x04nullP\x02:$e885bac1-9701-4aa2-806d-a0a04af79559B"\n\x1ddefault.file.column_separator\x12\x01|JT\x12\x05admin2\x1e\x1a\x011:\x19dx-test/stage/admin/admin:$f5d2704d-204c-43aa-ab42-f98daab31696J\x05adminJP\x12\x04root2\x1c\x1a\x011:\x17dx-test/stage/root/root:$78058906-b1c4-4a9f-8573-7ea8b45907beJ\x04rootJ\x8e\x02\x08\x01\x1a!internal_external_stage_cross_use2\x91\x01"$AKIDAE2aqpY0B7oFPIvHMBj01lFSO3RYOxFH* nJYWDepkQqzrWv3uWsxlJ0ScV7SXLs882\x16doris-build-1308700295:\nregressionB\x1bcos.ap-beijing.myqcloud.comJ\x04nullP\x02:$6c6fa051-6421-4aa1-8d60-8c03340657beB"\n\x1ddefault.file.column_separator\x12\x01|R\x00X\xed\xbb\xb9\x8c\xf30`\x01P\x00'
-------- Output --------
user_id: "gavin-user-id"
instance_id: "regression_instance-dx-1219"
name: "regression_instance-dx-1219-name"
clusters {
  cluster_id: "RESERVED_CLUSTER_ID_FOR_SQL_SERVER"
  cluster_name: "RESERVED_CLUSTER_NAME_FOR_SQL_SERVER"
  type: SQL
  nodes {
    cloud_unique_id: "regression-cloud-unique-id-fe-1"
    ip: "127.0.0.1"
    ctime: 1671433882
    mtime: 1671433882
    edit_log_port: 12103
    node_type: FE_MASTER
  }
}
obj_info {
  ctime: 1671433882
  mtime: 1671433882
  id: "1"
  ak: "xxxxx"
  sk: "yyyyy"
  bucket: "gavin-test-bj-1308700295"
  prefix: "dx-test"
  endpoint: "cos.ap-beijing.myqcloud.com"
  region: "ap-beijing"
  provider: COS
  external_endpoint: ""
}
stages {
  type: EXTERNAL
  name: "regression_test_tpch"
  obj_info {
    ak: "xxxx"
    sk: "yyyy"
    bucket: "doris-build-1308700295"
    prefix: "regression"
    endpoint: "cos.ap-beijing.myqcloud.com"
    region: "null"
    provider: COS
  }
  stage_id: "e885bac1-9701-4aa2-806d-a0a04af79559"
  properties {
    key: "default.file.column_separator"
    value: "|"
  }
}
status: NORMAL
```

## 未完项
1. range get, if need.