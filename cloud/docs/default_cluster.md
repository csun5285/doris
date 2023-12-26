## 描述

此文档说的用户名，都是MySQL用户名，比如`mysql -ujack`，其中jack为用户名

### 设置default cluster

1. 语法

为当前用户设置默认cluster

```
SET PROPERTY 'default_cloud_cluster' = {clusterName};
```

为其他用户设置默认cluster，注意需要有admin权限

```
SET PROPERTY FOR {user} 'default_cloud_cluster' = {clusterName};
```

展示当前用户默认cluster，default_cloud_cluster的value既是默认cluster

```
SHOW PROPERTY;
```

展示其他用户默认cluster，主要当前用户要有相关权限，default_cloud_cluster的value既是默认cluster

```
SHOW PROPERTY FOR {user};
```

展示当前warehouse下所有可用的clusters

```
SHOW CLUSTERS;
```

2. 注意：
- 当前用户拥有admin role，例如：CREATE USER jack IDENTIFIED BY '123456' DEFAULT ROLE "admin";
   - 可以给自己设置default cluster和给其他用户设置default cluster
   - 可以SHOW自己的PROPERTY和其他用户的PROPERTY
- 当前用户不拥有admin role， 例如CREATE USER jack1 IDENTIFIED BY '123456';
   - 可以给自己设置default cluster
   - 可以SHOW自己的PROPERTY
   - 不能SHOW CLUSTERS，会提示需要grant ADMIN权限
- 若当前用户没有配置默认cluster，目前实现在读写数据的时候，会报错。可以使用`use @cluster`设置当前context使用的cluster，也可以使用SET PROPERTY设置默认cluster
- 若当前用户配置了默认cluster，但是后面此cluster被drop掉了，读写数据会报错，可以使用`use @cluster`设置当前context使用的cluster，也可以使用SET PROPERTY设置默认cluster
- 若userA 设置 default cluster 为 ClusterA，drop clusterA后，用户的default cluster不会被修改，但是读写等操作会报错，报错内容类似下面
   ```
   mysql> select * from t1;
   ERROR 1105 (HY000): errCode = 2, detailMessage = 10444 have no queryable replicas. err: 10445's backend -1 does not exist or not alive, or you may not have permission to access the current cluster,  clusterName=regression_test_cluster_name2
   mysql> insert into t1 values(2, "bbb", 30);
   ERROR 1105 (HY000): errCode = 3, detailMessage = tablet 10444 alive replica num 0 < quorum replica num 1, alive backends: []or you may not have permission to access the current cluster clusterName=regression_test_cluster_name2
   ```
   用户遇到类似错误，可以自行排查下，通过SET PROPERTY FOR {user} 'default_cloud_cluster' = '{newClusterName}';更新default cluster，并注意用户需要newClusterName的usage_priv

3. 示例：

```
// 设置当前用户默认cluster
mysql> SET PROPERTY 'default_cloud_cluster' = 'regression_test_cluster_name0';
Query OK, 0 rows affected (0.02 sec)

// 展示当前用户的默认cluster
mysql> show PROPERTY;
+------------------------+-------------------------------+
| Key                    | Value                         |
+------------------------+-------------------------------+
| cpu_resource_limit     | -1                            |
| default_cloud_cluster  | regression_test_cluster_name0 |
| exec_mem_limit         | -1                            |
| load_mem_limit         | -1                            |
| max_query_instances    | -1                            |
| max_user_connections   | 100                           |
| quota.high             | 800                           |
| quota.low              | 100                           |
| quota.normal           | 400                           |
| resource.cpu_share     | 1000                          |
| resource.hdd_read_iops | 80                            |
| resource.hdd_read_mbps | 30                            |
| resource.io_share      | 1000                          |
| resource.ssd_read_iops | 1000                          |
| resource.ssd_read_mbps | 30                            |
| resource_tags          |                               |
| sql_block_rules        |                               |
+------------------------+-------------------------------+
17 rows in set (0.00 sec)

// 使用root账号在mysql client中创建jack用户
mysql> CREATE USER jack IDENTIFIED BY '123456' DEFAULT ROLE "admin";
Query OK, 0 rows affected (0.01 sec)

// 给jack用户设置默认cluster
mysql> SET PROPERTY FOR jack 'default_cloud_cluster' = 'regression_test_cluster_name1';
Query OK, 0 rows affected (0.00 sec)

// 展示其他用户的默认cluster
mysql> show PROPERTY for jack;
+------------------------+-------------------------------+
| Key                    | Value                         |
+------------------------+-------------------------------+
| cpu_resource_limit     | -1                            |
| default_cloud_cluster  | regression_test_cluster_name1 |
| exec_mem_limit         | -1                            |
| load_mem_limit         | -1                            |
| max_query_instances    | -1                            |
| max_user_connections   | 100                           |
| quota.high             | 800                           |
| quota.low              | 100                           |
| quota.normal           | 400                           |
| resource.cpu_share     | 1000                          |
| resource.hdd_read_iops | 80                            |
| resource.hdd_read_mbps | 30                            |
| resource.io_share      | 1000                          |
| resource.ssd_read_iops | 1000                          |
| resource.ssd_read_mbps | 30                            |
| resource_tags          |                               |
| sql_block_rules        |                               |
+------------------------+-------------------------------+
17 rows in set (0.00 sec)
```

若当前warehouse下不存在将要设置的默认cluster会报错，提示使用show clusters展示当前warehouse下所有有效的cluster，其中cluster列表示clusterName，is_current列表示当前用户是否使用此cluster，users列表示这些用户设置默认cluster为当前行的cluster

```
mysql> SET PROPERTY 'default_cloud_cluster' = 'not_exist_cluster';
ERROR 5091 (42000): errCode = 2, detailMessage = Cluster not_exist_cluster not exist, use SQL 'SHOW CLUSTERS' to get a valid cluster

mysql> show clusters;
+-------------------------------+------------+------------+
| cluster                       | is_current | users      |
+-------------------------------+------------+------------+
| regression_test_cluster_name0 | FALSE      | root, jack |
| regression_test_cluster_name5 | FALSE      |            |
+-------------------------------+------------+------------+
2 rows in set (0.01 sec)

mysql> SET PROPERTY 'default_cloud_cluster' = 'regression_test_cluster_name5';
Query OK, 0 rows affected (0.01 sec)
```


### 未设置default cluster，系统自动选择集群的规则

如果用户没有设置默认集群，则会找到一个有存活后端并且有使用权限的集群。并且在同次会话中，系统选择的集群将一直保持不变。

对于不同次的会话，存在以下情况，可能导致系统自动选择的集群发生改变
* 用户失去了上次选择集群的使用权限
* 有集群被添加或者移除
* 上次选择的集群不存在存活的后端

其中，第一种和第二种情况一定会导致系统自动选择的集群发生改变，第三种情况可能会导致系统自动选择的集群发生改变。