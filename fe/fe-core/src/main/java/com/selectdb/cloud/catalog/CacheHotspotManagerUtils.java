// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.selectdb.cloud.catalog;

import org.apache.commons.text.StringSubstitutor;
import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Table;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.ResultRow;
import org.apache.doris.statistics.util.InternalQuery;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TUniqueId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

public class CacheHotspotManagerUtils {
    private static final Logger LOG = LogManager.getLogger(CacheHotspotManagerUtils.class);
    private static final String TABLE_NAME = String.format("%s.%s",
            FeConstants.INTERNAL_DB_NAME, FeConstants.INTERNAL_FILE_CACHE_HOTSPOT_TABLE_NAME);
    // TODO(yuejing): 如何加字段
    private static final String CREATE_CACHE_TABLE =
            "create table " + TABLE_NAME + " (\n"
                + "    cluster_id varchar(65530),\n"
                + "    backend_id bigint,\n"
                + "    table_id bigint,\n"
                + "    index_id bigint,\n"
                + "    partition_id bigint,\n"
                + "    insert_day DATEV2,\n"
                + "    table_name varchar(65530),\n"
                + "    index_name varchar(65530),\n"
                + "    partition_name varchar(65530),\n"
                + "    cluster_name varchar(65530),\n"
                + "    file_cache_size bigint,\n"
                + "    query_per_day bigint,\n"
                + "    query_per_week bigint,\n"
                + "    last_access_time DATETIMEV2)\n"
                + "    UNIQUE KEY(cluster_id, backend_id, table_id, index_id, partition_id, insert_day)\n"
                + "    PARTITION BY RANGE (insert_day) ()\n"
                + "    DISTRIBUTED BY HASH (cluster_id)\n"
                + "    PROPERTIES (\n"
                + "    \"dynamic_partition.enable\" = \"true\",\n"
                + "    \"dynamic_partition.time_unit\" = \"DAY\",\n"
                + "    \"dynamic_partition.start\" = \"-7\",\n"
                + "    \"dynamic_partition.end\" = \"3\",\n"
                + "    \"dynamic_partition.prefix\" = \"p\",\n"
                + "    \"dynamic_partition.create_history_partition\" = \"true\",\n"
                + "    \"dynamic_partition.history_partition_num\" = \"7\"\n"
                + "    );";
    private static final String BATCH_INSERT_INTO_CACHE_TABLE_TEMPLATE =
            "INSERT INTO " + TABLE_NAME + " values";
    private static final String INSERT_INTO_CACHE_TABLE_TEMPLATE =
            "('${cluster_id}', '${backend_id}', '${table_id}', '${index_id}',"
            + " '${partition_id}', '${insert_day}', '${table_name}', "
            + " '${index_name}', '${partition_name}', '${cluster_name}', "
            + "'${file_cache_size}', '${qpd}', '${qpw}', '${last_access_time}')";
    private static final String CONTAINS_CLUSTER_TEMPLATE =
            "SELECT COUNT(*) FROM " + TABLE_NAME
            + " WHERE '${cluster_id}' = 'cluster'";

    private static final String GET_CLUSTER_FILE_CACHE_SIZE_TEMPLATE = "SELECT sum(file_cache_size)\n"
            + "FROM\n"
            + "  (SELECT *,\n"
            + "          row_number() OVER (PARTITION BY cluster_id,\n"
            + "                                          backend_id,\n"
            + "                                          table_id,\n"
            + "                                          index_id,\n"
            + "                                          partition_id\n"
            + "                             ORDER BY insert_day DESC) AS rn\n"
            + "   FROM " + TABLE_NAME + " t\n"
            + "WHERE rn = 1 and t.cluster_id = '${cluster_id}'";

    private static final String GET_CLUSTER_PARTITIONS_TEMPLATE = "SELECT table_name, index_id, partition_id\n"
            + "FROM\n"
            + "  (SELECT *,\n"
            + "          row_number() OVER (PARTITION BY cluster_id,\n"
            + "                                          backend_id,\n"
            + "                                          table_id,\n"
            + "                                          index_id,\n"
            + "                                          partition_id\n"
            + "                             ORDER BY insert_day DESC) AS rn\n"
            + "   FROM " + TABLE_NAME + " ) t\n"
            + "WHERE rn = 1 and t.cluster_id = '${cluster_id}' \n"
            + "order by t.query_per_day desc, t.query_per_week desc";
    private static String INTERNAL_TABLE_ID;

    public static boolean clusterContains(String clusterId) {
        if (clusterId == null) {
            return false;
        }
        Map<String, String> params = new HashMap<String, String>();
        params.put("cluster_id", clusterId);
        StringSubstitutor stringSubstitutor = new StringSubstitutor(params);
        String sql = stringSubstitutor.replace(CONTAINS_CLUSTER_TEMPLATE);
        InternalQuery query = new InternalQuery(FeConstants.INTERNAL_DB_NAME, sql);
        List<ResultRow> result = null;
        try {
            result = query.query();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return !(result == null || result.size() == 0);
    }

    // table_name, index_name, partition_name
    public static List<List<String>> getClusterTopNPartitions(String clusterId) {
        if (clusterId == null) {
            String err = String.format("cluster doesn't exist, clusterId %s", clusterId);
            LOG.warn(err);
            throw new RuntimeException(err);
        }
        Map<String, String> params = new HashMap<String, String>();
        params.put("cluster_id", clusterId);
        StringSubstitutor stringSubstitutor = new StringSubstitutor(params);
        String sql = stringSubstitutor.replace(GET_CLUSTER_PARTITIONS_TEMPLATE);
        InternalQuery query = new InternalQuery(FeConstants.INTERNAL_DB_NAME, sql);
        List<ResultRow> result = null;
        try {
            result = query.query();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if (result == null || result.size() == 0) {
            String err = String.format("cluster doesn't exist, clusterId %s", clusterId);
            LOG.warn(err);
            throw new RuntimeException(err);
        }
        return result.stream().map(ResultRow::getValues).collect(Collectors.toList());
    }

    public static void transformIntoCacheHotSpotTableValue(Map<String, String> params, List<String> values) {
        if (INTERNAL_TABLE_ID.equals(params.get("table_id"))) {
            // we don't insert into internal table
            return;
        }
        StringSubstitutor stringSubstitutor = new StringSubstitutor(params);
        String sql = stringSubstitutor.replace(INSERT_INTO_CACHE_TABLE_TEMPLATE);
        values.add(sql);
    }

    public static void doBatchInsert(List<String> values) throws Exception {
        if (values.isEmpty()) {
            return;
        }
        StringBuilder query = new StringBuilder(BATCH_INSERT_INTO_CACHE_TABLE_TEMPLATE);
        for (int i = 0; i < values.size(); i++) {
            query.append(values.get(i));
            if (i + 1 != values.size()) {
                query.append(",");
            } else {
                query.append(";");
            }
        }
        execUpdate(query.toString());
    }

    public static void execUpdate(String sql) throws Exception {
        try (AutoCloseConnectContext r = buildConnectContext()) {
            StmtExecutor stmtExecutor = new StmtExecutor(r.connectContext, sql);
            r.connectContext.setExecutor(stmtExecutor);
            stmtExecutor.execute();
        }
    }

    private static void execCreateDatabase() throws Exception {
        CreateDbStmt createDbStmt = new CreateDbStmt(true,
                ClusterNamespace.getFullName(SystemInfoService.DEFAULT_CLUSTER, FeConstants.INTERNAL_DB_NAME),
                null);
        createDbStmt.setClusterName(SystemInfoService.DEFAULT_CLUSTER);
        try {
            Env.getCurrentEnv().createDb(createDbStmt);
        } catch (DdlException e) {
            LOG.warn("Failed to create database: {}, will try again later",
                    FeConstants.INTERNAL_DB_NAME, e);
        }
    }

    public static void execCreateCacheTable() throws Exception {
        try (AutoCloseConnectContext r = buildConnectContext()) {
            execCreateDatabase();
            StmtExecutor stmtExecutor = new StmtExecutor(r.connectContext, CREATE_CACHE_TABLE);
            r.connectContext.setExecutor(stmtExecutor);
            stmtExecutor.execute();
        }
        Database db = Env.getCurrentInternalCatalog().getDbNullable(ClusterNamespace
                .getFullName(SystemInfoService.DEFAULT_CLUSTER, FeConstants.INTERNAL_DB_NAME));
        if (db == null) {
            LOG.warn("{} database doesn't exist", FeConstants.INTERNAL_DB_NAME);
        }

        Table t = db.getTableNullable(FeConstants.INTERNAL_FILE_CACHE_HOTSPOT_TABLE_NAME);
        if (t == null) {
            LOG.warn("{} table doesn't exist", FeConstants.INTERNAL_FILE_CACHE_HOTSPOT_TABLE_NAME);
        }
        INTERNAL_TABLE_ID = String.valueOf(t.getId());
    }

    public static AutoCloseConnectContext buildConnectContext() {
        ConnectContext connectContext = new ConnectContext();
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        sessionVariable.internalSession = true;
        // sessionVariable.setMaxExecMemByte(StatisticConstants.STATISTICS_MAX_MEM_PER_QUERY_IN_BYTES);
        sessionVariable.setEnableInsertStrict(true);
        // sessionVariable.parallelExecInstanceNum = StatisticConstants.STATISTIC_PARALLEL_EXEC_INSTANCE_NUM;
        sessionVariable.setEnableNereidsPlanner(false);
        sessionVariable.enableProfile = false;
        connectContext.setEnv(Env.getCurrentEnv());
        connectContext.setDatabase(FeConstants.INTERNAL_DB_NAME);
        UUID uuid = UUID.randomUUID();
        TUniqueId queryId = new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
        connectContext.setQueryId(queryId);
        connectContext.setStartTime();
        connectContext.setCluster(SystemInfoService.DEFAULT_CLUSTER);
        connectContext.setCurrentUserIdentity(UserIdentity.ROOT);
        connectContext.setQualifiedUser(UserIdentity.ROOT.getQualifiedUser());
        return new AutoCloseConnectContext(connectContext);
    }

    public static class AutoCloseConnectContext implements AutoCloseable {

        public final ConnectContext connectContext;

        private final ConnectContext previousContext;

        public AutoCloseConnectContext(ConnectContext connectContext) {
            this.previousContext = ConnectContext.get();
            this.connectContext = connectContext;
            connectContext.setThreadLocalInfo();
            connectContext.setCloudCluster();
        }

        @Override
        public void close() {
            ConnectContext.remove();
            if (previousContext != null) {
                previousContext.setThreadLocalInfo();
            }
        }
    }
}
