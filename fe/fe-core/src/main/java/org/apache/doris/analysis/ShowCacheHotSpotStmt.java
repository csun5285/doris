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

package org.apache.doris.analysis;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ShowCacheHotSpotStmt extends ShowStmt {
    public static final ShowResultSetMetaData[] RESULT_SET_META_DATAS = {
        ShowResultSetMetaData.builder()
            .addColumn(new Column("cluster_name", ScalarType.createVarchar(128)))
            .addColumn(new Column("file_cache_size", ScalarType.createType(PrimitiveType.BIGINT)))
            .addColumn(new Column("table_name", ScalarType.createVarchar(128)))
            .build(),
        ShowResultSetMetaData.builder()
            .addColumn(new Column("table_name", ScalarType.createVarchar(128)))
            .addColumn(new Column("last_access_time", ScalarType.createDatetimeType()))
            .addColumn(new Column("partition_name", ScalarType.createVarchar(65535)))
            .build(),
        ShowResultSetMetaData.builder()
            .addColumn(new Column("partition_name", ScalarType.createVarchar(65535)))
            .addColumn(new Column("last_access_time", ScalarType.createDatetimeType()))
            .build()
    };
    private int metaDataPos;
    private static final Logger LOG = LogManager.getLogger(ShowCacheHotSpotStmt.class);
    private static final TableName TABLE_NAME = new TableName(InternalCatalog.INTERNAL_CATALOG_NAME,
            FeConstants.INTERNAL_DB_NAME, FeConstants.INTERNAL_FILE_CACHE_HOTSPOT_TABLE_NAME);
    private final String tablePath;
    private List<String> whereExprVariables = Arrays.asList("cluster_name", "table_name");
    private List<String> whereExprValues = new ArrayList<>();
    private List<String> whereExpr = new ArrayList<>();
    private SelectStmt selectStmt;

    public ShowCacheHotSpotStmt(String url) {
        tablePath = url;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        if (Strings.isNullOrEmpty(tablePath)) {
            return;
        }

        if (!tablePath.startsWith("/")) {
            throw new AnalysisException("Path must starts with '/'");
        }
        String[] parts = tablePath.split("/");
        if (parts.length > 3) {
            throw new AnalysisException("Path must in format '/cluster/table/'");
        }
        whereExprValues = Arrays.asList(parts);
        for (int i = 1; i < whereExprValues.size(); i++) {
            whereExpr.add(String.format(" and t.%s = '%s' ", whereExprVariables.get(i - 1), whereExprValues.get(i)));
        }
        metaDataPos = whereExpr.size();
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return RESULT_SET_META_DATAS[metaDataPos];
    }

    private String generateQueryString() {
        StringBuilder query = null;
        StringBuilder orderClause = null;
        StringBuilder groupByClause = null;
        if (metaDataPos == 0) {
            query = new StringBuilder("SELECT cluster_name,\n"
                + "       sum(file_cache_size) AS total_file_cache_size,\n"
                + "       max_by(table_name, query_per_day) as top_hot_table\n"
                + "FROM\n"
                + "  (SELECT *,\n"
                + "          row_number() OVER (PARTITION BY cluster_id,\n"
                + "                                          backend_id,\n"
                + "                                          table_id,\n"
                + "                                          index_id,\n"
                + "                                          partition_id\n"
                + "                             ORDER BY insert_day DESC) AS rn\n"
                + "   FROM " +  TABLE_NAME.toString() + " ) t \n"
                + "WHERE rn = 1 group by t.cluster_name");
        }
        if (metaDataPos == 1) {
            query = new StringBuilder("SELECT table_name,\n"
                + "       last_access_time,\n"
                + "       max_by(partition_name, query_per_day) as top_hot_partition\n"
                + "FROM\n"
                + "  (SELECT *,\n"
                + "          row_number() OVER (PARTITION BY cluster_id,\n"
                + "                                          backend_id,\n"
                + "                                          table_id,\n"
                + "                                          index_id,\n"
                + "                                          partition_id\n"
                + "                             ORDER BY insert_day DESC) AS rn\n"
                + "   FROM " +  TABLE_NAME.toString() + " ) t \n"
                + "WHERE rn = 1 ");
            groupByClause = new StringBuilder("group by t.table_name, t.last_access_time");
        }
        if (metaDataPos == 2) {
            query = new StringBuilder("SELECT partition_name,\n"
                + "       last_access_time\n"
                + "FROM\n"
                + "  (SELECT *,\n"
                + "          row_number() OVER (PARTITION BY cluster_id,\n"
                + "                                          backend_id,\n"
                + "                                          table_id,\n"
                + "                                          index_id,\n"
                + "                                          partition_id\n"
                + "                             ORDER BY insert_day DESC) AS rn\n"
                + "   FROM " +  TABLE_NAME.toString() + " ) t \n"
                + "WHERE rn = 1");
            orderClause = new StringBuilder("\n order by t.query_per_day desc"
                + ", t.query_per_week desc");
        }
        Preconditions.checkState(query != null);
        for (String s : whereExpr) {
            query.append(s);
        }
        if (orderClause != null) {
            query.append(orderClause);
        }
        if (groupByClause != null) {
            query.append(groupByClause);
        }
        return query.toString();
    }

    @Override
    public SelectStmt toSelectStmt(Analyzer analyzer) throws AnalysisException {
        if (selectStmt != null) {
            return selectStmt;
        }
        try {
            analyze(analyzer);
        } catch (UserException e) {
            throw new AnalysisException(e.toString(), e);
        }
        String query = generateQueryString();
        LOG.debug("show cache hot spot stmt is {}", query);
        SqlScanner input = new SqlScanner(new StringReader(query));
        SqlParser parser = new SqlParser(input);
        try {
            selectStmt = (SelectStmt ) ((List<StatementBase> ) parser.parse().value).get(0);
        } catch (Exception e) {
            throw new AnalysisException(e.toString(), e);
        }
        return selectStmt;
    }
}
