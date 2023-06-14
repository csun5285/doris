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

package org.apache.doris.httpv2.rest;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.SelectStmt;
import org.apache.doris.analysis.ShowStmt;
import org.apache.doris.analysis.SqlParser;
import org.apache.doris.analysis.SqlScanner;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.util.SqlParserUtils;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.httpv2.util.ExecutionResultSet;
import org.apache.doris.qe.CommonResultSet;
import org.apache.doris.qe.CommonResultSet.CommonResultSetMetaData;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.qe.ResultSet;
import org.apache.doris.qe.ResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.util.InternalQuery;
import org.apache.doris.statistics.util.InternalQueryResult;
import org.apache.doris.statistics.util.InternalQueryResult.ResultRow;
import org.apache.doris.system.SystemInfoService;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.io.StringReader;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@RestController
public class ShowSqlAction extends RestBaseController {
    private static final Logger LOG = LogManager.getLogger(StmtExecutionAction.class);
    private static final long DEFAULT_ROW_LIMIT = 1000;
    private static final String TYPE_RESULT_SET = "result_set";

    @RequestMapping(path = "/api/show/{" + DB_KEY + "}", method = {RequestMethod.POST})
    public Object executeShowSQL(@PathVariable(value = DB_KEY) String dbName,
            HttpServletRequest request, HttpServletResponse response, @RequestBody String body) {
        Type type = new TypeToken<StmtRequestBody>() {
        }.getType();
        StmtRequestBody stmtRequestBody = new Gson().fromJson(body, type);
        if (Strings.isNullOrEmpty(stmtRequestBody.stmt)) {
            return ResponseEntityBuilder.badRequest("Missing statement request body");
        }
        LOG.info("stmt: {}, cloudCluster:{}, isSync:{}, limit: {}", stmtRequestBody.stmt, stmtRequestBody.cloudCluster,
                stmtRequestBody.is_sync, stmtRequestBody.limit);
        ConnectContext ctx = new ConnectContext();
        ctx.setQualifiedUser("");
        ctx.setCurrentUserIdentity(new UserIdentity("", "", false));
        ctx.setDatabase(getFullDbName(dbName));
        ctx.setEnv(Env.getCurrentEnv());
        ctx.setRemoteIP("");
        ctx.setCluster(SystemInfoService.DEFAULT_CLUSTER);
        if (!Strings.isNullOrEmpty(stmtRequestBody.cloudCluster)) {
            ctx.setCloudCluster(stmtRequestBody.cloudCluster);
        }
        ctx.setThreadLocalInfo();

        InternalShowQuery iQuery = new InternalShowQuery(dbName, stmtRequestBody.stmt, ctx);
        try {

            ResultSet rs = iQuery.query();
            ExecutionResultSet result = generateResultSet(rs);
            return ResponseEntityBuilder.ok(result.getResult());
        } catch (Exception e) {
            System.err.println("execute iQuery.query fail " + e.toString());
            LOG.warn("failed to execute stmt", e);
            e.printStackTrace();
            return ResponseEntityBuilder.okWithCommonError("Failed to execute sql: " + e.getMessage());
        }

    }

    /**
     * Result json sample:
     * {
     *      "type": "result_set",
     *      "data": [
     *                  [1],
     *                  [2]
     *              ],
     *      "meta": [{
     *           "name": "k1",
     *           "type": "INT"
     *      }],
     *      "status": {},
     * }
     */
    private ExecutionResultSet generateResultSet(ResultSet rs) {
        Map<String, Object> result = Maps.newHashMap();
        result.put("type", TYPE_RESULT_SET);
        if (rs == null) {
            return new ExecutionResultSet(result);
        }
        ResultSetMetaData metaData = rs.getMetaData();
        int colNum = metaData.getColumnCount();
        // 1. metadata
        List<Map<String, String>> metaFields = Lists.newArrayList();
        // index start from 1
        for (int i = 0; i < colNum; ++i) {
            Map<String, String> field = Maps.newHashMap();
            field.put("name", metaData.getColumn(i).getName());
            field.put("type", metaData.getColumn(i).getDataType().toString());
            metaFields.add(field);
        }
        result.put("meta", metaFields);
        result.put("data", rs.getResultRows());
        return new ExecutionResultSet(result);
    }

    private static class StmtRequestBody {
        public Boolean is_sync = true; // CHECKSTYLE IGNORE THIS LINE
        public Long limit = DEFAULT_ROW_LIMIT;
        public String stmt;
        public String cloudCluster;
    }

    private class InternalShowQuery {
        private int timeout = 0;
        private final String sql;
        private final String database;

        private ConnectContext context;

        private StatementBase stmt;
        private StmtExecutor executor = null;
        private ResultSet result;
        // private final List<TResultBatch> resultBatches = Lists.newArrayList();

        public InternalShowQuery(String database, String sql, ConnectContext context) {
            this.database = database;
            this.sql = sql;
            this.context = context;
        }

        /**
         * Execute the query internally and return the query result.
         *
         * @return Result of the query statement
         * @throws Exception Errors in parsing or execution
         */
        public ResultSet query() throws Exception {
            // step1: mock connectContext
            LOG.info("InernalShowQuery: begin build context");
            buildContext();
            LOG.info("InernalShowQuery: finish build context");

            // step2: parse sql
            LOG.info("InernalShowQuery: begin parse sql");
            parseSql();
            LOG.info("InernalShowQuery: end parse sql");

            // step3: generate plan
            // prepare();

            // step4: execute and get result
            LOG.info("InernalShowQuery: begin execute");
            execute();
            LOG.info("InernalShowQuery: end execute");

            // step5: parse result data and return
            return result;
        }

        private ResultSet convert(InternalQueryResult rs) {
            List<Column> columns = Lists.newArrayList();
            List<ResultRow> resultRows = rs.getResultRows();
            ResultRow row = resultRows.get(0);
            List<String> columnNames = row.getColumns();
            List<PrimitiveType> types = row.getTypes();
            for (int i = 0; i < columnNames.size(); i++) {
                columns.add(new Column(columnNames.get(i), types.get(i)));
            }
            CommonResultSetMetaData metaData = new CommonResultSetMetaData(columns);
            List<List<String>> data = Lists.newArrayList();
            for (int i = 0; i < resultRows.size(); i++) {
                row = resultRows.get(i);
                for (int j = 0; j < row.getColumns().size(); j++) {
                    data.add(row.getValues());
                }
            }
            return new CommonResultSet(metaData, data);
        }

        private void buildContext() {
            context.setEnv(Env.getCurrentEnv());
            context.setCluster(SystemInfoService.DEFAULT_CLUSTER);

            context.setNoAuth(true);
            context.getState().reset();

            context.setThreadLocalInfo();
            context.setStartTime();

            // If user does not set the timeout, then use max_cbo_statistics_task_timeout_sec
            timeout = timeout > 0 ? timeout : Config.max_cbo_statistics_task_timeout_sec;
            context.getSessionVariable().setQueryTimeoutS(timeout);
            String sqlHash = DigestUtils.md5Hex(sql);
            context.setSqlHash(sqlHash);
        }

        private void parseSql() throws DdlException {
            SqlScanner input = new SqlScanner(new StringReader(sql),
                    context.getSessionVariable().getSqlMode());
            SqlParser parser = new SqlParser(input);

            try {
                stmt = SqlParserUtils.getFirstStmt(parser);
                stmt.setOrigStmt(new OriginStatement(sql, 0));
            } catch (Exception e) {
                LOG.warn("InernalShowQuery: Failed to parse the statement: {}. {}", sql, e);
                throw new DdlException("InernalShowQuery: Failed to parse the statement:" + sql);
            }

            if (!(stmt instanceof ShowStmt)) {
                throw new DdlException("InernalShowQuery: Only show statements are supported:" + sql);
            }

        }

        private void execute() throws Exception {
            // Convert show statement to select statement here
            try {
                Analyzer analyzer = new Analyzer(context.getEnv(), context);
                SelectStmt selectStmt = ((ShowStmt) stmt).toSelectStmt(analyzer);
                if (selectStmt != null) {
                    InternalQuery selectQuery = new InternalQuery(database, sql);
                    InternalQueryResult internalResult = selectQuery.query();
                    result = convert(internalResult);
                } else {
                    executor = new StmtExecutor(context, stmt);
                    context.setExecutor(executor);
                    executor.execute();
                    if (context.getState().getStateType() == MysqlStateType.ERR) {
                        System.err.println("InernalShowQuery: execute show stmt encounter error");
                        LOG.warn("InernalShowQuery: execute show stmt encounter error");
                        throw new DdlException("InernalShowQuery: execute show stmt encounter error");
                    }
                    if (executor.isForwardToMaster()) {
                        result = executor.getShowResultSet();
                        if (result == null) {
                            System.err.println("InernalShowQuery: execute forwardToMaster stmt get null result");
                            LOG.warn("InernalShowQuery: execute forwardToMaster stmt get null result");
                            throw new DdlException("InernalShowQuery: execute forwardToMaster show stmt get"
                                    + "null result");
                        }
                    } else {
                        result = executor.fetchResultForNoAuth();
                        if (result == null) {
                            System.err.println("InernalShowQuery: execute noraml show stmt get null result");
                            LOG.warn("InernalShowQuery: execute noraml show stmt get null result");
                            throw new DdlException("InernalShowQuery: execute noraml show stmt get null result");
                        }
                    }
                }
            } catch (Exception e) {
                throw e;
            }
        }
    }
}

