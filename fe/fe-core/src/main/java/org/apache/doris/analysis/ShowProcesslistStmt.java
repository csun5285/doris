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
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSetMetaData;

// SHOW PROCESSLIST statement.
// Used to show connection belong to this user.
public class ShowProcesslistStmt extends ShowStmt {
    private static final ShowResultSetMetaData META_DATA = ShowResultSetMetaData.builder()
            .addColumn(new Column("Id", ScalarType.createType(PrimitiveType.BIGINT)))
            .addColumn(new Column("User", ScalarType.createVarchar(16)))
            .addColumn(new Column("Host", ScalarType.createVarchar(16)))
            .addColumn(new Column("Cluster", ScalarType.createVarchar(16)))
            .addColumn(new Column("Db", ScalarType.createVarchar(16)))
            .addColumn(new Column("Command", ScalarType.createVarchar(16)))
            .addColumn(new Column("Time", ScalarType.createType(PrimitiveType.INT)))
            .addColumn(new Column("State", ScalarType.createVarchar(64)))
            .addColumn(new Column("Info", ScalarType.STRING)).build();

    private boolean isFull;

    public ShowProcesslistStmt(boolean isFull) {
        this.isFull = isFull;
    }

    public boolean isFull() {
        return isFull;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        // ATTN: root has admin and operator Privileges
        if (!Config.cloud_unique_id.isEmpty()
                && !Env.getCurrentEnv().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.OPERATOR)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_UNSUPPORTED_OPERATION_ERROR);
        }
    }

    @Override
    public String toSql() {
        return "SHOW " + (isFull ? "FULL" : "") + "PROCESSLIST";
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }
}
