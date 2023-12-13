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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Objects;

public class WarmUpClusterStmt extends StatementBase {
    private static final Logger LOG = LogManager.getLogger(WarmUpClusterStmt.class);
    private TableName tableName;
    private String partitionName;
    private String dbName;
    private String dstClusterName;
    private String srcClusterName;
    private boolean isWarmUpWithTable;
    private boolean isForce;

    public WarmUpClusterStmt(String dstClusterName, String srcClusterName, boolean isForce) {
        this.dstClusterName = dstClusterName;
        this.srcClusterName = srcClusterName;
        this.isForce = isForce;
        this.isWarmUpWithTable = false;
    }

    public WarmUpClusterStmt(String dstClusterName, TableName tableName, String partitionName, boolean isForce) {
        this.dstClusterName = dstClusterName;
        this.tableName = tableName;
        this.partitionName = partitionName;
        this.isForce = isForce;
        this.isWarmUpWithTable = true;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        super.analyze(analyzer);
        if (!Env.getCurrentSystemInfo().containClusterName(dstClusterName)) {
            throw new AnalysisException("The dstClusterName " + dstClusterName + " doesn't exist");
        }
        if (!isWarmUpWithTable && !Env.getCurrentSystemInfo().containClusterName(srcClusterName)) {
            boolean contains = false;
            try {
                contains = Env.getCurrentEnv().getCacheHotspotMgr().containsCluster(srcClusterName);
            } catch (Exception e) {
                throw new AnalysisException(e.getMessage());
            }
            if (!contains) {
                throw new AnalysisException("The srcClusterName doesn't exist");
            }
        }
        if (!isWarmUpWithTable && Objects.equals(dstClusterName, srcClusterName)) {
            throw new AnalysisException("The dstClusterName: " + dstClusterName
                                        + " is same with srcClusterName: " + srcClusterName);
        }
        if (isWarmUpWithTable) {
            tableName.analyze(analyzer);
            dbName = tableName.getDb();
            if (Strings.isNullOrEmpty(dbName)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR, dbName);
            }
            Database db = Env.getCurrentInternalCatalog().getDbNullable(dbName);
            if (db == null) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR, dbName);
            }
            OlapTable table = (OlapTable) db.getTableNullable(tableName.getTbl());
            if (table == null) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_TABLE_ERROR, tableName.getTbl());
            }
            if (partitionName.length() != 0 && !table.containPartition(partitionName)) {
                throw new AnalysisException("The partition " + partitionName + " doesn't exist");
            }
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("WARM UP CLUSTER ").append(dstClusterName).append(" WITH ")
            .append(isWarmUpWithTable ? " TABLE " : " CLUSTER ")
            .append(isWarmUpWithTable ? tableName : srcClusterName);
        return sb.toString();

    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_WITH_SYNC;
    }

    // private TableName tableName;
    // private String partitionName;
    // private String dbName;
    // private String dstClusterName;
    // private String srcClusterName;
    // private boolean isWarmUpWithTable;
    // private boolean isForce;
    public String getTableName() {
        return tableName.getTbl();
    }

    public String getDbName() {
        return dbName;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public String getDstClusterName() {
        return dstClusterName;
    }

    public String getSrcClusterName() {
        return srcClusterName;
    }

    public boolean isWarmUpWithTable() {
        return isWarmUpWithTable;
    }

    public boolean isForce() {
        return isForce;
    }
}
