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

import org.apache.doris.backup.S3Storage;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.load.loadv2.LoadTask.MergeType;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.common.collect.Lists;
import com.selectdb.cloud.proto.SelectdbCloud.ObjectStoreInfoPB;
import com.selectdb.cloud.proto.SelectdbCloud.StagePB;
import com.selectdb.cloud.proto.SelectdbCloud.StagePB.StageType;
import com.selectdb.cloud.storage.RemoteBase.ObjectInfo;
import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Copy statement
 */
public class CopyStmt extends DdlStmt {
    private static final Logger LOG = LogManager.getLogger(CopyStmt.class);

    private static final ShowResultSetMetaData COPY_INTO_META_DATA =
            ShowResultSetMetaData.builder()
                .addColumn(new Column("id", ScalarType.createVarchar(64)))
                .addColumn(new Column("state", ScalarType.createVarchar(64)))
                .addColumn(new Column("type", ScalarType.createVarchar(64)))
                .addColumn(new Column("msg", ScalarType.createVarchar(128)))
                .addColumn(new Column("loadedRows", ScalarType.createVarchar(64)))
                .addColumn(new Column("filterRows", ScalarType.createVarchar(64)))
                .addColumn(new Column("unselectRows", ScalarType.createVarchar(64)))
                .addColumn(new Column("url", ScalarType.createVarchar(128)))
            .build();
    public static final String S3_BUCKET = "bucket";
    public static final String S3_PREFIX = "prefix";

    @Getter
    private final TableName tableName;
    @Getter
    private CopyFromParam copyFromParam;
    @Getter
    private CopyIntoProperties copyIntoProperties;

    private LabelName label = null;
    private BrokerDesc brokerDesc = null;
    private DataDescription dataDescription = null;
    private final Map<String, String> brokerProperties = new HashMap<>();
    private final Map<String, String> properties = new HashMap<>();

    @Getter
    private String stage;
    @Getter
    private String stageId;
    @Getter
    private StageType stageType;
    @Getter
    private ObjectInfo objectInfo;

    @Getter
    private long sizeLimit;
    @Getter
    private boolean async;

    /**
     * Use for cup.
     */
    public CopyStmt(TableName tableName, CopyFromParam copyFromParam, Map<String, String> properties) {
        this.tableName = tableName;
        this.copyFromParam = copyFromParam;
        this.stage = copyFromParam.getStageAndPattern().getStageName();
        this.copyIntoProperties = new CopyIntoProperties(properties);
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        // generate a label
        String labelName = "copy_" + DebugUtil.printId(analyzer.getContext().queryId()).replace("-", "_");
        label = new LabelName(tableName.getDb(), labelName);
        label.analyze(analyzer);
        // analyze stage
        analyzeStageName();
        // get stage from meta service
        StagePB stagePB;
        String user = ClusterNamespace.getNameFromFullName(
                ConnectContext.get().getCurrentUserIdentity().getQualifiedUser());
        if (stage.equals("~")) {
            List<StagePB> stagePBs = Env.getCurrentInternalCatalog().getStage(StageType.INTERNAL, user, null);
            if (stagePBs.isEmpty()) {
                throw new AnalysisException("Failed to get internal stage for user: " + user);
            }
            stagePB = stagePBs.get(0);
        } else {
            // check stage permission
            if (!Env.getCurrentEnv().getAuth()
                    .checkCloudPriv(ConnectContext.get().getCurrentUserIdentity(), stage, PrivPredicate.USAGE,
                            ResourceTypeEnum.STAGE)) {
                throw new AnalysisException("USAGE denied to user '" + ConnectContext.get().getQualifiedUser() + "'@'"
                        + ConnectContext.get().getRemoteIP() + "' for cloud stage '" + stage + "'");
            }
            List<StagePB> stagePBs = Env.getCurrentInternalCatalog().getStage(StageType.EXTERNAL, null, stage);
            if (stagePBs.isEmpty()) {
                throw new AnalysisException("Failed to get external stage with name: " + stage);
            }
            stagePB = stagePBs.get(0);
        }
        analyzeStagePB(stagePB);

        // generate broker desc
        sizeLimit = copyIntoProperties.getSizeLimit();
        brokerDesc = new BrokerDesc("S3", StorageBackend.StorageType.S3, brokerProperties);
        // generate data description
        String filePath = "s3://" + brokerProperties.get(S3_BUCKET) + "/" + brokerProperties.get(S3_PREFIX);
        Separator separator = copyIntoProperties.getColumnSeparator() != null ? new Separator(
                copyIntoProperties.getColumnSeparator()) : null;
        String fileFormatStr = copyIntoProperties.getFileType();
        Map<String, String> dataDescProperties = copyIntoProperties.getDataDescriptionProperties();
        copyFromParam.analyze(label.getDbName(), tableName);
        if (copyFromParam.isSelect()) {
            dataDescription = new DataDescription(tableName.getTbl(), null, Lists.newArrayList(filePath),
                    copyFromParam.getFileColumns(), separator, fileFormatStr, null, false,
                    copyFromParam.getColumnMappingList(), copyFromParam.getFileFilterExpr(),
                    null, MergeType.APPEND, null, null, dataDescProperties);
        } else {
            dataDescription = new DataDescription(tableName.getTbl(), null, Lists.newArrayList(filePath), null,
                    separator, fileFormatStr, null, false, null, null, null, MergeType.APPEND, null, null,
                    dataDescProperties);
        }
        // analyze data description
        dataDescription.analyze(label.getDbName());
        for (int i = 0; i < dataDescription.getFilePaths().size(); i++) {
            dataDescription.getFilePaths().set(i, brokerDesc.convertPathToS3(dataDescription.getFilePaths().get(i)));
            dataDescription.getFilePaths()
                    .set(i, ExportStmt.checkPath(dataDescription.getFilePaths().get(i), brokerDesc.getStorageType()));
        }

        try {
            properties.putAll(copyIntoProperties.getExecProperties());
            // TODO support exec params as LoadStmt
            LoadStmt.checkProperties(properties);
        } catch (DdlException e) {
            throw new AnalysisException(e.getMessage());
        }
    }

    private void analyzeStageName() throws AnalysisException {
        if (stage.isEmpty()) {
            throw new AnalysisException("Stage name can not be empty");
        }
    }

    // after analyzeStagePB, fileFormat and copyOption is not null
    private void analyzeStagePB(StagePB stagePB) throws AnalysisException {
        stageType = stagePB.getType();
        stageId = stagePB.getStageId();
        ObjectStoreInfoPB objInfo = stagePB.getObjInfo();
        objectInfo = new ObjectInfo(objInfo);
        brokerProperties.put(S3Storage.S3_ENDPOINT, objInfo.getEndpoint());
        brokerProperties.put(S3Storage.S3_REGION, objInfo.getRegion());
        brokerProperties.put(S3Storage.S3_AK, objInfo.getAk());
        brokerProperties.put(S3Storage.S3_SK, objInfo.getSk());
        brokerProperties.put(S3_BUCKET, objInfo.getBucket());
        brokerProperties.put(S3_PREFIX, objInfo.getPrefix());

        StageProperties stageProperties = new StageProperties(stagePB.getPropertiesMap());
        this.copyIntoProperties.mergeProperties(stageProperties);
        this.copyIntoProperties.analyze();
        this.async = this.copyIntoProperties.isAsync();
    }

    public String getDbName() {
        return label.getDbName();
    }

    public BrokerDesc getBrokerDesc() {
        return brokerDesc;
    }

    public List<DataDescription> getDataDescriptions() {
        return Lists.newArrayList(dataDescription);
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public LabelName getLabel() {
        return label;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("COPY INTO ").append(tableName.toSql()).append(" \n");
        sb.append("from ").append(copyFromParam.toSql()).append("\n");
        if (copyIntoProperties != null && copyIntoProperties.getProperties().size() > 0) {
            sb.append(" PROPERTIES ").append(copyIntoProperties.toSql());
        }
        return sb.toString();
    }

    public ShowResultSetMetaData getMetaData() {
        return COPY_INTO_META_DATA;
    }

    public String getPattern() {
        return this.copyFromParam.getStageAndPattern().getPattern();
    }
}
