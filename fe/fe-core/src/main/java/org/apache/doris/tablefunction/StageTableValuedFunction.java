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

package org.apache.doris.tablefunction;

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.analysis.StageProperties;
import org.apache.doris.analysis.StorageBackend.StorageType;
import org.apache.doris.catalog.Env;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.datasource.property.constants.S3Properties;
import org.apache.doris.load.loadv2.CleanCopyJobTask;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TFileType;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.selectdb.cloud.proto.SelectdbCloud.StagePB;
import com.selectdb.cloud.proto.SelectdbCloud.StagePB.StageType;
import com.selectdb.cloud.stage.StageUtil;
import com.selectdb.cloud.storage.RemoteBase;
import com.selectdb.cloud.storage.RemoteBase.ObjectInfo;
import org.apache.commons.collections.map.CaseInsensitiveMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The Implement of table valued function
 * STAGE("NAME" = "stage_name", "FILE_PATTERN" = "1.csv", "FORMAT" = "csv").
 */
public class StageTableValuedFunction extends ExternalFileTableValuedFunction {
    public static final Logger LOG = LogManager.getLogger(StageTableValuedFunction.class);
    public static final String NAME = "stage";

    public static final String STAGE_NAME = "stage_name";
    public static final String FILE_PATTERN = "file_pattern";
    public static final String SIZE_LIMIT = "size_limit";

    private static final ImmutableSet<String> PROPERTIES_SET = new ImmutableSet.Builder<String>()
            .add(STAGE_NAME).add(FILE_PATTERN).add(SIZE_LIMIT).build();

    private String stageName;
    private String filePattern;
    private long sizeLimit;
    private StagePB stagePB;
    private ObjectInfo objectInfo;
    private long tableId;
    private boolean force = true;

    public StageTableValuedFunction(Map<String, String> params) throws AnalysisException {
        Map<String, String> validParams = new CaseInsensitiveMap();
        for (String key : params.keySet()) {
            if (!PROPERTIES_SET.contains(key.toLowerCase()) && !FILE_FORMAT_PROPERTIES.contains(key.toLowerCase())) {
                throw new AnalysisException(key + " is invalid property");
            }
            validParams.put(key, params.get(key));
        }

        this.stageName = validParams.getOrDefault(STAGE_NAME, "");
        this.filePattern = validParams.getOrDefault(FILE_PATTERN, "");
        this.sizeLimit = Long.parseLong(validParams.getOrDefault(SIZE_LIMIT, "-1"));
        try {
            analyzeStage(stageName, ClusterNamespace.getNameFromFullName(
                    ConnectContext.get().getCurrentUserIdentity().getQualifiedUser()), true, validParams);
        } catch (DdlException e) {
            throw new AnalysisException(e.getMessage());
        }

        parseCommonProperties(validParams);
        parseFile();
    }

    public void parseFile(long tableId) throws AnalysisException {
        this.force = false;
        this.tableId = tableId;
        // begin copy and set fileStatuses
        String copyId = DebugUtil.printId(ConnectContext.get().queryId());
        long timeout = ConnectContext.get().getSessionVariable().getQueryTimeoutS() * 1000 + 5000; // add a delta
        try {
            fileStatuses.clear();
            fileStatuses.addAll(
                    StageUtil.beginCopy(stagePB.getStageId(), stagePB.getType(), objectInfo, tableId, copyId,
                            filePattern, sizeLimit, timeout));
        } catch (Exception e) {
            LOG.warn("Failed begin copy for copyId={}", copyId, e);
            throw new AnalysisException("Failed begin copy: " + e.getMessage());
        }
    }

    public void finishCopy(boolean success) {
        String copyId = DebugUtil.printId(ConnectContext.get().queryId());
        try {
            LOG.info((success ? "Finish " : "Cancel ") + " copy for stage={}, table={}, copyId={}",
                    stagePB.getStageId(), tableId, copyId);
            StageUtil.finishCopy(stagePB.getStageId(), stagePB.getType(), tableId, copyId, success);
            // delete internal stage files and copy job
            List<String> loadFiles = null;
            if (stagePB.getType() == StageType.INTERNAL) {
                List<String> paths = fileStatuses.stream().map(e -> e.path).collect(Collectors.toList());
                loadFiles = StageUtil.parseLoadFiles(paths, objectInfo.getBucket(), stagePB.getObjInfo().getPrefix());
            }
            if (success && Config.cloud_delete_loaded_internal_stage_files && fileStatuses != null
                    && stagePB.getType() == StageType.INTERNAL && !this.force) {
                CleanCopyJobTask copyJobCleanTask = new CleanCopyJobTask(objectInfo, stagePB.getStageId(),
                        stagePB.getType(), tableId, copyId, loadFiles);
                Env.getCurrentEnv().getLoadManager().createCleanCopyJobTask(copyJobCleanTask);
            }
        } catch (Exception e) {
            LOG.warn("Failed finish copy for stage={}, table={}, copyId={}, success={}", stagePB.getStageId(),
                    tableId, copyId, success, e);
        }
    }

    private void analyzeStage(String stage, String user, boolean checkAuth, Map<String, String> validParams)
            throws AnalysisException, DdlException {
        stagePB = StageUtil.getStage(stage, user, checkAuth);
        objectInfo = RemoteBase.analyzeStageObjectStoreInfo(stagePB);
        locationProperties = Maps.newHashMap();
        locationProperties.put(S3Properties.Env.ENDPOINT, objectInfo.getEndpoint());
        locationProperties.put(S3Properties.Env.ACCESS_KEY, objectInfo.getAk());
        locationProperties.put(S3Properties.Env.SECRET_KEY, objectInfo.getSk());
        locationProperties.put(S3Properties.Env.REGION, objectInfo.getRegion());
        if (objectInfo.getToken() != null) {
            locationProperties.put(S3Properties.Env.TOKEN, objectInfo.getToken());
        }
        locationProperties.put(S3Properties.Env.BUCKET, objectInfo.getBucket());
        StageProperties stageProperties = new StageProperties(stagePB.getPropertiesMap());
        Map<String, String> stageTvfProperties = stageProperties.getStageTvfProperties();
        stageTvfProperties.entrySet().forEach(e -> validParams.putIfAbsent(e.getKey(), e.getValue()));
        if (sizeLimit == -1) {
            sizeLimit = stageProperties.getSizeLimit();
        }
    }

    // =========== implement abstract methods of ExternalFileTableValuedFunction =================
    @Override
    public TFileType getTFileType() {
        return TFileType.FILE_S3;
    }

    @Override
    public String getFilePath() {
        return "s3://" + locationProperties.get(S3Properties.Env.BUCKET) + "/" + (
                StringUtils.isEmpty(stagePB.getObjInfo().getPrefix()) ? filePattern
                        : stagePB.getObjInfo().getPrefix() + "/" + filePattern);
    }

    @Override
    public BrokerDesc getBrokerDesc() {
        return new BrokerDesc("StageTvfBroker", StorageType.S3, locationProperties);
    }

    // =========== implement abstract methods of TableValuedFunctionIf =================
    @Override
    public String getTableName() {
        return "StageTableValuedFunction";
    }
}
