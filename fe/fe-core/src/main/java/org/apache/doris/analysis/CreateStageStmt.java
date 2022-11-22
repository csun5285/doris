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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.selectdb.cloud.proto.SelectdbCloud.StagePB;
import com.selectdb.cloud.storage.RemoteBase;
import com.selectdb.cloud.storage.RemoteBase.ObjectInfo;
import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.UUID;

/**
 * Create stage.
 */
public class CreateStageStmt extends DdlStmt {
    private static final Logger LOG = LogManager.getLogger(CreateStageStmt.class);

    @Getter
    private final boolean ifNotExists;
    @Getter
    private final String stageName;
    @Getter
    private StageProperties stageProperties;

    protected StagePB.StageType type;

    /**
     * Use for cup.
     */
    public CreateStageStmt(boolean ifNotExists, String stageName, Map<String, String> properties) {
        this.ifNotExists = ifNotExists;
        this.stageName = stageName;
        this.stageProperties = new StageProperties(properties);
        this.type = StagePB.StageType.EXTERNAL;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        // check auth
        if (!Env.getCurrentEnv().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }
        // check stage name
        FeNameFormat.checkResourceName(stageName, ResourceTypeEnum.STAGE);
        stageProperties.analyze();
        checkObjectStorageInfo();
    }

    private void checkObjectStorageInfo() throws AnalysisException {
        RemoteBase remote = null;
        try {
            remote = RemoteBase.newInstance(new ObjectInfo(stageProperties.getObjectStoreInfoPB()));
            // RemoteBase#headObject does not throw exception if key does not exist.
            remote.headObject("1");
        } catch (Exception e) {
            String message = e.getMessage();
            if (message != null) {
                int index = message.indexOf("Error message=");
                if (index != -1) {
                    message = message.substring(index);
                }
            }
            throw new AnalysisException("Incorrect object storage info, " + message);
        } finally {
            if (remote != null) {
                remote.close();
            }
        }
    }

    public StagePB toStageProto() throws DdlException {
        StagePB.Builder stageBuilder = StagePB.newBuilder();
        // external stage doesn't need username
        stageBuilder.setStageId(UUID.randomUUID().toString());
        switch (type) {
            case EXTERNAL:
                stageBuilder.setName(getStageName()).setType(StagePB.StageType.EXTERNAL)
                        .setObjInfo(stageProperties.getObjectStoreInfoPB());
                break;
            case INTERNAL:
            default:
                throw new DdlException("Cant not create stage with type=" + type);
        }
        stageBuilder.putAllProperties(stageProperties.getDefaultProperties());
        return stageBuilder.build();
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE STAGE ");
        if (ifNotExists) {
            sb.append("IF NOT EXISTS ");
        }
        sb.append(stageName).append(" PROPERTIES ").append(stageProperties.toSql());
        return sb.toString();
    }
}
