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

package org.apache.doris.catalog.iam;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.CloudProperty;
import org.apache.doris.datasource.ExternalCatalog;

import com.google.common.collect.Maps;
import com.selectdb.cloud.proto.SelectdbCloud;
import com.selectdb.cloud.storage.RemoteBase;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class IAMTokenMgr {

    private static final Logger LOG = LogManager.getLogger(IAMTokenMgr.class);
    // Required Properties
    public static final String CLOUD_ARN = "arn";
    public static final String CLOUD_ROLE_NAME = "role_name";
    public static final String CLOUD_EXTERNAL_ID = "external_id";
    // Cloud Properties
    private final ScheduledThreadPoolExecutor executor;
    private final SelectdbCloud.RamUserPB iamUser;
    private final Map<Long, ExternalCatalog> externalCatalogs;
    private final Map<Long, RemoteBase> idToRemoteStorage = Maps.newConcurrentMap();


    public IAMTokenMgr(Map<Long, ExternalCatalog> catalogIfs, ScheduledThreadPoolExecutor executor) {
        this.externalCatalogs = catalogIfs;
        this.executor = executor;
        try {
            SelectdbCloud.GetIamResponse iamUsers = Env.getCurrentInternalCatalog().getIam();
            if (!iamUsers.hasIamUser()) {
                throw new DdlException("Instance does not have iam user.");
            }
            iamUser = iamUsers.getIamUser();
        } catch (DdlException e) {
            throw new RuntimeException(e);
        }
    }

    public static boolean checkGetStsToken(RoleKeys roleKeys) throws DdlException {
        SelectdbCloud.GetIamResponse iamUsers = Env.getCurrentInternalCatalog().getIam();
        if (!iamUsers.hasIamUser()) {
            throw new DdlException("Instance does not have iam user.");
        }
        SelectdbCloud.RamUserPB iamUser = iamUsers.getIamUser();
        try {
            getRemote(roleKeys, iamUser).getStsToken();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private static RemoteBase getRemote(RoleKeys roleKeys, SelectdbCloud.RamUserPB iamUser) throws Exception {
        SelectdbCloud.ObjectStoreInfoPB.Provider provider;
        String[] endpointStr = roleKeys.getEndpoint().split("\\.");
        if (endpointStr.length < 2) {
            throw new DdlException("Fail to parse cloud storage type for " + roleKeys.getEndpoint());
        }
        String cloudStorageType = endpointStr[0];
        provider = SelectdbCloud.ObjectStoreInfoPB.Provider.valueOf(cloudStorageType.toUpperCase());
        RemoteBase.ObjectInfo objectInfo = new RemoteBase.ObjectInfo(provider, iamUser.getAk(), iamUser.getSk(),
                roleKeys.getEndpoint(), roleKeys.getRegion(), roleKeys.getRoleName(),
                roleKeys.getArn(), roleKeys.getExternalId());
        return RemoteBase.newInstance(objectInfo);
    }

    public void start() {
        executor.scheduleAtFixedRate(() -> {
            try {
                LOG.info("Schedule refreshTokens task.");
                refreshTokens();
            } catch (DdlException e) {
                throw new RuntimeException(e);
            }
        }, 0L, Config.sts_duration, TimeUnit.SECONDS);
    }

    public void refreshTokens() throws DdlException {
        for (ExternalCatalog catalog : externalCatalogs.values()) {
            refreshCatalogToken(catalog);
        }
    }

    private synchronized void refreshCatalogToken(ExternalCatalog catalog) throws DdlException {
        RemoteBase remote = idToRemoteStorage.get(catalog.getId());
        if (remote != null) {
            Triple<String, String, String> stsToken = remote.getStsToken();
            CatalogProperty catalogProperty = catalog.getCatalogProperty();
            Map<String, String> modifiedProperties = new HashMap<>();
            // Use aws client access all cloud storage.
            modifiedProperties.put(CloudProperty.CLOUD_ACCESS_KEY, stsToken.getLeft());
            modifiedProperties.put(CloudProperty.CLOUD_SECRET_KEY, stsToken.getMiddle());
            modifiedProperties.put(CloudProperty.CLOUD_SESSION_TOKEN, stsToken.getRight());
            catalogProperty.modifyCatalogProps(modifiedProperties);
            catalog.refreshCatalog();
        }
    }

    public void registerRemote(Long catalogId, RoleKeys roleKeys) throws Exception {
        ExternalCatalog catalog = externalCatalogs.get(catalogId);
        if (catalog != null) {
            idToRemoteStorage.put(catalogId, getRemote(roleKeys, iamUser));
            refreshCatalogToken(catalog);
        }
    }

    public void logoffRemote(Long catalogId) {
        idToRemoteStorage.remove(catalogId);
    }
}
