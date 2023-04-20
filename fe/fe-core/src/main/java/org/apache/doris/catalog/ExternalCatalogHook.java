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

package org.apache.doris.catalog;

import org.apache.doris.catalog.iam.IAMTokenMgr;
import org.apache.doris.catalog.iam.RoleKeys;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.datasource.CloudProperty;
import org.apache.doris.datasource.ExternalCatalog;

import com.google.common.collect.Maps;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class ExternalCatalogHook {
    private final Map<Long, ExternalCatalog> idToExternalCatalog = Maps.newConcurrentMap();
    private ScheduledThreadPoolExecutor hookExecutor;
    // IAM Token Service
    private IAMTokenMgr tokenManager;

    public ExternalCatalogHook() {
        hookExecutor = ThreadPoolManager.newDaemonScheduledThreadPool(1, "CatalogServiceHook", false);
    }

    public void removeCatalog(ExternalCatalog catalog) {
        idToExternalCatalog.remove(catalog.getId());
        if (tokenManager != null) {
            tokenManager.logoffRemote(catalog.getId());
        }
    }

    public void addCatalog(ExternalCatalog catalog) {
        idToExternalCatalog.put(catalog.getId(), catalog);
        try {
            updateCatalogTokenManager(catalog);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void updateCatalogTokenManager(ExternalCatalog catalog) throws Exception {
        Map<String, String> properties = catalog.getCatalogProperty().getProperties();
        String arn = properties.get(IAMTokenMgr.CLOUD_ARN);
        if (arn == null) {
            // do nothing
            return;
        }
        String roleName = Objects.requireNonNull(properties.get(IAMTokenMgr.CLOUD_ROLE_NAME), "Missing iam role name.");
        String endpoint = Objects.requireNonNull(properties.get(CloudProperty.CLOUD_ENDPOINT),
                CloudProperty.CLOUD_ENDPOINT + " can not be null");
        String region = Objects.requireNonNull(properties.get(CloudProperty.CLOUD_REGION),
                CloudProperty.CLOUD_REGION + " can not be null");
        String externalId = properties.get(IAMTokenMgr.CLOUD_EXTERNAL_ID); // Nullable property
        // Lazy init when adding the first catalog with arn
        if (tokenManager == null) {
            tokenManager = new IAMTokenMgr(idToExternalCatalog, hookExecutor);
            tokenManager.start();
        }
        tokenManager.registerRemote(catalog.getId(), RoleKeys.of(endpoint, region, roleName, arn, externalId));
    }
}
