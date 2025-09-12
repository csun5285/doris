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

package org.apache.doris.datasource.iceberg;

import org.apache.iceberg.Table;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.Map;

/**
 * Simplified Vended Credentials Provider for Iceberg tables.
 * Provides vended credentials functionality without complex abstraction layers.
 */
public class IcebergVendedCredentialsProvider {
    private static final Logger LOG = LogManager.getLogger(IcebergVendedCredentialsProvider.class);
    private static final IcebergVendedCredentialsProvider INSTANCE = new IcebergVendedCredentialsProvider();

    private IcebergVendedCredentialsProvider() {
        // Singleton pattern
    }

    public static IcebergVendedCredentialsProvider getInstance() {
        return INSTANCE;
    }

    /**
     * Get vended credentials from an Iceberg table if enabled.
     *
     * @param catalog The Iceberg REST catalog
     * @param table The Iceberg table
     * @return Map of backend-compatible credential properties, empty if disabled or unavailable
     */
    public Map<String, String> getVendedCredentials(IcebergRestExternalCatalog catalog, Table table) {
        try {
            if (!catalog.isVendedCredentialsEnabled()) {
                return Collections.emptyMap();
            }

            if (table == null || table.io() == null) {
                return Collections.emptyMap();
            }

            Map<String, String> ioProperties = table.io().properties();
            if (!IcebergCredentialExtractor.hasCloudStorageCredentials(ioProperties)) {
                return Collections.emptyMap();
            }

            Map<String, String> credentials = IcebergCredentialExtractor.extractCredentials(ioProperties);

            if (LOG.isDebugEnabled()) {
                LOG.debug("Successfully extracted vended credentials for table: {}", table.name());
            }

            return credentials;

        } catch (Exception e) {
            LOG.warn("Failed to get vended credentials for table: {}, returning empty map",
                    table != null ? table.name() : "null", e);
            return Collections.emptyMap();
        }
    }

}
