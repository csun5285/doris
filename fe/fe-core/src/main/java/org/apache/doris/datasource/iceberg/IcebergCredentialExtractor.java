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

import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Lightweight credential extractor for Iceberg vended credentials.
 * Converts Iceberg FileIO properties to backend-compatible credential format.
 */
public class IcebergCredentialExtractor {

    // AWS credential property keys for backend
    public static final String BACKEND_AWS_ACCESS_KEY = "AWS_ACCESS_KEY";
    public static final String BACKEND_AWS_SECRET_KEY = "AWS_SECRET_KEY";
    public static final String BACKEND_AWS_TOKEN = "AWS_TOKEN";
    public static final String BACKEND_AWS_ENDPOINT = "AWS_ENDPOINT";
    public static final String BACKEND_AWS_REGION = "AWS_REGION";

    /**
     * Extract credentials from Iceberg FileIO properties map.
     *
     * @param ioProperties FileIO properties map from table.io().properties()
     * @return extracted credentials as backend properties
     */
    public static Map<String, String> extractCredentials(Map<String, String> ioProperties) {
        if (ioProperties == null || ioProperties.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, String> credentials = new HashMap<>();
        // Extract AWS S3 credentials
        extractAwsCredentials(ioProperties, credentials);
        return credentials;
    }

    private static void extractAwsCredentials(Map<String, String> source, Map<String, String> target) {
        // S3 credentials from Iceberg S3FileIOProperties
        mapIfPresent(source, "s3.access-key-id", target, BACKEND_AWS_ACCESS_KEY);
        mapIfPresent(source, "s3.secret-access-key", target, BACKEND_AWS_SECRET_KEY);
        mapIfPresent(source, "s3.session-token", target, BACKEND_AWS_TOKEN);
        mapIfPresent(source, "s3.endpoint", target, BACKEND_AWS_ENDPOINT);

        // Region from AwsClientProperties
        mapIfPresent(source, "client.region", target, BACKEND_AWS_REGION);

        // Alternative AWS property formats
        mapIfPresent(source, "aws.access-key-id", target, BACKEND_AWS_ACCESS_KEY);
        mapIfPresent(source, "aws.secret-access-key", target, BACKEND_AWS_SECRET_KEY);
        mapIfPresent(source, "aws.session-token", target, BACKEND_AWS_TOKEN);
        mapIfPresent(source, "aws.region", target, BACKEND_AWS_REGION);
    }

    private static void mapIfPresent(Map<String, String> source, String sourceKey,
            Map<String, String> target, String targetKey) {
        String value = source.get(sourceKey);
        if (StringUtils.isNotBlank(value)) {
            target.put(targetKey, value);
        }
    }

    /**
     * Check if the properties contain any cloud storage credentials
     */
    public static boolean hasCloudStorageCredentials(Map<String, String> ioProperties) {
        if (ioProperties == null || ioProperties.isEmpty()) {
            return false;
        }

        return ioProperties.keySet().stream()
                .anyMatch(key -> key.startsWith("s3.")
                        || key.startsWith("oss.")
                        || key.startsWith("cos.")
                        || key.startsWith("aws."));
    }
}
