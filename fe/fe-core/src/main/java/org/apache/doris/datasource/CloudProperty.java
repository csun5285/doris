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

package org.apache.doris.datasource;

import org.apache.doris.catalog.S3Resource;

import com.google.common.collect.Maps;
import org.apache.hadoop.fs.obs.OBSConstants;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.S3AFileSystem;

import java.util.Map;

public class CloudProperty {

    public static final String S3_FS_PREFIX = "fs.s3";
    public static final String OBS_FS_PREFIX = "fs.obs";

    public static final String CLOUD_ENDPOINT = "AWS_ENDPOINT";
    public static final String CLOUD_REGION = "AWS_REGION";
    public static final String CLOUD_ACCESS_KEY = "AWS_ACCESS_KEY";
    public static final String CLOUD_SECRET_KEY = "AWS_SECRET_KEY";
    public static final String CLOUD_SESSION_TOKEN = "AWS_TOKEN";

    public enum PropertyType {
        S3,
        OBS,
        OSS,
        BOS,
        COS
    }

    public static Map<String, String> convert(Map<String, String> props) {
        if (props.containsKey(CLOUD_ENDPOINT)) {
            String endpoint = props.get(CLOUD_ENDPOINT);
            String[] endpointStr = endpoint.split("\\.");
            if (endpointStr.length < 2) {
                throw new IllegalArgumentException("Fail to parse cloud storage type for " + endpoint);
            }
            String cloudStorageType = endpointStr[0].toUpperCase();
            PropertyType type = PropertyType.valueOf(cloudStorageType);
            return type == PropertyType.OBS ? convertToOBSProperties(props) : convertToS3Properties(props);
        }
        return props;
    }

    public static Map<String, String> convertToOBSProperties(Map<String, String> props) {
        Map<String, String> obsProperties = Maps.newHashMap();
        obsProperties.put(OBSConstants.ENDPOINT, props.get(CLOUD_ENDPOINT));
        if (props.containsKey(CLOUD_ACCESS_KEY)) {
            obsProperties.put(OBSConstants.ACCESS_KEY, props.get(CLOUD_ACCESS_KEY));
        }
        if (props.containsKey(CLOUD_SECRET_KEY)) {
            obsProperties.put(OBSConstants.SECRET_KEY, props.get(CLOUD_SECRET_KEY));
        }
        obsProperties.put("fs.obs.impl.disable.cache", "true");
        if (props.containsKey(CLOUD_SESSION_TOKEN)) {
            obsProperties.put("fs.obs.session.token", props.get(CLOUD_SESSION_TOKEN));
        }
        for (Map.Entry<String, String> entry : props.entrySet()) {
            if (entry.getKey().startsWith(OBS_FS_PREFIX)) {
                obsProperties.put(entry.getKey(), entry.getValue());
            }
        }
        return obsProperties;
    }

    private static Map<String, String> convertToS3Properties(Map<String, String> properties) {
        Map<String, String> s3Properties = Maps.newHashMap();
        s3Properties.put(Constants.ENDPOINT, properties.get(CLOUD_ENDPOINT));
        if (properties.containsKey(CLOUD_ACCESS_KEY)) {
            s3Properties.put(Constants.ACCESS_KEY, properties.get(CLOUD_ACCESS_KEY));
        }
        if (properties.containsKey(CLOUD_SECRET_KEY)) {
            s3Properties.put(Constants.SECRET_KEY, properties.get(CLOUD_SECRET_KEY));
        }
        if (properties.containsKey(CLOUD_REGION)) {
            s3Properties.put(Constants.AWS_REGION, properties.get(CLOUD_REGION));
        }
        if (properties.containsKey(S3Resource.S3_MAX_CONNECTIONS)) {
            s3Properties.put(Constants.MAXIMUM_CONNECTIONS, properties.get(S3Resource.S3_MAX_CONNECTIONS));
        }
        if (properties.containsKey(S3Resource.S3_REQUEST_TIMEOUT_MS)) {
            s3Properties.put(Constants.REQUEST_TIMEOUT, properties.get(S3Resource.S3_REQUEST_TIMEOUT_MS));
        }
        if (properties.containsKey(S3Resource.S3_CONNECTION_TIMEOUT_MS)) {
            s3Properties.put(Constants.SOCKET_TIMEOUT, properties.get(S3Resource.S3_CONNECTION_TIMEOUT_MS));
        }
        s3Properties.put(Constants.MAX_ERROR_RETRIES, "2");
        s3Properties.put("fs.s3.impl.disable.cache", "true");
        s3Properties.put("fs.s3.impl", S3AFileSystem.class.getName());

        s3Properties.put(Constants.PATH_STYLE_ACCESS, properties.getOrDefault(S3Resource.USE_PATH_STYLE, "false"));
        if (properties.containsKey(CLOUD_SESSION_TOKEN)) {
            s3Properties.put(Constants.SESSION_TOKEN, properties.get(CLOUD_SESSION_TOKEN));
            s3Properties.put(Constants.AWS_CREDENTIALS_PROVIDER,
                    "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider");
            s3Properties.put("fs.s3.impl.disable.cache", "true");
            s3Properties.put("fs.s3a.impl.disable.cache", "true");
        }
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().startsWith(S3_FS_PREFIX)) {
                s3Properties.put(entry.getKey(), entry.getValue());
            }
        }

        return s3Properties;
    }

}
