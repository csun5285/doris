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

import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.property.PropertyConverter;
import org.apache.doris.datasource.property.constants.S3Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.rest.auth.OAuth2Properties;

import java.util.HashMap;
import java.util.Map;

public class IcebergRestExternalCatalog extends IcebergExternalCatalog {

    // REST catalog property constants
    private static final String PREFIX_PROPERTY = "prefix";
    private static final String VENDED_CREDENTIALS_HEADER = "header.X-Iceberg-Access-Delegation";
    private static final String VENDED_CREDENTIALS_VALUE = "vended-credentials";

    public IcebergRestExternalCatalog(long catalogId, String name, String resource, Map<String, String> props,
                                      String comment) {
        super(catalogId, name, comment);
        props = PropertyConverter.convertToMetaProperties(props);
        catalogProperty = new CatalogProperty(resource, props);
    }

    @Override
    protected void initCatalog() {
        icebergCatalogType = ICEBERG_REST;

        Configuration conf = replaceS3Properties(getConfiguration());

        catalog = CatalogUtil.buildIcebergCatalog(getName(),
                convertToRestCatalogProperties(),
                conf);
    }

    private Configuration replaceS3Properties(Configuration conf) {
        Map<String, String> catalogProperties = catalogProperty.getHadoopProperties();
        initS3Param(conf);
        String usePahStyle = catalogProperties.getOrDefault(PropertyConverter.USE_PATH_STYLE, "true");
        // Set path style
        conf.set(PropertyConverter.USE_PATH_STYLE, usePahStyle);
        conf.set(Constants.PATH_STYLE_ACCESS, usePahStyle);
        // Get AWS client retry limit
        conf.set(Constants.RETRY_LIMIT, catalogProperties.getOrDefault(Constants.RETRY_LIMIT, "1"));
        conf.set(Constants.RETRY_THROTTLE_LIMIT, catalogProperties.getOrDefault(Constants.RETRY_THROTTLE_LIMIT, "1"));
        conf.set(Constants.S3GUARD_CONSISTENCY_RETRY_LIMIT,
                catalogProperties.getOrDefault(Constants.S3GUARD_CONSISTENCY_RETRY_LIMIT, "1"));
        return conf;
    }

    private Map<String, String> convertToRestCatalogProperties() {

        Map<String, String> props = catalogProperty.getProperties();
        Map<String, String> restProperties = new HashMap<>();
        // Core catalog properties
        addCoreCatalogProperties(restProperties, props);
        // S3 FileIO properties (existing logic)
        addS3FileIOProperties(restProperties, props);
        // REST specific properties
        addRestSpecificProperties(restProperties, props);
        // Authentication properties
        addAuthenticationProperties(restProperties, props);
        return restProperties;
    }

    private void addCoreCatalogProperties(Map<String, String> restProperties, Map<String, String> props) {
        restProperties.put(CatalogUtil.ICEBERG_CATALOG_TYPE, CatalogUtil.ICEBERG_CATALOG_TYPE_REST);
        // Support both "iceberg.rest.uri" and "uri" property names
        String restUri = props.get("iceberg.rest.uri");
        if (StringUtils.isBlank(restUri)) {
            restUri = props.getOrDefault(CatalogProperties.URI, "");
        }
        restProperties.put(CatalogProperties.URI, restUri);

        // Optional prefix property
        addIfPresent("iceberg.rest.prefix", props, PREFIX_PROPERTY, restProperties);

        // Warehouse location
        addIfPresent("warehouse", props, CatalogProperties.WAREHOUSE_LOCATION, restProperties);
    }

    private void addS3FileIOProperties(Map<String, String> restProperties, Map<String, String> props) {
        if (props.containsKey(S3Properties.ENDPOINT)) {
            restProperties.put(S3FileIOProperties.ENDPOINT, props.get(S3Properties.ENDPOINT));
        }
        if (props.containsKey(S3Properties.ACCESS_KEY)) {
            restProperties.put(S3FileIOProperties.ACCESS_KEY_ID, props.get(S3Properties.ACCESS_KEY));
        }
        if (props.containsKey(S3Properties.SECRET_KEY)) {
            restProperties.put(S3FileIOProperties.SECRET_ACCESS_KEY, props.get(S3Properties.SECRET_KEY));
        }
        if (props.containsKey(S3Properties.REGION)) {
            restProperties.put(AwsClientProperties.CLIENT_REGION, props.get(S3Properties.REGION));
        }
        if (props.containsKey(PropertyConverter.USE_PATH_STYLE)) {
            restProperties.put(S3FileIOProperties.PATH_STYLE_ACCESS, props.get(PropertyConverter.USE_PATH_STYLE));
        }
    }

    private void addRestSpecificProperties(Map<String, String> restProperties, Map<String, String> props) {
        // Vended credentials support
        String vendedCredentialsEnabled = props.get("iceberg.rest.vended-credentials-enabled");
        if ("true".equalsIgnoreCase(vendedCredentialsEnabled)) {
            restProperties.put(VENDED_CREDENTIALS_HEADER, VENDED_CREDENTIALS_VALUE);
        }

        // Session properties (not fully supported yet, but we can pass them through)
        addIfPresent("iceberg.rest.session", props, "rest.session", restProperties);
        addIfPresent("iceberg.rest.session-timeout", props, "rest.session-timeout", restProperties);

        // Case insensitive name matching (pass through)
        addIfPresent("iceberg.rest.case-insensitive-name-matching", props, "rest.case-insensitive-name-matching",
                restProperties);
        addIfPresent("iceberg.rest.case-insensitive-name-matching.cache-ttl", props,
                "rest.case-insensitive-name-matching.cache-ttl", restProperties);
    }

    private void addAuthenticationProperties(Map<String, String> restProperties, Map<String, String> props) {
        String securityType = props.getOrDefault("iceberg.rest.security.type", "none");

        if ("oauth2".equalsIgnoreCase(securityType)) {
            addOAuth2Properties(restProperties, props);
        }
    }

    private void addOAuth2Properties(Map<String, String> restProperties, Map<String, String> props) {
        String credential = props.get("iceberg.rest.oauth2.credential");
        String token = props.get("iceberg.rest.oauth2.token");

        if (StringUtils.isNotBlank(credential)) {
            // Client Credentials Flow
            restProperties.put(OAuth2Properties.CREDENTIAL, credential);
            addIfPresent("iceberg.rest.oauth2.server-uri", props, "oauth2-server-uri", restProperties);
            addIfPresent("iceberg.rest.oauth2.scope", props, OAuth2Properties.SCOPE, restProperties);

            String tokenRefreshEnabled = props.getOrDefault("iceberg.rest.oauth2.token-refresh-enabled",
                    String.valueOf(OAuth2Properties.TOKEN_REFRESH_ENABLED_DEFAULT));
            restProperties.put(OAuth2Properties.TOKEN_REFRESH_ENABLED, tokenRefreshEnabled);
        } else if (StringUtils.isNotBlank(token)) {
            // Pre-configured Token Flow
            restProperties.put(OAuth2Properties.TOKEN, token);
        }
    }

    private void addIfPresent(String sourceKey, Map<String, String> source, String targetKey,
            Map<String, String> target) {
        String value = source.get(sourceKey);
        if (StringUtils.isNotBlank(value)) {
            target.put(targetKey, value);
        }
    }

    public boolean isVendedCredentialsEnabled() {
        Map<String, String> props = catalogProperty.getProperties();
        return "true".equalsIgnoreCase(props.get("iceberg.rest.vended-credentials-enabled"));
    }
}
