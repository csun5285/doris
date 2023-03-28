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

import javax.annotation.Nullable;

public class RoleKeys {
    private String endpoint;
    private String region;
    private String roleName;
    private String arn;

    private String externalId;

    private RoleKeys(String endpoint, String region, String roleName, String arn, @Nullable String externalId) {
        this.endpoint = endpoint;
        this.region = region;
        this.roleName = roleName;
        this.arn = arn;
        this.externalId = externalId;
    }

    public static RoleKeys of(String endpoint, String region, String roleName, String arn) {
        return of(endpoint, region, roleName, arn, null);
    }

    public static RoleKeys of(String endpoint, String region, String roleName, String arn, String externalId) {
        return new RoleKeys(endpoint, region, roleName, arn, externalId);
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getRegion() {
        return region;
    }

    public String getRoleName() {
        return roleName;
    }

    public String getArn() {
        return arn;
    }

    public String getExternalId() {
        return externalId;
    }
}
