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

package org.apache.doris.mysql.privilege;

import org.apache.doris.analysis.ResourcePattern;
import org.apache.doris.analysis.TablePattern;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PaloRole implements Writable {
    // operator is responsible for operating cluster, such as add/drop node
    public static String OPERATOR_ROLE = "operator";
    // admin is like DBA, who has all privileges except for NODE privilege held by operator
    public static String ADMIN_ROLE = "admin";

    public static PaloRole OPERATOR = new PaloRole(OPERATOR_ROLE,
            TablePattern.ALL, PrivBitSet.of(PaloPrivilege.NODE_PRIV, PaloPrivilege.ADMIN_PRIV),
            Arrays.asList(ResourcePattern.ALL_GENERAL, ResourcePattern.ALL_CLUSTER, ResourcePattern.ALL_STAGE),
            PrivBitSet.of(PaloPrivilege.NODE_PRIV, PaloPrivilege.ADMIN_PRIV));
    public static PaloRole ADMIN = new PaloRole(ADMIN_ROLE,
            TablePattern.ALL, PrivBitSet.of(PaloPrivilege.ADMIN_PRIV),
            Arrays.asList(ResourcePattern.ALL_GENERAL, ResourcePattern.ALL_CLUSTER, ResourcePattern.ALL_STAGE),
            PrivBitSet.of(PaloPrivilege.ADMIN_PRIV));

    private String roleName;
    private Map<TablePattern, PrivBitSet> tblPatternToPrivs = Maps.newConcurrentMap();
    private Map<ResourcePattern, PrivBitSet> resourcePatternToPrivs = Maps.newConcurrentMap();
    private Map<ResourcePattern, PrivBitSet> clusterPatternToPrivs = Maps.newConcurrentMap();
    private Map<ResourcePattern, PrivBitSet> stagePatternToPrivs = Maps.newConcurrentMap();
    // users which this role
    private Set<UserIdentity> users = Sets.newConcurrentHashSet();

    private PaloRole() {

    }

    public PaloRole(String roleName) {
        this.roleName = roleName;
    }

    public PaloRole(String roleName, TablePattern tablePattern, PrivBitSet privs) {
        this.roleName = roleName;
        this.tblPatternToPrivs.put(tablePattern, privs);
    }

    public PaloRole(String roleName, List<ResourcePattern> resourcePatterns, PrivBitSet privs) {
        this.roleName = roleName;
        resourcePatterns.forEach(r -> {
            if (r.isGeneralResource()) {
                this.resourcePatternToPrivs.put(r, privs);
            } else if (r.isClusterResource()) {
                this.clusterPatternToPrivs.put(r, privs);
            } else if (r.isStageResource()) {
                this.stagePatternToPrivs.put(r, privs);
            }
        });

    }

    public PaloRole(String roleName, TablePattern tablePattern, PrivBitSet tablePrivs,
                    List<ResourcePattern> resourcePatterns, PrivBitSet resourcePrivs) {
        this.roleName = roleName;
        this.tblPatternToPrivs.put(tablePattern, tablePrivs);
        resourcePatterns.forEach(r -> {
            if (r.isGeneralResource()) {
                this.resourcePatternToPrivs.put(r, resourcePrivs);
            } else if (r.isClusterResource()) {
                this.clusterPatternToPrivs.put(r, resourcePrivs);
            } else if (r.isStageResource()) {
                this.stagePatternToPrivs.put(r, resourcePrivs);
            }
        });
    }

    public String getRoleName() {
        return roleName;
    }

    public Map<TablePattern, PrivBitSet> getTblPatternToPrivs() {
        return tblPatternToPrivs;
    }

    public Map<ResourcePattern, PrivBitSet> getResourcePatternToPrivs() {
        return resourcePatternToPrivs;
    }

    public Map<ResourcePattern, PrivBitSet> getClusterPatternToPrivs() {
        return clusterPatternToPrivs;
    }

    public Map<ResourcePattern, PrivBitSet> getStagePatternToPrivs() {
        return stagePatternToPrivs;
    }

    public Set<UserIdentity> getUsers() {
        return users;
    }

    // merge role not check role name.
    public void mergeNotCheck(PaloRole other) {
        for (Map.Entry<TablePattern, PrivBitSet> entry : other.getTblPatternToPrivs().entrySet()) {
            if (tblPatternToPrivs.containsKey(entry.getKey())) {
                PrivBitSet existPrivs = tblPatternToPrivs.get(entry.getKey());
                existPrivs.or(entry.getValue());
            } else {
                tblPatternToPrivs.put(entry.getKey(), entry.getValue());
            }
        }
        for (Map.Entry<ResourcePattern, PrivBitSet> entry : other.resourcePatternToPrivs.entrySet()) {
            if (resourcePatternToPrivs.containsKey(entry.getKey())) {
                PrivBitSet existPrivs = resourcePatternToPrivs.get(entry.getKey());
                existPrivs.or(entry.getValue());
            } else {
                resourcePatternToPrivs.put(entry.getKey(), entry.getValue());
            }
        }

        for (Map.Entry<ResourcePattern, PrivBitSet> entry : other.clusterPatternToPrivs.entrySet()) {
            if (clusterPatternToPrivs.containsKey(entry.getKey())) {
                PrivBitSet existPrivs = clusterPatternToPrivs.get(entry.getKey());
                existPrivs.or(entry.getValue());
            } else {
                clusterPatternToPrivs.put(entry.getKey(), entry.getValue());
            }
        }

        for (Map.Entry<ResourcePattern, PrivBitSet> entry : other.stagePatternToPrivs.entrySet()) {
            if (stagePatternToPrivs.containsKey(entry.getKey())) {
                PrivBitSet existPrivs = stagePatternToPrivs.get(entry.getKey());
                existPrivs.or(entry.getValue());
            } else {
                stagePatternToPrivs.put(entry.getKey(), entry.getValue());
            }
        }
    }

    public void merge(PaloRole other) {
        Preconditions.checkState(roleName.equalsIgnoreCase(other.getRoleName()));
        mergeNotCheck(other);
    }

    public void addUser(UserIdentity userIdent) {
        users.add(userIdent);
    }

    public void dropUser(UserIdentity userIdentity) {
        Iterator<UserIdentity> iter = users.iterator();
        while (iter.hasNext()) {
            UserIdentity userIdent = iter.next();
            if (userIdent.equals(userIdentity)) {
                iter.remove();
            }
        }
    }

    public boolean containsUser(UserIdentity userIdent) {
        return users.contains(userIdent);
    }

    public static PaloRole read(DataInput in) throws IOException {
        PaloRole role = new PaloRole();
        role.readFields(in);
        return role;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, roleName);
        out.writeInt(tblPatternToPrivs.size());
        for (Map.Entry<TablePattern, PrivBitSet> entry : tblPatternToPrivs.entrySet()) {
            entry.getKey().write(out);
            entry.getValue().write(out);
        }
        out.writeInt(resourcePatternToPrivs.size());
        for (Map.Entry<ResourcePattern, PrivBitSet> entry : resourcePatternToPrivs.entrySet()) {
            entry.getKey().write(out);
            entry.getValue().write(out);
        }
        out.writeInt(clusterPatternToPrivs.size());
        for (Map.Entry<ResourcePattern, PrivBitSet> entry : clusterPatternToPrivs.entrySet()) {
            entry.getKey().write(out);
            entry.getValue().write(out);
        }
        out.writeInt(stagePatternToPrivs.size());
        for (Map.Entry<ResourcePattern, PrivBitSet> entry : stagePatternToPrivs.entrySet()) {
            entry.getKey().write(out);
            entry.getValue().write(out);
        }
        out.writeInt(users.size());
        for (UserIdentity userIdentity : users) {
            userIdentity.write(out);
        }
    }

    public void readFields(DataInput in) throws IOException {
        roleName = Text.readString(in);
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            TablePattern tblPattern = TablePattern.read(in);
            PrivBitSet privs = PrivBitSet.read(in);
            tblPatternToPrivs.put(tblPattern, privs);
        }
        size = in.readInt();
        for (int i = 0; i < size; i++) {
            ResourcePattern resourcePattern = ResourcePattern.read(in);
            PrivBitSet privs = PrivBitSet.read(in);
            resourcePatternToPrivs.put(resourcePattern, privs);
        }
        size = in.readInt();
        for (int i = 0; i < size; i++) {
            ResourcePattern resourcePattern = ResourcePattern.read(in);
            PrivBitSet privs = PrivBitSet.read(in);
            clusterPatternToPrivs.put(resourcePattern, privs);
        }
        size = in.readInt();
        for (int i = 0; i < size; i++) {
            ResourcePattern resourcePattern = ResourcePattern.read(in);
            PrivBitSet privs = PrivBitSet.read(in);
            stagePatternToPrivs.put(resourcePattern, privs);
        }
        size = in.readInt();
        for (int i = 0; i < size; i++) {
            UserIdentity userIdentity = UserIdentity.read(in);
            users.add(userIdentity);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("role: ").append(roleName).append(", db table privs: ").append(tblPatternToPrivs);
        sb.append(", resource privs: ").append(resourcePatternToPrivs);
        sb.append(", cluster privs: ").append(clusterPatternToPrivs);
        sb.append(", stage privs: ").append(stagePatternToPrivs);
        sb.append(", users: ").append(users);
        return sb.toString();
    }
}
