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

package org.apache.doris.ha;

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MasterInfo implements Writable {

    private String host;
    private int httpPort;
    private int rpcPort;

    public MasterInfo() {
        this.host = "";
        this.httpPort = 0;
        this.rpcPort = 0;
    }

    public MasterInfo(String host, int httpPort, int rpcPort) {
        this.host = host;
        this.httpPort = httpPort;
        this.rpcPort = rpcPort;
    }

    public String getHost() {
        return this.host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getHttpPort() {
        return this.httpPort;
    }

    public void setHttpPort(int httpPort) {
        this.httpPort = httpPort;
    }

    public int getRpcPort() {
        return this.rpcPort;
    }

    public void setRpcPort(int rpcPort) {
        this.rpcPort = rpcPort;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("MasterInfo: ip=").append(host)
                .append(" httpPort=").append(httpPort)
                .append(" rpcPort=").append(rpcPort);
        return sb.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, host);
        out.writeInt(httpPort);
        out.writeInt(rpcPort);
    }

    public void readFields(DataInput in) throws IOException {
        host = Text.readString(in);
        httpPort = in.readInt();
        rpcPort = in.readInt();
    }

}
