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
suite("smoke_test_index", "smoke") {
    if (context.config.cloudVersion != null && !context.config.cloudVersion.isEmpty()
            && compareCloudVersion(context.config.cloudVersion, "3.0.0") < 0) {
        log.info("case: smoke_test_index, cloud version ${context.config.cloudVersion} less than 3.0.0, skip".toString());
        return
    }
    // todo: test bitmap index, such as create, drop, alter table index
    def tables = sql "show tables"
    if (tables != null) {
        if (tables[0] != null) {
            def tb = tables[0][0]
            logger.info("$tb")
            sql "show index from ${tables[0][0]}"
        }
    }
}
