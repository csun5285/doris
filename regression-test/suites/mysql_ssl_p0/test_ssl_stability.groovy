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

suite("test_ssl_stability") {
    int test_count = 5;
    while (test_count-- > 1) {
        StringBuilder selectCommand = new StringBuilder();
        selectCommand.append("SELECT ");
        int select_row_count = 100000;
        for (int i = 0; i < select_row_count; ++i) {
            selectCommand.append(" " + i);
            if (i != select_row_count - 1) {
                selectCommand.append(", ");
            }
        }
        // Intentionally creating SQL syntax errors
        selectCommand.append(",");
        try {
            sql selectCommand.toString();
        } catch (java.sql.SQLException t) {
            assertTrue(true);
        }

    }
}
