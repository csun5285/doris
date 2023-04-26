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

package org.apache.doris.load.loadv2;

import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;

import com.google.common.collect.Lists;
import com.selectdb.cloud.stage.StageUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class CopyJobTest {

    @Test
    public void testParseLoadFiles() {
        Config.cloud_delete_loaded_internal_stage_files = true;

        String bucket = "selectdb";
        List<String> stagePrefixes = Lists.newArrayList("instance1/stage/root/root",
                "org1/instance1/stage/bob/8f3b7371-c096-48ef-b81c-0eb951dd0f52", "stage/root/root");
        for (String stagePrefix : stagePrefixes) {
            Pair<List<String>, List<String>> files = generateLoadFiles(bucket, stagePrefix);
            Assert.assertEquals(files.second, StageUtil.parseLoadFiles(files.first, bucket, stagePrefix));
        }

        Assert.assertNull(StageUtil.parseLoadFiles(null, bucket, stagePrefixes.get(0)));

        Assert.assertNull(StageUtil.parseLoadFiles(new ArrayList<>(), bucket, "instance1/data/dbId"));

        Config.cloud_delete_loaded_internal_stage_files = false;
        Pair<List<String>, List<String>> files = generateLoadFiles(bucket, stagePrefixes.get(0));
        Assert.assertNull(StageUtil.parseLoadFiles(files.first, bucket, stagePrefixes.get(0)));
    }

    private Pair<List<String>, List<String>> generateLoadFiles(String bucket, String stagePrefix) {
        List<String> loadFiles = new ArrayList<>();
        List<String> parseFiles = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            loadFiles.add("s3://" + bucket + "/" + stagePrefix + "/load" + i);
            parseFiles.add(stagePrefix + "/load" + i);
        }
        for (int i = 0; i < 10; i++) {
            loadFiles.add("s3://" + bucket + "/" + stagePrefix + "/load1/load" + i);
            parseFiles.add(stagePrefix + "/load1/load" + i);
        }
        return Pair.of(loadFiles, parseFiles);
    }
}
