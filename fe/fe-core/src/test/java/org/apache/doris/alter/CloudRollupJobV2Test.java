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

package org.apache.doris.alter;

import org.apache.doris.alter.AlterJobV2.JobState;
import org.apache.doris.analysis.AccessTestUtil;
import org.apache.doris.analysis.AddRollupClause;
import org.apache.doris.analysis.AlterClause;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.catalog.CatalogTestUtil;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FakeEditLog;
import org.apache.doris.catalog.FakeEnv;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.thrift.TTaskType;

import com.google.common.collect.Lists;
import com.selectdb.cloud.catalog.CloudPartition;
import com.selectdb.cloud.catalog.CloudReplica;
import com.selectdb.cloud.proto.SelectdbCloud;
import com.selectdb.cloud.proto.SelectdbCloud.CheckTxnConflictResponse;
import com.selectdb.cloud.proto.SelectdbCloud.CreateTabletsResponse;
import com.selectdb.cloud.proto.SelectdbCloud.GetCurrentMaxTxnResponse;
import com.selectdb.cloud.proto.SelectdbCloud.IndexResponse;
import com.selectdb.cloud.proto.SelectdbCloud.MetaServiceCode;
import com.selectdb.cloud.rpc.MetaServiceProxy;
import mockit.Mock;
import mockit.MockUp;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CloudRollupJobV2Test {
    private static final Logger LOG = LogManager.getLogger(CloudRollupJobV2Test.class);
    private static FakeEditLog fakeEditLog;
    private static FakeEnv fakeEnv;
    private static Env masterEnv;

    private static Analyzer analyzer;
    private static AddRollupClause clause;

    @Before
    public void setUp()
            throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException,
            NoSuchMethodException, SecurityException, AnalysisException, DdlException {
        Config.cloud_unique_id = "cloud_unique_id";
        Config.meta_service_endpoint = "127.0.0.1:20121";
        FakeEnv.setMetaVersion(FeMetaVersion.VERSION_CURRENT);

        analyzer = AccessTestUtil.fetchAdminAnalyzer(false);
        clause = new AddRollupClause(CatalogTestUtil.testRollupIndex2, Lists.newArrayList("k1", "v"), null,
                CatalogTestUtil.testIndex1, null);
        clause.analyze(analyzer);

        FeConstants.runningUnitTest = true;
        AgentTaskQueue.clearAllTasks();
    }

    @Test
    public void testCloudRollupJobV2() throws Exception {
        Set<Long> prepareIndexes = new HashSet<Long>();
        Set<Long> commitIndexes = new HashSet<Long>();
        new MockUp<MetaServiceProxy>(MetaServiceProxy.class) {
            @Mock
            public SelectdbCloud.IndexResponse prepareIndex(SelectdbCloud.IndexRequest request) {
                Assert.assertTrue(request.getExpiration() > 0);
                prepareIndexes.addAll(request.getIndexIdsList());
                LOG.info("prepareIndexes:{}", prepareIndexes);
                IndexResponse.Builder indexResponseBuilder = IndexResponse.newBuilder();
                indexResponseBuilder.setStatus(SelectdbCloud.MetaServiceResponseStatus.newBuilder()
                        .setCode(MetaServiceCode.OK).setMsg("OK"));
                return indexResponseBuilder.build();
            }

            @Mock
            public SelectdbCloud.CreateTabletsResponse createTablets(SelectdbCloud.CreateTabletsRequest request) {
                CreateTabletsResponse.Builder createTabletsResponseBuilder = CreateTabletsResponse.newBuilder();
                createTabletsResponseBuilder.setStatus(SelectdbCloud.MetaServiceResponseStatus.newBuilder()
                        .setCode(MetaServiceCode.OK).setMsg("OK"));
                return createTabletsResponseBuilder.build();
            }

            @Mock
            public SelectdbCloud.IndexResponse commitIndex(SelectdbCloud.IndexRequest request) {
                commitIndexes.addAll(request.getIndexIdsList());
                LOG.info("commitIndexes:{}", commitIndexes);
                IndexResponse.Builder indexResponseBuilder = IndexResponse.newBuilder();
                indexResponseBuilder.setStatus(SelectdbCloud.MetaServiceResponseStatus.newBuilder()
                        .setCode(MetaServiceCode.OK).setMsg("OK"));
                return indexResponseBuilder.build();
            }

            @Mock
            public SelectdbCloud.GetCurrentMaxTxnResponse getCurrentMaxTxnId(SelectdbCloud.GetCurrentMaxTxnRequest request) {
                GetCurrentMaxTxnResponse.Builder getCurrentMaxTxnResponseBuilder = GetCurrentMaxTxnResponse.newBuilder();
                getCurrentMaxTxnResponseBuilder.setStatus(SelectdbCloud.MetaServiceResponseStatus.newBuilder()
                        .setCode(MetaServiceCode.OK).setMsg("OK"))
                        .setCurrentMaxTxnId(1000);
                return getCurrentMaxTxnResponseBuilder.build();
            }

            @Mock
            public SelectdbCloud.CheckTxnConflictResponse checkTxnConflict(SelectdbCloud.CheckTxnConflictRequest request) {
                CheckTxnConflictResponse.Builder checkTxnConflictResponseBuilder = CheckTxnConflictResponse.newBuilder();
                checkTxnConflictResponseBuilder.setStatus(SelectdbCloud.MetaServiceResponseStatus.newBuilder()
                        .setCode(MetaServiceCode.OK).setMsg("OK"))
                        .setFinished(true);
                return checkTxnConflictResponseBuilder.build();
            }
        };

        new MockUp<CloudPartition>(CloudPartition.class) {
            @Mock
            public long getVisibleVersion() {
                return 1000;
            }
        };

        new MockUp<CloudReplica>(CloudReplica.class) {
            @Mock
            public long getBackendId() {
                return CatalogTestUtil.testBackendId1;
            }
        };

        new MockUp<AgentTaskExecutor>(AgentTaskExecutor.class) {
            @Mock
            public void submit(AgentBatchTask task) {
                // do nothing
                return;
            }
        };

        new MockUp<SystemInfoService>(SystemInfoService.class) {
            @Mock
            public String getCloudClusterIdByName(String clusterName) {
                // do nothing
                return "mock_cluster_name";
            }
        };

        fakeEditLog = new FakeEditLog();
        fakeEnv = new FakeEnv();
        masterEnv = CatalogTestUtil.createTestCatalog();
        Assert.assertEquals(Env.getCurrentEnv(), masterEnv);

        MaterializedViewHandler materializedViewHandler = Env.getCurrentEnv().getMaterializedViewHandler();

        // add a rollup job
        ArrayList<AlterClause> alterClauses = new ArrayList<>();
        alterClauses.add(clause);
        Database db = masterEnv.getInternalCatalog().getDbOrDdlException(CatalogTestUtil.testDbId1);
        OlapTable olapTable = (OlapTable) db.getTableOrDdlException(CatalogTestUtil.testTableId1);
        Partition testPartition = olapTable.getPartition(CatalogTestUtil.testPartitionId1);
        materializedViewHandler.process(alterClauses, db.getClusterName(), db, olapTable);
        Map<Long, AlterJobV2> alterJobsV2 = materializedViewHandler.getAlterJobsV2();
        Assert.assertEquals(1, alterJobsV2.size());
        RollupJobV2 rollupJob = (RollupJobV2) alterJobsV2.values().stream().findAny().get();

        // runPendingJob
        materializedViewHandler.runAfterCatalogReady();
        Assert.assertEquals(JobState.WAITING_TXN, rollupJob.getJobState());
        Assert.assertEquals(2, testPartition.getMaterializedIndices(IndexExtState.ALL).size());
        Assert.assertEquals(1, testPartition.getMaterializedIndices(IndexExtState.VISIBLE).size());
        Assert.assertEquals(1, testPartition.getMaterializedIndices(IndexExtState.SHADOW).size());

        // runWaitingTxnJob
        materializedViewHandler.runAfterCatalogReady();
        Assert.assertEquals(JobState.RUNNING, rollupJob.getJobState());

        // runWaitingTxnJob, task not finished
        materializedViewHandler.runAfterCatalogReady();
        Assert.assertEquals(JobState.RUNNING, rollupJob.getJobState());

        // finish all tasks
        List<AgentTask> tasks = AgentTaskQueue.getTask(TTaskType.ALTER);
        Assert.assertEquals(1, tasks.size());
        for (AgentTask agentTask : tasks) {
            agentTask.setFinished(true);
        }
        MaterializedIndex shadowIndex = testPartition.getMaterializedIndices(IndexExtState.SHADOW).get(0);
        for (Tablet shadowTablet : shadowIndex.getTablets()) {
            for (Replica shadowReplica : shadowTablet.getReplicas()) {
                shadowReplica.updateVersionInfo(testPartition.getVisibleVersion(),
                        shadowReplica.getDataSize(),
                        shadowReplica.getRemoteDataSize(),
                        shadowReplica.getRowCount());
            }
        }

        materializedViewHandler.runAfterCatalogReady();
        Assert.assertEquals(JobState.FINISHED, rollupJob.getJobState());
        Assert.assertEquals(prepareIndexes, commitIndexes);
    }
}
