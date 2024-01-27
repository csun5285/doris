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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.Pair;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.ha.FrontendNodeType;
import org.apache.doris.load.EtlJobType;
import org.apache.doris.meta.MetaContext;
import org.apache.doris.metric.GaugeMetric;
import org.apache.doris.metric.Metric;
import org.apache.doris.metric.MetricLabel;
import org.apache.doris.metric.MetricRepo;

import com.google.common.collect.Maps;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.List;
import java.util.Map;

public class LoadManagerTest {
    private static final Logger LOG = LogManager.getLogger(LoadManagerTest.class);
    private LoadManager loadManager;
    private final String fieldName = "idToLoadJob";
    private UserIdentity userInfo = UserIdentity.createAnalyzedUserIdentWithIp("root", "localhost");
    private long fakeId = 0;

    @Before
    public void setUp() throws Exception {
        FeConstants.runningUnitTest = true;
        MetricRepo.init();
    }

    @After
    public void tearDown() throws Exception {
        File file = new File("./loadManagerTest");
        if (file.exists()) {
            file.delete();
        }
    }

    @Test
    public void testSerializationNormal(@Mocked Env env, @Mocked InternalCatalog catalog, @Injectable Database database,
            @Injectable Table table) throws Exception {
        new Expectations() {
            {
                env.getInternalCatalog();
                minTimes = 0;
                result = catalog;
                catalog.getDbNullable(anyLong);
                minTimes = 0;
                result = database;
                database.getTableNullable(anyLong);
                minTimes = 0;
                result = table;
                table.getName();
                minTimes = 0;
                result = "tablename";
                Env.getCurrentEnvJournalVersion();
                minTimes = 0;
                result = FeMetaVersion.VERSION_CURRENT;
            }
        };

        loadManager = new LoadManager(new LoadJobScheduler());
        LoadJob job1 = new InsertLoadJob("job1", 1L, 1L, 1L, System.currentTimeMillis(), "", "", userInfo);
        Deencapsulation.invoke(loadManager, "addLoadJob", job1);

        File file = serializeToFile(loadManager);

        LoadManager newLoadManager = deserializeFromFile(file);

        Map<Long, LoadJob> loadJobs = Deencapsulation.getField(loadManager, fieldName);
        Map<Long, LoadJob> newLoadJobs = Deencapsulation.getField(newLoadManager, fieldName);
        Assert.assertEquals(loadJobs, newLoadJobs);
    }

    @Test
    public void testSerializationWithJobRemoved(@Mocked MetaContext metaContext, @Mocked Env env,
            @Mocked InternalCatalog catalog, @Injectable Database database, @Injectable Table table) throws Exception {
        new Expectations() {
            {
                env.getInternalCatalog();
                minTimes = 0;
                result = catalog;
                catalog.getDbNullable(anyLong);
                minTimes = 0;
                result = database;
                database.getTableNullable(anyLong);
                minTimes = 0;
                result = table;
                table.getName();
                minTimes = 0;
                result = "tablename";
                Env.getCurrentEnvJournalVersion();
                minTimes = 0;
                result = FeMetaVersion.VERSION_CURRENT;
            }
        };

        loadManager = new LoadManager(new LoadJobScheduler());
        LoadJob job1 = new InsertLoadJob("job1", 1L, 1L, 1L, System.currentTimeMillis(), "", "", userInfo);
        Deencapsulation.invoke(loadManager, "addLoadJob", job1);

        // make job1 don't serialize
        Config.streaming_label_keep_max_second = 1;
        Thread.sleep(2000);

        File file = serializeToFile(loadManager);

        LoadManager newLoadManager = deserializeFromFile(file);
        Map<Long, LoadJob> newLoadJobs = Deencapsulation.getField(newLoadManager, fieldName);

        Assert.assertEquals(0, newLoadJobs.size());
    }

    @Test
    public void testCleanOverLimitJobs(@Mocked Env env,
            @Mocked InternalCatalog catalog, @Injectable Database database, @Injectable Table table) throws Exception {
        new Expectations() {
            {
                env.getNextId();
                returns(1L, 2L);
                env.getInternalCatalog();
                minTimes = 0;
                result = catalog;
                catalog.getDbNullable(anyLong);
                minTimes = 0;
                result = database;
                database.getTableNullable(anyLong);
                minTimes = 0;
                result = table;
                table.getName();
                minTimes = 0;
                result = "tablename";
                Env.getCurrentEnvJournalVersion();
                minTimes = 0;
                result = FeMetaVersion.VERSION_CURRENT;
            }
        };

        loadManager = new LoadManager(new LoadJobScheduler());
        LoadJob job1 = new InsertLoadJob("job1", 1L, 1L, 1L, System.currentTimeMillis(), "", "", userInfo);
        Thread.sleep(100);
        LoadJob job2 = new InsertLoadJob("job2", 1L, 1L, 1L, System.currentTimeMillis(), "", "", userInfo);
        Deencapsulation.invoke(loadManager, "addLoadJob", job2);
        Deencapsulation.invoke(loadManager, "addLoadJob", job1);
        Config.label_num_threshold = 1;
        loadManager.removeOverLimitLoadJob();
        Map<Long, LoadJob> idToJobs = Deencapsulation.getField(loadManager, fieldName);
        Map<Long, Map<String, List<LoadJob>>> dbIdToLabelToLoadJobs = Deencapsulation.getField(loadManager,
                "dbIdToLabelToLoadJobs");
        Assert.assertEquals(1, idToJobs.size());
        Assert.assertEquals(1, dbIdToLabelToLoadJobs.size());
        LoadJob loadJob = idToJobs.get(job2.getId());
        Assert.assertEquals("job2", loadJob.getLabel());
        Assert.assertNotNull(dbIdToLabelToLoadJobs.get(1L).get("job2"));
    }

    private File serializeToFile(LoadManager loadManager) throws Exception {
        File file = new File("./loadManagerTest");
        file.createNewFile();
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));
        loadManager.write(dos);
        dos.flush();
        dos.close();
        return file;
    }

    private LoadManager deserializeFromFile(File file) throws Exception {
        DataInputStream dis = new DataInputStream(new FileInputStream(file));
        LoadManager loadManager = new LoadManager(new LoadJobScheduler());
        loadManager.readFields(dis);
        return loadManager;
    }

    public Map<Long, LoadJob> createFakeLoadJob(EtlJobType fakeJobType, JobState fakeJobState, long fakeJobNum) {
        Map<Long, LoadJob> idToLoadJob = Maps.newHashMap();
        for (int i = 0; i < fakeJobNum; i++) {
            switch (fakeJobType) {
                case BROKER: {
                    LoadJob loadJob = new BrokerLoadJob();
                    Deencapsulation.setField(loadJob, "state", fakeJobState);
                    Deencapsulation.setField(loadJob, "id", fakeId++);
                    idToLoadJob.put(loadJob.getId(), loadJob);
                }
                    break;
                case INSERT: {
                    LoadJob loadJob = new InsertLoadJob();
                    Deencapsulation.setField(loadJob, "state", fakeJobState);
                    Deencapsulation.setField(loadJob, "id", fakeId++);
                    idToLoadJob.put(loadJob.getId(), loadJob);
                }
                    break;
                default:
                    break;
            }
        }
        return idToLoadJob;
    }

    @Test
    public void testGetLoadJobNumMetrics() {
        loadManager = new LoadManager(null);
        Map<Long, LoadJob> idToLoadJob = Maps.newConcurrentMap();
        idToLoadJob.putAll(createFakeLoadJob(EtlJobType.BROKER, JobState.PENDING, 1235));
        idToLoadJob.putAll(createFakeLoadJob(EtlJobType.BROKER, JobState.LOADING, 1123));
        idToLoadJob.putAll(createFakeLoadJob(EtlJobType.BROKER, JobState.FINISHED, 1131));
        idToLoadJob.putAll(createFakeLoadJob(EtlJobType.BROKER, JobState.CANCELLED, 1132));

        idToLoadJob.putAll(createFakeLoadJob(EtlJobType.INSERT, JobState.PENDING, 9876));
        idToLoadJob.putAll(createFakeLoadJob(EtlJobType.INSERT, JobState.LOADING, 2431));
        idToLoadJob.putAll(createFakeLoadJob(EtlJobType.INSERT, JobState.FINISHED, 4321));
        idToLoadJob.putAll(createFakeLoadJob(EtlJobType.INSERT, JobState.CANCELLED, 932));
        Deencapsulation.setField(loadManager, "idToLoadJob", idToLoadJob);
        Map<Pair<EtlJobType, JobState>, Long> loadJobNum = loadManager.getLoadJobNum();

        LOG.info("loadJobNum:{}", loadJobNum);

        Assert.assertEquals(new Long(1132L), loadJobNum.get(Pair.of(EtlJobType.BROKER, JobState.CANCELLED)));
        Assert.assertEquals(new Long(932L), loadJobNum.get(Pair.of(EtlJobType.INSERT, JobState.CANCELLED)));
        Assert.assertEquals(new Long(4321L), loadJobNum.get(Pair.of(EtlJobType.INSERT, JobState.FINISHED)));

        Deencapsulation.setField(Env.getCurrentEnv(), "loadManager", loadManager);
        Deencapsulation.setField(Env.getCurrentEnv(), "feType", FrontendNodeType.MASTER);
        Deencapsulation.invoke(MetricRepo.class, "updateLoadJobMetrics");

        List<Metric> metrics = MetricRepo.getMetricsByName("job");
        boolean found = false;
        for (Metric metric : metrics) {
            if (!metric.getDescription().equals("job statistics")) {
                continue;
            }

            GaugeMetric<Long> gm = (GaugeMetric<Long>) metric;
            List<MetricLabel> labels = gm.getLabels();
            if (labels.get(0).getValue().equals("load")
                    && labels.get(1).getValue().equals(EtlJobType.BROKER.name())
                    && labels.get(2).getValue().equals(JobState.CANCELLED.name())) {
                found = true;
                Assert.assertEquals(Long.valueOf(1132L), (Long) gm.getValue());
            }
        }
        Assert.assertTrue(found);
    }
}
