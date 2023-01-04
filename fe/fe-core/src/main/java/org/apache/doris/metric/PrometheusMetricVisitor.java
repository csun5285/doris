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

package org.apache.doris.metric;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.monitor.jvm.JvmStats;
import org.apache.doris.monitor.jvm.JvmStats.GarbageCollector;
import org.apache.doris.monitor.jvm.JvmStats.MemoryPool;
import org.apache.doris.monitor.jvm.JvmStats.Threads;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;
import com.google.common.base.Joiner;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/*
 * Like this:
 * # HELP doris_fe_job_load_broker_cost_ms doris_fe_job_load_broker_cost_ms
 * # TYPE doris_fe_job_load_broker_cost_ms gauge
 * doris_fe_job{job="load", type="mini", state="pending"} 0
 */
public class PrometheusMetricVisitor extends MetricVisitor {
    // jvm
    private static final String JVM_HEAP_SIZE_BYTES = "jvm_heap_size_bytes";
    private static final String JVM_NON_HEAP_SIZE_BYTES = "jvm_non_heap_size_bytes";
    private static final String JVM_YOUNG_SIZE_BYTES = "jvm_young_size_bytes";
    private static final String JVM_OLD_SIZE_BYTES = "jvm_old_size_bytes";
    private static final String JVM_YOUNG_GC = "jvm_young_gc";
    private static final String JVM_OLD_GC = "jvm_old_gc";
    private static final String JVM_THREAD = "jvm_thread";

    private static final String HELP = "# HELP ";
    private static final String TYPE = "# TYPE ";

    private Set<String> metricNames = new HashSet();

    public PrometheusMetricVisitor() {
        super();
    }

    @Override
    public void setMetricNumber(int metricNumber) {
    }

    @Override
    public void visitJvm(StringBuilder sb, JvmStats jvmStats) {
        // heap
        sb.append(Joiner.on(" ").join(HELP, JVM_HEAP_SIZE_BYTES, "jvm heap stat\n"));
        sb.append(Joiner.on(" ").join(TYPE, JVM_HEAP_SIZE_BYTES, "gauge\n"));
        sb.append(JVM_HEAP_SIZE_BYTES).append("{type=\"max\"} ").append(jvmStats.getMem().getHeapMax().getBytes())
                .append("\n");
        sb.append(JVM_HEAP_SIZE_BYTES).append("{type=\"committed\"} ")
                .append(jvmStats.getMem().getHeapCommitted().getBytes()).append("\n");
        sb.append(JVM_HEAP_SIZE_BYTES).append("{type=\"used\"} ").append(jvmStats.getMem().getHeapUsed().getBytes())
                .append("\n");
        // non heap
        sb.append(Joiner.on(" ").join(HELP, JVM_NON_HEAP_SIZE_BYTES, "jvm non heap stat\n"));
        sb.append(Joiner.on(" ").join(TYPE, JVM_NON_HEAP_SIZE_BYTES, "gauge\n"));
        sb.append(JVM_NON_HEAP_SIZE_BYTES).append("{type=\"committed\"} ")
                .append(jvmStats.getMem().getNonHeapCommitted().getBytes()).append("\n");
        sb.append(JVM_NON_HEAP_SIZE_BYTES).append("{type=\"used\"} ")
                .append(jvmStats.getMem().getNonHeapUsed().getBytes()).append("\n");

        // mem pool
        Iterator<MemoryPool> memIter = jvmStats.getMem().iterator();
        while (memIter.hasNext()) {
            MemoryPool memPool = memIter.next();
            if (memPool.getName().equalsIgnoreCase("young")) {
                sb.append(Joiner.on(" ").join(HELP, JVM_YOUNG_SIZE_BYTES, "jvm young mem pool stat\n"));
                sb.append(Joiner.on(" ").join(TYPE, JVM_YOUNG_SIZE_BYTES, "gauge\n"));
                sb.append(JVM_YOUNG_SIZE_BYTES).append("{type=\"used\"} ")
                        .append(memPool.getUsed().getBytes()).append("\n");
                sb.append(JVM_YOUNG_SIZE_BYTES).append("{type=\"peak_used\"} ")
                        .append(memPool.getPeakUsed().getBytes()).append("\n");
                sb.append(JVM_YOUNG_SIZE_BYTES).append("{type=\"max\"} ")
                        .append(memPool.getMax().getBytes()).append("\n");
            } else if (memPool.getName().equalsIgnoreCase("old")) {
                sb.append(Joiner.on(" ").join(HELP, JVM_OLD_SIZE_BYTES, "jvm old mem pool stat\n"));
                sb.append(Joiner.on(" ").join(TYPE, JVM_OLD_SIZE_BYTES, "gauge\n"));
                sb.append(JVM_OLD_SIZE_BYTES).append("{type=\"used\"} ")
                        .append(memPool.getUsed().getBytes()).append("\n");
                sb.append(JVM_OLD_SIZE_BYTES).append("{type=\"peak_used\"} ")
                        .append(memPool.getPeakUsed().getBytes()).append("\n");
                sb.append(JVM_OLD_SIZE_BYTES).append("{type=\"max\"} "
                ).append(memPool.getMax().getBytes()).append("\n");
            }
        }

        // gc
        Iterator<GarbageCollector> gcIter = jvmStats.getGc().iterator();
        while (gcIter.hasNext()) {
            GarbageCollector gc = gcIter.next();
            if (gc.getName().equalsIgnoreCase("young")) {
                sb.append(Joiner.on(" ").join(HELP, JVM_YOUNG_GC, "jvm young gc stat\n"));
                sb.append(Joiner.on(" ").join(TYPE, JVM_YOUNG_GC, "gauge\n"));
                sb.append(JVM_YOUNG_GC).append("{type=\"count\"} ").append(gc.getCollectionCount()).append("\n");
                sb.append(JVM_YOUNG_GC).append("{type=\"time\"} ")
                        .append(gc.getCollectionTime().getMillis()).append("\n");
            } else if (gc.getName().equalsIgnoreCase("old")) {
                sb.append(Joiner.on(" ").join(HELP, JVM_OLD_GC, "jvm old gc stat\n"));
                sb.append(Joiner.on(" ").join(TYPE, JVM_OLD_GC, "gauge\n"));
                sb.append(JVM_OLD_GC).append("{type=\"count\"} ").append(gc.getCollectionCount()).append("\n");
                sb.append(JVM_OLD_GC).append("{type=\"time\"} ")
                        .append(gc.getCollectionTime().getMillis()).append("\n");
            }
        }

        // threads
        Threads threads = jvmStats.getThreads();
        sb.append(Joiner.on(" ").join(HELP, JVM_THREAD, "jvm thread stat\n"));
        sb.append(Joiner.on(" ").join(TYPE, JVM_THREAD, "gauge\n"));
        sb.append(JVM_THREAD).append("{type=\"count\"} ")
                .append(threads.getCount()).append("\n");
        sb.append(JVM_THREAD).append("{type=\"peak_count\"} ")
                .append(threads.getPeakCount()).append("\n");
        sb.append(JVM_THREAD).append("{type=\"new_count\"} ")
                .append(threads.getThreadsNewCount()).append("\n");
        sb.append(JVM_THREAD).append("{type=\"runnable_count\"} ")
                .append(threads.getThreadsRunnableCount()).append("\n");
        sb.append(JVM_THREAD).append("{type=\"blocked_count\"} ")
                .append(threads.getThreadsBlockedCount()).append("\n");
        sb.append(JVM_THREAD).append("{type=\"waiting_count\"} ")
                .append(threads.getThreadsWaitingCount()).append("\n");
        sb.append(JVM_THREAD).append("{type=\"timed_waiting_count\"} ")
                .append(threads.getThreadsTimedWaitingCount()).append("\n");
        sb.append(JVM_THREAD).append("{type=\"terminated_count\"} ")
                .append(threads.getThreadsTerminatedCount()).append("\n");
        return;
    }

    @Override
    public void visit(StringBuilder sb, String prefix, @SuppressWarnings("rawtypes") Metric metric) {
        // title
        final String fullName = prefix + metric.getName();
        if (!metricNames.contains(fullName)) {
            sb.append(HELP).append(fullName).append(" ").append(metric.getDescription()).append("\n");
            sb.append(TYPE).append(fullName).append(" ").append(metric.getType().name().toLowerCase()).append("\n");
            metricNames.add(fullName);
        }
        sb.append(fullName);

        // name
        @SuppressWarnings("unchecked")
        List<MetricLabel> labels = metric.getLabels();
        if (!labels.isEmpty()) {
            sb.append("{");
            List<String> labelStrs = labels.stream().map(l -> l.getKey() + "=\"" + l.getValue()
                    + "\"").collect(Collectors.toList());
            sb.append(Joiner.on(", ").join(labelStrs));
            sb.append("}");
        }

        // value
        sb.append(" ").append(metric.getValue().toString()).append("\n");
        return;
    }

    @Override
    public void visitHistogram(StringBuilder sb, String prefix, String name, Histogram histogram) {
        // The diff between cloud and branch-1.2-lts
        // branch-1.2-lts want to format the label, but it don't have another label except 'quantile'
        // so it is useless
        // cloud need to know metrics of each cluster, so add the label of cluster message.
        String fullName = prefix + name.replaceAll("\\.", "_");
        String clusterName = "UNKNOWN";
        String clusterId = "UNKNOWN";
        int idx = fullName.indexOf(MetricRepo.SELECTDB_TAG);
        if (!Config.cloud_unique_id.isEmpty() && idx != -1) {
            clusterName = fullName.substring(idx + MetricRepo.SELECTDB_TAG.length() + 1);
            fullName = fullName.substring(0, idx - 1);
            clusterId = Env.getCurrentSystemInfo().getCloudClusterNameToId().get(clusterName);
        }
        // final String fullTag = String.join(",", tags);
        sb.append(HELP).append(fullName).append(" ").append("\n");
        sb.append(TYPE).append(fullName).append(" ").append("summary\n");
        // String delimiter = tags.isEmpty() ? "" : ",";
        Snapshot snapshot = histogram.getSnapshot();
        // SELECTDB_CODE_BEGIN
        sb.append(fullName).append("{cluster_id=\"").append(clusterId).append("\", ")
                        .append("cluster_name=\"").append(clusterName).append("\", ")
                        .append("quantile=\"0.75\"} ").append(snapshot.get75thPercentile()).append("\n");
        sb.append(fullName).append("{cluster_id=\"").append(clusterId).append("\", ")
                        .append("cluster_name=\"").append(clusterName).append("\", ")
                        .append("quantile=\"0.95\"} ").append(snapshot.get95thPercentile()).append("\n");
        sb.append(fullName).append("{cluster_id=\"").append(clusterId).append("\", ")
                        .append("cluster_name=\"").append(clusterName).append("\", ")
                        .append("quantile=\"0.98\"} ").append(snapshot.get98thPercentile()).append("\n");
        sb.append(fullName).append("{cluster_id=\"").append(clusterId).append("\", ")
                        .append("cluster_name=\"").append(clusterName).append("\", ")
                        .append("quantile=\"0.99\"} ").append(snapshot.get99thPercentile()).append("\n");
        sb.append(fullName).append("{cluster_id=\"").append(clusterId).append("\", ")
                        .append("cluster_name=\"").append(clusterName).append("\", ")
                        .append("quantile=\"0.999\"} ").append(snapshot.get999thPercentile()).append("\n");
        sb.append(fullName).append("_sum").append("{cluster_id=\"").append(clusterId).append("\", ")
                        .append("cluster_name=\"").append(clusterName).append("\"} ")
                        .append(histogram.getCount() * snapshot.getMean()).append("\n");
        sb.append(fullName).append("_count").append("{cluster_id=\"").append(clusterId).append("\", ")
                        .append("cluster_name=\"").append(clusterName).append("\"} ")
                        .append(histogram.getCount()).append("\n");
        // SELECTDB_CODE_END
        return;
    }

    @Override
    public void getNodeInfo(StringBuilder sb) {
        final String NODE_INFO = "node_info";
        sb.append(Joiner.on(" ").join(TYPE, NODE_INFO, "gauge\n"));
        sb.append(NODE_INFO).append("{type=\"fe_node_num\", state=\"total\"} ")
                .append(Env.getCurrentEnv().getFrontends(null).size()).append("\n");
        sb.append(NODE_INFO).append("{type=\"be_node_num\", state=\"total\"} ")
                .append(Env.getCurrentSystemInfo().getBackendIds(false).size()).append("\n");
        sb.append(NODE_INFO).append("{type=\"be_node_num\", state=\"alive\"} ")
                .append(Env.getCurrentSystemInfo().getBackendIds(true).size()).append("\n");
        sb.append(NODE_INFO).append("{type=\"be_node_num\", state=\"decommissioned\"} ")
                .append(Env.getCurrentSystemInfo().getDecommissionedBackendIds().size()).append("\n");
        sb.append(NODE_INFO).append("{type=\"broker_node_num\", state=\"dead\"} ").append(
                Env.getCurrentEnv().getBrokerMgr().getAllBrokers()
                        .stream().filter(b -> !b.isAlive).count()).append("\n");

        // only master FE has this metrics, to help the Grafana knows who is the master
        if (Env.getCurrentEnv().isMaster()) {
            sb.append(NODE_INFO).append("{type=\"is_master\"} ").append(1).append("\n");
        }
        return;
    }
}
