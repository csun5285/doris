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
import org.apache.doris.metric.Metric.MetricUnit;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimerTask;

/*
 * MetricCalculator will collect and calculate some certain metrics at a fix rate,
 * such QPS, and save the result for users to get.
 */
public class MetricCalculator extends TimerTask {
    private long lastTs = -1;
    private long lastQueryCounter = -1;
    private long lastRequestCounter = -1;
    private long lastQueryErrCounter = -1;

    private Map<String, Long> cloudClusterLastQueryCounter = new HashMap<>();
    private Map<String, Long> cloudClusterLastRequestCounter = new HashMap<>();
    private Map<String, Long> cloudClusterLastQueryErrCounter = new HashMap<>();

    @Override
    public void run() {
        update();
    }

    private void updateClusterMetrics(long interval) {
        MetricRepo.CLOUD_CLUSTER_COUNTER_QUERY_ALL.forEach((clusterName, metric) -> {
            long clusterLastQueryCounter = cloudClusterLastQueryCounter.computeIfAbsent(clusterName, key -> {
                return metric.getValue();
            });
            long clusterCurrentQueryCounter = metric.getValue();
            double clusterQps = (double) (clusterCurrentQueryCounter - clusterLastQueryCounter) / interval;
            MetricRepo.CLOUD_CLUSTER_GAUGE_QUERY_PER_SECOND.computeIfAbsent(clusterName, key -> {
                String clusterId = Env.getCurrentSystemInfo().getCloudClusterNameToId().get(clusterName);
                GaugeMetricImpl<Double> gaugeQueryPerSecond = new GaugeMetricImpl<>("qps", MetricUnit.NOUNIT,
                        "query per second");
                gaugeQueryPerSecond.addLabel(new MetricLabel("cluster_id", clusterId));
                gaugeQueryPerSecond.addLabel(new MetricLabel("cluster_name", clusterName));
                gaugeQueryPerSecond.setValue(0.0);
                MetricRepo.DORIS_METRIC_REGISTER.addMetrics(gaugeQueryPerSecond);
                return gaugeQueryPerSecond;
            }).setValue(clusterQps < 0 ? 0.0 : clusterQps);
            cloudClusterLastQueryCounter.replace(clusterName, clusterCurrentQueryCounter);
        });

        MetricRepo.CLOUD_CLUSTER_COUNTER_REQUEST_ALL.forEach((clusterName, metric) -> {
            long clusterLastRequestCounter = cloudClusterLastRequestCounter.computeIfAbsent(clusterName, key -> {
                return metric.getValue();
            });
            long clusterCurrentRequestCounter = metric.getValue();
            double clusterRps = (double) (clusterCurrentRequestCounter - clusterLastRequestCounter) / interval;
            MetricRepo.CLOUD_CLUSTER_GAUGE_REQUEST_PER_SECOND.computeIfAbsent(clusterName, key -> {
                String clusterId = Env.getCurrentSystemInfo().getCloudClusterNameToId().get(clusterName);
                GaugeMetricImpl<Double> gaugeRequestPerSecond = new GaugeMetricImpl<>("rps", MetricUnit.NOUNIT,
                        "request per second");
                gaugeRequestPerSecond.addLabel(new MetricLabel("cluster_id", clusterId));
                gaugeRequestPerSecond.addLabel(new MetricLabel("cluster_name", clusterName));
                gaugeRequestPerSecond.setValue(0.0);
                MetricRepo.DORIS_METRIC_REGISTER.addMetrics(gaugeRequestPerSecond);
                return gaugeRequestPerSecond;
            }).setValue(clusterRps < 0 ? 0.0 : clusterRps);
            cloudClusterLastRequestCounter.replace(clusterName, clusterCurrentRequestCounter);
        });

        MetricRepo.CLOUD_CLUSTER_COUNTER_QUERY_ERR.forEach((clusterName, metric) -> {
            long clusterLastQueryErrCounter = cloudClusterLastQueryErrCounter.computeIfAbsent(clusterName, key -> {
                return metric.getValue();
            });
            long clusterCurrentQueryErrCounter = metric.getValue();
            double clusterErrRate = (double) (clusterCurrentQueryErrCounter - clusterLastQueryErrCounter) / interval;
            MetricRepo.CLOUD_CLUSTER_GAUGE_QUERY_ERR_RATE.computeIfAbsent(clusterName, key -> {
                String clusterId = Env.getCurrentSystemInfo().getCloudClusterNameToId().get(clusterName);
                GaugeMetricImpl<Double> gaugeQueryErrRate = new GaugeMetricImpl<>("query_err_rate",
                        MetricUnit.NOUNIT, "query error rate");
                gaugeQueryErrRate.addLabel(new MetricLabel("cluster_id", clusterId));
                gaugeQueryErrRate.addLabel(new MetricLabel("cluster_name", clusterName));
                gaugeQueryErrRate.setValue(0.0);
                MetricRepo.DORIS_METRIC_REGISTER.addMetrics(gaugeQueryErrRate);
                return gaugeQueryErrRate;
            }).setValue(clusterErrRate < 0 ? 0.0 : clusterErrRate);
            cloudClusterLastQueryErrCounter.replace(clusterName, clusterCurrentQueryErrCounter);
        });
    }

    private void update() {
        long currentTs = System.currentTimeMillis();
        if (lastTs == -1) {
            lastTs = currentTs;
            lastQueryCounter = MetricRepo.COUNTER_QUERY_ALL.getValue();
            lastRequestCounter = MetricRepo.COUNTER_REQUEST_ALL.getValue();
            lastQueryErrCounter = MetricRepo.COUNTER_QUERY_ERR.getValue();
            if (Config.isCloudMode()) {
                MetricRepo.CLOUD_CLUSTER_COUNTER_QUERY_ALL.forEach((clusterName, metric) -> {
                    cloudClusterLastQueryCounter.put(clusterName, metric.getValue());
                });
                MetricRepo.CLOUD_CLUSTER_COUNTER_REQUEST_ALL.forEach((clusterName, metric) -> {
                    cloudClusterLastRequestCounter.put(clusterName, metric.getValue());
                });
                MetricRepo.CLOUD_CLUSTER_COUNTER_QUERY_ERR.forEach((clusterName, metric) -> {
                    cloudClusterLastQueryErrCounter.put(clusterName, metric.getValue());
                });
            }
            return;
        }

        long interval = (currentTs - lastTs) / 1000 + 1;

        // qps
        long currentQueryCounter = MetricRepo.COUNTER_QUERY_ALL.getValue();
        double qps = (double) (currentQueryCounter - lastQueryCounter) / interval;
        MetricRepo.GAUGE_QUERY_PER_SECOND.setValue(qps < 0 ? 0.0 : qps);
        lastQueryCounter = currentQueryCounter;

        // rps
        long currentRequestCounter = MetricRepo.COUNTER_REQUEST_ALL.getValue();
        double rps = (double) (currentRequestCounter - lastRequestCounter) / interval;
        MetricRepo.GAUGE_REQUEST_PER_SECOND.setValue(rps < 0 ? 0.0 : rps);
        lastRequestCounter = currentRequestCounter;

        // err rate
        long currentErrCounter = MetricRepo.COUNTER_QUERY_ERR.getValue();
        double errRate = (double) (currentErrCounter - lastQueryErrCounter) / interval;
        MetricRepo.GAUGE_QUERY_ERR_RATE.setValue(errRate < 0 ? 0.0 : errRate);
        lastQueryErrCounter = currentErrCounter;

        if (Config.isCloudMode()) {
            updateClusterMetrics(interval);
        }

        lastTs = currentTs;

        // max tablet compaction score of all backends
        long maxCompactionScore = 0;
        List<Metric> compactionScoreMetrics = MetricRepo.getMetricsByName(MetricRepo.TABLET_MAX_COMPACTION_SCORE);
        for (Metric metric : compactionScoreMetrics) {
            if (((GaugeMetric<Long>) metric).getValue() > maxCompactionScore) {
                maxCompactionScore = ((GaugeMetric<Long>) metric).getValue();
            }
        }
        MetricRepo.GAUGE_MAX_TABLET_COMPACTION_SCORE.setValue(maxCompactionScore);
    }
}
