/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.flink.metrics;

import com.alibaba.fluss.metrics.CharacterFilter;
import com.alibaba.fluss.metrics.Counter;
import com.alibaba.fluss.metrics.Gauge;
import com.alibaba.fluss.metrics.Histogram;
import com.alibaba.fluss.metrics.Meter;
import com.alibaba.fluss.metrics.Metric;
import com.alibaba.fluss.metrics.groups.AbstractMetricGroup;
import com.alibaba.fluss.metrics.registry.MetricRegistry;

import org.apache.flink.metrics.MetricGroup;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * An implementation of {@link MetricRegistry} which registers all metrics into Flink's metric
 * system. It's mainly used for Fluss client to register its metrics to Flink's metric system.
 *
 * <p>All metrics of the Fluss client are registered under group "fluss", which is a child group of
 * {@link org.apache.flink.metrics.groups.OperatorMetricGroup}.
 *
 * <p>For example, the metrics sendLatencyMs will be reported in metric:
 * "{some_parent_groups}.operator.fluss.client_writer.client_id.sendLatencyMs"
 */
public class FlinkMetricRegistry implements MetricRegistry {

    public static final String FLUSS_GROUP_NAME = "fluss";
    private static final Character FIELD_DELIMITER = '_';

    private final MetricGroup metricGroupForFluss;
    private final Map<String, Metric> metrics;
    private final Set<String> exposedMetricNames;

    public FlinkMetricRegistry(MetricGroup flinkOperatorMetricGroup) {
        this(flinkOperatorMetricGroup, Collections.emptySet());
    }

    public FlinkMetricRegistry(
            MetricGroup flinkOperatorMetricGroup, Set<String> exposedMetricNames) {
        this.metricGroupForFluss = flinkOperatorMetricGroup.addGroup(FLUSS_GROUP_NAME);
        this.metrics = new HashMap<>();
        this.exposedMetricNames = exposedMetricNames;
    }

    @Override
    public int getNumberReporters() {
        // only flink as the reporter
        return 1;
    }

    @Override
    public void register(Metric metric, String metricName, AbstractMetricGroup group) {
        // use the logical group name from Fluss's metrics group as the group name
        // of Flink's metric group the metrics will be registered in
        String logicalGroupName =
                group.getLogicalScope(CharacterFilter.NO_OP_FILTER, FIELD_DELIMITER);
        MetricGroup currentMetricGroup = metricGroupForFluss.addGroup(logicalGroupName);
        // we need to put all the variables of the Fluss's metrics group to Flink's metrics group
        for (Map.Entry<String, String> variablesEntry : getVariables(group).entrySet()) {
            currentMetricGroup =
                    currentMetricGroup.addGroup(variablesEntry.getKey(), variablesEntry.getValue());
        }

        // now, register to the Flink's metrics group
        registerMetric(currentMetricGroup, metric, metricName);
    }

    /** Exposes the metrics of Fluss metics group for flink. */
    public Metric getFlussMetric(String metricName) {
        return metrics.get(metricName);
    }

    /**
     * Get all the variables of the group. It'll get the variables of the group from parent to
     * children to keep the orders of variables.
     *
     * @return the orders map of the variables
     */
    private Map<String, String> getVariables(AbstractMetricGroup group) {
        if (group == null) {
            return Collections.emptyMap();
        }
        Map<String, String> variablesMap = new LinkedHashMap<>(getVariables(group.getParent()));
        for (Map.Entry<String, String> variablesEntry : group.getAllVariables().entrySet()) {
            // only if it hasn't contains the variable, put the variable to the map
            if (!variablesMap.containsKey(variablesEntry.getKey())) {
                variablesMap.put(variablesEntry.getKey(), variablesEntry.getValue());
            }
        }
        return variablesMap;
    }

    private void registerMetric(MetricGroup metricGroup, Metric metric, String metricName) {
        switch (metric.getMetricType()) {
            case COUNTER:
                metricGroup.counter(metricName, new FlinkCounter((Counter) metric));
                break;
            case METER:
                metricGroup.meter(metricName, new FlinkMeter((Meter) metric));
                break;
            case GAUGE:
                metricGroup.gauge(metricName, new FlinkGauge<>((Gauge<?>) metric));
                break;
            case HISTOGRAM:
                metricGroup.histogram(metricName, new FlinkHistogram((Histogram) metric));
                break;
        }

        if (exposedMetricNames.contains(metricName)) {
            metrics.put(metricName, metric);
        }
    }

    @Override
    public void unregister(Metric metric, String metricName, AbstractMetricGroup group) {
        // do nothing since the metric is actually registered into Flink's metric system,
        // it's fine to not unregister in here now since when Fluss's writer/scanner needs to
        // unregister metrics, it means the writer/scanner needs to be closed along with closing
        // operator
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        ((org.apache.flink.runtime.metrics.groups.AbstractMetricGroup<?>) metricGroupForFluss)
                .close();
        return CompletableFuture.completedFuture(null);
    }
}
