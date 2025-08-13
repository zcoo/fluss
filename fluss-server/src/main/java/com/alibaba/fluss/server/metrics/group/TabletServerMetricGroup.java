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

package com.alibaba.fluss.server.metrics.group;

import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.metrics.CharacterFilter;
import com.alibaba.fluss.metrics.Counter;
import com.alibaba.fluss.metrics.MeterView;
import com.alibaba.fluss.metrics.MetricNames;
import com.alibaba.fluss.metrics.ThreadSafeSimpleCounter;
import com.alibaba.fluss.metrics.groups.AbstractMetricGroup;
import com.alibaba.fluss.metrics.registry.MetricRegistry;
import com.alibaba.fluss.utils.MapUtils;

import java.util.Map;

/** The metric group for tablet server. */
public class TabletServerMetricGroup extends AbstractMetricGroup {

    private static final String NAME = "tabletserver";

    private final Map<TablePath, TableMetricGroup> metricGroupByTable =
            MapUtils.newConcurrentHashMap();

    protected final String clusterId;
    protected final String rack;
    protected final String hostname;
    protected final int serverId;

    // ---- metrics ----
    private final Counter replicationBytesIn;
    private final Counter replicationBytesOut;
    private final Counter delayedWriteExpireCount;
    private final Counter delayedFetchFromFollowerExpireCount;
    private final Counter delayedFetchFromClientExpireCount;

    // aggregation metrics
    private final Counter messagesIn;
    private final Counter bytesIn;
    private final Counter bytesOut;

    public TabletServerMetricGroup(
            MetricRegistry registry, String clusterId, String rack, String hostname, int serverId) {
        super(registry, new String[] {clusterId, hostname, NAME}, null);
        this.clusterId = clusterId;
        this.rack = rack;
        this.hostname = hostname;
        this.serverId = serverId;

        replicationBytesIn = new ThreadSafeSimpleCounter();
        meter(MetricNames.REPLICATION_IN_RATE, new MeterView(replicationBytesIn));
        replicationBytesOut = new ThreadSafeSimpleCounter();
        meter(MetricNames.REPLICATION_OUT_RATE, new MeterView(replicationBytesOut));

        delayedWriteExpireCount = new ThreadSafeSimpleCounter();
        meter(MetricNames.DELAYED_WRITE_EXPIRES_RATE, new MeterView(delayedWriteExpireCount));
        delayedFetchFromFollowerExpireCount = new ThreadSafeSimpleCounter();
        meter(
                MetricNames.DELAYED_FETCH_FROM_FOLLOWER_EXPIRES_RATE,
                new MeterView(delayedFetchFromFollowerExpireCount));
        delayedFetchFromClientExpireCount = new ThreadSafeSimpleCounter();
        meter(
                MetricNames.DELAYED_FETCH_FROM_CLIENT_EXPIRES_RATE,
                new MeterView(delayedFetchFromClientExpireCount));

        messagesIn = new ThreadSafeSimpleCounter();
        meter(MetricNames.MESSAGES_IN_RATE, new MeterView(messagesIn));
        bytesIn = new ThreadSafeSimpleCounter();
        meter(MetricNames.BYTES_IN_RATE, new MeterView(bytesIn));
        bytesOut = new ThreadSafeSimpleCounter();
        meter(MetricNames.BYTES_OUT_RATE, new MeterView(bytesOut));
    }

    @Override
    protected final void putVariables(Map<String, String> variables) {
        variables.put("cluster_id", clusterId);
        if (rack != null) {
            variables.put("rack", rack);
        } else {
            // The value of an empty string indicates no rack
            variables.put("rack", "");
        }
        variables.put("host", hostname);
        variables.put("server_id", String.valueOf(serverId));
    }

    @Override
    protected String getGroupName(CharacterFilter filter) {
        return NAME;
    }

    public Counter replicationBytesIn() {
        return replicationBytesIn;
    }

    public Counter replicationBytesOut() {
        return replicationBytesOut;
    }

    public Counter delayedWriteExpireCount() {
        return delayedWriteExpireCount;
    }

    public Counter delayedFetchFromFollowerExpireCount() {
        return delayedFetchFromFollowerExpireCount;
    }

    public Counter delayedFetchFromClientExpireCount() {
        return delayedFetchFromClientExpireCount;
    }

    public Counter messageIn() {
        return messagesIn;
    }

    public Counter bytesIn() {
        return bytesIn;
    }

    public Counter bytesOut() {
        return bytesOut;
    }

    // ------------------------------------------------------------------------
    //  table buckets groups
    // ------------------------------------------------------------------------
    public BucketMetricGroup addTableBucketMetricGroup(
            TablePath tablePath, int bucket, boolean isKvTable) {
        TableMetricGroup tableMetricGroup =
                metricGroupByTable.computeIfAbsent(
                        tablePath,
                        table -> new TableMetricGroup(registry, tablePath, isKvTable, this));
        return tableMetricGroup.addBucketMetricGroup(bucket);
    }

    public void removeTableBucketMetricGroup(TablePath tablePath, int bucket) {
        // get the metric group of the table
        TableMetricGroup tableMetricGroup = metricGroupByTable.get(tablePath);
        // if get the table metric group
        if (tableMetricGroup != null) {
            // remove the bucket metric group
            tableMetricGroup.removeBucketMetricGroup(bucket);
            // if no any bucket groups remain in the physical table metrics group,
            // close and remove the physical table metric group
            if (tableMetricGroup.bucketGroupsCount() == 0) {
                tableMetricGroup.close();
                metricGroupByTable.remove(tablePath);
            }
        }
    }
}
