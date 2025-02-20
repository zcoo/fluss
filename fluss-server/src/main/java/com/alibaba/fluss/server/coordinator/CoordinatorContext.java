/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.server.coordinator;

import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.cluster.ServerNode;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableBucketReplica;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePartition;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.server.coordinator.statemachine.BucketState;
import com.alibaba.fluss.server.coordinator.statemachine.ReplicaState;
import com.alibaba.fluss.server.zk.data.LeaderAndIsr;
import com.alibaba.fluss.utils.types.Tuple2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** A context for {@link CoordinatorServer}, maintaining necessary objects in memory. */
@NotThreadSafe
public class CoordinatorContext {

    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorContext.class);

    public static final int INITIAL_COORDINATOR_EPOCH = 0;

    // for simplicity, we just use retry time, may consider make it a configurable value
    // and use combine retry times and retry delay
    public static final int DELETE_TRY_TIMES = 5;

    private int offlineBucketCount = 0;

    // a map from the tablet replica to the delete fail number,
    // once the delete fail number is greater than DELETE_TRY_TIMES, we consider it as
    // a success deletion.
    private final Map<TableBucketReplica, Integer> failDeleteNumbers = new HashMap<>();

    private final Map<Integer, ServerNode> liveTabletServers = new HashMap<>();

    // a map from the table bucket to the state of the bucket.
    private final Map<TableBucket, BucketState> bucketStates = new HashMap<>();
    // a map from the replica of the table bucket to the state of the bucket
    private final Map<TableBucketReplica, ReplicaState> replicaStates = new HashMap<>();

    // a map of table assignment, table_id -> <bucket, bucket_replicas>
    private final Map<Long, Map<Integer, List<Integer>>> tableAssignments = new HashMap<>();

    // a map of partition assignment, <table_id, partition_id> -> <bucket, bucket_replicas>
    private final Map<TablePartition, Map<Integer, List<Integer>>> partitionAssignments =
            new HashMap<>();
    // a map from partition_id -> partition_name
    private final Map<Long, String> partitionNameById = new HashMap<>();

    // a map from table_id to the table path
    private final Map<Long, TablePath> tablePathById = new HashMap<>();
    // TODO: will be used in the future metadata cache
    private final Map<Long, TableInfo> tableInfoById = new HashMap<>();

    private final Map<TableBucket, LeaderAndIsr> bucketLeaderAndIsr = new HashMap<>();
    private final Set<Long> tablesToBeDeleted = new HashSet<>();

    private final Set<TablePartition> partitionsToBeDeleted = new HashSet<>();

    /**
     * A mapping from tablet server to offline buckets. When the leader replica of a table bucket
     * become offline, we'll put the entry tablet_server_id -> table_bucket to this map so that we
     * won't elect the tablet server as the leader again in re-election. We'll remove the key
     * tablet_server_id after the tablet server become alive or dead.
     */
    private final Map<Integer, Set<TableBucket>> replicasOnOffline = new HashMap<>();

    private int coordinatorEpoch = INITIAL_COORDINATOR_EPOCH;

    public CoordinatorContext() {}

    public int getCoordinatorEpoch() {
        return coordinatorEpoch;
    }

    public Map<Integer, ServerNode> getLiveTabletServers() {
        return liveTabletServers;
    }

    @VisibleForTesting
    public void setLiveTabletServers(List<ServerNode> servers) {
        liveTabletServers.clear();
        servers.forEach(server -> liveTabletServers.put(server.id(), server));
    }

    public void addLiveTabletServer(ServerNode serverNode) {
        this.liveTabletServers.put(serverNode.id(), serverNode);
    }

    public void removeLiveTabletServer(int serverId) {
        this.liveTabletServers.remove(serverId);
    }

    public boolean isReplicaAndServerOnline(int serverId, TableBucket tableBucket) {
        return liveTabletServers.containsKey(serverId)
                && !replicasOnOffline
                        .getOrDefault(serverId, Collections.emptySet())
                        .contains(tableBucket);
    }

    public int getOfflineBucketCount() {
        return offlineBucketCount;
    }

    public void addOfflineBucketInServer(TableBucket tableBucket, int serverId) {
        Set<TableBucket> tableBuckets =
                replicasOnOffline.computeIfAbsent(serverId, (k) -> new HashSet<>());
        tableBuckets.add(tableBucket);
    }

    public void removeOfflineBucketInServer(int serverId) {
        replicasOnOffline.remove(serverId);
    }

    public Map<Long, TablePath> allTables() {
        return tablePathById;
    }

    public Set<TableBucket> allBuckets() {
        Set<TableBucket> allBuckets = new HashSet<>();
        for (Map.Entry<Long, Map<Integer, List<Integer>>> tableAssign :
                tableAssignments.entrySet()) {
            long tableId = tableAssign.getKey();
            tableAssign
                    .getValue()
                    .keySet()
                    .forEach((bucket) -> allBuckets.add(new TableBucket(tableId, bucket)));
        }
        for (Map.Entry<TablePartition, Map<Integer, List<Integer>>> partitionAssign :
                partitionAssignments.entrySet()) {
            TablePartition tablePartition = partitionAssign.getKey();
            partitionAssign
                    .getValue()
                    .keySet()
                    .forEach(
                            (bucket) ->
                                    allBuckets.add(
                                            new TableBucket(
                                                    tablePartition.getTableId(),
                                                    tablePartition.getPartitionId(),
                                                    bucket)));
        }
        return allBuckets;
    }

    public Set<TableBucketReplica> replicasOnTabletServer(int server) {
        Set<TableBucketReplica> replicasInServer = new HashSet<>();
        tableAssignments.forEach(
                // iterate all tables
                (tableId, assignments) ->
                        // iterate all buckets
                        assignments.forEach(
                                (bucket, replicas) -> {
                                    if (replicas.contains(server)) {
                                        replicasInServer.add(
                                                new TableBucketReplica(
                                                        new TableBucket(tableId, bucket), server));
                                    }
                                }));
        // Iterate over partitioned tables
        partitionAssignments.forEach(
                (tablePartition, assignments) ->
                        assignments.forEach(
                                (bucket, replicas) -> {
                                    if (replicas.contains(server)) {
                                        replicasInServer.add(
                                                new TableBucketReplica(
                                                        new TableBucket(
                                                                tablePartition.getTableId(),
                                                                tablePartition.getPartitionId(),
                                                                bucket),
                                                        server));
                                    }
                                }));
        return replicasInServer;
    }

    public void putTablePath(long tableId, TablePath tablePath) {
        this.tablePathById.put(tableId, tablePath);
    }

    public void putTableInfo(TableInfo tableInfo) {
        this.tableInfoById.put(tableInfo.getTableId(), tableInfo);
    }

    public void putPartition(long partitionId, String partitionName) {
        this.partitionNameById.put(partitionId, partitionName);
    }

    public TableInfo getTableInfoById(long tableId) {
        return this.tableInfoById.get(tableId);
    }

    public TablePath getTablePathById(long tableId) {
        return this.tablePathById.get(tableId);
    }

    public boolean containsTableId(long tableId) {
        return this.tablePathById.containsKey(tableId);
    }

    public boolean containsPartitionId(long partitionId) {
        return this.partitionNameById.containsKey(partitionId);
    }

    public @Nullable String getPartitionName(long partitionId) {
        return this.partitionNameById.get(partitionId);
    }

    public void updateBucketReplicaAssignment(
            TableBucket tableBucket, List<Integer> replicaAssignment) {
        Map<Integer, List<Integer>> assignments;
        if (tableBucket.getPartitionId() == null) {
            assignments =
                    tableAssignments.computeIfAbsent(
                            tableBucket.getTableId(), (k) -> new HashMap<>());
        } else {
            assignments =
                    partitionAssignments.computeIfAbsent(
                            new TablePartition(
                                    tableBucket.getTableId(), tableBucket.getPartitionId()),
                            (k) -> new HashMap<>());
        }
        assignments.put(tableBucket.getBucket(), replicaAssignment);
    }

    public List<Integer> getAssignment(TableBucket tableBucket) {
        Map<Integer, List<Integer>> assignments;
        if (tableBucket.getPartitionId() == null) {
            assignments = tableAssignments.get(tableBucket.getTableId());
        } else {
            assignments =
                    partitionAssignments.get(
                            new TablePartition(
                                    tableBucket.getTableId(), tableBucket.getPartitionId()));
        }
        if (assignments != null) {
            return assignments.getOrDefault(tableBucket.getBucket(), Collections.emptyList());
        } else {
            return Collections.emptyList();
        }
    }

    public Map<TableBucket, BucketState> getBucketStates() {
        return bucketStates;
    }

    public Set<TableBucketReplica> getBucketReplicas(Set<TableBucket> tableBuckets) {
        return tableBuckets.stream()
                .flatMap(
                        tableBucket ->
                                getAssignment(tableBucket).stream()
                                        .map(
                                                replica ->
                                                        new TableBucketReplica(
                                                                tableBucket, replica)))
                .collect(Collectors.toSet());
    }

    public Map<TableBucketReplica, ReplicaState> getReplicaStates() {
        return replicaStates;
    }

    public ReplicaState getReplicaState(TableBucketReplica replica) {
        return replicaStates.get(replica);
    }

    public void putReplicaStateIfNotExists(TableBucketReplica replica, ReplicaState state) {
        replicaStates.putIfAbsent(replica, state);
    }

    public void putReplicaState(TableBucketReplica replica, ReplicaState state) {
        replicaStates.put(replica, state);
    }

    public void removeReplicaState(TableBucketReplica replica) {
        replicaStates.remove(replica);
    }

    public Set<TableBucket> getAllBucketsForTable(long tableId) {
        Set<TableBucket> tableBuckets = new HashSet<>();
        tableAssignments
                .getOrDefault(tableId, Collections.emptyMap())
                .keySet()
                .forEach(bucket -> tableBuckets.add(new TableBucket(tableId, bucket)));
        return tableBuckets;
    }

    public Set<TableBucket> getAllBucketsForPartition(long tableId, long partitionId) {
        Set<TableBucket> tableBuckets = new HashSet<>();
        TablePartition tablePartition = new TablePartition(tableId, partitionId);
        partitionAssignments
                .getOrDefault(tablePartition, Collections.emptyMap())
                .keySet()
                .forEach(bucket -> tableBuckets.add(new TableBucket(tableId, partitionId, bucket)));
        return tableBuckets;
    }

    public Set<TableBucketReplica> getAllReplicasForTable(long tableId) {
        Set<TableBucketReplica> allReplicas = new HashSet<>();
        tableAssignments
                .getOrDefault(tableId, Collections.emptyMap())
                .forEach(
                        (bucket, replicas) -> {
                            TableBucket tableBucket = new TableBucket(tableId, bucket);
                            for (int replica : replicas) {
                                allReplicas.add(new TableBucketReplica(tableBucket, replica));
                            }
                        });
        return allReplicas;
    }

    public Set<TableBucketReplica> getAllReplicasForPartition(long tableId, long partitionId) {
        Set<TableBucketReplica> allReplicas = new HashSet<>();
        TablePartition tablePartition = new TablePartition(tableId, partitionId);
        partitionAssignments
                .getOrDefault(tablePartition, Collections.emptyMap())
                .forEach(
                        (bucket, replicas) -> {
                            TableBucket tableBucket = new TableBucket(tableId, partitionId, bucket);
                            for (int replica : replicas) {
                                allReplicas.add(new TableBucketReplica(tableBucket, replica));
                            }
                        });
        return allReplicas;
    }

    /**
     * Pick up the replicas that should retry delete and replicas that considered as success delete.
     *
     * @return A tuple of retry delete replicas and success delete replicas
     */
    public Tuple2<Set<TableBucketReplica>, Set<TableBucketReplica>>
            retryDeleteAndSuccessDeleteReplicas(Collection<TableBucketReplica> failDeleteReplicas) {
        Set<TableBucketReplica> retryDeleteReplicas = new HashSet<>();
        Set<TableBucketReplica> successDeleteReplicas = new HashSet<>();
        for (TableBucketReplica tableBucketReplica : failDeleteReplicas) {
            if (failDeleteNumbers.getOrDefault(tableBucketReplica, 0) >= DELETE_TRY_TIMES) {
                // if the current fail number is greater or equal than the threshold, we will
                // consider it as success delete
                LOG.warn(
                        "Delete replica {} failed, retry times is equal to the max retry times {},"
                                + " just mark it as a successful replica deletion directly.",
                        tableBucketReplica,
                        DELETE_TRY_TIMES);
                failDeleteNumbers.remove(tableBucketReplica);
                successDeleteReplicas.add(tableBucketReplica);
            } else {
                // increment the fail number
                failDeleteNumbers.merge(tableBucketReplica, 1, Integer::sum);
                LOG.warn(
                        "Delete replica {} failed, retry times = {}.",
                        tableBucketReplica,
                        failDeleteNumbers.get(tableBucketReplica));
                retryDeleteReplicas.add(tableBucketReplica);
            }
        }
        return Tuple2.of(retryDeleteReplicas, successDeleteReplicas);
    }

    /** Clear fail delete number for the given replicas. */
    public void clearFailDeleteNumbers(Collection<TableBucketReplica> replicas) {
        for (TableBucketReplica tableBucketReplica : replicas) {
            failDeleteNumbers.remove(tableBucketReplica);
        }
    }

    @VisibleForTesting
    protected Map<Integer, List<Integer>> getTableAssignment(long tableId) {
        return tableAssignments.getOrDefault(tableId, Collections.emptyMap());
    }

    @VisibleForTesting
    protected Map<Integer, List<Integer>> getPartitionAssignment(TablePartition tablePartition) {
        return partitionAssignments.getOrDefault(tablePartition, Collections.emptyMap());
    }

    public boolean isAnyReplicaInState(long tableId, ReplicaState replicaState) {
        return getAllReplicasForTable(tableId).stream()
                .anyMatch(replica -> getReplicaState(replica) == replicaState);
    }

    public boolean isAnyReplicaInState(TablePartition tablePartition, ReplicaState replicaState) {
        return getAllReplicasForPartition(
                        tablePartition.getTableId(), tablePartition.getPartitionId())
                .stream()
                .anyMatch(replica -> getReplicaState(replica) == replicaState);
    }

    public boolean areAllReplicasInState(long tableId, ReplicaState replicaState) {
        return getAllReplicasForTable(tableId).stream()
                .allMatch(replica -> getReplicaState(replica) == replicaState);
    }

    public boolean areAllReplicasInState(TablePartition tablePartition, ReplicaState replicaState) {
        return getAllReplicasForPartition(
                        tablePartition.getTableId(), tablePartition.getPartitionId())
                .stream()
                .allMatch(replica -> getReplicaState(replica) == replicaState);
    }

    public void removeBucketState(TableBucket tableBucket) {
        bucketStates.remove(tableBucket);
    }

    public Set<TableBucket> bucketsInStates(Set<BucketState> states) {
        return bucketStates.entrySet().stream()
                .filter(entry -> states.contains(entry.getValue()))
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
    }

    public void putBucketState(TableBucket tableBucket, BucketState targetState) {
        BucketState currentState = bucketStates.put(tableBucket, targetState);
        updateBucketStateMetrics(tableBucket, currentState, targetState);
    }

    private void updateBucketStateMetrics(
            TableBucket tableBucket, BucketState currentState, BucketState targetState) {
        if (!isToBeDeleted(tableBucket)) {
            if (currentState != BucketState.OfflineBucket
                    && targetState == BucketState.OfflineBucket) {
                offlineBucketCount += 1;
            } else if (currentState == BucketState.OfflineBucket
                    && targetState != BucketState.OfflineBucket) {
                offlineBucketCount -= 1;
            }
        }
    }

    private boolean isToBeDeleted(TableBucket tableBucket) {
        if (tableBucket.getPartitionId() == null) {
            return isTableQueuedForDeletion(tableBucket.getTableId());
        } else {
            return isPartitionQueuedForDeletion(
                    new TablePartition(tableBucket.getTableId(), tableBucket.getPartitionId()));
        }
    }

    public void putBucketStateIfNotExists(TableBucket tableBucket, BucketState targetState) {
        bucketStates.putIfAbsent(tableBucket, targetState);
    }

    public Map<TableBucket, LeaderAndIsr> bucketLeaderAndIsr() {
        return bucketLeaderAndIsr;
    }

    public void putBucketLeaderAndIsr(TableBucket tableBucket, LeaderAndIsr leaderAndIsr) {
        bucketLeaderAndIsr.put(tableBucket, leaderAndIsr);
    }

    public Optional<LeaderAndIsr> getBucketLeaderAndIsr(TableBucket tableBucket) {
        return Optional.ofNullable(bucketLeaderAndIsr.get(tableBucket));
    }

    public int getBucketLeaderEpoch(TableBucket tableBucket) {
        return getBucketLeaderAndIsr(tableBucket).map(LeaderAndIsr::leaderEpoch).orElse(-1);
    }

    public Set<TableBucket> getBucketsWithLeaderIn(int serverId) {
        Set<TableBucket> buckets = new HashSet<>();
        bucketLeaderAndIsr.forEach(
                (bucket, leaderAndIsr) -> {
                    if (leaderAndIsr.leader() == serverId) {
                        buckets.add(bucket);
                    }
                });
        return buckets;
    }

    public BucketState getBucketState(TableBucket tableBucket) {
        return bucketStates.get(tableBucket);
    }

    public Set<Long> getTablesToBeDeleted() {
        return tablesToBeDeleted;
    }

    public Set<TablePartition> getPartitionsToBeDeleted() {
        return partitionsToBeDeleted;
    }

    public boolean isTableQueuedForDeletion(long tableId) {
        return tablesToBeDeleted.contains(tableId);
    }

    public boolean isPartitionQueuedForDeletion(TablePartition tablePartition) {
        return partitionsToBeDeleted.contains(tablePartition);
    }

    public void queueTableDeletion(Set<Long> tables) {
        tablesToBeDeleted.addAll(tables);
    }

    public void queuePartitionDeletion(Set<TablePartition> tablePartitions) {
        partitionsToBeDeleted.addAll(tablePartitions);
    }

    public void removeTable(long tableId) {
        tablesToBeDeleted.remove(tableId);
        Map<Integer, List<Integer>> assignment = tableAssignments.remove(tableId);
        if (assignment != null) {
            // remove leadership info for each bucket from the context
            assignment
                    .keySet()
                    .forEach(bucket -> bucketLeaderAndIsr.remove(new TableBucket(tableId, bucket)));
        }
        tablePathById.remove(tableId);
        tableInfoById.remove(tableId);
    }

    public void removePartition(TablePartition tablePartition) {
        partitionsToBeDeleted.remove(tablePartition);
        Map<Integer, List<Integer>> assignment = partitionAssignments.remove(tablePartition);
        if (assignment != null) {
            // remove leadership info for each bucket from the context
            assignment
                    .keySet()
                    .forEach(
                            bucket ->
                                    bucketLeaderAndIsr.remove(
                                            new TableBucket(
                                                    tablePartition.getTableId(),
                                                    tablePartition.getPartitionId(),
                                                    bucket)));
        }
        partitionNameById.remove(tablePartition.getPartitionId());
    }

    private void clearTablesState() {
        tableAssignments.clear();
        partitionAssignments.clear();
        bucketLeaderAndIsr.clear();
        replicasOnOffline.clear();
        bucketStates.clear();
        replicaStates.clear();
        tablePathById.clear();
        tableInfoById.clear();
        partitionNameById.clear();
    }

    public void resetContext() {
        tablesToBeDeleted.clear();
        coordinatorEpoch = 0;
        clearTablesState();
        // clear the live tablet servers
        liveTabletServers.clear();
    }
}
