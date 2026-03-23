/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.coordinator;

import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.NotCoordinatorLeaderException;
import org.apache.fluss.rpc.GatewayClientProxy;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.messages.CreateDatabaseRequest;
import org.apache.fluss.rpc.metrics.TestingClientMetricGroup;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.CoordinatorAddress;
import org.apache.fluss.testutils.common.AllCallbackWrapper;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration test for Coordinator Server High Availability.
 *
 * <p>This test verifies that when multiple coordinator servers are started, only the leader
 * processes RPC requests while standby servers return {@link NotCoordinatorLeaderException}.
 */
class CoordinatorHAITCase {

    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION_WRAPPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private static ZooKeeperClient zookeeperClient;

    private CoordinatorServer coordinatorServer1;
    private CoordinatorServer coordinatorServer2;
    private RpcClient rpcClient;

    @BeforeAll
    static void baseBeforeAll() {
        zookeeperClient =
                ZOO_KEEPER_EXTENSION_WRAPPER
                        .getCustomExtension()
                        .getZooKeeperClient(NOPErrorHandler.INSTANCE);
    }

    @BeforeEach
    void setUp() throws Exception {
        Configuration clientConf = new Configuration();
        rpcClient = RpcClient.create(clientConf, TestingClientMetricGroup.newInstance(), false);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (coordinatorServer1 != null) {
            coordinatorServer1.close();
        }
        if (coordinatorServer2 != null) {
            coordinatorServer2.close();
        }
        if (rpcClient != null) {
            rpcClient.close();
        }
    }

    @Test
    void testLeaderAndStandbyRpcBehavior() throws Exception {
        coordinatorServer1 = new CoordinatorServer(createConfiguration());
        coordinatorServer2 = new CoordinatorServer(createConfiguration());

        List<CoordinatorServer> coordinatorServerList =
                Arrays.asList(coordinatorServer1, coordinatorServer2);

        // start 2 coordinator servers
        for (CoordinatorServer server : coordinatorServerList) {
            server.start();
        }

        // wait until one coordinator becomes leader
        waitUntilCoordinatorServerElected();

        CoordinatorAddress leaderAddress = zookeeperClient.getCoordinatorLeaderAddress().get();

        // find the leader and standby servers
        CoordinatorServer leaderServer = null;
        CoordinatorServer standbyServer = null;
        for (CoordinatorServer coordinatorServer : coordinatorServerList) {
            if (Objects.equals(coordinatorServer.getServerId(), leaderAddress.getId())) {
                leaderServer = coordinatorServer;
            } else {
                standbyServer = coordinatorServer;
            }
        }

        assertThat(leaderServer).isNotNull();
        assertThat(standbyServer).isNotNull();

        // create gateways for both servers
        CoordinatorGateway leaderGateway = createGatewayForServer(leaderServer);
        CoordinatorGateway standbyGateway = createGatewayForServer(standbyServer);

        // test: leader should process request normally (no exception means success)
        String testDbName = "test_ha_db_" + System.currentTimeMillis();
        CreateDatabaseRequest createDbRequest =
                new CreateDatabaseRequest().setDatabaseName(testDbName).setIgnoreIfExists(false);

        // leader should process request successfully without throwing exception
        leaderGateway.createDatabase(createDbRequest).get();

        // test: standby should throw NotCoordinatorLeaderException
        String testDbName2 = "test_ha_db2_" + System.currentTimeMillis();
        CreateDatabaseRequest createDbRequest2 =
                new CreateDatabaseRequest().setDatabaseName(testDbName2).setIgnoreIfExists(false);

        assertThatThrownBy(() -> standbyGateway.createDatabase(createDbRequest2).get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(NotCoordinatorLeaderException.class)
                .hasMessageContaining("not the current leader");
    }

    private CoordinatorGateway createGatewayForServer(CoordinatorServer server) {
        List<Endpoint> endpoints = server.getRpcServer().getBindEndpoints();
        Endpoint endpoint = endpoints.get(0);
        // Use server.hashCode() as id since serverId is a UUID string
        ServerNode serverNode =
                new ServerNode(
                        server.hashCode(),
                        endpoint.getHost(),
                        endpoint.getPort(),
                        ServerType.COORDINATOR);

        return GatewayClientProxy.createGatewayProxy(
                () -> serverNode, rpcClient, CoordinatorGateway.class);
    }

    private static Configuration createConfiguration() {
        Configuration configuration = new Configuration();
        configuration.setString(
                ConfigOptions.ZOOKEEPER_ADDRESS,
                ZOO_KEEPER_EXTENSION_WRAPPER.getCustomExtension().getConnectString());
        configuration.setString(
                ConfigOptions.BIND_LISTENERS, "CLIENT://localhost:0,FLUSS://localhost:0");
        configuration.setString(ConfigOptions.ADVERTISED_LISTENERS, "CLIENT://198.168.0.1:100");
        configuration.set(ConfigOptions.REMOTE_DATA_DIR, "/tmp/fluss/remote-data");

        return configuration;
    }

    private void waitUntilCoordinatorServerElected() {
        waitUntil(
                () -> zookeeperClient.getCoordinatorLeaderAddress().isPresent(),
                Duration.ofMinutes(1),
                "Fail to wait coordinator server elected");
    }
}
