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

package org.apache.fluss.server.coordinator.event.watcher;

import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.server.coordinator.event.DeadCoordinatorServerEvent;
import org.apache.fluss.server.coordinator.event.EventManager;
import org.apache.fluss.server.coordinator.event.NewCoordinatorServerEvent;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.fluss.shaded.curator5.org.apache.curator.utils.ZKPaths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A watcher to watch the coordinator server changes(new/delete) in zookeeper. */
public class CoordinatorServerChangeWatcher {

    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorServerChangeWatcher.class);
    private final CuratorCache curatorCache;

    private volatile boolean running;

    private final EventManager eventManager;

    public CoordinatorServerChangeWatcher(
            ZooKeeperClient zooKeeperClient, EventManager eventManager) {
        this.curatorCache =
                CuratorCache.build(
                        zooKeeperClient.getCuratorClient(), ZkData.CoordinatorIdsZNode.path());
        this.eventManager = eventManager;
        this.curatorCache.listenable().addListener(new CoordinatorServerChangeListener());
    }

    public void start() {
        running = true;
        curatorCache.start();
    }

    public void stop() {
        if (!running) {
            return;
        }
        running = false;
        LOG.info("Stopping CoordinatorServerChangeWatcher");
        curatorCache.close();
    }

    private final class CoordinatorServerChangeListener implements CuratorCacheListener {

        @Override
        public void event(Type type, ChildData oldData, ChildData newData) {
            if (newData != null) {
                LOG.debug("Received {} event (path: {})", type, newData.getPath());
            } else {
                LOG.debug("Received {} event", type);
            }

            switch (type) {
                case NODE_CREATED:
                    {
                        if (newData != null && newData.getData().length > 0) {
                            int serverId = getServerIdFromEvent(newData);
                            LOG.info("Received CHILD_ADDED event for server {}.", serverId);
                            eventManager.put(new NewCoordinatorServerEvent(serverId));
                        }
                        break;
                    }
                case NODE_DELETED:
                    {
                        if (oldData != null && oldData.getData().length > 0) {
                            int serverId = getServerIdFromEvent(oldData);
                            LOG.info("Received CHILD_REMOVED event for server {}.", serverId);
                            eventManager.put(new DeadCoordinatorServerEvent(serverId));
                        }
                        break;
                    }
                default:
                    break;
            }
        }
    }

    private int getServerIdFromEvent(ChildData data) {
        try {
            return Integer.parseInt(ZKPaths.getNodeFromPath(data.getPath()));
        } catch (NumberFormatException e) {
            throw new FlussRuntimeException(
                    "Invalid server id in zookeeper path: " + data.getPath(), e);
        }
    }
}
