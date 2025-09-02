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

package com.alibaba.fluss.server.coordinator.event;

import java.util.Objects;

/** An event for new coordinator server. */
public class NewCoordinatorServerEvent implements CoordinatorEvent {

    private final int serverId;

    public NewCoordinatorServerEvent(int serverId) {
        this.serverId = serverId;
    }

    public int getServerId() {
        return serverId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        NewCoordinatorServerEvent that = (NewCoordinatorServerEvent) o;
        return serverId == that.serverId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(serverId);
    }

    @Override
    public String toString() {
        return "NewCoordinatorServerEvent{" + "serverId=" + serverId + '}';
    }
}
