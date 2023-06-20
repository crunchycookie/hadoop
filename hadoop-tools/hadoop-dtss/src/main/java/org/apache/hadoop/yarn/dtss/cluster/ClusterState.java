package org.apache.hadoop.yarn.dtss.cluster;

/**
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

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.dtss.nm.MockNM;
import org.apache.hadoop.yarn.dtss.random.RandomGenerator;
import org.apache.hadoop.yarn.dtss.random.RandomSeeder;
import org.apache.hadoop.yarn.dtss.rm.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * This class records the current state of the cluster,
 * keeping track of the RM and the NMs registered to the cluster.
 */
@InterfaceAudience.Private
@Singleton
public final class ClusterState {
  private static final Logger LOG = Logger.getLogger(ClusterState.class.getName());

  private final Map<NodeId, MockNM> nodeManagers = new HashMap<>();
  private final RandomGenerator randomGenerator;
  private final MockRM rm;

  @Inject
  private ClusterState(
      final RandomSeeder randomSeeder,
      final Injector injector) {
    this.randomGenerator = randomSeeder.newRandomGenerator();
    this.rm = injector.getInstance(MockRM.class);
  }

  public MockNM getNM(final NodeId nodeId) {
    return nodeManagers.get(nodeId);
  }

  public MockNM getRandomNM() {
    final int nmIdx = randomGenerator.randomInt(nodeManagers.size());
    return getNMs().get(nmIdx);
  }

  public void addNM(final MockNM nm) {
    nodeManagers.put(nm.getNodeId(), nm);
  }

  public void removeNM(final MockNM nm) {
    nodeManagers.remove(nm.getNodeId());
  }

  public ImmutableList<MockNM> getNMs() {
    return ImmutableList.copyOf(nodeManagers.values());
  }

  /**
   * Triggers heartbeats from the NMs to the RM.
   */
  public void heartbeatAll() {
    for (final MockNM nm : getNMs()) {
      try {
        nm.nodeHeartbeat(true);
        ((AbstractYarnScheduler) rm.getRMContext().getScheduler()).update();
      } catch (final Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }

    rm.drainEvents();
  }
}
