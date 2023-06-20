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

package org.apache.hadoop.yarn.dtss.rm;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.commons.lang.NullArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.*;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.dtss.am.MockAM;
import org.apache.hadoop.yarn.dtss.metrics.MetricsManager;
import org.apache.hadoop.yarn.dtss.metrics.SchedulerWrapper;
import org.apache.hadoop.yarn.dtss.nm.MockNM;
import org.apache.hadoop.yarn.dtss.time.Clock;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.resourcemanager.*;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.AMLauncherEvent;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.ApplicationMasterLauncher;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.MemoryRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.NullRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.*;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.*;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.*;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.security.ClientToAMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.NMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.util.YarnVersionInfo;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.*;
import java.util.logging.Level;

/**
 * NOTE: Based on classes that support MockRM in the tests package
 * for resourcemanager.
 */
@SuppressWarnings("unchecked")
@Singleton
public class MockRM extends ResourceManager {
  private static final java.util.logging.Logger JAVA_LOGGER =
      java.util.logging.Logger.getLogger(MockRM.class.getName());
  static final Logger LOG = Logger.getLogger(MockRM.class);
  static final String ENABLE_WEBAPP = "mockrm.webapp.enabled";
  private static final int SECOND = 1000;
  private static final int TIMEOUT_MS_FOR_ATTEMPT = 40 * SECOND;
  private static final int TIMEOUT_MS_FOR_APP_REMOVED = 40 * SECOND;
  private static final int TIMEOUT_MS_FOR_CONTAINER_AND_NODE = 20 * SECOND;
  private static final int WAIT_MS_PER_LOOP = 10;

  private final boolean useNullRMNodeLabelsManager;

  private boolean disableDrainEventsImplicitly;
  private boolean serviceInitialized = false;

  private boolean useRealElector = false;
  private Boolean shouldSubmitToNamedQueues = null;

  private Set<String> configuredQueues;
  private MetricsManager metricsManager;

  @Inject
  public MockRM(
      final Configuration conf,
      final Clock clock,
      final MetricsManager metricsManager) {
    super();
    this.metricsManager = metricsManager;
    this.useNullRMNodeLabelsManager = true;
    this.useRealElector = false;
    this.clock = clock;
    init(conf instanceof YarnConfiguration ? conf : new YarnConfiguration(conf));
    Class storeClass = getRMContext().getStateStore().getClass();
    if (storeClass.equals(MemoryRMStateStore.class)) {
      MockMemoryRMStateStore mockStateStore = new MockMemoryRMStateStore();
      mockStateStore.init(conf);
      setRMStateStore(mockStateStore);
    } else if (storeClass.equals(NullRMStateStore.class)) {
      MockRMNullStateStore mockStateStore = new MockRMNullStateStore();
      mockStateStore.init(conf);
      setRMStateStore(mockStateStore);
    }
    disableDrainEventsImplicitly = false;
  }

  public class MockRMNullStateStore extends NullRMStateStore {
    @SuppressWarnings("rawtypes")
    @Override
    protected EventHandler getRMStateStoreEventHandler() {
      return rmStateStoreEventHandler;
    }
  }

  @Override
  protected ResourceScheduler createScheduler() {
    final ResourceScheduler sched = super.createScheduler();
    ((SchedulerWrapper)sched).setMetricsManager(metricsManager);
    ((AbstractYarnScheduler)sched).setClock(clock);
    return sched;
  }

  @Nullable
  public Set<String> getConfiguredQueues() {
    if (!serviceInitialized) {
      return null;
    }

    if (configuredQueues == null) {
      if (scheduler instanceof CapacityScheduler) {
        final CapacitySchedulerConfiguration conf = ((CapacityScheduler) scheduler).getConfiguration();
        configuredQueues = new HashSet<>(Arrays.asList(conf.getQueues("root")));
        JAVA_LOGGER.log(Level.INFO, "Got configured queues " + configuredQueues);
      } else {
        JAVA_LOGGER.log(Level.INFO, "Configured queues not supported!");
        configuredQueues = new HashSet<>();
      }
    }

    return configuredQueues;
  }

  @Nullable
  public Boolean isOnlyDefaultQueue() {
    final Set<String> configuredQueues = getConfiguredQueues();
    if (configuredQueues == null) {
      return null;
    }

    return getConfiguredQueues().isEmpty() ||
        (configuredQueues.size() == 1 && configuredQueues.contains("default"));
  }

  @Nullable
  public Boolean getShouldSubmitToNamedQueues() {
    if (!serviceInitialized || getConfiguredQueues() == null) {
      return null;
    }

    if (shouldSubmitToNamedQueues == null) {
      final Boolean isOnlyDefaultQueue = isOnlyDefaultQueue();
      shouldSubmitToNamedQueues = isOnlyDefaultQueue == null ? null : !isOnlyDefaultQueue;
    }

    return shouldSubmitToNamedQueues;
  }

  // Get cluster metrics at the end of the run
  public MetricsManager getMetricsManager() {
    return ((SchedulerWrapper)scheduler).getMetricsManager();
  }

  @Override
  protected RMNodeLabelsManager createNodeLabelManager()
      throws InstantiationException, IllegalAccessException {
    if (useNullRMNodeLabelsManager) {
      RMNodeLabelsManager mgr = new NullRMNodeLabelsManager();
      mgr.init(getConfig());
      return mgr;
    } else {
      return super.createNodeLabelManager();
    }
  }

  @Override
  protected Dispatcher createDispatcher() {
    return new DrainDispatcher();
  }

  @Override
  protected EmbeddedElector createEmbeddedElector() throws IOException {
    if (useRealElector) {
      return super.createEmbeddedElector();
    } else {
      return null;
    }
  }

  private void handleSchedulerEvent(final SchedulerEvent schedulerEvent) {
    scheduler.handle(schedulerEvent);
  }

  @Override
  protected EventHandler<SchedulerEvent> createSchedulerEventDispatcher() {
    return this::handleSchedulerEvent;
  }

  public void drainEvents() {
    Dispatcher rmDispatcher = rmContext.getDispatcher();
    if (rmDispatcher instanceof DrainDispatcher) {
      ((DrainDispatcher) rmDispatcher).await();
    } else {
      throw new UnsupportedOperationException("Not a Drain Dispatcher!");
    }
  }

  private boolean waitForState(ApplicationId appId, EnumSet<RMAppState> finalStates)
      throws InterruptedException {
    drainEventsImplicitly();
    RMApp app = getRMContext().getRMApps().get(appId);
    final int timeoutMsecs = 80 * SECOND;
    int timeWaiting = 0;
    while (!finalStates.contains(app.getState())) {
      if (timeWaiting >= timeoutMsecs) {
        LOG.info("App State is : " + app.getState());
        return false;
      }

      LOG.info("App : " + appId + " State is : " + app.getState() +
          " Waiting for state : " + finalStates);
      Thread.sleep(WAIT_MS_PER_LOOP);
      timeWaiting += WAIT_MS_PER_LOOP;
    }

    LOG.info("App State is : " + app.getState());
    return true;
  }

  /**
   * Wait until an application has reached a specified state.
   * The timeout is 80 seconds.
   * @param appId the id of an application
   * @param finalState the application state waited
   * @throws InterruptedException
   *         if interrupted while waiting for the state transition
   */
  public boolean waitForState(ApplicationId appId, RMAppState finalState)
      throws InterruptedException {
    drainEventsImplicitly();
    RMApp app = getRMContext().getRMApps().get(appId);
    final int timeoutMsecs = 80 * SECOND;
    int timeWaiting = 0;
    while (!finalState.equals(app.getState())) {
      if (timeWaiting >= timeoutMsecs) {
        LOG.info("App State is : " + app.getState());
        return false;
      }

      LOG.info("App : " + appId + " State is : " + app.getState() +
          " Waiting for state : " + finalState);
      Thread.sleep(WAIT_MS_PER_LOOP);
      timeWaiting += WAIT_MS_PER_LOOP;
    }

    LOG.info("App State is : " + app.getState());
    return true;
  }

  /**
   * Wait until an attempt has reached a specified state.
   * The timeout is 40 seconds.
   * @param attemptId the id of an attempt
   * @param finalState the attempt state waited
   * @throws InterruptedException
   *         if interrupted while waiting for the state transition
   */
  public boolean waitForState(ApplicationAttemptId attemptId,
                              RMAppAttemptState finalState) throws InterruptedException {
    return waitForState(attemptId, finalState, TIMEOUT_MS_FOR_ATTEMPT);
  }

  /**
   * Wait until an attempt has reached a specified state.
   * The timeout can be specified by the parameter.
   * @param attemptId the id of an attempt
   * @param finalState the attempt state waited
   * @param timeoutMsecs the length of timeout in milliseconds
   * @throws InterruptedException
   *         if interrupted while waiting for the state transition
   */
  public boolean waitForState(ApplicationAttemptId attemptId,
                           RMAppAttemptState finalState, int timeoutMsecs)
      throws InterruptedException {
    drainEventsImplicitly();
    RMApp app = getRMContext().getRMApps().get(attemptId.getApplicationId());
    RMAppAttempt attempt = app.getRMAppAttempt(attemptId);
    return MockRM.waitForState(attempt, finalState, timeoutMsecs);
  }

  /**
   * Wait until an attempt has reached a specified state.
   * The timeout is 40 seconds.
   * @param attempt an attempt
   * @param finalState the attempt state waited
   * @throws InterruptedException
   *         if interrupted while waiting for the state transition
   */
  public static boolean waitForState(RMAppAttempt attempt,
                                  RMAppAttemptState finalState) throws InterruptedException {
    return waitForState(attempt, finalState, TIMEOUT_MS_FOR_ATTEMPT);
  }

  /**
   * Wait until an attempt has reached a specified state.
   * The timeout can be specified by the parameter.
   * @param attempt an attempt
   * @param finalState the attempt state waited
   * @param timeoutMsecs the length of timeout in milliseconds
   * @throws InterruptedException
   *         if interrupted while waiting for the state transition
   */
  public static boolean waitForState(RMAppAttempt attempt,
                                     RMAppAttemptState finalState,
                                     int timeoutMsecs)
      throws InterruptedException {
    int timeWaiting = 0;
    while (finalState != attempt.getAppAttemptState()) {
      if (timeWaiting >= timeoutMsecs) {
        LOG.info("Attempt State is : " + attempt.getAppAttemptState());
        return false;
      }

      LOG.info("AppAttempt : " + attempt.getAppAttemptId() + " State is : " +
          attempt.getAppAttemptState() + " Waiting for state : " + finalState);
      Thread.sleep(WAIT_MS_PER_LOOP);
      timeWaiting += WAIT_MS_PER_LOOP;
    }

    LOG.info("Attempt State is : " + attempt.getAppAttemptState());
    return true;
  }

  public void waitForContainerToComplete(RMAppAttempt attempt,
                                         NMContainerStatus completedContainer) throws InterruptedException {
    drainEventsImplicitly();
    int timeWaiting = 0;
    while (timeWaiting < TIMEOUT_MS_FOR_CONTAINER_AND_NODE) {
      List<ContainerStatus> containers = attempt.getJustFinishedContainers();
      LOG.info("Received completed containers " + containers);
      for (ContainerStatus container : containers) {
        if (container.getContainerId().equals(
            completedContainer.getContainerId())) {
          return;
        }
      }
      Thread.sleep(WAIT_MS_PER_LOOP);
      timeWaiting += WAIT_MS_PER_LOOP;
    }
  }

  public MockAM waitForNewAMToLaunchAndRegister(ApplicationId appId, int attemptSize,
                                                MockNM nm) throws Exception {
    RMApp app = getRMContext().getRMApps().get(appId);
    int timeWaiting = 0;
    while (app.getAppAttempts().size() != attemptSize) {
      if (timeWaiting >= TIMEOUT_MS_FOR_ATTEMPT) {
        break;
      }
      LOG.info("Application " + appId
          + " is waiting for AM to restart. Current has "
          + app.getAppAttempts().size() + " attempts.");
      Thread.sleep(WAIT_MS_PER_LOOP);
      timeWaiting += WAIT_MS_PER_LOOP;
    }
    return launchAndRegisterAM(app, this, nm);
  }

  /**
   * Wait until a container has reached a specified state.
   * The timeout is 10 seconds.
   * @param nm A mock nodemanager
   * @param containerId the id of a container
   * @param containerState the container state waited
   * @return if reach the state before timeout; false otherwise.
   * @throws Exception
   *         if interrupted while waiting for the state transition
   *         or an unexpected error while MockNM is hearbeating.
   */
  public boolean waitForState(MockNM nm, ContainerId containerId,
                              RMContainerState containerState) throws Exception {
    return waitForState(nm, containerId, containerState,
        TIMEOUT_MS_FOR_CONTAINER_AND_NODE);
  }

  /**
   * Wait until a container has reached a specified state.
   * The timeout is specified by the parameter.
   * @param nm A mock nodemanager
   * @param containerId the id of a container
   * @param containerState the container state waited
   * @param timeoutMsecs the length of timeout in milliseconds
   * @return if reach the state before timeout; false otherwise.
   * @throws Exception
   *         if interrupted while waiting for the state transition
   *         or an unexpected error while MockNM is hearbeating.
   */
  public boolean waitForState(MockNM nm, ContainerId containerId,
                              RMContainerState containerState, int timeoutMsecs) throws Exception {
    return waitForState(Arrays.asList(nm), containerId, containerState,
        timeoutMsecs);
  }

  /**
   * Wait until a container has reached a specified state.
   * The timeout is 10 seconds.
   * @param nms array of mock nodemanagers
   * @param containerId the id of a container
   * @param containerState the container state waited
   * @return if reach the state before timeout; false otherwise.
   * @throws Exception
   *         if interrupted while waiting for the state transition
   *         or an unexpected error while MockNM is hearbeating.
   */
  public boolean waitForState(Collection<MockNM> nms, ContainerId containerId,
                              RMContainerState containerState) throws Exception {
    return waitForState(nms, containerId, containerState,
        TIMEOUT_MS_FOR_CONTAINER_AND_NODE);
  }

  /**
   * Wait until a container has reached a specified state.
   * The timeout is specified by the parameter.
   * @param nms array of mock nodemanagers
   * @param containerId the id of a container
   * @param containerState the container state waited
   * @param timeoutMsecs the length of timeout in milliseconds
   * @return if reach the state before timeout; false otherwise.
   * @throws Exception
   *         if interrupted while waiting for the state transition
   *         or an unexpected error while MockNM is hearbeating.
   */
  public boolean waitForState(Collection<MockNM> nms, ContainerId containerId,
                              RMContainerState containerState, int timeoutMsecs) throws Exception {
    drainEventsImplicitly();
    RMContainer container = scheduler.getRMContainer(containerId);
    int timeWaiting = 0;
    while (container == null) {
      if (timeWaiting >= timeoutMsecs) {
        return false;
      }

      for (MockNM nm : nms) {
        nm.nodeHeartbeat(true);
      }
      drainEventsImplicitly();
      container = scheduler.getRMContainer(containerId);
      LOG.info("Waiting for container " + containerId + " to be "
          + containerState + ", container is null right now.");
      Thread.sleep(WAIT_MS_PER_LOOP);
      timeWaiting += WAIT_MS_PER_LOOP;
    }

    while (!containerState.equals(container.getState())) {
      if (timeWaiting >= timeoutMsecs) {
        return false;
      }

      LOG.info("Container : " + containerId + " State is : "
          + container.getState() + " Waiting for state : " + containerState);
      for (MockNM nm : nms) {
        nm.nodeHeartbeat(true);
      }
      drainEventsImplicitly();
      Thread.sleep(WAIT_MS_PER_LOOP);
      timeWaiting += WAIT_MS_PER_LOOP;
    }

    LOG.info("Container State is : " + container.getState());
    return true;
  }

  // get new application id
  public GetNewApplicationResponse getNewAppId() throws Exception {
    ApplicationClientProtocol client = getClientRMService();
    return client.getNewApplication(Records
        .newRecord(GetNewApplicationRequest.class));
  }

  public RMApp submitApp(int masterMemory) throws Exception {
    return submitApp(masterMemory, false);
  }

  public RMApp submitApp(int masterMemory,
                         String name,
                         String user,
                         boolean unmanaged,
                         String queue,
                         Priority priority) throws Exception {
    Resource resource = Records.newRecord(Resource.class);
    resource.setMemorySize(masterMemory);
    return submitApp(
        resource,
        name,
        user,
        unmanaged,
        queue,
        priority
    );
  }

  public RMApp submitApp(Resource resource,
                         String name,
                         String user,
                         boolean unmanaged,
                         String queue,
                         Priority priority) throws Exception {
    priority = (priority == null) ? getDefaultPriority() : priority;
    ResourceRequest amResourceRequest = ResourceRequest.newInstance(
        priority, ResourceRequest.ANY, resource, 1);
    return submitApp(Collections.singletonList(amResourceRequest), name, user,
        null, unmanaged, queue, super.getConfig().getInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
            YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS), null, null, true,
        false, false, null,
        0, null,
        true, priority, null, null, null, null);
  }

  public RMApp submitApp(int masterMemory, Priority priority) throws Exception {
    Resource resource = Resource.newInstance(masterMemory, 0);
    return submitApp(resource, "", UserGroupInformation.getCurrentUser()
            .getShortUserName(), null, false, null,
        super.getConfig().getInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
            YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS), null, null, true,
        false, false, null, 0, null, true, priority);
  }

  public RMApp submitApp(int masterMemory, Priority priority,
                         Credentials cred, ByteBuffer tokensConf) throws Exception {
    Resource resource = Resource.newInstance(masterMemory, 0);
    return submitApp(resource, "", UserGroupInformation.getCurrentUser()
            .getShortUserName(), null, false, null,
        super.getConfig().getInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
            YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS), cred, null, true,
        false, false, null, 0, null, true, priority, null, null,
        tokensConf);
  }

  public RMApp submitApp(int masterMemory, boolean unmanaged)
      throws Exception {
    return submitApp(masterMemory, "", UserGroupInformation.getCurrentUser()
        .getShortUserName(), unmanaged);
  }

  // client
  public RMApp submitApp(int masterMemory, String name, String user) throws Exception {
    return submitApp(masterMemory, name, user, false);
  }

  public RMApp submitApp(int masterMemory, String name, String user,
                         boolean unmanaged)
      throws Exception {
    return submitApp(masterMemory, name, user, null, unmanaged, null,
        super.getConfig().getInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
            YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS), null);
  }

  public RMApp submitApp(int masterMemory, String name, String user,
                         Map<ApplicationAccessType, String> acls) throws Exception {
    return submitApp(masterMemory, name, user, acls, false, null,
        super.getConfig().getInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
            YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS), null);
  }

  public RMApp submitApp(int masterMemory, String name, String user,
                         Map<ApplicationAccessType, String> acls, String queue) throws Exception {
    return submitApp(masterMemory, name, user, acls, false, queue,
        super.getConfig().getInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
            YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS), null);
  }

  public RMApp submitApp(int masterMemory, String name, String user,
                         Map<ApplicationAccessType, String> acls, String queue, String amLabel)
      throws Exception {
    Resource resource = Records.newRecord(Resource.class);
    resource.setMemorySize(masterMemory);
    Priority priority = getDefaultPriority();
    return submitApp(resource, name, user, acls, false, queue,
        super.getConfig().getInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
            YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS), null, null, true, false,
        false, null, 0, null, true, priority, amLabel, null, null);
  }

  public RMApp submitApp(Resource resource, String name, String user,
                         Map<ApplicationAccessType, String> acls, String queue) throws Exception {
    return submitApp(resource, name, user, acls, false, queue,
        super.getConfig().getInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
            YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS), null, null,
        true, false, false, null, 0, null, true, null);
  }

  public RMApp submitApp(Resource resource, String name, String user,
                         Map<ApplicationAccessType, String> acls, boolean unManaged, String queue)
      throws Exception {
    return submitApp(resource, name, user, acls, unManaged, queue,
        super.getConfig().getInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
            YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS), null, null, true,
        false, false, null, 0, null, true, null);
  }

  public RMApp submitApp(int masterMemory, String name, String user,
                         Map<ApplicationAccessType, String> acls, String queue,
                         boolean waitForAccepted) throws Exception {
    return submitApp(masterMemory, name, user, acls, false, queue,
        super.getConfig().getInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
            YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS), null, null,
        waitForAccepted);
  }

  public RMApp submitApp(int masterMemory, String name, String user,
                         Map<ApplicationAccessType, String> acls, boolean unmanaged, String queue,
                         int maxAppAttempts, Credentials ts) throws Exception {
    return submitApp(masterMemory, name, user, acls, unmanaged, queue,
        maxAppAttempts, ts, null);
  }

  public RMApp submitApp(int masterMemory, String name, String user,
                         Map<ApplicationAccessType, String> acls, boolean unmanaged, String queue,
                         int maxAppAttempts, Credentials ts, String appType) throws Exception {
    return submitApp(masterMemory, name, user, acls, unmanaged, queue,
        maxAppAttempts, ts, appType, true);
  }

  public RMApp submitApp(int masterMemory, String name, String user,
                         Map<ApplicationAccessType, String> acls, boolean unmanaged, String queue,
                         int maxAppAttempts, Credentials ts, String appType,
                         boolean waitForAccepted)
      throws Exception {
    return submitApp(masterMemory, name, user, acls, unmanaged, queue,
        maxAppAttempts, ts, appType, waitForAccepted, false);
  }

  public RMApp submitApp(int masterMemory, String name, String user,
                         Map<ApplicationAccessType, String> acls, boolean unmanaged, String queue,
                         int maxAppAttempts, Credentials ts, String appType,
                         boolean waitForAccepted, boolean keepContainers) throws Exception {
    Resource resource = Records.newRecord(Resource.class);
    resource.setMemorySize(masterMemory);
    return submitApp(resource, name, user, acls, unmanaged, queue,
        maxAppAttempts, ts, appType, waitForAccepted, keepContainers,
        false, null, 0, null, true, getDefaultPriority());
  }

  public RMApp submitApp(int masterMemory, long attemptFailuresValidityInterval,
                         boolean keepContainers) throws Exception {
    Resource resource = Records.newRecord(Resource.class);
    resource.setMemorySize(masterMemory);
    Priority priority = getDefaultPriority();
    return submitApp(resource, "", UserGroupInformation.getCurrentUser()
            .getShortUserName(), null, false, null,
        super.getConfig().getInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
            YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS), null, null, true, keepContainers,
        false, null, attemptFailuresValidityInterval, null, true, priority);
  }

  public RMApp submitApp(int masterMemory, String name, String user,
                         Map<ApplicationAccessType, String> acls, boolean unmanaged, String queue,
                         int maxAppAttempts, Credentials ts, String appType,
                         boolean waitForAccepted, boolean keepContainers, boolean isAppIdProvided,
                         ApplicationId applicationId) throws Exception {
    Resource resource = Records.newRecord(Resource.class);
    resource.setMemorySize(masterMemory);
    Priority priority = getDefaultPriority();
    return submitApp(resource, name, user, acls, unmanaged, queue,
        maxAppAttempts, ts, appType, waitForAccepted, keepContainers,
        isAppIdProvided, applicationId, 0, null, true, priority);
  }

  public RMApp submitApp(int masterMemory,
                         LogAggregationContext logAggregationContext) throws Exception {
    Resource resource = Records.newRecord(Resource.class);
    resource.setMemorySize(masterMemory);
    Priority priority = getDefaultPriority();
    return submitApp(resource, "", UserGroupInformation.getCurrentUser()
            .getShortUserName(), null, false, null,
        super.getConfig().getInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
            YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS), null, null, true, false,
        false, null, 0, logAggregationContext, true, priority);
  }

  public RMApp submitApp(Resource capability, String name, String user,
                         Map<ApplicationAccessType, String> acls, boolean unmanaged, String queue,
                         int maxAppAttempts, Credentials ts, String appType,
                         boolean waitForAccepted, boolean keepContainers, boolean isAppIdProvided,
                         ApplicationId applicationId, long attemptFailuresValidityInterval,
                         LogAggregationContext logAggregationContext,
                         boolean cancelTokensWhenComplete, Priority priority) throws Exception {
    return submitApp(capability, name, user, acls, unmanaged, queue,
        maxAppAttempts, ts, appType, waitForAccepted, keepContainers,
        isAppIdProvided, applicationId, attemptFailuresValidityInterval,
        logAggregationContext, cancelTokensWhenComplete, priority, "", null,
        null);
  }

  public RMApp submitApp(Credentials cred, ByteBuffer tokensConf)
      throws Exception {
    return submitApp(Resource.newInstance(200, 1), "app1", "user", null, false,
        null, super.getConfig().getInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
            YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS), cred, null, true,
        false, false, null, 0, null, true, getDefaultPriority(), null, null,
        tokensConf);
  }

  public RMApp submitApp(List<ResourceRequest> amResourceRequests)
      throws Exception {
    return submitApp(amResourceRequests, "app1", "user", null, false, null,
        super.getConfig().getInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
            YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS), null, null, true,
        false, false, null, 0, null, true,
        amResourceRequests.get(0).getPriority(),
        amResourceRequests.get(0).getNodeLabelExpression(), null, null, null);
  }

  public RMApp submitApp(List<ResourceRequest> amResourceRequests,
                         String appNodeLabel) throws Exception {
    return submitApp(amResourceRequests, "app1", "user", null, false, null,
        super.getConfig().getInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
            YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS), null, null, true,
        false, false, null, 0, null, true,
        amResourceRequests.get(0).getPriority(),
        amResourceRequests.get(0).getNodeLabelExpression(), null, null,
        appNodeLabel);
  }

  public RMApp submitApp(Resource capability, String name, String user,
                         Map<ApplicationAccessType, String> acls, boolean unmanaged, String queue,
                         int maxAppAttempts, Credentials ts, String appType,
                         boolean waitForAccepted, boolean keepContainers, boolean isAppIdProvided,
                         ApplicationId applicationId, long attemptFailuresValidityInterval,
                         LogAggregationContext logAggregationContext,
                         boolean cancelTokensWhenComplete, Priority priority, String amLabel,
                         Map<ApplicationTimeoutType, Long> applicationTimeouts,
                         ByteBuffer tokensConf)
      throws Exception {
    priority = (priority == null) ? getDefaultPriority() : priority;
    ResourceRequest amResourceRequest = ResourceRequest.newInstance(
        priority, ResourceRequest.ANY, capability, 1);
    if (amLabel != null && !amLabel.isEmpty()) {
      amResourceRequest.setNodeLabelExpression(amLabel.trim());
    }
    return submitApp(Collections.singletonList(amResourceRequest), name, user,
        acls, unmanaged, queue, maxAppAttempts, ts, appType, waitForAccepted,
        keepContainers, isAppIdProvided, applicationId,
        attemptFailuresValidityInterval, logAggregationContext,
        cancelTokensWhenComplete, priority, amLabel, applicationTimeouts,
        tokensConf, null);
  }

  public RMApp submitApp(List<ResourceRequest> amResourceRequests, String name,
                         String user, Map<ApplicationAccessType, String> acls, boolean unmanaged,
                         String queue, int maxAppAttempts, Credentials ts, String appType,
                         boolean waitForAccepted, boolean keepContainers, boolean isAppIdProvided,
                         ApplicationId applicationId, long attemptFailuresValidityInterval,
                         LogAggregationContext logAggregationContext,
                         boolean cancelTokensWhenComplete, Priority priority, String amLabel,
                         Map<ApplicationTimeoutType, Long> applicationTimeouts,
                         ByteBuffer tokensConf, String appNodeLabel) throws Exception {
    ApplicationId appId = isAppIdProvided ? applicationId : null;
    ApplicationClientProtocol client = getClientRMService();
    if (! isAppIdProvided) {
      GetNewApplicationResponse resp = client.getNewApplication(Records
          .newRecord(GetNewApplicationRequest.class));
      appId = resp.getApplicationId();
    }
    SubmitApplicationRequest req = Records
        .newRecord(SubmitApplicationRequest.class);
    ApplicationSubmissionContext sub = Records
        .newRecord(ApplicationSubmissionContext.class);
    sub.setKeepContainersAcrossApplicationAttempts(keepContainers);
    sub.setApplicationId(appId);
    sub.setApplicationName(name);
    sub.setMaxAppAttempts(maxAppAttempts);
    if (applicationTimeouts != null && applicationTimeouts.size() > 0) {
      sub.setApplicationTimeouts(applicationTimeouts);
    }
    if (unmanaged) {
      sub.setUnmanagedAM(true);
    }
    if (queue != null) {
      sub.setQueue(queue);
    }
    if (priority != null) {
      sub.setPriority(priority);
    }
    if (appNodeLabel != null) {
      sub.setNodeLabelExpression(appNodeLabel);
    }
    sub.setApplicationType(appType);
    ContainerLaunchContext clc = Records
        .newRecord(ContainerLaunchContext.class);
    clc.setApplicationACLs(acls);
    if (ts != null && UserGroupInformation.isSecurityEnabled()) {
      DataOutputBuffer dob = new DataOutputBuffer();
      ts.writeTokenStorageToStream(dob);
      ByteBuffer securityTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
      clc.setTokens(securityTokens);
      clc.setTokensConf(tokensConf);
    }
    sub.setAMContainerSpec(clc);
    sub.setAttemptFailuresValidityInterval(attemptFailuresValidityInterval);
    if (logAggregationContext != null) {
      sub.setLogAggregationContext(logAggregationContext);
    }
    sub.setCancelTokensWhenComplete(cancelTokensWhenComplete);
    if (amLabel != null && !amLabel.isEmpty()) {
      for (ResourceRequest amResourceRequest : amResourceRequests) {
        amResourceRequest.setNodeLabelExpression(amLabel.trim());
      }
    }
    sub.setAMContainerResourceRequests(amResourceRequests);
    req.setApplicationSubmissionContext(sub);
    UserGroupInformation fakeUser =
        UserGroupInformation.createUserForTesting(user, new String[] {"someGroup"});
    PrivilegedExceptionAction<SubmitApplicationResponse> action =
        new PrivilegedExceptionAction<SubmitApplicationResponse>() {
          ApplicationClientProtocol client;
          SubmitApplicationRequest req;
          @Override
          public SubmitApplicationResponse run() throws IOException, YarnException {
            try {
              return client.submitApplication(req);
            } catch (YarnException | IOException e) {
              e.printStackTrace();
              throw  e;
            }
          }
          PrivilegedExceptionAction<SubmitApplicationResponse> setClientReq(
              ApplicationClientProtocol client, SubmitApplicationRequest req) {
            this.client = client;
            this.req = req;
            return this;
          }
        }.setClientReq(client, req);
    fakeUser.doAs(action);
    // make sure app is immediately available after submit
    if (waitForAccepted) {
      waitForState(appId, RMAppState.ACCEPTED);
    }
    RMApp rmApp = getRMContext().getRMApps().get(appId);

    // unmanaged AM won't go to RMAppAttemptState.SCHEDULED.
    if (waitForAccepted && !unmanaged) {
      waitForState(rmApp.getCurrentAppAttempt().getAppAttemptId(),
          RMAppAttemptState.SCHEDULED);
    }

    ((AbstractYarnScheduler)scheduler).update();

    return rmApp;
  }

  private Priority getDefaultPriority() {
    return Priority.newInstance(
        super.getConfig()
            .getInt(
                YarnConfiguration.NM_CONTAINER_EXECUTOR_SCHED_PRIORITY,
                YarnConfiguration.DEFAULT_NM_CONTAINER_EXECUTOR_SCHED_PRIORITY));
  }

  public MockNM unRegisterNode(MockNM nm) throws Exception {
    nm.unRegisterNode();
    drainEventsImplicitly();
    return nm;
  }

  public MockNM registerNode(String nodeIdStr, int memory) throws Exception {
    MockNM nm = new MockNM(nodeIdStr, memory, getResourceTrackerService());
    nm.registerNode();
    drainEventsImplicitly();
    return nm;
  }

  public MockNM registerNode(String nodeIdStr, int memory, int vCores)
      throws Exception {
    MockNM nm =
        new MockNM(nodeIdStr, memory, vCores, getResourceTrackerService());
    nm.registerNode();
    drainEventsImplicitly();
    return nm;
  }

  public MockNM registerNode(String nodeIdStr, int memory, int vCores,
                             List<ApplicationId> runningApplications) throws Exception {
    MockNM nm =
        new MockNM(nodeIdStr, memory, vCores, getResourceTrackerService(),
            YarnVersionInfo.getVersion());
    nm.registerNode(runningApplications);
    drainEventsImplicitly();
    return nm;
  }

  public MockNM registerNode(String nodeIdStr, Resource nodeCapability)
      throws Exception {
    MockNM nm = new MockNM(nodeIdStr, nodeCapability,
        getResourceTrackerService());
    nm.registerNode();
    drainEventsImplicitly();
    return nm;
  }

  public void sendNodeStarted(MockNM nm) throws Exception {
    RMNodeImpl node = (RMNodeImpl) getRMContext().getRMNodes().get(
        nm.getNodeId());
    NodeStatus mockNodeStatus = MockNM.createMockNodeStatus();
    node.handle(new RMNodeStartedEvent(nm.getNodeId(), null, null,
        mockNodeStatus));
    drainEventsImplicitly();
  }

  public void sendNodeLost(MockNM nm) throws Exception {
    RMNodeImpl node = (RMNodeImpl) getRMContext().getRMNodes().get(
        nm.getNodeId());
    node.handle(new RMNodeEvent(nm.getNodeId(), RMNodeEventType.EXPIRE));
    drainEventsImplicitly();
  }

  private RMNode getRMNode(NodeId nodeId) {
    RMNode node = getRMContext().getRMNodes().get(nodeId);
    if (node == null) {
      node = getRMContext().getInactiveRMNodes().get(nodeId);
    }
    return node;
  }

  /**
   * Wait until a node has reached a specified state.
   * The timeout is 20 seconds.
   * @param nodeId the id of a node
   * @param finalState the node state waited
   * @throws InterruptedException
   *         if interrupted while waiting for the state transition
   */
  public boolean waitForState(NodeId nodeId, NodeState finalState)
      throws InterruptedException {
    drainEventsImplicitly();
    int timeWaiting = 0;
    RMNode node = getRMNode(nodeId);
    while (node == null) {
      if (timeWaiting >= TIMEOUT_MS_FOR_CONTAINER_AND_NODE) {
        LOG.info("Node " + nodeId + " State is : " + node.getState());
        return false;
      }
      node = getRMNode(nodeId);
      Thread.sleep(WAIT_MS_PER_LOOP);
      timeWaiting += WAIT_MS_PER_LOOP;
    }
    while (!finalState.equals(node.getState())) {
      if (timeWaiting >= TIMEOUT_MS_FOR_CONTAINER_AND_NODE) {
        LOG.info("Node " + nodeId + " State is : " + node.getState());
        return false;
      }

      LOG.info("Node State is : " + node.getState()
          + " Waiting for state : " + finalState);
      Thread.sleep(WAIT_MS_PER_LOOP);
      timeWaiting += WAIT_MS_PER_LOOP;
    }

    LOG.info("Node " + nodeId + " State is : " + node.getState());
    return true;
  }

  public void sendNodeGracefulDecommission(
      MockNM nm, int timeout) throws Exception {
    RMNodeImpl node = (RMNodeImpl)
        getRMContext().getRMNodes().get(nm.getNodeId());
    node.handle(new RMNodeDecommissioningEvent(nm.getNodeId(), timeout));
  }

  public void sendNodeEvent(MockNM nm, RMNodeEventType event) throws Exception {
    RMNodeImpl node = (RMNodeImpl)
        getRMContext().getRMNodes().get(nm.getNodeId());
    node.handle(new RMNodeEvent(nm.getNodeId(), event));
  }

  public Integer getDecommissioningTimeout(NodeId nodeid) {
    return this.getRMContext().getRMNodes()
        .get(nodeid).getDecommissioningTimeout();
  }

  public KillApplicationResponse killApp(ApplicationId appId) throws Exception {
    ApplicationClientProtocol client = getClientRMService();
    KillApplicationRequest req = KillApplicationRequest.newInstance(appId);
    KillApplicationResponse response = client.forceKillApplication(req);
    drainEventsImplicitly();
    return response;
  }

  public FailApplicationAttemptResponse failApplicationAttempt(
      ApplicationAttemptId attemptId) throws Exception {
    ApplicationClientProtocol client = getClientRMService();
    FailApplicationAttemptRequest req =
        FailApplicationAttemptRequest.newInstance(attemptId);
    FailApplicationAttemptResponse response =
        client.failApplicationAttempt(req);
    drainEventsImplicitly();
    return response;
  }

  /**
   * recommend to use launchAM, or use sendAMLaunched like:
   * 1, wait RMAppAttempt scheduled
   * 2, send node heartbeat
   * 3, sendAMLaunched
   */
  public MockAM sendAMLaunched(ApplicationAttemptId appAttemptId)
      throws Exception {
    MockAM am = new MockAM(getRMContext(), masterService, appAttemptId);
    ((AbstractYarnScheduler)scheduler).update();
    waitForState(appAttemptId, RMAppAttemptState.ALLOCATED);
    //create and set AMRMToken
    Token<AMRMTokenIdentifier> amrmToken =
        this.rmContext.getAMRMTokenSecretManager().createAndGetAMRMToken(
            appAttemptId);
    ((RMAppAttemptImpl) this.rmContext.getRMApps()
        .get(appAttemptId.getApplicationId()).getRMAppAttempt(appAttemptId))
        .setAMRMToken(amrmToken);
    getRMContext()
        .getDispatcher()
        .getEventHandler()
        .handle(
            new RMAppAttemptEvent(appAttemptId, RMAppAttemptEventType.LAUNCHED));
    drainEventsImplicitly();
    return am;
  }

  /**
   * Launches the AM.
   * @param am
   */
  public void launchAM(final MockAM am) {
    //create and set AMRMToken
    Token<AMRMTokenIdentifier> amrmToken =
        this.rmContext.getAMRMTokenSecretManager().createAndGetAMRMToken(
            am.getApplicationAttemptId());
    ((RMAppAttemptImpl) this.rmContext.getRMApps()
        .get(am.getApplicationAttemptId().getApplicationId()).getRMAppAttempt(am.getApplicationAttemptId()))
        .setAMRMToken(amrmToken);
    getRMContext()
        .getDispatcher()
        .getEventHandler()
        .handle(
            new RMAppAttemptEvent(am.getApplicationAttemptId(), RMAppAttemptEventType.LAUNCHED));
    drainEventsImplicitly();
  }

  public void sendAMLaunchFailed(ApplicationAttemptId appAttemptId)
      throws Exception {
    MockAM am = new MockAM(getRMContext(), masterService, appAttemptId);
    waitForState(am.getApplicationAttemptId(), RMAppAttemptState.ALLOCATED);
    getRMContext().getDispatcher().getEventHandler()
        .handle(new RMAppAttemptEvent(appAttemptId,
            RMAppAttemptEventType.LAUNCH_FAILED, "Failed"));
    drainEventsImplicitly();
  }

  @Override
  protected ClientRMService createClientRMService() {
    return new ClientRMService(getRMContext(), scheduler,
        rmAppManager, applicationACLsManager, queueACLsManager,
        getRMContext().getRMDelegationTokenSecretManager(), clock) {
      @Override
      protected void serviceStart() {
        // override to not start rpc handler
      }

      @Override
      protected void serviceStop() {
        // don't do anything
      }
    };
  }

  @Override
  protected ResourceTrackerService createResourceTrackerService() {

    RMContainerTokenSecretManager containerTokenSecretManager =
        getRMContext().getContainerTokenSecretManager();
    containerTokenSecretManager.rollMasterKey();
    NMTokenSecretManagerInRM nmTokenSecretManager =
        getRMContext().getNMTokenSecretManager();
    nmTokenSecretManager.rollMasterKey();
    return new ResourceTrackerService(getRMContext(), nodesListManager,
        this.nmLivelinessMonitor, containerTokenSecretManager,
        nmTokenSecretManager) {

      @Override
      protected void serviceStart() {
        // override to not start rpc handler
      }

      @Override
      protected void serviceStop() {
        // don't do anything
      }
    };
  }

  @Override
  protected ApplicationMasterService createApplicationMasterService() {
    if (this.rmContext.getYarnConfiguration().getBoolean(
        YarnConfiguration.OPPORTUNISTIC_CONTAINER_ALLOCATION_ENABLED,
        YarnConfiguration.DEFAULT_OPPORTUNISTIC_CONTAINER_ALLOCATION_ENABLED)) {
      return new OpportunisticContainerAllocatorAMService(getRMContext(), scheduler) {
        @Override
        protected void serviceStart() {
          // override to not start rpc handler
        }

        @Override
        protected void serviceStop() {
          // don't do anything
        }
      };
    }
    return new ApplicationMasterService(getRMContext(), scheduler) {
      @Override
      protected void serviceStart() {
        // override to not start rpc handler
      }

      @Override
      protected void serviceStop() {
        // don't do anything
      }
    };
  }

  @Override
  protected ApplicationMasterLauncher createAMLauncher() {
    return new ApplicationMasterLauncher(getRMContext()) {
      @Override
      protected void serviceStart() {
        // override to not start rpc handler
      }

      @Override
      public void handle(AMLauncherEvent appEvent) {
        // don't do anything
      }

      @Override
      protected void serviceStop() {
        // don't do anything
      }
    };
  }

  @Override
  protected AdminService createAdminService() {
    return new AdminService(this) {
      @Override
      protected void startServer() {
        // override to not start rpc handler
      }

      @Override
      protected void stopServer() {
        // don't do anything
      }
    };
  }

  public NodesListManager getNodesListManager() {
    return this.nodesListManager;
  }

  public ClientToAMTokenSecretManagerInRM getClientToAMTokenSecretManager() {
    return this.getRMContext().getClientToAMTokenSecretManager();
  }

  public RMAppManager getRMAppManager() {
    return this.rmAppManager;
  }

  public AdminService getAdminService() {
    return this.adminService;
  }

  @Override
  protected void startWepApp() {
    if (getConfig().getBoolean(ENABLE_WEBAPP, false)) {
      super.startWepApp();
      return;
    }

    // Disable webapp
  }

  public static void finishAMAndVerifyAppState(RMApp rmApp, MockRM rm, MockNM nm,
                                               MockAM am) throws Exception {
    FinishApplicationMasterRequest req =
        FinishApplicationMasterRequest.newInstance(
            FinalApplicationStatus.SUCCEEDED, "", "");
    am.unregisterAppAttempt(req,true);
    rm.waitForState(am.getApplicationAttemptId(), RMAppAttemptState.FINISHING);
    nm.nodeHeartbeat(am.getApplicationAttemptId(), 1, ContainerState.COMPLETE);
    rm.drainEventsImplicitly();
    rm.waitForState(am.getApplicationAttemptId(), RMAppAttemptState.FINISHED);
    rm.waitForState(rmApp.getApplicationId(), RMAppState.FINISHED);
  }

  @SuppressWarnings("rawtypes")
  private static void waitForSchedulerAppAttemptAdded(
      ApplicationAttemptId attemptId, MockRM rm) throws InterruptedException {
    int tick = 0;
    rm.drainEventsImplicitly();
    // Wait for at most 5 sec
    while (null == ((AbstractYarnScheduler) rm.getResourceScheduler())
        .getApplicationAttempt(attemptId) && tick < 50) {
      Thread.sleep(100);
      if (tick % 10 == 0) {
        LOG.info("waiting for SchedulerApplicationAttempt="
            + attemptId + " added.");
      }
      tick++;
    }
  }

  public static MockAM launchAMWhenAsyncSchedulingEnabled(RMApp app, MockRM rm)
      throws Exception {
    int i = 0;
    while (app.getCurrentAppAttempt() == null) {
      if (i < 100) {
        i++;
      }
      Thread.sleep(50);
    }

    RMAppAttempt attempt = app.getCurrentAppAttempt();

    rm.waitForState(attempt.getAppAttemptId(),
        RMAppAttemptState.ALLOCATED);
    MockAM am = rm.sendAMLaunched(attempt.getAppAttemptId());
    rm.waitForState(attempt.getAppAttemptId(), RMAppAttemptState.LAUNCHED);

    return am;
  }

  /**
   * NOTE: nm.nodeHeartbeat is explicitly invoked,
   * don't invoke it before calling launchAM
   */
  public static MockAM launchAM(RMApp app, MockRM rm, MockNM nm)
      throws Exception {
    rm.drainEventsImplicitly();
    RMAppAttempt attempt = waitForAttemptScheduled(app, rm);
    LOG.info("Launch AM " + attempt.getAppAttemptId());
    nm.nodeHeartbeat(true);
    ((AbstractYarnScheduler)rm.getResourceScheduler()).update();
    rm.drainEventsImplicitly();
    nm.nodeHeartbeat(true);
    MockAM am = rm.sendAMLaunched(attempt.getAppAttemptId());
    rm.waitForState(attempt.getAppAttemptId(), RMAppAttemptState.LAUNCHED);
    return am;
  }

  public static MockAM launchUAM(RMApp app, MockRM rm, MockNM nm)
      throws Exception {
    rm.drainEventsImplicitly();
    // UAMs go directly to LAUNCHED state
    rm.waitForState(app.getApplicationId(), RMAppState.ACCEPTED);
    RMAppAttempt attempt = app.getCurrentAppAttempt();
    waitForSchedulerAppAttemptAdded(attempt.getAppAttemptId(), rm);
    LOG.info("Launch AM " + attempt.getAppAttemptId());
    nm.nodeHeartbeat(true);
    ((AbstractYarnScheduler)rm.getResourceScheduler()).update();
    rm.drainEventsImplicitly();
    nm.nodeHeartbeat(true);
    MockAM am = new MockAM(rm.getRMContext(), rm.masterService,
        attempt.getAppAttemptId());
    rm.waitForState(attempt.getAppAttemptId(), RMAppAttemptState.LAUNCHED);
    return am;
  }

  public static RMAppAttempt waitForAttemptScheduled(RMApp app, MockRM rm)
      throws Exception {
    rm.waitForState(app.getApplicationId(), RMAppState.ACCEPTED);
    RMAppAttempt attempt = app.getCurrentAppAttempt();
    waitForSchedulerAppAttemptAdded(attempt.getAppAttemptId(), rm);
    rm.waitForState(attempt.getAppAttemptId(), RMAppAttemptState.SCHEDULED);
    return attempt;
  }

  public static MockAM launchAndRegisterAM(RMApp app, MockRM rm, MockNM nm)
      throws Exception {
    MockAM am = launchAM(app, rm, nm);
    am.registerAppAttempt();
    rm.waitForState(app.getApplicationId(), RMAppState.RUNNING);
    return am;
  }

  public static MockAM launchAndRegisterAM(RMApp app, MockRM rm, MockNM nm,
                                           Map<Set<String>, PlacementConstraint> constraints) throws Exception {
    MockAM am = launchAM(app, rm, nm);
    for (Map.Entry<Set<String>, PlacementConstraint> e :
        constraints.entrySet()) {
      am.addPlacementConstraint(e.getKey(), e.getValue());
    }
    am.registerAppAttempt();
    rm.waitForState(app.getApplicationId(), RMAppState.RUNNING);
    return am;
  }

  public ApplicationReport getApplicationReport(ApplicationId appId)
      throws YarnException, IOException {
    ApplicationClientProtocol client = getClientRMService();
    GetApplicationReportResponse response =
        client.getApplicationReport(GetApplicationReportRequest
            .newInstance(appId));
    return response.getApplicationReport();
  }

  public void updateReservationState(ReservationUpdateRequest request)
      throws IOException, YarnException {
    ApplicationClientProtocol client = getClientRMService();
    client.updateReservation(request);
    drainEventsImplicitly();
  }

  // Explicitly reset queue metrics for testing.
  @SuppressWarnings("static-access")
  public void clearQueueMetrics(RMApp app) {
    ((AbstractYarnScheduler<SchedulerApplicationAttempt, SchedulerNode>) getResourceScheduler())
        .getSchedulerApplications().get(app.getApplicationId()).getQueue()
        .getMetrics().clearQueueMetrics();
  }

  public RMActiveServices getRMActiveService() {
    return activeServices;
  }

  public void signalToContainer(ContainerId containerId,
                                SignalContainerCommand command) throws Exception {
    ApplicationClientProtocol client = getClientRMService();
    SignalContainerRequest req =
        SignalContainerRequest.newInstance(containerId, command);
    client.signalToContainer(req);
    drainEventsImplicitly();
  }

  /**
   * Wait until an app removed from scheduler.
   * The timeout is 40 seconds.
   * @param appId the id of an app
   * @throws InterruptedException
   *         if interrupted while waiting for app removed
   */
  public void waitForAppRemovedFromScheduler(ApplicationId appId)
      throws InterruptedException {
    int timeWaiting = 0;
    drainEventsImplicitly();

    Map<ApplicationId, SchedulerApplication> apps  =
        ((AbstractYarnScheduler) getResourceScheduler())
            .getSchedulerApplications();
    while (apps.containsKey(appId)) {
      if (timeWaiting >= TIMEOUT_MS_FOR_APP_REMOVED) {
        break;
      }
      LOG.info("wait for app removed, " + appId);
      Thread.sleep(WAIT_MS_PER_LOOP);
      timeWaiting += WAIT_MS_PER_LOOP;
    }
    LOG.info("app is removed from scheduler, " + appId);
  }

  private void drainEventsImplicitly() {
    if (!disableDrainEventsImplicitly) {
      drainEvents();
    }
  }

  public void disableDrainEventsImplicitly() {
    disableDrainEventsImplicitly = true;
  }

  public void enableDrainEventsImplicityly() {
    disableDrainEventsImplicitly = false;
  }

  public RMApp submitApp(int masterMemory, Priority priority,
                         Map<ApplicationTimeoutType, Long> applicationTimeouts) throws Exception {
    Resource resource = Resource.newInstance(masterMemory, 0);
    return submitApp(
        resource, "", UserGroupInformation.getCurrentUser().getShortUserName(),
        null, false, null,
        super.getConfig().getInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
            YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS), null, null, true,
        false, false, null, 0, null, true, priority, null, applicationTimeouts,
        null);
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    if (rmContext.getDispatcher() instanceof AsyncDispatcher) {
      ((AsyncDispatcher) rmContext.getDispatcher()).disableExitOnDispatchException();
    }

    metricsManager.setScheduler(scheduler);
    if (scheduler == null) {
      throw new NullArgumentException("Scheduler cannot be null!");
    }

    if (metricsManager.isMetricsOn()) {
      metricsManager.init();
      metricsManager.start();
    }

    serviceInitialized = true;
  }

  public RMStateStore getRMStateStore() {
    return getRMContext().getStateStore();
  }

  @Override
  public void stop() {
    super.stop();
    metricsManager.stop();
  }
}
