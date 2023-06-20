package org.apache.hadoop.yarn.dtss.job;

import com.google.inject.Injector;
import com.google.inject.Key;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.dtss.am.MockAM;
import org.apache.hadoop.yarn.dtss.cluster.ClusterState;
import org.apache.hadoop.yarn.dtss.cluster.ClusterTopology;
import org.apache.hadoop.yarn.dtss.cluster.ClusterTopologyBuilder;
import org.apache.hadoop.yarn.dtss.config.parameters.IsUnmanaged;
import org.apache.hadoop.yarn.dtss.exceptions.EndOfExperimentNotificationException;
import org.apache.hadoop.yarn.dtss.job.exceptions.SimulationJobFailedException;
import org.apache.hadoop.yarn.dtss.lifecycle.LifeCycle;
import org.apache.hadoop.yarn.dtss.metrics.job.JobHistoryManager;
import org.apache.hadoop.yarn.dtss.nm.MockNM;
import org.apache.hadoop.yarn.dtss.rm.MockRM;
import org.apache.hadoop.yarn.dtss.time.Clock;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;

import java.io.IOException;
import java.text.MessageFormat;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This is the simulated job master, or application master, for a YARN job.
 * Users should implement this abstraction to customize the behavior of their
 * own application logic.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public abstract class SimulatedJobMaster extends LifeCycle {
  private static final Logger LOG = Logger.getLogger(SimulatedJobMaster.class.getName());

  final Clock clock;
  final SimulatedJob job;
  final boolean isUnmanaged;

  // How long it takes for the job to spin up in seconds
  protected final long jobSpinUpTimeSeconds;
  protected final MockRM rm;
  protected boolean stopScheduled = false;

  private final JobHistoryManager jobHistoryManager;

  private NodeId masterNode;
  private MockAM am;
  private RMApp app;
  private UUID heartbeatAlarmId;
  private boolean isLoggedTryStart = false;

  private final ClusterTopology topology;
  private final ClusterState clusterState;

  private final AtomicLong executedContainerTimeSeconds = new AtomicLong(0L);
  // This map is used to keep track of allocated containers
  private final Map<SimulatedContainer, SimulatedTask> containerTaskMap = new HashMap<>();
  private final long heartbeatSeconds;

  private Long jobStartTime = null;

  SimulatedJobMaster(
      final Injector injector, final SimulatedJob job, final long heartbeatSeconds, final long jobSpinUpTimeSeconds) {
    this.heartbeatSeconds = heartbeatSeconds;
    this.job = job;
    this.rm = injector.getInstance(MockRM.class);
    this.clock = injector.getInstance(Clock.class);
    this.clusterState = injector.getInstance(ClusterState.class);
    this.jobHistoryManager = injector.getInstance(JobHistoryManager.class);
    this.isUnmanaged = injector.getInstance(Key.get(Boolean.class, IsUnmanaged.class));
    this.jobSpinUpTimeSeconds = jobSpinUpTimeSeconds;

    final ClusterTopologyBuilder topologyBuilder = injector.getInstance(ClusterTopologyBuilder.class);
    try {
      this.topology = topologyBuilder.getClusterTopology();
    } catch (final IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  /**
   * Initializes the job, submits the job to the RM, and schedule a job start.
   */
  @Override
  public void init() {
    super.init();
    try {
      LOG.log(Level.INFO, MessageFormat.format(
          "Submitted job {0} at {1}.", job.getTraceJobId(), clock.getInstant()));
      app = submitApp();
      jobHistoryManager.onJobSubmitted(job, app);
      LOG.log(Level.INFO, MessageFormat.format("Job {0} assigned ID {1}.", job.getTraceJobId(),
          app.getApplicationId()));
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    final AtomicLong tryStartCount = new AtomicLong();
    // Spin up the job, unschedule to Alarm if spin up fails
    clock.schedulePeriodicAlarm(jobSpinUpTimeSeconds, periodicClientAlarm -> {
      if (tryStart(tryStartCount)) {
        clock.unschedulePeriodicAlarm(periodicClientAlarm.getPeriodicAlarmId());
      }
    });
  }

  public String getTraceJobId() {
    return job.getTraceJobId();
  }

  /**
   * Tries to start the job by checking its state with the RM.
   * @param tryStartCount How many times the job has tried to start
   * @return Whether or not the job has started successfully
   */
  private boolean tryStart(final AtomicLong tryStartCount) {
    final RMAppAttempt rmAppAttempt = app.getCurrentAppAttempt();

    if (!isLoggedTryStart) {
      LOG.log(Level.INFO, "Trying to start application " + job.getTraceJobId() + "...");
      isLoggedTryStart = true;
    }

    // Kick the scheduler...
    clusterState.heartbeatAll();
    ((AbstractYarnScheduler)rm.getResourceScheduler()).update();
    try {
      final RMAppAttemptState stateToWait = isUnmanaged ? RMAppAttemptState.LAUNCHED : RMAppAttemptState.ALLOCATED;
      if (!MockRM.waitForState(rmAppAttempt, stateToWait, 5)) {
        return false;
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    start();
    return true;
  }

  /**
   * Starts the job and register the application by allocating the AM.
   * Initializes the periodic heartbeat callbacks on the simulated clock.
   */
  @Override
  public void start() {
    super.start();

    // Create the AM container and regsiter application with RM
    try {
      final Container masterContainer = app.getCurrentAppAttempt().getMasterContainer();
      if (masterContainer != null) {
        masterNode = masterContainer.getNodeId();
      } else {
        if (!isUnmanaged) {
          throw new IllegalArgumentException("Expected managed AMs to have a master container!");
        }

        masterNode = null;
      }

      am = new MockAM(
          rm.getRMContext(),
          rm.getApplicationMasterService(),
          app.getCurrentAppAttempt().getAppAttemptId());

      // The AM is launched by default if the AM is unmanaged
      if (!isUnmanaged) {
        rm.launchAM(am);
      }

      LOG.log(Level.INFO, MessageFormat.format(
          "Job {0} launched at {1}!", app.getCurrentAppAttempt().getAppAttemptId(), clock.getInstant()));

      am.registerAppAttempt();

      LOG.log(Level.FINE, MessageFormat.format(
          "Job {0} registered with RM!", app.getCurrentAppAttempt().getAppAttemptId()));
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    try {
      onHeartbeat(SimulatedAllocateResponse.fromAllocateResponse(am.doHeartbeat()));
    } catch (final SimulationJobFailedException e) {
      tearDownAndScheduleFailure(e);
      return;
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    // Schedule heartbeat alarms for the AM
    heartbeatAlarmId = clock.schedulePeriodicAlarm(heartbeatSeconds, periodicClientAlarm -> {
      try {
        LOG.log(Level.FINER, MessageFormat.format("{0} heartbeat!", am.getApplicationAttemptId()));
        final AllocateResponse response = am.doHeartbeat();
        onHeartbeat(SimulatedAllocateResponse.fromAllocateResponse(response));
      } catch(final SimulationJobFailedException e) {
        tearDownAndScheduleFailure(e);
      } catch (final Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    });

    // Record in job history that the job has started
    jobHistoryManager.onJobStarted(job);
  }

  String getAttemptId() {
    if (app == null) {
      return null;
    }

    return app.getCurrentAppAttempt().getAppAttemptId().toString();
  }

  /**
   * Request containers from the resource manager by adding requests to the heartbeat.
   * Call this from implementing classes.
   * @param tasks The simulated tasks of the job
   */
  void requestContainers(final Collection<SimulatedTask> tasks) {
    clock.scheduleAlarm(heartbeatSeconds, alarm-> {
      try{
        am.addRequests(new String[] { "*" }, topology.getContainerMB(), 0, tasks.size());
        onHeartbeat(SimulatedAllocateResponse.fromAllocateResponse(am.schedule()));
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    });
  }

  /**
   * Simulate the run time of the container by specifying when it finishes.
   * Call this from implementing classes.
   * @param container The container allocated to the simulated job master.
   * @param task The task assigned to the container by the job.
   */
  void runContainer(final SimulatedContainer container, final SimulatedTask task) {
    final MockNM containerNM = clusterState.getNM(container.getNodeId());
    if (jobStartTime == null) {
      final Instant jobStartInstant = clock.getInstant();
      jobStartTime = jobStartInstant.getEpochSecond();
      LOG.log(Level.INFO, "Job " + job.getTraceJobId() + "'s first container started at " + jobStartInstant + "!");
    }

    try {
      LOG.log(Level.FINER, MessageFormat.format(
          "Trigger run for container {0}!", container.getId().getContainerId()));

      // Tell the node that the task is running
      containerNM.nodeHeartbeat(
          app.getCurrentAppAttempt().getAppAttemptId(),
          container.getId().getContainerId(),
          ContainerState.RUNNING);

      rm.drainEvents();

      // Set the start time of the container
      container.setStartTime(clock.getInstant());
      // Mark that the container has been allocated
      containerTaskMap.put(container, task);

      LOG.log(Level.FINER, "Container " + container.getId() + " running at " + clock.getInstant());

    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    // Schedule an alarm later at the completion time of the task
    // Currently we don't fully support failed tasks
    clock.scheduleAlarm(task.getDurationSeconds(), alarm -> {
      try {
        containerNM.nodeHeartbeat(
            app.getCurrentAppAttempt().getAppAttemptId(),
            container.getId().getContainerId(),
            ContainerState.COMPLETE);

        rm.drainEvents();

        final SimulatedTask removedTask = containerTaskMap.remove(container);
        if (container.getStartTime() == null) {
          throw new IllegalStateException("Container must have started to complete!");
        }

        final long executionSeconds = alarm.getInstant().getEpochSecond() -
            container.getStartTime().getEpochSecond();

        executedContainerTimeSeconds.addAndGet(executionSeconds);
        assert removedTask == task;

        LOG.log(Level.FINER, MessageFormat.format(
            "Container {0} completed at {1}!", container.getId(), clock.getInstant()));
      } catch (final Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    });
  }

  abstract JobSubmissionParameters getJobSubmissionParameters();

  abstract void onHeartbeat(final SimulatedAllocateResponse simulatedAllocateResponse);

  RMApp submitApp() throws Exception {
    final JobSubmissionParameters jobSubmissionParameters = getJobSubmissionParameters();
    return rm.submitApp(
        jobSubmissionParameters.getMasterMemory(),
        jobSubmissionParameters.getName(),
        jobSubmissionParameters.getUser(),
        jobSubmissionParameters.getIsUnmanaged(),
        jobSubmissionParameters.getQueue(),
        jobSubmissionParameters.getPriority()
    );
  }

  void tearDownAndScheduleFailure(final Exception e) {
    if (stopScheduled) {
      return;
    }

    stopScheduled = true;
    LOG.log(Level.INFO, MessageFormat.format(
        "Job {0} failed due to exception {1} at {2}! Tearing down job...",
        getTraceJobId(), e.getMessage(), clock.getInstant()));

    if (heartbeatAlarmId != null) {
      clock.unschedulePeriodicAlarm(heartbeatAlarmId);
    }

    clock.scheduleAlarm(1, alarm -> stop(e));
  }

  void teardownAndScheduleStop() {
    if (stopScheduled) {
      return;
    }

    stopScheduled = true;
    clock.unschedulePeriodicAlarm(heartbeatAlarmId);
    clock.scheduleAlarm(1, alarm -> stop());
  }

  @Override
  public void stop(final Exception e) {
    final String traceJobId = job.getTraceJobId();
    final boolean isEndOfExperiment = e instanceof EndOfExperimentNotificationException;
    if (am == null && !isEndOfExperiment) {
      throw new RuntimeException("AM not assigned to a valid job!");
    }

    if (am != null) {
      final ApplicationAttemptId attemptId = am.getApplicationAttemptId();

      try {
        if (!isEndOfExperiment) {
          LOG.log(Level.INFO, MessageFormat.format("Failing job {0}...", attemptId));

          final FinishApplicationMasterRequest req =
              FinishApplicationMasterRequest.newInstance(
                  FinalApplicationStatus.FAILED, "", "");

          am.unregisterAppAttempt(req, false);

          rm.drainEvents();

          if (!isUnmanaged) {
            // Tell the RM that the AM container is complete if AM is managed by RM
            clusterState.getNM(masterNode).nodeHeartbeat(
                am.getApplicationAttemptId(), 1, ContainerState.COMPLETE);
          }

          rm.drainEvents();

          LOG.log(Level.INFO, MessageFormat.format("Job {0} containers released!", attemptId));

          rm.waitForState(attemptId, RMAppAttemptState.FINISHED);

          LOG.log(Level.INFO, MessageFormat.format("Job {0} FAILED!", traceJobId));
        } else {
          LOG.log(Level.INFO, "Job " + traceJobId + " STOPPED at end of experiment");
        }
      } catch (final Exception innerE) {
        e.printStackTrace();
        throw new RuntimeException(innerE);
      }
    }

    if (e instanceof EndOfExperimentNotificationException) {
      super.stop();

      for (final SimulatedContainer container : containerTaskMap.keySet()) {
        final Instant containerStartTime = container.getStartTime();
        final long executionSeconds;
        if (containerStartTime != null) {
          executionSeconds = clock.getInstant().getEpochSecond() -
              container.getStartTime().getEpochSecond();
        } else {
          executionSeconds = 0L;
        }

        executedContainerTimeSeconds.addAndGet(executionSeconds);
      }

      jobHistoryManager.onJobNotCompletedByEndOfExperiment(job);
    } else {
      // TODO: Handle retries --- a job can have multiple attempts
      super.stop(e);
      jobHistoryManager.onJobFailed(job);
    }
  }

  @Override
  public void stop() {
    final ApplicationAttemptId attemptId = am.getApplicationAttemptId();

    try {
      am.unregisterAppAttempt();

      if (!isUnmanaged) {
        // Release the AM container if managed by RM
        rm.waitForState(am.getApplicationAttemptId(), RMAppAttemptState.FINISHING);
        LOG.log(Level.INFO, MessageFormat.format("Job {0} containers released!", attemptId));
        clusterState.getNM(masterNode).nodeHeartbeat(am.getApplicationAttemptId(), 1, ContainerState.COMPLETE);
      }

      rm.waitForState(attemptId, RMAppAttemptState.FINISHED);

      LOG.log(Level.INFO, MessageFormat.format(
          "Job " + job.getTraceJobId() + " FINISHED at {1}!", attemptId, clock.getInstant())
      );
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    super.stop();
    jobHistoryManager.onJobCompleted(job);
  }

  public abstract String getQueueName();

  public SimulatedJob getSimulatedJob() {
    return job;
  }
}
