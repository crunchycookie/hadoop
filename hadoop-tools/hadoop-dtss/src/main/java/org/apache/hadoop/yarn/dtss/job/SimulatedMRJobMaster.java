package org.apache.hadoop.yarn.dtss.job;

import com.google.inject.Injector;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.dtss.job.constants.TaskConstants;

import java.text.MessageFormat;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * The simulated ApplicationMaster for a simplified MapReduce job.
 */
public final class SimulatedMRJobMaster extends SimulatedJobMaster {
  private static final Logger LOG = Logger.getLogger(SimulatedMRJobMaster.class.getName());

  enum Phase {
    MAP,
    MAP_DEPLOYED,
    REDUCE,
    REDUCE_DEPLOYED,
    COMPLETED
  }

  private static final long MAP_PHASE_SETUP_SECONDS = 10;
  private static final long MR_AM_HEARTBEAT_SECONDS = 10;
  private static final long MR_JOB_SPINUP_TIME_SECONDS = 60;

  private final Map<ContainerId, SimulatedContainer> containersReceived = new HashMap<>();
  private final Set<ContainerId> runningContainers = new HashSet<>();
  private final List<SimulatedTask> mapTasks = new ArrayList<>();
  private final List<SimulatedTask> reduceTasks = new ArrayList<>();

  private int numCompletedTasksInPhase = 0;
  private boolean hasRequestedReduceContainers = false;

  private Phase phase = Phase.MAP;

  public SimulatedMRJobMaster(
      final Injector injector, final SimulatedJob job) {
    super(injector, job, MR_AM_HEARTBEAT_SECONDS, MR_JOB_SPINUP_TIME_SECONDS);
  }

  /**
   * Add tasks to the job and set up a call to initialize the job.
   * For implementors of a different type of ApplicationMaster,
   * remember to call super.init().
   */
  @Override
  public void init() {
    // Remember to call super.init() to set up alarms to invoke the start method
    super.init();
    for (final SimulatedTask task : job.getContainers()) {
      switch (task.getTaskType()) {
        case TaskConstants.MAP_TASK_TYPE:
          mapTasks.add(task);
          break;
        case TaskConstants.REDUCE_TASK_TYPE:
          reduceTasks.add(task);
          break;
        default:
          throw new NotImplementedException(MessageFormat.format(
              "Container type {0} is not yet supported!", task.getTaskType()));
      }
    }
  }

  /**
   * Begin by requesting map tasks to start the job.
   * For implementors of a different type of ApplicationMaster,
   * remember to call super.init().
   */
  @Override
  public void start() {
    super.start();
    if (stopScheduled) {
      return;
    }

    clock.scheduleAlarm(MAP_PHASE_SETUP_SECONDS, alarm -> {
      requestContainers(mapTasks);
    });
  }

  /**
   * Heartbeat from the RM. Check on the state of the job's tasks.
   * Finish when all map and reduce phase tasks are completed.
   * @param simulatedAllocateResponse The heartbeat response from the RM.
   */
  @Override
  void onHeartbeat(final SimulatedAllocateResponse simulatedAllocateResponse) {
    switch (phase) {
      case MAP:
        waitForContainersAndDeployTasks(
            simulatedAllocateResponse.getContainersAllocated(),
            simulatedAllocateResponse.getContainersCompleted(),
            mapTasks,
            Phase.MAP_DEPLOYED
        );
        break;
      case MAP_DEPLOYED:
        waitForContainersToComplete(
            simulatedAllocateResponse.getContainersCompleted(),
            mapTasks,
            Phase.REDUCE
        );
        break;
      case REDUCE:
        if (!hasRequestedReduceContainers) {
          requestContainers(reduceTasks);
          hasRequestedReduceContainers = true;
        }

        waitForContainersAndDeployTasks(
            simulatedAllocateResponse.getContainersAllocated(),
            simulatedAllocateResponse.getContainersCompleted(),
            reduceTasks,
            Phase.REDUCE_DEPLOYED);
        break;
      case REDUCE_DEPLOYED:
        waitForContainersToComplete(
            simulatedAllocateResponse.getContainersCompleted(),
            reduceTasks,
            Phase.COMPLETED
        );
        break;
      case COMPLETED:
        teardownAndScheduleStop();
        break;
      default:
        throw new UnsupportedOperationException("Phase not supported!");
    }
  }

  @Override
  public String getQueueName() {
    return job.getQueueName();
  }

  @Override
  JobSubmissionParameters getJobSubmissionParameters() {
    return JobSubmissionParameters.Builder.newInstance()
        .setIsUnmanaged(isUnmanaged)
        .setUser(job.getUser())
        .setQueue(job.getQueueName())
        .setName(job.getTraceJobId())
        .build();
  }

  private void waitForContainersAndDeployTasks(
      final List<SimulatedContainer> newContainersAllocated,
      final List<ContainerStatus> completedContainers,
      final List<SimulatedTask> tasks,
      final Phase transition) {
    for (final SimulatedContainer container : newContainersAllocated) {
      if (containersReceived.containsKey(container.getId())) {
        continue;
      }

      runContainer(container, tasks.get(containersReceived.size()));
      containersReceived.put(container.getId(), container);
      runningContainers.add(container.getId());
    }

    for (final ContainerStatus completedContainer : completedContainers) {
      if (!runningContainers.remove(completedContainer.getContainerId())) {
        LOG.log(Level.WARNING, MessageFormat.format(
            "Removed non-existent container {0}.", completedContainer.getContainerId()));
      } else {
        numCompletedTasksInPhase++;
      }
    }

    LOG.log(Level.FINE, containersReceived.size() + " allocated at " + clock.getInstant());
    if (containersReceived.size() < tasks.size()) {
      // Do not change state
      return;
    }

    phase = transition;
    LOG.log(Level.INFO, MessageFormat.format(
        "Job {0} transitioned to {1} phase!", getAttemptId(), transition));
  }

  private void waitForContainersToComplete(
      final List<ContainerStatus> completedContainers, final List<SimulatedTask> tasks, final Phase transition) {
    for (final ContainerStatus status : completedContainers) {
      if (!runningContainers.remove(status.getContainerId())) {
        LOG.log(Level.WARNING, MessageFormat.format(
            "Removed non-existent container {0}.", status.getContainerId()));
      } else {
        numCompletedTasksInPhase++;
      }
    }

    if (numCompletedTasksInPhase == tasks.size()) {
      if (runningContainers.size() > 0) {
        throw new RuntimeException("Expected to have no more running containers!");
      }

      LOG.log(Level.INFO, MessageFormat.format(
          "All tasks for phase {0} completed for job {1} at {2}! Entering {3} phase...",
          phase, getAttemptId(), clock.getInstant(), transition));

      phase = transition;
      numCompletedTasksInPhase = 0;
      containersReceived.clear();
      LOG.log(Level.INFO, MessageFormat.format(
          "Job {0} transitioned to {1} phase!", getAttemptId(), transition));
    }
  }
}
