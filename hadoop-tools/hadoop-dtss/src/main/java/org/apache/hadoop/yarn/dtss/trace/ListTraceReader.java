package org.apache.hadoop.yarn.dtss.trace;

import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import com.google.inject.Injector;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.dtss.exceptions.EndOfExperimentNotificationException;
import org.apache.hadoop.yarn.dtss.job.SimulatedJob;
import org.apache.hadoop.yarn.dtss.job.SimulatedJobMaster;
import org.apache.hadoop.yarn.dtss.lifecycle.LifeCycleState;
import org.apache.hadoop.yarn.dtss.metrics.job.JobHistoryManager;
import org.apache.hadoop.yarn.dtss.time.Clock;

import java.time.Instant;
import java.util.*;
import java.util.logging.Logger;

/**
 * A trace reader that reads in a trace into memory as a list.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public abstract class ListTraceReader extends TraceReader {
  private static final Logger LOG = Logger.getLogger(ListTraceReader.class.getName());
  private List<SimulatedJob> jobs;
  private final Map<SimulatedJobMaster, UUID> jobMasters = new HashMap<>();

  protected ListTraceReader(final Injector injector,
                            final Clock clock,
                            final JobHistoryManager jobHistoryManager) {
    super(injector, clock, jobHistoryManager);
  }

  /**
   * Periodically checks the set of all created {@link SimulatedJobMaster}s
   * and see if they are all done. If so, trace jobs are done.
   * @return Whether or not the trace has completed
   */
  @Override
  public boolean areTraceJobsDone() {
    if (getState().isDone()) {
      return true;
    }

    if (getState() != LifeCycleState.STARTED) {
      return false;
    }

    final List<SimulatedJobMaster> toRemove = new ArrayList<>();

    for (final SimulatedJobMaster jobMaster : jobMasters.keySet()) {
      if (jobMaster.getState().isDone()) {
        toRemove.add(jobMaster);
      }
    }

    for (final SimulatedJobMaster jobMaster : toRemove) {
      jobMasters.remove(jobMaster);
    }

    return jobMasters.isEmpty();
  }

  @Override
  public void init() {
    jobs = Lists.newArrayList(parseJobs());
    super.init();
  }

  /**
   * Parse the jobs
   * @return An {@link Iterable} of {@link SimulatedJob}s
   */
  protected abstract Iterable<SimulatedJob> parseJobs();

  @Override
  public void start() {
    // Mark jobs as read and add them to the set of job masters to check.
    // Schedule jobs on the simulated clock based on their submission times.
    for (final SimulatedJob job : jobs) {
      final SimulatedJobMaster jobMaster = job.createJobMaster(injector);
      jobHistoryManager.onJobRead(jobMaster);
      jobMasters.put(jobMaster, null);
      clock.scheduleAbsoluteAlarm(Instant.ofEpochSecond(job.getSubmitTimeSeconds()), alarm -> {
        // Registers the job with the RM
        jobMaster.init();
      });
    }

    super.start();
  }

  @Override
  public void onStop() {
    for (final SimulatedJobMaster jobMaster : jobMasters.keySet()) {
      if (!jobMaster.getState().isDone()) {
        jobMaster.stop(new EndOfExperimentNotificationException());
      }
    }
  }
}
