package org.apache.hadoop.yarn.dtss.trace;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import org.apache.hadoop.yarn.dtss.exceptions.EndOfExperimentNotificationException;
import org.apache.hadoop.yarn.dtss.job.SimulatedJob;
import org.apache.hadoop.yarn.dtss.job.SimulatedJobMaster;
import org.apache.hadoop.yarn.dtss.lifecycle.LifeCycleState;
import org.apache.hadoop.yarn.dtss.metrics.job.JobHistoryManager;
import org.apache.hadoop.yarn.dtss.time.Clock;

import java.time.Instant;
import java.util.*;

@Singleton
public final class TestTraceReader extends TraceReader {
  private final TestTraceProvider testTraceProvider;
  private final Map<SimulatedJobMaster, UUID> jobMasters = new HashMap<>();
  private long currSubmitTimeSeconds = 0L;

  @Inject
  protected TestTraceReader(final Injector injector,
                            final Clock clock,
                            final JobHistoryManager jobHistoryManager,
                            final TestTraceProvider testTraceProvider) {
    super(injector, clock, jobHistoryManager);
    this.testTraceProvider = testTraceProvider;
  }

  @Override
  public void start() {
    while (testTraceProvider.hasNextJob()) {
      final SimulatedJob job = testTraceProvider.getNextJob();
      assert job.getSubmitTimeSeconds() != null;
      // Time must move forward
      assert currSubmitTimeSeconds <= job.getSubmitTimeSeconds();
      currSubmitTimeSeconds = job.getSubmitTimeSeconds();
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
}
