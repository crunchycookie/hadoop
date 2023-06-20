package org.apache.hadoop.yarn.dtss.metrics.job;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.hadoop.yarn.dtss.job.ExperimentJobEndState;
import org.apache.hadoop.yarn.dtss.job.SimulatedJob;
import org.apache.hadoop.yarn.dtss.job.SimulatedJobMaster;
import org.apache.hadoop.yarn.dtss.job.SimulatedTask;
import org.apache.hadoop.yarn.dtss.lifecycle.LifeCycle;
import org.apache.hadoop.yarn.dtss.metrics.MetricsConfiguration;
import org.apache.hadoop.yarn.dtss.metrics.MetricsManager;
import org.apache.hadoop.yarn.dtss.stats.JobStats;
import org.apache.hadoop.yarn.dtss.time.Clock;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

/**
 * A singleton object that collects all job execution history
 * for a given trace in simulation.
 * Called upon job start, complete, and failed.
 */
@Singleton
public final class JobHistoryManager extends LifeCycle {
  private static final Logger LOG = Logger.getLogger(JobHistoryManager.class.getName());

  private final Clock clock;
  private final MetricsConfiguration metricsConfig;
  private final JobStats jobStats;

  private BufferedWriter jobRuntimeLogBW;

  private final Map<String, JobHistory> jobHistory = new HashMap<>();

  @Inject
  private JobHistoryManager(
      final MetricsConfiguration metricsConfig,
      final JobStats jobStats,
      final Clock clock) {
    this.clock = clock;
    this.metricsConfig = metricsConfig;
    this.jobStats = jobStats;
  }

  @Override
  public void init() {
    super.init();
    jobStats.init();
  }

  @Override
  public void start() {
    super.start();
    if (metricsConfig.isMetricsOn()) {
      try {
        final String jobRuntimeCsvFilePath = metricsConfig.getMetricsFilePath("jobruntime.csv");
        assert jobRuntimeCsvFilePath != null;

        // application running information
        jobRuntimeLogBW =
            new BufferedWriter(new OutputStreamWriter(new FileOutputStream(
                jobRuntimeCsvFilePath), StandardCharsets.UTF_8));
        jobRuntimeLogBW.write(
            "TraceJobID," +
                "real_submit_time_seconds," +
                "real_start_time_seconds," +
                "real_end_time_seconds," +
                "AppAttemptID," +
                "Queue," +
                "EndState," +
                "simulate_submit_time_seconds," +
                "simulate_start_time_seconds," +
                "simulate_end_time_seconds" +
                MetricsManager.EOL);

        jobRuntimeLogBW.flush();
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }

    jobStats.start();
  }

  /**
   * Called when a job has been read from the trace and its application master created.
   * @param job the mocked application master
   */
  public void onJobRead(final SimulatedJobMaster job) {
    jobHistory.put(job.getTraceJobId(), JobHistory.newInstance(job));
  }

  /**
   * Called when a job has been submitted to the RM.
   * @param job the submitted simulated job
   * @param app the RM representation of the job
   */
  public void onJobSubmitted(final SimulatedJob job, final RMApp app) {
    jobHistory.get(job.getTraceJobId()).jobSubmitted(clock.getInstant(), app);
  }

  /**
   * Called when a job has started.
   * @param job The started simulated job
   */
  public void onJobStarted(final SimulatedJob job) {
    jobHistory.get(job.getTraceJobId()).setStartTime(clock.getInstant());
  }

  /**
   * Called when a job completes. Metrics are logged if metrics are turned on.
   * @param job The completed simulated job.
   */
  public void onJobCompleted(final SimulatedJob job) {
    final JobHistory history = jobHistory.get(job.getTraceJobId());
    history.setEndTime(clock.getInstant(), ExperimentJobEndState.COMPLETED);
    logJobMetrics(job);
  }

  /**
   * Called when a job fails. Metrics are logged if metrics are turned on.
   * @param job The failed simulated job
   */
  public void onJobFailed(final SimulatedJob job) {
    final JobHistory history = jobHistory.get(job.getTraceJobId());
    history.setEndTime(null, ExperimentJobEndState.FAILED);
    logJobMetrics(job);
  }

  /**
   * Called on jobs for all jobs that are read but not completed/failed by the end of experiment.
   * @param job the simulated job
   */
  public void onJobNotCompletedByEndOfExperiment(final SimulatedJob job) {
    final JobHistory history = jobHistory.get(job.getTraceJobId());
    if (history.getSubmitTimeSeconds() == null) {
      history.setEndTime(null, ExperimentJobEndState.NOT_SUBMITTED);
    } else if (history.getStartTimeSeconds() == null) {
      history.setEndTime(null, ExperimentJobEndState.SUBMITTED);
    } else {
      history.setEndTime(null, ExperimentJobEndState.STARTED);
    }

    logJobMetrics(job);
  }

  /**
   * Write job metrics for a single finished job if metrics are enabled.
   * @param job The simulated completed job.
   */
  private void logJobMetrics(final SimulatedJob job) {
    if (!metricsConfig.isMetricsOn()) {
      return;
    }

    try {
      final JobHistory history = jobHistory.get(job.getTraceJobId());
      // Maintain job stats
      jobStats.appendJobHistory(job, history);

      // write job runtime information
      StringBuilder sb = new StringBuilder();
      sb.append(job.getTraceJobId()).append(",")
          .append(job.getSubmitTimeSeconds()).append(",")
          .append(emptyIfNull(job.getStartTimeSeconds())).append(",")
          .append(emptyIfNull(job.getEndTimeSeconds())).append(",")
          .append(emptyIfNull(history.getApplicationId())).append(",")
          .append(emptyIfNull(history.getQueue())).append(",")
          .append(emptyIfNull(history.getEndState())).append(",")
          .append(emptyIfNull(history.getSubmitTimeSeconds())).append(",")
          .append(emptyIfNull(history.getStartTimeSeconds())).append(",")
          .append(emptyIfNull(history.getEndTimeSeconds()));
      jobRuntimeLogBW.write(sb.toString() + MetricsManager.EOL);
      jobRuntimeLogBW.flush();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static String emptyIfNull(final Object o) {
    if (o == null) {
      return "";
    }

    return o.toString();
  }

  public JobStats getJobStats() {
    return jobStats;
  }

  @Override
  public void stop() {
    super.stop();

    if (jobRuntimeLogBW != null) {
      try {
        jobRuntimeLogBW.close();
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }

    if (metricsConfig.isMetricsOn()) {
      jobStats.stop();
    }
  }
}
