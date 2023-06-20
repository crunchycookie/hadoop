package org.apache.hadoop.yarn.dtss.stats;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.QuoteMode;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.yarn.dtss.exceptions.StateException;
import org.apache.hadoop.yarn.dtss.job.SimulatedJob;
import org.apache.hadoop.yarn.dtss.lifecycle.LifeCycle;
import org.apache.hadoop.yarn.dtss.metrics.MetricsConfiguration;
import org.apache.hadoop.yarn.dtss.metrics.job.JobHistory;
import org.apache.hadoop.yarn.dtss.time.SimulatedClock;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Supports the writing of overall job metrics.
 */
@InterfaceAudience.Private
public final class JobStats extends LifeCycle {
  private static final Logger LOG = Logger.getLogger(JobStats.class.getName());
  private static final DecimalFormat df2 = new DecimalFormat(".##");

  private final MetricsConfiguration metricsConfig;
  private final SimulatedClock clock;

  @Inject
  private JobStats(
      final MetricsConfiguration metricsConfig,
      final SimulatedClock clock) {
    this.metricsConfig = metricsConfig;
    this.clock = clock;
  }

  private static final class SimulatedJobWrapper {
    private JobHistory jobHistory;
    private SimulatedJob simulatedJob;

    private SimulatedJobWrapper(SimulatedJob sj, JobHistory jh) {
      this.simulatedJob = sj;
      this.jobHistory = jh;
    }
  }

  private final List<SimulatedJobWrapper> jobHistoryList = new LinkedList<>();

  public void appendJobHistory(final SimulatedJob job, final JobHistory jobHistory) {
    if (this.lifeCycleState.isDone()) {
      throw new StateException(
          "Cannot append job history for job " + job.getTraceJobId() +
              ", because JobStats is in state " + this.lifeCycleState);
    }

    final SimulatedJobWrapper sjw = new SimulatedJobWrapper(job, jobHistory);
    this.jobHistoryList.add(sjw);
  }

  private List<SimulatedJobWrapper> getSortedJobHistoryBySubmissionTime() {
    return jobHistoryList.stream()
        .sorted(Comparator.comparing(s -> {
          return s.jobHistory.getSubmitTimeSeconds() == null ? Long.MAX_VALUE : s.jobHistory.getSubmitTimeSeconds();
        }))
        .collect(Collectors.toList());
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    final List<SimulatedJobWrapper> submissionTimeSortedHistory = getSortedJobHistoryBySubmissionTime();

    sb.append("ApplicationID, NumContainers, EndState, SubmitTimeTS, StartTimeTS, EndTimeTS\n");
    for(final SimulatedJobWrapper sjw : submissionTimeSortedHistory) {
      sb.append(String.format("%" + 11 + "s", sjw.jobHistory.getApplicationId()) + ", ");
      sb.append(String.format("%" + 11 + "s", sjw.jobHistory.getQueue() + ", "));
      sb.append(String.format("%" + 13 + "s", sjw.simulatedJob.getContainers().size() + ", "));
      sb.append(String.format("%" + 11 + "s", sjw.jobHistory.getEndState() + ", "));
      sb.append(String.format("%" + 13 + "s", sjw.jobHistory.getSubmitTimeSeconds() + ", "));
      sb.append(String.format("%" + 12 + "s", sjw.jobHistory.getStartTimeSeconds() + ", "));
      sb.append(String.format("%" + 10 + "s", sjw.jobHistory.getEndTimeSeconds()));
      sb.append("\n");
    }
    return sb.toString();
  }

  public void writeResultsToTsv() {
    if (!metricsConfig.isMetricsOn() ||
        metricsConfig.getMetricsDirectory() == null ||
        metricsConfig.getMetricsDirectory().isEmpty()) {
      LOG.log(Level.INFO, "Metrics is not turned on, not writing results to TSV...");
      return;
    }

    final String jobStatsFilePathStr = metricsConfig.getMetricsFilePath("jobStatsFile.tsv");
    assert jobStatsFilePathStr != null;

    LOG.log(Level.INFO, "Writing job stats to " + jobStatsFilePathStr + "...");
    try (final CSVPrinter printer = new CSVPrinter(new FileWriter(jobStatsFilePathStr),
        CSVFormat.TDF.withEscape('\\').withQuoteMode(QuoteMode.NONE))
    ) {
      printer.printRecord(
          "SubmitTimeSecondsOffset",
          "TraceJobId",
          "Queue",
          "EndState",
          "StartTimeSecondsOffset",
          "EndTimeSecondsOffset",
          "Priority"
      );

      for (final SimulatedJobWrapper sjw : getSortedJobHistoryBySubmissionTime()) {
        printer.printRecord(
            getTimeFromSimulationStart(sjw.jobHistory.getSubmitTimeSeconds()),
            sjw.jobHistory.getTraceJobId(),
            sjw.jobHistory.getQueue(),
            sjw.jobHistory.getEndState(),
            getTimeFromSimulationStart(sjw.jobHistory.getStartTimeSeconds()),
            getTimeFromSimulationStart(sjw.jobHistory.getEndTimeSeconds()),
            sjw.jobHistory.getPriority()
        );
      }
    } catch (final IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    LOG.log(Level.INFO, "Done writing job stats!");
  }

  private String getTimeFromSimulationStart(@Nullable final Long seconds) {
    if (seconds == null) {
      return null;
    }

    return Long.toString(seconds - clock.getSimulationStartTime().getEpochSecond());
  }
}
