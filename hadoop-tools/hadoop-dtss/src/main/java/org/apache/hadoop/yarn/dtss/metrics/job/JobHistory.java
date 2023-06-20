package org.apache.hadoop.yarn.dtss.metrics.job;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.dtss.job.ExperimentJobEndState;
import org.apache.hadoop.yarn.dtss.job.SimulatedJob;
import org.apache.hadoop.yarn.dtss.job.SimulatedJobMaster;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;

import java.time.Instant;

/**
 * Records the history of a job, including its state at the
 * end of a simulation experiment, its application ID, and
 * its submit, start, and end times.
 */
@InterfaceAudience.Private
public final class JobHistory {
  private final SimulatedJobMaster simulatedJobMaster;

  private ApplicationId appId = null;
  private Instant submitTime = null;
  private Instant startTime = null;
  private Instant endTime = null;

  private ExperimentJobEndState endState = ExperimentJobEndState.READ;

  public static JobHistory newInstance(final SimulatedJobMaster simulatedJobMaster) {
    return new JobHistory(simulatedJobMaster);
  }

  private JobHistory(final SimulatedJobMaster simulatedJob) {
    this.simulatedJobMaster = simulatedJob;
  }

  public SimulatedJob getSimulatedJob() {
    return simulatedJobMaster.getSimulatedJob();
  }

  public void jobSubmitted(final Instant submitTime, final RMApp app) {
    this.submitTime = submitTime;
    this.appId = app.getApplicationId();
  }

  public void setStartTime(final Instant instant) {
    this.startTime = instant;
  }

  public void setEndTime(final Instant instant, final ExperimentJobEndState state) {
    this.endState = state;
    this.endTime = instant;
  }

  public Long getSubmitTimeSeconds() {
    if (submitTime == null) {
      return null;
    }

    return submitTime.toEpochMilli() / 1000;
  }

  public Long getStartTimeSeconds() {
    if (startTime == null) {
      return null;
    }

    return startTime.toEpochMilli() / 1000;
  }

  public Long getEndTimeSeconds() {
    if (endTime == null) {
      return null;
    }

    return endTime.toEpochMilli() / 1000;
  }

  public ExperimentJobEndState getEndState() {
    return endState;
  }

  public String getQueue() {
    return simulatedJobMaster.getQueueName();
  }

  public ApplicationId getApplicationId() {
    return appId;
  }

  public boolean isJobCompleted() {
    return endState == ExperimentJobEndState.COMPLETED;
  }

  public String getTraceJobId() {
    return simulatedJobMaster.getTraceJobId();
  }

  public Integer getPriority() {
    return simulatedJobMaster.getSimulatedJob().getPriority();
  }
}
