package org.apache.hadoop.yarn.dtss.trace.sls;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import com.google.gson.annotations.SerializedName;
import com.google.inject.Injector;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.dtss.job.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A simulated job implementation for an SLS job.
 * Serialized to and from JSON.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public final class SLSJob extends SimulatedJob {

  public static class Constants {
    public static final String JOB_TYPE = "am.type";

    public static final String JOB_ID = "job.id";

    public static final String JOB_USER = "job.user";

    public static final String JOB_SUBMIT_MS = "job.submit.ms";

    public static final String JOB_START_MS = "job.start.ms";

    public static final String JOB_END_MS = "job.end.ms";

    public static final String JOB_QUEUE_NAME = "job.queue.name";

    public static final String JOB_PRIORITY = "job.priority";

    public static final String JOB_TASKS = "job.tasks";

    public static final String MAPREDUCE_JOB_TYPE = "mapreduce";

    public static final String DEFAULT_JOB_TYPE = MAPREDUCE_JOB_TYPE;

    public static final String DEFAULT_JOB_USER = "default";

    private Constants() {
    }
  }

  @VisibleForTesting
  public static class Builder {
    // Type of AM, optional, the default value is "mapreduce"
    @SerializedName(Constants.JOB_TYPE)
    private String jobType;

    // The job id used to track the job, optional.
    // The default value, an zero-based integer increasing with number of jobs,
    // is used if this is not specified or job.count > 1
    @SerializedName(Constants.JOB_ID)
    private String jobId;

    // User, optional, the default value is "default"
    @SerializedName(Constants.JOB_USER)
    private String jobUser;

    @SerializedName(Constants.JOB_SUBMIT_MS)
    private Long submitTimeMs;

    @SerializedName(Constants.JOB_START_MS)
    private Long startTimeMs;

    @SerializedName(Constants.JOB_END_MS)
    private Long endTimeMs;

    // the queue job will be submitted to
    @SerializedName(Constants.JOB_QUEUE_NAME)
    private String queueName;

    @SerializedName(Constants.JOB_PRIORITY)
    private Integer priority = 20;

    @SerializedName(Constants.JOB_TASKS)
    private List<SLSTask> tasks;

    public SLSJob build() {
      return new SLSJob(this);
    }

    public Builder setStartTimeMs(final Long startTimeMs) {
      this.startTimeMs = startTimeMs;
      return this;
    }

    public Builder setEndTimeMs(final Long endTimeMs) {
      this.endTimeMs = endTimeMs;
      return this;
    }

    public Builder setPriority(final Integer priority) {
      this.priority = priority;
      return this;
    }

    public Builder setJobId(final String jobId) {
      this.jobId = jobId;
      return this;
    }

    public Builder setJobUser(final String jobUser) {
      this.jobUser = jobUser;
      return this;
    }

    public Builder setJobType(final String jobType) {
      this.jobType = jobType;
      return this;
    }

    public Builder setSubmitTimeMs(final Long submitTimeMs) {
      this.submitTimeMs = submitTimeMs;
      return this;
    }

    public Builder setTasks(final List<SLSTask> tasks) {
      this.tasks = tasks;
      return this;
    }

    public Builder setQueueName(final String queueName) {
      this.queueName = queueName;
      return this;
    }

    public static Builder newBuilder() {
      return new Builder();
    }
  }

  private final String jobType;
  private final String jobId;
  private final String jobUser;
  private final Long submitTimeSeconds;
  private final Long startTimeSeconds;
  private final Long endTimeSeconds;
  private final String queueName;
  private final Integer priority;
  private final List<SimulatedTask> containers;

  private SLSJob(final Builder builder) {
    this.jobType = builder.jobType == null ? Constants.DEFAULT_JOB_TYPE : builder.jobType;
    this.jobId = builder.jobId;
    this.jobUser = builder.jobUser == null ? Constants.DEFAULT_JOB_USER : builder.jobUser;
    if (builder.submitTimeMs == null) {
      assert builder.startTimeMs != null;
      this.submitTimeSeconds = builder.startTimeMs / 1000;
    } else {
      this.submitTimeSeconds = builder.submitTimeMs / 1000;
    }

    if (builder.startTimeMs != null) {
      this.startTimeSeconds = builder.startTimeMs / 1000;
    } else {
      this.startTimeSeconds = null;
    }

    if (builder.endTimeMs != null) {
      this.endTimeSeconds = builder.endTimeMs / 1000;
    } else {
      this.endTimeSeconds = null;
    }

    this.queueName = builder.queueName;
    this.priority = builder.priority;
    this.containers = builder.tasks == null ? Collections.emptyList() : new ArrayList<>(builder.tasks);
  }

  @Override
  public Long getSubmitTimeSeconds() {
    return submitTimeSeconds;
  }

  @Override
  public Long getStartTimeSeconds() {
    return startTimeSeconds;
  }

  @Override
  public Long getEndTimeSeconds() {
    return endTimeSeconds;
  }

  @Override
  public List<SimulatedTask> getContainers() {
    return containers;
  }

  @Override
  public String getQueueName() {
    return queueName;
  }

  @Override
  public Integer getPriority() {
    return priority;
  }

  @Override
  public String getUser() {
    return jobUser;
  }

  @Override
  public String getTraceJobId() {
    return jobId;
  }

  @Override
  public String getJobName() {
    return getTraceJobId();
  }

  @Override
  public SimulatedJobMaster createJobMaster(final Injector injector) {
    switch (jobType) {
      case Constants.MAPREDUCE_JOB_TYPE:
      default:
        return new SimulatedMRJobMaster(injector, this);
    }
  }
}
