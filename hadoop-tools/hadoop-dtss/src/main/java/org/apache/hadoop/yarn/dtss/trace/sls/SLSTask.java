package org.apache.hadoop.yarn.dtss.trace.sls;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import com.google.gson.annotations.SerializedName;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.dtss.job.SimulatedTask;
import org.apache.hadoop.yarn.dtss.job.constants.TaskConstants;

/**
 * A simulated task implementation for an SLS job.
 * Serialized to and from JSON.
 */
public final class SLSTask extends SimulatedTask {

  public static final class Constants {
    public static final String CONTAINER_COUNT = "count";

    public static final String CONTAINER_HOST = "container.host";

    public static final String CONTAINER_START_MS = "container.start.ms";

    public static final String CONTAINER_END_MS = "container.end.ms";

    public static final String CONTAINER_DURATION_MS = "duration.ms";

    public static final String CONTAINER_PRIORITY = "container.priority";

    public static final String CONTAINER_TYPE = "container.type";

    public static final int DEFAULT_CONTAINER_COUNT = 1;

    public static final int DEFAULT_CONTAINER_PRIORITY = 20;

    public static final String UNKNOWN_CONTAINER_TYPE = "unknown";

    public static final String MAP_CONTAINER_TYPE = "map";

    public static final String REDUCE_CONTAINER_TYPE = "reduce";

    public static final String DEFAULT_CONTAINER_TYPE = UNKNOWN_CONTAINER_TYPE;

    private Constants() { }
  }

  // TODO: Support this
  // number of tasks, optional, the default value is 1
  @SerializedName(Constants.CONTAINER_COUNT)
  private Integer count;

  // TODO: Support this
  // Host the container asks for
  @SerializedName(Constants.CONTAINER_HOST)
  private String host;

  // TODO: Support this for arbitrary jobs
  // container start time, optional
  @SerializedName(Constants.CONTAINER_START_MS)
  private Long startMs;

  // TODO: Support this for arbitrary jobs
  // container finish time, optional
  @SerializedName(Constants.CONTAINER_END_MS)
  private Long endMs;

  // duration of the container, optional if start and end time is specified
  @SerializedName(Constants.CONTAINER_DURATION_MS)
  private Long durationMs;

  // priority of the container, optional, the default value is 20
  @SerializedName(Constants.CONTAINER_PRIORITY)
  private Integer priority;

  @SerializedName(Constants.CONTAINER_TYPE)
  private String containerType;

  @InterfaceStability.Unstable
  @VisibleForTesting
  public static SLSTask newTask(final long durationMs) {
    final SLSTask t = new SLSTask();
    t.durationMs = durationMs;
    return t;
  }

  public Integer getCount() {
    if (count == null) {
      return Constants.DEFAULT_CONTAINER_COUNT;
    }

    return count;
  }

  public String getHost() {
    return host;
  }

  @Override
  public Long getStartTimeSeconds() {
    return startMs;
  }

  @Override
  public Long getDurationSeconds() {
    if (startMs != null && endMs != null) {
      return (long) Math.ceil((endMs - startMs) / 1000.0);
    }

    return (long) Math.ceil(durationMs / 1000.0);
  }

  @Override
  public Integer getPriority() {
    if (priority == null) {
      return Constants.DEFAULT_CONTAINER_PRIORITY;
    }

    return priority;
  }

  @Override
  public String getTaskType() {
    return translateContainerType(containerType);
  }

  private static String translateContainerType(final String cType) {
    if (cType == null) {
      return TaskConstants.MAP_TASK_TYPE;
    }

    switch (cType) {
      case Constants.REDUCE_CONTAINER_TYPE:
        return TaskConstants.REDUCE_TASK_TYPE;
      case Constants.MAP_CONTAINER_TYPE:
        return TaskConstants.MAP_TASK_TYPE;
      default:
        return TaskConstants.UNKNOWN_TASK_TYPE;
    }
  }
}
