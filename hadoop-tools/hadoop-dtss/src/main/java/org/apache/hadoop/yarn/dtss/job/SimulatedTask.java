package org.apache.hadoop.yarn.dtss.job;

import java.util.UUID;

/**
 * An abstract simulated task.
 * Implementors should implement this abstraction for their own tasks spun off from their jobs.
 */
public abstract class SimulatedTask {
  public abstract Long getStartTimeSeconds();

  public abstract Long getDurationSeconds();

  public abstract Integer getPriority();

  public abstract String getTaskType();
}
