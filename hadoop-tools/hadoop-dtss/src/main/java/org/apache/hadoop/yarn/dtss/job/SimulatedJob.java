package org.apache.hadoop.yarn.dtss.job;

import com.google.inject.Injector;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.util.List;

/**
 * This is the abstraction for a simulated job.
 * Users should implement this class when reading their own custom trace,
 * or convert the read-in trace into a class implementing this abstraction.
 * An example for this class is {@link org.apache.hadoop.yarn.dtss.trace.sls.SLSJob}.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public abstract class SimulatedJob {

  public abstract String getTraceJobId();

  public abstract String getUser();

  public abstract String getJobName();

  public abstract String getQueueName();

  public abstract Integer getPriority();

  public abstract List<SimulatedTask> getContainers();

  public abstract SimulatedJobMaster createJobMaster(Injector injector);

  public abstract Long getSubmitTimeSeconds();

  public abstract Long getStartTimeSeconds();

  public abstract Long getEndTimeSeconds();
}
