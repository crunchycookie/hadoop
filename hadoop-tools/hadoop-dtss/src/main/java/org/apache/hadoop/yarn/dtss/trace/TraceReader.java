package org.apache.hadoop.yarn.dtss.trace;


import com.google.inject.Injector;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.dtss.lifecycle.LifeCycle;
import org.apache.hadoop.yarn.dtss.metrics.job.JobHistoryManager;
import org.apache.hadoop.yarn.dtss.stats.JobStats;
import org.apache.hadoop.yarn.dtss.time.Clock;

/**
 * An abstraction for trace readers.
 * Works with the simulation environment through the function areTraceJobsDone
 * to determine whether or not a trace has completed.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public abstract class TraceReader extends LifeCycle {
  protected final Clock clock;
  protected final Injector injector;
  protected final JobHistoryManager jobHistoryManager;

  protected TraceReader(final Injector injector,
                        final Clock clock,
                        final JobHistoryManager jobHistoryManager) {
    this.injector = injector;
    this.clock = clock;
    this.jobHistoryManager = jobHistoryManager;
  }

  @Override
  public void init() {
    super.init();
    jobHistoryManager.init();
  }

  @Override
  public void start() {
    super.start();
    jobHistoryManager.start();
  }

  @Override
  public void stop() {
    super.stop();
    jobHistoryManager.stop();
  }

  public abstract void onStop();

  /**
   * Works with the simulation environment through the function areTraceJobsDone
   * to determine whether or not a trace has completed.
   * @return whether or not trace jobs are all completed
   */
  public abstract boolean areTraceJobsDone();
}
