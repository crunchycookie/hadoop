package org.apache.hadoop.yarn.dtss.metrics;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * A wrapper around the scheduler to wire metrics through to the simulation environment.
 * Please implement this in order to support new schedulers in the simulation environment.
 * For an example, please see {@link CapacitySchedulerWrapper}.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public interface SchedulerWrapper {

  void setMetricsManager(MetricsManager metricsManager);

  MetricsManager getMetricsManager();
}
