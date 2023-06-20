package org.apache.hadoop.yarn.dtss.job;

/**
 * The end states of jobs at the end of a simulation experiment.
 */
public enum ExperimentJobEndState {
  READ,
  NOT_SUBMITTED,
  SUBMITTED,
  STARTED,
  COMPLETED,
  CANCELLED,
  FAILED,
}
