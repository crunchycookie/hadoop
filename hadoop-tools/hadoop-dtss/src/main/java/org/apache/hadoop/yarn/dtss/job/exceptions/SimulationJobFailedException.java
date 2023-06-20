package org.apache.hadoop.yarn.dtss.job.exceptions;

/**
 * This exception should be thrown when a simulated job experiences a failure.
 */
public abstract class SimulationJobFailedException extends Exception {
  SimulationJobFailedException(final String message) {
    super(message);
  }
}
