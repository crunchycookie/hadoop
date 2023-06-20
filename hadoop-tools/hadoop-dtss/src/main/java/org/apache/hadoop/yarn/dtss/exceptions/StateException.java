package org.apache.hadoop.yarn.dtss.exceptions;

/**
 * The {@link Exception} thrown when a lifeCycleState machine is in an invalid lifeCycleState.
 */
public final class StateException extends RuntimeException {
  public StateException(final String message) {
    super(message);
  }

  public StateException(
      final String message, final Throwable throwable) {
    super(message, throwable);
  }
}
