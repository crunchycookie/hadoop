package org.apache.hadoop.yarn.dtss.exceptions;

/**
 * The {@link Exception} thrown when an operation is not supported.
 */
public final class OperationNotSupportedException extends RuntimeException {
  public OperationNotSupportedException(final String message) {
    super(message);
  }

  public OperationNotSupportedException(
      final String message, final Throwable throwable) {
    super(message, throwable);
  }
}

