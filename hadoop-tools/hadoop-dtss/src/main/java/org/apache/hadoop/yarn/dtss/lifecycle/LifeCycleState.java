package org.apache.hadoop.yarn.dtss.lifecycle;


import org.apache.hadoop.yarn.dtss.exceptions.StateException;

import java.text.MessageFormat;

/**
 * Tracks the life cycle of an object.
 * Used for objects implementing {@link LifeCycle}.
 */
public enum LifeCycleState {
  NOT_INITED,
  INITED,
  STARTED,
  STOPPED,
  FAILED,
  CANCELLED,
  UNKNOWN; // Should not enter this state!

  public LifeCycleState transition(final LifeCycleState to) {
    if (isValidTransition(to)) {
      return to;
    }

    throw new StateException(
        MessageFormat.format("Invalid lifeCycleState transition from {0} to {1}.", this, to));
  }

  public boolean isValidTransition(final LifeCycleState to) {
    if (this == to) {
      return true;
    }

    switch (this) {
      case NOT_INITED:
        switch (to) {
          case STARTED:
            return false;
          default:
            return true;
        }
      case INITED:
        switch (to) {
          case NOT_INITED:
            return false;
          default:
            return true;
        }
      case STARTED:
        switch (to) {
          case NOT_INITED:
          case INITED:
            return false;
          default:
            return true;
        }
      case STOPPED:
      case FAILED:
      case CANCELLED:
        return false;
      default:
        throw new StateException(String.format(
            "Invalid lifecycle lifeCycleState %s.", this));
    }
  }

  public boolean isDone() {
    return this == STOPPED || this == FAILED || this == CANCELLED;
  }
}

