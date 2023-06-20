package org.apache.hadoop.yarn.dtss.lifecycle;

/**
 * Represents an object that has a start/stop lifecycle.
 */
public abstract class LifeCycle {
  protected LifeCycleState lifeCycleState = LifeCycleState.NOT_INITED;

  /**
   * Initializes the lifecycle object.
   */
  public void init() {
    lifeCycleState = lifeCycleState.transition(LifeCycleState.INITED);
  }

  /**
   * The entry point of the lifecycle object.
   */
  public void start() {
    lifeCycleState = lifeCycleState.transition(LifeCycleState.STARTED);
  }

  /**
   * Stop due to failure.
   * @param e the Exception
   */
  public void stop(final Exception e) {
    lifeCycleState = lifeCycleState.transition(LifeCycleState.FAILED);
  }

  /**
   * The exit point of the lifecycle object.
   */
  public void stop() {
    lifeCycleState = lifeCycleState.transition(LifeCycleState.STOPPED);
  }

  /**
   * Directly transition to state.
   * @param state the state
   */
  public void transition(final LifeCycleState state) {
    lifeCycleState = lifeCycleState.transition(state);
  }

  /**
   * @return the lifeCycleState of the lifecycle object
   */
  public LifeCycleState getState() {
    return lifeCycleState;
  }
}
