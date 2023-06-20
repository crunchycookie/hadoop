package org.apache.hadoop.yarn.dtss.lifecycle;

/**
 * A lifecycle object that has already been initialized
 */
public abstract class InitializedLifeCycle extends LifeCycle {
  public InitializedLifeCycle() {
    lifeCycleState = lifeCycleState.transition(LifeCycleState.INITED);
  }

  @Override
  public void init() {
    // Do nothing
  }
}
