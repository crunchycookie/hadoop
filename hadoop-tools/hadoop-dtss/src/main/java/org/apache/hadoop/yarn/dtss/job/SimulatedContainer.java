package org.apache.hadoop.yarn.dtss.job;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;

import javax.annotation.Nullable;
import java.time.Instant;

/**
 * This is a simulated container allocated by the RM.
 */
final class SimulatedContainer implements Comparable<SimulatedContainer> {
  private Instant startTime = null;
  private Instant endTime = null;
  private final Container container;
  private boolean isComplete = false;

  static SimulatedContainer fromContainer(final Container container) {
    return new SimulatedContainer(container);
  }

  ContainerId getId() {
    return container.getId();
  }

  NodeId getNodeId() {
    return container.getNodeId();
  }

  Resource getResource() {
    return container.getResource();
  }

  int getPriority() {
    return container.getPriority().getPriority();
  }

  @Nullable
  public Instant getStartTime() {
    return startTime;
  }

  public void setStartTime(final Instant startTime) {
    this.startTime = startTime;
  }

  private SimulatedContainer(final Container container) {
    this.container = container;
  }

  @Override
  public int compareTo(final SimulatedContainer o) {
    return this.container.compareTo(o.container);
  }

  @Override
  public int hashCode() {
    return this.container.hashCode();
  }
}
