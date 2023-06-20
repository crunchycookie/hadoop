package org.apache.hadoop.yarn.dtss.job;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;

import java.util.ArrayList;
import java.util.List;

/**
 * A mocked wrapper for allocation responses from the RM.
 */
@InterfaceAudience.Private
final class SimulatedAllocateResponse {
  private final List<SimulatedContainer> containersAllocated = new ArrayList<>();
  private final List<ContainerStatus> containersCompleted = new ArrayList<>();

  static SimulatedAllocateResponse fromAllocateResponse(final AllocateResponse response) {
    return new SimulatedAllocateResponse(response);
  }

  private SimulatedAllocateResponse(final AllocateResponse response) {
    for (final Container container : response.getAllocatedContainers()) {
      containersAllocated.add(SimulatedContainer.fromContainer(container));
    }

    containersCompleted.addAll(response.getCompletedContainersStatuses());
  }

  public List<SimulatedContainer> getContainersAllocated() {
    return containersAllocated;
  }

  public List<ContainerStatus> getContainersCompleted() {
    return containersCompleted;
  }
}