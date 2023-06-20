package org.apache.hadoop.yarn.dtss.cluster;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.util.List;

/**
 * Represents the cluster topology as a 2D array.
 * Each row in the 2D array represents a rack, and
 * each entry in each row represents a machine.
 * The value of each entry represents the number
 * of containers available on the machine.
 * Serializes to and from JSON.
 * Also contains information on how a container is defined.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public final class ClusterTopologyProperties {
  private List<List<Integer>> rackNodeContainers;
  private int containerMB;
  private int containerVCores;
  private int containerCount;

  private ClusterTopologyProperties() {
  }

  @InterfaceAudience.Private
  @VisibleForTesting
  public ClusterTopologyProperties(
      final List<List<Integer>> rackNodeContainers,
      final int containerMB,
      final int containerVCores
  ) {
    this.rackNodeContainers = rackNodeContainers;
    this.containerMB = containerMB;
    this.containerVCores = containerVCores;
    int count = 0;
    for (final List<Integer> machineOnRack : rackNodeContainers) {
      for (final int containersOnMachine : machineOnRack) {
        count += containersOnMachine;
      }
    }

    this.containerCount = count;
  }

  public int getTotalContainers() {
    return containerCount;
  }

  public List<List<Integer>> getRackNodeContainers() {
    return rackNodeContainers;
  }

  public int getClusterRacks() {
    return rackNodeContainers.size();
  }

  public int getClusterNodes() {
    return rackNodeContainers.stream().map(List::size).mapToInt(Integer::intValue).sum();
  }

  public int getClusterContainers() {
    return rackNodeContainers.stream().flatMapToInt(l->l.stream().mapToInt(Integer::new)).sum();
  }

  public void setContainerMB(int containerMB) {
    this.containerMB = containerMB;
  }

  public int getContainerMB() {
    return containerMB;
  }

  public void setContainerVCores(int containerVCores) {
    this.containerVCores = containerVCores;
  }

  public int getContainerVCores() {
    return containerVCores;
  }
}
