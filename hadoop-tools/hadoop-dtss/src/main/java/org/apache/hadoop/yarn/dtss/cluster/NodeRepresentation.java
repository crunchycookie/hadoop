package org.apache.hadoop.yarn.dtss.cluster;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Represents a mocked node in the cluster.
 * Stores its host name and the number of containers registered on the node.
 */
@InterfaceAudience.Private
public final class NodeRepresentation {
  private final int containersOnNode;
  private final String nodeHostName;

  NodeRepresentation(final String nodeHostName, final int containersOnNode) {
    this.nodeHostName = nodeHostName;
    this.containersOnNode = containersOnNode;
  }

  public int getContainersOnNode() {
    return containersOnNode;
  }

  public String getNodeHostName() {
    return nodeHostName;
  }
}
