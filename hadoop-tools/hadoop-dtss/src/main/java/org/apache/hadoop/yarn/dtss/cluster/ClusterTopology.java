package org.apache.hadoop.yarn.dtss.cluster;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;

/**
 * Container for {@link ClusterTopologyProperties}, which describes the topology of the cluster,
 * and for the {@link NodeRepresentation}s of each mocked node in the cluster,
 * which contains information on their hostnames and containers.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class ClusterTopology {
  private static final Logger LOG = Logger.getLogger(ClusterTopology.class.getName());
  // The set of nodes
  private final Set<NodeRepresentation> nodeSet;
  // The cluster topology represented as a 2D array
  private final ClusterTopologyProperties topologyProperties;

  private ClusterTopology(
      final Set<NodeRepresentation> nodeSet,
      final ClusterTopologyProperties topologyProperties) {
    this.nodeSet = nodeSet;
    this.topologyProperties = topologyProperties;
  }

  /**
   * Writes the cluster topology as JSON.
   * @param topologyProperties The shape of the cluster topology
   * @param filePath The file path to write to
   * @throws IOException
   */
  @VisibleForTesting
  public static void writeClusterTopologyFile(
      final ClusterTopologyProperties topologyProperties,
      final String filePath
  ) throws IOException {
    final Gson gson = new Gson();
    final FileWriter writer = new FileWriter(filePath);
    gson.toJson(topologyProperties, writer);
    writer.close();
  }

  /**
   * Parses the cluster topology file from JSON.
   * @param clusterTopologyFilePath The path to the topology file
   * @return The {@link ClusterTopology} object
   * @throws IOException
   */
  static ClusterTopology parseTopologyFile(
      final String clusterTopologyFilePath) throws IOException {
    // Read topology property file
    final Gson gson = new Gson();
    final ClusterTopologyProperties topologyProperties = gson.fromJson(
        new FileReader(clusterTopologyFilePath), ClusterTopologyProperties.class);

    // Make container MB and Cores configurable - not necessarily as large as the YarnConfig
    // Also make sure they are not 0 - fall back to default Yarn values
    topologyProperties.setContainerMB(Math.max(topologyProperties.getContainerMB(),
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB));
    topologyProperties.setContainerVCores(Math.max(topologyProperties.getContainerVCores(),
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES));

    LOG.info("Cluster Racks: "+ topologyProperties.getClusterRacks() +
        " Containers: " + topologyProperties.getClusterContainers() +
        " Each ContainerMB: " + topologyProperties.getContainerMB() +
        " Each ContainerVCores: " + topologyProperties.getContainerVCores());

    final Set<NodeRepresentation> nodeSet = new HashSet<>();
    // Create NodeRepresentations per Rack with specific number of containers
    for (int rackId = 0; rackId < topologyProperties.getRackNodeContainers().size(); rackId++) {
      for (int nodeId = 0; nodeId < topologyProperties.getRackNodeContainers().get(rackId).size(); nodeId++) {
        final int containersOnNode = topologyProperties.getRackNodeContainers().get(rackId).get(nodeId);
        final String nodeHostName = "host" + ((rackId * topologyProperties.getRackNodeContainers().get(rackId).size()) + nodeId) + ":12345";
        final NodeRepresentation nodeRep = new NodeRepresentation(nodeHostName, containersOnNode);
        nodeSet.add(nodeRep);
      }
    }

    return new ClusterTopology(nodeSet, topologyProperties);
  }

  public ClusterTopologyProperties getClusterTopologyProperties() {
    return this.topologyProperties;
  }

  public int getTotalContainers() {
    return this.topologyProperties.getClusterContainers();
  }

  public int getContainerVCores() {
    return topologyProperties.getContainerVCores();
  }

  public int getContainerMB() {
    return topologyProperties.getContainerMB();
  }

  public Set<NodeRepresentation> getNodeSet() {
    return nodeSet;
  }
}
