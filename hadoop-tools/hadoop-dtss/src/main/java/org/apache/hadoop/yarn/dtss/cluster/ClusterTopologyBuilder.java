package org.apache.hadoop.yarn.dtss.cluster;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.yarn.dtss.config.parameters.ClusterTopologyFilePath;

import java.io.IOException;

/**
 * An injected object to make the same
 * {@link ClusterTopology} object available to all.
 */
@Singleton
@InterfaceAudience.Private
public final class ClusterTopologyBuilder {
  private final String filePath;

  private ClusterTopology topology = null;

  @Inject
  private ClusterTopologyBuilder(@ClusterTopologyFilePath final String filePath) {
    this.filePath = filePath;
  }

  public ClusterTopology getClusterTopology() throws IOException {
    if (topology == null) {
      topology = ClusterTopology.parseTopologyFile(filePath);
    }

    return topology;
  }
}
