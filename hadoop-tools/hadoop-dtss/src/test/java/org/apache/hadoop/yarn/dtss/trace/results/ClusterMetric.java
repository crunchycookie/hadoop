package org.apache.hadoop.yarn.dtss.trace.results;

public enum ClusterMetric {
  CLUSTER_ALLOCATED_VCORES,
  CLUSTER_AVAILABLE_VCORES,
  CLUSTER_ALLOCATED_MEMORY,
  CLUSTER_AVAILABLE_MEMORY,
  CLUSTER_RUNNING_APPS,
  CLUSTER_RUNNING_CONTAINERS;

  public static String getClusterMetricFileName(final ClusterMetric metric) {
    switch (metric) {
      case CLUSTER_ALLOCATED_VCORES:
        return "variable.cluster.allocated.vcores.csv";
      case CLUSTER_AVAILABLE_VCORES:
        return "variable.cluster.available.vcores.csv";
      case CLUSTER_ALLOCATED_MEMORY:
        return "variable.cluster.allocated.memory.csv";
      case CLUSTER_AVAILABLE_MEMORY:
        return "variable.cluster.available.memory.csv";
      case CLUSTER_RUNNING_APPS:
        return "variable.running.application.csv";
      case CLUSTER_RUNNING_CONTAINERS:
        return "variable.running.container.csv";
      default:
        throw new IllegalArgumentException("Invalid metric!");
    }
  }
}
