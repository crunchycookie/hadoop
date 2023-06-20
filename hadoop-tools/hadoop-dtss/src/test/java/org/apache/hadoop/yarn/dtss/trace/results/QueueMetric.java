package org.apache.hadoop.yarn.dtss.trace.results;

public enum QueueMetric {
  QUEUE_ALLOCATED_VCORES,
  QUEUE_AVAILABLE_VCORES,
  QUEUE_ALLOCATED_MEMORY,
  QUEUE_AVAILABLE_MEMORY;

  public static String getQueueMetricFileName(final String queueName, final QueueMetric queueMetric) {
    switch (queueMetric) {
      case QUEUE_ALLOCATED_VCORES:
        return "variable.queue." + queueName + ".allocated.vcores.csv";
      case QUEUE_AVAILABLE_VCORES:
        return "variable.queue." + queueName + ".available.vcores.csv";
      case QUEUE_ALLOCATED_MEMORY:
        return "variable.queue." + queueName + ".allocated.memory.csv";
      case QUEUE_AVAILABLE_MEMORY:
        return "variable.queue." + queueName + ".available.memory.csv";
      default:
        throw new IllegalArgumentException("Invalid queue metric!");
    }
  }
}
