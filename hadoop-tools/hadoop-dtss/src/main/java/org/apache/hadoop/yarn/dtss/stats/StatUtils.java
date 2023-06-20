package org.apache.hadoop.yarn.dtss.stats;

import org.apache.hadoop.classification.InterfaceAudience;

import java.util.Arrays;

/**
 * Supports computing statistics for cluster and job metrics.
 */
@InterfaceAudience.Private
public class StatUtils {
  double[] data;
  int size;

  public StatUtils(double[] data) {
    this.data = data;
    size = data.length;
  }

  double getVariance() {
    double mean = getMean();
    double temp = 0;
    for (double a : data)
      temp += (a - mean) * (a - mean);
    return temp / size;
  }

  double getStdDev() {
    return Math.sqrt(getVariance());
  }

  double getMean() {
    double sum = 0.0;
    for (double a : data)
      sum += a;
    return sum / size;
  }

  public double getMedian() {
    Arrays.sort(data);
    if (data.length % 2 == 0) {
      return (data[(data.length / 2) - 1] + data[data.length / 2]) / 2.0;
    }
    return data[data.length / 2];
  }
}