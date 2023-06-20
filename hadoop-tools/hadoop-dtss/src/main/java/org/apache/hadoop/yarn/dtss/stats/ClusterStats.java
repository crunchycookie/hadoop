package org.apache.hadoop.yarn.dtss.stats;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.dtss.cluster.ClusterTopologyBuilder;
import org.apache.hadoop.yarn.dtss.cluster.ClusterTopologyProperties;
import org.apache.hadoop.yarn.dtss.config.parameters.runner.SimulationConfigPath;
import org.apache.hadoop.yarn.dtss.exceptions.StateException;
import org.apache.hadoop.yarn.dtss.lifecycle.LifeCycle;
import org.apache.hadoop.yarn.dtss.metrics.MetricsConfiguration;
import org.apache.hadoop.yarn.dtss.time.SimulatedClock;

import javax.inject.Inject;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Supports the writing of overall cluster metrics.
 */
@InterfaceAudience.Private
public final class ClusterStats extends LifeCycle {
  private static final Logger LOG = Logger.getLogger(ClusterStats.class.getName());

  private static long GB = 1024L;
  private static DecimalFormat df2 = new DecimalFormat(".##");

  private final String config;
  private final String scheduler;
  private final MetricsConfiguration metricsConfig;
  private final ClusterTopologyBuilder ctpBuilder;
  private final SimulatedClock clock;

  // Cluster stats
  private int racks;
  private int nodes;
  // Per container stats
  private long containerMemGB;
  private int containerCores;
  // Per node stats
  private long nodeMemGB;
  private int nodeCores;

  private double clusterMemUtilization;
  private double clusterCoreUtilization;

  @Inject
  private ClusterStats(
      @SimulationConfigPath final String configPath,
      final MetricsConfiguration metricsConfig,
      final Configuration yarnConf,
      final ClusterTopologyBuilder ctpBuilder,
      final SimulatedClock clock) {
    this.metricsConfig = metricsConfig;
    final String schedulerClass = yarnConf.get(YarnConfiguration.RM_SCHEDULER);
    this.config = configPath.substring(configPath.lastIndexOf('/') + 1);
    this.scheduler = schedulerClass.substring(
        schedulerClass.lastIndexOf('.') + 1, schedulerClass.lastIndexOf("Scheduler"));
    this.ctpBuilder = ctpBuilder;
    this.clock = clock;
  }

  @Override
  public void start() {
    super.start();
    try {
      final ClusterTopologyProperties ctp = ctpBuilder.getClusterTopology().getClusterTopologyProperties();
      this.racks = ctp.getClusterRacks();
      this.nodes = ctp.getClusterNodes();
      this.containerMemGB = ctp.getContainerMB() / GB;
      this.containerCores = ctp.getContainerVCores();
      // TODO: Handle non-homogeneous cluster
      // Assuming homogeneous cluster (same number of containers per node)
      int containersPerNode = ctp.getRackNodeContainers().get(0).get(0);
      this.nodeMemGB = containersPerNode * containerMemGB;
      this.nodeCores = containersPerNode * containerCores;
    } catch (final IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  public void setClusterMemUtilization(final List<Double> clusterMemTS) {
    if (lifeCycleState.isDone()) {
      throw new StateException("Unable to set cluster memory utilization, as the state is done!");
    }

    final StatUtils su = new StatUtils(clusterMemTS.stream().mapToDouble(Double::doubleValue).toArray());
    this.clusterMemUtilization = su.getMean();
  }

  public void setClusterCoreUtilization(final List<Double> clusterCoresTS) {
    if (lifeCycleState.isDone()) {
      throw new StateException("Unable to set cluster core utilization, as the state is done!");
    }

    final StatUtils su = new StatUtils(clusterCoresTS.stream().mapToDouble(Double::doubleValue).toArray());
    this.clusterCoreUtilization = su.getMean();
  }

  @Override
  public String toString() {
    return String.format("%" + 18 + "s", config) + ", " +
        String.format("%" + 10 + "s", scheduler) + ", " +
        String.format("%" + 6 + "s", racks) + ", " +
        String.format("%" + 6 + "s", nodes) + ", " +
        String.format("%" + 17 + "s", containerMemGB) + ", " +
        String.format("%" + 15 + "s", containerCores) + ", " +
        String.format("%" + 12 + "s", nodeMemGB) + ", " +
        String.format("%" + 10 + "s", nodeCores) + ", " +
        String.format("%" + 18 + "s", df2.format(clusterMemUtilization)) + ", " +
        String.format("%" + 18 + "s", df2.format(clusterCoreUtilization));
  }

  public void writeResultsToTsv() {
    if (!metricsConfig.isMetricsOn() || metricsConfig.getMetricsDirectory() == null ||
        metricsConfig.getMetricsDirectory().isEmpty()) {
      LOG.log(Level.INFO, "Metrics is not turned on, not writing results to TSV...");
      return;
    }

    final String clusterStatsFilePathStr = metricsConfig.getMetricsFilePath("clusterStatsFile.tsv");
    assert clusterStatsFilePathStr != null;

    LOG.log(Level.INFO, "Writing cluster stats to " + clusterStatsFilePathStr + "...");
    try (final CSVPrinter printer = new CSVPrinter(new FileWriter(clusterStatsFilePathStr), CSVFormat.TDF)) {
      printer.printRecord(
          "ConfigPath",
          "SchedulerName",
          "Racks",
          "NumNodesPerRack",
          "ContainerMemoryGB",
          "ContainerCoresGB",
          "NodeMemoryGB",
          "NodeCores",
          "ClusterMemUtilization",
          "ClusterCoreUtilization",
          "StartTimeEpochSeconds"
      );

      printer.printRecord(
          config,
          scheduler,
          racks,
          nodes,
          containerMemGB,
          containerCores,
          nodeMemGB,
          nodeCores,
          clusterMemUtilization,
          clusterCoreUtilization,
          clock.getSimulationStartTime().getEpochSecond()
      );
    } catch (final IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    LOG.log(Level.INFO, "Done writing cluster stats!");
  }
}
