package org.apache.hadoop.yarn.dtss;


import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import com.google.gson.GsonBuilder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.dtss.cluster.ClusterState;
import org.apache.hadoop.yarn.dtss.cluster.ClusterTopology;
import org.apache.hadoop.yarn.dtss.cluster.ClusterTopologyBuilder;
import org.apache.hadoop.yarn.dtss.cluster.NodeRepresentation;
import org.apache.hadoop.yarn.dtss.config.parameters.IsUnmanaged;
import org.apache.hadoop.yarn.dtss.lifecycle.LifeCycleState;
import org.apache.hadoop.yarn.dtss.metrics.CapacitySchedulerWrapper;
import org.apache.hadoop.yarn.dtss.metrics.FairSchedulerWrapper;
import org.apache.hadoop.yarn.dtss.metrics.MetricsConfiguration;
import org.apache.hadoop.yarn.dtss.nm.MockNM;
import org.apache.hadoop.yarn.dtss.rm.MockRM;
import org.apache.hadoop.yarn.dtss.time.Clock;
import org.apache.hadoop.yarn.dtss.trace.TraceReader;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;

import java.io.FileWriter;
import java.io.IOException;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Launcher class for the discrete event-driven simulation.
 */
@InterfaceAudience.Private
final class Launcher {
  // How often we should check traces
  private static final long TRACE_CHECK_SECONDS = 60;

  // How often heartbeats between NMs and RMs should occur
  private static final long MIN_RM_NM_HEARTBEAT_INTERVAL_SECONDS = 10;

  private static final Logger LOG = Logger.getLogger(Launcher.class.getName());

  private Launcher() {
  }

  /**
   * Reads the YARN configuration and start the simulations.
   * @param configuration The combined simulation configuration
   */
  @VisibleForTesting
  public static void launch(final Module configuration) {
    final Injector injector = Guice.createInjector(configuration);

    final Configuration yarnConf = injector.getInstance(Configuration.class);
    final String schedulerClass = yarnConf.get(YarnConfiguration.RM_SCHEDULER);
    LOG.log(Level.INFO, "RM scheduler class: " + schedulerClass);
    boolean isUnmanaged = injector.getInstance(Key.get(Boolean.class, IsUnmanaged.class));
    if (isUnmanaged) {
      LOG.log(Level.INFO, "Using unmanaged AMs.");
    } else {
      LOG.log(Level.INFO, "Using managed AMs.");
    }

    // Only these schedulers are supported.
    // Overwrite the scheduler class with the wrapper class.
    try {
      if (Class.forName(schedulerClass) == CapacityScheduler.class) {
        yarnConf.set(YarnConfiguration.RM_SCHEDULER,
            CapacitySchedulerWrapper.class.getName());
      } else if(Class.forName(schedulerClass) == FairScheduler.class) {
        yarnConf.set(YarnConfiguration.RM_SCHEDULER,
            FairSchedulerWrapper.class.getName());
      } else {
        throw new YarnException(schedulerClass + " is not supported yet.");
      }
    } catch(Exception e) {
      throw new RuntimeException(e);
    }

    final Clock clock = injector.getInstance(Clock.class);
    // RM is created here
    final MockRM rm = injector.getInstance(MockRM.class);

    // Initialize the cluster
    final ClusterTopologyBuilder clusterTopologyBuilder = injector.getInstance(ClusterTopologyBuilder.class);
    final ClusterState clusterState = injector.getInstance(ClusterState.class);
    final ClusterTopology clusterTopology;
    final TraceReader traceReader = injector.getInstance(TraceReader.class);

    try {
      clusterTopology = clusterTopologyBuilder.getClusterTopology();

      // Start
      clock.start();
      rm.start();
      final UUID nmHeartbeatAlarmId = registerNodes(yarnConf, clusterTopology, clusterState, rm, clock);

      // Start trace-reading and periodic checking
      traceReader.init();
      traceReader.start();

      registerTraceCheckAlarm(clock, traceReader, clusterState, nmHeartbeatAlarmId);

      LifeCycleState clockState = clock.pollNextAlarm();

      // Run until either no more jobs/events, or until the end time is reached.
      while (shouldContinueRunning(clockState)) {
        clockState = clock.pollNextAlarm();
      }

      traceReader.onStop();
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    } finally {
      traceReader.stop();
      rm.stop();
      clock.stop();
    }

    // Write metric results
    // By now, the metrics manager has already stopped
    if (rm.getMetricsManager().isMetricsOn()) {
      assert rm.getMetricsManager().getState() == LifeCycleState.STOPPED;
      rm.getMetricsManager().writeResults();
      final CapacitySchedulerConfiguration capacitySchedulerConf;
      if (rm.getResourceScheduler() instanceof CapacityScheduler) {
        capacitySchedulerConf = ((CapacityScheduler) rm.getResourceScheduler()).getConfiguration();
      } else {
        capacitySchedulerConf = null;
      }
      try {
        writeConfigs(rm.getMetricsManager().getMetricsConfig(), yarnConf, capacitySchedulerConf, configuration);
      } catch (final IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
  }

  /**
   * Registers a periodic alarm on the clock that periodically checks
   * if all jobs in the trace are completed by checking with the {@link TraceReader}.
   * Unregister alarms if completed.
   * @param clock The clock
   * @param traceReader The trace reader
   * @param clusterState The current cluster state to unregister NMs
   * @param nmHeartbeatAlarmId The heartbeat ID for periodic NM-RM heartbeats
   */
  private static void registerTraceCheckAlarm(final Clock clock,
                                              final TraceReader traceReader,
                                              final ClusterState clusterState,
                                              final UUID nmHeartbeatAlarmId) {
    clock.schedulePeriodicAlarm(TRACE_CHECK_SECONDS, periodicClientAlarm -> {
      if (traceReader.areTraceJobsDone()) {
        LOG.log(Level.INFO, "Traces completed at " + clock.getInstant() + "!");
        clock.unschedulePeriodicAlarm(nmHeartbeatAlarmId);
        clock.unschedulePeriodicAlarm(periodicClientAlarm.getPeriodicAlarmId());
        for (final MockNM nm : clusterState.getNMs()) {
          try {
            nm.unRegisterNode();
          } catch(final Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
          }
        }

        clock.scheduleShutdown(1);
      }
    });
  }

  /**
   * Registers NMs wth the RM and schedule periodic heartbeats for each NM.
   * @param conf The YARN configuration
   * @param clusterTopology The cluster topology
   * @param clusterState The state of the cluster
   * @param rm The RM
   * @param clock The discrete-event clock
   * @return The periodic heartbeat alarm ID
   * @throws Exception
   */
  private static UUID registerNodes(final Configuration conf,
                                    final ClusterTopology clusterTopology,
                                    final ClusterState clusterState,
                                    final MockRM rm,
                                    final Clock clock) throws Exception {
    final long heartbeatIntervalMs = conf.getLong(
        YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS,
        YarnConfiguration.DEFAULT_RM_NM_HEARTBEAT_INTERVAL_MS);

    final long heartbeatIntervalSeconds = Math.max(
        MIN_RM_NM_HEARTBEAT_INTERVAL_SECONDS, (long) Math.ceil(heartbeatIntervalMs / 1000.0));

    for (final NodeRepresentation nodeRepresentation : clusterTopology.getNodeSet()) {
      final MockNM nm = new MockNM(nodeRepresentation.getNodeHostName(),
          nodeRepresentation.getContainersOnNode() * clusterTopology.getContainerMB(),
          nodeRepresentation.getContainersOnNode() * clusterTopology.getContainerVCores(),
          rm.getResourceTrackerService());

      clusterState.addNM(nm);
      nm.registerNode();
      nm.nodeHeartbeat(true);
    }

    LOG.log(Level.INFO, "NM heartbeats every " + heartbeatIntervalSeconds + " seconds.");

    return clock.schedulePeriodicAlarm(heartbeatIntervalSeconds, alarm -> {
      clusterState.heartbeatAll();
    });
  }

  private static boolean shouldContinueRunning(final LifeCycleState clockState) {
    return clockState != LifeCycleState.STOPPED;
  }

  /**
   * Write the new configs used for running simulations for diagnostic purposes.
   * @param metricsConfig The metric configuration
   * @param yarnConf The YARN configuration
   * @param capacitySchedulerConf The CapacityScheduler configuration
   * @param module the simulation configuration
   * @throws IOException
   */
  private static void writeConfigs(
      final MetricsConfiguration metricsConfig,
      final Configuration yarnConf,
      final CapacitySchedulerConfiguration capacitySchedulerConf,
      final Module module) throws IOException {
    final String yarnConfPath = metricsConfig.getMetricsFilePath("yarn-conf.xml");
    assert yarnConfPath != null;
    try (final FileWriter fw = new FileWriter(yarnConfPath)) {
      yarnConf.writeXml(fw);
    }

    if (capacitySchedulerConf != null) {
      final String capacityConfPath = metricsConfig.getMetricsFilePath("capacity-scheduler.xml");
      assert capacityConfPath != null;
      try (final FileWriter fw = new FileWriter(capacityConfPath)) {
        capacitySchedulerConf.writeXml(fw);
      }
    }

    final String simConfPath = metricsConfig.getMetricsFilePath("sim-conf.json");
    assert simConfPath != null;
    try (final FileWriter writer = new FileWriter(simConfPath)) {
      new GsonBuilder().setPrettyPrinting().create().toJson(module, writer);
    }
  }
}
