package org.apache.hadoop.yarn.dtss.config;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.inject.AbstractModule;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.util.Providers;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.dtss.config.parameters.*;
import org.apache.hadoop.yarn.dtss.time.Clock;
import org.apache.hadoop.yarn.dtss.time.SimulatedClock;
import org.apache.hadoop.yarn.dtss.trace.sls.config.SLSTraceModule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;

import java.io.FileReader;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The module configuration for running discrete event-based simulation.
 * Serialized from JSON.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public final class SimulatorModule extends AbstractModule {
  public static final class Constants {
    public static final String TRACE_TYPE = "traceType";

    final static HashMap<String, Class<? extends AbstractModule>> TRACE_MODULE_MAP =
        new HashMap<String, Class<? extends AbstractModule>>() {{
          put(SLSTraceModule.TRACE_TYPE, SLSTraceModule.class);
        }};
  }

  private static final Logger LOG = Logger.getLogger(SimulatorModule.class.getName());

  private static final Gson GSON = new Gson();

  // The ordering policy to use
  private String scheduler = CapacitySchedulerConfiguration.FIFO_APP_ORDERING_POLICY;

  private String clusterTopologyFilePath;

  private Long randomSeed = null;

  private boolean isMetricsOn = false;

  private int metricsTimerWindowSize = 100;

  private int metricsRecordIntervalSeconds = 5;

  private String metricsOutputDirectory = null;

  private Long simulationTraceStartTime = 0L;

  private Long simulationDurationMinutes = null;

  private String simulatorLogLevel = Level.INFO.getName();

  private boolean isUnmanaged = true;

  // Initialized by traceConfig, available to be injected in test
  private AbstractModule traceModule = null;

  private Map<String, String> traceConfig = new HashMap<>();

  private List<String> metricsSet = new ArrayList<>();

  /**
   * @return a new {@link Reader}
   */
  public static Reader newReader() {
    return new SimulatorModule().new Reader();
  }

  private SimulatorModule() {
  }

  @InterfaceAudience.Private
  @VisibleForTesting
  public static SimulatorModule newModule(
      final long randomSeed,
      final long simulationDurationMinutes,
      final String clusterTopologyFilePath,
      final String metricsOutputDirectory,
      final AbstractModule traceModule,
      final boolean isUnmanaged,
      final List<String> metricsSet
  ){
    final SimulatorModule m = new SimulatorModule();
    m.randomSeed = randomSeed;
    m.simulationDurationMinutes = simulationDurationMinutes;
    m.clusterTopologyFilePath = clusterTopologyFilePath;
    m.metricsOutputDirectory = metricsOutputDirectory;
    m.metricsSet = metricsSet;
    m.traceModule = traceModule;
    m.isMetricsOn = true;
    m.isUnmanaged = isUnmanaged;
    return m;
  }

  /**
   * Binds configurations to annotations for dependency injection.
   */
  @Override
  protected void configure() {
    bind(Clock.class).to(SimulatedClock.class);

    bind(Long.class).annotatedWith(SimulationDurationMinutes.class)
        .toProvider(Providers.of(simulationDurationMinutes));

    bind(Long.class).annotatedWith(RandomSeed.class)
        .toProvider(Providers.of(randomSeed));

    bind(String.class).annotatedWith(ClusterTopologyFilePath.class)
        .toInstance(clusterTopologyFilePath);

    bind(Boolean.class).annotatedWith(IsMetricsOn.class)
        .toInstance(isMetricsOn);

    bind(String.class).annotatedWith(RootMetricsOutputDirectory.class)
        .toProvider(Providers.of(metricsOutputDirectory));

    bind(Integer.class).annotatedWith(MetricsTimerWindowSize.class)
        .toProvider(Providers.of(metricsTimerWindowSize));

    bind(Integer.class).annotatedWith(MetricsRecordIntervalSeconds.class)
        .toProvider(Providers.of(metricsRecordIntervalSeconds));

    bind(Boolean.class).annotatedWith(IsUnmanaged.class)
        .toInstance(isUnmanaged);

    bind(Long.class).annotatedWith(SimulationTraceStartTime.class)
        .toProvider(Providers.of(simulationTraceStartTime));

    final Multibinder<String> multibinder = Multibinder.newSetBinder(binder(), String.class, MetricsSet.class);
    for (final String metric : metricsSet) {
      multibinder.addBinding().toProvider(Providers.of(metric));
    }

    LOG.log(Level.INFO, clusterTopologyFilePath);

    final YarnConfiguration conf = new YarnConfiguration();
    if (scheduler != null) {
      setSchedulerConfigs(conf);
    }

    conf.set(CapacitySchedulerConfiguration.NODE_LOCALITY_DELAY, "-1");
    conf.set(CapacitySchedulerConfiguration.MAX_ASSIGN_PER_HEARTBEAT, "-1");
    conf.set(CapacitySchedulerConfiguration.OFFSWITCH_PER_HEARTBEAT_LIMIT, "10000");

    bind(Configuration.class).toInstance(conf);

    QueueMetrics.clearQueueMetrics();
    DefaultMetricsSystem.setMiniClusterMode(true);

    if (traceModule != null) {
      LOG.log(Level.INFO, "Trace module is available!");
      install(traceModule);
    } else {
      LOG.log(Level.INFO, "Trace module is not available, initializing through parsed value...");
      install(parseTraceConfig());
    }
  }

  private void setSchedulerConfigs(final YarnConfiguration conf) {
    final CapacitySchedulerConfiguration capConf = new CapacitySchedulerConfiguration();
    for (final String queue : capConf.getQueues("root")) {
      final String queuePath = String.join(".", "root", queue);
      final String orderingPolicyPrefix = CapacitySchedulerConfiguration.PREFIX + queuePath + "." +
          CapacitySchedulerConfiguration.ORDERING_POLICY;
      conf.set(orderingPolicyPrefix, scheduler);
    }
  }

  public Level getSimulatorLogLevel() {
    return Level.parse(simulatorLogLevel);
  }

  public void setMetricsOutputDirectory(final String metricsOutputDirectory) {
    this.metricsOutputDirectory = metricsOutputDirectory;
  }

  /**
   * Parses the trace module to use, currently only SLS trace available.
   * @return An {@link AbstractModule} describing parameters for trace objects
   */
  private AbstractModule parseTraceConfig() {
    // Get the module class
    final Class<? extends AbstractModule> moduleClass =
        Constants.TRACE_MODULE_MAP.getOrDefault(
            traceConfig.getOrDefault(Constants.TRACE_TYPE, SLSTraceModule.TRACE_TYPE),
            SLSTraceModule.class
        );

    LOG.log(Level.INFO, MessageFormat.format(
        "Initializing the trace module: {0}", moduleClass.getName()));

    // Convert the traceConfig dictionary to JSON then to the specified module class
    return GSON.fromJson(GSON.toJsonTree(traceConfig), moduleClass);
  }

  /**
   * This is the reader that reads in the JSON configuration for simulations.
   */
  public final class Reader {
      private Reader() {
      }

      /**
       * Reads in the JSON configuration at the specified {@code configurationPath}.
       *
       * @param configurationPath the configuration path
       * @return {@link SimulatorModule} object
       * @throws IOException when the reader fails to read the file
       */
      public SimulatorModule readConfiguration(final String configurationPath)
          throws IOException {
        return GSON.fromJson(new FileReader(configurationPath), SimulatorModule.class);
      }
  }
}
