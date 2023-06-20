package org.apache.hadoop.yarn.dtss.metrics;

import com.codahale.metrics.Timer;
import com.codahale.metrics.*;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.commons.lang.NullArgumentException;
import org.apache.hadoop.yarn.dtss.config.parameters.MetricsRecordIntervalSeconds;
import org.apache.hadoop.yarn.dtss.config.parameters.MetricsTimerWindowSize;
import org.apache.hadoop.yarn.dtss.exceptions.StateException;
import org.apache.hadoop.yarn.dtss.lifecycle.LifeCycle;
import org.apache.hadoop.yarn.dtss.metrics.job.JobHistoryManager;
import org.apache.hadoop.yarn.dtss.stats.ClusterStats;
import org.apache.hadoop.yarn.dtss.time.Clock;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;

import java.io.File;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The manager of metrics, which keeps track of queue and overall cluster metrics.
 * Metrics to record are registered here.
 * Writes results at the end of simulation experiments.
 * Based on the SchedulerMetrics class in SLS.
 */
@Singleton
public final class MetricsManager extends LifeCycle {
  private static final Logger LOG = Logger.getLogger(MetricsManager.class.getName());

  public static final String EOL = System.getProperty("line.separator");

  private static final String METRIC_TYPE_VARIABLE = "variable";
  private static final String METRIC_TYPE_COUNTER = "counter";
  private static final String METRIC_TYPE_SAMPLER = "sampler";

  public static final String CLUSTER_ALLOCATED_MEMORY = "cluster.allocated.memory";
  public static final String CLUSTER_ALLOCATED_VCORES = "cluster.allocated.vcores";
  public static final String CLUSTER_AVAILABLE_MEMORY = "cluster.available.memory";
  public static final String CLUSTER_AVAILABLE_VCORES = "cluster.available.vcores";

  public static final String RUNNING_APPLICATIONS = "running.application";
  public static final String RUNNING_CONTAINERS = "running.container";
  public static final String PENDING_APPLICATIONS = "pending.application";

  public static final String SCHEDULER_OPERATION_ALLOCATE = "scheduler.operation.allocate";
  public static final String SCHEDULER_OPERATION_HANDLE = "scheduler.operation.handle";

  public enum QueueMetric {
    PENDING_MEMORY("pending.memory"),
    PENDING_VCORES("pending.vcores"),
    ALLOCATED_MEMORY("allocated.memory"),
    ALLOCATED_VCORES("allocated.vcores"),
    AVAILABLE_MEMORY("available.memory"),
    AVAILABLE_VCORES("available.vcores");

    private String value;

    QueueMetric(final String value) {
      this.value = value;
    }

    public String getConfigValue() {
      return "queue." + this.value;
    }
  }

  private static final int SAMPLING_SIZE = 60;

  private final Clock clock;

  private final MetricRegistry metricsRegistry;
  private final int metricsTimerWindowSize;
  private final int metricsRecordIntervalSeconds;
  private final JobHistoryManager jobHistoryManager;
  private final MetricsConfiguration metricsConfig;
  private final ClusterStats clusterStats;

  private UUID histogramsAlarm = null;

  private UUID metricsAlarm = null;

  private Counter schedulerHandleCounter;
  private Map<SchedulerEventType, Counter> schedulerHandleCounterMap;

  // Timers for scheduler allocate/handle operations
  private Timer schedulerAllocateTimer;
  private Counter schedulerAllocateCounter;

  private Timer schedulerHandleTimer;
  private Map<SchedulerEventType, Timer> schedulerHandleTimerMap;
  private List<Histogram> schedulerHistogramList;
  private Map<Histogram, Timer> histogramTimerMap;

  private final Set<String> trackedQueues = new HashSet<>();
  private ResourceScheduler scheduler = null;

  private transient List<Double> memoryUsedPercent = new LinkedList<>();
  private transient List<Double> vcoresUsedPercent = new LinkedList<>();

  @Inject
  private MetricsManager(
      @MetricsTimerWindowSize final Integer metricsTimerWindowSize,
      @MetricsRecordIntervalSeconds final Integer metricsRecordIntervalSeconds,
      final ClusterStats clusterStats,
      final JobHistoryManager jobHistoryManager,
      final MetricsConfiguration metricsConfig,
      final Clock clock) {
    this.clock = clock;
    this.metricsRegistry = new MetricRegistry();
    this.jobHistoryManager = jobHistoryManager;
    this.clusterStats = clusterStats;

    this.metricsTimerWindowSize = metricsTimerWindowSize;
    this.metricsRecordIntervalSeconds = metricsRecordIntervalSeconds;
    this.metricsConfig = metricsConfig;
  }

  public boolean isMetricsOn() {
    return metricsConfig.isMetricsOn();
  }

  public MetricsConfiguration getMetricsConfig() {
    return metricsConfig;
  }

  private void validateSettings() {
    if (!metricsConfig.isMetricsOn()) {
      return;
    }

    if (!(metricsTimerWindowSize > 0)) {
      throw new IllegalArgumentException("metricsTimerWindowSize must be greater than 0");
    }

    if (!(metricsRecordIntervalSeconds > 0)) {
      throw new IllegalArgumentException("metricsRecordIntervalSeconds must be greater than 0");
    }

    if (scheduler == null) {
      throw new NullArgumentException("Scheduler must be non-null");
    }

    if (metricsConfig.getMetricsDirectory() == null) {
      throw new NullArgumentException("Metrics output directory must be non-null");
    }
  }

  @Override
  public void init() {
    super.init();
    if (scheduler == null) {
      throw new NullArgumentException("Scheduler cannot be null!");
    }

    validateSettings();
    registerClusterResourceMetrics();
    registerContainerAppNumMetrics();
    registerSchedulerMetrics();
    clusterStats.init();
  }

  @Override
  public void start() {
    super.start();

    LOG.log(Level.INFO, "Starting metrics manager...");

    // .csv output
    initMetricsCSVOutput();

    // a thread to update histogram timer
    if (schedulerHistogramList != null) {
      histogramsAlarm = clock.schedulePeriodicAlarm(5, new HistogramsRunnable());
    }

    clusterStats.start();
    LOG.log(Level.INFO, "Metrics manager started!");
  }

  @Override
  public void stop() {
    super.stop();
    if (histogramsAlarm != null) {
      clock.cancelAlarm(histogramsAlarm);
    }

    if (metricsAlarm != null) {
      clock.cancelAlarm(metricsAlarm);
    }

    if (clusterStats != null) {
      clusterStats.setClusterMemUtilization(getMemoryUsedPercent());
      clusterStats.setClusterCoreUtilization(getVcoresUsedPercent());
      clusterStats.stop();
    }
  }

  public void writeResults() {
    if (!lifeCycleState.isDone()) {
      throw new StateException("Cannot write job and cluster results as simulation is not yet done!");
    }

    clusterStats.writeResultsToTsv();
    jobHistoryManager.getJobStats().writeResultsToTsv();

    System.out.println("=== Cluster stats ===");
    System.out.println(clusterStats.toString());
    System.out.println();
    System.out.println("=== Job stats ===");
    System.out.println(jobHistoryManager.getJobStats().toString());
    System.out.println();
  }

  class HistogramsRunnable implements Runnable {
    @Override
    public void run() {
      for (Histogram histogram : schedulerHistogramList) {
        Timer timer = histogramTimerMap.get(histogram);
        histogram.update((int) timer.getSnapshot().getMean());
      }
    }
  }

  private void registerClusterResourceMetrics() {
    if (metricsConfig.metricsContains(CLUSTER_ALLOCATED_MEMORY)) {
      metricsRegistry.register(METRIC_TYPE_VARIABLE + "." + CLUSTER_ALLOCATED_MEMORY,
          (Gauge<Long>) () -> {
            if (scheduler.getRootQueueMetrics() == null) {
              return 0L;
            } else {
              memoryUsedPercent.add(0D);

              memoryUsedPercent.add(
                  (double) scheduler.getRootQueueMetrics().getAllocatedMB() /
                      (
                          scheduler.getRootQueueMetrics().getAllocatedMB() +
                              scheduler.getRootQueueMetrics().getAvailableMB()
                      )
              );

              return scheduler.getRootQueueMetrics().getAllocatedMB();
            }
          }
      );
    }

    if (metricsConfig.metricsContains(CLUSTER_ALLOCATED_VCORES)) {
      metricsRegistry.register(METRIC_TYPE_VARIABLE + "." + CLUSTER_ALLOCATED_VCORES,
          (Gauge<Integer>) () -> {
            if (scheduler.getRootQueueMetrics() == null) {
              vcoresUsedPercent.add(0D);
              return 0;
            } else {
              vcoresUsedPercent.add(
                  (double) scheduler.getRootQueueMetrics().getAllocatedVirtualCores() /
                      (
                          scheduler.getRootQueueMetrics().getAllocatedVirtualCores() +
                              scheduler.getRootQueueMetrics().getAvailableVirtualCores()
                      )
              );

              return scheduler.getRootQueueMetrics().getAllocatedVirtualCores();
            }
          }
      );
    }

    if (metricsConfig.metricsContains(CLUSTER_AVAILABLE_MEMORY)) {
      metricsRegistry.register(METRIC_TYPE_VARIABLE + "." + CLUSTER_AVAILABLE_MEMORY,
          (Gauge<Long>) () -> {
            if (scheduler.getRootQueueMetrics() == null) {
              return 0L;
            } else {
              return scheduler.getRootQueueMetrics().getAvailableMB();
            }
          }
      );
    }

    if (metricsConfig.metricsContains(CLUSTER_AVAILABLE_VCORES)) {
      metricsRegistry.register(METRIC_TYPE_VARIABLE + "." + CLUSTER_AVAILABLE_VCORES,
          (Gauge<Integer>) () -> {
            if (scheduler.getRootQueueMetrics() == null) {
              return 0;
            } else {
              return scheduler.getRootQueueMetrics().getAvailableVirtualCores();
            }
          }
      );
    }
  }

  private void registerContainerAppNumMetrics() {
    if (metricsConfig.metricsContains(RUNNING_APPLICATIONS)) {
      metricsRegistry.register(METRIC_TYPE_VARIABLE + "." + RUNNING_APPLICATIONS,
          (Gauge<Integer>) () -> {
            if (scheduler.getRootQueueMetrics() == null) {
              return 0;
            } else {
              return scheduler.getRootQueueMetrics().getAppsRunning();
            }
          }
      );
    }

    if (metricsConfig.metricsContains(PENDING_APPLICATIONS)) {
      metricsRegistry.register(METRIC_TYPE_VARIABLE + "." + PENDING_APPLICATIONS,
          (Gauge<Integer>) () -> {
            if (scheduler.getRootQueueMetrics() == null) {
              return 0;
            } else {
              return scheduler.getRootQueueMetrics().getAppsPending();
            }
          }
      );
    }

    if (metricsConfig.metricsContains(RUNNING_CONTAINERS)) {
      metricsRegistry.register(METRIC_TYPE_VARIABLE + "." + RUNNING_CONTAINERS,
          (Gauge<Integer>) () -> {
            if (scheduler.getRootQueueMetrics() == null) {
              return 0;
            } else {
              return scheduler.getRootQueueMetrics().getAllocatedContainers();
            }
          }
      );
    }
  }

  private void registerSchedulerMetrics() {
    // counters for scheduler operations
    if (metricsConfig.metricsContains(SCHEDULER_OPERATION_ALLOCATE)) {
      schedulerAllocateCounter = metricsRegistry.counter(
          METRIC_TYPE_COUNTER + "." + SCHEDULER_OPERATION_ALLOCATE
      );

      schedulerAllocateTimer = new Timer(new SlidingWindowReservoir(metricsTimerWindowSize));

      // histogram for scheduler operations (Samplers)
      schedulerHistogramList = new ArrayList<>();
      histogramTimerMap = new HashMap<>();

      // allocateTimecostHistogram
      Histogram schedulerAllocateHistogram = new Histogram(new SlidingWindowReservoir(SAMPLING_SIZE));
      metricsRegistry.register(
          METRIC_TYPE_SAMPLER + "." + SCHEDULER_OPERATION_ALLOCATE + ".timecost",
          schedulerAllocateHistogram
      );

      schedulerHistogramList.add(schedulerAllocateHistogram);
      histogramTimerMap.put(schedulerAllocateHistogram, schedulerAllocateTimer);
    }

    if (metricsConfig.metricsContains(SCHEDULER_OPERATION_HANDLE)) {
      schedulerHandleCounter = metricsRegistry.counter(METRIC_TYPE_COUNTER + "." + SCHEDULER_OPERATION_HANDLE);
      schedulerHandleCounterMap = new HashMap<>();
      for (SchedulerEventType e : SchedulerEventType.values()) {
        Counter counter = metricsRegistry.counter(
            METRIC_TYPE_COUNTER + "." + SCHEDULER_OPERATION_HANDLE + "." + e);
        schedulerHandleCounterMap.put(e, counter);
      }

      schedulerHandleTimer = new Timer(
          new SlidingWindowReservoir(metricsTimerWindowSize));

      schedulerHandleTimerMap = new HashMap<>();
      for (SchedulerEventType e : SchedulerEventType.values()) {
        Timer timer = new Timer(new SlidingWindowReservoir(metricsTimerWindowSize));
        schedulerHandleTimerMap.put(e, timer);
      }

      // handleTimecostHistogram
      Histogram schedulerHandleHistogram = new Histogram(
          new SlidingWindowReservoir(SAMPLING_SIZE));
      metricsRegistry.register(
          METRIC_TYPE_SAMPLER + "." + SCHEDULER_OPERATION_HANDLE + ".timecost",
          schedulerHandleHistogram);
      schedulerHistogramList.add(schedulerHandleHistogram);
      histogramTimerMap.put(schedulerHandleHistogram, schedulerHandleTimer);

      for (SchedulerEventType e : SchedulerEventType.values()) {
        Histogram histogram = new Histogram(
            new SlidingWindowReservoir(SAMPLING_SIZE));
        metricsRegistry.register(
            METRIC_TYPE_SAMPLER + "." + SCHEDULER_OPERATION_HANDLE + "." + e + ".timecost", histogram);
        schedulerHistogramList.add(histogram);
        histogramTimerMap.put(histogram, schedulerHandleTimerMap.get(e));
      }
    }
  }

  private void initMetricsCSVOutput() {
    final String detailedMetricsDir = metricsConfig.getMetricsFilePath("detailed-metrics");
    if (detailedMetricsDir != null) {
      final File dir = new File(detailedMetricsDir);
      if(!dir.exists() && !dir.mkdirs()) {
        LOG.log(Level.SEVERE,"Cannot create directory " + dir.getAbsoluteFile());
        throw new RuntimeException("Not able to create directory " + dir.getAbsoluteFile() + " for metrics");
      }

      final MetricsCsvReporter reporter = MetricsCsvReporter.newBuilder(metricsRegistry, clock)
          .formatFor(Locale.US)
          .convertRatesTo(TimeUnit.SECONDS)
          .convertDurationsTo(TimeUnit.MILLISECONDS)
          .build(new File(detailedMetricsDir));

      reporter.start(metricsRecordIntervalSeconds, TimeUnit.SECONDS);
    }
  }

  public void setScheduler(final ResourceScheduler scheduler) {
    this.scheduler = scheduler;
  }

  public Timer getSchedulerHandleTimer() {
    return schedulerHandleTimer;
  }

  public Timer getSchedulerHandleTimer(SchedulerEventType schedulerEventType) {
    if (schedulerHandleTimerMap == null) {
      return null;
    }

    return schedulerHandleTimerMap.get(schedulerEventType);
  }

  public Timer getSchedulerAllocateTimer() {
    return schedulerAllocateTimer;
  }

  void increaseSchedulerAllocationCounter() {
    if (schedulerAllocateCounter != null) {
      schedulerAllocateCounter.inc();
    }
  }

  public String getQueueMetricName(final String queue, final QueueMetric metric) {
    return "variable.queue." + queue + "." + metric.value;
  }

  public void increaseSchedulerHandleCounter(SchedulerEventType schedulerEventType) {
    if (schedulerHandleCounter != null) {
      schedulerHandleCounter.inc();
      schedulerHandleCounterMap.get(schedulerEventType).inc();
    }
  }

  protected void registerQueueMetricsIfNew(final Queue queue) {
    if (trackedQueues.contains(queue.getQueueName())) {
      return;
    }

    trackedQueues.add(queue.getQueueName());

    for (final QueueMetric queueMetric : QueueMetric.values()) {
      final String metricName = getQueueMetricName(queue.getQueueName(), queueMetric);
      if (!metricsRegistry.getGauges().containsKey(metricName) &&
          metricsConfig.metricsContains(queueMetric.getConfigValue())) {

        final Gauge<Number> gauge;
        switch (queueMetric) {
          case ALLOCATED_MEMORY:
            gauge = () -> queue.getMetrics().getAllocatedMB();
            break;
          case ALLOCATED_VCORES:
            gauge = () -> queue.getMetrics().getAllocatedVirtualCores();
            break;
          case AVAILABLE_VCORES:
            gauge = () -> queue.getMetrics().getAvailableVirtualCores();
            break;
          case AVAILABLE_MEMORY:
            gauge = () -> queue.getMetrics().getAvailableMB();
            break;
          case PENDING_MEMORY:
            gauge = () -> queue.getMetrics().getPendingMB();
            break;
          case PENDING_VCORES:
            gauge = () -> queue.getMetrics().getPendingVirtualCores();
            break;
          default:
            throw new UnsupportedOperationException("Unknown queue metric: " + queueMetric);
        }

        metricsRegistry.register(metricName, gauge);
      }
    }
  }

  public List<Double> getMemoryUsedPercent() {
    return memoryUsedPercent;
  }

  public List<Double> getVcoresUsedPercent() {
    return vcoresUsedPercent;
  }
}
