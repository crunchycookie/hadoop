package org.apache.hadoop.yarn.dtss;

import net.jcip.annotations.NotThreadSafe;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.math3.util.Precision;
import org.apache.hadoop.yarn.dtss.cluster.ClusterTopology;
import org.apache.hadoop.yarn.dtss.cluster.ClusterTopologyProperties;
import org.apache.hadoop.yarn.dtss.config.SimulatorModule;
import org.apache.hadoop.yarn.dtss.job.SimulatedTask;
import org.apache.hadoop.yarn.dtss.metrics.MetricsManager;
import org.apache.hadoop.yarn.dtss.trace.TestTraceModule;
import org.apache.hadoop.yarn.dtss.trace.results.*;
import org.apache.hadoop.yarn.dtss.trace.sls.SLSJob;
import org.junit.Assert;

import java.io.File;
import java.io.IOException;
import java.util.*;

@NotThreadSafe
@SuppressWarnings("VisibilityModifier")
public abstract class BaseDTSSRunnerTest {
  private static final int RANDOM_SEED = 5;
  protected static final int DEFAULT_RUNTIME_MINUTES = 30;
  protected static final int DEFAULT_NUM_RACKS = 2;
  protected static final int DEFAULT_NUM_MACHINES_ON_RACK = 2;
  protected static final int DEFAULT_NUM_CONTAINERS_IN_MACHINE = 10;
  protected static final int DEFAULT_CONTAINER_MB = 1024;
  protected static final int DEFAULT_VCORES = 1;

  private static final String[] metricsList = new String[] {
      MetricsManager.CLUSTER_ALLOCATED_VCORES,
      MetricsManager.CLUSTER_AVAILABLE_VCORES,
      MetricsManager.RUNNING_APPLICATIONS,
      MetricsManager.RUNNING_CONTAINERS,
      "queue.allocated.vcores",
      "queue.available.vcores",
  };

  private static final MetricInvariantCheck<Long, Double> GEQ_ZERO_CHECKER = new MetricInvariantCheck<>(
      (t, v) -> Precision.compareTo(v, 0D, 1) >= 0,
      "GreaterOrEqualToZero"
  );

  protected SimulatorModule simulatorModule;
  protected Runner.RunnerModule runnerModule;
  protected String runId;
  protected File tempDir;

  public void setup(
      final int simulationRuntimeMinutes,
      final ClusterTopologyProperties clusterTopologyProperties,
      final TestTraceModule testTraceModule
  ) throws IOException {
    setup(simulationRuntimeMinutes, clusterTopologyProperties, testTraceModule, false);
  }

  public void setup(
      final int simulationRuntimeMinutes,
      final ClusterTopologyProperties clusterTopologyProperties,
      final TestTraceModule testTraceModule,
      final boolean isUnmanaged
  ) throws IOException {
    final File dtssDir = new File("target", "dtssTestOutput");
    runId = UUID.randomUUID().toString();
    tempDir = new File(dtssDir, runId);
    if (!tempDir.mkdirs()) {
      throw new RuntimeException("Failed mkdirs for directory " + tempDir.getAbsolutePath());
    }

    final File topologyPath = new File(tempDir, "topology.json");
    ClusterTopology.writeClusterTopologyFile(clusterTopologyProperties, topologyPath.getAbsolutePath());
    simulatorModule = SimulatorModule.newModule(
        RANDOM_SEED,
        simulationRuntimeMinutes,
        topologyPath.getAbsolutePath(),
        dtssDir.getAbsolutePath(),
        testTraceModule,
        isUnmanaged,
        Arrays.asList(metricsList)
    );

    runnerModule = Runner.RunnerModule.newModule("EMPTY", runId);
  }

  protected ClusterTopologyProperties createClusterTopologyProperties(
      final int numRacks,
      final int numMachinesOnRack,
      final int numContainersInMachine,
      final int containerMB,
      final int containerVCores
  ) {
    final List<List<Integer>> racksArray = new ArrayList<>();
    for (int r = 0; r < numRacks; r++) {
      final List<Integer> machinesArray = new ArrayList<>();
      for (int m = 0; m < numMachinesOnRack; m++) {
        machinesArray.add(numContainersInMachine);
      }

      racksArray.add(machinesArray);
    }

    return new ClusterTopologyProperties(
        racksArray, containerMB, containerVCores
    );
  }

  protected ClusterTopologyProperties createDefaultClusterTopologyProperties() {
    return createClusterTopologyProperties(
        DEFAULT_NUM_RACKS,
        DEFAULT_NUM_MACHINES_ON_RACK,
        DEFAULT_NUM_CONTAINERS_IN_MACHINE,
        DEFAULT_CONTAINER_MB,
        DEFAULT_VCORES
    );
  }

  private static void addDefaultQueueMetricInvariants(final QueueMetric metric,
                                                      final List<MetricInvariantCheck<Long, Double>> checks) {
    // All valid cluster metrics should be >= 0
    checks.add(GEQ_ZERO_CHECKER);
  }

  private static void checkMetricInvariants(
      final DetailedMetricReader reader, final List<MetricInvariantCheck<Long, Double>> checks) {
    while (reader.hasNextRecord()) {
      final CSVRecord record = reader.readRecord();
      final long time = Long.parseLong(record.get(0));
      final double vCores = Double.parseDouble(record.get(1));
      for (final MetricInvariantCheck<Long, Double> invCheck : checks) {
        Assert.assertTrue(invCheck.getErrorMessage(time, vCores), invCheck.evaluate(time, vCores));
      }
    }
  }
  protected void checkQueueMetricInvariants(final String queueName, final QueueMetric metric) throws IOException {
      checkQueueMetricInvariants(queueName, metric, new ArrayList<>());
  }

  protected void checkQueueMetricInvariants(final String queueName,
                                            final QueueMetric metric,
                                            final List<MetricInvariantCheck<Long, Double>> invariantChecks)
      throws IOException {
    final List<MetricInvariantCheck<Long, Double>> allChecks = new ArrayList<>();
    addDefaultQueueMetricInvariants(metric, allChecks);
    allChecks.addAll(invariantChecks);

    final DetailedMetricReader reader = new DetailedMetricReader(
        getMetricsFile(QueueMetric.getQueueMetricFileName(queueName, metric)).getAbsolutePath(),
        ',',
        true
    );

    checkMetricInvariants(reader, allChecks);
  }

  private static void addDefaultClusterMetricInvariants(final ClusterMetric metric,
                                                final List<MetricInvariantCheck<Long, Double>> checks) {
    // All valid cluster metrics should be >= 0
    checks.add(GEQ_ZERO_CHECKER);
  }

  protected void checkClusterMetricInvariants(final ClusterMetric metric) throws IOException {
    checkClusterMetricInvariants(metric, new ArrayList<>());
  }

  protected void checkClusterMetricInvariants(
      final ClusterMetric metric,
      final List<MetricInvariantCheck<Long, Double>> invariantChecks) throws IOException {
    final List<MetricInvariantCheck<Long, Double>> allChecks = new ArrayList<>();
    addDefaultClusterMetricInvariants(metric, allChecks);
    allChecks.addAll(invariantChecks);

    final DetailedMetricReader reader = new DetailedMetricReader(
        getMetricsFile(ClusterMetric.getClusterMetricFileName(metric)).getAbsolutePath(),
        ',',
        true
    );

    checkMetricInvariants(reader, allChecks);
  }

  protected File getMetricsDir() {
    return new File(tempDir.getAbsolutePath(), "detailed-metrics");
  }

  protected File getMetricsFile(final String fileName) {
    final File metricsFile = new File(getMetricsDir(), fileName);
    Assert.assertTrue(metricsFile.exists());
    return metricsFile;
  }

  protected List<JobStatsFileReader.JobStatsEntry> readJobsFromJobsStatsFile() throws IOException {
    final List<JobStatsFileReader.JobStatsEntry> entries = new ArrayList<>();
    final File jobStatsFile = getJobStatsFile();
    final JobStatsFileReader reader = new JobStatsFileReader(jobStatsFile.getAbsolutePath());
    while (reader.hasNextRecord()) {
      entries.add(reader.readRecord());
    }

    return entries;
  }

  protected File getJobStatsFile() {
    final File jobStatsFile = new File(tempDir.getAbsolutePath(), "jobStatsFile.tsv");
    Assert.assertTrue(jobStatsFile.exists());
    return jobStatsFile;
  }

  protected void checkJobInvariants(
      final List<JobStatsFileReader.JobStatsEntry> experimentJobs,
      final Map<String, SLSJob> referenceJobs) {
    for (final JobStatsFileReader.JobStatsEntry job : experimentJobs) {
      Assert.assertTrue(referenceJobs.containsKey(job.getTraceJobId()));
      final SLSJob referenceJob = referenceJobs.get(job.getTraceJobId());
      Assert.assertEquals(referenceJob.getQueueName(), job.getQueueName());
      switch(job.getExperimentJobEndState()) {
        case READ:
        case NOT_SUBMITTED:
          Assert.assertNull(job.getSubmitTimeSecondsOffset());
          Assert.assertNull(job.getStartTimeSecondsOffset());
          Assert.assertNull(job.getEndTimeSecondsOffset());
          break;
        case SUBMITTED:
          validateSubmissionTime(job, referenceJob);
          Assert.assertNull(job.getStartTimeSecondsOffset());
          Assert.assertNull(job.getEndTimeSecondsOffset());
          break;
        case STARTED:
          validateSubmissionTime(job, referenceJob);
          validateStartTime(job, referenceJob);
          Assert.assertNull(job.getEndTimeSecondsOffset());
          break;
        case COMPLETED:
          validateSubmissionTime(job, referenceJob);
          validateStartTime(job, referenceJob);
          validateCompletionTime(job, referenceJob);
          break;
        default:
          throw new RuntimeException("Unexpected state for experiments!");
      }
    }
  }

  private static void validateSubmissionTime(
      final JobStatsFileReader.JobStatsEntry job, final SLSJob referenceJob
  ) {
    Assert.assertNotNull(job.getSubmitTimeSecondsOffset());
    Assert.assertTrue(job.getSubmitTimeSecondsOffset() >= 0);
    Assert.assertTrue(
        job.getSubmitTimeSecondsOffset() >= referenceJob.getSubmitTimeSeconds()
    );
  }

  private static void validateStartTime(
      final JobStatsFileReader.JobStatsEntry job, final SLSJob referenceJob
  ) {
    Assert.assertNotNull(job.getStartTimeSecondsOffset());
    Assert.assertTrue(job.getStartTimeSecondsOffset() >= job.getSubmitTimeSecondsOffset());
    Assert.assertTrue(
        job.getStartTimeSecondsOffset() >= referenceJob.getSubmitTimeSeconds()
    );
  }

  private static void validateCompletionTime(
      final JobStatsFileReader.JobStatsEntry job, final SLSJob referenceJob
  ) {
    Assert.assertNotNull(job.getEndTimeSecondsOffset());
    Assert.assertTrue(job.getEndTimeSecondsOffset() >= job.getSubmitTimeSecondsOffset());
    Assert.assertTrue(job.getEndTimeSecondsOffset() >= job.getStartTimeSecondsOffset());
    Assert.assertTrue(
        job.getEndTimeSecondsOffset() >= referenceJob.getSubmitTimeSeconds()
    );

    long earliestEndTimePossible = referenceJob.getSubmitTimeSeconds();
    for (final SimulatedTask task : referenceJob.getContainers()) {
      earliestEndTimePossible = Math.max(
          referenceJob.getSubmitTimeSeconds() + task.getDurationSeconds(), earliestEndTimePossible
      );
    }

    Assert.assertTrue(job.getEndTimeSecondsOffset() >= earliestEndTimePossible);
  }

  protected static Map<String, SLSJob> constructJobMap(final List<SLSJob> jobList) {
    final Map<String, SLSJob> jobMap = new HashMap<>();
    for (final SLSJob job : jobList) {
      jobMap.put(job.getTraceJobId(), job);
    }

    return jobMap;
  }
}
