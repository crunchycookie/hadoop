package org.apache.hadoop.yarn.dtss;

import net.jcip.annotations.NotThreadSafe;
import org.apache.commons.math3.util.Precision;
import org.apache.hadoop.yarn.dtss.cluster.ClusterTopologyProperties;
import org.apache.hadoop.yarn.dtss.job.ExperimentJobEndState;
import org.apache.hadoop.yarn.dtss.trace.TestTraceModule;
import org.apache.hadoop.yarn.dtss.trace.TestTraceProvider;
import org.apache.hadoop.yarn.dtss.trace.results.ClusterMetric;
import org.apache.hadoop.yarn.dtss.trace.results.JobStatsFileReader;
import org.apache.hadoop.yarn.dtss.trace.results.MetricInvariantCheck;
import org.apache.hadoop.yarn.dtss.trace.results.QueueMetric;
import org.apache.hadoop.yarn.dtss.trace.sls.SLSJob;
import org.apache.hadoop.yarn.dtss.trace.sls.SLSTask;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * End-to-end tests for simple DTSSRunner.
 */
@NotThreadSafe
public class TestDTSSRunner extends BaseDTSSRunnerTest {
  private static final String QUEUE_1_NAME = "sls_queue_1";
  private static final String QUEUE_2_NAME = "sls_queue_2";
  private static final String QUEUE_3_NAME = "sls_queue_3";
  private static final String[] QUEUES = new String[]{QUEUE_1_NAME, QUEUE_2_NAME, QUEUE_3_NAME};

  @Test
  public void testNoResourcesAllocated() throws IOException {
    final ClusterTopologyProperties properties = createDefaultClusterTopologyProperties();
    setup(DEFAULT_RUNTIME_MINUTES, properties, new TestTraceModule(new TestTraceProvider(new ArrayList<>())));

    Runner.runSimulator(simulatorModule, runnerModule);
    checkClusterMetricInvariants(
        ClusterMetric.CLUSTER_ALLOCATED_VCORES,
        Collections.singletonList(
            new MetricInvariantCheck<>(
                (t, v) -> Precision.equals(v, 0D),
                "NoAllocation"
            )
        )
    );

    final List<JobStatsFileReader.JobStatsEntry> jobs = readJobsFromJobsStatsFile();
    Assert.assertEquals(0, jobs.size());
  }

  @Test
  public void testRunOneJobUnmanagedAM() throws IOException {
    final List<SLSJob> jobList = Collections.singletonList(
        SLSJob.Builder.newBuilder()
            .setTasks(Collections.singletonList(SLSTask.newTask(60 * 1000)))
            .setSubmitTimeMs(0L)
            .setPriority(30)
            .setJobId("Job_1")
            .setQueueName(QUEUE_1_NAME)
            .build()
    );

    final ClusterTopologyProperties properties = createDefaultClusterTopologyProperties();
    setup(DEFAULT_RUNTIME_MINUTES, properties, new TestTraceModule(new TestTraceProvider(jobList)), true);

    Runner.runSimulator(simulatorModule, runnerModule);

    final AtomicBoolean clusterSeenOneAllocated = new AtomicBoolean(false);
    checkClusterMetricInvariants(
        ClusterMetric.CLUSTER_ALLOCATED_VCORES,
        Collections.singletonList(
            new MetricInvariantCheck<>(
                (t, v) -> {
                  if (Precision.equals(v, 1D)) {
                    clusterSeenOneAllocated.set(true);
                  }

                  return Precision.compareTo(v, 1D, 1) <= 0;
                },
                "ClusterSingleAllocation"
            )
        )
    );

    Assert.assertTrue("Must have seen one container allocated.", clusterSeenOneAllocated.get());

    final AtomicBoolean queueSeenOneAllocated = new AtomicBoolean(false);
    checkQueueMetricInvariants(
        QUEUE_1_NAME,
        QueueMetric.QUEUE_ALLOCATED_VCORES,
        Collections.singletonList(
            new MetricInvariantCheck<>(
                (t, v) -> {
                  if (Precision.equals(v, 1D)) {
                    queueSeenOneAllocated.set(true);
                  }

                  return Precision.compareTo(v, 1D, 1) <= 0;
                },
                "QueueSingleAllocation"
            )
        )
    );

    Assert.assertTrue("Must have seen one container allocated.", queueSeenOneAllocated.get());
    final List<JobStatsFileReader.JobStatsEntry> jobs = readJobsFromJobsStatsFile();
    Assert.assertEquals(1, jobs.size());
    final JobStatsFileReader.JobStatsEntry oneJob = jobs.get(0);
    Assert.assertEquals(ExperimentJobEndState.COMPLETED, oneJob.getExperimentJobEndState());
    Assert.assertTrue(oneJob.getEndTimeSecondsOffset() >= oneJob.getStartTimeSecondsOffset());
    Assert.assertTrue(oneJob.getStartTimeSecondsOffset() >= oneJob.getSubmitTimeSecondsOffset());
    Assert.assertEquals(QUEUE_1_NAME, oneJob.getQueueName());

    final Map<String, SLSJob> jobMap = constructJobMap(jobList);
    checkJobInvariants(jobs, jobMap);
  }

  @Test
  public void testLargeJobCannotFinish() throws IOException {
    // SLS Queue 1 should have at most 10 containers,
    // based on the setting of capacity-scheduler.xml
    final List<SLSTask> largeJobTaskList = new ArrayList<>();
    final long taskRuntimeSeconds = 300;
    final long taskRuntimeMs = taskRuntimeSeconds * 1000;

    for (int i = 0; i < 100; i++) {
      largeJobTaskList.add(SLSTask.newTask(taskRuntimeMs));
    }

    final List<SLSJob> jobList = Collections.singletonList(
        SLSJob.Builder.newBuilder()
            .setTasks(largeJobTaskList)
            .setSubmitTimeMs(0L)
            .setPriority(30)
            .setJobId("Job_1")
            .setQueueName(QUEUE_1_NAME)
            .build()
    );

    final ClusterTopologyProperties properties = createDefaultClusterTopologyProperties();
    setup(DEFAULT_RUNTIME_MINUTES, properties, new TestTraceModule(new TestTraceProvider(jobList)));

    Runner.runSimulator(simulatorModule, runnerModule);

    checkClusterMetricInvariants(ClusterMetric.CLUSTER_ALLOCATED_VCORES);

    final List<JobStatsFileReader.JobStatsEntry> jobs = readJobsFromJobsStatsFile();
    Assert.assertEquals(1, jobs.size());
    final JobStatsFileReader.JobStatsEntry oneJob = jobs.get(0);
    Assert.assertTrue(oneJob.getExperimentJobEndState().equals(ExperimentJobEndState.SUBMITTED) ||
        oneJob.getExperimentJobEndState().equals(ExperimentJobEndState.STARTED));

    final Map<String, SLSJob> jobMap = constructJobMap(jobList);
    checkJobInvariants(jobs, jobMap);
  }

  @Test
  public void testLargeJobGetsDelayed() throws IOException {
    // SLS Queue 1 should have at most 10 containers,
    // based on the setting of capacity-scheduler.xml
    final List<SLSTask> largeJobTaskList = new ArrayList<>();
    final long taskRuntimeSeconds = 300;
    final long taskRuntimeMs = taskRuntimeSeconds * 1000;

    for (int i = 0; i < 30; i++) {
      largeJobTaskList.add(SLSTask.newTask(taskRuntimeMs));
    }

    final List<SLSJob> jobList = Collections.singletonList(
        SLSJob.Builder.newBuilder()
            .setTasks(largeJobTaskList)
            .setSubmitTimeMs(0L)
            .setPriority(30)
            .setJobId("Job_1")
            .setQueueName(QUEUE_1_NAME)
            .build()
    );

    final ClusterTopologyProperties properties = createDefaultClusterTopologyProperties();
    setup(DEFAULT_RUNTIME_MINUTES, properties, new TestTraceModule(new TestTraceProvider(jobList)));

    Runner.runSimulator(simulatorModule, runnerModule);

    checkClusterMetricInvariants(ClusterMetric.CLUSTER_ALLOCATED_VCORES);

    final List<JobStatsFileReader.JobStatsEntry> jobs = readJobsFromJobsStatsFile();
    Assert.assertEquals(1, jobs.size());
    final JobStatsFileReader.JobStatsEntry oneJob = jobs.get(0);
    Assert.assertEquals(ExperimentJobEndState.COMPLETED, oneJob.getExperimentJobEndState());
    Assert.assertTrue(
        oneJob.getEndTimeSecondsOffset() - oneJob.getStartTimeSecondsOffset() > 2 * taskRuntimeSeconds
    );

    final Map<String, SLSJob> jobMap = constructJobMap(jobList);
    checkJobInvariants(jobs, jobMap);
  }

  @Test
  public void testMultiJobResourceContention() throws IOException {
    // SLS Queue 1 should have at most 10 containers,
    // based on the setting of capacity-scheduler.xml
    final List<SLSJob> jobList = new ArrayList<>();
    final int numJobs = 3;
    final int numTasksPerJob = 15;
    final long taskRuntimeSeconds = 60 * 10;
    final long taskRuntimeMs = taskRuntimeSeconds * 1000;

    for (int jobId = 0; jobId < numJobs; jobId++) {
      final SLSJob.Builder builder = SLSJob.Builder.newBuilder();
      final List<SLSTask> taskList = new ArrayList<>();
      for (int taskId = 0; taskId < numTasksPerJob; taskId++) {
        taskList.add(SLSTask.newTask(taskRuntimeMs));
      }

      jobList.add(builder
          .setTasks(taskList)
          .setSubmitTimeMs(0L)
          .setPriority(30)
          .setJobId("Job_" + jobId)
          .setQueueName(QUEUE_1_NAME)
          .build()
      );
    }

    final ClusterTopologyProperties properties = createDefaultClusterTopologyProperties();
    setup(DEFAULT_RUNTIME_MINUTES, properties, new TestTraceModule(new TestTraceProvider(jobList)));

    Runner.runSimulator(simulatorModule, runnerModule);

    checkClusterMetricInvariants(ClusterMetric.CLUSTER_ALLOCATED_VCORES);

    final List<JobStatsFileReader.JobStatsEntry> jobs = readJobsFromJobsStatsFile();
    Assert.assertEquals(numJobs, jobs.size());
    boolean foundIncomplete = false;
    for (final JobStatsFileReader.JobStatsEntry jobStatsEntry : jobs) {
      if (jobStatsEntry.getExperimentJobEndState().equals(ExperimentJobEndState.SUBMITTED) ||
          jobStatsEntry.getExperimentJobEndState().equals(ExperimentJobEndState.STARTED)) {
        foundIncomplete = true;
      }
    }

    Assert.assertTrue(foundIncomplete);
    final AtomicBoolean queueSeenAllocation = new AtomicBoolean(false);
    checkQueueMetricInvariants(
        QUEUE_1_NAME,
        QueueMetric.QUEUE_ALLOCATED_VCORES,
        Collections.singletonList(
            new MetricInvariantCheck<>(
                (t, v) -> {
                  if (Precision.compareTo(v, 0D, 1) > 0) {
                    queueSeenAllocation.set(true);
                  }

                  return true;
                },
                "QueueAllocationCheck"
            )
        )
    );

    Assert.assertTrue(queueSeenAllocation.get());

    final Map<String, SLSJob> jobMap = constructJobMap(jobList);
    checkJobInvariants(jobs, jobMap);
  }

  @Test
  public void testDTSSStressUnmanaged() throws IOException {
    final List<SLSJob> jobList = new ArrayList<>();

    // 24 hours worth of simulation
    final int experimentRuntimeMinutes = 60 * 24;

    // Job arrives with 5 minute mean Poisson
    final int arrivalTimeMeanMinutes = 5;

    final int numBufferJobs = 20;
    final Random random = new Random(5);

    final int numJobs = experimentRuntimeMinutes / arrivalTimeMeanMinutes + numBufferJobs;
    long currMs = 0;

    for (int jobId = 0; jobId < numJobs; jobId++) {
      final SLSJob.Builder builder = SLSJob.Builder.newBuilder();
      final List<SLSTask> taskList = new ArrayList<>();
      // Create between 1 and 60 tasks
      final int numJobTasks = random.nextInt(60) + 1;
      // Average time for each task of the job between 1 and 10 minutes
      final int taskAverageRuntimeMinutes = 1 + random.nextInt(10);
      final double deviation = taskAverageRuntimeMinutes / 5.0;
      for (int taskId = 0; taskId < numJobTasks; taskId++) {
        final double gaussian = random.nextGaussian();
        final double taskRuntimeMinutes = Math.max(
            taskAverageRuntimeMinutes + gaussian * deviation, 1.0 / 60
        );

        final int taskRuntimeSeconds = (int) (taskRuntimeMinutes * 60);
        final long taskRuntimeMs = taskRuntimeSeconds * 1000;
        taskList.add(SLSTask.newTask(taskRuntimeMs));
      }

      final int queueIdxToSubmit = random.nextInt(QUEUES.length);
      currMs += getPoissonRandom(random, arrivalTimeMeanMinutes) * 60 * 1000;

      jobList.add(builder
          .setTasks(taskList)
          .setSubmitTimeMs(currMs)
          .setPriority(30)
          .setJobId("Job_" + jobId)
          .setQueueName(QUEUES[queueIdxToSubmit])
          .build()
      );
    }

    final ClusterTopologyProperties properties = createDefaultClusterTopologyProperties();

    setup(experimentRuntimeMinutes, properties, new TestTraceModule(new TestTraceProvider(jobList)), true);

    Runner.runSimulator(simulatorModule, runnerModule);

    checkClusterMetricInvariants(ClusterMetric.CLUSTER_ALLOCATED_VCORES);

    final List<JobStatsFileReader.JobStatsEntry> jobs = readJobsFromJobsStatsFile();
    Assert.assertEquals(numJobs, jobs.size());

    for (final String queueName : QUEUES) {
      final AtomicBoolean queueSeenAllocation = new AtomicBoolean(false);
      checkQueueMetricInvariants(
          queueName,
          QueueMetric.QUEUE_ALLOCATED_VCORES,
          Collections.singletonList(
              new MetricInvariantCheck<>(
                  (t, v) -> {
                    if (Precision.compareTo(v, 0D, 1) > 0) {
                      queueSeenAllocation.set(true);
                    }

                    return true;
                  },
                  "QueueAllocationCheck"
              )
          )
      );

      Assert.assertTrue(queueSeenAllocation.get());
    }

    final Map<String, SLSJob> jobMap = constructJobMap(jobList);
    checkJobInvariants(jobs, jobMap);
  }

  private static int getPoissonRandom(final Random r, final double mean) {
    double L = Math.exp(-mean);
    int k = 0;
    double p = 1.0;
    do {
      p = p * r.nextDouble();
      k++;
    } while (p > L);
    return k - 1;
  }
}
