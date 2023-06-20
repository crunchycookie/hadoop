package org.apache.hadoop.yarn.dtss.trace;

import org.apache.hadoop.yarn.dtss.trace.sls.SLSJob;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;

public final class TestTraceProvider {
  private final Queue<SLSJob> jobsList = new ArrayDeque<>();

  public TestTraceProvider(
      final SLSJobArrivalFunction arrivalFunction
  ) {
    SLSJob slsJob = arrivalFunction.getNextJob();
    while (slsJob != null) {
      jobsList.add(slsJob);
      slsJob = arrivalFunction.getNextJob();
    }
  }

  public TestTraceProvider(final List<SLSJob> jobsList) {
    this.jobsList.addAll(jobsList);
  }

  public boolean hasNextJob() {
    return jobsList.size() > 0;
  }

  public SLSJob getNextJob() {
    return jobsList.poll();
  }
}
