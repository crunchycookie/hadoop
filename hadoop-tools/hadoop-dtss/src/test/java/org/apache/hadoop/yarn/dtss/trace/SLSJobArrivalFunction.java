package org.apache.hadoop.yarn.dtss.trace;

import org.apache.hadoop.yarn.dtss.trace.sls.SLSJob;

public abstract class SLSJobArrivalFunction {
  protected long startTimeUnixTS;
  protected long endTimeUnixTS;

  public SLSJobArrivalFunction(
      final long startTimeUnixTS, final long endTimeUnixTS
  ) {
    this.startTimeUnixTS = startTimeUnixTS;
    this.endTimeUnixTS = endTimeUnixTS;
  }

  public abstract SLSJob getNextJob();
}
