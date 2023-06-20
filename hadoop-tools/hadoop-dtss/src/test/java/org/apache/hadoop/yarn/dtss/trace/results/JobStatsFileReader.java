package org.apache.hadoop.yarn.dtss.trace.results;

import org.apache.hadoop.thirdparty.com.google.common.base.Strings;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.yarn.dtss.job.ExperimentJobEndState;
import org.junit.Assert;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;

public final class JobStatsFileReader {
  private static final String[] HEADER = {
    "SubmitTimeSecondsOffset",
    "TraceJobId",
    "Queue",
    "EndState",
    "StartTimeSecondsOffset",
    "EndTimeSecondsOffset",
    "Priority"
  };

  private final CSVParser parser;
  private final Iterator<CSVRecord> iterator;

  public JobStatsFileReader(final String fpath) throws IOException {
    final InputStreamReader inputStreamReader = new InputStreamReader(new FileInputStream(fpath));
      parser = CSVFormat.TDF.withHeader().parse(inputStreamReader);

    iterator = parser.iterator();
  }

  public boolean hasNextRecord() {
    return iterator.hasNext();
  }

  public JobStatsEntry readRecord() {
    return new JobStatsEntry(iterator.next());
  }

  public static final class JobStatsEntry {
    private final Long submitTimeSecondsOffset;
    private final String traceJobId;
    private final String queueName;
    private final ExperimentJobEndState endState;
    private final Long startTimeSecondsOffset;
    private final Long endTimeSecondsOffset;
    private final Integer priority;

    JobStatsEntry(final CSVRecord record) {
      Assert.assertEquals(record.size(), HEADER.length);

      final String submitTimeSecondsOffsetStr = record.get(HEADER[0]);
      submitTimeSecondsOffset = Strings.isNullOrEmpty(submitTimeSecondsOffsetStr) ?
          null : Long.parseLong(submitTimeSecondsOffsetStr);
      this.traceJobId = record.get(HEADER[1]);
      this.queueName = record.get(HEADER[2]);
      this.endState = ExperimentJobEndState.valueOf(record.get(HEADER[3]));
      final String startTimeSecondsOffsetStr = record.get(HEADER[4]);
      this.startTimeSecondsOffset = Strings.isNullOrEmpty(startTimeSecondsOffsetStr) ?
          null : Long.parseLong(startTimeSecondsOffsetStr);
      final String endTimeSecondsOffsetStr = record.get(HEADER[5]);
      this.endTimeSecondsOffset = Strings.isNullOrEmpty(endTimeSecondsOffsetStr) ?
          null : Long.parseLong(endTimeSecondsOffsetStr);
      final String priorityStr = record.get(HEADER[6]);
      this.priority = Strings.isNullOrEmpty(priorityStr) ?
          null : Integer.parseInt(priorityStr);
    }

    public Long getSubmitTimeSecondsOffset() {
      return submitTimeSecondsOffset;
    }

    public String getTraceJobId() {
      return traceJobId;
    }

    public String getQueueName() {
      return queueName;
    }

    public Long getStartTimeSecondsOffset() {
      return startTimeSecondsOffset;
    }

    public Long getEndTimeSecondsOffset() {
      return endTimeSecondsOffset;
    }

    public Integer getPriority() {
      return priority;
    }

    public ExperimentJobEndState getExperimentJobEndState() {
      return endState;
    }
  }
}
