package org.apache.hadoop.yarn.dtss.trace.sls;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.dtss.metrics.job.JobHistoryManager;
import org.apache.hadoop.yarn.dtss.trace.sls.config.SLSTraceFilePath;
import org.apache.hadoop.yarn.dtss.job.SimulatedJob;
import org.apache.hadoop.yarn.dtss.time.Clock;
import org.apache.hadoop.yarn.dtss.trace.ListTraceReader;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A trace reader that reads in an SLS trace into memory.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
@Singleton
public final class SLSTraceReader extends ListTraceReader {
  private static final Logger LOG = Logger.getLogger(SLSTraceReader.class.getName());

  public static final String TRACE_TYPE = "SLS";

  private final String traceFilePath;
  private final Gson gson = new GsonBuilder().create();

  @Inject
  private SLSTraceReader(
      @SLSTraceFilePath final String traceFilePath,
      final Clock clock,
      final Injector injector,
      final JobHistoryManager jobHistoryManager) {
    super(injector, clock, jobHistoryManager);
    this.traceFilePath = traceFilePath;
  }

  @Override
  protected Iterable<SimulatedJob> parseJobs() {
    final List<SimulatedJob> jobs = new ArrayList<>();
    try {
      final JsonReader jsonReader = new JsonReader(new FileReader(traceFilePath));
      jsonReader.setLenient(true);
      while (jsonReader.peek() != JsonToken.END_DOCUMENT) {
        final SLSJob.Builder slsJobBuilder = gson.fromJson(jsonReader, SLSJob.Builder.class);
        final SLSJob slsJob = slsJobBuilder.build();
        LOG.log(Level.INFO, slsJob.getTraceJobId());
        jobs.add(slsJob);
      }
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    return jobs;
  }
}
