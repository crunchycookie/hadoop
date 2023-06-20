package org.apache.hadoop.yarn.dtss.trace.sls.config;

import com.google.inject.AbstractModule;
import org.apache.hadoop.yarn.dtss.trace.TraceReader;
import org.apache.hadoop.yarn.dtss.trace.sls.SLSTraceReader;

/**
 * Configurations associated with using an SLS trace.
 */
public final class SLSTraceModule extends AbstractModule {
  public static final String TRACE_TYPE = SLSTraceReader.TRACE_TYPE;

  private final String traceFilePath = null;

  @Override
  protected void configure() {
    bind(TraceReader.class).to(SLSTraceReader.class);
    bind(String.class).annotatedWith(SLSTraceFilePath.class).toInstance(traceFilePath);
  }
}
