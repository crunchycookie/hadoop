package org.apache.hadoop.yarn.dtss.trace;

import com.google.inject.AbstractModule;

public final class TestTraceModule extends AbstractModule {
  private final TestTraceProvider testTraceProvider;
  public TestTraceModule(final TestTraceProvider testTraceProvider) {
    this.testTraceProvider = testTraceProvider;
  }
  
  @Override
  protected void configure() {
    bind(TraceReader.class).to(TestTraceReader.class);
    bind(TestTraceProvider.class).toInstance(testTraceProvider);
  }
}
