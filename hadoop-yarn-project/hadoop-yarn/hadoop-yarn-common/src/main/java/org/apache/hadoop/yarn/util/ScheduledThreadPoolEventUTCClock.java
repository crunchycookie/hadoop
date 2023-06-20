package org.apache.hadoop.yarn.util;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public final class ScheduledThreadPoolEventUTCClock implements EventClock {
  private final ScheduledThreadPoolExecutor executor;
  private final Clock clock;

  public ScheduledThreadPoolEventUTCClock(final int poolSize) {
    this.executor = new ScheduledThreadPoolExecutor(poolSize);
    this.clock = new UTCClock();
  }

  @Override
  public void schedule(Runnable runnable, long delay, TimeUnit timeUnit) {
    executor.schedule(runnable, delay, timeUnit);
  }

  @Override
  public void scheduleWithFixedDelay(Runnable runnable, long initialDelay, long delay, TimeUnit timeUnit) {
    executor.scheduleWithFixedDelay(runnable, initialDelay, delay, timeUnit);
  }

  @Override
  public boolean isShutdown() {
    return executor.isShutdown();
  }

  @Override
  public void shutdown() {
    executor.shutdown();
  }

  @Override
  public long getTime() {
    return clock.getTime();
  }
}
