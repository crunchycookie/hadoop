package org.apache.hadoop.yarn.util;

import java.util.concurrent.TimeUnit;

public interface EventClock extends Clock {
  void schedule(Runnable runnable, long delay, TimeUnit timeUnit);

  void scheduleWithFixedDelay(Runnable runnable, long initialDelay, long delay, TimeUnit timeUnit);

  boolean isShutdown();

  void shutdown();
}
