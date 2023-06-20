package org.apache.hadoop.yarn.dtss.time;

import org.apache.hadoop.yarn.dtss.lifecycle.InitializedLifeCycle;
import org.apache.hadoop.yarn.dtss.lifecycle.LifeCycleState;

import javax.annotation.Nullable;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;

/**
 * The clock abstract class.
 */
public abstract class Clock extends InitializedLifeCycle implements org.apache.hadoop.yarn.util.EventClock {
  private final MetricsClock metricsClock = new MetricsClock();

  /**
   * Schedules a periodic alarm on the clock, following an initial delay
   * @param initialDelaySec initial delay in seconds
   * @param taskPeriodicSec the task period in seconds
   * @param handler the handler of the alarm event
   * @return an ID of the scheduled task
   */
  public abstract UUID schedulePeriodicAlarm(
      long initialDelaySec, long taskPeriodicSec, Consumer<PeriodicClientAlarm> handler);

  /**
   * Schedules a periodic alarm on the clock.
   *
   * @param taskPeriodSec the task period in seconds
   * @param handler       the handler of the Alarm event
   * @return an ID of the scheduled task
   */
  public abstract UUID schedulePeriodicAlarm(long taskPeriodSec, Consumer<PeriodicClientAlarm> handler);

  /**
   * Schedules a periodic alarm on the clock with a runnable.
   *
   * @param taskPeriodSec the task period in seconds
   * @param runnable       the handler of the Alarm event
   * @return an ID of the scheduled task
   */
  public abstract UUID schedulePeriodicAlarm(long taskPeriodSec, Runnable runnable);

  /**
   * Unschedules the periodic task provided the schedule ID.
   *
   * @param scheduleId the ID of the scheduled periodic task
   */
  public abstract void unschedulePeriodicAlarm(UUID scheduleId);

  /**
   * Schedules an alarm that fires {@code taskTimeOffsetSec} from now.
   *
   * @param taskTimeOffsetSec the offset from current time in seconds
   * @param alarmHandler      the handler for the alarm.
   */
  public abstract UUID scheduleAlarm(long taskTimeOffsetSec, Consumer<Alarm> alarmHandler);

  /**
   * Schedules an alarm that fires at or after {@code alarmTime}.
   *
   * @param alarmTime    the alarm time
   * @param alarmHandler the handler for the alarm
   */
  public abstract UUID scheduleAbsoluteAlarm(Instant alarmTime, Consumer<Alarm> alarmHandler);

  /**
   * Cancels the scheduled alarm.
   * @param alarmId the alarm ID
   */
  public abstract void cancelAlarm(UUID alarmId);

  /**
   * Schedules an alarm that triggers the shutdown of the clock after
   * {@code alarmTimeOffsetSec} seconds.
   *
   * @param alarmTimeOffsetSec the time to shut down the clock
   */
  public abstract void scheduleShutdown(long alarmTimeOffsetSec);

  /**
   * Schedules an alarm that triggers the shutdown of the clock at the specified time.
   *
   * @param alarmTime the time to shut down the clock
   */
  public abstract void scheduleShutdown(Instant alarmTime);

  /**
   * Polls the clock until the next alarm occurs.
   *
   * @return whether or not there are any more events in the clock
   */
  public abstract LifeCycleState pollNextAlarm();

  /**
   * @return the current time
   */
  public abstract Instant getInstant();

  @Override
  public long getTime() {
    return currentTimeMillis();
  }

  /**
   * @return the scheduled shutdown time, if any
   */
  @Nullable
  public abstract Instant getScheduledShutdown();

  /**
   * @return the current time in milliseconds
   */
  public abstract long currentTimeMillis();

  /**
   * @return the stop time of the clock
   */
  public abstract Optional<Instant> getStopTime();

  public com.codahale.metrics.Clock getMetricsClock() {
    return metricsClock;
  }

  private class MetricsClock extends com.codahale.metrics.Clock {
    private MetricsClock() {
    }

    @Override
    public long getTick() {
      return Clock.this.getInstant().toEpochMilli() * 1000;
    }
  }
}

