package org.apache.hadoop.yarn.dtss.time;

import org.apache.hadoop.classification.InterfaceAudience;

import java.time.Instant;
import java.util.UUID;
import java.util.function.Consumer;

/**
 * A periodic alarm that is fired periodically on the same handler by the {@link SimulatedClock}.
 */
@InterfaceAudience.Private
public final class PeriodicClientAlarm extends Alarm {
  private final UUID periodicAlarmId;
  private final long periodSec;
  private final Consumer<PeriodicClientAlarm> handler;

  public PeriodicClientAlarm(final UUID periodicAlarmId,
                             final long periodSec,
                             final Instant alarmTime,
                             final Consumer<PeriodicClientAlarm> handler) {
    super(periodicAlarmId, alarmTime);
    this.periodicAlarmId = periodicAlarmId;
    this.periodSec = periodSec;
    this.handler = handler;
  }

  @Override
  public void handleAlarm() {
    handler.accept(this);
  }

  public UUID getPeriodicAlarmId() {
    return periodicAlarmId;
  }

  public long getPeriodSec() {
    return periodSec;
  }
}
