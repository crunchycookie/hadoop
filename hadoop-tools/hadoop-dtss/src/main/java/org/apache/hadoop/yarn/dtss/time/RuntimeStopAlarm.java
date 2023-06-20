package org.apache.hadoop.yarn.dtss.time;

import org.apache.hadoop.classification.InterfaceAudience;

import java.time.Instant;
import java.util.UUID;

/**
 * A special event that signals that the simulated environment should stop.
 * Shuts down the experiment when scheduled.
 */
@InterfaceAudience.Private
public final class RuntimeStopAlarm extends Alarm {
  public RuntimeStopAlarm(final UUID alarmId, final Instant alarmTime) {
    super(alarmId, alarmTime);
  }

  @Override
  public void handleAlarm() {
    // Do nothing
  }
}
