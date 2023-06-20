package org.apache.hadoop.yarn.dtss.time;

import org.apache.hadoop.classification.InterfaceAudience;

import java.time.Instant;
import java.util.UUID;
import java.util.function.Consumer;

/**
 * An {@link Alarm}, or event, scheduled on the {@link SimulatedClock}. These are not periodic.
 */
@InterfaceAudience.Private
public final class ClientAlarm extends Alarm {
  private final Consumer<Alarm> handler;

  public ClientAlarm(
      final UUID alarmId, final Instant alarmTime, final Consumer<Alarm> handler
  ) {
    super(alarmId, alarmTime);
    this.handler = handler;
  }

  @Override
  public void handleAlarm() {
    handler.accept(this);
  }
}
