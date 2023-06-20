package org.apache.hadoop.yarn.dtss.time;

import org.apache.hadoop.classification.InterfaceAudience;

import java.time.Instant;
import java.util.UUID;

/**
 * Each {@link Alarm} object is tied to an event in the discrete event simulation.
 * An {@link Alarm} is marked by a timestamp and a UUID, where the UUID
 * allows unique identification of the event, while the timestamp allows
 * the {@link Alarm}s to be ordered in a priority queue in {@link SimulatedClock}.
 */
@InterfaceAudience.Private
public abstract class Alarm implements Comparable<Alarm> {
  private final Instant alarmTime;
  private final UUID alarmId;

  public Alarm(final UUID alarmId, final Instant alarmTime) {
    this.alarmTime = alarmTime;
    this.alarmId = alarmId;
  }

  public final UUID getAlarmId() {
    return alarmId;
  }

  public final Instant getInstant() {
    return alarmTime;
  }

  public abstract void handleAlarm();

  @Override
  public int compareTo(final Alarm that) {
    if (!this.alarmTime.equals(that.alarmTime)) {
      return this.alarmTime.compareTo(that.alarmTime);
    }

    return this.alarmId.compareTo(that.alarmId);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final Alarm alarm = (Alarm) o;

    if (alarmTime != null ? !alarmTime.equals(alarm.alarmTime) : alarm.alarmTime != null) {
      return false;
    }

    return alarmId != null ? alarmId.equals(alarm.alarmId) : alarm.alarmId == null;
  }

  @Override
  public int hashCode() {
    int result = alarmTime != null ? alarmTime.hashCode() : 0;
    result = 31 * result + (alarmId != null ? alarmId.hashCode() : 0);
    return result;
  }
}
