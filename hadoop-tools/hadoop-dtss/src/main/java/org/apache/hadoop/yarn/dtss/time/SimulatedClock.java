package org.apache.hadoop.yarn.dtss.time;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.yarn.dtss.config.parameters.SimulationDurationMinutes;
import org.apache.hadoop.yarn.dtss.config.parameters.SimulationTraceStartTime;
import org.apache.hadoop.yarn.dtss.exceptions.OperationNotSupportedException;
import org.apache.hadoop.yarn.dtss.lifecycle.LifeCycleState;
import org.apache.hadoop.yarn.dtss.random.RandomGenerator;
import org.apache.hadoop.yarn.dtss.random.RandomSeeder;

import javax.annotation.Nullable;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A discrete, event-driven clock that polls events, or {@link Alarm}s,
 * from a priority queue in discrete time order.
 * Allows clients to schedule events that simulate events firing in real time.
 * Shuts down when a {@link RuntimeStopAlarm} is encountered,
 * or when there are no more events remaining in the clock.
 */
@Singleton
@InterfaceAudience.Private
public final class SimulatedClock extends Clock {
  private static final Logger LOG = Logger.getLogger(SimulatedClock.class.getName());

  private final Map<UUID, Consumer<PeriodicClientAlarm>> periodicTasks = new HashMap<>();
  private final PriorityQueue<Alarm> eventQueue = new PriorityQueue<>();
  private final RandomGenerator random;
  private final Long simulationDurationMinutes;

  private final Map<UUID, Runnable> onClockStart = new HashMap<>();

  private Instant scheduledShutdown = null;
  private Instant simulationStartTime = null;
  private Instant currTime;

  @Inject
  private SimulatedClock(
      final RandomSeeder randomSeeder,
      @SimulationDurationMinutes final Long simulationDurationMinutes,
      @Nullable @SimulationTraceStartTime final Long simulationTraceStartTime) {
    this.random = randomSeeder.newRandomGenerator();
    this.simulationDurationMinutes = simulationDurationMinutes;
    if (simulationTraceStartTime != null) {
      setSimulationStartTime(Instant.ofEpochSecond(simulationTraceStartTime));
    }
  }

  private UUID schedulePeriodicAlarm(
      final Optional<Long> initialDelaySec, final long taskPeriodSec, final Consumer<PeriodicClientAlarm> handler) {
    final UUID periodicAlarmId = random.randomUUID();
    assert !periodicTasks.containsKey(periodicAlarmId);
    periodicTasks.put(periodicAlarmId, handler);

    // If the clock has not yet started, queue up the task
    if (lifeCycleState.compareTo(LifeCycleState.STARTED) < 0) {
      onClockStart.put(
          periodicAlarmId,
          () -> addPeriodicAlarm(initialDelaySec, taskPeriodSec, handler, periodicAlarmId)
      );
      return periodicAlarmId;
    }

    addPeriodicAlarm(initialDelaySec, taskPeriodSec, handler, periodicAlarmId);
    return periodicAlarmId;
  }

  private void addPeriodicAlarm(final Optional<Long> initialDelaySec,
                                final long taskPeriodSec,
                                final Consumer<PeriodicClientAlarm> handler,
                                final UUID periodicAlarmId) {
    final Instant firstAlarmTime = initialDelaySec.isPresent() ?
        currTime.plusSeconds(initialDelaySec.get()) : currTime.plusSeconds(taskPeriodSec);

    eventQueue.add(new PeriodicClientAlarm(
        periodicAlarmId, taskPeriodSec, firstAlarmTime, handler)
    );
  }

  /**
   * Allows users to queue up events periodically with an initial delay.
   * @param initialDelaySec initial delay in seconds
   * @param taskPeriodSec the periodicity of the event
   * @param handler the handler of the alarm event
   * @return the UUID associated with the periodic events
   */
  @Override
  public UUID schedulePeriodicAlarm(
      final long initialDelaySec, final long taskPeriodSec, final Consumer<PeriodicClientAlarm> handler) {
    return schedulePeriodicAlarm(Optional.of(initialDelaySec), taskPeriodSec, handler);
  }

  /**
   * Allows users to queue up events periodically without an initial delay.
   * @param taskPeriodSec the periodicity of the event
   * @param handler the handler of the alarm event
   * @return the UUID associated with the periodic events
   */
  @Override
  public synchronized UUID schedulePeriodicAlarm(
      final long taskPeriodSec,
      final Consumer<PeriodicClientAlarm> handler) {
    return schedulePeriodicAlarm(Optional.empty(), taskPeriodSec, handler);
  }

  /**
   * Allows users to queue up events periodically without an initial delay.
   * @param taskPeriodSec the periodicity of the event
   * @param runnable the handler of the alarm event
   * @return the UUID associated with the periodic events
   */
  @Override
  public UUID schedulePeriodicAlarm(final long taskPeriodSec, final Runnable runnable) {
    return schedulePeriodicAlarm(taskPeriodSec, h -> runnable.run());
  }

  /**
   * Cancels periodic tasks/events.
   * @param scheduleId the ID of the scheduled periodic task
   */
  @Override
  public synchronized void unschedulePeriodicAlarm(final UUID scheduleId) {
    if (scheduleId == null) {
      return;
    }

    onClockStart.remove(scheduleId);
    periodicTasks.remove(scheduleId);
  }

  private void addAlarm(final UUID alarmId, final Consumer<Alarm> alarmHandler, final long taskTimeOffsetSec) {
    eventQueue.add(new ClientAlarm(alarmId, currTime.plusSeconds(taskTimeOffsetSec), alarmHandler));
  }

  /**
   * Schedules an event on the clock in relative time.
   * @param taskTimeOffsetSec the offset from current time in seconds
   * @param alarmHandler      the handler for the alarm.
   * @return The identifier of the event.
   */
  @Override
  public synchronized UUID scheduleAlarm(
      final long taskTimeOffsetSec, final Consumer<Alarm> alarmHandler) {
    final UUID alarmId = random.randomUUID();
    if (lifeCycleState.compareTo(LifeCycleState.STARTED) < 0) {
      onClockStart.put(alarmId, () -> addAlarm(alarmId, alarmHandler, taskTimeOffsetSec));
      return alarmId;
    }

    addAlarm(alarmId, alarmHandler, taskTimeOffsetSec);
    return alarmId;
  }

  /**
   * Schedules an event on the clock in absolute time.
   * @param alarmTime    the alarm time
   * @param alarmHandler the handler for the alarm
   * @return the identifier of the event.
   */
  @Override
  public synchronized UUID scheduleAbsoluteAlarm(
      final Instant alarmTime, final Consumer<Alarm> alarmHandler) {
    final UUID alarmId = random.randomUUID();
    eventQueue.add(new ClientAlarm(alarmId, alarmTime, alarmHandler));
    return alarmId;
  }

  /**
   * Cancels a future event
   * @param alarmId the alarm ID
   */
  @Override
  public synchronized void cancelAlarm(final UUID alarmId) {
    onClockStart.remove(alarmId);
    eventQueue.removeIf(alarm -> alarm.getAlarmId().equals(alarmId));
  }

  /**
   * Schedule a shutdown event on the clock.
   * @param alarmTimeOffsetSec the time to shut down the clock
   */
  @Override
  public void scheduleShutdown(final long alarmTimeOffsetSec) {
    scheduleShutdown(currTime.plusSeconds(alarmTimeOffsetSec));
  }

  @Override
  @Nullable
  public Instant getScheduledShutdown() {
    return scheduledShutdown;
  }

  /**
   * Schedule a shutdown event on the clock.
   * @param alarmTime the time to shut down the clock
   */
  @Override
  public void scheduleShutdown(final Instant alarmTime) {
    LOG.log(Level.INFO, "Shutdown scheduled at " + alarmTime);
    if (scheduledShutdown == null || alarmTime.compareTo(scheduledShutdown) < 0) {
      scheduledShutdown = alarmTime;
    }

    eventQueue.add(new RuntimeStopAlarm(random.randomUUID(), alarmTime));
  }

  @Override
  public long currentTimeMillis() {
    return currTime.toEpochMilli();
  }

  public Instant getSimulationStartTime() {
    return simulationStartTime;
  }

  public void setSimulationStartTime(final Instant instant) {
    if (lifeCycleState.compareTo(LifeCycleState.STARTED) >= 0) {
      throw new IllegalArgumentException(
          "Clock has already started, should not be setting the simulation start time!");
    }

    assert instant != null;
    LOG.log(Level.INFO, "Setting simulation start time epoch to " + instant.getEpochSecond());
    simulationStartTime = instant;
    currTime = simulationStartTime;
  }

  @Override
  public Optional<Instant> getStopTime() {
    if (lifeCycleState.isDone()) {
      return Optional.of(currTime);
    }

    return Optional.empty();
  }

  /**
   * Polls for the next event in the priority queue.
   * @return The current state of the simulation
   */
  @Override
  public LifeCycleState pollNextAlarm() {
    final Alarm alarm;
    synchronized (this) {
      if (lifeCycleState.isDone()) {
        throw new OperationNotSupportedException(
            String.format("Clock cannot handle more alarms after entering %s lifeCycleState!", lifeCycleState)
        );
      }

      if (eventQueue.isEmpty()) {
        LOG.log(Level.INFO, "Simulation clock has no more events! Shutting down...");
        scheduleShutdown(0);
      }

      alarm = eventQueue.poll();
    }

    LifeCycleState newState = handleAlarm(alarm);
    LifeCycleState setState = newState;

    // Trigger all other events that occur at the current time.
    while (true) {
      final boolean condition;
      synchronized (this) {
        condition = !eventQueue.isEmpty() && eventQueue.peek().getInstant().compareTo(currTime) == 0;
      }

      if (!condition) {
        break;
      }

      newState = handleAlarm(eventQueue.poll());

      if (newState.isDone()) {
        setState = newState;
      }
    }

    synchronized (this) {
      lifeCycleState = lifeCycleState.transition(setState);
      return lifeCycleState;
    }
  }

  @Override
  public Instant getInstant() {
    return currTime;
  }

  /**
   * Check and handle the {@link Alarm} based on its type and the current state of the clock.
   * If the type is a {@link RuntimeStopAlarm}, the simulation should end.
   * If the type is periodic, a future alarm should also be scheduled.
   * If the type is not periodic, it should be fire and forget.
   * @param alarm The alarm polled
   * @return The state of the clock
   */
  private LifeCycleState handleAlarm(final Alarm alarm) {
    if (alarm instanceof RuntimeStopAlarm) {
      LOG.log(Level.INFO, () -> "Simulation clock received a RuntimeStopAlarm, setting lifeCycleState to STOPPED...");
      return LifeCycleState.STOPPED;
    } else if (alarm instanceof PeriodicClientAlarm) {
      synchronized (this) {
        final PeriodicClientAlarm periodicClientAlarm = (PeriodicClientAlarm) alarm;
        if (periodicTasks.containsKey(periodicClientAlarm.getPeriodicAlarmId())) {
          eventQueue.add(new PeriodicClientAlarm(
              periodicClientAlarm.getPeriodicAlarmId(),
              periodicClientAlarm.getPeriodSec(),
              periodicClientAlarm.getInstant().plusSeconds(periodicClientAlarm.getPeriodSec()),
              periodicTasks.get(periodicClientAlarm.getPeriodicAlarmId())));
        } else {
          // Ignore the periodic alarm if it is unscheduled already
          return lifeCycleState;
        }
      }
    }

    synchronized (this) {
      if (alarm.getInstant().compareTo(currTime) < 0) {
        LOG.log(Level.WARNING, "Alarm went backward in time! Alarm time: [" + alarm.getInstant() + "]," +
            " current time: [" + currTime + "]. Ignoring the alarm!");
        return lifeCycleState;
      }

      currTime = currTime.compareTo(alarm.getInstant()) > 0 ? currTime : alarm.getInstant();
    }

    alarm.handleAlarm();

    return lifeCycleState;
  }

  @Override
  public synchronized void start() {
    assert currTime != null;
    assert simulationStartTime != null;

    LOG.log(Level.INFO, String.format("Simulation clock starting at %s!", currTime));
    super.start();
    if (simulationDurationMinutes != null) {
      LOG.log(Level.INFO, "Simulation scheduled to run for " + simulationDurationMinutes + " minutes.");
      scheduleShutdown(simulationDurationMinutes * 60);
    }

    LOG.log(Level.INFO, "Running clock start events...");
    for (final Runnable clockStartEvents : onClockStart.values()) {
      clockStartEvents.run();
    }

    onClockStart.clear();
    LOG.log(Level.INFO, "Done running clock start events!");
  }

  @Override
  public synchronized void stop() {
    LOG.log(Level.INFO, String.format("Simulation clock stopped at %s!", currTime));
    super.stop();
  }

  @Override
  public void schedule(
      final Runnable runnable, final long delay, final TimeUnit timeUnit) {
    final long seconds = TimeUnit.SECONDS.convert(delay, timeUnit);
    scheduleAlarm(seconds, alarm -> runnable.run());
  }

  @Override
  public void scheduleWithFixedDelay(
      final Runnable runnable, final long initialDelay, final long delay, final TimeUnit timeUnit) {
    final long initialDelaySeconds = TimeUnit.SECONDS.convert(initialDelay, timeUnit);
    final long delaySeconds = TimeUnit.SECONDS.convert(delay, timeUnit);
    scheduleAlarm(initialDelaySeconds, alarm -> {
      runnable.run();
      schedulePeriodicAlarm(delaySeconds, periodicAlarm -> runnable.run());
    });
  }

  @Override
  public boolean isShutdown() {
    return lifeCycleState.isDone();
  }

  @Override
  public void shutdown() {
    stop();
  }
}

