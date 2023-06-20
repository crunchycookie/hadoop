package org.apache.hadoop.yarn.dtss.metrics;

import com.codahale.metrics.*;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.yarn.dtss.time.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Writes metrics in CSV when called on by the clock.
 */
@InterfaceAudience.Private
public final class MetricsCsvReporter implements Reporter {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsCsvReporter.class);
  private static final Logger LOGGER = LoggerFactory.getLogger(CsvReporter.class);
  private static final Charset UTF_8 = Charset.forName("UTF-8");
  private final File directory;
  private final Locale locale;
  private final CsvFileProvider csvFileProvider;

  private final MetricRegistry registry;
  private final MetricFilter filter;
  private final long durationFactor;
  private final String durationUnit;
  private final long rateFactor;
  private final String rateUnit;
  private final Clock clock;

  private UUID alarmId = null;

  private MetricsCsvReporter(
      final Clock clock,
      final MetricRegistry registry,
      final MetricFilter filter,
      final TimeUnit rateUnit,
      final TimeUnit durationUnit,
      final File directory,
      final Locale locale,
      final CsvFileProvider csvFileProvider) {
    this.clock = clock;
    this.registry = registry;
    this.filter = filter;
    this.rateFactor = rateUnit.toSeconds(1L);
    this.rateUnit = this.calculateRateUnit(rateUnit);
    this.durationFactor = durationUnit.toNanos(1L);
    this.durationUnit = durationUnit.toString().toLowerCase(Locale.US);
    this.directory = directory;
    this.locale = locale;
    this.csvFileProvider = csvFileProvider;
  }

  public void start(long period, TimeUnit unit) {
    this.start(period, period, unit);
  }

  public synchronized void start(long initialDelay, long period, TimeUnit unit) {
    final long initialDelaySec = TimeUnit.SECONDS.convert(initialDelay, unit);
    final long periodSec = TimeUnit.SECONDS.convert(period, unit);

    alarmId = clock.schedulePeriodicAlarm(initialDelaySec, periodSec, alarm -> {
      try {
        MetricsCsvReporter.this.report();
      } catch (Throwable var2) {
        MetricsCsvReporter.LOG.error(
            "Exception thrown from {}#report. Exception was suppressed.",
            MetricsCsvReporter.this.getClass().getSimpleName(), var2);
      }
    });
  }

  public void stop() {
    clock.cancelAlarm(alarmId);
  }

  public void close() {
    this.stop();
  }

  public void report() {
    synchronized(this) {
      this.report(
          this.registry.getGauges(this.filter),
          this.registry.getCounters(this.filter),
          this.registry.getHistograms(this.filter),
          this.registry.getMeters(this.filter),
          this.registry.getTimers(this.filter));
    }
  }

  public void report(
      final SortedMap<String, Gauge> gauges,
      final SortedMap<String, Counter> counters,
      final SortedMap<String, Histogram> histograms,
      final SortedMap<String, Meter> meters,
      final SortedMap<String, com.codahale.metrics.Timer> timers) {
    long timestamp = TimeUnit.MILLISECONDS.toSeconds(this.clock.getTime());
    Iterator var8 = gauges.entrySet().iterator();

    Map.Entry entry;
    while(var8.hasNext()) {
      entry = (Map.Entry)var8.next();
      this.reportGauge(timestamp, (String)entry.getKey(), (Gauge)entry.getValue());
    }

    var8 = counters.entrySet().iterator();

    while(var8.hasNext()) {
      entry = (Map.Entry)var8.next();
      this.reportCounter(timestamp, (String)entry.getKey(), (Counter)entry.getValue());
    }

    var8 = histograms.entrySet().iterator();

    while(var8.hasNext()) {
      entry = (Map.Entry)var8.next();
      this.reportHistogram(timestamp, (String)entry.getKey(), (Histogram)entry.getValue());
    }

    var8 = meters.entrySet().iterator();

    while(var8.hasNext()) {
      entry = (Map.Entry)var8.next();
      this.reportMeter(timestamp, (String)entry.getKey(), (Meter)entry.getValue());
    }

    var8 = timers.entrySet().iterator();

    while(var8.hasNext()) {
      entry = (Map.Entry)var8.next();
      this.reportTimer(timestamp, (String)entry.getKey(), (com.codahale.metrics.Timer)entry.getValue());
    }

  }

  private void reportTimer(long timestamp, String name, com.codahale.metrics.Timer timer) {
    Snapshot snapshot = timer.getSnapshot();
    this.report(timestamp, name, "count,max,mean,min,stddev,p50,p75,p95,p98,p99,p999,mean_rate,m1_rate,m5_rate,m15_rate,rate_unit,duration_unit", "%d,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,calls/%s,%s", timer.getCount(), this.convertDuration((double)snapshot.getMax()), this.convertDuration(snapshot.getMean()), this.convertDuration((double)snapshot.getMin()), this.convertDuration(snapshot.getStdDev()), this.convertDuration(snapshot.getMedian()), this.convertDuration(snapshot.get75thPercentile()), this.convertDuration(snapshot.get95thPercentile()), this.convertDuration(snapshot.get98thPercentile()), this.convertDuration(snapshot.get99thPercentile()), this.convertDuration(snapshot.get999thPercentile()), this.convertRate(timer.getMeanRate()), this.convertRate(timer.getOneMinuteRate()), this.convertRate(timer.getFiveMinuteRate()), this.convertRate(timer.getFifteenMinuteRate()), this.getRateUnit(), this.getDurationUnit());
  }

  private void reportMeter(long timestamp, String name, Meter meter) {
    this.report(timestamp, name, "count,mean_rate,m1_rate,m5_rate,m15_rate,rate_unit", "%d,%f,%f,%f,%f,events/%s", meter.getCount(), this.convertRate(meter.getMeanRate()), this.convertRate(meter.getOneMinuteRate()), this.convertRate(meter.getFiveMinuteRate()), this.convertRate(meter.getFifteenMinuteRate()), this.getRateUnit());
  }

  private void reportHistogram(long timestamp, String name, Histogram histogram) {
    Snapshot snapshot = histogram.getSnapshot();
    this.report(timestamp, name, "count,max,mean,min,stddev,p50,p75,p95,p98,p99,p999", "%d,%d,%f,%d,%f,%f,%f,%f,%f,%f,%f", histogram.getCount(), snapshot.getMax(), snapshot.getMean(), snapshot.getMin(), snapshot.getStdDev(), snapshot.getMedian(), snapshot.get75thPercentile(), snapshot.get95thPercentile(), snapshot.get98thPercentile(), snapshot.get99thPercentile(), snapshot.get999thPercentile());
  }

  private void reportCounter(long timestamp, String name, Counter counter) {
    this.report(timestamp, name, "count", "%d", counter.getCount());
  }

  private void reportGauge(long timestamp, String name, Gauge gauge) {
    this.report(timestamp, name, "value", "%s", gauge.getValue());
  }

  private void report(long timestamp, String name, String header, String line, Object... values) {
    try {
      File file = this.csvFileProvider.getFile(this.directory, name);
      boolean fileAlreadyExists = file.exists();
      if (fileAlreadyExists || file.createNewFile()) {
        PrintWriter out = new PrintWriter(new OutputStreamWriter(new FileOutputStream(file, true), UTF_8));

        try {
          if (!fileAlreadyExists) {
            out.println("t," + header);
          }

          out.printf(this.locale, String.format(this.locale, "%d,%s%n", timestamp, line), values);
        } finally {
          out.close();
        }
      }
    } catch (IOException var14) {
      LOGGER.warn("Error writing to {}", name, var14);
    }
  }

  protected String getRateUnit() {
    return this.rateUnit;
  }

  protected String getDurationUnit() {
    return this.durationUnit;
  }

  protected double convertDuration(double duration) {
    return duration / (double)this.durationFactor;
  }

  protected double convertRate(double rate) {
    return rate * (double)this.rateFactor;
  }

  private String calculateRateUnit(TimeUnit unit) {
    String s = unit.toString().toLowerCase(Locale.US);
    return s.substring(0, s.length() - 1);
  }

  public static Builder newBuilder(final MetricRegistry registry, final Clock clock) {
    return new Builder(registry, clock);
  }

  public static class Builder {
    private final MetricRegistry registry;
    private final Clock clock;
    private Locale locale;
    private TimeUnit rateUnit;
    private TimeUnit durationUnit;
    private MetricFilter filter;
    private CsvFileProvider csvFileProvider;

    private Builder(MetricRegistry registry, Clock clock) {
      this.registry = registry;
      this.clock = clock;
      this.locale = Locale.getDefault();
      this.rateUnit = TimeUnit.SECONDS;
      this.durationUnit = TimeUnit.MILLISECONDS;
      this.filter = MetricFilter.ALL;
      this.csvFileProvider = new FixedNameCsvFileProvider();
    }

    public MetricsCsvReporter.Builder formatFor(Locale locale) {
      this.locale = locale;
      return this;
    }

    public MetricsCsvReporter.Builder convertRatesTo(TimeUnit rateUnit) {
      this.rateUnit = rateUnit;
      return this;
    }

    public MetricsCsvReporter.Builder convertDurationsTo(TimeUnit durationUnit) {
      this.durationUnit = durationUnit;
      return this;
    }

    public MetricsCsvReporter.Builder filter(MetricFilter filter) {
      this.filter = filter;
      return this;
    }

    public MetricsCsvReporter.Builder withCsvFileProvider(CsvFileProvider csvFileProvider) {
      this.csvFileProvider = csvFileProvider;
      return this;
    }

    public MetricsCsvReporter build(File directory) {
      return new MetricsCsvReporter(
          clock,
          registry,
          filter,
          rateUnit,
          durationUnit,
          directory,
          locale,
          csvFileProvider);
    }
  }
}
