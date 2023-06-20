package org.apache.hadoop.yarn.dtss.metrics;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.yarn.dtss.config.parameters.IsMetricsOn;
import org.apache.hadoop.yarn.dtss.config.parameters.MetricsSet;
import org.apache.hadoop.yarn.dtss.config.parameters.RootMetricsOutputDirectory;
import org.apache.hadoop.yarn.dtss.config.parameters.runner.RunId;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Singleton;
import java.nio.file.Paths;
import java.util.Set;

/**
 * The configuration for metrics.
 */
@InterfaceAudience.Private
@Singleton
public final class MetricsConfiguration {
  private final String runId;
  private final String rootMetricsOutputDir;
  private final String metricsOutputDir;
  private final boolean isMetricsOn;
  private final Set<String> metricsSet;

  @Inject
  private MetricsConfiguration(
      @IsMetricsOn final boolean isMetricsOn,
      @RunId final String runId,
      @MetricsSet final Set<String> metricsSet,
      @Nullable @RootMetricsOutputDirectory final String rootMetricsOutputDir) {
    this.runId = runId;
    this.isMetricsOn = isMetricsOn;
    this.metricsSet = metricsSet;

    if (isMetricsOn) {
      if (rootMetricsOutputDir == null || rootMetricsOutputDir.trim().isEmpty()) {
        throw new IllegalArgumentException("RootMetricsOutputDirectory must be configured if metrics is on!");
      }

      this.rootMetricsOutputDir = rootMetricsOutputDir;
      this.metricsOutputDir = Paths.get(rootMetricsOutputDir, runId).toString();
    } else {
      this.rootMetricsOutputDir = null;
      this.metricsOutputDir = null;
    }
  }

  public String getMetricsDirectory() {
    return metricsOutputDir;
  }

  public String getMetricsFilePath(final String... pathNames) {
    if (isMetricsOn) {
      return Paths.get(metricsOutputDir, pathNames).toAbsolutePath().toString();
    }

    return null;
  }

  public String getRunId() {
    return runId;
  }

  public String getRootMetricsOutputDir() {
    return rootMetricsOutputDir;
  }

  public boolean isMetricsOn() {
    return isMetricsOn;
  }

  public Set<String> getMetricsSet() {
    return metricsSet;
  }

  public boolean metricsContains(final String metric) {
    if (metricsSet == null || metricsSet.isEmpty()) {
      return true;
    }

    return metricsSet.contains(metric);
  }
}
