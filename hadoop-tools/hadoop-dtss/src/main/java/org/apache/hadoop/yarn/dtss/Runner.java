package org.apache.hadoop.yarn.dtss;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import com.google.inject.AbstractModule;
import com.google.inject.util.Modules;
import org.apache.commons.cli.*;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.dtss.config.SimulatorModule;
import org.apache.hadoop.yarn.dtss.config.parameters.runner.RunId;
import org.apache.hadoop.yarn.dtss.config.parameters.runner.SimulationConfigPath;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

/**
 * Houses the main class to {@link Launcher}, which begins the simulation.
 */
@InterfaceAudience.Private
public final class Runner {
  private static final Logger LOG = Logger.getLogger(Runner.class.getName());
  private static String runId = DateTimeFormatter.ofPattern(
      "yyyy-MM-dd-HH-mm-ss").format(LocalDateTime.now());

  @VisibleForTesting
  public static final class RunnerModule extends AbstractModule {
    private final String configPath;
    private final String runId;

    @VisibleForTesting
    public static RunnerModule newModule(final String configPath, final String runId) {
      return new RunnerModule(configPath, runId);
    }

    private RunnerModule(final String configPath, final String runId) {
      this.configPath = configPath;
      this.runId = runId;
    }

    @Override
    protected void configure() {
      bind(String.class).annotatedWith(SimulationConfigPath.class).toInstance(configPath);
      bind(String.class).annotatedWith(RunId.class).toInstance(runId);
    }
  }

  private static Options commandLineOptions() {
    final Options options = new Options();
    options
        .addOption("i", "id", true, "The Run ID of the job.")
        .addOption("m", "metrics", true, "The metrics output directory");

    return options;
  }

  @InterfaceStability.Unstable
  public static void runSimulator(
      final SimulatorModule simulatorModule, final RunnerModule runnerModule){
    LOG.log(Level.INFO, String.format(
        "Read configuration\n%s",
        ReflectionToStringBuilder.toString(simulatorModule, ToStringStyle.MULTI_LINE_STYLE)
    ));

    final Level logLevel = simulatorModule.getSimulatorLogLevel();
    LOG.log(Level.INFO, "Log level set to " + logLevel + ".");

    setSimulatorLogLevel(logLevel);

    Launcher.launch(Modules.combine(simulatorModule, runnerModule));
  }

  public static void main(final String[] args) {
    // Sets the Apache logger level
    org.apache.log4j.Logger.getRootLogger().setLevel(org.apache.log4j.Level.WARN);
    BasicConfigurator.configure();

    final String configPath;
    final String metricsOutputDirectory;
    final CommandLineParser parser = new BasicParser();
    try {
      final CommandLine cmd = parser.parse(commandLineOptions(), args);

      if (cmd.hasOption('i')) {
        runId = cmd.getOptionValue('i');
        if (runId == null || runId.trim().isEmpty()) {
          throw new IllegalArgumentException("Run ID cannot be set to empty!");
        }
      }

      if (cmd.hasOption('m')) {
        metricsOutputDirectory = cmd.getOptionValue('m');
        if (metricsOutputDirectory == null || metricsOutputDirectory.trim().isEmpty()){
          throw new IllegalArgumentException("Metrics output directory cannot be empty!");
        }
      } else {
        metricsOutputDirectory = null;
      }

      final String[] restOfArgs = cmd.getArgs();

      assert restOfArgs.length == 1 : "Should only have one command line argument holding " +
          "the path to the configuration file.";
      configPath = restOfArgs[0];
    } catch (final ParseException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    LOG.log(Level.INFO, "Running simulation with ID " + runId + ".");
    LOG.log(Level.INFO, String.format("Reading configuration from %s", configPath));

    final SimulatorModule simulatorModule;
    try {
      simulatorModule = SimulatorModule.newReader().readConfiguration(configPath);
      if (metricsOutputDirectory != null) {
        LOG.log(Level.INFO, "Overwriting metrics output directory to " + metricsOutputDirectory);
        simulatorModule.setMetricsOutputDirectory(metricsOutputDirectory);
      }
    } catch (final IOException e) {
      LOG.log(
          Level.SEVERE,
          () -> String.format("Unable to read simulation configuration at %s", configPath)
      );

      throw new RuntimeException(e);
    }

    final RunnerModule runnerModule = new RunnerModule(configPath, runId);
    try {
      runSimulator(simulatorModule, runnerModule);
    } catch (final AssertionError assertionError) {
      LOG.log(Level.SEVERE,
          "Assertion failed with message: " + assertionError);
      throw assertionError;
    } catch (final Throwable e) {
      LOG.log(Level.SEVERE,
          "Simulation failed with throwable: " + e);
      throw new RuntimeException(e);
    }
  }

  private static void setSimulatorLogLevel(final Level logLevel) {
    final Logger rootLogger = LogManager.getLogManager().getLogger("");
    rootLogger.setLevel(logLevel);
    for (final Handler h : rootLogger.getHandlers()) {
      h.setLevel(logLevel);
    }
  }
}
