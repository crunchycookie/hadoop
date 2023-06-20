package org.apache.hadoop.yarn.dtss.trace.results;

import java.util.function.BiFunction;

public final class MetricInvariantCheck<T, V> {
  private final BiFunction<T, V, Boolean> checker;
  private final BiFunction<T, V, String> errorMessage;
  private final String checkName;

  public MetricInvariantCheck(final BiFunction<T, V, Boolean> checker,
                              final String checkName) {
    this(checker, checkName, null);
  }

  public MetricInvariantCheck(final BiFunction<T, V, Boolean> checker,
                              final String checkName,
                              final BiFunction<T, V, String> errorMessage) {
    this.checker = checker;
    this.checkName = checkName;
    this.errorMessage = errorMessage;
  }

  public String getCheckName() {
    return checkName;
  }

  public boolean evaluate(final T time, final V value) {
    return checker.apply(time, value);
  }

  public String getErrorMessage(final T time, final V value) {
    if (errorMessage != null) {
      return "Invariant with name " + checkName + " failed with message:\n" + errorMessage.apply(time, value);
    }

    return "Invariant with name " + checkName + " failed at time " + time + " with value " + value;
  }
}
