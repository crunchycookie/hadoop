package org.apache.hadoop.yarn.dtss.job;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.records.Priority;

/**
 * Object used when submitting jobs.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public final class JobSubmissionParameters {
  private final Builder builder;

  private JobSubmissionParameters(final Builder builder) {
    this.builder = builder;
  }

  boolean getIsUnmanaged() {
    return builder.isUnmanaged;
  }

  int getMasterMemory() {
    return builder.masterMemory;
  }

  String getName() {
    return builder.name;
  }

  String getUser() {
    return builder.user;
  }

  String getQueue() {
    return builder.queue;
  }

  Priority getPriority() {
    return builder.priority;
  }

  public static final class Builder {
    private boolean isUnmanaged = false;
    private int masterMemory = 1;
    private String name;
    private String user;
    private String queue = null;
    private Priority priority = null;

    private Builder() { }

    static Builder newInstance() {
      return new Builder();
    }

    Builder setQueue(final String queue) {
      this.queue = queue;
      return this;
    }

    Builder setUser(final String user) {
      this.user = user;
      return this;
    }

    Builder setName(final String name) {
      this.name = name;
      return this;
    }

    Builder setIsUnmanaged(final boolean isUnmanaged) {
      this.isUnmanaged = isUnmanaged;
      return this;
    }

    Builder setMasterMemory(final int masterMemory) {
      this.masterMemory = masterMemory;
      return this;
    }

    Builder setPriority(final Priority priority) {
      this.priority = priority;
      return this;
    }

    JobSubmissionParameters build() {
      return new JobSubmissionParameters(new Builder()
          .setMasterMemory(masterMemory)
          .setIsUnmanaged(isUnmanaged)
          .setName(name)
          .setUser(user)
          .setQueue(queue)
          .setPriority(priority)
      );
    }
  }
}
