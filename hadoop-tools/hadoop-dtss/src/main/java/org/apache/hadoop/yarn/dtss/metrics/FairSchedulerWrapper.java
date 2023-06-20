package org.apache.hadoop.yarn.dtss.metrics;

import com.codahale.metrics.Timer;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ContainerUpdates;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;

import java.util.List;

/**
 * Wrapper around the {@link FairScheduler} to enable discrete event-driven simulation.
 * Wires metrics for scheduler.
 */
public final class FairSchedulerWrapper extends FairScheduler implements SchedulerWrapper {
  private MetricsManager metricsManager;

  public FairSchedulerWrapper() {
  }

  @Override
  public void setMetricsManager(final MetricsManager metricsManager) {
    this.metricsManager = metricsManager;
  }

  @Override public MetricsManager getMetricsManager() {
    return metricsManager;
  }

  @Override
  public Allocation allocate(ApplicationAttemptId attemptId,
      List<ResourceRequest> resourceRequests,
      List<SchedulingRequest> schedulingRequests, List<ContainerId> containerIds,
      List<String> blacklistAdditions, List<String> blacklistRemovals,
      ContainerUpdates updateRequests) {
    if (metricsManager.isMetricsOn()) {
      final SchedulerApplication app = (SchedulerApplication) ((AbstractYarnScheduler) this).getSchedulerApplications()
          .get(attemptId.getApplicationId());

      metricsManager.registerQueueMetricsIfNew(app.getQueue());

      Timer.Context allocateContext = null;
      if (metricsManager.getSchedulerAllocateTimer() != null) {
        allocateContext = metricsManager.getSchedulerAllocateTimer().time();
      }

      try {
        return super.allocate(attemptId, resourceRequests,
            schedulingRequests, containerIds,
            blacklistAdditions, blacklistRemovals, updateRequests);
      } finally {
        if (allocateContext != null) {
          allocateContext.stop();
        }
        metricsManager.increaseSchedulerAllocationCounter();
      }
    } else {
      return super.allocate(attemptId, resourceRequests, schedulingRequests,
          containerIds,
          blacklistAdditions, blacklistRemovals, updateRequests);
    }
  }

  @Override
  public void handle(SchedulerEvent schedulerEvent) {
    // metrics off
    if (!metricsManager.isMetricsOn()) {
      super.handle(schedulerEvent);
      return;
    }

    Timer.Context handlerTimer = null;
    Timer.Context operationTimer = null;

    try {
      if (metricsManager.getSchedulerHandleTimer() != null) {
        handlerTimer = metricsManager.getSchedulerHandleTimer().time();
      }

      if (metricsManager.getSchedulerHandleTimer(schedulerEvent.getType()) != null) {
        operationTimer = metricsManager.getSchedulerHandleTimer(schedulerEvent.getType()).time();
      }

      super.handle(schedulerEvent);
    } finally {
      if (handlerTimer != null) {
        handlerTimer.stop();
      }
      if (operationTimer != null) {
        operationTimer.stop();
      }
      metricsManager.increaseSchedulerHandleCounter(schedulerEvent.getType());
    }
  }
}
