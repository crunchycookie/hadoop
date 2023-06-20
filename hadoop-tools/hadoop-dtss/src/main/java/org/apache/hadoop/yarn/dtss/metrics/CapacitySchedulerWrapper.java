package org.apache.hadoop.yarn.dtss.metrics;

import com.codahale.metrics.Timer;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ContainerUpdates;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;

import java.util.List;

/**
 * Wrapper around the {@link CapacityScheduler} to enable event-driven simulation.
 * Wires metrics for the scheduler.
 */
@InterfaceAudience.Private
public final class CapacitySchedulerWrapper extends CapacityScheduler implements SchedulerWrapper {
  private MetricsManager metricsManager;

  public CapacitySchedulerWrapper() {
  }

  @Override
  public void setMetricsManager(final MetricsManager metricsManager) {
    this.metricsManager = metricsManager;
  }

  @Override public MetricsManager getMetricsManager() {
    return metricsManager;
  }

  @Override
  public Allocation allocate(
      ApplicationAttemptId applicationAttemptId,
      List<ResourceRequest> resourceRequests,
      List<SchedulingRequest> schedulingRequests,
      List<ContainerId> containerIds,
      List<String> strings,
      List<String> strings2,
      ContainerUpdates updateRequests) {
    if (metricsManager.isMetricsOn()) {
      final SchedulerApplication app = (SchedulerApplication) ((AbstractYarnScheduler) this).getSchedulerApplications()
          .get(applicationAttemptId.getApplicationId());

      metricsManager.registerQueueMetricsIfNew(app.getQueue());

      Timer.Context allocateContext = null;
      if (metricsManager.getSchedulerAllocateTimer() != null) {
        allocateContext = metricsManager.getSchedulerAllocateTimer().time();
      }

      try {
        return super.allocate(
            applicationAttemptId,
            resourceRequests,
            schedulingRequests,
            containerIds,
            strings,
            strings2,
            updateRequests
        );
      } finally {
        if (allocateContext != null) {
          allocateContext.stop();
        }
        metricsManager.increaseSchedulerAllocationCounter();
      }
    } else {
      return super.allocate(applicationAttemptId, resourceRequests, schedulingRequests,
          containerIds, strings,
          strings2, updateRequests);
    }
  }

  @Override
  public void handle(SchedulerEvent schedulerEvent) {
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
