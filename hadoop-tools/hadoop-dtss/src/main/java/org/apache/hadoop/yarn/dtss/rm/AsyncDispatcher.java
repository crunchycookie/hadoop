//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package org.apache.hadoop.yarn.dtss.rm;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import java.lang.Thread.State;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

/**
 * NOTE: Based on classes that support MockRM in the tests package
 * for resourcemanager.
 */
public class AsyncDispatcher extends AbstractService implements Dispatcher {
  private static final Log LOG = LogFactory.getLog(AsyncDispatcher.class);
  private final BlockingQueue<Event> eventQueue;
  private volatile int lastEventQueueSizeLogged;
  private volatile boolean stopped;
  private volatile boolean drainEventsOnStop;
  private volatile boolean drained;
  private final Object waitForDrained;
  private volatile boolean blockNewEvents;
  private final EventHandler<Event> handlerInstance;
  private Thread eventHandlingThread;
  protected final Map<Class<? extends Enum>, EventHandler> eventDispatchers;
  private boolean exitOnDispatchException;
  private String dispatcherThreadName;

  public AsyncDispatcher() {
    this(new LinkedBlockingQueue());
  }

  public AsyncDispatcher(BlockingQueue<Event> eventQueue) {
    super("Dispatcher");
    this.lastEventQueueSizeLogged = 0;
    this.stopped = false;
    this.drainEventsOnStop = false;
    this.drained = true;
    this.waitForDrained = new Object();
    this.blockNewEvents = false;
    this.handlerInstance = new AsyncDispatcher.GenericEventHandler();
    this.exitOnDispatchException = true;
    this.dispatcherThreadName = "AsyncDispatcher event handler";
    this.eventQueue = eventQueue;
    this.eventDispatchers = new HashMap();
  }

  public AsyncDispatcher(String dispatcherName) {
    this();
    this.dispatcherThreadName = dispatcherName;
  }

  Runnable createThread() {
    return new Runnable() {
      public void run() {
        while(!AsyncDispatcher.this.stopped && !Thread.currentThread().isInterrupted()) {
          AsyncDispatcher.this.drained = AsyncDispatcher.this.eventQueue.isEmpty();
          if (AsyncDispatcher.this.blockNewEvents) {
            synchronized(AsyncDispatcher.this.waitForDrained) {
              if (AsyncDispatcher.this.drained) {
                AsyncDispatcher.this.waitForDrained.notify();
              }
            }
          }

          Event event;
          try {
            event = (Event)AsyncDispatcher.this.eventQueue.take();
          } catch (InterruptedException var4) {
            if (!AsyncDispatcher.this.stopped) {
              AsyncDispatcher.LOG.warn("AsyncDispatcher thread interrupted", var4);
            }

            return;
          }

          if (event != null) {
            AsyncDispatcher.this.dispatch(event);
          }
        }
      }
    };
  }

  @VisibleForTesting
  public void disableExitOnDispatchException() {
    this.exitOnDispatchException = false;
  }

  protected void serviceStart() throws Exception {
    super.serviceStart();
    this.eventHandlingThread = new Thread(this.createThread());
    this.eventHandlingThread.setName(this.dispatcherThreadName);
    this.eventHandlingThread.start();
  }

  public void setDrainEventsOnStop() {
    this.drainEventsOnStop = true;
  }

  protected void serviceStop() throws Exception {
    if (this.drainEventsOnStop) {
      this.blockNewEvents = true;
      LOG.info("AsyncDispatcher is draining to stop, ignoring any new events.");
      long endTime = System.currentTimeMillis() + this.getConfig().getLong("yarn.dispatcher.drain-events.timeout", 300000L);
      Object var3 = this.waitForDrained;
      synchronized(this.waitForDrained) {
        while(!this.isDrained() && this.eventHandlingThread != null && this.eventHandlingThread.isAlive() && System.currentTimeMillis() < endTime) {
          this.waitForDrained.wait(100L);
          LOG.info("Waiting for AsyncDispatcher to drain. Thread state is :" + this.eventHandlingThread.getState());
        }
      }
    }

    this.stopped = true;
    if (this.eventHandlingThread != null) {
      this.eventHandlingThread.interrupt();

      try {
        this.eventHandlingThread.join();
      } catch (InterruptedException var5) {
        LOG.warn("Interrupted Exception while stopping", var5);
      }
    }

    super.serviceStop();
  }

  protected void dispatch(Event event) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Dispatching the event " + event.getClass().getName() + "." + event.toString());
    }

    Class type = event.getType().getDeclaringClass();

    try {
      EventHandler handler = (EventHandler)this.eventDispatchers.get(type);
      if (handler == null) {
        throw new Exception("No handler for registered for " + type);
      }

      handler.handle(event);
    } catch (Throwable var5) {
      LOG.fatal("Error in dispatcher thread", var5);
      if (this.exitOnDispatchException && !ShutdownHookManager.get().isShutdownInProgress() && !this.stopped) {
        this.stopped = true;
        Thread shutDownThread = new Thread(this.createShutDownThread());
        shutDownThread.setName("AsyncDispatcher ShutDown handler");
        shutDownThread.start();
      }
    }

  }

  public void register(Class<? extends Enum> eventType, EventHandler handler) {
    EventHandler<Event> registeredHandler = (EventHandler)this.eventDispatchers.get(eventType);
    LOG.info("Registering " + eventType + " for " + handler.getClass());
    if (registeredHandler == null) {
      this.eventDispatchers.put(eventType, handler);
    } else {
      AsyncDispatcher.MultiListenerHandler multiHandler;
      if (!(registeredHandler instanceof AsyncDispatcher.MultiListenerHandler)) {
        multiHandler = new AsyncDispatcher.MultiListenerHandler();
        multiHandler.addHandler(registeredHandler);
        multiHandler.addHandler(handler);
        this.eventDispatchers.put(eventType, multiHandler);
      } else {
        multiHandler = (AsyncDispatcher.MultiListenerHandler)registeredHandler;
        multiHandler.addHandler(handler);
      }
    }

  }

  public EventHandler<Event> getEventHandler() {
    return this.handlerInstance;
  }

  Runnable createShutDownThread() {
    return new Runnable() {
      public void run() {
        AsyncDispatcher.LOG.info("Exiting, bbye..");
        System.exit(-1);
      }
    };
  }

  @VisibleForTesting
  protected boolean isEventThreadWaiting() {
    return this.eventHandlingThread.getState() == State.WAITING;
  }

  protected boolean isDrained() {
    return this.drained;
  }

  protected boolean isStopped() {
    return this.stopped;
  }

  static class MultiListenerHandler implements EventHandler<Event> {
    List<EventHandler<Event>> listofHandlers = new ArrayList();

    public MultiListenerHandler() {
    }

    public void handle(Event event) {
      Iterator var2 = this.listofHandlers.iterator();

      while(var2.hasNext()) {
        EventHandler<Event> handler = (EventHandler)var2.next();
        handler.handle(event);
      }

    }

    void addHandler(EventHandler<Event> handler) {
      this.listofHandlers.add(handler);
    }
  }

  class GenericEventHandler implements EventHandler<Event> {
    GenericEventHandler() {
    }

    public void handle(Event event) {
      if (!AsyncDispatcher.this.blockNewEvents) {
        AsyncDispatcher.this.drained = false;
        int qSize = AsyncDispatcher.this.eventQueue.size();
        if (qSize != 0 && qSize % 1000 == 0 && AsyncDispatcher.this.lastEventQueueSizeLogged != qSize) {
          AsyncDispatcher.this.lastEventQueueSizeLogged = qSize;
          AsyncDispatcher.LOG.info("Size of event-queue is " + qSize);
        }

        int remCapacity = AsyncDispatcher.this.eventQueue.remainingCapacity();
        if (remCapacity < 1000) {
          AsyncDispatcher.LOG.warn("Very low remaining capacity in the event-queue: " + remCapacity);
        }

        try {
          AsyncDispatcher.this.eventQueue.put(event);
        } catch (InterruptedException var5) {
          if (!AsyncDispatcher.this.stopped) {
            AsyncDispatcher.LOG.warn("AsyncDispatcher thread interrupted", var5);
          }

          AsyncDispatcher.this.drained = AsyncDispatcher.this.eventQueue.isEmpty();
          throw new YarnRuntimeException(var5);
        }
      }
    }
  }
}
