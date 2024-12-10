/* /*
 * Morgan Stanley makes this available to you under the Apache License, Version 2.0 (the "License").
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 
package msjava.logbackutils.async.internal;

import java.time.Instant;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;

import msjava.management.jmx.AutoExporter;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.jmx.export.naming.SelfNaming;

import com.codahale.metrics.Metric;
import com.google.common.annotations.VisibleForTesting;

import ch.qos.logback.core.spi.ContextAwareBase;
import msjava.base.lang.StackTraceUtils;
import msjava.base.multimeter.meters.YammerMeter;
import msjava.threadmonitor.thread.MSThreadMonitor;
import msjava.logbackutils.async.AsyncDispatcher;

/** Provides common circular-logging guard functionality for async dispatchers. 
@ManagedResource
public abstract class AsyncDispatcherBasePatch<E> extends ContextAwareBase
    implements AsyncDispatcher<E>, MSThreadMonitor.ExtraInfoProvider, SelfNaming {

  public static final String QUEUE_FULL_WARN_MSG =
      "Application thread is about to block because async logging message queue is full!";

  // how often can we emit a queue full warning for the same thread, in milliseconds
  private static final long QUEUE_FULL_WARN_FREQ =
      Long.getLong("msjava.logbackutils.async.dispatchqueuewarnfreq", 30000);

  private static final boolean DISABLE_DIAGNOSTICS =
      getBoolean("msjava.logbackutils.async.disablediag", true);

  private static final boolean DISABLE_MULTIMETER =
      getBoolean("msjava.logbackutils.async.disablediagmetrics", DISABLE_DIAGNOSTICS);

  private static final boolean DISABLE_THREAD_MONITOR =
      getBoolean("msjava.logbackutils.async.disablediagthreadmonitors", DISABLE_DIAGNOSTICS);

  private static final boolean DISABLE_EVENT_CACHE =
      getBoolean("msjava.logbackutils.async.disablediageventcache", DISABLE_DIAGNOSTICS);

  private static final int EVENT_BACKLOG_LIMIT =
      Integer.getInteger("msjava.logbackutils.async.diageventcachesize", 1000);

  private static final int EVENT_RETENTION_SECONDS =
      Integer.getInteger("msjava.logbackutils.async.diageventcacheretentionseconds", 60 * 60);

  private static final boolean QUEUE_FULL_WARN_TO_SYSERR =
      getBoolean("msjava.logbackutils.async.dispatchqueuewarntosyserr", true);

  private static final long THREAD_BLOCK_INTERVAL =
      Long.getLong("msjava.logbackutils.async.logthreadblockwarnfreq", 30000);

  private static final int EXPIRED_EVENT_CLEANUP_INTERVAL_SECONDS =
      Integer.getInteger("msjava.logbackutils.async.diageventcachecleanupintervalseconds", 300);

  private static final boolean REPORT_UNBLOCKING =
      getBoolean("msjava.logbackutils.async.reportunblocking", true);

  // unfortunately there's no Boolean.getBoolean(String, boolean) similar to e.g.
  // Long.getLong(String, long), so
  // creating one here (not using SystemPropertyUtils because we usually can't even log anything at
  // this point)
  private static boolean getBoolean(String key, boolean defaultValue) {
    final String value = System.getProperty(key);

    if (value != null) {
      if (value.equalsIgnoreCase("true")) {
        return true;
      } else if (value.equalsIgnoreCase("false")) {
        return false;
      }
    }
    return defaultValue;
  }

  private static final ThreadLocal<Boolean> guard = ThreadLocal.withInitial(() -> false);

  protected static Boolean isGuard() {
    return guard.get();
  }

  protected static void setGuard(boolean state) {
    guard.set(state);
  }

  private final ThreadLocal<Long> lastQueueFullWarningTimestamp = ThreadLocal.withInitial(() -> 0L);

  private final AtomicInteger numQueued = new AtomicInteger();

  private int bufferSize = 1000;

  /** used for metrics and for logging 
  private final ConcurrentHashMap<Thread, Long> threadsAndBlockTimes = new ConcurrentHashMap<>();

  private final AtomicLong totalBlockTime = new AtomicLong();

  private final AtomicLong maxBlockTime = new AtomicLong();

  private final AtomicLong blockCount = new AtomicLong();

  private volatile String metricsId;

  private volatile YammerMeter meter;

  private static final AtomicBoolean firstStart = new AtomicBoolean(true);

  /**
   * @param metricsId a unique ID for naming published metrics, not null and not empty
   
  protected AsyncDispatcherBasePatch(String metricsId) {
    this.metricsId = metricsId;

    AutoExporter.registerForExport(this);
  }

  @Override
  public void start() {
    if (firstStart.getAndSet(false)) {
      addInfo(
          "Async dispatcher queue fullness warning frequency globally limited at "
              + QUEUE_FULL_WARN_FREQ
              + " ms (use msjava.logbackutils.async.dispatchqueuewarnfreq system property to override)");
      addInfo(
          "Async dispatcher diagnostics globally "
              + (DISABLE_DIAGNOSTICS ? "disabled" : "enabled")
              + " by default (use msjava.logbackutils.async.disablediag system property to override)");
      addInfo(
          "Async dispatcher multimeter metrics collection globally "
              + (DISABLE_MULTIMETER ? "disabled" : "enabled")
              + " (use msjava.logbackutils.async.disablediagmetrics system property to override, default affected by msjava.logbackutils.async.disablediag)");
      addInfo(
          "Async dispatcher thread monitoring globally "
              + (DISABLE_THREAD_MONITOR ? "disabled" : "enabled")
              + " (use msjava.logbackutils.async.disablediagthreadmonitors system property to override, default affected by msjava.logbackutils.async.disablediag)");
      addInfo(
          "Async dispatcher event caching globally "
              + (DISABLE_EVENT_CACHE ? "disabled" : "enabled")
              + " (use msjava.logbackutils.async.disablediageventcache system property to override, default affected by msjava.logbackutils.async.disablediag)");
      addInfo(
          "Async dispatcher event cache size globally limited at "
              + EVENT_BACKLOG_LIMIT
              + " (use msjava.logbackutils.async.diageventcachesize system property to override)");
      addInfo(
          "Async dispatcher cached event retention time globally limited at "
              + EVENT_RETENTION_SECONDS
              + " s (use msjava.logbackutils.async.diageventcacheretentionseconds system property to override)");
      addInfo(
          "Async dispatcher queue fullness warning copied to syserr globally "
              + (QUEUE_FULL_WARN_TO_SYSERR ? "enabled" : "disabled")
              + " (use msjava.logbackutils.async.dispatchqueuewarntosyserr system property to override)");
      addInfo(
          "Async dispatcher log thread blocked warning frequency globally limited at "
              + THREAD_BLOCK_INTERVAL
              + " ms (use msjava.logbackutils.async.logthreadblockwarnfreq system property to override)");
      addInfo(
          "Async dispatcher event cache cleanup frequency globally set at "
              + EXPIRED_EVENT_CLEANUP_INTERVAL_SECONDS
              + " s (use msjava.logbackutils.async.diageventcachecleanupintervalseconds system property to override)");
      addInfo(
          "Async dispatcher "
              + (REPORT_UNBLOCKING ? "will" : "will not")
              + " report unblocking application thread at Info level"
              + "(use msjava.logbackutils.async.reportunblocking system property to override)");
    }
  }

  public String getMetricsId() {
    return metricsId;
  }

  public void setMetricsId(String metricsId) {
    this.metricsId = metricsId;
  }

  private final Object meter_lock = new Object();

  private YammerMeter getMeter() {
    if (!DISABLE_MULTIMETER && meter == null) {
      synchronized (meter_lock) {
        if (meter == null) {
          YammerMeter m = new YammerMeter("msjava.logbackutils.async.dispatchers." + metricsId);
          try {
            m.registerGauge("queueSize", numQueued::get);
            m.registerGauge("blockedEvent.totalTimeBlockedMs", this::getTotalBlockTime);
            m.registerGauge("blockedEvent.maxTimeBlockedMs", this::getMaxBlockTime);
            m.registerGauge(
                "blockedEvent.avgTimeBlockedMs",
                () -> {
                  long bc = blockCount.get();
                  if (bc == 0L) {
                    return 0L;
                  } else {
                    return getTotalBlockTime() / bc;
                  }
                });
          } catch (Exception e) {
            addError(
                "Failed to configure multimeter metrics for "
                    + metricsId
                    + ", make sure the metricsId is unique if set",
                e);
            assert false
                : "Failed to configure multimeter metrics for "
                    + metricsId
                    + ", make sure the metricsId is unique if set: "
                    + e.toString()
                    + "\n"
                    + StackTraceUtils.stackTraceToString(e.getStackTrace());
          }
          this.meter = m;
        }
      }
    }

    return meter;
  }

  private long getTotalBlockTime() {
    long tbt = totalBlockTime.get();

    long now = System.currentTimeMillis();

    for (Long val : threadsAndBlockTimes.values()) {
      tbt += now - val;
    }

    return tbt;
  }

  private long getMaxBlockTime() {
    long mbt = maxBlockTime.get();

    long now = System.currentTimeMillis();

    for (Long val : threadsAndBlockTimes.values()) {
      mbt = Math.max(mbt, now - val);
    }

    return mbt;
  }

  private void registerThreadUnblocked() {
    Long blockSince = threadsAndBlockTimes.remove(Thread.currentThread());
    if (blockSince != null) {
      long now = System.currentTimeMillis();
      long blockTime = now - blockSince;
      if (!DISABLE_MULTIMETER) {
        totalBlockTime.addAndGet(blockTime);
        setIfGreater(maxBlockTime, blockTime);
      }
      if (REPORT_UNBLOCKING) {
        String msg =
            Thread.currentThread().getName()
                + " - "
                + formatTimestamp(now)
                + " - Application thread has unblocked after "
                + blockTime
                + " ms";
        addInfo(msg);
      }
    }
  }

  private static void setIfGreater(AtomicLong container, long value) {
    while (true) {
      long oldMax = container.get();
      if (value > oldMax) {
        if (container.compareAndSet(oldMax, value)) {
          break;
        }
      } else {
        break;
      }
    }
  }

  /**
   * Call to emit a warning if the application thread will be blocked
   *
   * @deprecated use {@link #emitQueueFullWarning(Object, boolean)}
   
  @Deprecated
  public void emitQueueFullWarning() {
    emitQueueFullWarning(null, false);
  }

  /**
   * Call to emit a warning if the queue is full - the application thread will be blocked or the
   * message will be thrown away. Make sure to call {@link #eventQueued(Object, boolean)} or {@link
   * #eventInterrupted(Object)} once it's unblocked!
   *
   * @param event
   * @param willThrowAway rather than block, drop the event.
   
  public void emitQueueFullWarning(E event, boolean willThrowAway) {
    try {
      if (event != null) {
        addEvent(event, willThrowAway);
        if (!willThrowAway) {
          registerThreadWatcher();
        }
      }

      long now = System.currentTimeMillis();
      if (!willThrowAway) {
        registerBlocking(now);
      }

      if (lastQueueFullWarningTimestamp.get() < now - QUEUE_FULL_WARN_FREQ) {
        lastQueueFullWarningTimestamp.set(now);

        String nows = formatTimestamp(now);
        String warnMessage =
            event == null
                ? (nows + " - " + QUEUE_FULL_WARN_MSG)
                : (nows
                    + " - Async logging message queue for "
                    + metricsId
                    + " is full (size="
                    + getBufferSize()
                    + ") -- "
                    + (willThrowAway
                        ? "event will be thrown away: " + event
                        : "application thread will block"));

        addWarn(warnMessage);
        if (QUEUE_FULL_WARN_TO_SYSERR) {
          System.err.println(Thread.currentThread().toString() + " - " + warnMessage);
        }
      }

      if (!DISABLE_MULTIMETER) {
        String metricsKey = willThrowAway ? "droppedEvent" : "blockedEvent";
        getMeter().getCounter(metricsKey + ".totalCount").incrByOne();
        getMeter().getThroughputMonitor(metricsKey + ".rate").mark();
      }
    } catch (Exception e) {
      addError("Unexpected exception caught while emitting a queue full warning", e);
      assert false
          : "Unexpected exception caught while emitting a queue full warning: "
              + e.toString()
              + "\n"
              + StackTraceUtils.stackTraceToString(e.getStackTrace());
    }
  }

  private void registerBlocking(long now) {
    if (!DISABLE_MULTIMETER || REPORT_UNBLOCKING) {
      threadsAndBlockTimes.put(Thread.currentThread(), now);
      blockCount.incrementAndGet();
    }
  }

  /**
   * Call for metrics gathering after an event has been added to the event queue.
   *
   * @param event
   
  protected void eventQueued(E event, boolean wasBlocked) {
    try {
      if (!DISABLE_MULTIMETER) this.numQueued.incrementAndGet();
      addEvent(event, false);
      unregisterThreadWatcher();
      if (wasBlocked) registerThreadUnblocked();

      if (!DISABLE_MULTIMETER) {
        getMeter().getCounter("queuedEvent.totalCount").incrByOne();
        getMeter().getThroughputMonitor("queuedEvent.rate").mark();
      }
    } catch (Exception e) {
      addError("Unexpected exception caught while registering a queued event", e);
      assert false
          : "Unexpected exception caught while registering a queued event: "
              + e.toString()
              + "\n"
              + StackTraceUtils.stackTraceToString(e.getStackTrace());
    }
  }

  /**
   * Call for metrics gathering if an event is not added to the queue because the thread was
   * interrupted.
   *
   * @param event
   
  protected void eventInterrupted(E event) {
    try {
      addEvent(event, true);
      unregisterThreadWatcher();
      registerThreadUnblocked();

      if (!DISABLE_MULTIMETER) {
        getMeter().getCounter("interruptedEvent.totalCount").incrByOne();
        getMeter().getThroughputMonitor("interruptedEvent.rate").mark();
      }
    } catch (Exception e) {
      addError("Unexpected exception caught while registering an interrupted event", e);
      assert false
          : "Unexpected exception caught while registering an interrupted event: "
              + e.toString()
              + "\n"
              + StackTraceUtils.stackTraceToString(e.getStackTrace());
    }
  }

  /**
   * Call for metrics gathering after an event has been flushed from the event queue.
   *
   * @param event
   
  protected void eventFlushed(E event) {
    try {
      if (!DISABLE_MULTIMETER) numQueued.decrementAndGet();
      removeEvent(event);

      if (!DISABLE_MULTIMETER) {
        getMeter().getThroughputMonitor("flushedEvent.rate").mark();
      }
    } catch (Exception e) {
      addError("Unexpected exception caught while registering a flushed event", e);
      assert false
          : "Unexpected exception caught while registering a flushed event: "
              + e.toString()
              + "\n"
              + StackTraceUtils.stackTraceToString(e.getStackTrace());
    }
  }

  @VisibleForTesting
  int getNumQueued() {
    return numQueued.get();
  }

  private void addEvent(Object event, boolean willThrowAway) {
    if (DISABLE_EVENT_CACHE) {
      return;
    }

    getEventCache().addOrUpdate(event, willThrowAway);
  }

  private void removeEvent(Object event) {
    if (DISABLE_EVENT_CACHE) {
      return;
    }

    getEventCache().expire(event);
  }

  private void registerThreadWatcher() {
    if (DISABLE_THREAD_MONITOR) {
      return;
    }

    MSThreadMonitor.instance().register(THREAD_BLOCK_INTERVAL, this);
  }

  private void unregisterThreadWatcher() {
    if (DISABLE_THREAD_MONITOR) {
      return;
    }

    MSThreadMonitor.instance().unregisterIfRegistered();
  }

  @Override
  @ManagedAttribute(
      description =
          "The max number of items allowed in the queue or buffer "
              + "before logging is blocked or further log entries are thrown away")
  public int getBufferSize() {
    return bufferSize;
  }

  @Override
  public void setBufferSize(int size) {
    if (isStarted()) {
      throw new IllegalStateException(
          "The buffer size cannot be set after the dispatcher is started started!");
    }
    addInfo("Buffer size was set to: " + size);
    this.bufferSize = size;
  }

  @Override
  public ObjectName getObjectName() {
    try {
      return new ObjectName(
          "msjava.logbackutils.async.dispatchers:type="
              + getClass().getSimpleName()
              + ",name="
              + metricsId);
    } catch (MalformedObjectNameException e) {
      throw new RuntimeException("Could not generate JMX object name", e);
    }
  }

  @ManagedAttribute(description = "A summary of the metrics collected of this dispatcher")
  public String getMetricsSummary() {
    return doGetExtraInformation(true);
  }

  @ManagedAttribute(description = "List of logging events pending dispatch")
  public List<String> getPendingEvents() {
    LinkedList<String> events = new LinkedList<String>();

    if (DISABLE_EVENT_CACHE) {
      return events;
    }

    Iterator<EventCache.Event> ei = getEventCache().eventIterator();

    while (ei.hasNext()) {
      EventCache.Event event = ei.next();

      if (!event.willThrowAway) {
        events.add(event.toString());
      }
    }

    return events;
  }

  @ManagedAttribute(
      description = "List of logging events that were recently dropped because the buffer was full")
  public List<String> getDroppedEvents() {
    LinkedList<String> events = new LinkedList<>();

    if (DISABLE_EVENT_CACHE) {
      return events;
    }

    Iterator<EventCache.Event> ei = getEventCache().eventIterator();

    while (ei.hasNext()) {
      EventCache.Event event = ei.next();

      if (event.willThrowAway) {
        events.add(event.toString());
      }
    }

    return events;
  }

  private static String formatTimestamp(long timestamp) {
    return Instant.ofEpochMilli(timestamp).toString();
  }

  @Override
  public String getExtraInformation(boolean extended) {
    return doGetExtraInformation(false);
  }

  private String doGetExtraInformation(boolean includeBlockedThreadStacks) {
    if (DISABLE_MULTIMETER) {
      return "Metrics collection disabled";
    }

    StringBuilder sb = new StringBuilder("\n");

    try {
      for (Entry<String, Metric> e : getMeter().getMetrics().entrySet()) {
        sb.append(e.getKey()).append(" : ");
        write(e.getValue(), sb);
        sb.append("\n");
      }

      sb.append("\nBlocked threads:\n\n");
      long now = System.currentTimeMillis();
      for (Entry<Thread, Long> e : threadsAndBlockTimes.entrySet()) {
        sb.append(e.getKey())
            .append(" : since ")
            .append(formatTimestamp(e.getValue()))
            .append(" (")
            .append(now - e.getValue())
            .append(" ms)\n");
        if (includeBlockedThreadStacks) {
          sb.append("Stack trace:\n");
          // StackTraceUtils.appendStackTrace(sb, e.getKey().getStackTrace());
          sb.append("\n");
        }
      }
    } catch (Exception e) {
      sb.append("Failed to get extra information: ").append(e.toString());
      // StackTraceUtils.appendStackTrace(sb, e.getStackTrace());
    }

    return sb.toString();
  }

  private void write(Metric metric, StringBuilder sb) {
    if (metric instanceof com.codahale.metrics.Gauge) {
      Object value = ((com.codahale.metrics.Gauge<?>) metric).getValue();
      if (value instanceof Number) {
        sb.append(value);
      } else if (value instanceof CompositeData) {
        CompositeData cd = (CompositeData) value;
        boolean first = true;
        for (String key : cd.getCompositeType().keySet()) {
          if (first) {
            first = false;
          } else {
            sb.append(", ");
          }
          sb.append(key).append("=").append(cd.get(key));
        }
      }
    } else if (metric instanceof com.codahale.metrics.Counter) {
      sb.append(((com.codahale.metrics.Counter) metric).getCount());
    } else if (metric instanceof com.codahale.metrics.Histogram) {
      com.codahale.metrics.Histogram histogram = (com.codahale.metrics.Histogram) metric;
      sb.append("count=").append(histogram.getCount()).append(", ");
      sb.append("min=").append(histogram.getSnapshot().getMin()).append(", ");
      sb.append("max=").append(histogram.getSnapshot().getMax()).append(", ");
      sb.append("mean=").append(histogram.getSnapshot().getMean()).append(", ");
      sb.append("median=").append(histogram.getSnapshot().getMedian());
    } else if (metric instanceof com.codahale.metrics.Meter) {
      com.codahale.metrics.Meter meter = (com.codahale.metrics.Meter) metric;
      sb.append("count=").append(meter.getCount()).append(", ");
      sb.append("meanRate (m)=").append(meter.getMeanRate()).append(" / sec , ");
      sb.append("1minRate (m1)=").append(meter.getOneMinuteRate()).append(" / sec , ");
      sb.append("5minRate (m5)=").append(meter.getFiveMinuteRate()).append(" / sec , ");
      sb.append("15minRate (m15)=").append(meter.getFifteenMinuteRate()).append(" / sec");
    } else if (metric instanceof com.codahale.metrics.Timer) {
      com.codahale.metrics.Timer timer = (com.codahale.metrics.Timer) metric;
      // snapshot unit is nanosecond
      sb.append("min=").append(timer.getSnapshot().getMin() / 1000000L).append(" ms , ");
      sb.append("max=").append(timer.getSnapshot().getMax() / 1000000L).append(" ms, ");
      sb.append("mean=").append(timer.getSnapshot().getMean() / 1000000L).append(" ms, ");
      sb.append("median=").append(timer.getSnapshot().getMedian() / 1000000L).append(" ms");
    }
  }

  private final Object eventCache_lock = new Object();

  private volatile EventCache eventCache;

  private EventCache getEventCache() {
    if (eventCache == null) {
      synchronized (eventCache_lock) {
        if (eventCache == null) {
          eventCache = new EventCache(EVENT_BACKLOG_LIMIT, EVENT_RETENTION_SECONDS);
        }
      }
    }
    return eventCache;
  }

  private static final AtomicInteger EXPIRED_EVENT_CLEANER_TID = new AtomicInteger();

  private static final ScheduledExecutorService EXPIRED_EVENT_CLEANER =
      Executors.newScheduledThreadPool(
          1,
          r -> {
            Thread t =
                new Thread(
                    r,
                    "logbackutils-asyncdispatcher-expiredeventcleaner-"
                        + EXPIRED_EVENT_CLEANER_TID.incrementAndGet());
            t.setDaemon(true);
            return t;
          });

  @VisibleForTesting
  static class EventCache {
    @VisibleForTesting
    class Event {

      @VisibleForTesting final long timestamp;

      @VisibleForTesting
      final String thread; // String to avoid holding a ref to a thread that might have died

      @VisibleForTesting final Object event;

      @VisibleForTesting boolean willThrowAway;

      Event(Object event, boolean willThrowAway) {
        timestamp = System.currentTimeMillis();
        thread = Thread.currentThread().toString();
        this.event = event;
        this.willThrowAway = willThrowAway;
      }

      // this only checks the equality of the contained events to avoid duplicates in the map
      @Override
      public boolean equals(Object obj) {
        if (obj == null) {
          return false;
        }

        if (obj == this) {
          return true;
        }

        if (!getClass().equals(obj.getClass())) {
          return false;
        }

        return event.equals(((Event) obj).event);
      }

      @Override
      public int hashCode() {
        return event.hashCode();
      }

      @Override
      public String toString() {
        return formatTimestamp(timestamp) + " : " + thread + " : " + event;
      }
    }

    private final ConcurrentLinkedHashSet<Event> events = new ConcurrentLinkedHashSet<>();

    @VisibleForTesting final AtomicInteger size = new AtomicInteger();

    private final int eventBacklogLimit;

    private final long eventRetentionMillis;

    EventCache(int eventBacklogLimit, int eventRetentionSeconds) {
      this.eventBacklogLimit = eventBacklogLimit;
      eventRetentionMillis = eventRetentionSeconds * 1000L;

      // this removes all expired events on init
      EXPIRED_EVENT_CLEANER.scheduleWithFixedDelay(
          this::eventIterator,
          EXPIRED_EVENT_CLEANUP_INTERVAL_SECONDS,
          EXPIRED_EVENT_CLEANUP_INTERVAL_SECONDS,
          TimeUnit.SECONDS);
    }

    boolean addOrUpdate(Object event, boolean willThrowAway) {
      Event searchedEvent = new Event(event, willThrowAway);
      Event foundEvent = events.get(searchedEvent);
      if (foundEvent != null) {
        foundEvent.willThrowAway = willThrowAway;
        return false;
      } else {
        events.put(searchedEvent);
        if (size.incrementAndGet() > eventBacklogLimit) {
          expireOldest();
        }
        return true;
      }
    }

    @VisibleForTesting
    Event expireOldest() {
      Event oldest = events.pollFirstEntry();
      if (oldest != null) {
        int s = size.decrementAndGet();
        assert s >= 0;
      }
      return oldest;
    }

    boolean expire(Object event) {
      if (events.remove(new Event(event, false)) != null) {
        int s = size.decrementAndGet();
        assert s >= 0;
        return true;
      }
      return false;
    }

    Iterator<Event> eventIterator() {
      return new Iterator<Event>() {

        final Iterator<Event> internalIterator = events.iterator();

        Event prev;

        Event next = internalNext();

        private Event internalNext() {
          long expiredBefore = System.currentTimeMillis() - eventRetentionMillis;

          while (internalIterator.hasNext()) {
            Event n = internalIterator.next();

            if (n.timestamp < expiredBefore) {
              expire(n.event);
            } else {
              next = n;
              return n;
            }
          }

          next = null;
          return null;
        }

        @Override
        public boolean hasNext() {
          return next != null;
        }

        @Override
        public Event next() {
          if (next == null) {
            throw new NoSuchElementException();
          }
          prev = next;
          internalNext();
          return prev;
        }

        @Override
        public void remove() {
          expire(prev.event);
        }
      };
    }

    @Override
    public String toString() {
      return events.toString();
    }
  }

  static class ConcurrentLinkedHashSet<K> {

    private static class LinkedNode<K> {
      final Object sync = new Object();

      volatile LinkedNode<K> prev;

      volatile LinkedNode<K> next;

      volatile K value;
    }

    private final LinkedNode<K> head;

    private final LinkedNode<K> tail;

    private final ConcurrentHashMap<K, LinkedNode<K>> map = new ConcurrentHashMap<>();

    ConcurrentLinkedHashSet() {
      head = new LinkedNode<>();
      tail = new LinkedNode<>();
      head.next = tail;
      tail.prev = head;
    }

    // lock order: new / removed node's sync, then back to front
    // synchronizing on node.sync prevents synchronizing on node.next while it's still null
    // synchronizing on the tail before putting the new node in the map would achieve the same, but
    // would
    // potentially make other threads wait unnecessarily

    void put(K key) {
      LinkedNode<K> node = new LinkedNode<>();
      synchronized (node.sync) {
        if (map.putIfAbsent(key, node) == null) {
          // not yet added, set up the link
          synchronized (tail) {
            // synchronized (node) { // not reachable, not needed
            // (before it could be reached from either its next or prev, we lock both of those and
            // reaching
            // it from the map is prevented by its sync)
            synchronized (tail.prev) {
              node.value = key;
              node.next = tail;
              node.prev = tail.prev;
              tail.prev.next = node;
              tail.prev = node;
            }
            // }
          }
        }
      }
    }

    K get(K key) {
      LinkedNode<K> entry = map.get(key);
      return entry != null ? entry.value : null;
    }

    K remove(K key) {
      if (key == null) {
        return null;
      }
      LinkedNode<K> node = map.remove(key);
      K value = null;
      if (node != null) {
        // unlink
        synchronized (node.sync) {
          synchronized (node.next) {
            synchronized (node) {
              synchronized (node.prev) {
                node.prev.next = node.next;
                node.next.prev = node.prev;
                value = node.value;
                node.value = null;
              }
            }
          }
        }

        assert value != null : "Value was null for key " + key;
      }

      return value;
    }

    K pollFirstEntry() {
      K value = null;
      while (head.next != tail && value == null) {
        value = remove(head.next.value);
      }
      return value;
    }

    Iterator<K> iterator() {
      return new Iterator<K>() {

        LinkedNode<K> next = head;

        K prevValue;

        K nextValue = internalNext();

        @Override
        public boolean hasNext() {
          return nextValue != null;
        }

        private K internalNext() {
          K value = null;
          while (value == null && next != tail) {
            // a value becomes null if the node has just been removed
            next = next.next;
            value = next.value;
          }
          return value;
        }

        // won't return null
        // won't return elements that have been removed before the previous call to next()
        // will return elements that have been added since the previous call to next()
        // won't return anything twice
        // won't finish before reaching the tail
        // won't throw if hasNext() is true before calling it
        // will return elements that have been removed since the previous call to next()
        @Override
        public K next() {
          if (nextValue == null) {
            throw new NoSuchElementException();
          }

          prevValue = nextValue;
          nextValue = internalNext();

          return prevValue;
        }

        @Override
        public void remove() {
          ConcurrentLinkedHashSet.this.remove(prevValue);
        }
      };
    }
  }
}
 */