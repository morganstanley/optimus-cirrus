/*
 * Morgan Stanley makes this available to you under the Apache License, Version 2.0 (the "License").
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package optimus.graph;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiConsumer;
import msjava.slf4jutils.scalalog.Logger;
import msjava.slf4jutils.scalalog.package$;
import optimus.core.MonitoringBreadcrumbs$;
import optimus.graph.OGTraceStore.Table;
import optimus.graph.cache.NCSupport;
import optimus.graph.diagnostics.EvictionReason;
import optimus.graph.diagnostics.PNodeTaskInfo;
import optimus.graph.diagnostics.ProfiledEvent;
import optimus.graph.diagnostics.SchedulerProfileEntry;
import optimus.graph.diagnostics.gridprofiler.GridProfiler;
import optimus.graph.diagnostics.messages.BookmarkCounter;
import optimus.graph.diagnostics.messages.ProfilerEventsWriter;
import optimus.graph.diagnostics.messages.ProfilerMessages;
import optimus.graph.diagnostics.messages.ProfilerMessagesGenerator;
import optimus.graph.diagnostics.messages.ProfilerMessagesReader;
import optimus.graph.diagnostics.messages.StartupEvent;
import optimus.graph.diagnostics.messages.StartupEventCounter;
import optimus.graph.diagnostics.sampling.Cardinality;
import optimus.graph.diagnostics.sampling.KnownStackRecorders;
import optimus.graph.diagnostics.sampling.SamplingProfiler;
import optimus.graph.diagnostics.trace.OGEventsObserver;
import optimus.graph.diagnostics.trace.OGTraceMode;
import optimus.platform.EvaluationContext;
import optimus.platform.EvaluationQueue;
import optimus.platform.inputs.GraphInputConfiguration$;
import optimus.utils.misc.Color;

/**
 * This is the backend that runs graph profiling. Trace records all graph events in memory-mapped
 * append-only storage. Basic rule: If it's thread local OR doesn't need synchronization you can
 * (don't have to) update the counters inline otherwise serialize into Table to be processed later
 *
 * <p>Threading in profiler! 1. Management of Tables from a pool of tables/dynamically resized mmap
 * file is under lock in OGTraceStore.lock 2. Management of LocalTable (aka per thread profiler
 * state) under lock in LocalTable [SEE_LOCK_LOCAL_TABLES]
 *
 * <p>LocalTable values and its members are ONLY updated by the thread that owns them. Except for
 * [SEE_OBSERVED_VALUE] variables. Which are touched by one of the observing threads under some
 * lock, or by virtue of being the only 'processing' thread
 *
 * <p>Support: reset() ...
 *
 * <p>Support looking at stats available so far. With the following caveat: Values might be unstable
 * [SEE_PROFILER_UNSTABLE]. But will always 'recover' (won't drift off) Calls snapTables and Can be
 * called from any thread!
 *
 * <p>LocalTables PThreadContext ProfilerEventsWriter Table/events (only set by owner as described
 * above!) Table/edges (only set by owner as described above!)
 */
public class OGTrace {
  // bump this up whenever you add fields to OGTraceStore.Table, or change the types of existing
  // fields, to ensure
  // that we don't try to read ogtrace files generated from older code versions with different
  // fields
  public static final int FORMAT = 4;
  static final int FILE_HEADER_SIZE =
      4 /* sizeOf(FORMAT) */ + 16 /* millis, nano sync */ + Table.HEADER_SIZE;

  private static final OGTraceReader liveReader;
  static final ProfilerMessagesGenerator gen =
      ProfilerMessagesGenerator.create(ProfilerMessages.class);
  static final ProfilerEventsWriter writer_proto =
      (ProfilerEventsWriter) gen.createWriterObject(ProfilerEventsWriter.class);
  static final ProfilerMessagesReader reader_proto;
  public static final OGTraceStore trace =
      new OGTraceStore("optimus", false /* At least for testing need to pass mode */);
  // Aka the last non-none mode
  private static volatile OGEventsObserver _pausedObserver = OGTraceMode.none;
  public static OGEventsObserver observer = OGTraceMode.none; // Only set by setting the mode
  private static final ArrayList<BiConsumer<OGEventsObserver, OGEventsObserver>>
      _modeChangeListeners = new ArrayList<>();
  private static final Object lock = new Object();

  static {
    reader_proto = (ProfilerMessagesReader) gen.createReaderObject(ProfilerMessagesReader.class);
    liveReader = new OGTraceReaderLive(trace);
  }

  static final Logger log = package$.MODULE$.getLogger("OGTrace");

  // Special profiled block IDs (see GridProfiler.scopeToBlockID).
  // Defined here because OGTrace is its main user
  // for (unscoped) Property Table's PNTI info (name, flags, etc)
  public static final int BLOCK_ID_PROPERTY = 0;
  // for cache hits etc caught outside any profiled block
  public static final int BLOCK_ID_UNSCOPED = 1;
  public static final int BLOCK_ID_ALL = -1; // Sentinel to say combine all blocks information

  // settable in tests for fixed delays in cache time and NCSupport.matchXScenario (in ms)
  public static long TESTABLE_FIXED_DELAY_XS = 1;
  public static long TESTABLE_FIXED_DELAY_XSFT = 1;
  public static long TESTABLE_FIXED_DELAY_CACHE = 1;

  public static long nanoTime() {
    return TestableClock.nanoTime();
  }

  public static void publishEvent(int eventId, ProfiledEvent state) {
    TestableClock.publishEvent(eventId, state);
  }

  static void consumeEvent(int eventId, ProfiledEvent state) {
    TestableClock.consumeEvent(eventId, state);
  }

  public static void consumeTimeInMs(long ms, NanoClockTopic topic) {
    TestableClock.consumeTimeInMs(ms, topic);
  }

  public static void setClock(NanoClock clock) {
    TestableClock.setClock(clock);
  }

  public static String eventToName(int evt) {
    return reader_proto.nameOfEvent(evt);
  }

  private static void verifyEntityAgentInjectedTheFields() {
    if (DiagnosticSettings.traceAvailable) {
      NodeTask acn = new AlreadyCompletedNode<>(null);
      // Lets test if in fact tracing fields have been added
      // Try to assign a value and make sure we can read it back
      acn.setId(-1);
      if (acn.getId() != -1) {
        DiagnosticSettings.traceAvailable = false;
        log.javaLogger()
            .error(
                "optimus.graph is not instrumented, please add -javaagent:[fullpath]/optimus.platform.entityagent.jar=e");
      }
    }
  }

  static {
    verifyEntityAgentInjectedTheFields();
    GridProfiler.ensureLoaded();
    AsyncProfilerIntegration.ensureLoadedIfEnabled();
    SamplingProfiler.ensureLoadedIfEnabled();
    // traceAvailable, we should be prepared to summarize some profile data.
    // Should we make it even lazier? Should we lower priority?
    if (DiagnosticSettings.traceAvailable) {
      Thread thread =
          new Thread(
              () -> {
                while (true) { // we don't want thread to die from exceptions (stacktrace below)
                  try {
                    liveReader.liveCatchUp();
                  } catch (Exception ex) {
                    ex.printStackTrace();
                  }
                }
              },
              "optimus-trace-processor");
      thread.setDaemon(true);
      thread.start();

      registerOnTraceModeChanged(
          (oldMode, newMode) -> {
            NodeTrace.traceWaits.setValue(newMode.traceWaits());
            NodeTrace.traceTweaks.setValue(newMode.traceTweaks());
          });
    }
  }

  @Deprecated
  public static void changeObserver(OGEventsObserver newObserver) {
    if (!DiagnosticSettings.traceAvailable && newObserver.requiresNodeID())
      throw new GraphException("entityagent needs to inject the basic fields");

    if (observer.ignoreReset()) return;

    boolean modeChanged = false;

    if (observer != newObserver) {
      // Setting current mode to none is kind of a form of pause. At least if not followed by
      // reset...
      if (observer != OGTraceMode.none && newObserver == OGTraceMode.none) {
        liveReader.catchUp();
        _pausedObserver = observer;
      }

      OGEventsObserver oldObserver = observer;
      observer = newObserver;
      liveReader.setLiveProcess(newObserver.liveProcess());
      modeChanged = true;

      boolean timeLine = observer.collectsTimeLine();
      OGTraceIsolation$.MODULE$.setOGTracing(timeLine);
      if (AsyncProfilerIntegration.enabled()) {
        boolean wasTimeLine = oldObserver.collectsTimeLine();
        if (timeLine && !wasTimeLine) AsyncProfilerIntegration.traceStart();
        else if (wasTimeLine && !timeLine) AsyncProfilerIntegration.traceStop();
      }
    } else OGTraceIsolation$.MODULE$.setOGTracing(observer.collectsTimeLine());
    BookmarkCounter.report("TraceMode=" + newObserver.name(), Color.MAGENTA.getRGB());

    if (modeChanged) raiseTraceModeChanged();
  }

  @SuppressWarnings("unchecked")
  private static void raiseTraceModeChanged() {
    BiConsumer<OGEventsObserver, OGEventsObserver>[] consumers;
    synchronized (_modeChangeListeners) {
      consumers = _modeChangeListeners.toArray(new BiConsumer[0]);
    }
    for (var consumer : consumers) {
      raiseTraceModeChanged(consumer);
    }
  }

  private static void raiseTraceModeChanged(
      BiConsumer<OGEventsObserver, OGEventsObserver> consumer) {
    synchronized (lock) {
      try {
        OGEventsObserver availableMode = observer == OGTraceMode.none ? _pausedObserver : observer;
        consumer.accept(availableMode, observer);
      } catch (Throwable ex) {
        log.javaLogger().error("Trace mode changed listener failed", ex);
      }
    }
  }

  public static void registerOnTraceModeChanged(
      BiConsumer<OGEventsObserver, OGEventsObserver> consumer) {
    synchronized (_modeChangeListeners) {
      _modeChangeListeners.add(consumer);
    }
    raiseTraceModeChanged(consumer);
  }

  public static OGEventsObserver getTraceMode() {
    return observer;
  }

  public static Map<String, SchedulerProfileEntry> getSchedulerProfiles() {
    return OGLocalTables.getContexts();
  }

  /** Call out from graph */
  public static void completed(EvaluationQueue eq, NodeTask task) {
    if (Settings.allowTestableClock)
      OGTrace.publishEvent(task.getId(), ProfiledEvent.NODE_COMPLETED);
    if (task.pluginTracked()) {
      var pt = task.getReportingPluginType();
      if (Objects.nonNull(pt)) pt.decrementInFlightTaskCount(eq);
    }
    observer.completed(eq, task);
  }

  public static void adapted(OGSchedulerContext ec, NodeTask ntsk) {
    var pt = ntsk.getReportingPluginType();
    if (Objects.nonNull(pt)) {
      pt.recordLaunch(ec, ntsk);
      observer.adapted(ec, pt);
    }
  }

  public static void evict(EvictionReason reason, long num) {
    if (DiagnosticSettings.samplingProfilerStatic && num > 0) {
      var lt = OGLocalTables.getOrAcquire();
      lt.evictionCounter.add(reason, num);
      lt.release();
    }
  }

  private static final int cacheHitIdx = KnownStackRecorders.CacheHit().id();

  public static void lookupEndProxy(
      EvaluationQueue eq,
      PropertyNode<?> underlying,
      PropertyNode<?> proxy,
      boolean cacheHit,
      boolean countMiss,
      String sigil) {
    if (DiagnosticSettings.sampleCacheLookups && cacheHit) {
      var lt = OGLocalTables.getOrAcquire(eq);
      lt.stackRecorder(cacheHitIdx).accrueAndMaybeRecord(proxy, 1, sigil);
    }
    observer.lookupEndProxy(eq, underlying, proxy, cacheHit, countMiss);
  }

  public static void lookupEnd(
      OGLocalTables lCtx, long startTime, NodeTask candidate, NodeTask lookupResult) {
    if (DiagnosticSettings.samplingProfilerStatic) {
      if (candidate != lookupResult.cacheUnderlyingNode()) {
        if (candidate.scenarioStack()._cacheID() != lookupResult.scenarioStack()._cacheID()) {
          lCtx.stackRecorder(cacheHitIdx).accrueAndMaybeRecord(candidate, 1, "x"); // xs
        } else {
          lCtx.stackRecorder(cacheHitIdx).accrueAndMaybeRecord(candidate, 1, "t"); // trivial
        }
      }
    }
    OGTrace.observer.lookupEnd(lCtx, startTime, candidate, lookupResult);
  }

  /**
   * Set what will _potentially_ be the enqueuer of this node if it is not found in cache. This
   * allows reporting a stack hit using the stack that would have been executed had it been a miss.
   */
  public static void setPotentialEnqueuer(
      OGSchedulerContext ec, PropertyNode<?> node, boolean syncStack) {
    if (DiagnosticSettings.sampleCacheLookups && !node.isStable()) {
      // Store enqueuer reference weakly, since it might not be cleared explicitly, as happens for
      // nodes that are actually enqueued.
      AwaitStackManagement.setLaunchData(
          ec.getCurrentNodeTask(), node, syncStack, NodeTask.WEAK_LAUNCHER_REF);
    }
  }

  public static void countDistinct(Cardinality.Category cat, long hash) {
    if (hash != 0) {
      var lt = OGLocalTables.getOrAcquire();
      lt.cardinalities.add(cat, hash);
      lt.release();
    }
  }

  /** Call out from graph */
  public static void start(OGLocalTables prfCtx, NodeTask ntsk, boolean aNew) {
    if (Settings.allowTestableClock) OGTrace.consumeEvent(ntsk.getId(), ProfiledEvent.NODE_CREATED);
    observer.start(prfCtx, ntsk, aNew);
  }

  /** Call out from graph */
  public static void dependency(NodeTask task, NodeTask child, EvaluationQueue eq) {
    if (Settings.allowTestableClock)
      OGTrace.consumeEvent(child.getId(), ProfiledEvent.NODE_COMPLETED);
    observer.dependency(task, child, eq);
  }

  /** Call out from graph */
  public static void enqueue(NodeTask fromTask, NodeTask toTask, boolean syncStack) {
    // no parent node (aka) internal scheduling that doesn't need to show up in user profiling
    // fromTask.isDone() onComplete scheduling should not be visible to a user
    // fromTask == toTask self re-enqueuing manual tasks are internal
    if (fromTask == null || fromTask.isDoneEx() || fromTask == toTask) return;
    observer.enqueue(fromTask, toTask);
    if (!DiagnosticSettings.awaitStacks) return;
    // Concurrency tools need to see enqueue calls even to completed or already enqueued calls
    // The stack walking for debugging or profiling does NOT. Instead will be cause memory leaks
    if (toTask.isDoneOrNotRunnable()) return;

    AwaitStackManagement.setLaunchData(fromTask, toTask, syncStack, 0);
  }

  /** Set enqueuer data without all the checks in enqueue(). */
  public static void publishNodeSending(NodeTask task, long timeStamp) {
    observer.publishNodeSending(task, timeStamp);
  }

  public static void publishNodeReceived(NodeTask task) {
    observer.publishNodeReceived(task);
  }

  public static void publishNodeStolenForLocalExecution(NodeTask task) {
    observer.publishNodeStolenForLocalExecution(task);
  }

  public static void publishNodeExecuted(NodeTask task) {
    observer.publishNodeExecuted(task);
  }

  public static void publishNodeResultReceived(NodeTask task) {
    observer.publishNodeResultReceived(task);
  }

  /**
   * Called when serialized results arrive from external systems (GSF or DHT) but before they are
   * processed
   */
  public static void publishSerializedNodeResultArrived(@SuppressWarnings("unused") NodeTask task) {
    // observer.publishSerializedNodeResultArrived(task);
  }

  /**
   * In traceNodes when graph is doing a LOT of work and profiler is publishing a LOT of data, our
   * file backing store can run out of space altogether. Attempting to continue tracing causes
   * runtime exceptions later because e.g. storage fails to be allocated for tables, and later
   * attempts to read those tables crash with buffer positions out of bounds. Rather than crashing
   * the entire process, we should stop tracing, warn loudly, and continue.
   */
  public static void panic(Exception e, File fileLocation) {
    var fileStr = (fileLocation == null) ? "[null fileLocation]" : fileLocation.toString();
    var msg = "Failed to write to " + fileStr + ", stopping tracing";
    log.javaLogger().error(msg, e);
    MonitoringBreadcrumbs$.MODULE$.sendOGTraceStoreProblemCrumb(fileStr, e.toString());
    GraphInputConfiguration$.MODULE$.setTraceMode(OGTraceMode.none);
  }

  /**
   * Call out from graph - this and call outs below are tightly coupled to scheduler because they
   * are called under _lock
   */
  static void schedulerStalling(SchedulerStallSource stallReason) {
    OGSchedulerTimes.schedulerStallingBegin(stallReason);
  }

  /**
   * Note - if (newWorkCount == idealCount - 1) then graph threads are no longer fully saturated
   * This is a placeholder in case we're interested in properly estimating this time again
   */
  @SuppressWarnings("unused")
  static void schedulerThreadMovedFromWorkToWait(
      int newWorkCount, int newWaitCount, int idealCount) {}

  /** Call out from graph */
  static void schedulerThreadMovedFromWaitToWork(@SuppressWarnings("unused") int newWorkCount) {}

  /**
   * Call out from graph when a worker thread is added from a user code OR new helper thread OR from
   * idle
   */
  static void schedulerThreadAddedToWork(int newWorkCount, int waitCount) {
    if (newWorkCount == 1 && waitCount == 0) OGSchedulerTimes.enterGraphTime(OGTrace.nanoTime());
  }

  /**
   * Call out from graph when a worker thread is removed to idle OR completely exited (user or
   * helper)
   */
  static void schedulerThreadRemovedFromWork(int newWorkCount, int waitCount) {
    if (newWorkCount == 0 && waitCount == 0) OGSchedulerTimes.exitGraphTime(OGTrace.nanoTime());
  }

  /**
   * Call out from app just before optimus environment is set up this includes off-graph user code
   * from def setup
   */
  public static void startupSetupComplete(StartupEvent startupEvent) {
    StartupEventCounter.reportCompleted(startupEvent);
    long time = OGTrace.nanoTime();
    log.javaLogger().debug("startupSetupComplete at " + time);
    OGSchedulerTimes.preOptimusStartupComplete(time);
  }

  /**
   * Call out from app after optimus environment is set up this includes DAL fetching serverTime and
   * availableRoles
   */
  public static void startupOptimusInitComplete() {
    long time = OGTrace.nanoTime();
    log.javaLogger().debug("startupOptimusInitComplete at " + time);
    OGSchedulerTimes.optimusStartupComplete(time);
  }

  /**
   * Call out from app after app-related init is finished, before run/mainGui this includes on-graph
   * user code from def preRun
   */
  public static void startupAppInitComplete() {
    long time = OGTrace.nanoTime();
    log.javaLogger().debug("startupAppInitComplete at " + time);
    OGSchedulerTimes.postOptimusAppStartupComplete(time);
  }

  /**
   * waits for any node tasks to finish - must be called from graph thread! (Because it grabs the
   * current OGScheduler)
   */
  public static void waitForSchedulerToQuiesce() {
    waitForSchedulerToQuiesce((OGScheduler) EvaluationContext.scheduler(), 0);
  }

  /**
   * If nonzero timeout is supplied, return true if the count reached zero and false if the waiting
   * time elapsed before the count reached zero. If timeout is 0, return true (or wait forever...)
   */
  public static boolean waitForSchedulerToQuiesce(OGScheduler ogScheduler, long timeoutMS) {
    if (ogScheduler.dbgQueuedWaitingAndWorkingCount() != 0) {
      CountDownLatch latch = new CountDownLatch(1);

      // When this scheduler stalls it's 'quiet' so now's a good time for whoever was waiting to do
      // its thing
      var oowListener =
          new SchedulerCallback() {
            @Override
            public void onGraphStalled(Scheduler scheduler, ArrayList<NodeTask> tasks) {
              latch.countDown();
            }
          };
      ogScheduler.addOnOutOfWorkListener(oowListener, true);

      // avoid deadlock if the scheduler ran out of work in between when we asked above and when we
      // registered the
      // out of work listener (which doesn't get called if the scheduler was already out of work)
      if (ogScheduler.dbgQueuedWaitingAndWorkingCount() == 0) latch.countDown();

      // when a timeout is supplied, wait once up to the timeout and return
      if (timeoutMS != 0) {
        return checkAndWarn(ogScheduler, latch, timeoutMS, timeoutMS);
      } else { // otherwise keep waiting (maybe infinitely), but log every 5 seconds
        var cumulativeMS = 5000;
        while (latch.getCount() != 0) {
          checkAndWarn(ogScheduler, latch, 5000, cumulativeMS);
          cumulativeMS += 5000;
        }
      }
      ogScheduler.removeOutOfWorkListener(oowListener);
    }
    return true;
  }

  // return true if the count reached zero and false if the waiting time elapsed before the count
  // reached zero
  private static boolean checkAndWarn(
      OGScheduler scheduler, CountDownLatch latch, long timeoutMS, long cumulativeMS) {
    try {
      var doneInTime = latch.await(timeoutMS, MILLISECONDS);
      if (!doneInTime)
        log.javaLogger()
            .warn("Scheduler still busy after " + (cumulativeMS * 1e-3) + "s: \n" + scheduler);
      return doneInTime;
    } catch (InterruptedException ignored) {
    }
    return true;
  }

  /**
   * specifically for tests that wait for scheduler to be totally done with work (not like
   * waitForSchedulerToQuiesce, which waits for scheduler to report a stall) By default times out in
   * 5 sec
   *
   * @return True if done in time and false otherwise
   */
  public static boolean dbgWaitForDone(OGScheduler ogScheduler) {
    int countDownTime = 5_000;
    while (ogScheduler.dbgQueuedWaitingAndWorkingCount() != 0) {
      try {
        //noinspection BusyWait
        Thread.sleep(1);
        countDownTime--;
        if (countDownTime == 0) return false;
      } catch (InterruptedException e) {
        e.printStackTrace();
        return false;
      }
    }
    return true;
  }

  /**
   * Waits for the current scheduler to exit/park all of its threads
   *
   * @return True if done in time and false otherwise
   */
  public static boolean dbgWaitForDone() {
    return dbgWaitForDone((OGScheduler) EvaluationContext.scheduler());
  }

  /****************************** END EVENT PUBLISHING FUNCTIONS ******************/

  public static void memory(Map<Class<?>, NCSupport.JVMClassInfo> hash) {
    trace.memory(hash);
  }

  /** Resets trace and live reader and scheduler times. Must be called 'off graph' */
  public static void reset() {
    reset(true);
  }

  public static void reset(boolean resetTraceStorage) {
    EvaluationContext.verifyOffGraph(false, false);
    if (resetTraceStorage && !observer.ignoreReset()) trace.reset();
    liveReader.reset();
    OGLocalTables.resetTables(); // must call OGTrace.LocalTables.resetTables here!
    OGSchedulerTimes.reset();
    NodeTrace.resetNodeTrace();
  }

  public static long traceFileSize() throws IOException {
    return trace.fileSize();
  }

  // only meaningful for TRACEEVENTS
  public static void copyTraceTo(String name) throws IOException {
    trace.copyTraceTo(name);
  }

  public static void writeHotspotsTo(String name, ArrayList<PNodeTaskInfo> pntis)
      throws IOException {
    OGTraceStore.writeAllTaskInfos(name, pntis);
  }

  /** Only tests should use this method */
  public static OGTraceReader readingFromLiveBackingStore() {
    return trace.storeForReading();
  }

  /** Only live profiling and tests should use this method */
  public static OGTraceReader liveReader() {
    return liveReader;
  }

  public static OGTraceReader readingFromFile(String name) throws IOException {
    return new OGTraceFileReader(name);
  }

  public static OGTraceReader readingFromFiles(File[] files) throws IOException {
    return new OGTraceFileReader(files, false);
  }

  public static OGTraceReader readingFromFilesAcrossMultipleProcesses(File[] files)
      throws IOException {
    return new OGTraceFileReader(files, true);
  }

  public static OGTraceReader readingFromLocal(PNodeTaskInfo[] pntis) {
    return new OGTraceLocalReader(pntis);
  }

  /*
  JVMTI event callbacks. Not called unless enabled via enableXYZ
  */
  @SuppressWarnings("unused")
  public static native void enableClassLoadCallbacks();

  @SuppressWarnings("unused")
  public static native void disableClassLoadCallbacks();

  @SuppressWarnings("unused")
  public static void classLoadCallback(Thread t, Class<?> k) {
    log.javaLogger().debug("JVMTI classLoad for " + k.getName() + " on thread " + t.getName());
  }

  @SuppressWarnings("unused")
  public static void classPrepareCallback(Thread t, Class<?> k) {
    log.javaLogger().debug("JVMTI classPrepare for " + k.getName() + " on thread " + t.getName());
  }

  @SuppressWarnings("unused")
  public static native void enableThreadEndCallback();

  @SuppressWarnings("unused")
  public static native void disableThreadEndCallback();

  @SuppressWarnings("unused")
  public static void threadEndCallback(Thread t) {
    log.javaLogger().debug("JVMTI thread end for thread " + t.getName());
  }
}
