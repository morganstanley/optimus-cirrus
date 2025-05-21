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

import static optimus.graph.WaitingChainWriter.writeWaitingChain;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import msjava.slf4jutils.scalalog.package$;
import optimus.breadcrumbs.crumbs.RequestsStallInfo;
import optimus.core.ArrayListWithMarker;
import optimus.core.IndexedArrayList;
import optimus.core.StallInfoAppender;
import optimus.graph.diagnostics.GraphDiagnostics;
import optimus.graph.diagnostics.InfoDumper;
import optimus.graph.diagnostics.WaitProfile;
import optimus.graph.diagnostics.messages.BlockingWaitCounter;
import optimus.platform.AgentInfo;
import optimus.platform.EvaluationContext;
import optimus.platform.EvaluationQueue;
import optimus.platform.ScenarioStack;
import optimus.platform.StartNode;
import optimus.platform.util.PrettyStringBuilder;
import optimus.platform.util.ThreadDumper;
import optimus.scalacompat.collection.javaapi.CollectionConverters;
import org.slf4j.Logger;
import scala.Function0;
import scala.None$;
import scala.Option;
import scala.Some;
import scala.Tuple3;

public class OGScheduler extends Scheduler {
  static final String schedulerVersion = "OGScheduler_v15.4";
  public static final Logger log = package$.MODULE$.getLogger("optimus.graph").javaLogger();

  private static final ExecutorService virtualExecutorService;
  public static final String ogThreadNamePrefix = "og.worker-";
  private static final long ONE_SECOND_IN_NANOSECONDS = 1000000000;

  private static long deadlockDetectionTimeout;
  private static long stallPrintInterval;
  private static long reportNoProgress;
  private static long adaptedFullInfoPrintTimeout;

  public static void readSettingsForTestingPurposes() {
    deadlockDetectionTimeout =
        (long) (Settings.deadlockDetectionTimeout * ONE_SECOND_IN_NANOSECONDS);
    stallPrintInterval = (long) (Settings.stallPrintInterval * ONE_SECOND_IN_NANOSECONDS);
    reportNoProgress = (long) (Settings.reportNoProgress * ONE_SECOND_IN_NANOSECONDS);
    adaptedFullInfoPrintTimeout =
        (long) (Settings.adaptedStallPrintAllInfoTimeout * ONE_SECOND_IN_NANOSECONDS);
  }

  static {
    AgentInfo.agentInfo(); // fail fast if entityagent is not present
    OGTrace.nanoTime(); // This is to avoid deadlock where OGScheduler -> NodeTask -> OGTrace, now
    // OGScheduler -> OGTrace
    readSettingsForTestingPurposes();

    ExecutorService executorService = null;
    if (Settings.useVirtualThreads) {
      // Replace with direct Executors.newVirtualThreadPerTaskExecutor() on Java21
      var mt = MethodType.methodType(ExecutorService.class);
      try {
        var mh =
            MethodHandles.lookup()
                .findStatic(Executors.class, "newVirtualThreadPerTaskExecutor", mt);
        executorService = (ExecutorService) mh.invoke();
      } catch (Throwable e) {
        log.warn("Couldn't get virtual thread executor", e);
      }
    }
    virtualExecutorService = executorService;
  }

  /**
   * 1. The first 2 fields must be final and therefore if a thread has a reference to the Context
   * it's safe to read those 2 fields (used for work stealing)
   */
  public static class Context extends IndexedArrayList.IndexedItem {
    Thread thread;
    final int threadID; // Linear 'logical' thread id
    final OGSchedulerContext.WorkQueue queue = new OGSchedulerContext.WorkQueue();
    OGSchedulerContext schedulerContext; // Used for easy debugging
    boolean userEntry; // False for threads above ideal count to take 'stuck' global items
    volatile boolean parked; // We are spinning on this field...
    volatile boolean shutdown; // Kill switch...

    /** Five fields below are temporary variables which are available per thread during wait */
    NodeTaskInfo awaitedTaskInfoEndOfChain = NodeTaskInfo.EndOfChain; // Never null

    SchedulerPlugin awaitedTaskPluginEndOfChain;
    PluginType awaitedTaskPluginTypeEndOfChain = PluginType.None(); // Never null

    int awaitedTaskEndOfChainInfoID; // To connect Profiler wait screen with Debugger trace screen
    int awaitedTaskEndOfChainID; // Display task instance at the end of chain of an awaited task in
    // blocking wait

    NodeTask awaitedTask; // Task thread is waiting for to complete OR updated to atask to process

    public Option<NodeTask> lastTask() {
      var at = this.awaitedTask;
      if (Objects.isNull(at)) return None$.empty();
      var chain = at.visitForwardChain();
      if (chain.isEmpty()) return None$.empty();
      else return new Some<>(chain.get(chain.size() - 1));
    }

    // 'meanwhile'
    int awaitedCausalityID; // In the waiting state effectively equals to syncStack depth
    // In the waiting state marks this context as causing lost
    boolean awaitedCausalityNeedsReporting;
    // concurrency
    boolean captureWaiterForProfiling;
    PluginType awaitedCausalityStalledOn = PluginType.None(); // if null, there was no stall

    Context(Thread thread, int threadID) {
      this.thread = thread;
      this.threadID = threadID;
    }

    public final PrettyStringBuilder writePrettyString(
        PrettyStringBuilder sb, boolean showEmptyQueues, int nodeLimit) {
      var threadName = thread != null ? thread.getName() : "";
      if (threadName.isEmpty()) threadName = "vt_" + threadID;
      sb.append(threadName);

      if (showEmptyQueues || !queue.isEmpty()) {
        if (schedulerContext != null) {
          sb.append(schedulerContext.syncStackCount() > 1 ? " slq" : " lq");
          sb.append(userEntry ? ".u " : " ");
          if (awaitedTask != null && Settings.dangerousIncludeAwaitedTaskInSchedulerString) {
            PrettyStringBuilder sbX = new PrettyStringBuilder();
            sbX.showNodeArgs_$eq(true);
            sbX.showNodeState_$eq(true);
            sbX.showCausalityID_$eq(true);
            sbX.simpleName_$eq(true);
            awaitedTask.writePrettyString(sbX);
            sb.append(sbX.toString());
          }
        }
        queue.toString(sb, nodeLimit);
      } else {
        sb.endln();
      }
      return sb;
    }

    @Override
    public String toString() {
      return writePrettyString(new PrettyStringBuilder(), true, Integer.MAX_VALUE).toString();
    }
  }

  // Evaluation Contexts (threads) that are awaiting work
  static class Contexts extends IndexedArrayList<Context> {
    private final String name; // Just for debugging

    Contexts(String name) {
      this.name = name;
    }

    @Override
    protected Context[] createInitialArray() {
      return new Context[4];
    }

    public final PrettyStringBuilder writePrettyString(
        PrettyStringBuilder sb, boolean showEmptyQueues, int nodeLimit) {
      if (showEmptyQueues || !isEmpty()) {
        sb.append(name);
        if (count == 0) sb.appendln(" [] ");
        else {
          sb.startBlock();
          foreach(c -> c.writePrettyString(sb, showEmptyQueues, nodeLimit));
          sb.endBlock();
        }
      }
      return sb;
    }

    @Override
    public String toString() {
      return writePrettyString(new PrettyStringBuilder(), true, Integer.MAX_VALUE).toString();
    }
  }

  /**
   * For properties that are declared that they are single threaded It's static, because if the
   * library is not thread safe, it won't help if you have multiple schedulers: it's really a
   * sub-optimal solution, because it just blocks threads a much better solution is to create queues
   */
  private static final ReentrantLock[] _syncLocks;

  /** Used only by OGSchedulerContext */
  void syncLock(int index) {
    _syncLocks[index].lock();
  }

  /** Used only by OGSchedulerContext */
  void syncUnlock(int index) {
    _syncLocks[index].unlock();
  }

  static {
    ReentrantLock[] locks = new ReentrantLock[16];
    for (int i = 0; i < 16; i++) locks[i] = new ReentrantLock();
    _syncLocks = locks;
  }

  /** This class exist to easily recognize this lock in a profiler */
  private static class OGSLock {}

  /** If anyone wants to have a reference to the scheduler this is the reference to keep */
  final WeakReference<Scheduler> weakView = new WeakReference<>(this);

  /** Scheduler's big lock. The idea is to of course use it as infrequently as possible */
  private final OGSLock _lock = new OGSLock();
  /**
   * Global queue! The last place where threads looks for work and accessed only under _lock Review
   * the usage of PriorityQueue
   */
  private final PriorityQueue<NodeTask> _queue =
      new PriorityQueue<>(32, Comparator.comparingInt(NodeTask::priority));
  /** The non-priority based queue, but allows stealing and no-synchronization */
  private final OGSchedulerContext.WorkQueue _qqueue = new OGSchedulerContext.WorkQueue();

  // Contexts/threads that are blocked waiting for node completion
  private final Contexts _waiters = new Contexts("waitThreads");
  // Contexts/threads that are running
  private final Contexts _workers = new Contexts("workThreads");
  // Threads that have nothing to do
  private final Contexts _idlePool = new Contexts("idleThreads");
  // Threads are blocked due to some external blockage (locks etc.)
  private final Contexts _externallyBlocked = new Contexts("externallyBlockedThreads");

  // Currently used under _lock, but consider removing lock completely on at least reduce contention
  private final PriorityQueue<OutOfWorkListener> _onStalledCallbacks =
      new PriorityQueue<>(4, Comparator.comparingInt(OutOfWorkListener::priority));
  // Support for re-adding callbacks in the callback()
  private boolean _onStalledCallbacksInProgress;
  // Pending callbacks to be added/re-added
  private ArrayList<OutOfWorkListener> _onStalledCallbacksPending;

  /**
   * To avoid the issue where a cycle is not detected because no thread is waiting for any part of
   * the cycle, keep track of evaluateAsyncAndTrackCompletion submitted nodes. _lock is needed to
   * update this
   */
  private final IndexedArrayList<TrackedTask> _evalAsyncTrackedNodes =
      new IndexedArrayList<>() {
        @Override
        protected TrackedTask[] createInitialArray() {
          return new TrackedTask[4];
        }
      };

  private boolean _runCycleDetectionOnStall; // Set on every new wait() and reset on 'stalled' state
  /**
   * ID of a spinning thread. 0 means there is no spinning thread Only the currently spinning thread
   * can give up its spinning status i.e. if(_spinningThreadID == threadID ) _spinningThreadID = 0
   * All other manipulations should be under _lock If unlimited threads, there is always a spinning
   * thread with awaitedCausalityID 0 unless workers.count == idealCount
   */
  // Use to ramp up (as opposed to just start them all) the number of threads
  private volatile int _spinningThreadID;
  // Note: it's not final so we can optionally ramp up, the number of threads
  private int _idealCount;
  private volatile int _idlePoolCount; // Should this be larger than ideal?

  private int _threadPriority = Settings.threadPriority;
  private String _threadNamePrefix = "";

  private boolean _schedulerShutdown;

  private final ReentrantLock stallLock = new ReentrantLock();
  // used for stall detection, so use real (System) time
  private volatile long lastStallPrintedTime = System.nanoTime() - stallPrintInterval;

  public OGScheduler() {
    registerScheduler(this);
    setIdealThreadCount(Settings.threadsIdeal, Settings.threadsMin, Settings.threadsMax);
  }

  public OGScheduler(String name, int idealThreadCount, int threadPriority) {
    registerScheduler(this);
    _threadPriority = threadPriority;
    _threadNamePrefix = name;
    setIdealThreadCount(idealThreadCount);
  }

  public OGScheduler(boolean forcedSingleThreaded) {
    registerScheduler(this);
    if (forcedSingleThreaded) _idlePoolCount = _idealCount = 0;
    else setIdealThreadCount(Settings.threadsIdeal, Settings.threadsMin, Settings.threadsMax);
  }

  final Instrumentation instr =
      Instrumentation.isSampling
          ? new Instrumentation(this, _workers, _waiters, _idlePool, _queue, _qqueue)
          : null;

  public final boolean isSingleThreaded() {
    return _idealCount == 0;
  }

  public final void assertNotSingleThreaded(String msg) {
    if (isSingleThreaded()) {
      throw new IllegalStateException(msg + ". Try re-running with -Doptimus.gthread.ideal=4");
    }
  }

  @Override
  public void assertAtLeastTwoThreads(String msg) {
    if (_idealCount < 2) {
      throw new IllegalStateException(msg + ". Try re-running with -Doptimus.gthread.ideal=4");
    }
  }

  final int getSpinningThreadID() {
    return _spinningThreadID;
  }

  /** Negative count implies a portion of cores (i.e. -1 all cores, -2 half the cores) */
  @Override
  public final void setIdealThreadCount(
      int idealCount, int minimumIfNegative, int maximumIfNegative) {
    setActualIdealThreadCount(
        Settings.idealThreadCountToCount(idealCount, minimumIfNegative, maximumIfNegative));
  }

  public final void setIdealThreadCount(int idealCount) {
    setActualIdealThreadCount(Settings.idealThreadCountToCount(idealCount));
  }

  private void setActualIdealThreadCount(int actualIdealCount) {
    _idealCount = actualIdealCount;
    log.info("Setting ideal thread count to " + actualIdealCount);
    // Set _idlePoolCount to _idealCount * 2 to avoid thread leaking when we spin up extra thread to
    // unblock sync stacks
    _idlePoolCount = _idealCount * 2;
  }

  @Override
  public final int getIdealThreadCount() {
    return _idealCount;
  }

  /** Shuts down idle thread, note: Scheduler is not dead */
  @Override
  public void shutdown() {
    synchronized (_lock) {
      _schedulerShutdown = true;

      // shutdown currently idle threads quickly
      Context idle;
      while ((idle = _idlePool.pop()) != null) {
        idle.shutdown = true;
        idle.parked = false;
        LockSupport.unpark(idle.thread);
      }
    }
  }

  private static final ThreadGroup group = new ThreadGroup("optimus_graph");
  private static final AtomicInteger schedulerNumber = new AtomicInteger(1);
  private final int schedulerID = schedulerNumber.getAndIncrement();
  // 0 value is reserved for no-thread
  private final AtomicInteger threadCounter = new AtomicInteger(1);

  class OGLogicalThread implements Runnable {
    private final Context ctx;
    OGSchedulerContext ec; // Is set once in the run method!

    OGLogicalThread(Context ctx) {
      this.ctx = ctx;
    }

    @Override
    public void run() {
      ctx.thread = Thread.currentThread();
      ec = new OGSchedulerContext(null, OGScheduler.this, ctx);
      OGSchedulerContext.setCurrent(ec);
      // We start on _workPool and spinningThreadID == ctx.threadID
      OGTrace.observer.graphEnter(ec.prfCtx);
      try {
        while (true) {
          if (ctx.awaitedTask != null) {
            ctx.userEntry = true;
            ec.setCurrentNode(ScenarioStack.nullNode());
            ec.runAndWait(ctx.awaitedTask); // [SEE_evaluateOnGraphThreadAndTrackCompletion]
            ec.setCurrentNode(null);
            ctx.awaitedTask = null;
            ctx.userEntry = false; // No longer `user thread` and can become generic helper
          }
          NodeTask ntsk = spin(ctx, null); // no _lock
          if (ntsk == null) ntsk = nextStepForWorkingThread(ctx, null); // _lock
          if (ntsk != null) {
            ec.drainQueue(ntsk, -1); // Run everything we can locally....
            // Even though we are not a spinning thread anymore we will try to find work again
            continue;
          }

          // NOTE: Spinning state and parked state are NOT stable states, they are "suggested"
          if (_spinningThreadID == ctx.threadID) continue;
          OGTrace.observer.graphExit(ec.prfCtx);
          OGLocalTables.releaseProfilingContext(ec);
          // Idle and could be replaced by just LockSupport.park() (but provides a great place to
          // put breakpoint)
          while (ctx.parked) {
            LockSupport.parkNanos(ONE_SECOND_IN_NANOSECONDS);
          }
          if (ctx.shutdown) break;
          // See releaseProfilingContext a few lines above
          // This line re-acquires the context. While the thread is idle it's released
          // Also note that if virtualThread case we won't get here we always shutdown
          ec.prfCtx = OGLocalTables.acquireProfilingContext(false);
          OGTrace.observer.graphEnter(ec.prfCtx);
        }
      } catch (Throwable e) {
        panic(e);
      }
    }
  }

  final class OGThread extends Thread {
    final OGLogicalThread logicalThread;

    OGThread(OGLogicalThread logicalThread) {
      super(
          group,
          ogThreadNamePrefix + schedulerID + "-" + _threadNamePrefix + logicalThread.ctx.threadID);
      // We have inherited the contextClassLoader of the thread who created us, but that might be a
      // web server
      // classloader which might get torn down before we get die (especially in Optimus UI tests).
      // To avoid problems
      // we will use the classloader of the OGScheduler class, since our lifetime is tied to that
      setContextClassLoader(OGScheduler.class.getClassLoader());
      setDaemon(true);
      setPriority(_threadPriority);
      this.logicalThread = logicalThread;
    }

    @Override
    public void run() {
      logicalThread.run();
    }

    @Override
    public String toString() {
      return OGScheduler.this.toString();
    }
  }

  private void panic(Throwable e) {
    InfoDumper.graphPanic("panic", schedulerVersion + ": panic called", 66, e, null);
  }

  /** Must be called under the same thread that will 'install' this evaluation context */
  @Override
  public OGSchedulerContext newEvaluationContext(ScenarioStack scenarioStack) {
    Context ctx = new Context(Thread.currentThread(), threadCounter.getAndIncrement());
    return new OGSchedulerContext(new StartNode(scenarioStack), this, ctx);
  }

  public <T> T asIfRunning(NodeTask task, Supplier<T> supplier) {
    Context ctx = new Context(Thread.currentThread(), 1);
    var tmp = new OGSchedulerContext(task, this, ctx);
    T r;
    try {
      OGSchedulerContext.setCurrent(tmp);
      r = supplier.get();
      tmp.localCleanup(this);
    } finally {
      OGSchedulerContext.setCurrent(null);
    }
    return r;
  }

  @Override
  public final void markAsRunnableAndEnqueue(NodeTask ntsk, boolean trackCompletion) {
    // All nodes that are not yet completed must be marked as runnable
    // This condition is just to help plugins that enqueue nodes twice
    if (!ntsk.isDoneOrRunnable()) ntsk.markAsRunnable();

    if (Settings.schedulerAsserts && ntsk.getWaitingOn() != null)
      log.error(
          "markAsRunnableAndEnqueue used for a node that declared its dependency (waitingOn)");

    // Distributed tasks don't serialize causality id
    // (correct, it's meaningless on a remote machine)
    // However, to the local scheduler it looks like a
    // 'safe' task, we need remove that 'safe' promise
    if (ntsk.causalityThreadID() == 0) ntsk.poisonCausality();
    enqueueAndTrackCompletion(ntsk, trackCompletion);
  }

  /**
   * 1. add to the list of tracked nodes 2. enqueues the task 3. Subscribes to the completion
   * notification to remove task from the tracked nodes list
   *
   * <p>Otherwise it's basically an enqueue
   */
  @Override
  public void enqueueAndTrackCompletion(NodeTask task) {
    var trackedNode = new TrackedTask(task);
    synchronized (_evalAsyncTrackedNodes) {
      _evalAsyncTrackedNodes.add(trackedNode);
    }

    enqueue(task);

    task.continueWith(
        new NodeAwaiter() {
          @Override
          public void onChildCompleted(EvaluationQueue eq, NodeTask node) {
            // note that node is the same as trackedNode.task here (tryRemove: don't throw if
            // already removed in nextStep)
            synchronized (_evalAsyncTrackedNodes) {
              _evalAsyncTrackedNodes.tryRemove(trackedNode);
            }
          }
        },
        this);
  }

  static class TrackedTask extends IndexedArrayList.IndexedItem {
    final NodeTask task;

    TrackedTask(NodeTask task) {
      this.task = task;
    }
  }

  /**
   * Enqueues the task into global scheduler. Note this method doesn't modify Causality and if the
   * node scheduled has sync -> async -> sync transition can lead to graph deadlock
   */
  @Override
  public final void enqueue(NodeTask ntsk) {
    if (Settings.schedulerAsserts) {
      if (ntsk.scenarioStack() == null) throw new GraphInInvalidState();
      ntsk.markAsDebugEnqueued();
    }

    synchronized (_lock) {
      boolean givenOut = false;
      /* note - there is a small optimisation possible if we iterate backwards because 'hottest' thread is last */
      var waiters = _waiters.getUnderlyingList();
      for (int i = 0; i < _waiters.count; ++i) {
        Context ctx = waiters[i];
        if (canTake(ntsk, ctx.threadID, ctx.awaitedCausalityID)) {
          wakeupThread(ntsk, ctx);
          givenOut = true;
          break;
        }
      }
      if (!givenOut) {
        if (ntsk.priority() == 0) _qqueue.localPush(ntsk);
        else _queue.offer(ntsk);
        // Spin more threads to handle the task, also handles evalAsync case
        addWorkerIfNeeded();
      }
    }
  }

  /**
   * Always starts a new thread and on that thread starts @nodeTask using runAndWait() which
   * effectively causes "tracking" of completion as the only way to exit that thread would be to
   * complete @nodeTask The most important reason to track completion is to detect cycles
   * [SEE_evaluateOnGraphThreadAndTrackCompletion]
   */
  @Override
  public void evaluateOnGraphThreadAndTrackCompletion(NodeTask nodeTask) {
    synchronized (_lock) {
      injectThread(nodeTask);
      addWorkerIfNeeded();
    }
  }

  /** Called under lock */
  private void wakeupThread(NodeTask tsk2process, Context ctx) {
    boolean removed = tryWakeupThread(tsk2process, ctx, true);
    if (Settings.schedulerAsserts && !removed) throw new GraphInInvalidState();
  }

  /** Called under _lock and return true if removal was successful */
  private boolean tryWakeupThread(NodeTask tsk2process, Context ctx, boolean unpark) {
    if (_waiters.tryRemove(ctx)) {
      _workers.add(ctx);
      OGTrace.schedulerThreadMovedFromWaitToWork(_workers.count);
      ctx.awaitedTask = tsk2process;
      ctx.parked = false; // We are spinning on this field it must be the last thing we set
      // Thread.currentThread() != ctx.thread should be the common case
      if (unpark) LockSupport.unpark(ctx.thread);
      addWorkerIfNeeded();
      return true;
    }
    return false;
  }

  /** Adds a new spinning thread that tries to steal work Called under _lock */
  @SuppressWarnings("UnusedReturnValue")
  private boolean addWorkerIfNeeded() {
    if (_spinningThreadID != 0) return false; // There is another spinning thread working...

    int activeCount = _workers.count + _waiters.count;
    // On ST scheduler, the last thread is exiting and
    // there is still work left.... Forced to inject a thread
    if (isSingleThreaded() && activeCount == 0 && !(_qqueue.isEmpty() && _queue.isEmpty()))
      return injectThread(null);

    if (Settings.limitThreads && activeCount >= _idealCount) {
      return false; // Plenty of working threads already
    } else if (_workers.count >= _idealCount) {
      return false; // Plenty of working threads already
    }
    return injectThread(null);
  }

  /**
   * Adds new thread at a time, first trying from parked pool of idle threads in order not to over
   * saturate the system we are using _spinningThreadID as a gate injectThread is called to spin or
   * to process startingNode
   */
  private boolean injectThread(NodeTask startingNode) {
    // this is useful for idle queue to return the 'hottest' (least likely swapped out) thread
    Context idle = _idlePool.popFirst();
    Context toInject;
    if (idle != null) toInject = idle;
    else toInject = new Context(null, threadCounter.getAndIncrement());

    toInject.userEntry = startingNode != null;
    if (!toInject.userEntry) _spinningThreadID = toInject.threadID;
    _workers.add(toInject);
    toInject.awaitedTask = startingNode;
    toInject.parked = false;
    schedulerThreadAdded();

    if (toInject == idle) LockSupport.unpark(idle.thread);
    else {
      var logicalThread = new OGLogicalThread(toInject);
      if (Settings.useVirtualThreads) virtualExecutorService.submit(logicalThread);
      else new OGThread(logicalThread).start();
    }
    return true;
  }

  /**
   * Called by user (not helper) thread that 'joins' the graph (sync way) on the first entry into
   * the graph
   */
  final void enterGraph(Context ctx, boolean wasExternallyBlocked) {
    OGTrace.observer.graphEnter(ctx.schedulerContext.prfCtx);
    synchronized (_lock) {
      if (wasExternallyBlocked) _externallyBlocked.remove(ctx);
      _workers.add(ctx);
      ctx.userEntry = true;
      schedulerThreadAdded();
      addWorkerIfNeeded();
    }
  }

  /** Called by user thread that 'joins' the graph */
  final void exitGraph(Context ctx, boolean isExternallyBlocked) {
    if (Settings.schedulerAsserts && ctx.thread != Thread.currentThread())
      throw new GraphInInvalidState();

    ctx.schedulerContext.localCleanup(this);
    OGTrace.observer.graphExit(ctx.schedulerContext.prfCtx);

    synchronized (_lock) {
      _workers.remove(ctx);
      if (isExternallyBlocked) _externallyBlocked.add(ctx);
      OGTrace.schedulerThreadRemovedFromWork(_workers.count, _waiters.count);
      updateCausalityChainOnPossibleStall(0);
      // If this thread was spinning OR after we exit only one thread that might be spinning left
      int spinningThreadID = _spinningThreadID;
      if (ctx.threadID == spinningThreadID || _workers.count == 1) _spinningThreadID = 0;

      if (_workers.count == 0) {
        if (!_onStalledCallbacks.isEmpty()) runOnStalledCallbacks();
        // Cleanup global queue that might have tasks that are processed but are still enqueued
        // Note: just call .clean() but for debugging purposes, don't touch nodes that are not
        // done...
        _queue.removeIf(NodeTask::isDoneOrNotRunnable);

        // Cleanup quick global queue
        _qqueue.localCleanup(null);
      }

      if (isExternallyBlocked || _workers.count > 0) {
        // In case there are other user threads and can extra thread to process it's requests
        addWorkerIfNeeded();
      }
    }
  }

  /**
   * Returns null when waiting thread is 'done' or spinning thread needs to exit
   *
   * <ol>
   *   <li>Search for work (including possibly running onStalledCallbacks and deadlock detection)
   *   <li>If no work is found but at least one other thread is working and we can help (causality =
   *       0, enough cores), and no other thread is spinning, then spin
   *   <li>If we are the only working thread and we are spinning but there is no work, stop spinning
   *       and either:
   *       <ol>
   *         <li>if we're waiting for a task, move to _waiters (sleep)
   *         <li>otherwise, go idle
   *       </ol>
   * </ol>
   */
  private NodeTask nextStepForWorkingThread(Context ctx, NodeTask awaitTask) {
    synchronized (_lock) {
      if (Settings.schedulerAsserts
          && (!_workers.contains(ctx) || ctx.thread != Thread.currentThread()))
        throw new GraphInInvalidState();

      NodeTask ntsk;
      if ((ntsk = findWork(ctx, awaitTask, true)) != null) return ntsk;

      // Spin if no-one else spins and there is at
      // least one other thread 'working' that we could help
      if (_spinningThreadID == 0
          && _workers.count > 1
          && _workers.count <= _idealCount
          && ctx.awaitedCausalityID == 0) {
        _spinningThreadID = ctx.threadID;
      } else if (!(_workers.count > 1 && ctx.threadID == _spinningThreadID)) {
        // The condition above is need to keep on spinning if there is another worker thread and
        // this thread can help it
        if (ctx.threadID == _spinningThreadID)
          // This is the only work thread (_workPool.count == 1) with no work, stop spinning
          _spinningThreadID = 0;

        _workers.remove(ctx); // OGTrace.schedulerThreadXXXReporting is below based on transition
        if (awaitTask != null) {
          // Go back to waiting....
          _waiters.add(ctx);
          ctx.awaitedTask = awaitTask;
          ctx.parked = true;

          OGTrace.schedulerThreadMovedFromWorkToWait(_workers.count, _waiters.count, _idealCount);
          if (!Settings.limitThreads) {
            ctx.queue.drainTo(_qqueue);
            addWorkerIfNeeded();
          }
          updatePossibleLostConcurrency(false);
          updateCausalityChainOnPossibleStall(0);

          // Even though findWorkFromQueue detects cycles, in MT case the cycle could be formed
          // after
          // the local calls to findWorkFromQueue by another thread. We need to set the flag to
          // allow
          // a thread from a thread pool that is not busy to run cycle detection
          _runCycleDetectionOnStall = true;
        } else {
          OGTrace.schedulerThreadRemovedFromWork(_workers.count, _waiters.count);
          updateCausalityChainOnPossibleStall(0);
          // Experiment with this: int idlePoolCount = threadCounter.get() > 100 ? _idlePoolCount :
          // _idlePoolCount - 2;
          if (!Settings.useVirtualThreads
              && _idlePool.count < _idlePoolCount
              && !_schedulerShutdown) {
            ctx.parked = true;
            _idlePool.add(ctx);
          } else {
            // We aren't parking this thread, so it will naturally die in OGLogicalThread.run()
            ctx.shutdown = true; // Goodbye helper thread...
          }
          scheduleTrackedWorkIfNeeded();
        }
      }
    }
    return null;
  }

  /* Must be called under _lock */
  private void scheduleTrackedWorkIfNeeded() {
    if (_workers.count == 0 && _waiters.count == 0) {
      NodeTask firstTrackedTask = null;
      synchronized (_evalAsyncTrackedNodes) {
        var trackedTask = _evalAsyncTrackedNodes.pop();
        if (trackedTask != null) firstTrackedTask = trackedTask.task;
      }

      if (firstTrackedTask != null) evaluateOnGraphThreadAndTrackCompletion(firstTrackedTask);
    }
  }

  /**
   * Marks this thread as "externally blocked" while func is run, so that this thread doesn't count
   * as "working". This is mostly equivalent to this thread temporarily exiting the graph. If this
   * was the last working thread, on-stalled callbacks will be triggered. This is useful when func
   * is holding some non-graph lock that is indirectly waiting for graph work to complete.
   */
  <T> T doBlockingAction(Context ctx, Function0<T> func) {
    if (ctx.schedulerContext.syncStackCount() == 0) return func.apply();
    else {
      exitGraph(ctx, true);
      try {
        return func.apply();
      } finally {
        enterGraph(ctx, true);
      }
    }
  }

  /**
   * Tries to find work at least once and spins if the current ctx is the spinning thread Returns
   * the task to process, and null if thread stops being the spinning thread
   */
  private NodeTask spin(Context ctx, NodeTask awaitTask) {
    OGTrace.observer.graphSpinEnter(ctx.schedulerContext.prfCtx);
    do {
      NodeTask ntsk = findWork(ctx, awaitTask, false);
      if (ntsk != null) return completeSpin(ctx, ntsk);

      // Linux/Windows differences are big here, consider more intelligence.
      // We just want to give up out time slice passing sleep(0) on some JVM would just return
      // Also, it's possible that sometimes less aggressive spinning is better
      // After upgrading to JVM9+ look at http://openjdk.java.net/jeps/285
      if (_spinningThreadID == ctx.threadID) {
        synchronized (_lock) {
          // Deal with stalls
          if (_workers.count == 1) {
            ntsk = findWork(ctx, awaitTask, true);
            if (ntsk != null) return completeSpin(ctx, ntsk);

            // The only thread 'in-graph' and can't find any work... stop it from spinning and exit
            _spinningThreadID = 0;
          }
        }
        Thread.yield();
      }
    } while (_spinningThreadID == ctx.threadID);
    OGTrace.observer.graphSpinExit(ctx.schedulerContext.prfCtx);
    return null;
  }

  private NodeTask completeSpin(Context ctx, NodeTask ntsk) {
    // We have work, but there might be room to steal work and increase the number of threads
    if (_spinningThreadID == ctx.threadID) {
      synchronized (_lock) {
        _spinningThreadID = 0; // Free up the spinning honor if this thread held it
        addWorkerIfNeeded();
      }
    }
    OGTrace.observer.graphSpinExit(ctx.schedulerContext.prfCtx);
    return ntsk;
  }

  /**
   * The thread other than the one identified by `ctx` completed node That ctx is potentially
   * waiting on this node
   */
  final void awakeOnComplete(NodeTask ntsk, Context ctx) {
    synchronized (_lock) {
      if (ctx.awaitedTask == ntsk) tryWakeupThread(ntsk, ctx, true);
    }
  }

  /**
   * Thread calls wait to 'block' on completion of awaitTask (which is never null here)
   *
   * <p>Returns null when task was successfully awaited or we found another NodeTask to process in
   * the meantime
   *
   * <p>if causalityID is 0 we are ready to pick any work at all! Otherwise we can pick up only the
   * work for nodes with given causality.
   *
   * <p>OPTIMUS-11566 There is what seems to be a common degenerate case to be addressed. When DAL
   * schedules 1000s of individual items back onto the queue they will be picked up by the call to
   * findWorkOnForwardChain and will remain on qqueue for a long time until they will get cleaned
   * up. A quicker cleanup would be significantly more memory efficient
   */
  final NodeTask wait(NodeTask awaitTask, Context ctx, int causalityID) {
    NodeTask ntsk = null;
    try {
      ctx.awaitedCausalityID = causalityID;
      // In the ideal case when there are no sync-stacks we can just become another thread that
      // spins and waits
      if (causalityID == 0) {
        if ((ntsk = spin(ctx, awaitTask)) != null) return ntsk;
      }

      // See comment above: findWorkOnForwardChain must come before the call to
      // _qqueue.scanToFindWork in findWork
      // in the case where large batches are returned from DAL calls (otherwise scanToFindWork can
      // be a bottleneck)
      // Note: this is not in nextStepForWorkingThread because not all calls to it need
      // findWorkOnForwardChain
      if (Settings.limitThreads && (ntsk = findWorkOnForwardChain(ctx, awaitTask, true)) != null)
        return ntsk;

      if ((ntsk = nextStepForWorkingThread(ctx, awaitTask)) != null) return ntsk;
      ntsk = fullWait(awaitTask, ctx);
    } catch (Throwable t) {
      panic(t);
    } finally {
      ctx.awaitedCausalityID = 0;
      ctx.awaitedTask = null;
      // needed here because we already have a task (may not enter drainQueue)
      OGSchedulerTimes.schedulerStallingEnd();
    }
    return ntsk;
  }

  private NodeTask fullWait(final NodeTask awaitTask, Context ctx) {
    int statsDumped = 0; // Just a counter to keep track of how many times we print the stall info
    long startTime = System.nanoTime(); // approximate wait time in ns
    long lastDeadlockDetectionTime = startTime; // We just ran it at the entrance to wait()
    NodeTask ntsk;

    var blockingWaitEvent =
        BlockingWaitCounter.report(
            ctx.thread.getName(),
            awaitTask.toString(),
            new Exception("Blocking Wait"),
            awaitTask.getId(),
            ctx.awaitedTaskEndOfChainID);

    OGTrace.observer.graphEnterWait(ctx.schedulerContext.prfCtx, ctx.awaitedCausalityID);
    // NOTE: Spinning state and parked state are NOT stable states, they are "suggested" states
    while (true) {
      // We could have been unparked() to spin or to process a specific task
      if (!ctx.parked) {
        if (_spinningThreadID == ctx.threadID) {
          if ((ntsk = spin(ctx, awaitTask)) != null) break;
          if ((ntsk = nextStepForWorkingThread(ctx, awaitTask)) != null) break;
          continue; // back to the top...
        }
        ntsk = ctx.awaitedTask;
        break; // Another thread just awoke this thread
      }

      // used for deadlock detection, so use real (System) time
      long time = System.nanoTime();
      if ((time - lastDeadlockDetectionTime) > deadlockDetectionTimeout) {
        lastDeadlockDetectionTime = time;
        // findWorkOnForwardChain effectively runs cycle detection
        // Note: this does resolve extra helper threads going away, but it's not a great 'fix'
        // because it effectively runs on timer.
        ntsk = findWorkOnForwardChain(ctx, awaitTask, true);
        if (ntsk != null) {
          synchronized (_lock) {
            // if wakeup is successful, ntsk will be stored in ctx.awaitedTask
            tryWakeupThread(ntsk, ctx, false);
          }
          continue; // while(true) loop
        }
        // This is a recovery code for not called stalled callbacks. This is not a fix...
        // First quick check outside of the _lock that we seem to hit the bad state
        if (!_onStalledCallbacks.isEmpty() && _workers.count == 0) {
          synchronized (_lock) {
            // Double check...
            if (!_onStalledCallbacks.isEmpty() && _workers.count == 0 && runOnStalledCallbacks()) {
              log.error("Recovering from stalled callbacks not called!");
            }
          }
        }
      }

      statsDumped = tryPrintStall(statsDumped, time, startTime, ctx);
      // Logically .park(), place breakpoint after this line on stall
      LockSupport.parkNanos(ONE_SECOND_IN_NANOSECONDS);
    }

    OGTrace.observer.graphExitWait(ctx.schedulerContext.prfCtx);
    // N.B. we use OGTrace.nanoTime() (which might be a fake clock) here for profiling, but
    // System.nanoTime() elsewhere
    // for logging timeouts etc.
    long endTime = OGTrace.nanoTime();
    // consider removing this callout (unify with OGTrace above)
    NodeTrace.addGraphWaitTime(
        new WaitProfile(
            startTime,
            endTime,
            awaitTask,
            ctx.awaitedTaskEndOfChainInfoID,
            NodeTrace.traceWaits.getValue() ? new Exception("Blocking Wait") : null));

    BlockingWaitCounter.reportCompleted(blockingWaitEvent);

    if (ctx.captureWaiterForProfiling || Settings.reportEveryFullWait)
      ctx.awaitedCausalityStalledOn.recordFullWait(ctx.schedulerContext, ntsk, endTime - startTime);

    if (ctx.awaitedCausalityNeedsReporting) {
      ctx.awaitedCausalityStalledOn.recordFullWait(ctx.schedulerContext, ntsk, endTime - startTime);
      OGSchedulerLostConcurrency.reportCriticalSyncStack(
          ntsk, ctx.awaitedCausalityStalledOn, endTime - startTime);
      ctx.awaitedCausalityStalledOn = PluginType.None();
      ctx.awaitedCausalityNeedsReporting = false;
      ctx.captureWaiterForProfiling = false;
    }

    return ntsk;
  }

  private boolean isLastTaskAdapted(Context ctx) {
    NodeTask lastTask = lastTaskInWaitingChain(ctx);
    return lastTask != null && lastTask.isAdapted();
  }

  private NodeTask lastTaskInWaitingChain(Context ctx) {
    if (ctx.awaitedTask != null) {
      ArrayListWithMarker<NodeTask> forwardChain = ctx.awaitedTask.visitForwardChain(1000);
      return forwardChain.get(forwardChain.size() - 1);
    } else return null;
  }

  private int tryPrintStall(int statsDumped, long curTime, long waitStartTime, Context ctx) {
    // used for stall detection, so use real (System) time
    long tryPrintStallTime = System.nanoTime();
    var reportImmediately = GraphDiagnostics.reportImmediately(_workers.count);
    boolean loggingEnabled = Settings.reportNoProgress > 0;
    boolean dontPrintMoreThanTwice = statsDumped <= 2;
    // 30 sec by default
    boolean waitingTimeExceedsLimit = (curTime - waitStartTime) > reportNoProgress;
    boolean haventPrintedStall = tryPrintStallTime - lastStallPrintedTime > stallPrintInterval;
    boolean timeToReport =
        loggingEnabled && dontPrintMoreThanTwice && waitingTimeExceedsLimit && haventPrintedStall;
    // Please keep the tryLock action at the end - if the lock is acquired, please make sure the
    // finally{} block will be hit to release it
    if ((reportImmediately || timeToReport) && stallLock.tryLock()) {
      try {
        // for adapted tasks we want to print smaller stall info first and full dump when timeout
        // exceeded
        if (isLastTaskAdapted(ctx)) {
          // haven't already printed stall info for adapted tasks
          if (statsDumped == 0) {
            log.info("Printing minimal stall info for adapted waiting task");
            printGraphState(false, ctx.awaitedTask);
            statsDumped = 1;
          } else if ((tryPrintStallTime - waitStartTime) > adaptedFullInfoPrintTimeout
              && statsDumped == 1) {
            // we have already logged the short stall info for adapted node case
            log.info("Printing full stall info for adapted waiting task");
            printGraphState(true, ctx.awaitedTask);
            statsDumped = 2;
          }
        } else if (statsDumped == 0) {
          log.info("Printing stall info for non adapted waiting task");
          lastStallPrintedTime = tryPrintStallTime;
          printGraphState(true, ctx.awaitedTask);
          statsDumped = 1;
        }
      } finally {
        stallLock.unlock();
      }
    }
    return statsDumped;
  }

  private void printGraphState(boolean fullStallInfo, NodeTask awaitedTask) {
    if (DiagnosticSettings.outOfProcess) JMXConnection.graph().getFullSchedulerState();
    else GraphDiagnostics.graphStall(this, fullStallInfo, awaitedTask);
  }

  static boolean canTake(NodeTask task, int threadID, int causalityID) {
    // Scheduler thread is compatible with everything if causalityID is zero (no sync stacks on
    // thread)
    if (causalityID == 0) return true;
    int taskTID = task.causalityThreadID();
    int taskCausalityId = task.causalityID();
    // task is compatible with every scheduler if it has no causality information marked on it
    return ((taskCausalityId == 0 && taskTID == 0)
        // task is compatible with this thread if the causalityID is not less than our current one
        // (because it's caused
        // by work we're currently processing)
        || (causalityID <= taskCausalityId && (taskTID == threadID || taskTID == 0)));
  }

  /**
   * Called from exactly one place under the following conditions: (1) Under _lock (2) Could not
   * find or safely take any work (3) current ctx is in _workPool If _workPool.count == 1 and
   * current thread is the only thread running and can't find work.....
   */
  private boolean isStalling(Context ctx) {
    if (Settings.limitThreads) return _workers.count == 1;
    else {
      if (_workers.count != 1) return false;
      // Can't find any work or *take it safely* but there might be work on queues that we couldn't
      // just take,
      // or we skipped searching somewhere because Settings.limitThreads is false
      // Spinning thread would have snooped all the threads
      if (_spinningThreadID != ctx.threadID && _spinningThreadID != 0) {
        addWorkerIfNeeded();
        return false;
      }
      return true;
    }
  }

  private NodeTask findWork(Context ctx, NodeTask awaitTask, boolean desperateSearchUnderLock) {
    NodeTask ntsk = awaitTask;
    if (ntsk != null && ntsk.isDoneEx()) return ntsk;

    // Stealing random tasks is fast but doesn't have causality checks
    if (ctx.awaitedCausalityID == 0) {
      if ((ntsk = _qqueue.tryToSteal()) != null) return ntsk;

      if ((ntsk = findWorkByStealing(ctx)) != null) return ntsk;
    }

    if (desperateSearchUnderLock) {
      // scanToFindWork is expensive and not needed in the case of unlimited threads because
      // there is always (unless workers.count == idealCount) a spinning thread with
      // awaitedCausalityID == 0 and it will steal the work in tryToSteal call above
      if (Settings.limitThreads)
        if ((ntsk = _qqueue.scanToFindWork(ctx.threadID, ctx.awaitedCausalityID)) != null)
          return ntsk;

      if ((ntsk = findWorkFromLowPriorityQueue(ctx)) != null) return ntsk;

      // This is the last working thread and couldn't find any work, run `OnStalledCallbacks`
      if (isStalling(ctx)) {
        if (runOnStalledCallbacks()) {
          // runOnStalledCallbacks could have scheduled nodes so we need to re-check the queues
          if ((ntsk = _qqueue.scanToFindWork(ctx.threadID, ctx.awaitedCausalityID)) != null)
            return ntsk;
          if ((ntsk = findWorkFromLowPriorityQueue(ctx)) != null) return ntsk;
        }

        // runOnStalledCallbacks() could have awakened one of the threads in which case we are no
        // longer stalling. Therefore, must check again _workPool.count
        if (_workers.count == 1) {
          updateCausalityChainOnPossibleStall(1);
          // Check for deadlocks... Since some spinning thread could have just 'completed' someone's
          // cycle
          if (_runCycleDetectionOnStall) {
            // For every wait call, we run cycle detection only once
            _runCycleDetectionOnStall = false;
            // This is a loop over _waitPool, reconsider with Loom..
            _waiters.foreach(this::detectCycles);
            detectCycles(ctx);
            // In case graph thread is **invisibly** waiting on tracked nodes, which may have
            // cycles, to complete.
            // Another solution would be to wrap the **invisible wait** with
            // EvaluationContext.current.doBlockingAction,
            // or establish a dependency between the wait and these nodes [SEE_WAIT_FOR_TRACKED]
            _evalAsyncTrackedNodes.foreach(tracked -> detectCycles(tracked.task));
            // detectCycles can break cycles and generate tasks need to check quick queue (but not
            // low priority queue
            // because detectCycles won't put work there)
            if ((ntsk = _qqueue.scanToFindWork(ctx.threadID, ctx.awaitedCausalityID)) != null)
              return ntsk;
          }
        }
      }
    }

    return null;
  }

  private NodeTask findWorkByStealing(Context ctx) {
    NodeTask ntsk = null;
    if (Settings.limitThreads)
      ntsk = tryStealing(ctx, _waiters); // Try stealing from blocked threads first
    if (ntsk == null) ntsk = tryStealing(ctx, _workers);
    // Consider counting and reporting: ctx.schedulerContext.prfCtx.ctx.stealCount++;
    return ntsk;
  }

  /** Called under _lock! */
  private NodeTask findWorkFromLowPriorityQueue(Context ctx) {
    int causalityID = ctx.awaitedCausalityID;
    NodeTask ntsk = null;
    if (!_queue.isEmpty()) {
      if (causalityID == 0) ntsk = _queue.poll();
      else {
        // Currently O(n), consider other data structures
        java.util.Iterator<NodeTask> it = _queue.iterator();
        while (it.hasNext()) {
          NodeTask ttsk = it.next();
          if (ttsk.isDoneEx()) {
            it.remove(); // Cleanup
          } else if (canTake(ttsk, ctx.threadID, causalityID)) {
            it.remove();
            ntsk = ttsk;
            break;
          }
        }
      }
    }
    return ntsk;
  }

  /**
   *
   *
   * <ol>
   *   <li>Follows the waitingOn chain to see if it can grab the last dependency if it's ready to
   *       run
   *   <li>As part of that process and in order to avoid cycles, it checks for cycles
   *   <li>Returns the end of the chain in ctx.awaitedTaskInfoEndOfChain field (needs to be cleared
   *       after the call)
   * </ol>
   *
   * @param returnFirstRunnableTask If true return first runnable task or run cycle detection (which
   *     can generate tasks)
   */
  private NodeTask findWorkOnForwardChain(
      Context ctx, NodeTask awaitTask, boolean returnFirstRunnableTask) {
    // is awaitedTask itself runnable? so lets try to run that
    // is awaitedTask completed? unwind the wait and any search we can be in...
    if (awaitTask.isDoneOrRunnable()) return awaitTask;
    NodeTask ntsk = null;
    // Upgrade causality of the entire chain!
    // In case of a cycle we need to prevent ourselves from looping forever, effectively running
    // Floyd's cycle detection
    NodeTask tsk = awaitTask.getWaitingOn();
    NodeTask fastWalker = tsk;
    NodeTask endOfChain = tsk;
    if (fastWalker != null) fastWalker = fastWalker.getWaitingOn();

    while (tsk != null && tsk != fastWalker) {
      // Updating causality, to allow the completion of the tasks to wake up the thread that is
      // awaiting for this chain
      tsk.setCausality(ctx.awaitedCausalityID, ctx.threadID);
      endOfChain = tsk;
      if (returnFirstRunnableTask
          && tsk.isRunnable()
          && tsk.valueWasRequested()) { // [SEE_XSFT_REQUESTED]
        ntsk = tsk; // We can just run this command right away
        break;
      }

      tsk = tsk.getWaitingOn();
      if (fastWalker != null) {
        fastWalker = fastWalker.getWaitingOn();
        if (fastWalker != null) fastWalker = fastWalker.getWaitingOn();
      } else if (tsk != null)
        // fastWalker should 'try again' in case of a cycle, otherwise will just end with 'slow'
        // walker
        fastWalker = tsk.getWaitingOn();
    }

    if (tsk != null && tsk == fastWalker && returnFirstRunnableTask) {
      /*
       We have a cycle! In most cases here we just collect path and set exception, but in the case of XSFT we'll
       attempt to recover, which involves re-enqueuing a proxy after updating our knowledge of tweak dependencies.
       If re-enqueuing happens to land on the global scheduler queue (e.g. DAL) we'll need to take _lock
       when we're already holding _visitLock (the lock on the NodeTask that protects walking the forward chain).
       This can cause a deadlock:
       -> Thread A holds scheduler _lock and gets to detectCyclesAndRecover, which needs to wait for _visitLock
       -> Thread B was recovering from a cycle and holds _visitLock, but needs _lock to enqueue the proxy retry

       To avoid it, make sure that we take the global scheduler _lock here (normally a huge performance penalty,
       but by the time we're here we know we have a cycle and there's really nothing else we can do). That guarantees
       that when we take _visitLock we know we must already hold _lock.
       // [SEE_XSFT_GLOBAL_SCHEDULER_ENQUEUE]
      */
      synchronized (_lock) {
        detectCycles(awaitTask);
      }
      ntsk = awaitTask; // It's going to be completed by now
    }

    // For proxies it's not interesting to show A -> A (looks like a cycle)
    if (endOfChain == null || awaitTask.cacheUnderlyingNode() == endOfChain) {
      ctx.awaitedTaskInfoEndOfChain = NodeTaskInfo.EndOfChain;
      ctx.awaitedTaskEndOfChainInfoID = 0;
      ctx.awaitedTaskEndOfChainID = 0;
    } else {
      ctx.awaitedTaskInfoEndOfChain = endOfChain.executionInfo();
      ctx.awaitedTaskEndOfChainInfoID = endOfChain.getProfileId();
      ctx.awaitedTaskEndOfChainID = endOfChain.getId();
      ctx.awaitedTaskPluginEndOfChain = endOfChain.getPlugin();
      ctx.awaitedTaskPluginTypeEndOfChain = endOfChain.getReportingPluginType();
    }
    return ntsk;
  }

  private void schedulerThreadAdded() {
    OGTrace.schedulerThreadAddedToWork(_workers.count, _waiters.count);
    updatePossibleLostConcurrency(true);
  }

  /**
   * Update lost concurrency info for any waitCtx that has a sync stack.
   *
   * @param speculative if true, might be over-reporting - the sync stack might not actually cause a
   *     stall
   */
  @SuppressWarnings("unused")
  private void updatePossibleLostConcurrency(boolean speculative) {
    if (!Settings.limitThreads) return;
    var waiters = _waiters.getUnderlyingList();
    if (_waiters.count > 0 && _workers.count + _waiters.count >= _idealCount) {
      for (int i = 0; i < _waiters.count; i++) {
        Context waitCtx = waiters[i];
        if (waitCtx.awaitedCausalityID > 0) {
          waitCtx.captureWaiterForProfiling = true;
          // this will probably be updated in updateCausalityChainOnPossibleStall but might as well
          // set it here too
          // note - it might not be because the condition is different
          // (updateCausalityChainOnPossibleStall only runs
          // if there is no other work)
          if (waitCtx.awaitedCausalityStalledOn == PluginType.None())
            waitCtx.awaitedCausalityStalledOn = waitCtx.awaitedTaskPluginTypeEndOfChain;
          if (!waitCtx.awaitedTask.scenarioStack().ignoreSyncStacks())
            waitCtx.awaitedCausalityNeedsReporting = true;
        }
      }
    }
  }

  /**
   * Called under _lock during possible stall If there are only waiting threads left (i.e. no
   * progress is being made) update their stall causality a. The forward chain should be stable now
   * (nothing is actually progressing) b. It's not a trivial cost (walking forward chains) and we
   * can run findWorkOnForwardChain now (nothing is actually progressing) Note: expectedWorkCount ==
   * 1 means this is not a possible stall but a real one
   */
  private void updateCausalityChainOnPossibleStall(int expectedWorkCount) {
    if (_waiters.count > 0 && _workers.count == expectedWorkCount) {
      boolean syncStackPresent = false;
      var waiters = _waiters.getUnderlyingList();
      for (int i = 0; i < _waiters.count; i++) {
        Context waitCtx = waiters[i];
        findWorkOnForwardChain(waitCtx, waitCtx.awaitedTask, false);

        // note - this is the same as checking waitCtx.awaitedCausalityNeedsReporting (ie, there was
        // a sync stack)
        // but these could be out of sync because awaitedCausalityNeedsReporting is a stronger
        // condition
        if (waitCtx.awaitedCausalityID > 0) { // consider adding check for other work available too
          if (waitCtx.awaitedCausalityStalledOn == PluginType.None())
            waitCtx.awaitedCausalityStalledOn = waitCtx.awaitedTaskPluginTypeEndOfChain;
          syncStackPresent = true;
        }
      }

      ArrayList<AwaitedTasks> awaitedTasksList = new ArrayList<>(waiters.length);
      for (Context waitCtx : waiters) {
        if (waitCtx == null) break;
        awaitedTasksList.add(
            new AwaitedTasks(
                waitCtx.awaitedTask,
                waitCtx.awaitedTaskInfoEndOfChain,
                waitCtx.awaitedTaskPluginEndOfChain,
                waitCtx.awaitedTaskPluginTypeEndOfChain));
      }
      AwaitedTasks[] awaitedTasks = awaitedTasksList.toArray(new AwaitedTasks[0]);
      OGTrace.schedulerStalling(new SchedulerStallSource(awaitedTasks, syncStackPresent));
    }
  }

  private static NodeTask tryStealing(Context requestingCtx, Contexts ctxs) {
    Context[] q = ctxs.getUnderlyingList(); // Grab array
    // Grab count (race in allocating new array and changing the count)
    int count = Math.min(ctxs.count, q.length);
    if (count == 0) return null;

    int random = ThreadLocalRandom.current().nextInt(count);
    for (int i = 0; i < count; i++) {
      Context ctx = q[(i + random) % count];
      if (ctx != null && ctx != requestingCtx) { // It can change under us, so we need to check
        NodeTask ntsk = ctx.queue.tryToSteal();
        if (ntsk != null) return ntsk;
      }
    }
    return null;
  }

  /** If the the cycle is found awaitedTask is completed with CircularReferenceException */
  private void detectCycles(Context awaitingCtx) {
    // Consider the case where c = a + b and a throws....
    // meanwhile b gets stolen by a helper thread and forms a cycle with a sync stack (gets into
    // wait()) if we only check for userEntry thread we effectively will just leak the thread
    // (reporting cycle is not important)
    // in the the case of virtual threads, the virtual thread(s) will get GCed
    if (awaitingCtx.userEntry || !Settings.useVirtualThreads) detectCycles(awaitingCtx.awaitedTask);
  }

  /** If the the cycle is found awaitedTask is completed with CircularReferenceException */
  private void detectCycles(NodeTask awaitedTask) {
    if (awaitedTask == null || awaitedTask.isDoneEx()) return;

    awaitedTask.detectCyclesAndRecover(this);
  }

  /** Note: Called under _lock */
  private void runBatcherDiagnostics() {
    final PrettyStringBuilder sb = new PrettyStringBuilder();
    final int[] seenReadyToRunTasks = {0};
    Consumer<Context> c =
        ctx -> {
          seenReadyToRunTasks[0] = 0;
          ctx.queue.foreach(
              task -> {
                if (task.isRunnable()) {
                  if (sb.isEmpty())
                    sb.appendln(
                        "On stalled callback will be raised, but queues contain runnable tasks");

                  if (seenReadyToRunTasks[0] == 0) {
                    sb.appendln("SyncStack for thread containing runnable nodes:");
                    ctx.schedulerContext.foreachSyncStackNode(sb::appendln);
                    sb.appendln("Runnable Node(s):");
                  }
                  sb.appendln(task.toString());
                  seenReadyToRunTasks[0] += 1;
                }
              });
        };
    _workers.foreach(c);
    _waiters.foreach(c);

    if (!sb.isEmpty() || !_queue.isEmpty()) log.warn(sb.toString());
  }

  private boolean checkNewWorkAdded(int initQSize) {
    int afterSize = _queue.size() + _qqueue.size();
    return afterSize != initQSize;
  }

  /**
   * Run all callbacks that were registered with addOutOfWorkListener
   *
   * <p>Note: Currently this must always be called while holding the _lock <br>
   * TODO (OPTIMUS-17080): Avoid calling the callbacks under lock
   *
   * @return true if something was added to one the scheduler queues!
   */
  private boolean runOnStalledCallbacks() {
    _onStalledCallbacksInProgress = true;
    boolean newWorkAdded = false;

    var cb = _onStalledCallbacks.peek();
    if (cb != null) { // Also means the queue is not empty

      // Give an opportunity to batchers to discover more nodes that could be running ....
      // Specifically XSFT delayed proxy chains could be broken up here to increase concurrency
      var stopBatchProcessing = false;
      for (var callback : _onStalledCallbacks) {
        var newCount = callback.checkForNewWork(OGScheduler.this);
        stopBatchProcessing |= (newCount == Integer.MAX_VALUE);
      }

      if (stopBatchProcessing) return true;

      int initQSize = _queue.size() + _qqueue.size();
      int priority = cb.priority();

      // This is great place to put a breakpoint and examine why we don't have more nodes batched
      if (Settings.schedulerDiagnoseBatchers) runBatcherDiagnostics();

      var outstandingTasks = new ArrayList<NodeTask>();

      // Call/Remove callbacks until the priority changes and no work has been added
      while ((cb = _onStalledCallbacks.peek()) != null
          && (cb.priority() == priority || !newWorkAdded)) {
        _onStalledCallbacks.poll(); // Remove from queue
        if (!newWorkAdded)
          priority =
              cb.priority(); // Update (to a higher priority) until at least some nodes were added
        try {
          cb.onGraphStalled(OGScheduler.this, outstandingTasks);
        } catch (Throwable t) {
          // If the onGraphStalled callbacks threw, we are likely heading for a hang, so might as
          // well panic right now.
          panic(new GraphException("onGraphStalled threw an exception", t));
        }
        newWorkAdded = checkNewWorkAdded(initQSize);
      }

      if (!newWorkAdded && !outstandingTasks.isEmpty()) {
        var unique = new IdentityHashMap<NodeTask, NodeTask>();
        for (var task : outstandingTasks) {
          if (unique.putIfAbsent(task, task) == null) detectCycles(task); // [SEE_XSFT_BATCH_SCOPE]
        }

        newWorkAdded = checkNewWorkAdded(initQSize);
      }
    }

    if (Settings.schedulerDiagnoseBatchers && !_onStalledCallbacks.isEmpty())
      log.info(
          "Not all callbacks were raised, this is a recent change and is not an issue in itself");

    // Process any pending additions
    if (_onStalledCallbacksPending != null) _onStalledCallbacks.addAll(_onStalledCallbacksPending);
    _onStalledCallbacksInProgress = false;
    _onStalledCallbacksPending = null;
    return newWorkAdded;
  }

  /**
   * Early attempt to raise delayed batchers Note: Potentially called from outside of any context or
   * just from a different scheduler Basically don't rely on any OGSchedulerContext.current() ...
   */
  @Override
  public final void reportCallsInFlightCountCleared() {
    synchronized (_lock) {
      if (_workers.count == 0) runOnStalledCallbacks();
    }
  }

  /**
   * Note: most often has a context but we should allow calling it from non-optimus thread and
   * therefore ctx should be assumed to be unavailable
   */
  public final void addOutOfWorkListener(OutOfWorkListener callback) {
    if (callback == null) throw new GraphException("callback can't be null");
    var mustWait = callback.mustWaitDelay();
    if (mustWait > 0) {
      runDelayedTask(
          // call the impl so that we aren't stuck in an infinite loop of mustWaits.
          () -> addOutOfWorkListenerImpl(callback), mustWait, TimeUnit.MILLISECONDS);
    } else {
      addOutOfWorkListenerImpl(callback);
    }
  }

  private void addOutOfWorkListenerImpl(OutOfWorkListener callback) {
    // This contains the logic to add the callback, without the mustWait timing.

    if (callback.mustCallDelay() > 0) {
      // Fine to run if the callback has been removed or not added -- we just won't do very much!
      runDelayedTask(
          () -> runOutOfWorkListeners(t -> t == callback),
          callback.mustCallDelay(),
          TimeUnit.MILLISECONDS);
    }

    synchronized (_lock) {
      if (_onStalledCallbacks.contains(callback)) return;

      if (_onStalledCallbacksInProgress) {
        if (_onStalledCallbacksPending == null) _onStalledCallbacksPending = new ArrayList<>();
        _onStalledCallbacksPending.add(callback);
      } else {
        _onStalledCallbacks.add(callback);
        if (_workers.count == 0 && _waiters.count > 0) {
          // At least one thread is blocking and there will be no-one to pick up our work (no
          // workers)
          if (runOnStalledCallbacks()) addWorkerIfNeeded();
        }
      }
    }
  }

  public final void removeOutOfWorkListener(OutOfWorkListener callback) {
    synchronized (_lock) {
      _onStalledCallbacks.remove(callback);
    }
  }

  /**
   * runs all callbacks with a .limitTag that matches the tag or any callbacks that matches the
   * passed in value
   */
  public final void runOutOfWorkListeners(Predicate<OutOfWorkListener> pred) {
    // TODO (OPTIMUS-17080): don't hold the lock while we call the callbacks
    synchronized (_lock) {
      // First collect callbacks and then call them
      // The reason for this 2 pass:
      //  callback can be adding more callbacks during callback
      ArrayList<OutOfWorkListener> cbs = new ArrayList<>(); // Callbacks to run
      Iterator<OutOfWorkListener> it = _onStalledCallbacks.iterator();
      while (it.hasNext()) {
        OutOfWorkListener callback = it.next();
        if (pred.test(callback)) {
          cbs.add(callback);
          it.remove(); // Note: O(n) but can be made O(1)
        }
      }
      for (OutOfWorkListener callback : cbs) {
        try {
          callback.onGraphStalled(this);
        } catch (Throwable t) {
          panic(new GraphException("onGraphStalled threw an exception", t));
        }
      }
    }
  }

  /**
   * Wait for any nodes which are currently running in cancelled CancellationScope cs to stop
   * running on this Scheduler. This is mostly useful if you have thread safety requirements which
   * require that those nodes *must* not be running, since even without this method, no further
   * nodes would start (re)running in the CancellationScope. See [WAIT_FOR_CANCELLED]
   *
   * <p>More precisely: if you cancel CancellationScope cs and then call
   * waitForCancelledNodesToStop, you can be sure once the call returns that all nodes in
   * CancellationScope cs are either: - not running on this scheduler and will never run again, OR -
   * are themselves in a call to waitForCancelledNodesToStop()
   */
  public final void waitForCancelledNodesToStop(CancellationScope cs) {
    if (!cs.isCancelled())
      throw new IllegalArgumentException(
          "Can't wait for cancelled nodes to stop in non-cancelled CS " + cs);
    // mark ourself to avoid deadlocks with other nodes also calling waitForCancelledNodesToStop
    NodeTask callerNode =
        EvaluationContext.isInitialised() ? EvaluationContext.currentNode() : null;
    if (callerNode != null) callerNode.markAsWaitingForCancelledNodesToStop();

    // created as zero length since we usually don't need to wait for any nodes
    final ArrayList<Tuple3<HasCurrentNode, OGSchedulerContext, NodeTask>> contextsToPoll =
        new ArrayList<>(0);

    // Capture any currently sync-suspended and/or running cancelled nodes.
    // Note that this is an arbitrary snapshot of state, but since no cancelled nodes will (re)start
    // running, we only
    // care about any that were already running at the point of cancellation. We might grab more
    // nodes that we need to,
    // but we can't possibly miss any here.
    NodeStackVisitor visitor =
        (HasCurrentNode frame, OGSchedulerContext owner) -> {
          NodeTask node = frame.getCurrentNodeTask();
          if (node != null
              && node.notWaitingForCancelledNodesToStop()
              && node.isCancelled()
              && node.scenarioStack().cancelScope().isSameOrTransitiveChildOf(cs)) {
            contextsToPoll.add(new Tuple3<>(frame, owner, node));
            // only the first node per stack matters, because we know any subsequent nodes must have
            // already stopped
            // before the first can (resume and then) stop, so once we've found one we can end the
            // visit
            return false;
          } else return true;
        };

    synchronized (_lock) {
      visitNodeStacks(_workers, visitor);
      visitNodeStacks(_waiters, visitor);
    }

    // Poll until the nodes stop running
    for (Tuple3<HasCurrentNode, OGSchedulerContext, NodeTask> contextAndNode : contextsToPoll)
      waitForCancelledNodeToStop(
          contextAndNode._1(), contextAndNode._2(), contextAndNode._3(), callerNode);
  }

  private void waitForCancelledNodeToStop(
      HasCurrentNode frame, OGSchedulerContext owner, NodeTask nodeToWaitFor, NodeTask callerNode) {
    // as soon as the context is done with this node, we know it won't run again so we're safe (and
    // hopefully it might already be done, so we'll only loop once).
    // Note that we check both the sync frame and the context,
    // since when the node resumes, it moves from the sync frame to the context (and the context is
    // updated before the sync frame is popped, so this is safe) see [WAIT_FOR_CANCELLED]
    while ((frame.getCurrentNodeTask() == nodeToWaitFor
            || owner.getCurrentNodeTask() == nodeToWaitFor)
        &&
        // recheck- the node could have been marked after we snapped
        nodeToWaitFor.notWaitingForCancelledNodesToStop()) {
      // check for possible deadlock
      if (frame instanceof OGSchedulerContext.SyncFrame
          && nodeToWaitFor.visitForwardChain().containsEQ(callerNode)) {
        // We're deadlocked! (OPTIMUS-43488)
        // Specifically, we're currently visiting nodes on our own thread (the first of the above
        // checks),
        // but there's a cancelled node a sync-stack away, which no other thread will take.
        // (Note that `frame` can only be our context or a sync frame; if it's our context then node
        // is our node).
        String msg =
            "Waiting for cancelled node in sync stack; this would be a deadlock. "
                + "The most likely cause is asking for a dependency-tracker update under a sync stack. "
                + "Current node: "
                + callerNode
                + "; awaited cancelled node: "
                + nodeToWaitFor;
        throw new GraphInInvalidState(msg);
      } else Thread.yield();
    }
  }

  // visits frames from each stack until visitor returns false (then continues with next frame)
  private void visitNodeStacks(Contexts contexts, NodeStackVisitor visitor) {
    contexts.foreach(
        ctx -> {
          OGSchedulerContext sc = ctx.schedulerContext;
          if (sc != null) sc.concurrentlyVisitNodesOnStack(visitor);
        });
  }

  private void generateSchedulerStatus(
      PrettyStringBuilder sb, boolean showEmptyQueues, int maxNodes) {
    sb.appendDivider();
    // vX is just a marker so that we can tell from a dump the version
    sb.append(schedulerVersion);
    sb.append(" id: ");
    sb.append(schedulerID);
    sb.append(" idealThreads: ").append(_idealCount);
    sb.append(" spinningThread: ").appendln(_spinningThreadID);
    sb.indent();

    try {
      ArrayList<OutOfWorkListener> callbacks;
      synchronized (_lock) {
        // Copy under lock!
        callbacks = new ArrayList<>(_onStalledCallbacks);
      }
      if (showEmptyQueues || !callbacks.isEmpty()) {
        int cbCount = callbacks.size();
        sb.append("onStalledCallbacks count: " + cbCount);
        if (cbCount > 0) {
          sb.startBlock();
          HashMap<String, Integer> schedulerCallBacksMap = new HashMap<>();
          for (OutOfWorkListener cb : callbacks) {
            // getClass.toString vs toString: we don't want to trust toString on callback.
            if (cb != null) {
              String classStr = cb.getClass().toString();
              int count = schedulerCallBacksMap.getOrDefault(classStr, 0) + 1;
              schedulerCallBacksMap.put(classStr, count);
            }
          }
          for (String name : schedulerCallBacksMap.keySet()) {
            sb.appendln(name + " [" + schedulerCallBacksMap.get(name) + "]");
          }
          sb.endBlock();
        } else sb.appendln("");
      }
      if (showEmptyQueues || !_queue.isEmpty()) {
        sb.append("global queue ");
        sb.appendln(_queue);
      }
      if (showEmptyQueues || !_qqueue.isEmpty()) {
        sb.append("global quick queue ");
        _qqueue.toString(sb, maxNodes);
      }

      _workers.writePrettyString(sb, showEmptyQueues, maxNodes);
      _waiters.writePrettyString(sb, showEmptyQueues, maxNodes);
      _idlePool.writePrettyString(sb, showEmptyQueues, maxNodes);
      _externallyBlocked.writePrettyString(sb, showEmptyQueues, maxNodes);

      if (!_evalAsyncTrackedNodes.isEmpty()) {
        sb.append("Node(s) tracked for completion ");
        sb.startBlock();
        _evalAsyncTrackedNodes.foreach(tracked -> tracked.task.writePrettyString(sb).endln());
        sb.endBlock();
      }

    } catch (Throwable t) {
      sb.appendln("Unable to fully expand...");
    } finally {
      sb.unIndent();
    }
  }

  private void generateWaitingChain(Context awaitingCtx, PrettyStringBuilder sb) {
    IdentityHashMap<NodeTask, Integer> syncStackHash =
        awaitingCtx.schedulerContext.dbgHashOfSyncStack();
    IdentityHashMap<NodeTask, Integer> waitChainHash = new IdentityHashMap<>();

    // We need to protect against cycles, it's possible that cycle was just completed by another
    // thread while this thread was busy creating this dump
    sb.appendDivider();
    sb.appendln("Waiting Chain...");
    sb.indent();
    writeWaitingChain(awaitingCtx.awaitedTask, sb, syncStackHash, waitChainHash);
    sb.endln();
    sb.unIndent();
    sb.appendDivider();
    sb.appendln("Waiting Context...");
    sb.indent();
    awaitingCtx.schedulerContext.writePrettyString(
        sb, true, waitChainHash, Settings.nodeLimitInSchedulerStatusMessages);
    sb.unIndent();
  }

  // this will print stall info from underlying plugin (if any) + any extra data attached to
  // NodeTask
  private void extraStallInfoForAwaitingTask(
      PrettyStringBuilder sb, NodeTask lastTask, SchedulerPlugin awaitingPlugin) {
    if (lastTask != null) {
      Option<GraphStallInfo> gsiOpt = lastTask.stallInfo();
      if (gsiOpt.isDefined()) {
        // get stall details from plugin
        GraphStallInfo gsi = gsiOpt.get();
        if (awaitingPlugin != null) {
          attachPluginWaitingMsg(sb, awaitingPlugin.getClass().toString(), gsi.message());
        } else {
          attachPluginWaitingMsg(sb, gsi.pluginType().name(), gsi.message());
        }
        // print the plugin requests stall info if any attached
        if (gsi.requestsStallInfo().isDefined()) {
          attachRequestsInfo(sb, gsi.requestsStallInfo().get());
        }
        sb.endln();
      }
      // print any extra info attached to last task
      var nodeExtraInfo = StallInfoAppender.getExtraData(lastTask);
      if (nodeExtraInfo != null) {
        GraphStallInfo graphStall = StallInfoAppender.getExtraData(lastTask).apply();
        attachPluginWaitingMsg(sb, graphStall.pluginType().name(), graphStall.message());
        if (graphStall.requestsStallInfo().isDefined()) {
          RequestsStallInfo extraStallInfo = nodeExtraInfo.apply().requestsStallInfo().get();
          attachRequestsInfo(sb, extraStallInfo);
        }
      }
    }
  }

  private void attachPluginWaitingMsg(PrettyStringBuilder sb, String pluginType, String stallMsg) {
    sb.append("Graph is waiting for plugin: ")
        .append(pluginType)
        .append(" (")
        .append(stallMsg)
        .append(")")
        .endln();
  }

  private void attachRequestsInfo(PrettyStringBuilder sb, RequestsStallInfo reqStallInfo) {
    sb.indent();
    sb.append("Pending ").append(reqStallInfo.reqCount()).append(" request(s).").endln();
    sb.indent();
    Collection<String> requests = CollectionConverters.asJavaCollection(reqStallInfo.req());
    int requestNum = 1;
    for (String request : requests) {
      sb.append("Request ").append(requestNum++).append(": ").append(request).endln();
    }
    sb.unIndent();
    sb.unIndent();
  }

  @Override
  public final String getAllWaitStatus(
      PrettyStringBuilder sb,
      Context[] tempWaitQueue,
      Context[] tempWorkQueue,
      Context[] externallyBlockedQueue,
      ThreadDumper.ThreadInfos threadInfos) {
    generateSchedulerStatus(sb, false, Settings.nodeLimitInSchedulerStatusMessages);
    sb.indent();
    for (Context waitContext : tempWaitQueue) {
      sb.appendDivider();
      sb.appendln("Context wait status for thread: " + waitContext.thread.getName());
      sb.endln();
      sb.indent();
      generateWaitingStatus(sb, waitContext);
      sb.appendDivider();
      sb.appendln("Stacktrace...");
      sb.indent();
      ThreadDumper.dumpThreadById(sb, threadInfos, waitContext.thread.threadId());
      sb.unIndent();
      sb.unIndent();
    }

    for (Context workContext : tempWorkQueue) {
      sb.appendDivider();
      sb.appendln("Stacktrace for working thread: " + workContext.thread.getName() + "...");
      sb.indent();
      ThreadDumper.dumpThreadById(sb, threadInfos, workContext.thread.threadId());
      sb.unIndent();
    }

    for (Context blockedContext : externallyBlockedQueue) {
      sb.appendDivider();
      sb.appendln(
          "Stacktrace for externally blocked thread: " + blockedContext.thread.getName() + "...");
      sb.indent();
      ThreadDumper.dumpThreadById(sb, threadInfos, blockedContext.thread.threadId());
      sb.unIndent();
    }
    sb.unIndent();
    return sb.toString();
  }

  @Override
  public final String getAllWaitStatus(PrettyStringBuilder sb, Context[] tempWaitQueue) {
    generateSchedulerStatus(sb, false, Settings.nodeLimitInSchedulerStatusMessages);
    sb.indent();
    for (Context waitContext : tempWaitQueue) {
      sb.appendDivider();
      sb.appendln("Context wait status for thread: " + waitContext.thread.getName());
      sb.endln();
      sb.indent();
      generateWaitingStatus(sb, waitContext);
    }
    sb.unIndent();
    return sb.toString();
  }

  // Snapshot the wait/work queues in order to produce thread dumps for their threads
  @Override
  public final ContextSnapshot getContexts() {
    Context[] tempWaitQueue, tempWorkQueue, tempExternallyBlockedQueue;
    synchronized (_lock) {
      tempWaitQueue = Arrays.copyOf(_waiters.getUnderlyingList(), _waiters.count);
      tempWorkQueue = Arrays.copyOf(_workers.getUnderlyingList(), _workers.count);
      tempExternallyBlockedQueue =
          Arrays.copyOf(_externallyBlocked.getUnderlyingList(), _externallyBlocked.count);
    }
    return new ContextSnapshot(tempWaitQueue, tempWorkQueue, tempExternallyBlockedQueue);
  }

  final void assertNotRegistered(Context context) {
    synchronized (_lock) {
      if (_waiters.contains(context) || _workers.contains(context) || _idlePool.contains(context))
        throw new GraphInInvalidState("Context " + context + " was still in use: " + this);
    }
  }

  @Override
  public int getSchedulerId() {
    return schedulerID;
  }

  private void generateWaitingStatus(PrettyStringBuilder sb, Context awaitingCtx) {
    NodeTask awaitedTask = awaitingCtx.awaitedTask;
    if (awaitedTask == null) return;

    NodeTask lastTask = lastTaskInWaitingChain(awaitingCtx);
    SchedulerPlugin awaitingPlugin = lastTask == null ? null : lastTask.getPlugin();
    var nodeExtraInfo = StallInfoAppender.getExtraData(lastTask);

    if (awaitingPlugin != null || nodeExtraInfo != null) {
      extraStallInfoForAwaitingTask(sb, lastTask, awaitingPlugin);
    } else {
      sb.appendln("Graph is stalling for unknown reason: Waiting for " + awaitedTask);
    }
    generateWaitingChain(awaitingCtx, sb);
  }

  /* You should not rely on this! For (weird) tests only */
  public final int dbgIdleThreadCount() {
    synchronized (_lock) {
      return _idlePool.count;
    }
  }

  int dbgOnStallCallBacksCount() {
    synchronized (_lock) {
      return _onStalledCallbacks.size();
    }
  }

  int dbgQueuedWaitingAndWorkingCount() {
    synchronized (_lock) {
      return _qqueue.size() + _queue.size() + _waiters.count + _workers.count;
    }
  }

  public boolean schedulerInBadState() {
    return dbgQueuedWaitingAndWorkingCount() != 0 || dbgOnStallCallBacksCount() != 0;
  }

  @Override
  public String toString() {
    PrettyStringBuilder sb = new PrettyStringBuilder();
    generateSchedulerStatus(sb, true, Integer.MAX_VALUE);
    return sb.toString();
  }

  /**
   * A simple wrapper meant to hold information about scheduler when stalling. All `endOfChain*`
   * parameters are taken from a sample node task that is an unfinished computation that `task`
   * depends on. We don't hold on to the randomly selected end of chain node task itself because it
   * might hold on to arbitrary memory.
   */
  public static final class AwaitedTasks {

    private final NodeTask task;
    private final NodeTaskInfo endOfChainTaskInfo;
    private final SchedulerPlugin endOfChainPlugin;
    private final PluginType endOfChainPluginType;

    // TODO(OPTIMUS-68964) Convert to Record once Scala 2.13 is available
    public AwaitedTasks(
        NodeTask task,
        NodeTaskInfo endOfChainTaskInfo,
        SchedulerPlugin endOfChainPlugin,
        PluginType endOfChainPluginType) {
      this.task = task;
      this.endOfChainTaskInfo = endOfChainTaskInfo;
      this.endOfChainPlugin = endOfChainPlugin;
      this.endOfChainPluginType = endOfChainPluginType;
    }

    public NodeTask task() {
      return task;
    }

    public NodeTaskInfo endOfChainTaskInfo() {
      return endOfChainTaskInfo;
    }

    public SchedulerPlugin endOfChainPlugin() {
      return endOfChainPlugin;
    }

    public PluginType endOfChainPluginType() {
      return endOfChainPluginType;
    }

    public int hashCode() {
      return Objects.hash(task, endOfChainTaskInfo, endOfChainPlugin, endOfChainPluginType);
    }

    public boolean equals(Object o) {
      if (this == o) return true;
      else if (!(o instanceof AwaitedTasks that)) return false;
      else {
        return Objects.equals(task, that.task)
            && Objects.equals(endOfChainTaskInfo, that.endOfChainTaskInfo)
            && Objects.equals(endOfChainPlugin, that.endOfChainPlugin)
            && Objects.equals(endOfChainPluginType, that.endOfChainPluginType);
      }
    }
  }
}
