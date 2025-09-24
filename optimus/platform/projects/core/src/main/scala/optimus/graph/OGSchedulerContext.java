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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import optimus.graph.diagnostics.InfoDumper$;
import optimus.graph.diagnostics.ProfiledEvent;
import optimus.graph.diagnostics.rtverifier.RTVerifierNodeRerunner$;
import optimus.platform.EvaluationQueue;
import optimus.platform.ScenarioStack;
import optimus.platform.util.PrettyStringBuilder;
import scala.Function0;

public class OGSchedulerContext extends EvaluationQueue
    implements HasCurrentNode, AwaitableContext {
  private static final VarHandle currentNodeHandle;
  private static final VarHandle queue_h;
  private static final VarHandle base_h;

  static {
    try {
      var lookup = MethodHandles.lookup();
      currentNodeHandle =
          lookup.findVarHandle(OGSchedulerContext.class, "_currentNode", NodeTask.class);
      queue_h = MethodHandles.arrayElementVarHandle(NodeTask[].class);
      base_h = lookup.findVarHandle(WorkQueue.class, "_base", int.class);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new GraphException("Unable to get VarHandle", e);
    }
  }

  private static final ThreadLocal<OGSchedulerContext> _current = new ThreadLocal<>();
  private static final AtomicInteger ecCounter = new AtomicInteger();
  // marks the root of the OGTrace call graph, RuntimeEnvironment$NullNode
  public static final int NODE_ID_ROOT = 1;
  // skipping NODE_ID_INVALID and NODE_ID_ROOT
  public static final int NODE_ID_FIRST_VALID = NODE_ID_ROOT + 1;
  private static final AtomicInteger nodeIdCounter = new AtomicInteger(NODE_ID_FIRST_VALID);

  private static final int NODE_ID_STEP = 1000;

  static int nextNodeId() {
    int newNodeID;
    OGSchedulerContext ec = current();
    if (ec == null) newNodeID = nodeIdCounter.getAndIncrement();
    else if (ec.nodeId == ec.nodeIdWatermark) {
      ec.nodeId = nodeIdCounter.getAndAdd(NODE_ID_STEP);
      ec.nodeIdWatermark = ec.nodeId + NODE_ID_STEP - 1;
      newNodeID = ec.nodeId;
    } else {
      ec.nodeId++;
      newNodeID = ec.nodeId;
    }
    if (Settings.allowTestableClock) OGTrace.publishEvent(newNodeID, ProfiledEvent.NODE_CREATED);
    return newNodeID;
  }

  public final int id = ecCounter.incrementAndGet();
  private final WorkQueue localQ;
  private final SyncStack syncStack = new SyncStack();
  private final OGScheduler _scheduler;
  private final OGScheduler.Context _context; // Maintained by Scheduler
  private NodeTask _currentNode;
  private int safeStackDepth; // Allow for local safe stack overflow avoidance

  /** Profiler Data. Internal tables for profiler */
  OGLocalTables prfCtx;

  int syncStackCount() {
    return syncStack.count;
  }

  void appendSyncStackNodes(ArrayList<NodeTask> list) {
    syncStack.foreach(list::add);
  }

  void foreachSyncStackNode(Consumer<NodeTask> consumer) {
    syncStack.foreach(consumer);
  }

  /**
   * Visits every suspended frame on the sync stack plus the currently running frame. Note that
   * there is NO synchronization and this is NOT a snapshot, so you may see inconsistent state
   * including null nodes. This method is only useful for discovering nodes that were
   * sync-suspended/running on this thread at some point between when you called this method and
   * when this method finished. The only nodes you are guaranteed to see are those which were
   * sync-suspended/running before your call and remained in that state until after your call.
   */
  void concurrentlyVisitNodesOnStack(NodeStackVisitor visitor) {
    // if visitor didn't abort during walking stack, also visit ourselves (to get currently running
    // node)
    if (syncStack.concurrentlyVisitNodesOnStack(visitor)) visitor.visit(this, this);
  }

  OGSchedulerContext(NodeTask envNode, OGScheduler scheduler, OGScheduler.Context context) {
    // envNode == null only for helper threads and in that case the entry into scheduler
    // will not be runAndWait but rather just run.
    // In order for the causality assignment and sync/async detection to work correctly we add
    // a fake sentinel to syncStack. Note that we use ScenarioStack.nullNode rather than
    // NullNode because we have no particular env for helper threads
    if (envNode == null) syncStack.push(ScenarioStack.nullNode());
    _currentNode = envNode;
    _scheduler = scheduler;
    _context = context;
    localQ = context.queue;
    localQ.schedulerContext = this;
    context.schedulerContext = this;
  }

  /** Helper to define exception once */
  public final void assertCorrectThread() {
    if (_context.thread != Thread.currentThread())
      throw new GraphInInvalidState("EvaluationContexts cannot migrate from thread to thread");
  }

  /** Return current SchedulerContext. The goal is to make it internal */
  public static OGSchedulerContext current() {
    var thread = Thread.currentThread();
    if (thread instanceof OGScheduler.OGThread)
      return ((OGScheduler.OGThread) (thread)).logicalThread.ec;
    return _current.get();
  }

  /**
   * DO NOT USE! Use optimus.graph.OGSchedulerContext#current() instead.
   *
   * <p>This method is an exact copy of optimus.graph.OGSchedulerContext#current() and it should
   * NEVER be used outside of optimus.graph.InstrumentationSupport. It allows our instrumentation to
   * call Thread.currentThread() without being detected while identifying other code that relies on
   * Thread.currentThread().
   */
  public static OGSchedulerContext _TRACESUPPORT_unsafe_current() {
    var thread = Thread.currentThread();
    if (thread instanceof OGScheduler.OGThread)
      return ((OGScheduler.OGThread) (thread)).logicalThread.ec;
    return _current.get();
  }

  public static boolean onOgThread() {
    Thread thread = Thread.currentThread();
    return (thread instanceof OGScheduler.OGThread);
  }

  /** Sets current SchedulerContext. */
  public static void setCurrent(OGSchedulerContext newContext) {
    var previous = _current.get();
    if (previous != null) {
      if (Settings.schedulerAsserts && newContext == null)
        previous._scheduler.assertNotRegistered(previous._context);
    }
    if (newContext != null) {
      if (previous != null && previous.prfCtx != null) {
        newContext.prfCtx = previous.prfCtx; // Simply transfer the value (it's always per thread)
      } else newContext.prfCtx = OGLocalTables.acquireProfilingContext(true);
    } else if (previous != null) {
      OGLocalTables.releaseProfilingContext(previous); // Full cleanup
    }
    _current.set(newContext);
  }

  /** Return currently running nodeTask or the default nullTask */
  public final NodeTask getCurrentNodeTask() {
    return _currentNode;
  }

  /** Return scenarioStack of currently running nodeTask or the default nullTask */
  public final ScenarioStack scenarioStack() {
    return _currentNode.scenarioStack();
  }

  /** Profiler data */
  private int nodeId;

  private int nodeIdWatermark;

  /**
   * Entry for every node 'currently executing' on the stack. 'currently executing' here means on
   * the thread stack, they are eagerly allocated and lazily disposed, to reduce memory allocation.
   *
   * <p>Used to recover 'node stack' across multiple threads Only task is updated and no allocation
   * is done most of the time, The cost is just extra memory per sync entry into the graph. Which
   * should not be big as sync node stacks hopefully are very small
   */
  public final class SyncFrame extends NodeAwaiter implements HasCurrentNode, DebugWaitChainItem {
    /** the position of this SyncFrame in the SyncStack frames array */
    int index;
    /**
     * The NodeTask which we are synchronously waiting on (i.e. the one which caused this sync
     * stack), or null if this is the entry point of a graph helper thread which isn't waiting for
     * anything
     */
    NodeTask ntsk;

    @Override
    public void onChildCompleted(EvaluationQueue eq, NodeTask child) {
      // Ignore the case when we completed on the same thread, we can't be blocking ourselves
      if (eq == OGSchedulerContext.this) return;
      // The original OGSchedulerContext might be blocked and we need to unblock it
      _scheduler.awakeOnComplete(child, _context);
    }

    @Override
    public NodeTask awaiter() {
      return ntsk;
    }

    @Override
    public NodeTask getCurrentNodeTask() {
      return ntsk;
    }

    @Override
    public String toString() {
      return "sync:" + (ntsk != null ? ntsk.toString() : "empty");
    }

    @Override
    public void addToWaitChain(ArrayList<DebugWaitChainItem> appendTo, boolean includeProxies) {
      appendTo.add(this);
      ntsk.addToWaitChain(appendTo, includeProxies);
    }

    public Thread thread() {
      return _context.thread;
    }
  }

  /**
   * Used to optionally keep sync stack in EC null are inserted on transitions from sync -> async.
   */
  final class SyncStack {
    private final int INITIAL_STACK_SIZE = 32;
    private SyncFrame[] frames = new SyncFrame[INITIAL_STACK_SIZE];
    private int count = 0;

    boolean isEmpty() {
      return count == 0;
    }

    SyncFrame push(NodeTask ntsk) {
      if (count >= frames.length) frames = Arrays.copyOf(frames, count * 2);
      SyncFrame frame = frames[count];
      if (frame == null) {
        frame = new SyncFrame();
        frame.index = count;
        frames[count] = frame;
      }
      frame.ntsk = ntsk;

      count++;
      return frame;
    }

    void pop() {
      SyncFrame frame = frames[--count];
      frame.ntsk = null;
    }

    @Override
    public String toString() {
      return toString(new PrettyStringBuilder(), null).toString();
    }

    PrettyStringBuilder toString(
        PrettyStringBuilder sb, IdentityHashMap<NodeTask, Integer> waitChainHash) {
      for (int i = 0; i < count; i++) {
        sb.append(" > ");
        sb.append(frames[i].ntsk);
        if (waitChainHash != null) {
          Integer waitChainLoc = waitChainHash.get(frames[i].ntsk);
          if (waitChainLoc != null) sb.append(" [WC:" + waitChainLoc + " (sync stack deadlock?)]");
        }
      }
      return sb;
    }

    void foreach(Consumer<NodeTask> consumer) {
      for (int i = 0; i < count; i++) {
        SyncFrame qi = frames[i];
        if (qi != null) {
          NodeTask n = qi.ntsk;
          if (n != null) consumer.accept(n);
        }
      }
    }

    // Inconsistent walk of the sync stack. If visitor returns false then stops walk and returns
    // false, else returns true.
    boolean concurrentlyVisitNodesOnStack(NodeStackVisitor visitor) {
      for (int i = 0; i < count; i++) {
        SyncFrame f = frames[i];
        if (f != null) if (!visitor.visit(f, OGSchedulerContext.this)) return false;
      }

      return true;
    }
  }

  final void localCleanup(EvaluationQueue incompleteQueue) {
    localQ.localCleanup(incompleteQueue);
  }

  /**
   * Work Stealing Queue LIFO (also viewed as Circular Work-Stealing Deque) Local thread does
   * push/pop from the top and other workers steal from the base. Reasoning is that the oldest
   * posted task (closer to base) probably will fan out to most work and last posted task (closest
   * to top) was just generated and it's data is hotter in cache
   *
   * <p>See:
   *
   * <oL>
   *   <li>"The Art of Multiprocessing Programming" (Herlihy et. al.)
   *   <li><a
   *       href="https://www.researchgate.net/publication/221643488_Idempotent_Work_Stealing">"Idempotent
   *       work stealing" (Michael et. al.)</a>
   *   <li><a href="http://www.cs.tau.ac.il/~mad/publications/asplos2014-ffwsq.pdf">Fence-Free Work
   *       Stealing on Bounded TSO Processors</a>
   *   <li>Doug Lea's implementation in JDK with comments or the latest at
   *       jsr166/src/main/java/util/concurrent/ForkJoinPool.java
   *   <li>WorkStealingQueue .NET Core /System.Private.CoreLib/shared/System/Threading/ThreadPool.cs
   * </oL>
   *
   * <p>Notes: All work that assumes C++ can't be used directly because in GC runtimes we need to
   * clear out references to avoid leaks. Event [1] that's kind of in java is a not "production
   * ready". Hence look at [4] & [5]
   *
   * <p>[4] Has known issues to us a. It doesn't seem to handle index overflows correctly like [5]
   * does and it's possible to just never hit that b. Our previous implementation that was more
   * close following [4] Ran into ABA style problem. Unlike java's FJP our task are often
   * continuously re-enqueued. e.g. FSM based nodes that call continueWith(this) are by default
   * re-enqueued. So in [4] base counter would be allowed to overflow _top and this would result in
   * a task getting lost in the queue.
   *
   * <p>[4] and [5] Are constrained by the fact that each task can be attempted to be executed only
   * once. Where is in our case we must have an additional guard on a task (because multiple threads
   * can enqueue same task multiple times) The problem is not unique to our graph system. From [2]
   * ... these semantics are too restrictive for a wide range of applications deal-ing with
   * irregular computation patterns. Sample domains include: parallel garbage collection, fixed
   * point computations in program analysis, constraint solver
   *
   * <p>_base can be off by +1. Specifically the following are then valid states: a. _top == _base
   * and q is empty b. _top == _base - 1 and q is empty (_base overrun by +1) c. _top == _base and q
   * has 1 element in _top - 1 (_base overrun by +1) The way we get into _base overrun by + 1 is by
   * racing localPop and trySteal both reading a task and trySteal succeeding and incrementing _base
   * and localPop is not even verifying and decrementing _top
   */
  static class WorkQueue {
    OGSchedulerContext schedulerContext;
    private final int INITIAL_Q_SIZE = 64;
    private NodeTask[] _q = new NodeTask[INITIAL_Q_SIZE];
    private volatile int _base; // other workers steal from this end
    private int _top; // index of next slot for push and updated by owner (aka local thread) only

    /** Unstable value! */
    final boolean isEmpty() {
      return _top <= _base;
    }

    /** Unstable value! */
    final int size() {
      return _top - _base;
    }

    final int getTop() {
      return _top;
    }

    @SuppressWarnings("unused")
    final NodeTask dbgPeekTop() {
      var q = _q;
      return q[(_top - 1) & (q.length - 1)];
    }

    final NodeTask tryToSteal() {
      synchronized (this) { // Needed to avoid ABA problem
        NodeTask[] q = _q;
        int mask = q.length - 1;
        int base = _base;
        int i = base & mask;
        NodeTask r = (NodeTask) queue_h.getVolatile(q, i);
        if (r != null && queue_h.compareAndSet(q, i, r, null)) {
          base_h.setOpaque(this, base + 1); // We don't need a full barrier here
          return r;
        }
      }
      return null;
    }

    /**
     * TODO(OPTIMUS-28068): this implementation required CAS on every pop, replace with algorithm
     * that doesn't need CAS most of the time It's possible because we have a CAS guarding running a
     * node, so allowing 2 threads 'get' the same node is OK
     */
    final NodeTask localPop(int topLimit) {
      int top = _top - 1;
      if (topLimit > top) return null;
      if (top < _base) return null; // Empty
      NodeTask[] q = _q;
      int mask = q.length - 1;
      var i = top & mask;
      NodeTask r = (NodeTask) queue_h.get(q, i);
      if (r != null && queue_h.compareAndSet(q, i, r, null)) {
        _top = top;
      }
      return r;
    }

    final void tryLocalPop(NodeTask taskToRemove) {
      int top = _top - 1;
      if (top < _base) return; // Empty
      NodeTask[] q = _q;
      int mask = q.length - 1;
      int i = top & mask;
      NodeTask r = (NodeTask) queue_h.get(q, i);
      if (r == taskToRemove && queue_h.compareAndSet(q, i, r, null)) {
        _top = top;
      }
    }

    final void localPush(NodeTask task) {
      NodeTask[] q = _q;
      int top = _top;
      int base = _base;
      int mask = q.length - 1;
      // _base could have been off by +1 (so we grow the queue potentially earlier by 1
      if ((top - base) >= mask) {
        growArray(base - 1); // and try to copy 1 more in growArray
        q = _q;
        mask = q.length - 1;
      }

      // Assert? if(!U.compareAndSwapObject(q, ((top & mask) << ASHIFT) + ABASE, null, task))
      queue_h.setOpaque(q, top & mask, task);
      _top = top + 1; // In non TSO world change to putOrderedInt() because it's read by trySteal

      if (_top == Integer.MAX_VALUE) resetBounds(mask);
    }

    private void resetBounds(int mask) {
      // We don't need to move any elements, base and top will continue to point to the same slots
      // in q
      // We still have to make sure that top > base when we mask, but this just works because top at
      // this point
      // is MaxValue which is all (but the top one) bits set, so masking will have all bits set as
      // well
      synchronized (this) {
        _top = _top & mask;
        _base = _base & mask;
      }
    }

    /**
     * Doubles the capacity. _base can move. But we cannot move _top because of causality mark We
     * could consider finding and updating all causality marks
     */
    final void growArray(int base) {
      // Relying on the fact that tasks can be on more than one queue at a time, we simply copy the
      // existing tasks over.
      // Meanwhile some other thread can be stealing them from the original queue
      NodeTask[] oq = _q;
      int omask = oq.length - 1;
      NodeTask[] nq = new NodeTask[oq.length << 1];
      int nmask = nq.length - 1;
      int nbase = _top;
      boolean okToDropDoneTasks =
          schedulerContext == null || schedulerContext.currentCausalityID == 0;
      for (int i = nbase - 1; i >= base; i--) {
        // Read the old value (trySteal can set it to null while we are copying)
        NodeTask nt = oq[i & omask];
        if (nt != null) {
          if (nt.isDoneOrNotRunnable()) {
            // Ideally we just drop the task (not copying it over)
            // However when the thread has sync stack its _top of the local queue was queried as
            // saved as queueMarker
            // (see drainQueue). Therefore we can't just drop it, but in order to reduce memory
            // pressure we can
            // copy nullTask into it
            if (!okToDropDoneTasks) nq[(--nbase) & nmask] = NodeTask.nullTask;
          } else nq[(--nbase) & nmask] = nt;
        }
      }
      synchronized (this) {
        _base = nbase;
        _q = nq;
      }
    }

    /**
     * Cleanup local queue from all completed nodes to reduce memory pressure (a.k.a. leaks) There
     * is a sequence of calls of 'enqueue' and 'get' (technically valid, just inefficient and should
     * probably be profiled for)
     */
    final void localCleanup(EvaluationQueue incompleteQueue) {
      NodeTask task;
      while ((task = localPop(0)) != null) {
        if (!task.isDoneOrNotRunnable()) { // [CAN_DROP]
          if (incompleteQueue != null) incompleteQueue.enqueue(null, task);
          else {
            // We don't want to loose this unfinished item, but perhaps this is safe to drop them
            // here?
            localPush(task);
            break;
          }
        }
      }
    }

    /**
     * Both WorkQueues are 'locked': qqueue is under lock and `this` aka waitQueue on the current
     * thread
     */
    public void drainTo(WorkQueue qqueue) {
      NodeTask task;
      while ((task = tryToSteal()) != null) {
        if (!task.isDoneOrNotRunnable()) { // [CAN_DROP]
          qqueue.localPush(task);
        }
      }
    }

    /**
     * scanToFindWork is called ONLY on the _qqueue in OGScheduler and under 'owner' lock
     * scanToFindWork is called ONLY from wait. This is SLOW, it's a desperate attempt and should be
     * called as a last resort.
     *
     * <p>If thread has no sync stacks (aka causality of 0) it can take the next item on the queue
     * otherwise it needs to scan the queue to find the task that it can prove (OGScheduler.canTake)
     * belongs to it
     */
    final NodeTask scanToFindWork(int threadID, int causalityID) {
      if (causalityID == 0) return localPop(0);

      NodeTask[] q = _q;
      int base = _base;
      int top = _top;
      int mask = q.length - 1;

      // Before taking a lock we will first scan the queue
      // to make sure there is a reason to take that lock
      int scanTop = top - 1;
      while (scanTop >= base) {
        NodeTask t = q[scanTop & mask];
        if (t == null)
          return null; // All tasks are gone... and there is nothing to take from this queue
        if (t.isDoneOrNotRunnable()) {
          base = removeAt(scanTop);
        } else if (OGScheduler.canTake(t, threadID, causalityID)) {
          removeAt(scanTop);
          return t;
        } else {
          --scanTop;
        }
      }
      return null;
    }

    // Assumption is that this code is not called that often
    private int removeAt(int index) {
      int base;
      NodeTask[] q = _q; // _q is not changing (we own the lock and not resizing.
      int mask = q.length - 1;
      synchronized (this) {
        base = _base; // Re-read base
        // We have to verify that base is not moved passed our item
        // It could have been moved by a trySteal()
        if (index >= base) {
          q[index & mask] = q[base & mask];
          q[base & mask] = null;
          if (base < _top) {
            base++;
            base_h.setOpaque(this, base); // We don't need a full barrier here
          }
        }
      }
      return base;
    }

    /**
     * Iterates over the tasks in the list, not performant and not thread safe, debugging/tracing
     * only
     */
    void foreach(Consumer<NodeTask> consumer) {
      NodeTask[] q = _q;
      int base = _base;
      int top = _top;
      int mask = q.length - 1;
      if (base < top) {
        for (int i = base; i < top; i++) {
          NodeTask ntsk = q[i & mask];
          if (ntsk != null) consumer.accept(ntsk);
        }
      }
    }

    void appendToList(ArrayList<NodeTask> list) {
      foreach(list::add);
    }

    final void toString(PrettyStringBuilder sb, int nodeLimit) {
      NodeTask[] q = _q;
      int base = _base;
      int top = _top;
      int mask = q.length - 1;
      if (base >= top) {
        for (NodeTask ntsk : q) {
          if (ntsk != null) {
            sb.appendln("[] with: " + ntsk);
          }
        }
        sb.appendln("[] ");
      } else {
        sb.startBlock();
        int count = 0;
        for (int i = base; i < top; i++) {
          NodeTask t = q[i & mask];
          if (t != null) {
            if (count < nodeLimit) {
              // force prettyName parameters (ignore the ones in the PrettyStringBuilder supplied)
              boolean appendedLine =
                  sb.appendlnAggregate(t.toPrettyName(false, false, true, false, false, false));
              if (appendedLine) count++;
            } else {
              count++;
            }
          }
        }
        sb.appendlnAggregateEnd();
        if (count > nodeLimit) sb.append("[and ").append(count - nodeLimit).appendln(" more]");
        sb.endBlock();
      }
    }

    @Override
    public String toString() {
      PrettyStringBuilder sb = new PrettyStringBuilder();
      toString(sb, Integer.MAX_VALUE);
      return sb.toString();
    }
  }

  /**
   * Process all available tasks on the local queue, up to marker
   *
   * @param queueMarker top of the queue allowed to be picked up
   */
  final void drainQueue(NodeTask startWithTask, int queueMarker) {
    if (Settings.schedulerAsserts) assertCorrectThread();
    OGSchedulerTimes.schedulerStallingEnd();
    NodeTask ntsk = startWithTask;
    while (ntsk != null) {
      run(ntsk);
      ntsk = localQ.localPop(queueMarker);
    }
  }

  /**
   * The currentCausalityID is incremented when pushing a new SyncFrame where it is not safe to run
   * nodes from previous SyncFrames. When we are doing runAndWait for a SyncFrame with a causalityID
   * of N, we will not start running any NodeTasks with a causalityID of less than N.
   */
  private int currentCausalityID = 0;
  /**
   * Marks the earliest task that it's safe to run from the local work queue. This is updated to the
   * current queue position whenever we push a new SyncFrame where it is not safe to run nodes from
   * previous SyncFrames
   */
  private int currentLocalQueueMarker = 0;

  final AwaitStackManagement awaitMgt =
      (DiagnosticSettings.awaitStacks && AsyncProfilerIntegration.ensureLoadedIfEnabled())
          ? new AwaitStackManagement(this)
          : null;

  final boolean awaitStacks = Objects.nonNull(awaitMgt);

  @Override
  public final void maybeSaveAwaitStack(NodeTask ntsk) {
    if (awaitStacks) awaitMgt.maybeSaveAwaitStack(ntsk);
  }

  /**
   * Wait for the node to be completed
   *
   * <p>Node must be already attached to scenarioStack
   *
   * <p>You often have to call combineInfo() immediately after this call
   */
  public final void runAndWait(NodeTask ntsk) {
    NodeTask prevNode = _currentNode;
    OGTrace.enqueue(prevNode, ntsk, true);

    /* It's not wrong for limitThreads, it's just not likely to be needed
     * Specifically get() functions that call into runAndWait() specifically do NOT enqueue
     * If loom is to implement an optimization that $queued immediately followed by toValue
     * this is still a benefit but smaller.
     * The other optimization might be to remove all completed nodes on exit from runAndWait()
     * needs to respect causality in this case....
     * */
    if (!Settings.limitThreads) localQ.tryLocalPop(ntsk);

    if (!ntsk.isDoneOrInvalidated()) {
      if (Settings.schedulerAsserts && ntsk.scenarioStack() == null)
        throw new GraphInInvalidState();
      prevNode.setWaitingOn(ntsk);

      int previousCausalityID = currentCausalityID;
      int previousLocalQueueMarker = currentLocalQueueMarker;

      // Try finally here are guarding against code throwing Error(s)
      // Exceptions are already dealt with in the run()
      // Nodes are all broken at this point, but the state of evaluation context is somewhat stable
      SyncFrame frame = null;
      try {
        if (syncStack.isEmpty()) _scheduler.enterGraph(_context, false);
        frame = syncStack.push(prevNode);

        // Increment currentCausalityID and update localQueueMarker if it would not be safe to take
        // on work from the
        // previous SyncFrame while waiting on this one. These unsafe cases are:
        // - if the node can have more than one waiter, taking on work from previous causalityID
        // levels could result in
        //   deadlock due to waiting for a node which is already executing further down this
        // (thread) stack
        // - if we have too many SyncFrames, we could end up running out of (thread) stack space
        if (!prevNode.executionInfo().atMostOneWaiter()
            || syncStack.count > Settings.syncStackSafetyLimit) {
          currentCausalityID += 1;
          currentLocalQueueMarker = localQ.getTop();
        }

        if (Settings.syncStacksDetectionEnabled)
          OGSchedulerLostConcurrency.checkSyncStacks(ntsk, currentCausalityID, syncStack);

        // Some other thread may complete our task, in which case we'll need to be notified
        // If we lose the race and won't be able to subscribe it means the node is done, and we'll
        // check again
        // The reason this call is before run, is so that we can always get a reliable node stack
        // it would be a drop faster to make this optional
        ntsk.tryAddToWaiterList(frame);

        // Below is where we actually do run ntsk.
        drainQueue(ntsk, currentLocalQueueMarker);

        // Note: currentCausalityID == 0 implies thread can take on any new work.
        // This is the Wait part of the runAndWait()
        while (!ntsk.isDoneEx()) {
          NodeTask newTask = _scheduler.wait(ntsk, _context, currentCausalityID);
          drainQueue(newTask, currentLocalQueueMarker);
        }
      } catch (Throwable ex) {
        OGScheduler.log.error("Caught an error in runAndWait, re-throwing...", ex);
        throw ex;
      } finally {
        // Restore state before this frame
        currentCausalityID = previousCausalityID;
        currentLocalQueueMarker = previousLocalQueueMarker;
        prevNode.setWaitingOn(null);
        // Sync case needs to restore the "current" node just like a direct return.
        // Important that we set _currentNode before we pop - see [WAIT_FOR_CANCELLED]
        currentNodeHandle.setOpaque(this, prevNode);
        if (frame != null) syncStack.pop();
        if (syncStack.isEmpty()) {
          _scheduler.exitGraph(_context, false);
        } else {
          OGTrace.observer.startAfterSyncStack(prfCtx, prevNode);
        }
      }
    }
  }

  public <T> T doBlockingAction(Function0<T> func) {
    return ((OGScheduler) scheduler()).doBlockingAction(_context, func);
  }

  /** Used in debugging code only to report potential scheduling issues */
  final IdentityHashMap<NodeTask, Integer> dbgHashOfSyncStack() {
    IdentityHashMap<NodeTask, Integer> hash = new IdentityHashMap<>();
    for (int i = 0; i < syncStack.count; i++) {
      SyncFrame sframe = syncStack.frames[i];
      hash.put(sframe.ntsk, i);
    }
    return hash;
  }

  private int taskCounter = 0;

  public int getTaskCounter() {
    return taskCounter;
  }

  /**
   * Runs given task Notes: Same task can be found on multiple queues (consider 2 nodes finding and
   * reusing the same node) Therefore the first thing run() does it CASes the not-runnable flag into
   * _state of the NodeTask If it succeeds in transitioning from runnable to non-runnable run()
   * executes the NodeTask.run() Corollary: [CAN_DROP] If we EVER see a task on a queue which is not
   * runnable we can safely ignore it or drop it. Because we could have attempted to run it and we
   * would just fail to transition from non-runnable to runnable Corollary: [MUST_ENQUEUE] If ever
   * transition from non-runnable to runnable we must re-enqueue the task, even if we just enqueued
   * it in a statement above
   */
  private void run(NodeTask ntsk) {
    int prevState = ntsk.tryRun();
    if ((prevState & NodeTask.STATE_NOT_RUNNABLE) == 0) {
      OGTrace.start(prfCtx, ntsk, NodeTask.isNew(prevState));

      // Increment a counter every time we run a task so we can quickly check if anything has moved
      // on this context
      ++taskCounter;

      // Note on setting causalityID. We don't set on the node that was adopted!
      // Here is the problem we avoid
      // 1. Node [A] gets adapted and used to get cid = [1] (it's marked as SR...)
      // 2. New sync entry -> enqueues [A] but [A] can't be process it's already R
      // 3. we block on [A] with causality [2]
      // 4. someone schedules (but not completes!) [A] back on to global queue (with cid = [1])
      // .... We effectively deadlock!
      NodeTaskInfo info = ntsk.executionInfo();
      try {
        _currentNode = ntsk; // Keep track of currently executing node
        ntsk.setCausality(currentCausalityID, _context.threadID); // Keep causality up to date

        // if task was started on a different scheduler, replace it with current one (so task will
        // be enqueued there)
        if (Settings.keepTaskSchedulerAffinity
            && ntsk.scenarioStack().siParams().scheduler() != _scheduler.weakView)
          ntsk.replace(ntsk.scenarioStack().withScheduler(_scheduler.weakView));

        // only audit nodes with no plugin for now
        if (Settings.auditing && NodeTask.isNew(prevState) && !info.shouldLookupPlugin()) {
          // this is the first time we've seen this node, let the auditor have a stab at it
          if (AuditTrace.visitBeforeRun(ntsk, this)) {
            // the auditor has taken over this node, it will reschedule it when ready
            return;
          }
        }

        ScenarioStack ss = ntsk.scenarioStack();
        CancellationScope cs = ss.cancelScope();
        if (cs.isCancelled()) {
          ntsk.cancelInsteadOfRun(this);
        } else if (info.hasNoPlugAndNotST()) {
          // A node might pretend  be adapted...
          if (ntsk.hasPluginType()) OGTrace.adapted(this, ntsk);

          if (DiagnosticSettings.enableRTVNodeRerunner && NodeTask.isNew(prevState))
            RTVerifierNodeRerunner$.MODULE$.testRTness(ntsk, this);
          if (awaitStacks) awaitMgt.runChain(ntsk);
          else ntsk.run(this);
        } else {
          runIntercepted(ntsk, prevState, info);
        }
      } catch (Throwable ex) { // It really would be nice to just catch Exceptions
        ntsk.completeWithException(ex, this);
      } finally {
        OGTrace.observer.stop(prfCtx, ntsk);
        _currentNode = null;
      }
    }
  }

  /** Uncommon path for run is factored out here */
  private void runIntercepted(NodeTask ntsk, int prevState, NodeTaskInfo info) {
    boolean adapted = false;
    // We want to read the value just once, in case it's updated concurrently
    SchedulerPlugin plugin = null;

    if (info.shouldLookupPlugin() && NodeTask.isNew(prevState)) {
      try {
        plugin = ntsk.getPlugin();
        if (plugin != null) {
          ntsk.markAsAdapted(); // Speculatively mark adapted
          adapted = plugin.adaptInternal(ntsk, this);
        }
      } catch (Throwable e) {
        // TODO (OPTIMUS-12121): Raise alert once alerter api is in place
        OGScheduler.log.error(
            "Unrecoverable error: Adapt is not allowed to throw! Plugin was "
                + plugin
                + ". Exiting process",
            e);
        try {
          OGScheduler.log.error(
              "Node trace where adapt threw: {}", ntsk.waitersToNodeStack(false, false, false, -1));
        } catch (Throwable ignored) {
        }
        InfoDumper$.MODULE$.graphPanic(
            "adapt error", "Exception was thrown from adapt method", 1, e, ntsk);
      }
    }

    if (adapted) {
      OGTrace.adapted(this, ntsk);
      OGSchedulerLostConcurrency.reportPossibleLostConcurrency(ntsk);
    } else {
      ntsk.markAsNotAdapted();
      if (info.getSingleThreaded()) {
        _scheduler.syncLock(info.syncID);
        try {
          if (awaitStacks) awaitMgt.runChain(ntsk);
          else ntsk.run(this);
        } finally {
          _scheduler.syncUnlock(info.syncID);
        }
      } else {
        if (awaitStacks) awaitMgt.runChain(ntsk);
        else ntsk.run(this);
      }
    }
  }

  private void schedulerAssertOnEnqueue(NodeTask task) {
    task.markAsDebugEnqueued();
    assertCorrectThread();
    if (task.scenarioStack() == null)
      throw new GraphInInvalidState(task + " was not attached to a scenario stack");
  }

  /**
   * NOTE: Can only be called on the current thread This call is identical to enqueue, but will not
   * check for causality, and can only be used when running node is enqueuing direct dependency
   * Reasonable usage is from method$queued methods
   */
  public final void enqueueDirect(NodeTask task) {
    // draw the edge even if not really enqueuing (for concurrency tracing)
    OGTrace.enqueue(_currentNode, task, false);
    if (task.isDoneOrNotRunnable()) return; // [CAN_DROP] No point to enqueue
    if (Settings.schedulerAsserts) schedulerAssertOnEnqueue(task);
    localQ.localPush(task);
  }

  /** NOTE: Can only be called on the current thread */
  @Override
  public void enqueue(NodeTask fromTask, NodeTask task) {
    if (task.isDoneOrNotRunnable()) return; // [CAN_DROP] No point to enqueue
    if (Settings.schedulerAsserts) schedulerAssertOnEnqueue(task);
    OGTrace.enqueue(fromTask, task, false);

    if (currentCausalityID == 0 || OGScheduler.canTake(task, _context.threadID, currentCausalityID))
      localQ.localPush(task);
    else _scheduler.enqueue(task);
  }

  /** NOTE: Can only be called on the current thread */
  @Override
  public final void enqueue(NodeTask task) {
    enqueue(_currentNode, task);
  }

  @Override
  public final boolean delayOnChildCompleted(NodeTask parent, NodeTask child) {
    if (delayOnChildCompleted(parent, child, safeStackDepth)) return true;
    safeStackDepth++;
    return false;
  }

  @Override
  public final void afterOnChildCompleted() {
    safeStackDepth--;
  }

  /**
   * Try to remove from the queue (used by nodes that enqueue themselves multiple times (e.g.
   * SequenceNode) Specifically the expectation is to find it right on the top of the queue
   */
  @Override
  public final void tryToRemove(NodeTask ntsk) {
    localQ.tryLocalPop(ntsk);
  }

  /** Sets currently running node task. Not the API you are looking for! */
  public final NodeTask setCurrentNode(NodeTask task) {
    NodeTask prevNode = _currentNode;
    _currentNode = task;
    return prevNode;
  }

  public PrettyStringBuilder writePrettyString(
      PrettyStringBuilder sb,
      boolean skipQueue,
      IdentityHashMap<NodeTask, Integer> waitChainHash,
      int nodeLimit) {
    sb.append("current: ");
    if (_currentNode == null) sb.append("(null)");
    else _currentNode.writePrettyString(sb);
    sb.appendln("");
    if (!skipQueue) {
      sb.appendln("queue: ");
      localQ.toString(sb, nodeLimit);
    }
    // only show the sync stack if it has more than one entry (since every working/waiting thread
    // has at least one)
    if (syncStack.count > 1) {
      sb.append("SyncStack");
      syncStack.toString(sb, waitChainHash).endln();
    }
    return sb;
  }

  @Override
  public String toString() {
    return writePrettyString(new PrettyStringBuilder(), false, null, Integer.MAX_VALUE).toString();
  }

  @Override
  public final Scheduler scheduler() {
    return _scheduler;
  }

  /** Injected by loom adapter into nodes that need to dynamically decide on scheduling */
  @SuppressWarnings("unused")
  public static boolean hasEnoughWork() {
    var ec = OGSchedulerContext.current();
    return ec.localQ.size() > 4;
  }
}

interface NodeStackVisitor {
  /**
   * @param frame the current frame - either a SyncFrame or the OGSchedulerContext itself
   * @param owner the OGSchedulerContext which owns the frame
   * @return true to continue visiting or false to stop
   */
  boolean visit(HasCurrentNode frame, OGSchedulerContext owner);
}

interface HasCurrentNode {
  NodeTask getCurrentNodeTask();
}
