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

import static optimus.graph.OGScheduler.schedulerVersion;
import static optimus.graph.cache.NCSupport.isDelayedProxy;
import static optimus.graph.cache.NCSupport.isProxy;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serial;
import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.WeakHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import optimus.breadcrumbs.ChainedID;
import optimus.core.ArrayListWithMarker;
import optimus.core.CoreHelpers;
import optimus.core.EdgeList;
import optimus.core.MonitoringBreadcrumbs$;
import optimus.core.SparseBitSet;
import optimus.core.TPDMask;
import optimus.exceptions.RTList;
import optimus.graph.cache.NCSupport;
import optimus.graph.diagnostics.GraphDiagnostics;
import optimus.graph.diagnostics.InfoDumper;
import optimus.graph.diagnostics.NodeName;
import optimus.graph.diagnostics.NodeName$;
import optimus.graph.diagnostics.PNodeTask;
import optimus.graph.diagnostics.ProfiledEvent;
import optimus.graph.diagnostics.gridprofiler.GridProfiler;
import optimus.graph.loom.LNodeClsID;
import optimus.graph.tracking.CancelOrphanedDependencyTrackerNodesException;
import optimus.platform.EvaluationQueue;
import optimus.platform.RuntimeEnvironment;
import optimus.platform.ScenarioStack;
import optimus.platform.ScenarioStack$;
import optimus.platform.util.PrettyStringBuilder;
import scala.Option;

/*
 * Base class for Node
 *
 * It's in java to deal explicitly with atomics and all non-result relevant code
 *
 * Notes on back traces AKA node stacks.
 *
 * 1. Wait chain forms a directed tree. This is different from a classical thread linear dependency.
 *
 * 2. Wait chain (_waiters) is not available after the node completes. Therefore it can NOT be used
 * to see the parent after node completes.
 *
 * 3. Wait chain is not immediately available when the node is created and perhaps surprisingly not
 * necessarily when node is executed. Consider the case:
 *
 * def c = a + b.
 *
 * Assume a, b, c are nodes. We logically re-write c node as follows:
 *
 * val an = schedule(a) // Some other thread can now execute a node val bn = schedule(b)
 * // Yet another thread can execute b and in fact complete it before the next line executes!
 * an.continueWith( { // a node waiters will point to c node if a node wasn't yet completed.
 *   val a = an.result // c node uses the result of a node and the dependency of c on a is recorded
 *   // note: if a completed with exception, the lines below won't be executed
 *   bn.continueWith( {
 *     // b node waiters will point to c node if b node wasn't yet completed.
 *     val b = bn.result // c node uses the result of b node and the dependency of c on b is recorded
 *     completeWithResult(a+b) // c nodes will complete with the result of a + b
 *   })
 * }
 */

public abstract class NodeTask extends NodeAwaiter
    implements LNodeClsID, Launchable, NodeCause, DebugWaitChainItem, Serializable, Cloneable {
  static final long serialVersionUID = 1;
  private static final VarHandle waiters_h;
  private static final VarHandle state_h;
  // Used to mark final state of CASed _waiters
  protected static final Object completed = new Object();
  static final NodeTask nullTask; // Marker used by scheduler
  public static Object[] argsUnknown = new Object[0];
  public static Object[] argsEmpty = new Object[0];

  static {
    // Keep this block on the top, order could matter!
    try {
      var lookup = MethodHandles.lookup();
      waiters_h = lookup.findVarHandle(NodeTask.class, "_waiters", Object.class);
      state_h = lookup.findVarHandle(NodeTask.class, "_state", int.class);
    } catch (Exception ex) {
      throw new Error(ex);
    }

    nullTask = new NullTask();
  }

  @SuppressWarnings(
      "unused") // Ensure that GraphDebuggerExtensionsServer class is loaded before first break
  // point is hit
  private static final String ensureGraphDebuggerExtensionsServerLoaded =
      GraphDebuggerExtensionsServer.name;

  private static final int STATE_NEW = 0;
  static final int STATE_STARTED = 1; // Used to call plug-ins only on the first run
  static final int STATE_NOT_RUNNABLE = 2; // If bit is clear the node is eligible to run
  // Set when node is done, almost always used with STATE_NOT_RUNNABLE
  public static final int STATE_DONE_FLAG = 4; // Done and can't run anymore
  public static final int STATE_DONE = STATE_DONE_FLAG | STATE_NOT_RUNNABLE;
  private static final int STATE_DONE_FAILED = 8 | STATE_DONE; // Done and have exception
  // Non-RT exception present, set with DONE_FAILED
  private static final int STATE_HIDE_ON_DIFF_CS = 0x10;
  private static final int STATE_DONE_FAILED_HIDE = STATE_HIDE_ON_DIFF_CS | STATE_DONE_FAILED;
  private static final int STATE_RUN_MASK = 0x1F; // First 5 bits are reserved for running states

  private static final int STATE_SCENARIO_DEPENDENT = 0x20; // Transitively looked up a tweak...
  private static final int STATE_INVALID_CACHE = 0x40; // Mark PropertyNode as invalid in cache
  // Has tracking information to pass to tweakable listener
  private static final int STATE_TRACKING_VALUE = 0x80;
  private static final int STATE_XSCENARIO_OWNER = 0x100; // Owner of x-scenario
  // Currently used by remoting plugin in-proc test code
  private static final int STATE_REMOTED = 0x1000;
  private static final int STATE_ADAPTED = 0x2000; // Marks nodes as adapted,
  // TODO (OPTIMUS-13315): review the usage! management
  // mark all nodes on this engine currently in cache on task startup
  private static final int STATE_SEEN_BEFORE = 0x4000;

  private static final int STATE_VISITED_GRAY = 0x8000; // Used by any graph walker
  private static final int STATE_VISITED_BLACK = 0x10000; // Used by any graph walker
  private static final int STATE_DEBUG_ENQUEUED = 0x20000; // Used by some asserts
  // Used by scheduler when waiting for cancelled nodes to stop - see [WAIT_FOR_CANCELLED]
  private static final int STATE_WAITING_FOR_CANCELLED_NODES = 0x80000;

  // The task has a plugin even if not actually adapted - e.g. a promise.
  private static final int STATE_HAS_REPORTING_PLUGIN_TYPE = 0x100000;
  // E.g. the the batch has been sent out:
  private static final int STATE_PLUGIN_FIRED = 0x200000;
  // Distinct from STATE_ADAPTED, as it is not set pre-emptively:
  private static final int STATE_PLUGIN_TRACKED = 0x400000;

  // Only one walker is allowed at a time
  private static final ReentrantLock _visitLock = new ReentrantLock();

  /** values are combination of the above STATE_* set atomically */
  @SuppressWarnings("unused")
  private volatile int _state;

  /**
   * Causality with respect to the stack of a given thread this field is used to avoid picking up
   * work from a global queue(s) that is unrelated to the 'current' stack of a given thread Internal
   * read by OGScheduler / set by OGSchedulerContext
   */
  private transient int _causality;

  final int causalityID() {
    return _causality & 0xFFFF; // Lower 16 bits
  }

  final int causalityThreadID() {
    return _causality >> 16; // High 16 bits
  }

  final void setCausality(int causality, int threadID) {
    _causality = (causality & 0xFFFF) | (threadID << 16);
  }

  /**
   * In the cases where it's impossible to prove the dependency a node can take it's important to
   * mark this node as not not-safe for a random thread to pick up
   */
  final void poisonCausality() {
    setCausality(1, Integer.MAX_VALUE);
  }

  /**
   * List of Nodes waiting for our result States: null - initial state NodeTask - single waiter
   * NWList - more than one waiter NodeTaskBase.completed - the final state
   *
   * <p>TODO (OPTIMUS-13315): Consider special case 2 or 3 waiters as well if it would turn out to
   * be a common case
   */
  private transient volatile Object _waiters;

  /** ScenarioStack under which to execute node */
  private ScenarioStack _scenarioStack;

  /** Extra information that node carries as the result. That includes exceptions and warnings */
  // NodeExtraInfo or StdNodeExtraInfo
  protected NodeExtendedInfo _xinfo = NullNodeExtendedInfo$.MODULE$;

  /** Can be null to mean that this Node has never been scheduled to be executed */
  public final ScenarioStack scenarioStack() {
    return _scenarioStack;
  }

  static boolean isNew(int state) {
    return (state & STATE_RUN_MASK) == STATE_NEW;
  }

  /** Returns true if this node has never been ran, not the method you are looking for */
  public final boolean isNew() {
    return isNew(_state);
  }

  /**
   * Most nodes are scheduled and they are of course interested in their values. Current exception
   * is the XSFTProxy, that some other proxy might be waiting for a value from But it's not
   * guaranteed that anyone will actually ask for the value of that proxy in that scenarioStack.
   * Note: the result is not stable (someone could become interested in the value before this
   * function even returns) Therefore: code should not be sensitive to a spurious false returned
   */
  public boolean valueWasRequested() {
    return true;
  }

  public boolean hasWaiters() {
    return _waiters != null;
  }

  /* Allow for recover logic on some node after a cycle has been detected */
  public boolean tryToRecoverFromCycle(TPDMask mask, EvaluationQueue eq) {
    return false;
  }

  public final boolean isRemoted() {
    return (_state & STATE_REMOTED) == STATE_REMOTED;
  }

  public final void markRemoted() {
    updateState(STATE_REMOTED, 0);
  }

  // for cache hits between different tasks on the same engine
  public final boolean isSeenBefore() {
    return (_state & STATE_SEEN_BEFORE) == STATE_SEEN_BEFORE;
  }

  public final void markSeenBefore() {
    updateState(STATE_SEEN_BEFORE, 0);
  }

  public final boolean isDone() {
    return (_state & STATE_DONE) == STATE_DONE;
  }

  /** Tests the node if it's in final state. Consider most uses isDone should be isDoneEx */
  public final boolean isDoneEx() {
    return (_state & (STATE_DONE_FLAG | STATE_INVALID_CACHE)) != 0;
  }

  public final boolean isDoneOrInvalidated() {
    return (_state & (STATE_DONE_FLAG | STATE_INVALID_CACHE)) != 0;
  }

  public final boolean isStarted() {
    return (_state & STATE_STARTED) == STATE_STARTED;
  }

  private boolean debugIsNotEnqueued() {
    return (_state & (STATE_DEBUG_ENQUEUED | STATE_RUN_MASK)) == 0;
  }

  public final boolean isDoneAndIsScenarioIndependent() {
    return (_state & (STATE_DONE | STATE_SCENARIO_DEPENDENT)) == STATE_DONE;
  }

  private static boolean isDone(int state) {
    return (state & STATE_DONE) == STATE_DONE;
  }

  public final boolean isDoneWithResult() {
    return (_state & STATE_DONE_FAILED) == STATE_DONE;
  }

  private static boolean isDoneWithException(int state) {
    return (state & STATE_DONE_FAILED) == STATE_DONE_FAILED;
  }

  public final boolean isDoneWithException() {
    return isDoneWithException(_state);
  }

  private static boolean isDoneWithExceptionToHide(int state) {
    return (state & STATE_DONE_FAILED_HIDE) == STATE_DONE_FAILED_HIDE;
  }

  public final boolean isDoneWithExceptionToHide() {
    return isDoneWithExceptionToHide(_state);
  }

  // if we've already read state --- save a volatile read
  private static boolean isDoneWithUsableResult(int state) {
    // DEH = Done Exception Hide, we want ?1?0?
    return (state & (STATE_DONE | STATE_HIDE_ON_DIFF_CS)) == STATE_DONE;
  }

  /** Result is fully computed and in the case of exception it's a valid result */
  public final boolean isDoneWithUsableResult() {
    return isDoneWithUsableResult(_state);
  }

  public final String toPrettyName(boolean useEntityType, boolean showKeys) {
    return toPrettyName(useEntityType, showKeys, false);
  }

  @Override
  public final String toSimpleName() {
    return toPrettyName(false, false, false, true, false, false);
  }

  public final String toPrettyName(boolean useEntityType, boolean showKeys, boolean showNodeState) {
    return toPrettyName(useEntityType, showKeys, showNodeState, false, true, false);
  }

  public final String toPrettyName(
      boolean useEntityType,
      boolean showKeys,
      boolean showNodeState,
      boolean simpleName,
      boolean showCausalityID) {
    return toPrettyName(useEntityType, showKeys, showNodeState, simpleName, showCausalityID, false);
  }

  public final String toPrettyName(
      boolean useEntityType,
      boolean showKeys,
      boolean showNodeState,
      boolean simpleName,
      boolean showCausalityID,
      boolean includeHint) {
    PrettyStringBuilder sb = new PrettyStringBuilder();
    sb.useEntityType_$eq(useEntityType);
    sb.showKeys_$eq(showKeys);
    sb.showNodeState_$eq(showNodeState);
    sb.showCausalityID_$eq(showCausalityID);
    sb.simpleName_$eq(simpleName);
    sb.includeHint_$eq(includeHint);
    return writePrettyString(sb).toString();
  }

  public String classMethodName() {
    PrettyStringBuilder sb = new PrettyStringBuilder();
    return writePrettyString(sb).toString();
  }

  @Override
  public String toString() {
    return toPrettyName(false, false, true);
  }

  /**
   * Override this method every time you customize writePrettyString's class/entity name if the node
   * is a wrapper around some other node, or if the node derives from some other node with a shared
   * profileId (eg ProfilerResultNode)
   */
  public Object subProfile() {
    return null;
  }

  /** Most nodes don't have underlying task that is different than 'this' except for proxy nodes */
  public NodeTask cacheUnderlyingNode() {
    return this;
  }
  /** For most nodes, once they have an assigned cacheID, they will be computed in that cacheID */
  public long computedInCacheID() {
    return _scenarioStack._cacheID().id();
  }

  public PrettyStringBuilder writePrettyString(PrettyStringBuilder sb) {
    sb.append(nodeName().toString(sb.simpleName(), sb.includeHint()));

    if (subProfile() != null) {
      sb.append(" ")
          .append(
              NodeName$.MODULE$
                  .fromSubProfile(subProfile())
                  .toString(sb.simpleName(), sb.includeHint()));
    }
    if (sb.showNodeState()) {
      sb.append(":");
      sb.append(stateAsString());
    }
    return sb;
  }

  private static PrettyStringBuilder stateAsString(int state) {
    PrettyStringBuilder sb = new PrettyStringBuilder();
    if (isNew(state)) sb.append("N");
    else {
      if ((state & STATE_STARTED) == STATE_STARTED) sb.append("S");
      if ((state & STATE_NOT_RUNNABLE) == STATE_NOT_RUNNABLE) sb.append("R");
      if ((state & STATE_DONE_FAILED) == STATE_DONE_FAILED) sb.append("C[EX]");
      else if ((state & STATE_DONE) == STATE_DONE) sb.append("C");

      if ((state & STATE_SCENARIO_DEPENDENT) != STATE_SCENARIO_DEPENDENT) sb.append("i");
      if ((state & STATE_ADAPTED) == STATE_ADAPTED) sb.append("a");
    }
    // note that this flag only get set if you have Settings.schedulerAsserts enabled - in normal
    // apps it's always false
    if ((state & STATE_DEBUG_ENQUEUED) == STATE_DEBUG_ENQUEUED) sb.append("q");
    if ((state & STATE_REMOTED) == STATE_REMOTED) sb.append("r");
    if ((state & STATE_TRACKING_VALUE) != 0) sb.append("T");
    if ((state & STATE_XSCENARIO_OWNER) != 0) sb.append("_X");
    if ((state & STATE_INVALID_CACHE) != 0) sb.append("#");

    if ((state & STATE_WAITING_FOR_CANCELLED_NODES) != 0) sb.append("W");

    return sb;
  }

  public final String stateAsString() {
    PrettyStringBuilder sb = stateAsString(_state);
    return stateAsString(sb);
  }

  private String stateAsString(PrettyStringBuilder sb) {
    sb.append(':');
    sb.append(executionInfo().flagsAsString());
    if (sb.showCausalityID() && !isDone() && !isNew()) {
      sb.append(':');
      sb.append(causalityID());
      sb.append("-T");
      sb.append(causalityThreadID());
    }
    return sb.toString();
  }

  public String flameFrameName(String extraMods) {
    var prefix = "@"; // for optimus-attuned eyes
    var suffix = "_[a]"; // for categorization by async-profiler
    var pname = NodeName.profileFrom(this).toString();
    var info = executionInfo();
    // proxies already end in "~$"
    var modifiers = (info.getCacheable() && !info.isProxy()) ? "$" : "";
    return prefix + pname + modifiers + extraMods + suffix;
  }

  public boolean elideChildFrame() {
    return executionInfo().isProxy();
  }

  public void launch(AwaitableContext ec) {
    run((OGSchedulerContext) ec);
  }

  public NodeTask underlyingAwaitable() {
    return cacheUnderlyingNode();
  }

  protected final int getState() {
    return _state;
  }

  /**
   * Atomically sets and clear flags Returns the value right before the update (Obviously it can be
   * stale by then)
   */
  protected final int updateState(int setFlags, int clearFlags) {
    int prevState, newState;
    do {
      prevState = _state;
      newState = (prevState | setFlags) & ~clearFlags;
    } while (!state_h.compareAndSet(this, prevState, newState));

    return prevState;
  }

  protected final boolean getAndSet(int setFlags) {
    return (updateState(setFlags, 0) & setFlags) == setFlags;
  }

  /**
   * Atomically sets flags but only if the done flag is not set Returns the value right before the
   * update (Obviously it can be stale by then)
   */
  private int tryUpdateState(int setFlags, int clearFlags) {
    int prevState, newState;
    do {
      prevState = _state;
      if ((prevState & STATE_DONE) == STATE_DONE) return prevState;
      newState = (prevState | setFlags) & ~clearFlags;
    } while (!state_h.compareAndSet(this, prevState, newState));

    return prevState;
  }

  /**
   * NOT thread safe and can ONLY be called in the constructor of a derived class
   * [PERF_OGTRACE_STOP_HERE_TO_ATTRIBUTE_TIME_TO_REQUESTING_NODE]
   */
  protected final void initAsRunning(ScenarioStack ss) {
    state_h.setOpaque(this, _state | STATE_STARTED | STATE_NOT_RUNNABLE);
    _scenarioStack = ss;
  }

  /** NOT thread safe and can ONLY be called in the constructor of a derived class */
  protected final void initAsCompleted() {
    initAsCompleted(ScenarioStack$.MODULE$.constant());
  }

  /** NOT thread safe and can ONLY be called in the constructor of a derived class */
  protected final void initAsFailed(Throwable ex) {
    initAsFailed(ex, ScenarioStack$.MODULE$.constant());
  }

  /**
   * NOT thread safe and can ONLY be called in the constructor of a derived class but has MB
   * [PERF_OGTRACE_STOP_HERE_TO_ATTRIBUTE_TIME_TO_REQUESTING_NODE]
   */
  protected final void initAsCompleted(ScenarioStack ss) {
    state_h.setOpaque(this, _state | STATE_DONE | STATE_STARTED);
    _scenarioStack = ss;
    setLauncherRef(null);
    _waiters = completed; // MB!
    if (Settings.allowTestableClock) OGTrace.publishEvent(getId(), ProfiledEvent.NODE_COMPLETED);
    OGTrace.observer.initializeAsCompleted(this);
  }

  /**
   * NOT thread safe and can ONLY be called in the constructor of a derived class, but has MB
   * [PERF_OGTRACE_STOP_HERE_TO_ATTRIBUTE_TIME_TO_REQUESTING_NODE]
   */
  protected final void initAsFailed(Throwable ex, ScenarioStack ss) {
    _xinfo = _xinfo.withException(ex);
    state_h.setOpaque(this, _state | STATE_STARTED | stateFlagsForThrowable(ex));
    _scenarioStack = ss;
    setLauncherRef(null);
    _waiters = completed; // MB!
    if (Settings.allowTestableClock) OGTrace.publishEvent(getId(), ProfiledEvent.NODE_COMPLETED);
  }

  /**
   * Returns true if the node is tweakable/tweaked (aka TRACKING_VALUE or XS proxy) but not XS_OWNER
   * Note: Need to exclude XS_OWNER because XS proxies transfer Tweak/Tweakable information manually
   * See NCSupport#matchXscenario(NodeTask, ScenarioStack, RecordedTweakables) [XS_BY_NAME_TWEAKS]
   */
  private static boolean isTrackingValueNonXSOwner(int state) {
    return (state & (STATE_TRACKING_VALUE | STATE_XSCENARIO_OWNER)) == STATE_TRACKING_VALUE;
  }

  public final boolean isTrackingValue() {
    return (_state & STATE_TRACKING_VALUE) != 0;
  }

  /**
   * For recording scenarios this tells the combineInfo that this node has tweakables to report.
   * (flag set) For tracking scenario DependencyTracker's ttracks will be pointing to XS proxy
   * itself (flag clear)
   *
   * <p>Note: NOT thread safe and can ONLY be called in the constructor of a derived class
   *
   * <p>Note 2: complete() unflags this value. This is fine... until someone calls reset(). If you
   * call this function you *must* provide a reset override that re-marks the node.
   */
  public final void markAsTrackingValue() {
    state_h.setOpaque(this, _state | STATE_TRACKING_VALUE);
  }

  public final boolean isXScenarioOwner() {
    return (_state & STATE_XSCENARIO_OWNER) != 0;
  }

  /** Note: NOT thread safe and can ONLY be called in the constructor of a derived class */
  public final void markAsXScenarioOwner() {
    state_h.setOpaque(this, _state | STATE_XSCENARIO_OWNER);
  }

  public final void markScenarioDependent() {
    updateState(STATE_SCENARIO_DEPENDENT, 0);
  }

  final void markAsWaitingForCancelledNodesToStop() {
    updateState(STATE_WAITING_FOR_CANCELLED_NODES, 0);
  }

  final boolean notWaitingForCancelledNodesToStop() {
    return (_state & STATE_WAITING_FOR_CANCELLED_NODES) == 0;
  }

  private static boolean isScenarioDependent(int state) {
    return (state & STATE_SCENARIO_DEPENDENT) != 0;
  }

  private static boolean isAdapted(int state) {
    return (state & STATE_ADAPTED) != 0;
  }

  private static boolean pluginTracked(int state) {
    return (state & STATE_PLUGIN_TRACKED) != 0;
  }

  public final SchedulerPlugin getPlugin() {
    return executionInfo().getPlugin(scenarioStack());
  }

  // Is there a plugin type for any reason, whether a real plugin or just for reporting
  private static boolean hasPluginType(int state) {
    return (state & (STATE_ADAPTED | STATE_HAS_REPORTING_PLUGIN_TYPE)) != 0;
  }

  public final boolean isAdapted() {
    return isAdapted(_state);
  }

  public boolean pluginTracked() {
    return pluginTracked(_state);
  }

  public final boolean hasPluginType() {
    return hasPluginType(_state);
  }

  public final PluginType getPluginType() {
    return PluginType.getPluginType(this);
  }

  private static boolean pluginFired(int state) {
    return (state & STATE_PLUGIN_FIRED) != 0;
  }

  public final boolean pluginFired() {
    return pluginFired(_state);
  }

  public final void setPluginFired() {
    updateState(STATE_PLUGIN_FIRED, 0);
  }

  public final PluginType getReportingPluginType() {
    if (!hasPluginType(_state)) return PluginType.None();
    // First check explicit instance-specific type (usually due to readapt)
    PluginType tp = getPluginType();
    if (tp != PluginType.None()) return tp;
    // Fall back to NodeTaskInfo
    return executionInfo().reportingPluginType(scenarioStack());
  }

  final void markAsAdapted() {
    updateState(STATE_ADAPTED, 0);
  }

  final boolean getAndSetPluginTracked() {
    return getAndSet(STATE_PLUGIN_TRACKED);
  }

  final boolean getAndSetPluginFired() {
    return getAndSet(STATE_PLUGIN_FIRED);
  }

  public final void markAsHavingPluginType() {
    updateState(STATE_HAS_REPORTING_PLUGIN_TYPE, 0);
  }

  final void markAsNotAdapted() {
    if (isAdapted()) updateState(0, STATE_ADAPTED);
  }

  /** Used to debug issues with forgetting to enqueue nodes before waiting for them */
  final void markAsDebugEnqueued() {
    updateState(STATE_DEBUG_ENQUEUED, 0);
  }

  /**
   * Marks the node as non-runnable. Returning previous value of the state TODO (OPTIMUS-13315):
   * MUST become internal!!!!
   */
  protected final int tryRun() {
    return updateState(STATE_STARTED | STATE_NOT_RUNNABLE, 0);
  }

  /** Mark the node as runnable see [MUST_ENQUEUE] */
  final void markAsRunnable() {
    if (!isDoneOrRunnable()) updateState(0, STATE_NOT_RUNNABLE);
    else if (Settings.schedulerAsserts)
      // It is always incorrect to markAsRunnable a completed node...
      //
      // except that when we discover a circular reference exception we can complete a parent node
      // without completing its children, which causes it to be re-enqueued when the child
      // completes.
      if (!(isDoneWithException() && (exception() instanceof CircularReferenceException)))
        throw new GraphInInvalidState(
            "markAsRunnable called on a completed or already runnable node!");
  }

  /** Return true if successfully marked the node as runnable */
  final boolean tryMarkAsRunnable() {
    int prevState = tryUpdateState(0, STATE_NOT_RUNNABLE);
    return !NodeTask.isDone(prevState);
  }

  /** Note: Unstable value */
  final boolean isRunnable() {
    return (_state & STATE_NOT_RUNNABLE) == 0;
  }

  /**
   * @return true if the task is marked as runnable or done DONE NOT_RUNNABLE RUNNABLE_DERIVED
   *     isDoneOrRunnable 0 0 1 T 0 1 0 F Just check for this value 1 0 1 T 1 1 0 T
   */
  private static boolean isDoneOrRunnable(int state) {
    return (state & (STATE_DONE | STATE_NOT_RUNNABLE)) != STATE_NOT_RUNNABLE;
  }

  final boolean isDoneOrRunnable() {
    return isDoneOrRunnable(_state);
  }

  /** Checks for 3 conditions common conditions for [CAN_DROP] */
  final boolean isDoneOrNotRunnable() {
    return (_state & (STATE_DONE | STATE_NOT_RUNNABLE | STATE_INVALID_CACHE)) != 0;
  }

  /**
   * This callback is called when a "child node" is completed. A "child node" is one which we have
   * subscribed to by calling child.continueWith*(this). The callback could happen immediately,
   * during the call to child.continueWith, if child was already complete (potentially on the same
   * thread), or it could happen later and/or from a different thread. This callback is called at
   * most once per call to continueWith (i.e. no spurious/duplicate callbacks).
   *
   * <p>The default implementation just marks this node as runnable and enqueues it so that run()
   * will be called (at some point in the future).
   *
   * <p>One possible optimization for subclasses is to override this method to bypass enqueue() and
   * the work that run() would have done here instead. <b>Warning:</b> one must be careful to avoid
   * stack overflow in that case (due to a completion of this node causing onChildComplete on others
   * and so forth, all on the same stack). It's safer to call this.complete* from run() than from
   * here.
   *
   * <p><b>Further warnings:</b> The EvaluationQueue eq should never be null (if it is then it's the
   * caller's bug), but it may be a OGScheduler rather than an OGSchedulerContext if we're called
   * from a non-graph thread. In that case EvaluationContext.current will be unavailable, so
   * someNode$queued / Node#enqueue will fail. Also note that the EvaluationContext.currentNode is
   * somewhat arbitrary and must not be relied on - it's not necessarily the same as child. It's
   * safer and easier to enqueue other nodes from run() rather than from here.
   *
   * <p><b>Yet another warning:</b> If onChildCompleted throws an exception, the Graph will
   * logAndDie. So either don't do anything which could reasonably cause an exception, or catch it,
   * or do it in run() instead of here.
   */
  @Override
  protected void onChildCompleted(EvaluationQueue eq, NodeTask child) {
    if (Settings.schedulerAsserts && eq == null)
      throw new GraphInInvalidState("onChildCompleted(eq = null, " + child + ") this = " + this);
    markAsRunnable();
    if (Settings.keepTaskSchedulerAffinity) {
      var prefScheduler = _scenarioStack.siParams().scheduler().get();
      if (prefScheduler != null && eq.scheduler() != prefScheduler) {
        prefScheduler.enqueue(null, this);
      } else eq.enqueue(null, this);
    } else eq.enqueue(null, this);
  }

  @Override
  public NodeTask awaiter() {
    return this;
  }

  /** Singly linked list with zero abstraction (as in no-overhead) */
  protected static class NWList {
    public NodeAwaiter waiter;
    public NWList prev;

    NWList(NodeAwaiter waiter, NWList prev) {
      this.waiter = waiter;
      this.prev = prev;
    }

    /** Creates a new list with the newWaiter at the end of the chain */
    public NWList append(NodeAwaiter newWaiter) {
      // Copy the list and keep track of the last item
      NWList result = new NWList(waiter, null);
      NWList last = result;
      NWList org = this;
      while (org.prev != null) {
        last.prev = new NWList(org.prev.waiter, null);
        last = last.prev;
        org = org.prev;
      }
      last.prev = new NWList(newWaiter, null);
      return result;
    }
  }

  /**
   * Normally it's OK and supported to subscribe multiple times to the same node's completion
   * notification However, it's NOT likely what you want. This code if left here to enable such
   * checks when needed. If interested you would need to inject this call into tryAddToWaiterList
   * and/or tryAddToWaiterListLast
   */
  @SuppressWarnings("unused")
  private void verifyUnique(NodeAwaiter waiter) {
    Object waiters = _waiters; // Previous waiters
    if (waiters == null) return;
    if (waiters == waiter) throw new GraphInInvalidState("Re-registering waiter?!");

    if (waiters instanceof NWList lst) {
      while (lst != null) {
        if (lst.waiter == waiter) throw new GraphInInvalidState("Re-registering waiter?!");
        lst = lst.prev;
      }
    }
  }

  /**
   * Adds to waiters a new callback/parent task if Node is not done (MT safe) returns true is added
   * to list, false if waiters are already removed to be notified (on completion) The order is LIFO
   */
  public final boolean tryAddToWaiterList(NodeAwaiter waiter) {
    waiter.setWaitingOn(this); // Tentatively set to this node
    while (true) {
      Object pwait = _waiters; // Previous waiters
      if (pwait == completed) {
        // Node is already completed
        waiter.setWaitingOn(null);
        return false;
      }

      Object nwait;
      if (pwait == null) nwait = waiter;
      else {
        NWList prev =
            (pwait instanceof NodeAwaiter) ? new NWList((NodeAwaiter) pwait, null) : (NWList) pwait;
        nwait = new NWList(waiter, prev);
      }

      if (waiters_h.compareAndSet(this, pwait, nwait)) return true;
    }
  }

  /**
   * Adds to waiters a new callback/parent task if Node is not done (MT safe) returns true is added
   * to list, false if waiters are already removed to be notified (on completion) The order is LILO
   */
  private boolean tryAddToWaiterListLast(NodeAwaiter waiter) {
    waiter.setWaitingOn(this); // Tentatively set to this node
    while (true) {
      Object pwait = _waiters; // Previous waiters
      if (pwait == completed) {
        // Node is already completed
        waiter.setWaitingOn(null);
        return false;
      }

      Object nwait;
      if (pwait == null) nwait = waiter;
      else if (pwait instanceof NodeAwaiter nodeAwaiter) {
        nwait = new NWList(nodeAwaiter, new NWList(waiter, null));
      } else {
        nwait = ((NWList) pwait).append(waiter); // Previous list of waiters
      }

      if (waiters_h.compareAndSet(this, pwait, nwait)) return true;
    }
  }

  /** Atomically transition waiters to 'completed' state and notify previously registered waiters */
  private void notifyWaiters(EvaluationQueue eq) {
    Object waiters = _waiters; // unsafe.swap would be great!
    while (!waiters_h.compareAndSet(this, waiters, completed)) {
      waiters = _waiters;
    }
    setLauncherRef(null);

    if ((waiters == null) || (waiters == completed)) return;
    if (waiters instanceof NodeAwaiter waiter) {
      waiter.setWaitingOn(null);
      waiter.onChildCompleted(eq, this);
    } else {
      NWList lst = (NWList) waiters;
      while (lst != null) {
        lst.waiter.setWaitingOn(null);
        lst.waiter.onChildCompleted(eq, this);
        lst = lst.prev;
      }
    }
  }

  @Serial
  private void writeObject(ObjectOutputStream out) throws IOException {
    out.defaultWriteObject();
  }

  /**
   * Very unlikely to be the method you are looking for It sometimes (not even always) will lookup
   * the thread local context and use that for scheduling responses. The word 'sometimes' above
   * should scare you! It also could be outright wrong. Consider completing nodes from non-graph
   * threads or wrong schedulers There are legitimate uses but they are not common and unless you
   * can prove that it's safe, don't use it
   */
  public final void continueOnCurrentContextWith(NodeAwaiter waiter) {
    continueWith(waiter, null);
  }

  /**
   * Registers waiter to be called back at onChildCompleted when this node completes. If this node
   * is already complete, the callback happens immediately before continueWith returns. Otherwise it
   * happens later, potentially from a different thread.
   *
   * <p>If you're calling this from your run() method, note that you may get called back from
   * another thread before that run() completes, so it's generally safest to call this only in tail
   * position, i.e. avoid touching your own state after calling continueWith (see note on run()).
   */
  public final void continueWith(NodeAwaiter waiter, EvaluationQueue eq) {
    if (Settings.schedulerAsserts && debugIsNotEnqueued())
      throw new GraphInInvalidState("continueWith should not be called on an unscheduled node!");

    // we record this as a suspend because almost all use cases of continueWith suspend the waiter
    // immediately after
    OGTrace.observer.suspend(eq, waiter);

    if (!tryAddToWaiterList(waiter))
      waiter.onChildCompleted(eq != null ? eq : OGSchedulerContext.current(), this);
  }

  // same as continueWith except that we don't assert that this node has been enqueued. you probably
  // don't
  // want to call this version of the method unless you are intentionally listening for a node which
  // may never run.
  public final void continueWithIfEverRuns(NodeAwaiter waiter, EvaluationQueue eq) {
    // note that the users of this method (UTracks, weird manual nodes) are not really running when
    // they call this
    // (though some are marked as running), so are not suspending after calling this, so we don't
    // call OGTrace.suspend.

    if (!tryAddToWaiterList(waiter))
      waiter.onChildCompleted(eq != null ? eq : OGSchedulerContext.current(), this);
  }

  /** This API is for a very unusual case and is generally slower than continueWith */
  public final void continueWithLast(NodeAwaiter waiter, EvaluationQueue eq) {
    if (Settings.schedulerAsserts) {
      // note that the users of this method (Scheduler async evaluate methods) are never actually
      // running themselves so are not suspending after calling this, so we don't call
      // OGTrace.suspend. Let's just make sure that's true!
      if ((waiter instanceof NodeTask) && ((NodeTask) waiter).isStarted()) {
        throw new GraphInInvalidState(
            "continueWithLast should not be called with a waiter which actually ran! " + waiter);
      }
    }

    // Usually, if we care about being last in the waiters list it is because we are effectively
    // enqueuing the thing, even if we aren't actually going through enqueue()
    if (DiagnosticSettings.awaitStacks && waiter instanceof Awaitable aw)
      AwaitStackManagement.setLaunchData(aw, this, false, 0);

    if (!tryAddToWaiterListLast(waiter)) waiter.onChildCompleted(eq, this);
  }

  public final void continueWithFSM(NodeStateMachine continuation) {
    NodeTask curNode = continuation._result();
    OGSchedulerContext ec = continuation.getAndResetEC();

    if (Settings.schedulerAsserts) {
      if (debugIsNotEnqueued())
        throw new GraphInInvalidState("continueWithFSM called on a new node!");
      if (curNode != OGSchedulerContext.current().getCurrentNodeTask())
        throw new GraphInInvalidState(
            "continueWithFSM called with result() set to something that is not current node!");
    }

    continuation.setChildNode(this);
    curNode.setContinuation(continuation);
    continueWith(curNode, ec); // Schedule continuation
  }

  /**
   * Attaches given scenarioStack to a new node. It's forgiving for trying to attach the same
   * scenarioStack multiple times
   *
   * <p>Only a careful set of functions should be allowed to call this function Expectation is that
   * attach method is called exactly once, and this needs to be proved and not checked for
   */
  public final void attach(ScenarioStack ss) {
    if (_scenarioStack == null) _scenarioStack = ss;
    else if (_scenarioStack != ss && !isStable()) {
      // [SEE_RESET_SS]
      if (!(isDone() && _scenarioStack._cacheID() == ss._cacheID()))
        throw new GraphInInvalidState("attach can be called only once on " + this);
    }
  }

  /**
   * The valid replace calls are only by SchedulerPlugin(s) that call it early enough before any
   * code could have made any use of it, or inserting custom ScenarioStack that is equivalent to the
   * current as far as the rest of runtime is involved
   *
   * <p>Only a careful set of functions should be allowed to call this function!!!!
   */
  public final void replace(ScenarioStack ss) {
    _scenarioStack = ss;
  }

  /** For internal use only Makes protected call available to the package */
  Object cloneTask() throws CloneNotSupportedException {
    NodeTask task = (NodeTask) super.clone(); // Make a copy first to avoid race condition
    task.setProfile(null);
    if (DiagnosticSettings.traceAvailable) {
      task.setId(OGSchedulerContext.nextNodeId());
    }
    if (!isNew(task._state)) throw new GraphInInvalidState("Only new nodes can be cloned");
    return task;
  }

  /**
   * Used for test purposes only! It is used by the RT Verifier Node Rerunner to clone new nodes
   * (which is normally not allowed!)
   */
  public NodeTask DEBUG_ONLY_cloneTask() throws CloneNotSupportedException {
    NodeTask task = (NodeTask) super.clone();
    task.reset();
    return task;
  }

  /** Stop Generic clone from being used and propagated */
  @Override
  protected final Object clone() throws CloneNotSupportedException {
    throw new CloneNotSupportedException();
  }

  /**
   * Returns a node that one can evaluate to get the value of this node under the specified
   * scenario. It might return mutate and return this node, or might return some other node.
   */
  public NodeTask prepareForExecutionIn(ScenarioStack ss) throws CloneNotSupportedException {
    return attachOrClone(ss);
  }

  /**
   * If this.scenarioStack is null, then reuse this node, by 'attaching' the scenarioStack. If this
   * node isStable, just return it (result doesn't depend on ScenarioStack anyway). If
   * this.scenarioStack is already set to something else, clone the Node, reset its state and attach
   * the given scenarioStack. Don't call this method directly - instead use
   * NodeTask#prepareForExecutionIn or PropertyNode#asKeyIn depending on whether you want a node to
   * execute or only to use as a key.
   */
  protected final NodeTask attachOrClone(ScenarioStack ss) throws CloneNotSupportedException {
    NodeTask result;
    if (_scenarioStack == null || _scenarioStack == ss) {
      result = this;
      result._scenarioStack = ss;
    } else if (isStable()) result = this;
    else {
      result = (NodeTask) super.clone();
      result.reset();
      result._scenarioStack = ss;
    }
    return result;
  }

  /** Marks node as not re-usable from cache, and ensures that it won't run anymore */
  public void invalidateCache() {
    updateState(STATE_INVALID_CACHE | STATE_NOT_RUNNABLE, STATE_DONE_FLAG);
    OGTrace.observer.invalidated(this);
  }

  public final boolean isInvalidCache() {
    return (_state & STATE_INVALID_CACHE) != 0;
  }

  public final boolean isCancelled() {
    // It's an invariant all over the place:
    // _scenarioStack != null and _scenarioStack.cancelScope != null
    return _scenarioStack.cancelScope().isCancelled();
  }

  private boolean isDoneWithCancellation(int state) {
    return (isDoneWithExceptionToHide(state)
        && (exception() instanceof ScopeWasCancelledException));
  }

  public final boolean isDoneWithCancellation() {
    return isDoneWithCancellation(_state);
  }

  /**
   * Used right after cloneTask to create new nodes from existing ones...
   *
   * <p>TODO (OPTIMUS-13315): Change to protected when TrackingScenario moves to optimus .graph Also
   * OPTIMUS-18757 Also, remove all the usage outside of cloneXXX
   */
  public /*g protected */ void reset() {
    // NOTE: currently _scenarioStack is not reset for ease of debugging
    //
    // if STATE_TRACKING_VALUE is set, we keep it, however it is cleared on complete, so it needs
    // explicit processing in reset overrides.
    state_h.setOpaque(this, STATE_NEW | (STATE_TRACKING_VALUE & _state));
    _xinfo = NullNodeExtendedInfo$.MODULE$;
    setLauncherRef(null);
    _waiters = null;
    setWaitingOn(null);
    setTweakInfection(null);
    setTweakPropertyDependency(TPDMask.empty);
    setProfile(null);
    // [SEE_INIT_CLONE_RESET_DESERIALIZE]
    if (DiagnosticSettings.traceAvailable) {
      setId(OGSchedulerContext.nextNodeId());
    }
  }

  /**
   * Used to avoid clone/reset for nodes that will not change their values Currently only
   * AlreadyCompletedNode(s).... and LazyPickledReference TweakNode.cloneWith assumes that any
   * isStable node is created with an attached scenario stack. TODO (OPTIMUS-13315): After changing
   * TrackingScenario update access to internal
   */
  public boolean isStable() {
    return false;
  }

  /**
   * Used to mark nodes that cannot be tracked by RTVerifierRerunner as they cannot be cloned for
   * rerun.
   */
  public boolean isClonable() {
    return true;
  }

  /**
   * Execution info, most of the time it's same as propertyInfo, but see TweakNode for an important
   * override
   */
  public NodeTaskInfo executionInfo() {
    return NodeTaskInfo.Default;
  }

  /** cached by system or by user ( e.g. Utrack) */
  public boolean effectivelyCacheable() {
    return executionInfo().getCacheable();
  }

  /** Completes the task. Use it for Task and not CompletableNode derived classes */
  protected final void complete(EvaluationQueue ec) {
    complete(ec, STATE_DONE);
  }

  /**
   * Every node that runs completes via this method. Alternatively look at
   * initAsCompleted/initAsFailed
   */
  protected final void complete(EvaluationQueue eq, int doneState) {
    try {
      // This must happen first - DO NOT MOVE!!!
      // Finalize book-keeping of this node, BEFORE consumers (aka parents) can read those stats
      OGTrace.completed(eq, this);

      // True if the node is being completed as the result of a cancellation.
      var isCancelled = isDoneWithCancellation(doneState);

      if (Settings.auditing && !executionInfo().isProxy()) AuditTrace.visit(this);

      int clearFlags = 0;
      if (isTrackingValue() && _scenarioStack.isTrackingIndividualTweakUsage()) {
        // [SEE_ON_TWEAK_USED] && [SEE_TRK_REPORT]
        // Why is it ok to not report when the node is cancelled?
        //
        // If we were cancelled (and are completing *due to the cancellation*) then we aren't
        // actually using the value of the tweak for our result (which is a cancellation exception).
        if (!isCancelled)
          _scenarioStack._tweakableListener().onTweakableNodeCompleted((PropertyNode<?>) this);
        clearFlags = STATE_TRACKING_VALUE; // No point of reporting twice!
      }

      if (!isCancelled) _xinfo.nodeCompleted(this);
      else _xinfo.nodeCancelled(this);

      // call tryUpdateState after OGTrace.completed so that anyone who uses our result
      // and therefore calls OGTrace.dependency will get accurate ANC or reused node timings
      int prevState = tryUpdateState(doneState, clearFlags); // STATE_DONE or STATE_DONE_FAILED

      // Don't allow result to be set twice, but do allow exceptions to be reset
      if (isDone(prevState))
        logAndDie(new GraphException("Cannot complete node twice!", this.exception()));

      executionInfo().updateDependsOnMask(this);

      if (DiagnosticSettings.awaitStacks && eq != null) eq.maybeSaveAwaitStack(this);

      /* Consider adding an assertion or even a permanent code to eq.setCurrentNode(null | nullTask) */
      notifyWaiters(eq);

      // Reset to the stable SS to save memory, UNLESS doing so would lose the CancellationScope in
      // a case where it matters for caching (see NCSupport.isUsableWRTCancellationScope)
      if (DiagnosticSettings.resetScenarioStackOnCompletion) {

        if (
        // If we have a result, we can always reuse it and we don't care about the cancel scope:
        isDoneWithUsableResult()
            // if we have a non rt exception, we can still do the swap provided that the
            // cancellation scope doesn't change when we do
            || (_scenarioStack.cancelScope() == _scenarioStack._stableSS().cancelScope())) {
          _scenarioStack = _scenarioStack._stableSS(); // [SEE_RESET_SS]
        }
      }
    } catch (Throwable e) {
      // If the above doesn't finish because of an error, we are probably heading for a bad
      // deadlock, so we panic and kill the process.
      logAndDie(e);
    }
  }

  private static boolean inLogAndDie = false;

  private void logAndDie(Throwable e) {
    synchronized (NodeTask.class) {
      if (inLogAndDie) {
        // Should not be possible, but since the point of this method is to catch things that should
        // be impossible....
        OGScheduler.log.error("logAndDie called recursively!");
        return;
      }
      inLogAndDie = true;
      Throwable wrapped =
          new GraphInInvalidState(
              "logAndDie for node "
                  + this.toPrettyName(true, false)
                  + " with state "
                  + this.stateAsString(),
              e);
      OGScheduler.log.error("OGScheduler: unexpected error, exiting (121).", wrapped);
      wrapped.printStackTrace();
      try {
        OGScheduler.log.error(GraphDiagnostics.getGraphState());
      } catch (Throwable t) {
        OGScheduler.log.error("Error dumping graph state!");
      }

      InfoDumper.graphPanic("logAndDie", schedulerVersion + ": unexpected error", 121, wrapped);
    }
  }

  /** This is called for every child node, whose result is used by the parent */
  public final void combineInfo(NodeTask child, EvaluationQueue eq) {
    OGTrace.dependency(this, child, eq);
    // If child took scenario dependency so this node must too
    int state = _state;
    int childState = child._state;
    if (!isScenarioDependent(state) && isScenarioDependent(childState)) markScenarioDependent();

    if (Settings.schedulerAsserts) {
      if ((eq instanceof OGSchedulerContext) && eq != OGSchedulerContext.current())
        throw new GraphInInvalidState("Scheduler context is per thread");

      boolean recordingAndNotProxy =
          scenarioStack().isRecordingTweakUsage() && !isDelayedProxy(this);
      if (recordingAndNotProxy && child.scenarioStack().isTrackingIndividualTweakUsage())
        throw new GraphInInvalidState("Should never combineInfo from tracked node into XS node");
    }

    // null += null == null and A += A == A
    if (child._xinfo != _xinfo) _xinfo = _xinfo.combine(child._xinfo, this);

    if (DiagnosticSettings.traceTweaksEnabled) combineDebugInfo(child);

    mergeTweakPropertyDependency(child);

    if (isTrackingValueNonXSOwner(childState) && _scenarioStack.isTrackingOrRecordingTweakUsage())
      child.reportTweakableUseBy(this); // [SEE_REC_REPORT]
  }

  private void combineDebugInfo(NodeTask child) {
    // [SEE_XS_TWEAK_INFECTION]
    if (OGTrace.observer.traceTweaks() && !child.isXScenarioOwner()) {
      SparseBitSet sbs = getTweakInfection();
      SparseBitSet csbs = child.getTweakInfection();
      if (sbs == null) setTweakInfection(csbs);
      else if (csbs != null) setTweakInfection(sbs.merge(csbs));
    }
  }

  /**
   * Report Tweak(s) and/or Tweakable(s) information to a parent
   * node.scenarioStack.tweakableListener In order not to double report for TrackingScenarioStack
   * nodes that execute will only report via onTweakableNodeCompleted All nodes, but tweaks and
   * tweakables will have nothing to report
   */
  protected void reportTweakableUseBy(NodeTask node) {
    throw new GraphInInvalidState();
  }

  /**
   * TODO (OPTIMUS-13315): Change to protected.
   *
   * <p>This method does a lot less than combineInfo, so be very careful when calling it!
   */
  public final void partialCombineInfo(NodeExtendedInfo childInfo) {
    // Invariants: null += null == null and A += A == A
    if (childInfo != _xinfo) _xinfo = _xinfo.combine(childInfo, this);
  }

  public final void setInfo(NodeExtendedInfo inf) {
    _xinfo = _xinfo.withInfo(inf);
    if (_xinfo == null) _xinfo = NullNodeExtendedInfo$.MODULE$;
  }

  /** TODO (OPTIMUS-13315): MUST become internal!!!! */
  public final void unsafeResetInfo() {
    _xinfo = NullNodeExtendedInfo$.MODULE$;
  }

  /** Returns misc. data associated with the result of the node */
  public final NodeExtendedInfo info() {
    return _xinfo.info();
  }

  /**
   * Returns exception if Node completes with exception Calling result before node completes is not
   * guaranteed to return anything meaningful
   */
  public Throwable exception() {
    return _xinfo.exception();
  }

  /**
   * Returns warnings associated with current node Calling result before node completes is not
   * guaranteed to return anything meaningful
   */
  public scala.collection.immutable.Set<Warning> warnings() {
    return _xinfo.warnings();
  }

  /** Attaches warning for the given calculation */
  public void attach(Warning warning) {
    _xinfo = _xinfo.withWarning(warning);
  }

  /** Complete the node with exception on cancelled CS. See where we override this function */
  public void cancelInsteadOfRun(EvaluationQueue eq) {
    var cs = _scenarioStack.cancelScope();
    var cause = cs.cancellationCauseToAbortNodeWith(_scenarioStack, this);
    completeWithException(cause, eq);
  }

  private static boolean exceptionIsTheSame(Throwable ex1, Throwable ex2) {
    if (ex1 == ex2) return true;
    return ex1 instanceof OptimusCancellationException
        && ex2 instanceof OptimusCancellationException;
  }

  /**
   * Mark Node as complete and set exception Note: _xinfo is not volatile update to _state in the
   * next step will MB
   */
  public void completeWithException(Throwable ex, EvaluationQueue ec) {
    if (ex == null) throw new GraphException("Exception cannot be null");
    // Next line allows for the same exception set twice to be silently ignored.
    // This works 'reliably' only when the ordering clearly exist, otherwise you have a race.
    // Specific use case is CircularReferenceException, which is set by Scheduler to break the cycle
    // and then will be attempted again by unwrapping of the nodes involved in cycle
    if (isDoneWithException() && exceptionIsTheSame(ex, this.exception())) return;
    recordNodeTrace(ex);
    CancellationScope cs = _scenarioStack.cancelScope();
    if (cs.cancelOnException()) cs.cancelInternal(ex);

    _xinfo = _xinfo.withException(ex);
    complete(ec, stateFlagsForThrowable(ex));
  }

  private static int stateFlagsForThrowable(Throwable ex) {
    // [EXPLANATION] why do we hide exceptions here? see docs/CompleteNodeWithHiddenException.md
    if ((ex instanceof ScopeWasCancelledException)
        || (Settings.hideNonRTExceptions && !RTList.isRT(ex))) return STATE_DONE_FAILED_HIDE;
    else return STATE_DONE_FAILED;
  }

  /**
   * Similar API to completeWithException, but will return silently if the node is already completed
   * Should be used to manually completed nodes and ONLY paired with other tryCompleteXXX Method
   * also takes a lock!
   */
  public boolean tryCompleteWithException(Throwable ex, EvaluationQueue ec) {
    if (ex == null) throw new GraphException("Exception cannot be null");
    recordNodeTrace(ex);
    if (isDone()) return false;
    synchronized (this) {
      if (isDone()) return false;
      _xinfo = _xinfo.withException(ex);
      complete(ec, STATE_DONE_FAILED);
      return true;
    }
  }

  /**
   * WeakHashMap used to maintain mapping from Throwable to wait chain on the node that originally
   * "caused" the exception. We maintain a map of strings, optionally constructed expensively when
   * the exception is first caught, as well as a map of arrays of classes, which is cheap to
   * construct and can be inflated to a somewhat less verbose string later if necessary.
   */
  private static final Map<Throwable, String> nodeTraceStringMap =
      Collections.synchronizedMap(new WeakHashMap<>());

  private static final Map<Throwable, ArrayList<StackTraceElement>> exceptionNodeTrace =
      Collections.synchronizedMap(new WeakHashMap<>());
  private static final Cache<Class<? extends NodeTask>, Integer> exceptionThrowerCache =
      CacheBuilder.newBuilder().maximumSize(Settings.exceptionThrowerCacheSize).build();

  // Construct a hopefully-useful node trace from the class info.  This is not as fancy as the one
  // you can generate with actual Node references.
  private static String simpleNodeTrace(ArrayList<StackTraceElement> cs) {
    if (cs != null)
      return IntStream.range(0, cs.size())
          .mapToObj(i -> i + ". " + cs.get(i))
          .collect(Collectors.joining("\n"));
    else return null;
  }

  /**
   * Used by ExceptionWithNodeTrace implicit object to effectively attach getNodeTrace method to any
   * Exception.
   */
  public static String getNodeTrace(Throwable ex) {
    if (ex == null) return null; // very defensive
    // First look for a pre-formatted verbose string.
    String nt = nodeTraceStringMap.get(ex);
    if (nt != null) return nt;
    else return simpleNodeTrace(exceptionNodeTrace.get(ex));
  }

  // Honestly, this is mostly for testing, since one would usually want the fanciest trace
  // available.
  public static String getSimpleNodeTrace(Throwable ex) {
    if (ex == null) return null;
    return simpleNodeTrace(exceptionNodeTrace.get(ex));
  }

  public static ArrayList<StackTraceElement> getNodeClassTrace(Throwable ex) {
    if (ex == null) return null;
    else return exceptionNodeTrace.get(ex);
  }

  private void recordNodeTrace(Throwable ex) {
    // these get thrown all of the time and don't indicate
    // a problem so we don't want to pollute the logs with them
    if (ex instanceof CancelOrphanedDependencyTrackerNodesException) return;

    Class<? extends NodeTask> cls = this.getClass();
    // Note: there's technically a race condition, but the worst case is that we slightly miscount
    // and record
    // a few more exceptions than we'd planned.
    Integer nThrown = exceptionThrowerCache.getIfPresent(cls);
    if (nThrown == null) nThrown = 0;
    else if (nThrown > Settings.maxTracedExceptionsPerClass) return;
    else if (nThrown == Settings.maxTracedExceptionsPerClass)
      OGScheduler.log.warn("No longer capturing node traces on exception for " + cls.getName());

    exceptionThrowerCache.put(cls, nThrown + 1);

    if (ex != null) {
      recordTerseNodeTrace(ex); // Always record terse trace.
      if (Settings.recordVerboseNodeTraceOnException) recordVerboseNodeTrace(ex);
    }
  }

  private void recordTerseNodeTrace(Throwable ex) {
    if (!exceptionNodeTrace.containsKey(ex)) {
      ArrayList<StackTraceElement> cs = new ArrayList<>();
      waitersToNodeStack(
          ntsk -> {
            cs.add(ntsk.stackTraceElem());
            return true;
          });
      exceptionNodeTrace.put(ex, cs);
    }
  }

  /** Record information about the Throwable, and the wait chain (call stack of nodes) */
  private void recordVerboseNodeTrace(Throwable ex) {
    if (!nodeTraceStringMap.containsKey(ex)) {
      StringBuilder sb = new StringBuilder();
      waitersToNodeStackPossiblyReducedFlag(new PrettyStringBuilder(sb));
      String nodeTrace = sb.toString();
      nodeTraceStringMap.put(ex, nodeTrace);
      if (Settings.dumpNodeTraceOnException && !(ex instanceof FlowControlException)) {
        OGScheduler.log.error("Node evaluation resulted in exception: " + ex + "\n" + nodeTrace);
      }
    }
  }

  /**
   * Executes the node.
   *
   * <p>This method will be called on an arbitrary scheduler thread. It will be called at most once
   * on a freshly created node (regardless of how many times that node was enqueued), and will
   * subsequently be called at most once after a call to markAsRunnable() and enqueue.
   *
   * <p>This means that node implementations can usually safely modify their own state during calls
   * to run() with no synchronization. <b>But note:</b> If you subscribe to another node using
   * child.continueWith(this) during your run() method, then run() may be called again concurrently
   * from another thread before the first call to run() exits (because the default onChildCompleted
   * calls markAsRunnable() and enqueue). For this reason you should typically only call
   * continueWith in tail position.
   *
   * <p>During calls to this method, EvaluationContext.current is equal to ec, and
   * EvaluationContext.currentNode is equal to this. That means that you can safely call
   * Node#enqueue or node$enqueued on other nodes and the ScenarioStack and enqueuer will propagate
   * automatically from this node.
   */
  public void run(OGSchedulerContext ec) {
    throw new UnsupportedOperationException(getClass().getName() + " does not support run()");
  }

  /**
   * Before we improve FSM to just use nodes, this will set continuation information It's public
   * because NodeFutureSystem is in optimus.core
   */
  public /* protected */ void setContinuation(NodeStateMachine k) {
    throw new GraphInInvalidState("called from the node that doesn't support FSM");
  }

  /**
   * Generic/untyped result n.b. if you call this from a node you almost certainly also need to call
   * yourNode.combineInfo(thisNode) otherwise we won't track your dependency on this node
   */
  public Object resultObject() {
    return null;
  }

  /**
   * Generic/untyped result as soon as result is available (or null if not available). Unlike
   * resultObject, this doesn't throw an exception if the node is incomplete, so you can use it to
   * observe the result of the node before the complete flag is set. Useful only for certain
   * diagnostic purposes.
   */
  public Object resultObjectEvenIfIncomplete() {
    return resultObject();
  }

  /**
   * Called BEFORE state is updated (so checking isDoneWithX won't work) but AFTER we know the node
   * is done
   */
  public final String resultAsStringEvenIfIncomplete() {
    Object res = resultObjectEvenIfIncomplete();
    if (res != null) return CoreHelpers.safeToString(res, null, 1000);
    else {
      res = exception();
      return (res != null)
          ? res.toString()
          : !executionInfo().isInternal() ? "[..." + stateAsString() + "]" : "";
    }
  }

  public final String resultAsString() {
    return resultAsString(1000);
  }

  public final String resultAsString(int maxChars) {
    if (isDoneWithException()) return exception().toString();
    else if (isDoneWithResult()) return CoreHelpers.safeToString(resultObject(), null, maxChars);
    return "[..." + stateAsString() + "]";
  }

  public final String resultAsStringOrElse(String defaultStr) {
    if (isDone()) return resultAsString();
    else return defaultStr;
  }

  /** For convenience of reporting, generic interceptor */
  public Object[] args() {
    return argsUnknown;
  }

  /** For convenience of reporting, generic interceptor */
  public Object methodThis() {
    return null;
  }

  /** For convenience of reporting, generic interceptor */
  public NodeName nodeName() {
    return NodeName.from(this);
  }

  public String nameAndSource() {
    return NodeName.nameAndSource(this);
  }

  /********************************************************************************
   * Set of dependency walkers
   * Basic visitor callback
   */
  public static class Visitor {
    public ArrayList<NodeTask> path;

    /** Return true to continue walk, false to stop */
    protected boolean visit(NodeTask ntsk, int depth) {
      return true; // Continue the walk
    }

    protected void visit(NodeCauseInfo info, int depth) {}

    protected boolean stopOnAnyFullStack() {
      return false;
    }

    protected boolean includeProxies() {
      return DiagnosticSettings.proxyInWaitChain;
    }

    protected boolean useLauncher() {
      return DiagnosticSettings.awaitStacks;
    }

    void reportCycle() {}
  }

  /** A classical DFS */
  public void forAllWaiters(Visitor vtor) {
    forAllWaiters(vtor, this);
  }

  /** Interprets the object 'o' as a waiter list and adds the waiters to the list 'q' */
  private static void unpackAndAdd(ArrayList<NodeCause> q, Object o) {
    if (o == null || o == completed) return;
    if (o instanceof NodeAwaiter nodeAwaiter) {
      var cause = nodeAwaiter.awaiter();
      if (cause != null) q.add(cause);
    } else if (o instanceof NWList wl) {
      while (wl != null) {
        var cause = wl.waiter.awaiter();
        if (cause != null) q.add(cause);
        wl = wl.prev;
      }
    } else if (Settings.schedulerAsserts)
      throw new GraphInInvalidState(); // This is just an integrity check
  }

  private static void forAllWaiters(Visitor vtor, NodeTask awaitedTask) {
    // In q we re-insert task and 'null' guard to mark 'back visit' [BACK_VISIT]
    // Visited list. In order to clean up the walking state. We can't walk again as the tree could
    // change under us
    ArrayList<NodeCause> q = new ArrayList<>(); // Queue of nodes to visit
    ArrayList<NodeTask> v = new ArrayList<>(64);
    ArrayList<NodeTask> stack = new ArrayList<>();
    vtor.path = stack;

    q.add(awaitedTask);
    _visitLock.lock();
    try {
      int depth = 0;
      while (!q.isEmpty()) {
        NodeCause cause = q.remove(q.size() - 1); // pop()
        if (cause == null) { // [BACK_VISIT]
          NodeTask pv = stack.remove(stack.size() - 1);
          pv.updateState(STATE_VISITED_BLACK, 0);
          depth--;
          continue;
        }
        if (cause instanceof NodeTask t) {
          int state = t._state; // volatile read
          if ((state & STATE_VISITED_BLACK) != 0) continue; // We are done with that node
          if ((state & STATE_VISITED_GRAY) != 0) {
            vtor.reportCycle();
            break;
          }
          var visible = vtor.includeProxies() || t.cacheUnderlyingNode() == t;
          if (visible && !vtor.visit(t, depth++)) {
            break;
          }
          t.updateState(STATE_VISITED_GRAY, 0);
          v.add(t);

          if (visible) {
            stack.add(t);
            q.add(null); // [BACK_VISIT]
          }

          int qSize = q.size();
          if (t._waiters == null && vtor.useLauncher()) unpackAndAdd(q, t.getLauncher());
          else unpackAndAdd(q, t._waiters);
          if (qSize == q.size() && vtor.stopOnAnyFullStack()) break;
        } else if (cause instanceof NodeCauseInfo causeInfo) {
          vtor.visit(causeInfo, depth);
        }
      }
    } finally {
      for (NodeTask t : v) t.updateState(0, STATE_VISITED_BLACK | STATE_VISITED_GRAY);
      _visitLock.unlock();
    }
  }

  /** Find and return any path to SI node */
  public ArrayList<NodeTask> nodeStackToSI() {
    return nodeStackToPropertyFlags(NodeTaskInfo.SCENARIOINDEPENDENT, 0);
  }

  /** Find and return any path to cache-able */
  public ArrayList<NodeTask> nodeStackToCacheable() {
    return nodeStackToPropertyFlags(0, NodeTaskInfo.DONT_CACHE);
  }

  public ArrayList<NodeTask> nodeStackToNode() {
    Visitor v =
        new Visitor() {
          @Override
          public boolean visit(NodeTask ntsk, int depth) {
            long flags = ntsk.executionInfo().snapFlags();
            if (((flags & NodeTaskInfo.NOTNODE) == 0) && !(ntsk instanceof TweakNode)) {
              path.add(ntsk);
              return false;
            }
            return true;
          }
        };
    forAllWaiters(v);
    return v.path;
  }

  private ArrayList<NodeTask> nodeStackToPropertyFlags(final long setFlags, final long clearFlags) {
    Visitor v =
        new Visitor() {
          @Override
          public boolean visit(NodeTask ntsk, int depth) {
            long flags = ntsk.executionInfo().snapFlags();
            if (((flags & setFlags) == setFlags) && ((flags & clearFlags) == 0)) {
              path.add(ntsk);
              return false;
            }
            return true;
          }
        };
    forAllWaiters(v);
    return v.path;
  }

  /** Find and return any path to start node */
  public ArrayList<NodeTask> nodeStackAny() {
    return nodeStackAny(DiagnosticSettings.proxyInWaitChain, null);
  }

  /**
   * Find and return any path to start node and allow to filter out delayed proxies up to a given
   * node task
   */
  public ArrayList<NodeTask> nodeStackAny(boolean proxyInWaitChain, NodeTask upTo) {
    Visitor v =
        new Visitor() {
          @Override
          protected boolean stopOnAnyFullStack() {
            return true;
          }

          @Override
          protected boolean visit(NodeTask ntsk, int depth) {
            return upTo == null || ntsk.cacheUnderlyingNode() != upTo.cacheUnderlyingNode();
          }
        };
    forAllWaiters(v);
    return proxyInWaitChain ? v.path : NCSupport.removeProxies(v.path);
  }

  /**
   * Note: Wait chains form a tree and this code takes only 1 branch of that tree
   *
   * @return Currently registered wait chain
   */
  public final ArrayList<DebugWaitChainItem> dbgWaitChain(boolean includeProxies) {
    ArrayList<DebugWaitChainItem> a = new ArrayList<>();
    addToWaitChain(a, includeProxies);
    return a;
  }

  /**
   * Walk the wait chain, including waiters like SyncFrames. If there are no waiters which can be
   * added to the chain, use the enqueuer. Note: Wait chains form a tree and this code takes only 1
   * branch of that tree
   */
  @Override
  public final void addToWaitChain(ArrayList<DebugWaitChainItem> appendTo, boolean includeProxies) {
    if (includeProxies
        || appendTo.isEmpty()
        || cacheUnderlyingNode() != appendTo.get(appendTo.size() - 1)) appendTo.add(this);

    Object o = _waiters;
    if (o instanceof DebugWaitChainItem dwci) dwci.addToWaitChain(appendTo, includeProxies);
    else if (o instanceof NWList olist && olist.waiter instanceof DebugWaitChainItem dwci)
      dwci.addToWaitChain(appendTo, includeProxies);
    else if (getLauncher() instanceof DebugWaitChainItem dwci)
      dwci.addToWaitChain(appendTo, includeProxies);
  }

  /**
   * Returns entire forward chain. It useful in the debugger<br>
   * Using floyd algorithm to protect and detect cycles
   */
  public ArrayListWithMarker<NodeTask> visitForwardChain() {
    return visitForwardChain(Integer.MAX_VALUE);
  }

  public ArrayListWithMarker<NodeTask> visitForwardChain(int limit) {
    ArrayListWithMarker<NodeTask> v = new ArrayListWithMarker<>(64, -1);
    NodeTask tsk = this; // last task in the chain of the awaitOn...
    NodeTask fastWalker = this;

    do {
      if (v.size() > limit) {
        tsk = null;
        break;
      }
      v.add(tsk);
      tsk = tsk.getWaitingOn();
      if (fastWalker != null) {
        fastWalker = fastWalker.getWaitingOn();
        if (fastWalker != null) fastWalker = fastWalker.getWaitingOn();
      }
    } while (tsk != null && tsk != fastWalker);

    boolean cycleDetected = tsk != null;
    if (cycleDetected) {
      // We do have a cycle...
      int startOfCycle = 0;
      v.add(tsk);
      tsk = this;
      while (tsk != fastWalker) {
        tsk = tsk.getWaitingOn();
        fastWalker = fastWalker.getWaitingOn();
        startOfCycle++;
        // the waitingOn chain has been broken! this means another scheduler resolved this cycle
        // while we were visiting,
        // so we have nothing more to do
        if (tsk == null || fastWalker == null) return v;
      }
      v.marker = startOfCycle;
      v.shrinkTo(startOfCycle + 1);

      fastWalker = tsk.getWaitingOn();
      while (tsk != fastWalker) {
        v.add(fastWalker);
        fastWalker = fastWalker.getWaitingOn();
        if (fastWalker == null) {
          // the waitingOn chain has been broken! this means another scheduler resolved this cycle
          // while we were
          // visiting it. we need to reset the marker so the caller knows there is no cycle
          v.marker = -1;
          return v;
        }
      }
      v.add(tsk);
    }
    return v;
  }

  boolean fixWaiterChainForXSFTFromBatcher(EvaluationQueue eq) {
    final boolean[] rerunProxy = {false};

    forAllWaiters(
        new Visitor() {
          @Override
          protected boolean includeProxies() {
            return true;
          }

          @Override
          protected boolean useLauncher() {
            return false;
          }

          @Override
          protected boolean visit(NodeTask ntsk, int depth) {
            rerunProxy[0] |= ntsk.breakProxyChains(eq);
            return true;
          }
        });

    return rerunProxy[0];
  }

  /**
   * Detects cycles and breaks them by first trying to break XSFT cycles with broken dependency mask
   * assumptions otherwise completing the node with CircularReferenceException
   */
  void detectCyclesAndRecover(EvaluationQueue eq) {
    try {
      // Currently we need the lock because multiple thread could be trying to run this algo and
      // breaking the cycle
      // will result in NPEs
      _visitLock.lock();
      // check again under lock (previous check was outside of lock in detectCycles)
      if (isDone()) return;
      ArrayListWithMarker<NodeTask> chain = visitForwardChain();
      if (chain.marker >= 0) {
        // collect stack for reporting later
        var sb = new PrettyStringBuilder();
        sb.simpleName_$eq(true);
        WaitingChainWriter.writeWaitingChain(this, sb);
        // Collect potentially a better mask, just in case some node can recover using that mask
        TPDMask mask = new TPDMask();
        for (int i = chain.marker; i < chain.size(); i++) {
          NodeTask tsk = chain.get(i);
          tsk.mergeTweakPropertyDependenciesInto(mask);
        }

        boolean cycleIsUnrecoverable = true;
        // Note: 'breaking' the chain as close as possible to the awaitedNode
        // but should not be a proxy. Proxies state could much more complicated.
        // Alternatively, it's possible to invoke something like cancelInsteadOfRun()
        var nodeToCompleteWithException = chain.get(chain.marker);
        var nodeToCompleteIsProxy = isProxy(nodeToCompleteWithException);
        for (int i = chain.marker; i < chain.size(); i++) {
          var tsk = chain.get(i);
          if (nodeToCompleteIsProxy && !isProxy(tsk)) {
            nodeToCompleteWithException = tsk;
            nodeToCompleteIsProxy = false;
          }
          if (tsk.tryToRecoverFromCycle(mask, eq)) { // Note: this takes additional locks
            cycleIsUnrecoverable = false;
            OGScheduler.log.warn("Recovering from XSFT cycle");
            MonitoringBreadcrumbs$.MODULE$.sendXSFTCycleRecoveryCrumb(tsk, sb.toString());
            break;
          }
        }
        if (cycleIsUnrecoverable) {
          Collections.reverse(chain); // Want to present cycle as a node stack, so reverse....
          // in case anyone else walks the forward chain of this node
          nodeToCompleteWithException.setWaitingOn(null);
          nodeToCompleteWithException.completeWithException(
              new CircularReferenceException(chain), eq);
        }
      }
    } finally {
      _visitLock.unlock();
    }
  }

  private void waitersToNodeStackPossiblyReducedFlag(final PrettyStringBuilder sb) {
    if (Settings.fullNodeTraceOnException) {
      waitersToFullMultilineNodeStack(false, sb);
    } else {
      PrettyStringBuilder psb = new PrettyStringBuilder();
      NodeStackVisitor nsv = waitersToNodeStack(false, psb, false, -1);
      sb.appendln(
          "Reduced Node Stack (single path shown - log full paths with -Doptimus.scheduler"
              + ".fullNodeTraceOnException=true) total nodes awaiting="
              + nsv.nodes
              + ", distinct paths="
              + nsv.paths
              + ", max path depth="
              + nsv.maxDepth);
      sb.appendln(psb);
    }
  }

  public String enqueuerChain() {
    return String.join(" <- ", AwaitStackManagement.awaitStack(this));
  }

  public String simpleChain() {
    try {
      PrettyStringBuilder sb = new PrettyStringBuilder();
      appendSimpleChain(sb);
      return sb.toString();
    } catch (Throwable t) {
      return "Error in simpleChain: " + t;
    }
  }

  private void appendSimpleChain(PrettyStringBuilder sb) {
    NodeTask tsk = this;
    while (tsk != null) {
      tsk.writePrettyString(sb);
      Awaitable enq = tsk.getLauncher();
      if (Objects.nonNull(enq) && enq instanceof NodeTask) tsk = (NodeTask) enq;
      else {
        Object o = tsk._waiters;
        if (o == null || o == completed) return;
        if (o instanceof NWList nw) {
          while (!(o instanceof NodeTask) && nw != null) {
            o = nw.waiter;
            nw = nw.prev;
          }
        }
        if (o instanceof NodeTask) {
          tsk = (NodeTask) o;
        } else tsk = null;
      }
      if (Objects.nonNull(tsk)) sb.append("<-");
    }
  }

  public String waitersToFullMultilineNodeStack() {
    return waitersToNodeStack(false, true, false, -1);
  }

  public String waitersToNodeStack(
      final boolean enableDangerousParamPrinting,
      final boolean fullNodeStack,
      final boolean singleLine,
      final int maxLength) {
    PrettyStringBuilder psb = new PrettyStringBuilder();
    waitersToNodeStack(enableDangerousParamPrinting, psb, fullNodeStack, maxLength);
    String ret = psb.toString();
    if (singleLine) ret = ret.replace("\n", "; ");
    return ret;
  }

  void appendNodeStack(final ArrayList<NodeTask> list) {
    NodeStackVisitor nsv = new NodeStackVisitor(false, null, list, null, false, 50);
    forAllWaiters(nsv);
  }

  private void waitersToNodeStack(Function<NodeTask, Boolean> consumeAndContinue) {
    NodeStackVisitor nsv =
        new NodeStackVisitor(
            false, null, null, consumeAndContinue, Settings.fullNodeTraceOnException, -1);
    forAllWaiters(nsv);
  }

  public PrettyStringBuilder waitersToFullMultilineNodeStack(
      final boolean enableDangerousParamPrinting, final PrettyStringBuilder sb) {
    return waitersToNodeStack(enableDangerousParamPrinting, sb, true, -1).sb;
  }

  private String stackLineName() {
    String nodeClassName = this.toPrettyName(false, false);
    String nodeEntityName = this.toPrettyName(true, false);
    // show the actual entity name, not the name of the class/trait containing the node, if they are
    // different
    String distinctEntityName =
        nodeClassName != null && !nodeClassName.equals(nodeEntityName)
            ? " [" + nodeEntityName + "]"
            : "";
    return nodeClassName + distinctEntityName;
  }

  public String stackLine(boolean addState) {
    var ste = this.stackTraceElem();
    String ret = stackLineName() + " - (" + ste.getFileName() + ":" + ste.getLineNumber() + ")";
    if (addState) ret += this.stateAsString();
    return ret;
  }

  static class NodeStackVisitor extends Visitor {
    final boolean enableDangerousParamPrinting;
    final PrettyStringBuilder sb;
    final boolean fullNodeStack;
    ArrayList<NodeTask> list;
    Function<NodeTask, Boolean> consumeAndContinue;

    int expectedDepth = 0;
    int maxDepth = 0;
    int paths = 1;
    int nodes = 0;
    int maxLength;

    boolean skipRest = false;

    private NodeStackVisitor(
        boolean enableDangerousParamPrinting,
        PrettyStringBuilder sb,
        ArrayList<NodeTask> list,
        Function<NodeTask, Boolean> consumeAndContinue,
        boolean fullNodeStack,
        int maxLength) {
      this.maxLength = maxLength;
      this.enableDangerousParamPrinting = enableDangerousParamPrinting;
      this.sb = sb;
      this.list = list;
      this.consumeAndContinue = consumeAndContinue;
      this.fullNodeStack = fullNodeStack;
    }

    @Override
    public boolean visit(NodeTask ntsk, int depth) {
      if (expectedDepth != depth) {
        paths++;
        skipRest = true;
      }
      expectedDepth = depth + 1;
      if (depth > maxDepth) maxDepth = depth;

      if (!fullNodeStack && skipRest) return true;

      nodes++;
      if (maxLength > 0 && nodes > maxLength) return false;

      if (list != null) {
        list.add(ntsk);
      }

      if (sb != null) {
        scala.collection.Iterator<optimus.platform.PluginTagKeyValue<?>> pluginTags =
            ntsk.scenarioStack().pluginTags().iterator();

        String postfix = "";

        while (pluginTags.hasNext()) {
          Object pluginInfo = pluginTags.next().value();
          if (pluginInfo instanceof PluginNodeTraceInfo info) {
            if (info.node() == ntsk) {
              postfix = " :: " + info.info();
              break;
            }
          }
        }
        postfix = postfix.replace(System.lineSeparator(), System.lineSeparator() + '\t');

        sb.append(String.format("%3d. ", depth));
        sb.appendln(ntsk.stackLine(false) + ntsk.stateAsString() + postfix);

        if (enableDangerousParamPrinting) {
          Object[] cargs = ntsk.args();
          if (cargs != null) {
            for (Object arg : cargs) {
              sb.append("      ");
              sb.appendln(arg);
            }
          }
        }
      }

      if (consumeAndContinue != null) return consumeAndContinue.apply(ntsk);
      return true; // Continue
    }

    @Override
    protected void visit(NodeCauseInfo info, int depth) {
      if (!skipRest) {
        if (sb != null) {
          var details = info.details();
          if (!details.isEmpty()) {
            sb.indent();
            sb.appendln(details);
            sb.unIndent();
          }
        }
      }
    }
  }

  NodeStackVisitor waitersToNodeStack(
      final boolean enableDangerousParamPrinting,
      final PrettyStringBuilder sb,
      final boolean fullNodeStack,
      int maxLength) {
    NodeStackVisitor nsv =
        new NodeStackVisitor(
            enableDangerousParamPrinting, sb, null, null, fullNodeStack, maxLength);
    forAllWaiters(nsv);
    return nsv;
  }

  void adjustAfterDeserialization() {
    if (!isDone()) {
      updateState(0, STATE_RUN_MASK);
    } else _waiters = completed;
  }

  @Serial
  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    adjustAfterDeserialization();
    // [SEE_INIT_CLONE_RESET_DESERIALIZE]
    if (DiagnosticSettings.traceAvailable) {
      setId(OGSchedulerContext.nextNodeId());
    }
  }

  /**
   * Used by priority queue in scheduler to define nodes that should wait for other work to be
   * completed. See NodeBatcher for concrete use case.
   */
  public int priority() {
    return 0;
  }

  /** If available report NodeTask specific stall information for logging otherwise null. */
  final Option<GraphStallInfo> stallInfo() {
    if (executionInfo().shouldLookupPlugin()) {
      // if we have extra data available at NodeTask level, we don't need to print unknown reason
      // for stalling from underlying plugin
      return getPlugin().graphStallInfoForTask(this.executionInfo(), this);
    } else return Option.empty();
  }

  /* BEGIN PROFILE HELPERS ****************************************************************************** */

  @SuppressWarnings(
      "unused") // _Profile field is injected by entityagent, accessors are updated to use it
  public final void setProfile(PNodeTask task) {}

  /**
   * use this over accessProfile where you don't need the side-effect of recording PNodeTask. Must
   * check for null!
   */
  public final PNodeTask getProfile() {
    return null;
  }

  @SuppressWarnings(
      "unused") // _SelfPlusANCTime field is injected by entityagent, accessors are updated to use
  // it
  public final void setSelfPlusANCTime(long time) {}

  public final long getSelfPlusANCTime() {
    return 0;
  }

  @SuppressWarnings(
      "unused") // _TweakInfection field is injected by entityagent, accessors are updated to use it
  public final void setTweakInfection(SparseBitSet tweakInfection) {}

  public final SparseBitSet getTweakInfection() {
    return null;
  }

  /** Request weak references to enqueuer. */
  public static final int WEAK_LAUNCHER_REF = 1;

  /**
   * Keeps track of the node that enqueued this one. Note that a node can have an enqueuer even if
   * it has no awaiters. For example, if a, b, c, are nodes and c = a + b, then while a is
   * executing, b has no awaiters but its enqueuer is c.
   *
   * <p>The field is cleared when the node completes or is invalidated.
   */
  @Override
  public final void setLaunchData(Awaitable ntsk, long hash, int implFlags) {
    // In OGTrace.enqueue, we made sure that we aren't setting enqueuers due to a simple
    // completion callback. So at this point either we are the first enqueue OR we were re-enqueued
    // from somewhere else after being pulled out of the cache.  In both cases we want to record the
    // enqueueing.

    // There is a race where we set the enqueuer and then the stack hash (or the other way around)
    // while the async stack trace gets computed. This is fine, we protect against it in
    // AwaitStackManagement.
    setLauncherStackHash(hash);
    if ((implFlags & WEAK_LAUNCHER_REF) != 0) {
      setLauncherRef(new WeakReference<>(ntsk, null));
    } else {
      setLauncherRef(ntsk);
    }
  }

  // _LauncherRef and _LauncherStackHash fields are injected by entityagent.
  @SuppressWarnings("unused")
  private void setLauncherRef(Object ntsk) {}

  private Object getLauncherRef() {
    return null;
  }

  @SuppressWarnings("unused")
  private void setLauncherStackHash(long hash) {}

  public final Awaitable getLauncher() {
    Object obj = getLauncherRef();
    return (Awaitable) ((obj instanceof WeakReference<?>) ? ((WeakReference<?>) obj).get() : obj);
  }

  public final long getLauncherStackHash() {
    return 0;
  }

  // Tweak Property Dependency Mask (part 0) [SEE_MASK_SUPPORT_GENERATION]
  // Note: if -Doptimus.graph.tweakUsageQWords=0, these fields will be removed by entityagent
  private long _tpd0;
  private long _tpd1;

  /* This function could be re-written by entityagent [SEE_MASK_SUPPORT_GENERATION] */
  public final void setTweakPropertyDependency(TPDMask mask) {
    _tpd0 = mask.m0;
    _tpd1 = mask.m1;
  }

  /* This function could be re-written by entityagent [SEE_MASK_SUPPORT_GENERATION] */
  private void mergeTweakPropertyDependency(NodeTask child) {
    _tpd0 |= child._tpd0;
    _tpd1 |= child._tpd1;
  }

  /* This function could be re-written by entityagent [SEE_MASK_SUPPORT_GENERATION] */
  final void mergeTweakPropertyDependenciesInto(TPDMask mask) {
    mask.m0 |= _tpd0;
    mask.m1 |= _tpd1;
  }

  /* Inline version of TPDMask.subsetOf and could be re-written by entityagent [SEE_MASK_SUPPORT_GENERATION] */
  final boolean isTweakPropertyDependencySubsetOf(TPDMask mask) {
    return (mask.m0 | _tpd0) == mask.m0 && (mask.m1 | _tpd1) == mask.m1;
  }

  /* This function could be re-written by entityagent [SEE_MASK_SUPPORT_GENERATION] */
  public final boolean tweakPropertyDependenciesIntersect(TPDMask mask) {
    return (_tpd0 & mask.m0) != 0 || (_tpd1 & mask.m1) != 0;
  }

  public final void addSelfPlusANCTime(long time) {
    setSelfPlusANCTime(getSelfPlusANCTime() + time);
  }

  /**
   * ID of the type of the node entityagent overrides this for anonymous nodes, which share
   * executionInfo
   */
  public int getProfileId() {
    return executionInfo().profile;
  }

  /** ID of the instance of the node the field is injected by entityagent */
  @Override
  public final int getId() {
    return 0;
  }

  /** ID of the instance of the node the field is injected by entityagent */
  @SuppressWarnings("unused")
  public final void setId(int id) {}

  NodeTask() {
    // [SEE_INIT_CLONE_RESET_DESERIALIZE]
    if (DiagnosticSettings.traceAvailable) {
      setId(OGSchedulerContext.nextNodeId());
    }
  }

  /**
   * Field is used by graph debugger/profiler and injected at runtime if flags are set accordingly
   * TODO (OPTIMUS-13315): Allocate lazily? Pool? Per Thread Pool? etc...
   *
   * <p>TODO (OPTIMUS-13315): Just store id and allocated buffers with data elsewhere
   *
   * <p>TODO (OPTIMUS-13315): Make callers / callees optional?
   *
   * <p>The reason for all those forwarders/helpers it to avoid dependency on exact location of
   * Profile.NodeTask data WARNING! these getters should use getProfile, not NodeTrace.accessProfile
   */
  public final long getFirstStartTime() {
    return getProfile() == null ? 0 : getProfile().firstStartTime;
  }

  public final long getCompletedTime() {
    return getProfile() == null ? 0 : getProfile().completedTime;
  }

  public final long getWallTime() {
    return getProfile() == null ? 0 : getProfile().wallTime();
  }

  public final long getSelfTime() {
    return getProfile() == null ? 0 : getProfile().selfTime;
  }

  public final Iterator<PNodeTask> getCallees() {
    return getProfile() == null ? EdgeList.empty.iterator() : getProfile().getCallees();
  }

  public final ArrayList<PNodeTask> getCallers() {
    return getProfile() == null ? EdgeList.empty : getProfile().getCallers();
  }

  /**
   * Node is traceable if it is marked to be traced or one of its children requested parents to be
   * traced [SEE_PARTIAL_TRACE]
   */
  final boolean isIncludedInTrace() {
    return executionInfo().isTraceSelfAndParent()
        || NodeTrace.accessProfile(this).traceSelfAndParents;
  }

  @Override
  public final Option<String> getAppId() {
    return RuntimeEnvironment.getAppId(this);
  }

  @Override
  public final Option<String> tagString() {
    return GridProfiler.tagStringForNode(this);
  }

  /* END OF PROFILE HELPERS ****************************************************************************** */

  @Override
  public final ChainedID ID() {
    return _scenarioStack.getTrackingNodeID();
  }

  public final int profileBlockID() {
    return _scenarioStack.profileBlockID();
  }

  private static class NullTask extends NodeTask {
    NullTask() {
      initAsCompleted();
    }

    @Override
    public boolean isStable() {
      return true;
    }
  }

  /**
   * Let's say during a run you want to switch to a different scenario stack. You have to switch the
   * node you are running, because current scenarioStack is looked up as
   * EC.currentNode.scenarioStack Use this Node to set as current and the rest of the infra where
   * needed (hopefully not a lot) Will adjust for this special case
   */
  public static class ScenarioStackNullTask extends NodeTask {
    public NodeTask orgTask;

    public ScenarioStackNullTask(NodeTask orgTask) {
      this.orgTask = orgTask;
      this.replace(orgTask._scenarioStack);
    }

    @Override
    public boolean isStable() {
      return true;
    }
  }
}
