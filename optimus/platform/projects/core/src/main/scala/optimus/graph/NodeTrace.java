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

import static optimus.graph.NodeTaskInfo.configureCachePolicy;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;

import optimus.core.CoreUtils;
import optimus.core.TPDMask;
import optimus.graph.cache.NCPolicy;
import optimus.graph.cache.NCSupport;
import optimus.graph.diagnostics.PNodeInvalidate;
import optimus.graph.diagnostics.PNodeTask;
import optimus.graph.diagnostics.PNodeTaskInfo;
import optimus.graph.diagnostics.PNodeTaskLive;
import optimus.graph.diagnostics.WaitProfile;
import optimus.graph.diagnostics.trace.OGEventsObserver;
import optimus.graph.diagnostics.trace.OGTraceMode;
import optimus.platform.inputs.GraphInputConfiguration$;
import optimus.platform.util.PrettyStringBuilder;
import scala.Function0;

/**
 * Class with static methods to assist graph code in *collecting* node trace information for
 * profiling and/or for debugging
 */
public class NodeTrace {
  /** All seen nodes synchronized (avoid making copies or even locks) */
  private static final ArrayList<PNodeTaskLive> trace = new ArrayList<>();
  // Really needs to be ReadonlyArrayList
  public static final ArrayList<PNodeTask> empty = new ArrayList<>();

  private static final int N_TASK_INFOS = 1024;
  private static NodeTaskInfo[] taskInfos = new NodeTaskInfo[N_TASK_INFOS];
  private static final Object taskInfosLock = new Object();

  /**
   * use this if the taskInfos could be changing underneath you - ie when accessing the list without
   * locking)
   */
  public static NodeTaskInfo[] taskInfos() {
    synchronized (taskInfosLock) {
      return taskInfos.clone();
    }
  }

  public static void TEST_ONLY_clearTaskInfos() {
    synchronized (taskInfosLock) {
      taskInfos = new NodeTaskInfo[N_TASK_INFOS];
    }
  }

  /** always take the lock on registration */
  static int register(NodeTaskInfo info, boolean register) {
    if (!register) return 0; // PROFILE_ID_INVALID
    int id = NodeTaskInfoRegistry.nextId();
    return register(info, id);
  }

  static int registerWithLinkUp(NodeTaskInfo info) {
    var runtimeClass = info.runtimeClass();
    var className = runtimeClass != null ? runtimeClass.getName() : null;
    int id = NodeTaskInfoRegistry.allocateId(className, info.name());
    return register(info, id);
  }

  private static int register(NodeTaskInfo info, int id) {
    synchronized (taskInfosLock) {
      if (id >= taskInfos.length)
        taskInfos = Arrays.copyOf(taskInfos, Math.max(id + 1, taskInfos.length + N_TASK_INFOS));
      taskInfos[id] = info;
    }
    return id;
  }

  /**
   * Used by PropertyNodeLoom derived classed. Note: property id is resolved at link up time, no
   * checking is needed. Note: likely to replace "burning-in" id with propertyInfo directly
   */
  public static NodeTaskInfo forID(int propertyID) {
    return taskInfos[propertyID];
  }

  static void registerTweakablePropertiesWithOGTrace() {
    synchronized (taskInfosLock) {
      for (NodeTaskInfo info : taskInfos) {
        if (info != null && info.isTweakable())
          OGTrace.trace.ensureProfileRecorded(info.profile, info);
      }
    }
  }

  static void undoExternalCacheConfig() {
    synchronized (taskInfosLock) {
      for (NodeTaskInfo info : taskInfos) {
        if (info != null) info.undoExternalCacheConfig();
      }
    }
  }

  static void TEST_ONLY_clearBackupConfig() {
    synchronized (taskInfosLock) {
      for (NodeTaskInfo info : taskInfos) {
        if (info != null) info.TEST_ONLY_clearBackupConfiguration();
      }
    }
  }

  private static String dependsOnTweakableString(long[] mask) {
    List<Integer> ids = CoreUtils.maskToIndexList(mask);
    PrettyStringBuilder sb = new PrettyStringBuilder();
    sb.append("[").append(ids, NodeTrace::nonNullName, ", ").append("]");
    return sb.toString();
  }

  public static String dependsOnTweakableString(NodeTaskInfo nti) {
    return dependsOnTweakableString(nti.dependsOnTweakMask().toArray());
  }

  // blakedav - this isn't reliable when id > qword*64 (it wraps around)
  public static String dependsOnTweakableString(NodeTask tsk) {
    TPDMask mask = new TPDMask();
    tsk.mergeTweakPropertyDependenciesInto(mask); // Copy bits
    return dependsOnTweakableString(mask.toArray());
  }

  public static String dependsOnTweakableMask(NodeTask tsk) {
    TPDMask mask = new TPDMask();
    tsk.mergeTweakPropertyDependenciesInto(mask); // Copy bits
    return mask.stringEncoded();
  }

  public static String dependsOnTweakableMaskFixedWidth(NodeTask tsk) {
    TPDMask mask = new TPDMask();
    tsk.mergeTweakPropertyDependenciesInto(mask); // Copy bits
    return mask.stringEncodedFixedWidth();
  }

  public static int dependsOnTweakableMaskBitCount(NodeTask tsk) {
    TPDMask mask = new TPDMask();
    tsk.mergeTweakPropertyDependenciesInto(mask); // Copy bits
    return mask.countOfSetBits();
  }

  public static boolean dependsOnIsPoison(NodeTask tsk) {
    TPDMask mask = new TPDMask();
    tsk.mergeTweakPropertyDependenciesInto(mask); // Copy bits
    return mask.allBitsAreSet();
  }

  private static String nonNullName(int tweakableID) {
    NodeTaskInfo n = findWithTweakableID(tweakableID);
    return n == null ? "[n/a]" : n.name();
  }

  private static NodeTaskInfo findWithTweakableID(int tweakableID) {
    for (NodeTaskInfo info : taskInfos()) {
      if (info != null && info.tweakableID() == tweakableID) return info;
    }
    return null;
  }

  private static void infoLog(NCPolicy newPolicy) {
    if (newPolicy != null)
      OGTrace.log.javaLogger().info("Setting all eligible cache policies to " + newPolicy);
    else OGTrace.log.javaLogger().info("Reverting all policies to backup");
  }

  /**
   * Set new policy only if the previous policy on the nti matches the filterPolicy. If newPolicy is
   * null, revert to backup, else default.
   *
   * @param filterPolicy -- previous policy to match on (if null, apply to all)
   * @param newPolicy -- new policy to set (if null, revert to backup if there was one, and
   *     otherwise default Note: always runs under taskInfosLock to avoid race in NTI registration
   *     (see comment on disableXSFT)
   */
  @Deprecated
  public static void setAllCachePoliciesTo(NCPolicy filterPolicy, NCPolicy newPolicy) {
    infoLog(newPolicy);
    synchronized (taskInfosLock) {
      for (NodeTaskInfo info : taskInfos) {
        configureCachePolicy(
            info,
            filterPolicy,
            newPolicy); // extracted for testing without bulk-enabling a policy on all NTIs
      }
    }
  }

  public static NodeTaskInfo[] findAllTweakables() {
    return findAllMatching(nti -> nti.tweakableID() > 0, false);
  }

  @SuppressWarnings("unused")
  public static NodeTaskInfo[] findAllWithDependencies() {
    return findAllMatching(nti -> nti.dependsOnTweakMask().nonEmpty(), false);
  }

  public static void resetWasTweakedForNTIs(NodeTaskInfoFilter filter) {
    NodeTaskInfo[] ntis = findAllMatching(filter, false);
    for (NodeTaskInfo nti : ntis) {
      nti.DEBUG_ONLY_markNeverInstanceTweaked();
      nti.DEBUG_ONLY_markNeverPropertyTweaked();
    }
  }

  /* for tests */
  public static NodeTaskInfo[] findConstructorNodeFor(String name) {
    String constructorName = name + ConstructorNode$.MODULE$.suffix();
    return findAllMatching(nti -> nti.name().equals(constructorName), false);
  }

  /* sort = true used from debugger */
  private static NodeTaskInfo[] findAllMatching(
      NodeTaskInfoFilter filter, @SuppressWarnings("SameParameterValue") boolean sort) {
    ArrayList<NodeTaskInfo> r = new ArrayList<>();
    for (NodeTaskInfo info : taskInfos()) {
      if (info != null && filter.match(info)) r.add(info);
    }
    if (sort) r.sort(Comparator.comparing(NodeTaskInfo::toString));
    return r.toArray(new NodeTaskInfo[0]);
  }

  @SuppressWarnings("unchecked")
  public static ArrayList<PNodeTask> getTrace() {
    synchronized (trace) {
      return (ArrayList<PNodeTask>) trace.clone();
    }
  }

  /** Try to use it only in debug sessions */
  public static ArrayList<NodeTask> getTraceLive() {
    ArrayList<NodeTask> r = new ArrayList<>();
    synchronized (trace) {
      for (PNodeTask task : trace) {
        r.add(task.getTask());
      }
      return r;
    }
  }

  /** Returns true if there are no nodes recorded in trace */
  public static boolean getTraceIsEmpty() {
    return trace.isEmpty();
  }

  public static ArrayList<PNodeTask> getTrace(NodeTaskInfo nti) {
    return getTraceBy(ntsk -> (ntsk.executionInfo().equals(nti)), true);
  }

  public static ArrayList<PNodeTask> getTrace(boolean includeProxies) {
    return getTraceBy(p -> true, includeProxies);
  }

  public static ArrayList<PNodeTask> getTrace(PNodeTaskInfo pnti) {
    return getTrace(pnti, true);
  }

  public static ArrayList<PNodeTask> getTrace(PNodeTaskInfo pnti, boolean includeProxies) {
    return getTraceBy(ntsk -> (ntsk.getProfileId() == pnti.id), includeProxies);
  }

  public static ArrayList<PNodeTask> getTraceBy(
      Predicate<? super NodeTask> predicate, boolean includeProxies) {
    return getTraceBy(predicate, includeProxies, Long.MAX_VALUE);
  }

  public static ArrayList<PNodeTask> getTraceBy(
      Predicate<? super NodeTask> predicate, boolean includeProxies, long max) {
    ArrayList<PNodeTask> r = new ArrayList<>();
    long i = 0;
    synchronized (trace) {
      for (PNodeTask task : trace) {
        NodeTask nodeTask = task.getTask();
        if ((includeProxies || !NCSupport.isDelayedProxy(nodeTask)) && predicate.test(nodeTask)) {
          r.add(task);
          i += 1;
          if (i > max) return r;
        }
      }
    }
    return r;
  }

  public static void visitTraces(Predicate<? super NodeTask> visitAndContinue) {
    synchronized (trace) {
      for (PNodeTask task : trace) {
        if (!visitAndContinue.test(task.getTask())) return;
      }
    }
  }

  /**
   * User code can call this API from anywhere to mark current node and its parents to be traced
   * Also in debugger console intercepted code can call this API [SEE_PARTIAL_TRACE]
   */
  @SuppressWarnings("unused")
  public static void markCurrentNodeAndParentsToBeTraced() {
    markNodeAndParentsToBeTraced(OGSchedulerContext.current().getCurrentNodeTask());
  }

  /** Sets forceTraceSelfAndParents on the PNodeTask AND adds to trace [SEE_PARTIAL_TRACE] */
  public static void markNodeAndParentsToBeTraced(NodeTask ntsk) {
    markNodeAndParentsToBeTraced(accessProfile(ntsk));
  }

  /** Sets forceTraceSelfAndParents on the PNodeTask AND adds to trace [SEE_PARTIAL_TRACE] */
  public static void markNodeAndParentsToBeTraced(PNodeTaskLive pnt) {
    if (!pnt.traceSelfAndParents) {
      pnt.traceSelfAndParents = true;
      if (!pnt.addedToTrace) {
        synchronized (trace) {
          if (OGTrace.observer.traceNodes && !pnt.addedToTrace) {
            pnt.addedToTrace = true;
            trace.add(pnt);
          }
        }
      }
    }
  }

  /**
   * Switches all properties tracing inclusion and default for new properties [SEE_PARTIAL_TRACE]
   */
  public static void markAllPropertiesToBeTraced(boolean enable) {
    Settings.defaultToIncludeInTrace = enable;
    for (NodeTaskInfo nti : taskInfos()) {
      if (nti != null) nti.setTraceSelfAndParent(enable);
    }
  }

  public static void markAllAsSeen() {
    synchronized (trace) {
      for (PNodeTask task : trace) {
        task.setSeenBefore();
      }
    }
  }

  /** All PNodeTasks of all seen roots */
  private static final ArrayList<PNodeTask> roots = new ArrayList<>();

  @SuppressWarnings("unchecked")
  public static ArrayList<PNodeTask> getRoots() {
    synchronized (roots) {
      return (ArrayList<PNodeTask>) roots.clone();
    }
  }

  /** All recorded invalidates */
  private static final ArrayList<PNodeInvalidate> invalidates = new ArrayList<>();

  @SuppressWarnings("unchecked")
  public static ArrayList<PNodeInvalidate> getInvalidates() {
    synchronized (invalidates) {
      return (ArrayList<PNodeInvalidate>) invalidates.clone();
    }
  }

  public static final Set<NodeTaskInfo> untrackedProperties =
      Collections.synchronizedSet(new HashSet<>());

  /*
   * val tags = ArrayBuffer.empty[PNodeTaskInfo]
   * val tagsRemote = ArrayBuffer.empty[PNodeTaskInfo]
   */

  /** All recorded waits */
  private static final ArrayList<WaitProfile> waits = new ArrayList<>();

  @SuppressWarnings("unchecked")
  public static ArrayList<WaitProfile> getWaits() {
    synchronized (waits) {
      return (ArrayList<WaitProfile>) waits.clone();
    }
  }

  public static class WithChangeListener<T> implements Serializable {

    // Avoid passing callbacks when debugger is out of process.
    private final transient List<Consumer<T>> callbacks =
        Collections.synchronizedList(new ArrayList<>());

    public final void addCallback(Consumer<T> callback) {
      callbacks.add(callback);
    }

    private volatile T value;
    public final boolean available;

    public T getValue() {
      return value;
    }

    public void setValue(T value, boolean runCallbacks) {
      this.value = value;
      if (runCallbacks) {
        callbacks.forEach(callback -> callback.accept(value));
      }
    }

    public void setValue(T value) {
      setValue(value, true);
    }

    WithChangeListener(T value) {
      this.value = value;
      this.available = true;
    }

    WithChangeListener(T value, boolean available) {
      this.value = value;
      this.available = available;
    }
  }

  public static WithChangeListener<Boolean> traceWaits = new WithChangeListener<>(false);
  public static WithChangeListener<Boolean> traceInvalidates = new WithChangeListener<>(false);
  public static WithChangeListener<Boolean> profileSSUsage =
      new WithChangeListener<>(DiagnosticSettings.profileScenarioStackUsage);
  // this will not be available/enabled on engine
  public static WithChangeListener<Boolean> traceTweaks =
      new WithChangeListener<>(
          DiagnosticSettings.internal_traceTweaksOnStartNoDefault,
          DiagnosticSettings.traceTweaksEnabled);
  ;

  private static final VarHandle profileField_vh;

  static {
    Settings.ensureTraceAvailable();
    if (DiagnosticSettings.traceAvailable) {
      try {
        var lookup = MethodHandles.privateLookupIn(NodeTask.class, MethodHandles.lookup());
        //noinspection JavaLangInvokeHandleSignature
        profileField_vh = lookup.findVarHandle(NodeTask.class, "_Profile", PNodeTask.class);
      } catch (IllegalAccessException | NoSuchFieldException e) {
        throw new RuntimeException("Unable to get _Profile field", e);
      }
    } else profileField_vh = null;
  }

  /**
   * warning! this reads _profile field atomically from NodeTask and adds PNodeTask to trace if it
   * wasn't previously recorded. Use NodeTask.getProfile instead if using this as a getter and don't
   * want side effects (but check for null)
   */
  public static PNodeTaskLive accessProfile(NodeTask ntsk) {
    if (DiagnosticSettings.traceAvailable) {
      PNodeTaskLive p = (PNodeTaskLive) profileField_vh.getVolatile(ntsk);
      if (p == null) {
        p = new PNodeTaskLive(ntsk);
        // accessProfile can be hit from multiple threads nearly simultaneously (e.g. if a node
        // starts on one thread
        // and is very quickly completed on another thread by a node it was waiting on), so we need
        // to initialize
        // the _Profile field atomically.
        OGTrace.trace.ensureProfileRecorded(ntsk.getProfileId(), ntsk);
        if (!profileField_vh.compareAndSet(ntsk, null, p))
          // Since it only ever transitions from null -> value, if the CAS fails we can just grab
          // the value that another thread wrote.
          p = (PNodeTaskLive) profileField_vh.getVolatile(ntsk);
        // the thread that wins the CAS also adds the node to trace
        else if (ntsk.isIncludedInTrace()) {
          p.traceSelfAndParents = true;
          if (OGTrace.observer.traceNodes)
            synchronized (trace) {
              p.addedToTrace = true;
              trace.add(p);
            }
        }
      }
      return p;
    } else return new PNodeTaskLive(ntsk);
  }

  /** For hierarchical walking we need to know entry points */
  public static void registerRoot(NodeTask roottsk) {
    if (OGTrace.observer.traceNodes) {
      PNodeTask pnt = accessProfile(roottsk);
      synchronized (roots) {
        roots.add(pnt);
      }
    }
  }

  /**
   * Call to this method is injected by entity agent during profiling Profiler modifies anonymous
   * node class by adding the following Notice lock is effectively provided by cctor of the nodeCls
   * <code>
   * class xxx$node extends ...Node {
   *  static int __profileId = allocateAnonPNodeTaskInfo(classOf(xxx$node))
   *  int getProfileId() { return __profileId; }
   * }
   * </code>
   */
  @SuppressWarnings({"unused", "UnusedReturnValue"})
  public static int allocateAnonPNodeTaskInfo(Class<?> nodeCls) {
    return NodeTaskInfoRegistry.nextId();
  }

  /**
   * Reset traces... notes:
   *
   * <p>1. don't want to reset taskInfos since they are registered once when NTIs are created
   *
   * <p>2. if unit test failure only repros when running multiple tests, consider disabling profiler
   * reset between tests
   */
  public static void resetNodeTrace() {
    OGTrace.log.javaLogger().info("Resetting trace and PNodeTask relationships");
    synchronized (trace) {
      for (PNodeTask pntsk : trace) {
        if (pntsk != null) {
          pntsk.resetRelationships(); // The check for null here is only needed when partial
          // debugging is enabled
          // note: [start] is already added to trace here if running tests, and gets reset
          pntsk.addedToTrace = false;
        }
      }
      trace.clear();
    }
    synchronized (roots) {
      for (PNodeTask pnt : roots) {
        pnt.resetRelationships();
      }
      // Roots are added in registerRoot() and are never re-added (We should probably drop 'roots'
      // completely)
    }
    synchronized (invalidates) {
      invalidates.clear();
    }
    resetWaits();
  }

  /** Reset all recorder waits */
  public static void resetWaits() {
    synchronized (waits) {
      waits.clear();
    }
  }

  /** Keeps track of all waits */
  static void addGraphWaitTime(WaitProfile wi) {
    synchronized (waits) {
      if (traceWaits.getValue()) waits.add(wi);
    }
  }

  public static void invalidate(PNodeInvalidate inv) {
    synchronized (invalidates) {
      invalidates.add(inv);
    }
  }
}
