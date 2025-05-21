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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import optimus.debug.InstrumentationConfig;
import optimus.debug.RTVerifierCategory;
import optimus.graph.diagnostics.DbgObjectSupport;
import optimus.graph.diagnostics.NodeName;
import optimus.graph.diagnostics.rtverifier.RTVerifierReporter$;
import optimus.graph.diagnostics.SingleProfiledTaskData;
import optimus.platform.ScenarioStack;
import optimus.platform.storable.Entity;
import optimus.platform.util.PrettyStringBuilder;

@SuppressWarnings("unused") // entityagent routes calls here
public class InstrumentationSupport {
  public static class JVMNodeTask extends NodeTask {
    private final int profileID;
    private final Object _this; // null if method is static
    private final Object[] _args;
    // Might be sufficient just to pass name
    private final InstrumentationConfig.MethodDesc methodDesc;
    Object result;
    NodeTask prevTask;

    JVMNodeTask(
        int profileID,
        InstrumentationConfig.MethodDesc methodDesc,
        ScenarioStack ss,
        Object _this,
        Object[] args) {
      attach(ss);
      this.profileID = profileID;
      this.methodDesc = methodDesc;
      this._this = _this;
      this._args = args;
    }

    @Override
    public int getProfileId() {
      return profileID;
    }

    @Override
    public Object[] args() {
      return _args;
    }

    @Override
    public Object methodThis() {
      return _this;
    }

    @Override
    public NodeName nodeName() {
      return (NodeName) methodDesc.tag;
    }

    @Override
    public Object resultObject() {
      return this.result;
    }

    @Override
    public PrettyStringBuilder writePrettyString(PrettyStringBuilder sb) {
      return sb.append(nodeName());
    }
  }

  public static class ExceptionNodeTask extends NodeTask {
    private static final ConcurrentHashMap<Class<?>, HashMap.SimpleEntry<NodeName, Integer>> map =
        new ConcurrentHashMap<>();
    private final int profileID;
    private final NodeName nodeName;
    private final Exception ex;

    ExceptionNodeTask(Exception ex) {
      var entry =
          map.computeIfAbsent(
              ex.getClass(),
              cls -> {
                var name = NodeName.fromNodeCls(cls);
                var profileID = NodeTaskInfoRegistry.nextId();
                return new HashMap.SimpleEntry<>(name, profileID);
              });
      this.ex = ex;
      this.profileID = entry.getValue();
      this.nodeName = entry.getKey();
    }

    @Override
    public int getProfileId() {
      return profileID;
    }

    @Override
    public Object resultObject() {
      return ex;
    }

    @Override
    public NodeName nodeName() {
      return nodeName;
    }
  }

  public static class CachedValue {
    public Object result;
    // The code sets result and then sets hasResult, on x86 (writes are NOT re-ordered and volatile
    // is not needed)
    // CONSIDER: make hasResult into a function, add magic NotReady constant, etc....
    public volatile boolean hasResult;
  }

  public static void genericCall() {
    System.out.println(">>> Tracing");
  }

  public static void dumpStackIfEntityConstructing() {
    var ec = OGSchedulerContext._TRACESUPPORT_unsafe_current();
    if (ec == null) return;
    var pss = ec.scenarioStack().pss();
    if (pss != null && pss.constructingMethodID() != 0) {
      System.err.println(
          ">> WARNING(ENTITY_CTOR): \n"
              + ec.getCurrentNodeTask().waitersToFullMultilineNodeStack());
      Thread.dumpStack();
    }
  }

  /**
   * Primary reason for this interceptor is to dump warnings if tweakable are touched/found in
   * entity ctor [SEE_verifyScenarioStackGetNode]
   *
   * @see RTVerifierCategory#TWEAKABLE_IN_ENTITY_CTOR
   * @see RTVerifierCategory#TWEAK_IN_ENTITY_CTOR
   */
  public static void verifyScenarioStackGetNode(
      Node<?> retValue, ScenarioStack ss, PropertyNode<?> key, OGSchedulerContext ec) {
    if (ss.isScenarioIndependent() || !key.propertyInfo().isDirectlyTweakable())
      return; // Can't be our case

    var pss = ec.scenarioStack().pss();
    if (pss == null || pss.constructingMethodID() == 0) return; // Not constructing an entity

    var foundTweak = retValue instanceof TweakNode<?> || retValue instanceof TwkResolver<?>;
    if (InstrumentationConfig.reportTouchingTweakable
        || (InstrumentationConfig.reportFindingTweaks && foundTweak)) {
      pss.foundTweaks_$eq(foundTweak);
      var waitChain = ec.getCurrentNodeTask().nodeStackAny(false, pss.entryTask());
      pss.addViolation(key, waitChain);
    }
  }

  /** [SEE_verifyScenarioStackGetNode] */
  public static void runAndWaitEntry(OGSchedulerContext ec, NodeTask taskNode) {
    var pss = ec.scenarioStack().pss();
    if (pss != null) pss.pushRunAndWaitCount(taskNode);
  }

  public static Consumer<String> TEST_ONLY_reportingTweakableInConstructor;

  /** [SEE_verifyScenarioStackGetNode] */
  public static void runAndWaitExit(OGSchedulerContext ec) {
    var pss = ec.scenarioStack().pss();
    if (pss != null) {
      ArrayList<StackTraceElement> cleanStack = null;
      String ctorCls = null;
      if (pss.popRunAndWaitCount() && pss.hasViolations()) {
        var methodDesc = InstrumentationConfig.getMethodInfo(pss.constructingMethodID());
        ctorCls = methodDesc.ref.cls.replace('/', '.');
        var clsToStopAt = ctorCls;
        cleanStack =
            StackWalker.getInstance(StackWalker.Option.RETAIN_CLASS_REFERENCE)
                .walk(
                    stream -> {
                      var stack = new ArrayList<StackTraceElement>();
                      var it = stream.iterator();
                      while (it.hasNext()) {
                        var frame = it.next();
                        var clsName = frame.getClassName();
                        if (clsName.startsWith("optimus.graph")) continue; // Skip internal frames
                        stack.add(frame.toStackTraceElement());
                        if (frame.getMethodName().equals("<init>") && clsName.equals(clsToStopAt))
                          break;
                      }
                      return stack;
                    });
      }
      if (cleanStack != null) {
        PrettyStringBuilder sb = new PrettyStringBuilder();
        sb.appendln(">> (ERROR: Non RT Constructor) Entity: " + ctorCls);
        sb.append(pss.foundTweaks() ? "Found tweaks" : "Touched tweakables");
        pss.writeViolations(sb);
        sb.append("JVM stack for entry into graph");
        sb.startBlock();
        StackTraceElement lastFrame = null;
        for (var frame : cleanStack) {
          sb.appendln(frame.toString());
          lastFrame = frame;
        }
        sb.endBlock();

        if (TEST_ONLY_reportingTweakableInConstructor != null)
          TEST_ONLY_reportingTweakableInConstructor.accept(sb.toString());
        else {
          if (DiagnosticSettings.enableRTVerifier) {
            var category =
                pss.foundTweaks()
                    ? RTVerifierCategory.TWEAK_IN_ENTITY_CTOR
                    : RTVerifierCategory.TWEAKABLE_IN_ENTITY_CTOR;
            String frame = lastFrame == null ? "UNKNOWN" : lastFrame.toString();
            String clazzName = lastFrame == null ? null : lastFrame.getClassName();
            String stackTrace =
                cleanStack.stream().map(Object::toString).collect(Collectors.joining(", "));
            RTVerifierReporter$.MODULE$.reportClassViolation(
                category, frame, sb.toString(), clazzName);
          } else
            System.err.println(
                ">> RT violation detected (entity constructor)! Accessing a tweak/tweakable while constructing an entity\n"
                    + sb);
        }

        pss.clearViolations();
      }
    }
  }

  private static String cleanNodeName(CallWithArgs function) {
    var cleanNodeName = NodeName.cleanNodeClassName(function.fullName(), '.');
    return cleanNodeName.substring(0, cleanNodeName.lastIndexOf('.'));
  }

  public static CallWithArgs timerStart(CallWithArgs function) {
    var name = cleanNodeName(function);
    SingleProfiledTaskData.updateStarts(name);
    function.completeWithResult(OGTrace.nanoTime());
    return function;
  }

  // TODO (OPTIMUS-57169): revisit timings
  public static void timerEnd(Object result, CallWithArgs function) {
    var name = cleanNodeName(function);
    var time = OGTrace.nanoTime() - (Long) function.result;
    SingleProfiledTaskData.updateTime(name, time);
  }

  private static JVMNodeTask createTask(
      OGSchedulerContext ec, int methodID, Object _this, Object[] args) {
    var methodDesc = InstrumentationConfig.getMethodInfo(methodID);
    var profileID = methodDesc.profileID;
    if (profileID == 0)
      // (once installed per injection site, never reassigned)
      synchronized (methodDesc) {
        if (methodDesc.profileID == 0) {
          profileID = methodDesc.profileID = NodeTaskInfoRegistry.nextId();
          methodDesc.tag = NodeName.apply(methodDesc.ref);
        }
      }

    var nodeName = (NodeName) methodDesc.tag;
    OGTrace.trace.ensureProfileRecorded(profileID, nodeName.pkgName(), nodeName.name());
    return new JVMNodeTask(profileID, methodDesc, ec.scenarioStack(), _this, args);
  }

  /**
   * Helper method for recording jvm functions as node and auto tracing parents [SEE_TRACE_AS_NODE]
   */
  private static void traceAsNode(
      int methodID, Object _this, Object[] args, Object retValue, boolean traceParents) {
    if (OGTrace.observer.traceNodes) {
      OGSchedulerContext ec = OGSchedulerContext._TRACESUPPORT_unsafe_current();
      if (ec != null) {
        var task = createTask(ec, methodID, _this, args);
        task.result = retValue;
        task.initAsCompleted();
        OGTrace.dependency(ec.getCurrentNodeTask(), task, ec);
      }
    }
  }

  /**
   * Method injected by entityagent to support viewing/recording/browsing of jvm tasks as nodes
   * [SEE_TRACE_AS_NODE]
   */
  public static void traceAsNode(int methodID, Object _this, Object[] args) {
    traceAsNode(methodID, _this, args, null, false);
  }

  public static void traceAsNodeAndParents(int methodID, Object _this, Object[] args) {
    traceAsNode(methodID, _this, args, null, true);
  }

  /**
   * Method injected by entityagent to support viewing/recording/browsing of jvm tasks as nodes
   * [SEE_TRACE_AS_NODE]
   */
  public static void traceValAsNode(Object retValue, int methodID, Object _this) {
    traceAsNode(methodID, _this, null, retValue, false);
  }

  /**
   * Method injected by entityagent to support viewing/recording/browsing of jvm tasks as nodes
   * [SEE_TRACE_AS_NODE]
   */
  public static Object traceAsNodeEnter(int methodID, Object _this, Object[] args) {
    if (OGTrace.observer.traceNodes) {
      OGSchedulerContext ec = OGSchedulerContext._TRACESUPPORT_unsafe_current();
      if (ec != null) {
        var task = createTask(ec, methodID, _this, args);
        task.prevTask = ec.getCurrentNodeTask();
        OGTrace.start(ec.prfCtx, task, true);
        OGTrace.dependency(task.prevTask, task, ec);
        ec.setCurrentNode(task);
        return task;
      }
    }
    return null;
  }

  /**
   * Method injected by entityagent to support viewing/recording/browsing of jvm tasks as nodes
   * [SEE_TRACE_AS_NODE]
   */
  public static void traceAsNodeExit(Object retValue, Object otask) {
    var task = (JVMNodeTask) otask;
    // task can be null if tracing was off
    if (task != null) {
      OGSchedulerContext ec = OGSchedulerContext._TRACESUPPORT_unsafe_current();
      task.result = retValue;
      task.complete(ec);
      OGTrace.observer.stop(ec.prfCtx, task);
      ec.setCurrentNode(task.prevTask);
      task.prevTask = null;
    }
  }

  /** Registers hook to report exceptions as nodes */
  public static void registerTraceExceptionAsNode() {
    InstrumentationConfig.registerExceptionHook(InstrumentationSupport::traceExceptionAsNode);
  }

  /**
   * Method injected by entityagent to support viewing/recording/browsing of jvm exceptions as nodes
   */
  public static void traceExceptionAsNode(Exception ex) {
    if (OGTrace.observer.traceNodes) {
      OGSchedulerContext ec = OGSchedulerContext._TRACESUPPORT_unsafe_current();
      if (ec != null) {
        var nodeTask = ec.getCurrentNodeTask();
        if (nodeTask != null && nodeTask.executionInfo() != NodeTaskInfo.Start) {
          var task = new ExceptionNodeTask(ex);
          NodeTrace.accessProfile(task).traceSelfAndParents = true;
          OGTrace.dependency(nodeTask, task, ec);
        }
      }
    }
  }

  /** Method injected by entityagent */
  public static Object traceAsStackCollector(int methodID, Object _this, Object[] args) {
    return new Exception();
  }

  /** Method injected by entityagent */
  public static Object traceCurrentNodeAndStack(int methodID, Object _this, Object[] args) {
    OGSchedulerContext ec = OGSchedulerContext._TRACESUPPORT_unsafe_current();
    var stack = Thread.currentThread().getStackTrace();
    if (ec != null) {
      var cn = ec.getCurrentNodeTask();
      return new HashMap.SimpleEntry<>(cn, stack);
    }
    return stack;
  }

  /** Method injected by entityagent to enable detection of calls under constructing entities */
  public static Object traceAsScenarioStackMarkerEnter(int methodID, Object _this, Object[] args) {
    OGSchedulerContext ec = OGSchedulerContext._TRACESUPPORT_unsafe_current();
    if (ec != null) {
      var cn = ec.getCurrentNodeTask();
      var prevSS = cn.scenarioStack();
      var newSS = prevSS.withNewProfile();
      newSS.pss().constructingMethodID_$eq(methodID);
      cn.replace(newSS);
      return prevSS;
    }
    return null;
  }

  /** Method injected by entityagent */
  public static void traceAsScenarioStackMarkerExit(Object prevSSObj, Object _this) {
    var prevSS = (ScenarioStack) prevSSObj;
    // task can be null if tracing was off
    if (prevSS != null) {
      OGSchedulerContext ec = OGSchedulerContext._TRACESUPPORT_unsafe_current();
      ec.getCurrentNodeTask().replace(prevSS);
    }
  }

  private static final ConcurrentHashMap<FullCachedValue, FullCachedValue> cache =
      new ConcurrentHashMap<>();

  private static class FullCachedValue extends CachedValue {
    volatile CountDownLatch lock = new CountDownLatch(1);
    int methodID;
    Object _this;
    Object[] args;

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      FullCachedValue that = (FullCachedValue) o;
      return methodID == that.methodID
          && Objects.equals(_this, that._this)
          && Arrays.equals(args, that.args);
    }

    @Override
    public int hashCode() {
      int result = Objects.hash(methodID, _this);
      result = 31 * result + Arrays.hashCode(args);
      return result;
    }

    FullCachedValue(int methodID, Object _this, Object[] args) {
      this.methodID = methodID;
      this._this = _this;
      this.args = args;
    }
  }

  public static CachedValue cacheFunctionEnter(int methodID, Object _this, Object[] args)
      throws InterruptedException {
    var value = new FullCachedValue(methodID, _this, args);
    var preValue = cache.putIfAbsent(value, value);
    if (preValue == null) // We own the latch (aka lock)
      // New CacheValue will tell the calling code to execute....
      return value;
    // Note: 1. read lock, 2. read hasResult (see the Exit with order reversed)
    var lock = preValue.lock;
    if (preValue.hasResult) return preValue;

    lock.await();
    return preValue;
  }

  public static void cacheFunctionExit(Object retValue, CachedValue _cachedValue) {
    var cachedValue = (FullCachedValue) _cachedValue;
    cachedValue.result = retValue;
    // Note: 1. write hasResult, 2. write lock (see Enter with order reversed)
    cachedValue.hasResult = true;
    cachedValue.lock.countDown();
    cachedValue.lock = null;
  }

  /**
   * The call to this method is injected by entityagent as a suffix call into
   * optimus.platform.storable.Entity#argsEqualsHook(optimus.platform.storable.Entity)
   */
  @SuppressWarnings("unused")
  public static void expectAllToEquals(boolean result, Entity a, Entity b) throws Throwable {
    if (result) DbgObjectSupport.expectAllToEqualsSafe(a.$info().dbgFieldMap(), a, b);
  }

  //  intercepts Equals() if return true is recorded for a pairs of objects
  public abstract static class CallWithArgs {
    public Object result;
    public Throwable throwable;

    public Object reApply() {
      return null;
    }

    public Object[] args() {
      return null;
    }

    public String fullName() {
      return null;
    }

    public String name() {
      return this.getClass().getSimpleName();
    }

    public void completeWithResult(Object result) {
      this.result = result;
    }

    public void completeWithException(Throwable throwable) {
      this.throwable = throwable;
    }

    public Boolean isRT() {
      try {
        return reApply().equals(result);
      } catch (Throwable ex) {
        return throwable != null && ex.getClass() == throwable.getClass();
      }
    }
  }

  public static CallWithArgs cwaPrefix(CallWithArgs args) {
    return args;
  }

  public static void cwaSuffix(Object result, CallWithArgs args) {
    args.completeWithResult(result);
  }

  public static void cwaSuffixOnException(Throwable ex, CallWithArgs args) {
    args.completeWithException(ex);
  }

  private static final ArrayList<CallWithArgs> recordings = new ArrayList<>();
  private static final Lock lock = new ReentrantLock();

  /** Default/Sample implementation of native prefix */
  public static void nativePrefix() {}

  /** Default/Sample implementation of native suffix */
  public static void nativeSuffix() {
    if (!recordings.isEmpty()) {
      if (!allAreEquals()) {
        System.err.println("Ouch!");
      }
    }
  }

  /** Sample code to check if the call values haven't changed */
  private static boolean allAreEquals() {
    try {
      lock.lock();
      for (var args : recordings) {
        var newValue = args.reApply();
        if (!Objects.equals(newValue, args.result)) return false;
      }
      return true;
    } finally {
      lock.unlock();
    }
  }

  public static void nativeSuffix(Object result, CallWithArgs args) {
    try {
      lock.lock();
      args.result = result;
      recordings.add(args);
    } finally {
      lock.unlock();
    }
  }

  /** Sample implementation of function interceptor, see also trace_config.sc */
  public static boolean interceptEquals(boolean result, Object a, Object b) {
    System.err.println("interceptEquals: " + result);
    return result;
  }
}
