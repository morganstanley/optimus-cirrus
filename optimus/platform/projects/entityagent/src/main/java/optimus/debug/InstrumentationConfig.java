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
package optimus.debug;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Predicate;

import optimus.EntityAgent;
import org.objectweb.asm.Type;

enum EntityInstrumentationType {
  markScenarioStack, recordConstructedAt, none
}

public class InstrumentationConfig {
  final private static HashMap<String, ClassPatch> clsPatches = new HashMap<>();
  final private static HashMap<String, Boolean> entityClasses = new HashMap<>();
  final private static HashMap<String, Boolean> moduleExclusions = new HashMap<>();
  final private static ArrayList<ClassPatch> multiClsPatches = new ArrayList<>();

  public static boolean setExceptionHookToTraceAsNodeOnStartup;
  static EntityInstrumentationType instrumentAllEntities = EntityInstrumentationType.none;
  static boolean instrumentAllModuleConstructors = false;
  public static boolean instrumentAllHashCodes = false;
  public static String instrumentAllNativePackagePrefixes = null;
  public static String instrumentNativePrefix = null;
  public static String instrumentNativeSuffix = null;
  static boolean instrumentAllEntityApplies = false;
  public static boolean reportTouchingTweakable = false;
  public static boolean reportFindingTweaks = false;

  public static final String OBJECT_DESC = "Ljava/lang/Object;";
  public static final String OBJECT_ARR_DESC = "[Ljava/lang/Object;";
  public static final Type OBJECT_TYPE = Type.getObjectType("java/lang/Object");

  final private static String ENTITY_TYPE = "optimus/platform/storable/Entity";
  final private static String SS_TYPE = "optimus/platform/ScenarioStack";
  final private static String OGSC_TYPE = "optimus/graph/OGSchedulerContext";
  public final static String IS =  "optimus/graph/InstrumentationSupport";
  final static String CACHED_VALUE_TYPE = "optimus/graph/InstrumentationSupport$CachedValue";
  private final static String IMC_TYPE = "optimus/debug/InstrumentedModuleCtor";
  private final static String IEC_TYPE = "optimus/debug/InstrumentedExceptionCtor";
  private final static String ICS_TYPE = "optimus/graph/ICallSite";
  private static final String CACHED_VALUE_DESC = "L" + CACHED_VALUE_TYPE + ";";
  private static final String CACHED_FUNC_DESC = "(I" + OBJECT_DESC + OBJECT_ARR_DESC + ")" + CACHED_VALUE_DESC;

  static final String __constructedAt = "constructedAt";

  private static final MethodRef traceAsNode = new MethodRef(IS, "traceAsNode");
  private static final MethodRef traceAsNodeAndParents = new MethodRef(IS, "traceAsNodeAndParents");
  private static final MethodRef traceAsNodeEnter = new MethodRef(IS, "traceAsNodeEnter");
  private static final MethodRef traceAsNodeExit = new MethodRef(IS, "traceAsNodeExit");
  private static final MethodRef cacheFunctionEnter = new MethodRef(IS, "cacheFunctionEnter", CACHED_FUNC_DESC);
  private static final MethodRef cacheFunctionExit = new MethodRef(IS, "cacheFunctionExit");
  private static final MethodRef traceAsStackCollector = new MethodRef(IS, "traceAsStackCollector");
  static MethodRef traceCurrentNodeAndStack = new MethodRef(IS, "traceCurrentNodeAndStack");
  private static final MethodRef traceAsScenarioStackMarkerEnter = new MethodRef(IS, "traceAsScenarioStackMarkerEnter");
  private static final MethodRef traceAsScenarioStackMarkerExit = new MethodRef(IS, "traceAsScenarioStackMarkerExit");
  private static final MethodRef traceValAsNode = new MethodRef(IS, "traceValAsNode");
  public static MethodRef dumpIfEntityConstructing = new MethodRef(IS, "dumpStackIfEntityConstructing");

  private static final MethodRef exceptionConstructor = new MethodRef("java/lang/Exception", "<init>");
  private static final MethodRef exceptionHook = new MethodRef(IEC_TYPE, "exceptionInitializing");
  static InstrumentationConfig.MethodRef iecPause = new InstrumentationConfig.MethodRef(IEC_TYPE, "pauseReporting", "()I");
  static InstrumentationConfig.MethodRef iecResume = new InstrumentationConfig.MethodRef(IEC_TYPE, "resumeReporting", "(I)V");

  public final static String CALL_WITH_ARGS_NAME = "CallWithArgs";
  public final static String CALL_WITH_ARGS = IS + "$" + CALL_WITH_ARGS_NAME;
  static final MethodRef nativePrefix = new MethodRef(IS, "nativePrefix", "()V");
  static final MethodRef nativeSuffix = new MethodRef(IS, "nativeSuffix", "()V");

  static InstrumentationConfig.MethodRef expectEquals = new InstrumentationConfig.MethodRef(IS, "expectAllToEquals", "(ZLoptimus/platform/storable/Entity;Loptimus/platform/storable/Entity;)V");
  static InstrumentationConfig.MethodRef equalsHook = new InstrumentationConfig.MethodRef(ENTITY_TYPE, "argsEqualsHook");

  /** [SEE_verifyScenarioStackGetNode] */
  private final static String getNodeDesc = "(Loptimus/graph/PropertyNode;Loptimus/graph/OGSchedulerContext;)Loptimus/graph/Node;";
  private final static String verifySSGetNodeDesc = "(Loptimus/graph/Node;Loptimus/platform/ScenarioStack;Loptimus/graph/PropertyNode;Loptimus/graph/OGSchedulerContext;)V";
  private static final InstrumentationConfig.MethodRef verifyScenarioStackGetNode = new InstrumentationConfig.MethodRef(IS, "verifyScenarioStackGetNode", verifySSGetNodeDesc);
  private static final InstrumentationConfig.MethodRef verifyRunAndWaitEntry = new InstrumentationConfig.MethodRef(IS, "runAndWaitEntry", "(Loptimus/graph/OGSchedulerContext;)V");
  private static final InstrumentationConfig.MethodRef verifyRunAndWaitExit = new InstrumentationConfig.MethodRef(IS, "runAndWaitExit", "(Loptimus/graph/OGSchedulerContext;)V");
  private static final InstrumentationConfig.MethodRef scenarioStackGetNode = new InstrumentationConfig.MethodRef(SS_TYPE, "getNode", getNodeDesc);
  private static final InstrumentationConfig.MethodRef runAndWait = new InstrumentationConfig.MethodRef(OGSC_TYPE, "runAndWait");


  private static final InstrumentationConfig.MethodRef imcEnterCtor = new InstrumentationConfig.MethodRef(IMC_TYPE, "enterReporting");
  private static final InstrumentationConfig.MethodRef imcExitCtor = new InstrumentationConfig.MethodRef(IMC_TYPE, "exitReporting");
  static InstrumentationConfig.MethodRef imcPause = new InstrumentationConfig.MethodRef(IMC_TYPE, "pauseReporting", "()I");
  static InstrumentationConfig.MethodRef imcResume = new InstrumentationConfig.MethodRef(IMC_TYPE, "resumeReporting", "(I)V");

  // Allows fast attribution to the location
  public final static ArrayList<MethodDesc> descriptors = new ArrayList<>();

  static {
    entityClasses.putIfAbsent("optimus/platform/storable/EntityImpl", Boolean.TRUE);
    descriptors.add(new MethodDesc(new MethodRef("none", "none")));
    InstrumentationCmds.loadCommands();
  }

  static boolean instrumentAnyGroups() {
    return instrumentAllHashCodes || instrumentAllEntities != EntityInstrumentationType.none
           || instrumentAllEntityApplies || instrumentAllModuleConstructors;
  }

  public static boolean isEntity(String className, String superName) {
    synchronized (entityClasses) {
      if (!entityClasses.containsKey(superName))
        return false;
      entityClasses.putIfAbsent(className, Boolean.TRUE);
    }
    return true;
  }

  static boolean isModuleExcluded(String className) {
    synchronized (moduleExclusions) {
      return moduleExclusions.containsKey(className);
    }
  }

  static void addModuleExclusion(String className) {
    synchronized (moduleExclusions) {
      moduleExclusions.put(className, Boolean.TRUE);
    }
  }

  static void addVerifyScenarioStackCalls() {
    var ssHook = addSuffixCall(scenarioStackGetNode, verifyScenarioStackGetNode);
    ssHook.suffixWithThis = true;
    ssHook.suffixWithArgs = true;
    ssHook.suffixNoArgumentBoxing = true;
    ssHook.suffixWithReturnValue = true;

    var runAndWaitEntry = addPrefixCall(runAndWait, verifyRunAndWaitEntry, false, false);
    runAndWaitEntry.prefixWithThis = true;

    var runAndWaitExit = addSuffixCall(runAndWait, verifyRunAndWaitExit);
    runAndWaitExit.suffixNoArgumentBoxing = true;
    runAndWaitExit.suffixWithThis = true;
  }

  public static void recordConstructorInvocationSite(String className) {
    addRecordPrefixCallIntoMemberWithCallSite(__constructedAt, asMethodRef(className + ".<init>"));
  }

  public static class MethodDesc {
    final public MethodRef ref;
    public volatile Object tag;   // Used to stash NodeName for JVMNodeTask [SEE_TRACE_AS_NODE]
    public volatile int profileID;

    MethodDesc(MethodRef ref) { this.ref = ref; }
  }

  public static class MethodRef {
    final public String cls;
    final public String method;
    final public String descriptor; // If specified used to match and use in generated code

    @Override
    public String toString() {
      return cls + "." + method;
    }

    public MethodRef(String cls, String method) {
      this.cls = cls;
      this.method = method;
      this.descriptor = null;
    }

    public MethodRef(String cls, String method, String descriptor) {
      this.cls = cls;
      this.method = method;
      this.descriptor = descriptor;
    }
  }

  static class MethodPatch {
    final public MethodRef from;
    public MethodRef prefix;
    public MethodRef suffix;
    FieldRef cacheInField;
    boolean checkAndReturn;
    boolean prefixWithID;
    boolean prefixWithThis;
    boolean prefixWithArgs;
    boolean passLocalValue;
    boolean suffixWithID;
    boolean suffixWithThis;
    boolean suffixWithReturnValue;
    boolean suffixWithArgs;
    boolean suffixNoArgumentBoxing;
    boolean suffixWithCallArgs;     ///  All arguments are packaged into custom generated class
    FieldRef storeToField;
    ClassPatch classPatch;
    BiPredicate<String, String> predicate;

    MethodPatch(MethodRef from) { this.from = from; }
  }

  static class GetterMethod {
    MethodRef mRef;
    FieldRef field;

    public GetterMethod(MethodRef getter, FieldRef field) {
      this.mRef = getter;
      this.field = field;
    }
  }

  static class MethodForward {
    public MethodRef from;
    public MethodRef to;

    MethodForward(MethodRef from, MethodRef to) {
      this.from = from;
      this.to = to;
    }
  }

  static class FieldRef {
    final public String name;
    final public String type;

    FieldRef(String name, String type) {
      this.name = name;
      this.type = type == null ? OBJECT_DESC : type;
    }
  }

  static class ClassPatch {
    Object id;
    Predicate<String> classPredicate = null;
    ArrayList<MethodPatch> methodPatches = new ArrayList<>();
    ArrayList<FieldRef> fieldRefs = new ArrayList<>();
    ArrayList<String> interfacePatches = new ArrayList<>();
    GetterMethod getterMethod;
    MethodForward methodForward;
    boolean traceValsAsNodes;
    boolean poisonCacheEquality;
    boolean cacheAllApplies;
    boolean bracketAllLzyComputes;
    boolean wrapNativeCalls;

    String replaceObjectAsBase;
    MethodPatch allMethodsPatch;

    MethodPatch forMethod(String name, String desc) {
      for (MethodPatch patch : methodPatches) {
        if(patch.from.method.equals(name)) {
          if (patch.from.descriptor == null || desc.equals(patch.from.descriptor)) {
            patch.classPatch = this;
            return patch;
          }
        }
      }
      return null;
    }
  }

  static MethodPatch patchForSuffixAsNode(String clsName, String method) {
    var patch = new MethodPatch(new MethodRef(clsName, method));
    patch.suffix = traceValAsNode;
    patch.suffixWithID = true;
    patch.suffixWithThis = true;
    patch.suffixWithReturnValue = true;
    return patch;
  }

  static MethodPatch patchForBracketingLzyCompute(String clsName, String method) {
    var patch = new MethodPatch(new MethodRef(clsName, method));
    patch.prefix = InstrumentationConfig.imcEnterCtor;
    patch.suffix = InstrumentationConfig.imcExitCtor;
    return patch;
  }

  static MethodPatch patchForCachingMethod(String clsName, String method) {
    var patch = new MethodPatch(new MethodRef(clsName, method));
    patch.prefix = cacheFunctionEnter;
    patch.prefixWithID = true;
    patch.prefixWithThis = true;
    patch.prefixWithArgs = true;
    patch.suffix = cacheFunctionExit;
    patch.suffixWithReturnValue = true;
    patch.passLocalValue = true;
    patch.checkAndReturn = true;
    return patch;
  }

  public static MethodRef asMethodRef(String name) {
    String cleanName = name.trim();
    int lastDot = cleanName.lastIndexOf('.');
    String cls = cleanName.substring(0, lastDot).replace('.', '/');
    String method = cleanName.substring(lastDot + 1);
    int descriptorStart = method.indexOf('(');
    String descriptor = null;
    if (descriptorStart > 0) {
      descriptor = method.substring(descriptorStart);
      method = method.substring(0, descriptorStart);
    }
    return new MethodRef(cls, method, descriptor);
  }

  public static ClassPatch forClass(String className) {
    var exact =  clsPatches.get(className);
    if(exact != null || multiClsPatches.isEmpty()) return exact;
    for(var v: multiClsPatches) {
      if(v.classPredicate != null && v.classPredicate.test(className))
        return v;
    }
    return null;
  }

  static int allocateID(MethodRef ref) {
    MethodDesc desc = new MethodDesc(ref);
    synchronized (descriptors) {
      descriptors.add(desc);
      return descriptors.size() - 1;
    }
  }

  public static MethodDesc getMethodInfo(int methodID) {
    return descriptors.get(methodID);
  }

  public static boolean isEnabled() {
    return !clsPatches.isEmpty() || !multiClsPatches.isEmpty() ||  instrumentAnyGroups();
  }

  static ClassPatch putIfAbsentClassPatch(String clsName) {
    ClassPatch clsPatch = clsPatches.get(clsName);
    if (clsPatch == null) {
      clsPatch = new ClassPatch();
      clsPatches.put(clsName, clsPatch);
    }
    return clsPatch;
  }

  static MethodPatch putIfAbsentMethodPatch(MethodRef from) {
    var clsPatch = putIfAbsentClassPatch(from.cls);
    var methodPatch = clsPatch.forMethod(from.method, from.descriptor);
    if (methodPatch == null) {
      methodPatch = new MethodPatch(from);
      clsPatch.methodPatches.add(methodPatch);
    }
    return methodPatch;
  }

  /////////////////////////// BEGIN PUBLIC INTERFACE /////////////////////////////////////

  /** Injects a given interface to class 'type' */
  public static void addInterfacePatch(String type, String interfaceType) {
    var clsPatch = putIfAbsentClassPatch(type);
    clsPatch.interfacePatches.add(interfaceType);
  }

  /** Inject call site interface (to give a stack for given call) */
  public static void addCallSiteInterface(String type) {
    addInterfacePatch(type, ICS_TYPE);
  }

  public static void addGetterMethod(MethodRef method, FieldRef field) {
    var clsPatch = putIfAbsentClassPatch(method.cls);
    clsPatch.getterMethod = new GetterMethod(method, field);
  }

  public static MethodPatch addPrefixCall(MethodRef from, MethodRef to, boolean withID, boolean withArgs) {
    var methodPatch = putIfAbsentMethodPatch(from);
    methodPatch.prefixWithID = withID;
    methodPatch.prefixWithThis = withArgs;
    methodPatch.prefixWithArgs = withArgs;
    methodPatch.prefix = to;
    return methodPatch;
  }

  static MethodPatch addSuffixCall(MethodRef from, MethodRef to) {
    var methodPatch = putIfAbsentMethodPatch(from);
    methodPatch.suffix = to;
    return methodPatch;
  }

  public static MethodPatch addSuffixCallWithCallsArgs(MethodRef from, MethodRef to) {
    var methodPatch = putIfAbsentMethodPatch(from);
    methodPatch.suffix = to;
    methodPatch.suffixWithCallArgs = true;
    return methodPatch;
  }


  /** Hook should probably be added just once, but it's actually OK to call it multiple times */
  private static void addExceptionHook() {
    var patch = addSuffixCall(exceptionConstructor, exceptionHook);
    patch.suffixWithThis = true;
    EntityAgent.retransform(Exception.class);
  }

  /** Leaf node exceptioNode */
  static void addTraceSelfAndParentOnException() {
    addExceptionHook();
    setExceptionHookToTraceAsNodeOnStartup = true;
  }


  /** Register a callback when exception is constructed */
  public static Consumer<Exception> registerExceptionHook(Consumer<Exception> callback) {
    var prev = InstrumentedExceptionCtor.callback;
    InstrumentedExceptionCtor.callback = callback;
    return prev;
  }

  /** Leaf node support, simpler version of addTraceAsNode */
  static void addTraceEntryAsNode(MethodRef mref) {
    addPrefixCall(mref, traceAsNode, true, true);
  }

  /** Leaf node support, simpler version of addTraceAsNode */
  static void addTraceEntryAsNodeAndParents(MethodRef mref) {
    addPrefixCall(mref, traceAsNodeAndParents, true, true);
  }

  public static void addTraceAsNode(MethodRef mref) {
    var patch = addPrefixCall(mref, traceAsNodeEnter, true, true);
    addSuffixCall(mref, traceAsNodeExit);
    patch.suffixWithReturnValue = true;
    patch.passLocalValue = true;
  }

  public static void addCacheFunction(MethodRef mref) {
    var patch = addPrefixCall(mref, cacheFunctionEnter, true, true);
    addSuffixCall(mref, cacheFunctionExit);
    patch.suffixWithReturnValue = true;
    patch.passLocalValue = true;
    patch.checkAndReturn = true;
  }

  public static void addInstrumentAllValsAsNodes(String clsName) {
    var clsPatch = putIfAbsentClassPatch(clsName);
    clsPatch.traceValsAsNodes = true;
  }

  public static void addMarkScenarioStackAsInitializing(String clsName) {
    var mref = new MethodRef(clsName, "<init>");
    var patch = addPrefixCall(mref, traceAsScenarioStackMarkerEnter, true, true);
    addSuffixCall(mref, traceAsScenarioStackMarkerExit);
    patch.passLocalValue = true;
    patch.suffixWithThis = true;
  }

  public static void addImplementCallByForwarding(MethodRef from, MethodRef to) {
    var clsPatch = putIfAbsentClassPatch(from.cls);
    clsPatch.methodForward = new MethodForward(from, to);
  }

  public static FieldRef addField(String clsName, String fieldName) {
    return addField(clsName, fieldName, null);
  }

  public static FieldRef addField(String clsName, String fieldName, String type) {
    var clsPatch = putIfAbsentClassPatch(clsName);
    var fieldPatch = new FieldRef(fieldName, type);
    clsPatch.fieldRefs.add(fieldPatch);
    return fieldPatch;
  }

  public static void addRecordPrefixCallIntoMemberWithStackTrace(String fieldName, MethodRef mref) {
    addRecordPrefixCallIntoMemberWithStackTrace(fieldName, mref, traceAsStackCollector);
  }

  public static void addRecordPrefixCallIntoMemberWithCallSite(String fieldName, MethodRef mref) {
    var fieldRef = addRecordPrefixCallIntoMemberWithStackTrace(fieldName, mref, traceAsStackCollector);
    addCallSiteInterface(mref.cls);
    addGetterMethod(new MethodRef(mref.cls, "getCallSite"), fieldRef);
  }

  static FieldRef addRecordPrefixCallIntoMemberWithStackTrace(String fieldName, MethodRef mref, MethodRef methodToInject) {
    var fieldRef = addField(mref.cls, fieldName);
    var patch = addPrefixCall(mref, methodToInject, true, true);
    patch.storeToField = fieldRef;
    return fieldRef;
  }

  public static void addModuleConstructionIntercept(String clsName) {
    var mref = new MethodRef(clsName, "<clinit>");
    addPrefixCall(mref, InstrumentationConfig.imcEnterCtor, false, false);
    addSuffixCall(mref, InstrumentationConfig.imcExitCtor);
    putIfAbsentClassPatch(clsName).bracketAllLzyComputes = true;
  }
  public static void addAllMethodPatchAndChangeSuper(
      Object id,
      Predicate<String> classPredicate,
      BiPredicate<String, String> methodPredicate,
      String newBase, MethodRef prefix, MethodRef suffix) {
    // This shouldn't happen very often
    if(multiClsPatches.stream().anyMatch(c -> id.equals(c.id)))  return;
    var clsPatch = new ClassPatch();
    clsPatch.id = id;
    clsPatch.replaceObjectAsBase = newBase;
    clsPatch.classPredicate = classPredicate;
    clsPatch.allMethodsPatch = new MethodPatch(null);
    clsPatch.allMethodsPatch.classPatch = clsPatch;
    clsPatch.allMethodsPatch.predicate = methodPredicate;
    clsPatch.allMethodsPatch.prefix = prefix;
    clsPatch.allMethodsPatch.suffix = suffix;
    clsPatch.allMethodsPatch.passLocalValue = true;
    clsPatch.allMethodsPatch.suffixWithReturnValue = true;
    multiClsPatches.add(clsPatch);
  }

  private static void injectLdPreloadRemover() {
    if (System.getProperty("os.name", "a great mystery").startsWith("Linux")) {
      String cls = "optimus/patches/MiscPatches";
      String removeLdPreloadMethod = "remove_LD_PRELOAD";
      try {
        var unload = new MethodRef(cls, removeLdPreloadMethod);
        String pbCls = "java/lang/ProcessBuilder";
        var mref = new MethodRef(pbCls, "<init>");
        var patch = addSuffixCall(mref, unload);
        patch.suffixWithThis = true;
      } catch (Exception e) {
        EntityAgent.logMsg("Unable to load LD_PRELOAD remover: " + e);
      }
    }
  }

  public static void init() {
    injectLdPreloadRemover();
  }

}
