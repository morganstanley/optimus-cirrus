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

import static optimus.debug.InstrumentationInjector.OBJECT_ARR_DESC;
import static optimus.debug.InstrumentationInjector.OBJECT_DESC;

public class InstrumentationConfig {
  final private static HashMap<String, ClassPatch> clsPatches = new HashMap<>();
  final private static HashMap<String, Boolean> entityClasses = new HashMap<>();
  final private static HashMap<String, Boolean> moduleExclusions = new HashMap<>();

  static boolean instrumentAllEntities = false;
  static boolean instrumentAllModuleConstructors = false;
  public static boolean instrumentAllHashCodes = false;
  static boolean instrumentAllEntityApplies = false;

  final private static String IS =  "optimus/graph/InstrumentationSupport";
  static String CACHED_VALUE_TYPE = "optimus/graph/InstrumentationSupport$CachedValue";
  private static String CACHED_VALUE_DESC = "L" + CACHED_VALUE_TYPE + ";";
  private static String CACHED_FUNC_DESC = "(I" + OBJECT_DESC + OBJECT_ARR_DESC + ")" + CACHED_VALUE_DESC;

  private static MethodRef traceAsNode = new MethodRef(IS, "traceAsNode");
  private static MethodRef traceAsNodeEnter = new MethodRef(IS, "traceAsNodeEnter");
  private static MethodRef traceAsNodeExit = new MethodRef(IS, "traceAsNodeExit");
  private static MethodRef cacheFunctionEnter = new MethodRef(IS, "cacheFunctionEnter", CACHED_FUNC_DESC);
  private static MethodRef cacheFunctionExit = new MethodRef(IS, "cacheFunctionExit");
  private static MethodRef traceAsStackCollector = new MethodRef(IS, "traceAsStackCollector");
  private static MethodRef traceAsScenarioStackMarkerEnter = new MethodRef(IS, "traceAsScenarioStackMarkerEnter");
  private static MethodRef traceAsScenarioStackMarkerExit = new MethodRef(IS, "traceAsScenarioStackMarkerExit");
  private static MethodRef traceValAsNode = new MethodRef(IS, "traceValAsNode");
  public static MethodRef dumpIfEntityConstructing = new MethodRef(IS, "dumpStackIfEntityConstructing");

  // Allows fast attribution to the location
  public final static ArrayList<MethodDesc> descriptors = new ArrayList<>();

  static {
    entityClasses.putIfAbsent("optimus/platform/storable/EntityImpl", Boolean.TRUE);
    InstrumentationCmds.loadCommands();
  }

  static boolean instrumentAnyGroups() {
    return instrumentAllHashCodes ||
           instrumentAllEntities ||
           instrumentAllEntityApplies ||
           instrumentAllModuleConstructors;
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
    FieldPatch cacheInField;
    boolean checkAndReturn;
    boolean prefixWithID;
    boolean prefixWithArgs;
    boolean passLocalValue;
    boolean suffixWithID;
    boolean suffixWithThis;
    boolean suffixWithReturnValue;
    FieldPatch storeToField;

    MethodPatch(MethodRef from) { this.from = from; }
  }

  static class MethodForward {
    public MethodRef from;
    public MethodRef to;

    MethodForward(MethodRef from, MethodRef to) {
      this.from = from;
      this.to = to;
    }
  }

  static class FieldPatch {
    final public String name;
    final public String type;

    FieldPatch(String name, String type) {
      this.name = name;
      this.type = type;
    }
  }

  static class ClassPatch {
    ArrayList<MethodPatch> methodPatches = new ArrayList<>();
    ArrayList<FieldPatch> fieldPatches = new ArrayList<>();
    MethodForward methodForward;
    boolean traceValsAsNodes;
    boolean poisonCacheEquality;
    boolean cacheAllApplies;
    boolean bracketAllLzyComputes;

    MethodPatch forMethod(String name, String desc) {
      for (MethodPatch patch : methodPatches) {
        boolean descMatched = (patch.from.descriptor == null) || desc.equals(patch.from.descriptor);
        if (descMatched && patch.from.method.equals(name))
          return patch;
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
    patch.prefix = InstrumentedModuleCtor.mrEnterCtor;
    patch.suffix = InstrumentedModuleCtor.mrExitCtor;
    return patch;
  }

  static MethodPatch patchForCachingMethod(String clsName, String method) {
    var patch = new MethodPatch(new MethodRef(clsName, method));
    patch.prefix = cacheFunctionEnter;
    patch.prefixWithID = true;
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
    return clsPatches.get(className);
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
    return !clsPatches.isEmpty() || instrumentAnyGroups();
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

  public static MethodPatch addPrefixCall(MethodRef from, MethodRef to, boolean withID, boolean withArgs) {
    var methodPatch = putIfAbsentMethodPatch(from);
    methodPatch.prefixWithID = withID;
    methodPatch.prefixWithArgs = withArgs;
    methodPatch.prefix = to;
    return methodPatch;
  }

  static void addSuffixCall(MethodRef from, MethodRef to) {
    var methodPatch = putIfAbsentMethodPatch(from);
    methodPatch.suffix = to;
  }

  /** Leaf node support, simpler version of addTraceAsNode */
  static void addTraceEntryAsNode(MethodRef mref) {
    addPrefixCall(mref, traceAsNode, true, true);
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
  }

  public static void addImplementCallByForwarding(MethodRef from, MethodRef to) {
    var clsPatch = putIfAbsentClassPatch(from.cls);
    clsPatch.methodForward = new MethodForward(from, to);
  }

  public static FieldPatch addField(String clsName, String fieldName) {
    return addField(clsName, fieldName, null);
  }

  public static FieldPatch addField(String clsName, String fieldName, String type) {
    var clsPatch = putIfAbsentClassPatch(clsName);
    var fieldPatch = new FieldPatch(fieldName, type);
    clsPatch.fieldPatches.add(fieldPatch);
    return fieldPatch;
  }

  public static void addRecordPrefixCallIntoMember(String fieldName, MethodRef mref) {
    var fieldPatch = addField(mref.cls, fieldName);
    var patch = addPrefixCall(mref, traceAsStackCollector, true, true);
    patch.storeToField = fieldPatch;
  }

  public static void addModuleConstructionIntercept(String clsName) {
    var mref = new MethodRef(clsName, "<clinit>");
    addPrefixCall(mref, InstrumentedModuleCtor.mrEnterCtor, false, false);
    addSuffixCall(mref, InstrumentedModuleCtor.mrExitCtor);
    putIfAbsentClassPatch(clsName).bracketAllLzyComputes = true;
  }
}
