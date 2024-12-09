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
package optimus.graph.loom;

import static optimus.CoreUtils.stripPrefix;
import static optimus.CoreUtils.stripSuffix;
import static optimus.debug.CommonAdapter.dupNamed;
import static optimus.debug.CommonAdapter.isInterface;
import static optimus.debug.CommonAdapter.makePrivate;
import static optimus.debug.CommonAdapter.sameArguments;
import static optimus.graph.loom.LPropertyDescriptor.COLUMN_NA;
import static optimus.graph.loom.LPropertyDescriptor.register;
import static optimus.graph.loom.LoomConfig.AsyncAnnotation;
import static optimus.graph.loom.LoomConfig.CompilerAnnotation;
import static optimus.graph.loom.LoomConfig.DESERIALIZE;
import static optimus.graph.loom.LoomConfig.DESERIALIZE_BSM_MT;
import static optimus.graph.loom.LoomConfig.DESERIALIZE_MT;
import static optimus.graph.loom.LoomConfig.ExposeArgTypesParam;
import static optimus.graph.loom.LoomConfig.IMPL_SUFFIX;
import static optimus.graph.loom.LoomConfig.LOOM_SUFFIX;
import static optimus.graph.loom.LoomConfig.LoomAnnotation;
import static optimus.graph.loom.LoomConfig.LoomImmutablesParam;
import static optimus.graph.loom.LoomConfig.LoomLambdasParam;
import static optimus.graph.loom.LoomConfig.LoomLcnParam;
import static optimus.graph.loom.LoomConfig.LoomNodesParam;
import static optimus.graph.loom.LoomConfig.NEW_NODE_SUFFIX;
import static optimus.graph.loom.LoomConfig.NF_TRIVIAL;
import static optimus.graph.loom.LoomConfig.NodeAnnotation;
import static optimus.graph.loom.LoomConfig.QUEUED_SUFFIX;
import static optimus.graph.loom.LoomConfig.SCALA_ANON_PREFIX;
import static optimus.graph.loom.LoomConfig.ScenarioIndependentAnnotation;
import static optimus.graph.loom.LoomConfig.bsmScalaFunc;
import static optimus.graph.loom.LoomConfig.bsmScalaFuncR;
import static optimus.graph.loom.LoomConfig.enableCompilerDebug;
import static optimus.graph.loom.LoomConfig.setCompilerLevelZero;
import static optimus.graph.loom.LoomConfig.setAssumeGlobalMutation;
import static optimus.graph.loom.LoomConfig.setTurnOffNodeReorder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import optimus.graph.loom.compiler.LCompiler;
import optimus.graph.loom.compiler.LError;
import org.objectweb.asm.Handle;
import org.objectweb.asm.Label;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.AnnotationNode;
import org.objectweb.asm.tree.ClassNode;
import org.objectweb.asm.tree.InvokeDynamicInsnNode;
import org.objectweb.asm.tree.LineNumberNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;

public class LoomAdapter implements Opcodes {
  private static final int NON_NODE_ACCESS = ACC_STATIC | ACC_SYNTHETIC | ACC_ABSTRACT | ACC_BRIDGE;
  private static final int LAMBDA_CANDIDATE_ACCESS = ACC_STATIC | ACC_SYNTHETIC;

  private final HashMap<String, String> implFieldMap = new HashMap<>();
  private final HashMap<String, String> implMethodMap = new HashMap<>();

  /** async with plugins and node methods */
  private final ArrayList<NodeMethod> asyncMethods = new ArrayList<>();

  private final ArrayList<TransformableMethod> lambdaMethods = new ArrayList<>();

  private final HashSet<LNodeCall> recomputeFrames = new HashSet<>();

  private final HashMap<String, HashSet<String>> superTypes = new HashMap<>();
  private final HashMap<String, ArrayList<String>> superTypesPerType = new HashMap<>();

  public boolean needsToComputeFrames(String name, String descriptor) {
    return recomputeFrames.contains(new LNodeCall("", name, descriptor));
  }

  public void registerForFrameCompute(String name, String desc) {
    recomputeFrames.add(new LNodeCall("", name, desc));
  }

  public String getCommonType(String type1, String type2) {
    var superTypes1 = getSuperTypes(type1);
    var superTypes2 = getSuperTypes(type2);
    var visited = new HashSet<>(superTypes1);
    for (var st : superTypes2) {
      if (visited.contains(st)) return st;
    }
    LError.fatal("Cannot find a common super type " + type1 + " & " + type2);
    return null;
  }

  private ArrayList<String> getSuperTypes(String type) {
    return superTypesPerType.computeIfAbsent(
        type,
        k -> {
          var superTypes = new ArrayList<String>();
          getSuperTypes(k, superTypes);
          Collections.reverse(superTypes);
          return superTypes;
        });
  }

  private void getSuperTypes(String type, ArrayList<String> supers) {
    var mySupers = superTypes.get(type);
    if (mySupers != null) {
      for (var mySuper : mySupers) getSuperTypes(mySuper, supers);
    }
    supers.add(type);
  }

  public void registerCommonType(String type, String superType) {
    if (type.equals(superType)) return;
    superTypes.computeIfAbsent(type, k -> new HashSet<>()).add(superType);
  }

  private static class ReplacedBSM {
    InvokeDynamicInsnNode indy;
    int lineNumber;
    int localID; // class local ID

    public Handle getTarget() {
      return (Handle) indy.bsmArgs[1];
    }
  }

  /** [INNER_NAME] Soon to be reimplemented */
  private static class EnrichedName {
    final String enclosingMethod; // def foo() { .... }
    final String nestedCallIn; // given() { lambda }
    final int lineNumber;
    final int columnNumber;

    public EnrichedName(String enclosingMethod, int line, int column, String nestedCallIn) {
      this.enclosingMethod = enclosingMethod;
      this.lineNumber = line;
      this.columnNumber = column;
      this.nestedCallIn = nestedCallIn;
    }
  }

  private final ArrayList<ReplacedBSM> replacedBSM = new ArrayList<>();
  private final HashSet<String> trivialMethods = new HashSet<>();
  // [INNER_NAME] local names like anon$func$1 -> enriched name
  private final HashMap<String, EnrichedName> nameTranslate = new HashMap<>();
  /* method, null*/
  private final HashMap<LNodeCall, LNodeCall> nodeCalls = new HashMap<>();

  /* lambdas, column number */
  private final HashMap<String, Integer> lambdasWithCN = new HashMap<>();
  public final HashSet<String> immutableTypes = new HashSet<>() {};

  public final ClassNode cls;
  private final String privatePrefix;

  private CompilerArgs classCompilerArgs = CompilerArgs.Default;

  public LoomAdapter(ClassNode cls) {
    this.cls = cls;
    var suffix = isInterface(cls.access) ? "$$" : "$";
    this.privatePrefix = cls.name.replace('/', '$') + suffix;
  }

  public void transform() {
    // Order here is important...
    setupNodeAndLambdaCalls();
    findImplFields();
    findImplMethods();
    findAllNodeAndTrivialFunctions();
    cls.methods.removeIf(this::isTransforming);
    transformNodeFunctions(); // Function re-ordering
    implementDeserializeLLambda();
    implementPrivateLoomMethods();
  }

  private void setupNodeAndLambdaCalls() {
    for (var ann : cls.invisibleAnnotations) {
      if (LoomAnnotation.equals(ann.desc)) {
        var lambdas = new ArrayList<String>();
        var lcns = new ArrayList<Integer>();
        var values = ann.values;
        if (values != null) {
          for (int i = 0, n = values.size(); i < n; i += 2) {
            var name = (String) values.get(i);
            var value = values.get(i + 1);
            //noinspection IfCanBeSwitch ...it doesn't look very readable :)
            if (name.equals(LoomNodesParam)) trackNodeCalls(asStrings(value));
            else if (name.equals(LoomLambdasParam)) lambdas = asStrings(value);
            else if (name.equals(LoomLcnParam)) lcns = asInts(value);
            else if (name.equals(LoomImmutablesParam)) immutableTypes.addAll(asStrings(value));
          }
          LError.require(lambdas.size() == lcns.size(), "Same size for lambdas and lcn expected");
          for (var i = 0; i < lambdas.size(); i++) {
            lambdasWithCN.put(lambdas.get(i), lcns.get(i));
          }
        }
      } else if (CompilerAnnotation.equals(ann.desc))
        classCompilerArgs = CompilerArgs.parse(ann, classCompilerArgs);
    }
  }

  private ArrayList<String> asStrings(Object entry) {
    //noinspection unchecked
    return (ArrayList<String>) entry;
  }

  private ArrayList<Integer> asInts(Object entry) {
    //noinspection unchecked
    return (ArrayList<Integer>) entry;
  }

  private void trackNodeCalls(ArrayList<String> value) {
    String owner = null;
    for (var ownerOrMethod : value) {
      if (ownerOrMethod.indexOf('/') > 0) owner = ownerOrMethod;
      else nodeCalls.putIfAbsent(new LNodeCall(owner, ownerOrMethod), null);
    }
  }

  private void findImplFields() {
    // Extract information about fields
    for (var field : cls.fields) {
      var name = field.name;
      if (name.endsWith(IMPL_SUFFIX)) implFieldMap.put(stripSuffix(name, IMPL_SUFFIX), field.desc);
    }
  }

  private void findImplMethods() {
    // Extract information about methods
    for (var method : cls.methods) {
      var name = method.name;
      if (name.endsWith(IMPL_SUFFIX))
        implMethodMap.put(stripSuffix(name, IMPL_SUFFIX), method.desc);
    }
  }

  private void findAllNodeAndTrivialFunctions() {
    for (var method : cls.methods) {
      var asyncMethod = methodAsync(method);
      if (asyncMethod != null) {
        enrichMethod(method, asyncMethod);
        asyncMethods.add(asyncMethod);
      } else if (lambdasWithCN.containsKey(method.name)) {
        var lambda = new TransformableMethod(method, classCompilerArgs);
        enrichMethod(method, lambda);
        lambdaMethods.add(lambda);
      } else enrichMethod(method, null);
    }
  }

  /** tmethod might be null if we don't care to set other values */
  private void enrichMethod(MethodNode method, TransformableMethod tmethod) {
    boolean trivial = true;
    int lineNumber = -1; // Keep track of the latest line number
    ReplacedBSM lastBSM = null; // [INNER_NAME] Candidate for name enrichment
    for (var instr : method.instructions) {
      if (instr instanceof LineNumberNode) {
        lineNumber = ((LineNumberNode) instr).line;
        if (tmethod != null && tmethod.lineNumber == 0) {
          tmethod.lineNumber = lineNumber;
          if (tmethod.clsID > 0) LPropertyDescriptor.get(tmethod.clsID).lineNumber = lineNumber;
        }
      } else if (instr instanceof MethodInsnNode) {
        trivial = false;
        // Record the fact that this method makes calls to @node/@async...
        if (tmethod != null) {
          var mi = (MethodInsnNode) instr;
          if (!tmethod.hasNodeCalls) tmethod.hasNodeCalls = isNodeCall(mi);
          if (enableCompilerDebug(mi)) tmethod.compilerArgs.debug = true;
          if (setCompilerLevelZero(mi)) tmethod.compilerArgs.level = 0;
          if (setTurnOffNodeReorder(mi)) tmethod.compilerArgs.level = 0;
          if (setAssumeGlobalMutation(mi)) tmethod.compilerArgs.assumeGlobalMutation = true;
        }
        if (lastBSM != null) enrichName(lastBSM, instr, method.name, lineNumber);
      } else if (instr instanceof InvokeDynamicInsnNode) {
        trivial = false;
        var indy = (InvokeDynamicInsnNode) instr;
        if (bsmScalaFunc.equals(indy.bsm)) {
          indy.bsm = bsmScalaFuncR;
          var replaced = new ReplacedBSM();
          replaced.indy = indy;
          replaced.lineNumber = lineNumber;
          replaced.localID = replacedBSM.size();
          lastBSM = isLocalCall(replaced); // [INNER_NAME]
          replacedBSM.add(replaced);
        }
      } else if (instr.getOpcode() == ATHROW) trivial = false;
      else if (instr.getOpcode() == PUTFIELD || instr.getOpcode() == PUTSTATIC) {
        trivial = false;
        if (tmethod != null && !tmethod.asyncOnly) {
          var methodName = cls.name + "." + method.name;
          System.err.println("LWARNING: Method " + methodName + " is unsafe to transform!");
          tmethod.unsafeToReorder = true;
        }
      }
    }

    // This should be unique (based on how scala/java create those synthetic methods
    if ((method.access & LAMBDA_CANDIDATE_ACCESS) == LAMBDA_CANDIDATE_ACCESS && trivial)
      trivialMethods.add(method.name);
    if (tmethod != null) tmethod.trivial = trivial;
  }

  private void transformNodeFunctions() {
    var methodCount = 0;
    for (var method : asyncMethods) {
      method.id = methodCount++;
      LCompiler.transform(method, this);
    }
    for (var method : lambdaMethods) {
      method.id = methodCount++;
      LCompiler.transform(method, this);
    }
  }

  private ReplacedBSM isLocalCall(ReplacedBSM rbsm) {
    var methodTarget = rbsm.getTarget();
    if (methodTarget.getOwner().equals(cls.name)) return rbsm;
    else return null;
  }

  /**
   * Three legit ways to avoid this questionable logic of detecting the enclosed-in functions
   * <li>Propagate with the rest of the information in @loom attribute i.e. for every lambda have a
   *     parallel array of functions they are enclosed in. The obvious downsides: entityplugin is
   *     needed and therefore further from support java and jar is a drop bigger
   * <li>Use the logic we have in LCompiler to properly understand the flow of the values. The
   *     downside is performance, reasonable choice if we always post compile immediately
   * <li>At runtime ... If you pass node function to a map it can gain that 'enclosed-in' meaning.
   */
  private void enrichName(
      ReplacedBSM lastBSM, AbstractInsnNode instr, String enclMethod, int line) {
    var methodInsn = (MethodInsnNode) instr;
    if (methodInsn.desc.contains("Lscala/Function") || isTweakFunction(methodInsn)) {
      var lambdaName = lastBSM.getTarget().getName();
      var lambdaRefName = stripSuffix(lambdaName, "$adapted");
      var nestInName =
          lambdasWithCN.containsKey(lambdaRefName) || isNodeCall(methodInsn) ? methodInsn.name : "";

      // Replace of "$" is just to match current (bad) behaviour of cleanNodeClassName()
      nestInName = nestInName.replace('$', '.');
      var column = lambdasWithCN.getOrDefault(lambdaRefName, COLUMN_NA);
      nameTranslate.putIfAbsent(
          lambdaRefName, new EnrichedName(enclMethod, line, column, nestInName));
    }
  }

  private boolean isTweakFunction(MethodInsnNode min) {
    return min.name.equals("$colon$eq") && min.owner.equals("optimus/graph/TweakTargetKey");
  }

  public boolean isNodeCall(MethodInsnNode methodInsn) {
    return nodeCalls.containsKey(new LNodeCall(methodInsn.owner, methodInsn.name));
  }

  public boolean isImmutable(String clsName) {
    return immutableTypes.contains(clsName);
  }

  private String enrichedName(String lambdaName, EnrichedName en) {
    if (en == null) return stripPrefix(lambdaName, SCALA_ANON_PREFIX);
    var isCtor = en.enclosingMethod.equals("<init>");
    var enclosingMethod =
        en.enclosingMethod.startsWith(SCALA_ANON_PREFIX)
            ? enrichedName(en.enclosingMethod, nameTranslate.get(en.enclosingMethod))
            : isCtor ? "" : en.enclosingMethod;
    var nestedCallIn = en.nestedCallIn.isEmpty() ? "" : (isCtor ? "" : "_") + en.nestedCallIn;
    var nestedCallLn = nestedCallIn.isEmpty() ? "" : "_" + en.lineNumber;
    var columnSuffix = en.columnNumber != COLUMN_NA ? "_" + Math.abs(en.columnNumber) : "";
    return enclosingMethod + nestedCallIn + nestedCallLn + columnSuffix;
  }

  private void implementDeserializeLLambda() {
    if (replacedBSM.isEmpty()) return; // nothing to do!

    var access = ACC_STATIC | ACC_SYNTHETIC | ACC_PRIVATE;
    var desc = DESERIALIZE_MT.toMethodDescriptorString();
    var mv = new MethodNode(access, DESERIALIZE, desc, null, null);
    cls.methods.add(mv);
    mv.visitVarInsn(Opcodes.ILOAD, 0);
    var defaultLabel = new Label();
    var labels = new Label[replacedBSM.size()];
    for (int i = 0; i < labels.length; i++) labels[i] = new Label();
    mv.visitTableSwitchInsn(0, replacedBSM.size() - 1, defaultLabel, labels);
    for (var patch : replacedBSM) {
      var indy = patch.indy;
      var methodTarget = patch.getTarget();
      var en = nameTranslate.get(stripSuffix(methodTarget.getName(), "$adapted"));
      var cleanName = enrichedName(methodTarget.getName(), en); // [INNER_NAME]
      var col = en == null ? COLUMN_NA : en.columnNumber;
      var clsID = register(cls, cleanName, patch.lineNumber, col, patch.localID);
      var bsmDesc = DESERIALIZE_BSM_MT.toMethodDescriptorString();
      var handle = dupNamed(indy.bsm, "handleFactory");
      // this needs to match NodeMetaFactory.handleFactory!
      var factoryType = Type.getMethodType(indy.desc);
      var args = new Object[5];
      args[0] = indy.bsmArgs[0];
      args[1] = methodTarget;
      args[2] = factoryType;
      args[3] = trivialMethods.contains(methodTarget.getName()) ? NF_TRIVIAL : 0;
      args[4] = clsID;

      mv.visitLabel(labels[patch.localID]);
      mv.visitFrame(Opcodes.F_SAME, 0, null, 0, null);
      mv.visitInvokeDynamicInsn(indy.name, bsmDesc, handle, args);
      mv.visitInsn(Opcodes.ARETURN);

      // Patch the original instruction
      var ctorType = Type.getMethodType(Type.VOID_TYPE, factoryType.getArgumentTypes());
      indy.bsmArgs = new Object[] {patch.localID, ctorType};
    }

    // returning null for the default case
    mv.visitLabel(defaultLabel);
    mv.visitFrame(Opcodes.F_SAME, 0, null, 0, null);
    mv.visitInsn(Opcodes.ACONST_NULL);
    mv.visitInsn(Opcodes.ARETURN);
  }

  private void implementPrivateLoomMethods() {
    for (var nm : asyncMethods) {
      nm.writeNodeSyncFunc(cls);
      // Preserve the attribute on the original method
      var newSyncEntry = cls.methods.get(cls.methods.size() - 1);
      // Preserve visible annotations so that they can be visible at runtime by user code
      newSyncEntry.visibleAnnotations = nm.method.visibleAnnotations;

      nm.writeQueuedFunc(cls);
      nm.writeNewNodeFunc(cls);

      if (nm.implFieldDesc != null) {
        cls.methods.remove(nm.method);
      } else {
        nm.method.name = nm.method.name + LOOM_SUFFIX;
        // we can just drop the annotations
        nm.method.visibleAnnotations = null;
        nm.method.invisibleAnnotations = null;
        nm.method.parameters = null;
        nm.method.access = makePrivate(nm.method.access);
      }
    }
  }

  /** Find matching NodeMethod using clean names. */
  private NodeMethod findMethod(MethodNode mn, String suffix) {
    var cleanName = stripSuffix(stripPrefix(mn.name, privatePrefix), suffix);
    for (var m : asyncMethods) {
      if (m.cleanName.equals(cleanName) && sameArguments(mn.desc, m.method.desc)) return m;
    }
    return null;
  }

  private boolean isTransforming(MethodNode method) {
    if ((method.access & ACC_BRIDGE) != 0) return false;
    if (asyncMethods.isEmpty()) return false;

    if (method.name.endsWith(QUEUED_SUFFIX)) {
      var nodeMethod = findMethod(method, QUEUED_SUFFIX);
      if (nodeMethod != null) {
        nodeMethod.queuedMethod = method;
        return true;
      }
    } else if (method.name.endsWith(NEW_NODE_SUFFIX)) {
      var nodeMethod = findMethod(method, NEW_NODE_SUFFIX);
      if (nodeMethod != null) {
        nodeMethod.newNodeMethod = method;
        return true;
      }
    }
    return false;
  }

  private NodeMethod methodAsync(MethodNode method) {
    if ((method.access & NON_NODE_ACCESS) != 0) return null;
    if (method.visibleAnnotations == null && method.invisibleAnnotations == null) return null;

    var annotations = new ArrayList<AnnotationNode>();
    if (method.visibleAnnotations != null) annotations.addAll(method.visibleAnnotations);
    if (method.invisibleAnnotations != null) annotations.addAll(method.invisibleAnnotations);

    // either @node (visible) or @async (invisible)
    AnnotationNode asyncAnnotation = null;
    var asyncOnly = false;
    var scenarioIndependent = false;

    var compilerArgs = classCompilerArgs;

    for (var ann : annotations) {
      if (NodeAnnotation.equals(ann.desc)) {
        asyncAnnotation = ann;
      } else if (AsyncAnnotation.equals(ann.desc)) {
        asyncAnnotation = ann;
        asyncOnly = true;
      } else if (ScenarioIndependentAnnotation.equals(ann.desc)) {
        scenarioIndependent = true;
      } else if (CompilerAnnotation.equals(ann.desc)) {
        compilerArgs = CompilerArgs.parse(ann, compilerArgs);
      }
    }
    if (asyncAnnotation == null) return null;

    var asyncMethod = new NodeMethod(cls, privatePrefix, method, compilerArgs);
    asyncMethod.isScenarioIndependent = scenarioIndependent;
    asyncMethod.implFieldDesc = implFieldMap.get(method.name);
    asyncMethod.implMethodDesc = implMethodMap.get(method.name);
    asyncMethod.asyncOnly = asyncOnly;
    asyncMethod.clsID = register(cls, method.name, asyncMethod.lineNumber, COLUMN_NA, -1);
    parseAsyncAnnotation(asyncMethod, asyncAnnotation);
    return asyncMethod;
  }

  private void parseAsyncAnnotation(NodeMethod method, AnnotationNode asyncAnnotation) {
    var values = asyncAnnotation.values;
    if (values != null) {
      for (int i = 0, n = values.size(); i < n; i += 2) {
        var name = (String) values.get(i);
        var value = values.get(i + 1);
        if (name.equals(ExposeArgTypesParam)) method.trait = (Boolean) value;
      }
    }
  }
}
