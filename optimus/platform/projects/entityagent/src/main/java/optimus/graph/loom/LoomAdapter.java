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
import static optimus.graph.loom.LoomConfig.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import optimus.graph.loom.compiler.LCompiler;
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

  private final ArrayList<NodeMethod> asyncMethods = new ArrayList<>();
  private final ArrayList<TransformableMethod> lambdaMethods = new ArrayList<>();

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

    public EnrichedName(String enclosingMethod, int lineNumber, String nestedCallIn) {
      this.enclosingMethod = enclosingMethod;
      this.lineNumber = lineNumber;
      this.nestedCallIn = nestedCallIn;
    }
  }

  private final ArrayList<ReplacedBSM> replacedBSM = new ArrayList<>();
  private final HashSet<String> trivialMethods = new HashSet<>();
  // [INNER_NAME] local names like anon$func$1 -> enriched name
  private final HashMap<String, EnrichedName> nameTranslate = new HashMap<>();
  /* method, null*/
  private final HashMap<LNodeCall, LNodeCall> nodeCalls = new HashMap<>();
  /* caller, lambdas */
  private final HashSet<String> lambdaCalls = new HashSet<>();

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
    findAllNodeFunctions();
    transformNodeFunctions();
    cls.methods.removeIf(this::isTransforming);
    findAllTrivialFunctions();
    implementDeserializeLLambda();
    implementPrivateLoomMethods();
  }

  private void setupNodeAndLambdaCalls() {
    for (var ann : cls.invisibleAnnotations) {
      if (LoomAnnotation.equals(ann.desc)) {
        var values = ann.values;
        if (values != null) {
          for (int i = 0, n = values.size(); i < n; i += 2) {
            var name = (String) values.get(i);
            @SuppressWarnings("unchecked")
            var value = (ArrayList<String>) values.get(i + 1);
            if (name.equals(LoomNodesParam)) trackNodeCalls(value);
            else if (name.equals(LoomLambdasParam)) lambdaCalls.addAll(value);
          }
        }
      } else if (CompilerAnnotation.equals(ann.desc))
        classCompilerArgs = CompilerArgs.parse(ann, classCompilerArgs);
    }
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

  private void findAllNodeFunctions() {
    for (var method : cls.methods) {
      var asyncMethod = methodAsync(method);
      if (asyncMethod != null) asyncMethods.add(asyncMethod);
      else if (lambdaCalls.contains(method.name)) {
        var hasNodeCalls = makesNodeCalls(method);
        lambdaMethods.add(new TransformableMethod(method, classCompilerArgs, hasNodeCalls));
      }
    }
  }

  private void transformNodeFunctions() {
    for (var method : asyncMethods) if (!method.asyncOnly) LCompiler.transform(method, this);
    for (var method : lambdaMethods) LCompiler.transform(method, this);
  }

  private void findAllTrivialFunctions() {
    for (var method : cls.methods) {
      boolean trivial = true;
      int lineNumber = -1; // Keep track of the latest line number
      ReplacedBSM lastBSM = null; // [INNER_NAME] Candidate for name enrichment
      for (var instr : method.instructions) {
        if (instr instanceof LineNumberNode) lineNumber = ((LineNumberNode) instr).line;
        else if (instr instanceof MethodInsnNode) {
          trivial = false;
          if (lastBSM != null) enrichName(lastBSM, instr, method.name, lineNumber);
        } else if (instr instanceof InvokeDynamicInsnNode) {
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
          trivial = false;
        } else if (instr.getOpcode() == ATHROW) trivial = false;
      }

      // This should be unique (based on how scala/java create those synthetic methods
      if ((method.access & LAMBDA_CANDIDATE_ACCESS) == LAMBDA_CANDIDATE_ACCESS && trivial)
        trivialMethods.add(method.name);
    }
  }

  private ReplacedBSM isLocalCall(ReplacedBSM rbsm) {
    var methodTarget = rbsm.getTarget();
    if (methodTarget.getOwner().equals(cls.name)) return rbsm;
    else return null;
  }

  private void enrichName(
      ReplacedBSM lastBSM, AbstractInsnNode instr, String enclMethod, int lineNumber) {
    var methodInsn = (MethodInsnNode) instr;
    if (methodInsn.desc.contains("Lscala/Function")) {
      var lambda = stripSuffix(lastBSM.getTarget().getName(), "$adapted");
      var nestInName = isNodeCall(methodInsn) ? methodInsn.name : "";
      nameTranslate.putIfAbsent(lambda, new EnrichedName(enclMethod, lineNumber, nestInName));
    }
  }

  private boolean makesNodeCalls(MethodNode method) {
    for (var instruction : method.instructions) {
      if (instruction instanceof MethodInsnNode && isNodeCall((MethodInsnNode) instruction))
        return true;
    }
    return false;
  }

  public boolean isNodeCall(MethodInsnNode methodInsn) {
    return nodeCalls.containsKey(new LNodeCall(methodInsn.owner, methodInsn.name));
  }

  private String enrichedName(String lambdaName) {
    var en = nameTranslate.get(lambdaName);
    if (en == null) return stripPrefix(lambdaName, SCALA_ANON_PREFIX);
    var enclosingMethod =
        en.enclosingMethod.startsWith(SCALA_ANON_PREFIX)
            ? enrichedName(en.enclosingMethod)
            : en.enclosingMethod;
    var nestedCallIn = en.nestedCallIn.isEmpty() ? "" : "_" + en.nestedCallIn + "_" + en.lineNumber;
    return enclosingMethod + nestedCallIn;
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
      var cleanName = enrichedName(methodTarget.getName()); // [INNER_NAME]
      var clsID = LPropertyDescriptor.register(cls, cleanName, patch.lineNumber, patch.localID);
      var bsmDesc = DESERIALIZE_BSM_MT.toMethodDescriptorString();
      var handle = dupNamed(indy.bsm, "handleFactory");
      // this needs to match NodeMetaFactory.handleFactory!
      var factoryType = Type.getMethodType(indy.desc);
      var args = new Object[5];
      args[0] = indy.bsmArgs[0];
      args[1] = methodTarget;
      args[2] = factoryType;
      args[3] = trivialMethods.contains(methodTarget.getName()) ? FLAG_TRIVIAL : 0;
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

    var hasNodeCalls = makesNodeCalls(method);
    var asyncMethod = new NodeMethod(cls, privatePrefix, method, compilerArgs, hasNodeCalls);
    asyncMethod.isScenarioIndependent = scenarioIndependent;
    asyncMethod.implFieldDesc = implFieldMap.get(method.name);
    asyncMethod.implMethodDesc = implMethodMap.get(method.name);
    asyncMethod.asyncOnly = asyncOnly;
    asyncMethod.lineNumber = getLineNumber(method);
    asyncMethod.clsID = LPropertyDescriptor.register(cls, method.name, asyncMethod.lineNumber, -1);
    parseAsyncAnnotation(asyncMethod, asyncAnnotation);
    return asyncMethod;
  }

  private int getLineNumber(MethodNode method) {
    for (var instr : method.instructions) {
      if (instr instanceof LineNumberNode) return ((LineNumberNode) instr).line;
    }
    return -1;
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
