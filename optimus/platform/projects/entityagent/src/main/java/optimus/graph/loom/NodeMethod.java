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

import static optimus.CoreUtils.merge;
import static optimus.debug.CommonAdapter.newMethod;
import static optimus.graph.loom.LoomConfig.BSM_DESC;
import static optimus.graph.loom.LoomConfig.CMD_ASYNC;
import static optimus.graph.loom.LoomConfig.CMD_GET;
import static optimus.graph.loom.LoomConfig.CMD_GETSI;
import static optimus.graph.loom.LoomConfig.CMD_NODE;
import static optimus.graph.loom.LoomConfig.CMD_NODE_ACPN;
import static optimus.graph.loom.LoomConfig.CMD_OBSERVED_VALUE_NODE;
import static optimus.graph.loom.LoomConfig.CMD_QUEUED;
import static optimus.graph.loom.LoomConfig.NF_EXPOSE_ARGS_TRAIT;
import static optimus.graph.loom.LoomConfig.NF_TRIVIAL;
import static optimus.graph.loom.LoomConfig.NODE_DESC;
import static optimus.graph.loom.LoomConfig.NODE_FACTORY;
import static optimus.graph.loom.LoomConfig.NODE_FUTURE_TYPE;
import static optimus.graph.loom.LoomConfig.NODE_GETTER_DESC;
import static optimus.graph.loom.LoomConfig.NODE_TYPE;
import static optimus.graph.loom.NameMangler.mkImplName;
import static optimus.graph.loom.NameMangler.unmangleName;
import static org.objectweb.asm.Opcodes.H_INVOKESPECIAL;
import static org.objectweb.asm.Opcodes.H_INVOKESTATIC;
import static org.objectweb.asm.Opcodes.INVOKEINTERFACE;
import static org.objectweb.asm.Opcodes.INVOKEVIRTUAL;
import optimus.debug.CommonAdapter;
import optimus.graph.loom.compiler.LMessage;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.Handle;
import org.objectweb.asm.Label;
import org.objectweb.asm.Type;
import org.objectweb.asm.tree.ClassNode;
import org.objectweb.asm.tree.MethodNode;

public class NodeMethod extends TransformableMethod {

  private final ClassNode cls;
  private final String mangledClsName;
  final String cleanName; // Unmangled name (gets mangled when private function is accessed
  private final Type returnType; // Computed in ctor
  private final String[] argNames; // Computed in ctor
  private final boolean isInterface; // True on interface
  public MethodNode queuedMethod;
  public MethodNode newNodeMethod;

  // @node(exposeArgTypes = true)/@async(exposeArgTypes = true) needs to inherit this trait
  boolean trait;

  String implFieldDesc; // null if not a simple ($impl) field, else no need to create nodeClass
  String implMethodDesc; // null if not a simple ($impl) method, else no need to create nodeClass
  boolean isScenarioIndependent;

  public NodeMethod(ClassNode cls, String mangledClsName, MethodNode method, CompilerArgs cArgs) {
    super(method, cArgs);
    this.cls = cls;
    this.mangledClsName = mangledClsName;
    this.cleanName = unmangleName(this.mangledClsName, method.name);
    this.isInterface = CommonAdapter.isInterface(cls.access);
    if (method.parameters == null) this.argNames = null;
    else this.argNames = method.parameters.stream().map(p -> p.name).toArray(String[]::new);
    this.returnType = Type.getReturnType(method.desc);
  }

  public void writeNodeSyncFunc(ClassVisitor cv) {
    var cmd = isScenarioIndependent ? CMD_GETSI : CMD_GET;
    writeInvokeNewNode(cv, cmd, method, returnType, method.access);
  }

  public void writeQueuedFunc(ClassVisitor cv) {
    if (method.name.startsWith(mangledClsName) != queuedMethod.name.startsWith(mangledClsName)) {
      LMessage.warning("Queued Method access is different!!!!", this, cls);
    }
    // var queuedPM = isPublic(queuedMethod) && queuedMethod.name.startsWith(mangledClsName);
    // var newAccess = queuedPM ? queuedMethod.access : method.access;
    var newAccess = method.access;
    writeInvokeNewNode(cv, CMD_QUEUED, queuedMethod, NODE_FUTURE_TYPE, newAccess);
  }

  private void writeInvokeNewNode(
      ClassVisitor cv, String cmd, MethodNode org, Type returnType, int access) {
    if (newNodeMethod == null)
      LMessage.fatal("Could not find matching $newNode method! " + cls.name + "." + org.name);

    var desc = Type.getMethodDescriptor(returnType, argTypes);
    try (var mv = newMethod(cv, access, org.name, desc)) {
      mv.visitLineNumber(lineNumber, new Label());
      var newNodeDesc = Type.getMethodDescriptor(NODE_TYPE, argTypes);
      var newNodeName = newNodeMethod.name;
      var handle = new Handle(H_INVOKESPECIAL, cls.name, newNodeName, newNodeDesc, isInterface);
      invokeCmd(mv, cmd, handle, returnType, null);
    }
  }

  public void writeNewNodeFunc(ClassVisitor cv) {
    if (newNodeMethod == null) {
      LMessage.fatal("Could not find matching $newNode method! " + cls.name + ".");
    }
    var newDesc = Type.getMethodDescriptor(NODE_TYPE, argTypes);
    try (var mv = newMethod(cv, newNodeMethod.access, newNodeMethod.name, newDesc)) {
      if (NODE_DESC.equals(implFieldDesc) || NODE_GETTER_DESC.equals(implMethodDesc)) {
        mv.loadThis();
        var instr = NODE_DESC.equals(implFieldDesc) ? INVOKEVIRTUAL : INVOKEINTERFACE;
        var implName = mkImplName(mangledClsName, method.name);
        mv.visitMethodInsn(instr, cls.name, implName, NODE_GETTER_DESC, isInterface);
        mv.returnValue();
        return;
      }
      var needsImplSuffix = implFieldDesc != null || implMethodDesc != null;
      var methodToCall = needsImplSuffix ? mkImplName(mangledClsName, method.name) : method.name;
      var cmd = asyncOnly ? CMD_ASYNC : CMD_NODE; // Default....
      if (implFieldDesc != null) cmd = CMD_NODE_ACPN;
      else if (implMethodDesc != null) cmd = CMD_OBSERVED_VALUE_NODE;
      var handleIsInterface = implFieldDesc == null && isInterface;

      Handle orgHandle =
          new Handle(H_INVOKESPECIAL, cls.name, methodToCall, method.desc, handleIsInterface);
      invokeCmd(mv, cmd, orgHandle, NODE_TYPE, argNames);
    }
  }

  private void invokeCmd(
      CommonAdapter mv, String cmd, Handle handle, Type returnType, String[] argNames) {
    mv.loadThis(); /* this (entity) */
    mv.loadArgs(); /* method(args) */
    var bsmHandle = new Handle(H_INVOKESTATIC, NODE_FACTORY, "mfactory", BSM_DESC, false);

    var methodOwner = Type.getObjectType(cls.name);
    var descX = Type.getMethodDescriptor(returnType, merge(methodOwner, argTypes));

    int flags = (trivial ? NF_TRIVIAL : 0) | (trait ? NF_EXPOSE_ARGS_TRAIT : 0);
    var dflt = new Object[] {handle, cls.sourceFile, lineNumber, flags};
    var bsmParams = merge(dflt, argNames);
    mv.visitInvokeDynamicInsn(cmd, descX, bsmHandle, bsmParams);
    mv.returnValue();
  }
}
