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

import static java.lang.invoke.MethodType.methodType;
import static optimus.CoreUtils.dropFirst;
import static optimus.CoreUtils.stripPrefix;
import static optimus.debug.CommonAdapter.asTypes;
import static optimus.debug.CommonAdapter.newMethod;
import static optimus.debug.InstrumentationConfig.*;
import static optimus.graph.loom.LoomConfig.*;
import java.lang.invoke.CallSite;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandleInfo;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import optimus.graph.NodeKey;
import optimus.graph.OGSchedulerContext;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.ClassWriterEx;
import org.objectweb.asm.Handle;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;

/**
 * Class that generates PropertyNodeLoomD derived class [SEE_PROP_NODE_GEN] <br>
 * Note: Optimization ideas<br>
 * LLI and ILL classes can share most (except for args()). This is an improvement because of two
 * reasons: (1) argsEquals/argsHash will jit faster (2) And because the very base version is going
 * to prioritize primitive values, argsEquals will return false faster.
 */
public class NodeClassGenerator implements Opcodes {
  public static final String NODE_PROPERTY_PRIVATE = Type.getInternalName(LNodeProperty.class);
  public static final String NODE_ASYNC_PRIVATE = Type.getInternalName(LNodeAsync.class);

  public static final String[] NODE_FUNCTIONS;
  private static final String[] DEFAULT_NAMES;

  public static final MethodType bsmObjectMethodsType =
      methodType(
          CallSite.class,
          MethodHandles.Lookup.class,
          String.class,
          MethodType.class,
          MethodHandle[].class);

  public static Handle bsmObjectMethods =
      new Handle(
          6,
          Type.getInternalName(NodeMetaFactory.class),
          "objMethod",
          bsmObjectMethodsType.descriptorString(),
          false);

  static final int EXPOSE_ARGS_TRAIT = 1;
  static final int PLAIN_ASYNC = 1 << 1;
  static final int PLAIN_LAMBDA = 1 << 2;
  static final int NODE_FUNCTION = PLAIN_LAMBDA | (1 << 3);
  static final int TWEAKHANDLER = 1 << 4;

  static {
    var names = new ArrayList<String>();
    var lnodeFunctions = new ArrayList<String>();
    var lnodeBase = Type.getInternalName(LNodeFunction.class);
    for (int i = 0; i < 23; i++) {
      names.add("arg" + i);
      lnodeFunctions.add(lnodeBase + i);
    }
    DEFAULT_NAMES = names.toArray(new String[0]);
    NODE_FUNCTIONS = lnodeFunctions.toArray(new String[0]);
  }

  private static final MethodType MT_ARGS_HASH = methodType(int.class);
  private static final MethodType MT_ARGS_EQUALS = methodType(boolean.class, NodeKey.class);
  private static final MethodType MT_EQUALS = methodType(boolean.class, Object.class);
  private static final MethodType MT_CLSID = methodType(int.class);
  private static final MethodType FuncMethodType = MethodType.methodType(Object.class);

  private final int propertyID;
  private final boolean isInterface;
  private final boolean isStatic;
  private final String nodeClsName;
  final Type nodeClsType;
  private final Type[] cArgs; // captured arguments
  private final Type returnType;
  private final Type methodOwner;
  private final String methodName;
  private final String methodDesc;
  private final MethodType methodType;
  private final MethodType factoryType;
  public String runMethod = "func";
  public MethodType runMethodType = FuncMethodType;
  public int clsID;
  String[] argNames = DEFAULT_NAMES;
  private final boolean privateClass;
  final String ctorDesc;
  final int flags;

  private boolean exposeArgsTrait() {
    return (flags & EXPOSE_ARGS_TRAIT) != 0;
  }

  private boolean isAsync() {
    return (flags & PLAIN_ASYNC) != 0;
  }

  private boolean isLambda() {
    return (flags & PLAIN_LAMBDA) != 0;
  }

  private boolean isNodeFunction() {
    return (flags & NODE_FUNCTION) == NODE_FUNCTION;
  }

  private boolean plainNode() {
    return (flags & (PLAIN_ASYNC | PLAIN_LAMBDA)) == 0;
  }

  private boolean hasTweakHandler() {
    return (flags & TWEAKHANDLER) != 0;
  }

  public NodeClassGenerator(int propertyID, MethodHandleInfo info, MethodType invoker, int flags) {
    this.privateClass = true;
    this.flags = flags;
    this.propertyID = propertyID;
    this.factoryType = invoker;

    var loadingCLass = info.getDeclaringClass();
    this.isInterface = loadingCLass.isInterface();
    this.isStatic = Modifier.isStatic(info.getModifiers());
    this.methodOwner = Type.getType(loadingCLass);
    this.methodName = info.getName();
    this.methodType = info.getMethodType();
    this.methodDesc = methodType.toMethodDescriptorString();

    var argsToDrop = isStatic ? 0 : 1;
    var ctorArgs = asTypes(factoryType.parameterArray());
    this.ctorDesc = Type.getMethodDescriptor(Type.VOID_TYPE, ctorArgs);
    this.cArgs = dropFirst(ctorArgs, argsToDrop); // propertyInfo, entity
    this.returnType = Type.getType(methodType.returnType());

    var propertySuffix = propertyID > 0 ? "-" + propertyID : "";
    var simpleName = stripPrefix(methodName, SCALA_ANON_PREFIX) + propertySuffix;
    this.nodeClsName = methodOwner.getInternalName() + "_" + simpleName;
    this.nodeClsType = Type.getObjectType(nodeClsName);
  }

  private String getSuperName() {
    return isNodeFunction()
        ? NODE_FUNCTIONS[runMethodType.parameterCount()]
        : isLambda() ? OBJECT_CLS_NAME : (isAsync() ? NODE_ASYNC_PRIVATE : NODE_PROPERTY_PRIVATE);
  }

  private String exposeArgsTraitName() {
    var owner = methodOwner.getInternalName();
    var separator = owner.endsWith("$") ? "" : "$";
    return owner + separator + methodName + "$node";
  }

  private String[] getInterfaces() {
    var interfaces = new ArrayList<String>();
    if (exposeArgsTrait()) interfaces.add(exposeArgsTraitName());
    if (isLambda()) interfaces.add(Type.getInternalName(factoryType.returnType()));
    if ((flags & FLAG_TRIVIAL) != 0) interfaces.add(Type.getInternalName(ITrivialLamdba.class));
    return interfaces.toArray(new String[0]);
  }

  Type entityFieldType() {
    return privateClass ? methodOwner : ENTITY_TYPE;
  }

  public byte[] generateArgsClass() {
    var cw = new ClassWriterEx(ClassWriter.COMPUTE_FRAMES);

    cw.visit(V11, ACC_PUBLIC | ACC_SUPER, nodeClsName, null, getSuperName(), getInterfaces());
    generateFields(cw);
    generateConstructor(cw);
    generateClsID(cw);
    if (plainNode()) {
      generatePropertyInfo(cw, "propertyInfo");
      if (cArgs.length > 0) {
        generateArgsHash(cw, "argsHash");
        generateArgsEquals(cw, "argsEquals", MT_ARGS_EQUALS);
      }
      if (hasTweakHandler()) generateTransformTweak(cw);
      cw.writeAccessor(nodeClsName, "entity", entityFieldType(), ENTITY_TYPE);
    } else {
      // Note: even plain async can get an ID (for example exposeArgsTrait = true)
      if (propertyID > 1) generatePropertyInfo(cw, "executionInfo");
      // Serialization requires getting an access to the instance (let's call it "entity")
      cw.writeAccessor(nodeClsName, "entity", entityFieldType(), OBJECT_TYPE);
    }

    if (isLambda() && cArgs.length > 0) {
      generateArgsHash(cw, "argsHashCode");
      generateArgsEquals(cw, "equals", MT_EQUALS);
    }

    if (privateClass) {
      generateRunMethod(cw);
      // Serialization requires getting an access to all the args
      if (cArgs.length > 0) generateArgs(cw);

      if (exposeArgsTrait()) {
        cw.writeAccessor(nodeClsName, "entity", entityFieldType(), entityFieldType());
        generateTraitsOverrides(cw);
      }
    } else cw.writeAccessor(nodeClsName, "propertyInfo", NODE_TASK_INFO);
    cw.visitEnd();
    return cw.toByteArray();
  }

  /**
   * @see optimus.graph.loom.LNodeClsID#_clsID()
   */
  private void generateClsID(ClassWriterEx cw) {
    try (var mv = newMethod(cw, ACC_PUBLIC, "_clsID", MT_CLSID.descriptorString())) {
      mv.push(clsID);
      mv.returnValue();
    }
  }

  /**
   * @see optimus.graph.NodeTask#run( OGSchedulerContext)
   * @see optimus.graph.Node#func()
   */
  private void generateRunMethod(ClassWriter cw) {
    String desc = runMethodType.toMethodDescriptorString();
    try (var mv = newMethod(cw, ACC_PUBLIC, runMethod, desc)) {
      if (!isStatic) mv.getThisField(nodeClsName, "entity", methodOwner);
      for (int i = 0; i < cArgs.length; i++) {
        mv.getThisField(nodeClsName, argNames[i], cArgs[i]);
      }
      if (isLambda()) {
        var forwardedArgs = methodType.dropParameterTypes(0, cArgs.length).parameterArray();
        mv.loadArgsWithCast(forwardedArgs);
      }
      var methodName = isLambda() ? this.methodName : this.methodName + LOOM_SUFFIX;
      var methodOwner = this.methodOwner.getInternalName();
      var opCode = isStatic ? INVOKESTATIC : isInterface ? INVOKEINTERFACE : INVOKEVIRTUAL;
      mv.visitMethodInsn(opCode, methodOwner, methodName, methodDesc, isInterface);
      if (methodType.returnType() != runMethodType.returnType()) mv.valueOfForScala(returnType);
      mv.returnValue();
    }
  }

  private void generateTraitsOverrides(ClassWriterEx cw) {
    for (int i = 0; i < cArgs.length; i++) {
      cw.writeAccessor(nodeClsName, argNames[i], cArgs[i]);
    }
  }

  private void generateFields(ClassWriterEx cw) {
    if (!privateClass) cw.writeField("propertyID", Type.INT_TYPE);
    if (!isStatic) cw.writeField("entity", entityFieldType());
    for (int i = 0; i < cArgs.length; i++) cw.writeField(argNames[i], cArgs[i]);
  }

  private void generateConstructor(ClassWriter cw) {
    try (var mv = newMethod(cw, ACC_PUBLIC, "<init>", ctorDesc)) {
      var argSlot = 1; // skip this...
      if (!privateClass) argSlot += assignField(mv, argSlot, "propertyID", Type.INT_TYPE);
      if (!isStatic) argSlot += assignField(mv, argSlot, "entity", entityFieldType());

      for (int i = 0; i < cArgs.length; i++) {
        argSlot += assignField(mv, argSlot, argNames[i], cArgs[i]);
      }
      mv.loadThis();
      mv.invokeInitCtor(Type.getObjectType(getSuperName()), PROPERTY_NODE_LOOM_DESC);
      mv.returnValue();
    }
  }

  private int assignField(GeneratorAdapter mv, int argSlot, String name, Type arg) {
    mv.loadThis();
    mv.visitVarInsn(arg.getOpcode(ILOAD), argSlot);
    mv.putField(nodeClsType, name, arg);
    return arg.getSize();
  }

  /**
   * @see optimus.graph.PropertyNode#argsEquals( NodeKey)
   * @see optimus.graph.loom.LNodeFunction#equals( Object)
   */
  private void generateArgsEquals(ClassWriter cw, String name, MethodType mt) {
    generateArgsMethods("equals", cw, name, mt);
  }

  /**
   * @see optimus.graph.PropertyNode#argsHash()
   * @see optimus.graph.loom.LNodeFunction#argsHashCode()
   */
  private void generateArgsHash(ClassWriter cw, String name) {
    generateArgsMethods("hashCode", cw, name, MT_ARGS_HASH);
  }

  private Handle[] argsHandles = null; // Allocate once and reuse

  private void generateArgsMethods(String argMethod, ClassWriter cw, String name, MethodType mt) {
    if (argsHandles == null) {
      argsHandles = new Handle[cArgs.length];
      for (int i = 0; i < cArgs.length; i++) {
        argsHandles[i] = new Handle(1, nodeClsName, argNames[i], cArgs[i].getDescriptor(), false);
      }
    }
    try (var mv = newMethod(cw, ACC_PUBLIC, name, mt.descriptorString())) {
      mv.loadThis();
      String desc;
      if (mt.parameterCount() == 1) {
        mv.loadArg(0);
        desc = Type.getMethodDescriptor(Type.BOOLEAN_TYPE, OBJECT_TYPE, OBJECT_TYPE);
      } else desc = Type.getMethodDescriptor(Type.INT_TYPE, OBJECT_TYPE);

      mv.invokeDynamic(argMethod, desc, bsmObjectMethods, (Object[]) argsHandles);
      mv.returnValue();
    }
  }

  /**
   * @see optimus.graph.PropertyNode#args()
   */
  private void generateArgs(ClassWriter cw) {
    String descriptor = Type.getMethodDescriptor(Type.getObjectType(OBJECT_ARR_DESC));

    try (var mv = newMethod(cw, ACC_PUBLIC | ACC_FINAL, "args", descriptor)) {
      mv.push(cArgs.length);
      mv.visitTypeInsn(ANEWARRAY, OBJECT_CLS_NAME);
      for (int i = 0; i < cArgs.length; i++) {
        mv.dup();
        mv.push(i);
        mv.getThisField(nodeClsName, argNames[i], cArgs[i]);
        mv.valueOf(cArgs[i]);
        mv.visitInsn(AASTORE);
      }
      mv.returnValue();
    }
  }

  /**
   * @see optimus.graph.NodeTask#executionInfo()
   * @see optimus.graph.PropertyNode#propertyInfo()
   */
  private void generatePropertyInfo(ClassWriter cw, String accessorName) {
    String descriptor = Type.getMethodDescriptor(NODE_TASK_INFO);
    var access = ACC_PUBLIC | ACC_FINAL;
    try (var mv = newMethod(cw, access, accessorName, descriptor)) {
      if (privateClass) mv.push(propertyID);
      else mv.getThisField(nodeClsName, "propertyID", Type.INT_TYPE);
      mv.invokeStatic(NODE_TRACE, NODE_TRACE_FORID);
      mv.returnValue();
    }
  }

  /**
   * @see optimus.graph.PropertyNode#transformTweak( Object)
   */
  private void generateTransformTweak(ClassWriterEx cw) {
    var access = ACC_PUBLIC | ACC_FINAL;
    try (var mv = newMethod(cw, access, "transformTweak", TRANSFORM_TWEAK_DESC)) {
      mv.getThisField(nodeClsName, "entity", entityFieldType()); // move this to common adaptor?
      mv.loadArgsWithCast(returnType);
      var desc = Type.getMethodDescriptor(SEQ, returnType);
      var ownder = entityFieldType().getInternalName();
      var alsoSetName = methodName + ALSO_SET_SUFFIX;
      var instr = isInterface ? INVOKEINTERFACE : INVOKEVIRTUAL;
      mv.visitMethodInsn(instr, ownder, alsoSetName, desc, isInterface);
      mv.returnValue();
    }
  }
}
