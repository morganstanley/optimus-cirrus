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
import static org.objectweb.asm.Opcodes.INVOKESTATIC;
import java.lang.invoke.MethodType;
import java.util.HashMap;
import org.objectweb.asm.Handle;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.Method;
import org.objectweb.asm.tree.MethodInsnNode;

public class LoomConfig {

  public static final int COLUMN_NA = Integer.MIN_VALUE;

  static final int NF_EXPOSE_ARGS_TRAIT = 1;
  static final int NF_PLAIN_ASYNC = 1 << 1;
  static final int NF_PLAIN_LAMBDA = 1 << 2;
  static final int NF_NODE_FUNCTION = NF_PLAIN_LAMBDA | (1 << 3);
  static final int NF_TWEAKHANDLER = 1 << 4;
  /** Functions and lambdas that don't make any calls or set any fields... */
  public static final int NF_TRIVIAL = 1 << 5;

  public static final String LoomAnnotation = "Loptimus/platform/loom;";
  public static final String LoomNodesParam = "nodes";
  public static final String LoomLambdasParam = "lambdas";
  public static final String LoomLcnParam = "lcn";
  public static final String LoomImmutablesParam = "immutables";

  public static final String CompilerAnnotation = "Loptimus/platform/compiler;";
  public static final String CompilerLevelParam = "level";
  public static final String CompilerDebugParam = "debug";
  public static final String CompilerEnqueueEarlierParam = "enqueueEarlier";
  public static final String CompilerQueueSizeSensitiveParam = "queueSizeSensitive";
  public static final String CompilerAssumeGlobalMutationParam = "assumeGlobalMutation";

  public static final String NodeAnnotation = "Loptimus/platform/node;";
  public static final String AsyncAnnotation = "Loptimus/platform/async;";
  public static final String ExposeArgTypesParam = "exposeArgTypes";

  public static final String ScenarioIndependentAnnotation =
      "Loptimus/platform/scenarioIndependent;";

  public static final String TO_VALUE_PREFIX = "toValue$";

  public static final String NODE_FACTORY = "optimus/graph/loom/NodeMetaFactory";
  public static final String INTERCEPTED_BSM_DESC =
      "(Ljava/lang/invoke/MethodHandles$Lookup;Ljava/lang/String;Ljava/lang/invoke/MethodType;[Ljava/lang/Object;)Ljava/lang/invoke/CallSite;";
  public static final String INTERCEPTED_BSM_OWNER = "java/lang/invoke/LambdaMetafactory";
  public static final String INTERCEPTED_BSM_METHOD = "altMetafactory";

  public static final String DESERIALIZE = "$deserializeLLambda$";

  public static final String SCALA_ANON_PREFIX = "$anonfun$";

  public static final MethodType DESERIALIZE_MT = methodType(Class.class, int.class);
  public static final MethodType DESERIALIZE_BSM_MT = methodType(Class.class);

  public static final Handle bsmScalaFunc =
      new Handle(6, INTERCEPTED_BSM_OWNER, INTERCEPTED_BSM_METHOD, INTERCEPTED_BSM_DESC, false);
  public static final Handle bsmScalaFuncR =
      new Handle(6, NODE_FACTORY, INTERCEPTED_BSM_METHOD, INTERCEPTED_BSM_DESC, false);

  public static final Type ARRAY_BOOLEAN_TYPE = Type.getType("[Z");
  public static final Type ARRAY_CHAR_TYPE = Type.getType("[C");
  public static final Type ARRAY_FLOAT_TYPE = Type.getType("[F");
  public static final Type ARRAY_DOUBLE_TYPE = Type.getType("[D");
  public static final Type ARRAY_BYTE_TYPE = Type.getType("[B");
  public static final Type ARRAY_SHORT_TYPE = Type.getType("[S");
  public static final Type ARRAY_INT_TYPE = Type.getType("[I");
  public static final Type ARRAY_LONG_TYPE = Type.getType("[L");

  public static final String NODE_DESC = "Loptimus/graph/Node;";
  public static final String NODE = "optimus/graph/Node";

  public static final String NODE_GETTER_DESC = "()" + NODE_DESC;
  public static final Type NODE_TYPE = Type.getObjectType(NODE);

  public static final String NODE_FUTURE_DESC = "Loptimus/graph/NodeFuture;";
  public static final String NODE_FUTURE = "optimus/graph/NodeFuture";
  public static final Type NODE_FUTURE_TYPE = Type.getObjectType(NODE_FUTURE);

  public static final String ENTITY_DESC = "optimus/platform/storable/Entity";
  public static final Type ENTITY_TYPE = Type.getObjectType(ENTITY_DESC);

  public static final Type NODE_TASK_INFO = Type.getObjectType("optimus/graph/NodeTaskInfo");
  public static final Type NODE_TRACE = Type.getObjectType("optimus/graph/NodeTrace");
  public static final Method NODE_TRACE_FORID =
      new Method("forID", NODE_TASK_INFO, new Type[] {Type.INT_TYPE});
  public static final String PROPERTY_NODE_LOOM_DESC = Type.getMethodDescriptor(Type.VOID_TYPE);

  public static final String CMD_NODE = "node";
  public static final String CMD_ASYNC = "nodeAsync";
  public static final String CMD_NODE_ACPN = "nodeACPN";
  public static final String CMD_OBSERVED_VALUE_NODE = "nodeOVN";
  public static final String CMD_QUEUED = "queued";
  public static final String CMD_GET = "get";
  public static final String CMD_GETSI = "getSI";

  private static final Type STRING_TYPE = Type.getObjectType("java/lang/String");

  public static final String BSM_DESC =
      Type.getMethodDescriptor(
          Type.getObjectType("java/lang/invoke/CallSite"), // ...return...
          Type.getObjectType("java/lang/invoke/MethodHandles$Lookup"), // ...args...
          STRING_TYPE, // cmd
          Type.getObjectType("java/lang/invoke/MethodType"),
          Type.getObjectType("java/lang/invoke/MethodHandle"),
          STRING_TYPE, // source
          Type.INT_TYPE, // line
          Type.INT_TYPE, // nodeFlags
          Type.getObjectType("[Ljava/lang/String;"));

  private static final HashMap<LNodeCall, Boolean> trivialCalls = new HashMap<>();

  private static void registerTC(String owner, String method, String desc) {
    trivialCalls.put(new LNodeCall(owner, method, desc), Boolean.TRUE);
  }

  static {
    registerTC("scala/Predef$", "Map", "()Lscala/collection/immutable/Map$;");
    registerTC("scala/collection/immutable/Map$", "empty", "()Lscala/collection/immutable/Map;");
    registerTC("optimus/scala212/DefaultSeq$", "Seq", "()Lscala/collection/Seq$;");
    registerTC("scala/collection/Seq$", "empty", "()Lscala/collection/SeqOps;");
    registerTC("scala/collection/immutable/Seq$", "empty", "()Lscala/collection/SeqOps;");
    registerTC("scala/Predef$", "Set", "()Lscala/collection/immutable/Set$;");
    registerTC("scala/collection/immutable/Set$", "empty", "()Lscala/collection/immutable/Set;");
    registerTC("scala/package$", "List", "()Lscala/collection/immutable/List$;");
    registerTC("scala/collection/immutable/List$", "empty", "()Lscala/collection/immutable/List;");
  }

  // TODO (OPTIMUS-66991): revisit once we are on Scala 2.13 only
  private static Class<?> seqClass() {
    try {
      return Class.forName("scala.collection.Seq");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  public static final String TRANSFORM_TWEAK_DESC =
      methodType(seqClass(), Object.class).toMethodDescriptorString();
  public static final String ALSO_SET_SUFFIX = "_$colon$eq";
  public static final Type SEQ = Type.getObjectType(seqClass().getName().replace(".", "/"));

  private static final String PLUGIN_HELPERS = "optimus/platform/PluginHelpers";
  private static final String PH_ENABLE_DEBUG = "enableCompilerDebug";
  private static final String PH_LEVEL_ZERO = "setCompilerLevelZero";

  private static final String FLOW_CONTROL = "optimus/platform/FlowControl";
  private static final String TURN_OFF_REORDER = "turnOffNodeReorder";
  private static final String ASSUME_GLOBAL_MUTATION = "mutatesGlobalState";

  public static boolean enableCompilerDebug(MethodInsnNode mi) {
    return mi.getOpcode() == INVOKESTATIC
        && mi.owner.equals(PLUGIN_HELPERS)
        && mi.name.equals(PH_ENABLE_DEBUG);
  }

  public static boolean setCompilerLevelZero(MethodInsnNode mi) {
    return mi.getOpcode() == INVOKESTATIC
        && mi.owner.equals(PLUGIN_HELPERS)
        && mi.name.equals(PH_LEVEL_ZERO);
  }

  public static boolean setTurnOffNodeReorder(MethodInsnNode mi) {
    return mi.getOpcode() == INVOKESTATIC
        && mi.owner.equals(FLOW_CONTROL)
        && mi.name.equals(TURN_OFF_REORDER);
  }

  public static boolean setAssumeGlobalMutation(MethodInsnNode mi) {
    return mi.getOpcode() == INVOKESTATIC
        && mi.owner.equals(FLOW_CONTROL)
        && mi.name.equals(ASSUME_GLOBAL_MUTATION);
  }

  public static boolean isTrivialCall(MethodInsnNode mi) {
    return trivialCalls.containsKey(new LNodeCall(mi.owner, mi.name, mi.desc));
  }
}
