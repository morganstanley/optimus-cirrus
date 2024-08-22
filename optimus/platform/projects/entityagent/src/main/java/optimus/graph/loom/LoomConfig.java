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
import java.lang.invoke.MethodType;
import org.objectweb.asm.Handle;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.Method;

public class LoomConfig {
  public static final int FLAG_TRIVIAL = 1 << 5;

  public static final String LoomAnnotation = "Loptimus/platform/loom;";
  public static final String LoomNodesParam = "nodes";
  public static final String LoomLambdasParam = "lambdas";

  public static final String CompilerAnnotation = "Loptimus/platform/compiler;";
  public static final String CompilerLevelParam = "level";
  public static final String CompilerDebugParam = "debug";
  public static final String CompilerEnqueueEarlierParam = "enqueueEarlier";
  public static final String CompilerQueueSizeSensitiveParam = "queueSizeSensitive";

  public static final String NodeAnnotation = "Loptimus/platform/node;";
  public static final String AsyncAnnotation = "Loptimus/platform/async;";
  public static final String ExposeArgTypesParam = "exposeArgTypes";

  public static final String ScenarioIndependentAnnotation =
      "Loptimus/platform/scenarioIndependent;";

  public static final String LOOM_SUFFIX = "$_";
  public static final String PLAIN_SUFFIX = "__";
  public static final String NEW_NODE_SUFFIX = "$newNode";
  public static final String QUEUED_SUFFIX = "$queued";
  public static final String IMPL_SUFFIX = "$impl";

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

  public static final String NODE_DESC = "Loptimus/graph/Node;";
  public static final String NODE = "optimus/graph/Node";

  public static final String NODE_GETTER_DESC = "()" + NODE_DESC;
  public static final Type NODE_TYPE = Type.getObjectType(NODE);

  public static final String ENTITY_DESC = "optimus/platform/storable/Entity";
  public static final Type ENTITY_TYPE = Type.getObjectType(ENTITY_DESC);

  public static final Type NODE_TASK_INFO = Type.getObjectType("optimus/graph/NodeTaskInfo");
  public static final Type NODE_TRACE = Type.getObjectType("optimus/graph/NodeTrace");
  public static final Method NODE_TRACE_FORID =
      new Method("forID", NODE_TASK_INFO, new Type[] {Type.INT_TYPE});
  public static final String PROPERTY_NODE_LOOM_DESC = Type.getMethodDescriptor(Type.VOID_TYPE);

  public static final String CMD_NODE = "node";
  public static final String CMD_ASYNC = "nodeAsync";

  public static final String TRAIT_SUFFIX = "Ex";
  public static final String CMD_NODE_WITH_TRAIT = CMD_NODE + TRAIT_SUFFIX;
  public static final String CMD_ASYNC_WITH_TRAIT = CMD_ASYNC + TRAIT_SUFFIX;
  public static final String CMD_NODE_ACPN = "nodeACPN";
  public static final String CMD_OBSERVED_VALUE_NODE = "nodeOVN";
  public static final String CMD_QUEUED = "queued";
  public static final String CMD_GET = "get";
  public static final String CMD_GETSI = "getSI";

  public static final String BSM_DESC =
      "(Ljava/lang/invoke/MethodHandles$Lookup;Ljava/lang/String;Ljava/lang/invoke/MethodType;Ljava/lang/invoke/MethodHandle;I[Ljava/lang/String;)Ljava/lang/invoke/CallSite;";

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
}
