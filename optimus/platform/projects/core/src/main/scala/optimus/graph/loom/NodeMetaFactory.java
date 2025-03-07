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

import static java.lang.invoke.MethodHandles.Lookup.ClassOption.NESTMATE;
import static java.lang.invoke.MethodHandles.Lookup.ClassOption.STRONG;
import static java.lang.invoke.MethodHandles.filterReturnValue;
import static java.lang.invoke.MethodHandles.foldArguments;
import static java.lang.invoke.MethodHandles.insertArguments;
import static java.lang.invoke.MethodType.methodType;
import static optimus.CoreUtils.stripFromLast;
import static optimus.graph.DiagnosticSettings.injectNodeMethods;
import static optimus.graph.OGTrace.CachedSuffix;
import static optimus.graph.loom.LPropertyDescriptor.register;
import static optimus.graph.loom.LoomConfig.*;
import static optimus.graph.loom.NameMangler.stripImplSuffix;
import static optimus.graph.loom.NameMangler.stripNewNodeSuffix;
import static optimus.graph.loom.NameMangler.unmangleName;
import java.lang.invoke.CallSite;
import java.lang.invoke.ConstantCallSite;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandleInfo;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.runtime.ObjectMethods;
import optimus.graph.AlreadyCompletedNode;
import optimus.graph.AlreadyCompletedPropertyNode;
import optimus.graph.Node;
import optimus.graph.NodeTaskInfo;
import optimus.graph.NodeTaskInfoRegistry;
import optimus.graph.NodeTrace;
import optimus.graph.PropertyNode;
import optimus.platform.PluginHelpers;
import optimus.platform.storable.Entity;

public class NodeMetaFactory {
  static final MethodHandle mhLookupAndGet;
  static final MethodHandle mhLookupAndGetSI;

  static final MethodHandle mhLookupAndGetJob;
  static final MethodHandle mhLookupAndEnqueue;
  static final MethodHandle mhLookupAndEnqueueJob;
  static final MethodHandle mhPluginACPN;
  static final MethodHandle mhPluginACN;
  static final MethodHandle mhPluginOVN;

  static {
    var lookup = MethodHandles.lookup();
    try {
      mhLookupAndGet = lookup.findVirtual(Node.class, "lookupAndGet", methodType(Object.class));
      mhLookupAndGetSI = lookup.findVirtual(Node.class, "lookupAndGetSI", methodType(Object.class));
      mhLookupAndGetJob =
          lookup.findVirtual(Node.class, "lookupAndGetJob", methodType(Object.class));
      mhLookupAndEnqueue =
          lookup.findVirtual(Node.class, "lookupAndEnqueue", methodType(Node.class));
      mhLookupAndEnqueueJob =
          lookup.findVirtual(Node.class, "lookupAndEnqueueJob", methodType(Node.class));
      var acpnMethodType =
          methodType(AlreadyCompletedPropertyNode.class, Object.class, Entity.class, int.class);
      mhPluginACPN = lookup.findStatic(PluginHelpers.class, "acpn", acpnMethodType);
      var acnMethodType = methodType(AlreadyCompletedNode.class, Object.class);
      mhPluginACN = lookup.findStatic(PluginHelpers.class, "acn", acnMethodType);
      var ovnMethodType = methodType(PropertyNode.class, Object.class, Entity.class, int.class);
      mhPluginOVN = lookup.findStatic(PluginHelpers.class, "observedValueNode", ovnMethodType);
    } catch (NoSuchMethodException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("unused") // used by entityagent
  public static CallSite mfactory(
      MethodHandles.Lookup caller,
      String cmd,
      MethodType factoryType,
      MethodHandle nodeMethod,
      String source,
      int line,
      int nodeFlags,
      String... params)
      throws IllegalAccessException, NoSuchMethodException {

    var info = caller.revealDirect(nodeMethod);
    var entityCls = caller.lookupClass();
    var isEntity = Entity.class.isAssignableFrom(entityCls);

    return switch (cmd) {
      case CMD_NODE_ACPN -> {
        if (isEntity) {
          var propertyID = getPropertyID(entityCls, info);
          // acpn(e.method, e, 148 aka propertyID)
          var value = nodeMethod.asType(methodType(Object.class, Entity.class));
          var mhIncomplete = foldArguments(mhPluginACPN, value);
          yield mkConstantCallSite(insertArguments(mhIncomplete, 1, propertyID), factoryType);
        } else {
          // acn(e.method)
          var value = nodeMethod.asType(methodType(Object.class, entityCls));
          yield mkConstantCallSite(filterReturnValue(value, mhPluginACN), factoryType);
        }
      }
      case CMD_OBSERVED_VALUE_NODE -> {
        var propertyID = getPropertyID(entityCls, info);
        // observedValueNode(e.method, e, 148 aka propertyID)
        var mhIncomplete =
            foldArguments(mhPluginOVN, nodeMethod.asType(methodType(Object.class, Entity.class)));
        yield mkConstantCallSite(insertArguments(mhIncomplete, 1, propertyID), factoryType);
      }
      case CMD_NODE, CMD_ASYNC -> {
        var methodName = unmangleName(entityCls, info.getName());
        var clsID = register(entityCls, methodName, source, line, COLUMN_NA, -1);
        var pd = LPropertyDescriptor.get(clsID);
        pd.methodType = info.getMethodType();
        // Consider: Making all of the flags should come from byte code
        var customTrait = (nodeFlags & NF_EXPOSE_ARGS_TRAIT) != 0;
        var async = cmd.startsWith(CMD_ASYNC);
        var hasProperty = (isEntity && !async) || customTrait;
        var propertyID = hasProperty ? getPropertyID(entityCls, info) : -1;
        var flags = nodeFlags;
        flags |= isEntity && !async ? 0 : NF_PLAIN_ASYNC;
        if (hasProperty && NodeTrace.forID(propertyID).hasTweakHandler()) flags |= NF_TWEAKHANDLER;
        var gen = new NodeClassGenerator(propertyID, info, factoryType, flags);
        gen.argNames = params;
        gen.clsID = clsID;
        if (propertyID < 0) assignProfileID(pd); // For profiling of non-nodes
        yield mkConstantCallSite(mkCtorHandle(caller, gen), factoryType);
      }
      case CMD_GET -> {
        var propertyID = getPropertyID(entityCls, info);
        var nti = NodeTrace.forID(propertyID);
        var holder = new CallSiteHolder(caller, entityCls, factoryType, nti, info, nodeMethod);
        nti.callSiteHolder = holder;
        yield holder.mutableCallsite;
      }
      case CMD_GETSI -> mkConstantCallSite(
          filterReturnValue(nodeMethod, mhLookupAndGetSI), factoryType);
      case CMD_QUEUED -> mkConstantCallSite(
          filterReturnValue(nodeMethod, mhLookupAndEnqueue), factoryType);
      default -> throw new NoSuchMethodException(cmd + " not found");
    };
  }

  private static ConstantCallSite mkConstantCallSite(MethodHandle handle, MethodType factoryType) {
    var methodHandle = handle.asType(factoryType);
    return new ConstantCallSite(methodHandle);
  }

  private static MethodHandle mkCtorHandle(MethodHandles.Lookup caller, NodeClassGenerator gen)
      throws NoSuchMethodException, IllegalAccessException {
    var nodeClass = mkClass(caller, gen);
    var ctorType = MethodType.fromMethodDescriptorString(gen.ctorDesc, nodeClass.getClassLoader());
    return caller.findConstructor(nodeClass, ctorType);
  }

  private static Class<?> mkClass(MethodHandles.Lookup caller, NodeClassGenerator gen) {
    var bytes = gen.generateArgsClass();
    try {
      return caller.defineHiddenClass(bytes, false, NESTMATE, STRONG).lookupClass();
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("unused") // used by entityagent
  public static CallSite altMetafactory(
      MethodHandles.Lookup caller, String invokedName, MethodType invokedType, Object... args)
      throws Throwable {

    var localID = (int) args[0];
    var ctorType = (MethodType) args[1];
    var serializer = caller.findStatic(caller.lookupClass(), DESERIALIZE, DESERIALIZE_MT);
    var nodeClass = (Class<?>) serializer.invokeExact(localID);

    var handle = caller.findConstructor(nodeClass, ctorType);
    return new ConstantCallSite(handle.asType(invokedType));
  }

  @SuppressWarnings("unused") // used by entityagent
  public static CallSite handleFactory(
      MethodHandles.Lookup caller, String invokedName, MethodType localType, Object... args) {
    var interfaceMethodType = (MethodType) args[0];
    var implementation = (MethodHandle) args[1];
    var invokedType = (MethodType) args[2];
    var lambdaFlags = (int) args[3];
    var methodName = (String) args[4];
    var source = (String) args[5];
    var lineNumber = (int) args[6];
    var columnNumber = (int) args[7];
    var localId = (int) args[8];

    var entityCls = caller.lookupClass();
    var info = caller.revealDirect(implementation);
    int flags = NF_NODE_FUNCTION;
    flags |= lambdaFlags & NF_TRIVIAL;

    var clsID = register(entityCls, methodName, source, lineNumber, columnNumber, localId);
    assignProfileID(LPropertyDescriptor.get(clsID));

    var gen = new NodeClassGenerator(1, info, invokedType, flags);
    gen.runMethod = invokedName;
    gen.runMethodType = interfaceMethodType;
    gen.clsID = clsID;

    var cls = mkClass(caller, gen);
    return new ConstantCallSite(MethodHandles.constant(Class.class, cls));
  }

  @SuppressWarnings("unused") // used by entityagent
  public static CallSite objMethod(
      MethodHandles.Lookup caller, String method, MethodType invokedType, MethodHandle... args)
      throws Throwable {
    var cls = caller.lookupClass();
    var mh =
        (MethodHandle)
            ObjectMethods.bootstrap(caller, method, MethodHandle.class, cls, "a;b;c", args);
    return new ConstantCallSite(mh.asType(invokedType));
  }

  private static void assignProfileID(LPropertyDescriptor pd) {
    // If debugging/tracing enabled allocate independent profile info per property
    pd.profileID = injectNodeMethods ? NodeTaskInfoRegistry.nextId() : NodeTaskInfo.Default.profile;
  }

  /**
   * This function needs to be as performant as possible, so we use mutability (local to the method,
   * not be exposed!) -- see currentClass and tryModule
   */
  private static int getPropertyID(Class<?> entityCls, MethodHandleInfo info) {
    var unmangledName = unmangleName(entityCls, info.getName());
    var methodName = stripImplSuffix(stripNewNodeSuffix(unmangledName));
    int id;
    String currentClass = entityCls.getName();
    boolean tryModule = true;
    while (!currentClass.isEmpty()) {
      id = NodeTaskInfoRegistry.getId(entityCls.getName(), methodName);
      if (id > 0) return id;

      if (tryModule && !currentClass.endsWith(CachedSuffix)) {
        tryModule = false;
        try {
          Class.forName(currentClass + CachedSuffix, true, entityCls.getClassLoader());
        } catch (ClassNotFoundException ignored) {
        }
        continue;
      }

      currentClass = stripFromLast(currentClass, '$');
      if (currentClass.isEmpty()) break;
      tryModule = true;
      try {
        Class.forName(currentClass, true, entityCls.getClassLoader());
      } catch (ClassNotFoundException ignored) {
      }
    }

    return NodeTaskInfo.Default.profile;
  }
}
