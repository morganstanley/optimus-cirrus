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

import static optimus.CoreUtils.merge;
import static optimus.graph.loom.NameMangler.mangleName;
import static optimus.graph.loom.NameMangler.mkNewNodeName;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serial;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import optimus.graph.loom.LPropertyDescriptor;
import optimus.platform.ScenarioStack;
/**
 * Hidden Node Class Moniker
 *
 * <p>Notes: Hidden Node Classes are created during invokedynamic in methods that represent `@node`
 * and `@async` They are loaded as hidden classes on the fly. As such, regular serialization doesn't
 * work and those classes are expected to implement custom serialization In this case they implement
 * writeReplace() = new HNCMoniker()
 *
 * @see java.lang.invoke.MethodHandles.Lookup#defineHiddenClass(byte[], boolean,
 *     MethodHandles.Lookup.ClassOption...)
 */
public class HNCMoniker implements Externalizable {
  CompletableNode<Object> node;
  Object instance; // Could be storable.Entity or just any object

  public HNCMoniker() {}

  @SuppressWarnings("unchecked")
  public HNCMoniker(CompletableNode<?> node, Object instance) {
    this.node = (CompletableNode<Object>) node;
    this.instance = instance;
  }

  @Serial
  private Object readResolve() {
    return node;
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    var propDesc = LPropertyDescriptor.get(node._clsID());
    if (propDesc.methodType == null) throw new GraphInInvalidState("methodType is not setup");
    out.writeObject(propDesc.className);
    out.writeObject(propDesc.methodName);
    out.writeObject(propDesc.methodType.toMethodDescriptorString());
    out.writeInt(node.getState());
    out.writeObject(node.scenarioStack());
    out.writeObject(instance);
    out.writeObject(node.args());
    out.writeObject(node.resultObjectEvenIfIncomplete());
    out.writeObject(node._xinfo);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    var className = (String) in.readObject();
    var methodName = (String) in.readObject();
    // It would be better to serialize MethodType directly, but there seems to be a bug
    // custom fast serializer we are using! Specifically consider a method with a signature
    // foo(Object) <- Object is not serializable, but all the arguments are!
    var methodType =
        MethodType.fromMethodDescriptorString(
            (String) in.readObject(), Thread.currentThread().getContextClassLoader());
    var state = in.readInt();
    var scenarioStack = (ScenarioStack) in.readObject();
    var entity = in.readObject();
    var args = (Object[]) in.readObject();
    var result = (Object) in.readObject();
    var xinfo = (NodeExtendedInfo) in.readObject();
    var clazz = Class.forName(className);
    try {
      var lookup = MethodHandles.privateLookupIn(clazz, MethodHandles.lookup());
      var newNodeType = MethodType.methodType(Node.class, methodType.parameterArray());
      var newNodeName = mkNewNodeName(mangleName(clazz), methodName);
      var method = lookup.findSpecial(clazz, newNodeName, newNodeType, clazz);
      node = (CompletableNode<Object>) method.invokeWithArguments(merge(entity, args));
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }
    node.updateState(state, 0);
    node._xinfo = xinfo;
    node.replace(scenarioStack);
    node._result_$eq(result);
    node.adjustAfterDeserialization();
  }
}
