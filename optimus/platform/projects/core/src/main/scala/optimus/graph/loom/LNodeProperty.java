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

import java.io.Serial;
import optimus.graph.GraphInInvalidState;
import optimus.graph.HNCMoniker;
import optimus.graph.Node;
import optimus.graph.NodeExtendedInfo;
import optimus.graph.PropertyNodeSync;
import optimus.platform.ScenarioStack;
import scala.Function1;
import scala.Function2;
import scala.Function3;

/** Base class for all @nodes on @entity for "private" (node class per def) implementation */
@SuppressWarnings("unused")
public abstract class LNodeProperty extends PropertyNodeSync<Object> {

  protected LNodeProperty() {}

  private LNodeProperty(
      int state, ScenarioStack scenarioStack, Object result, NodeExtendedInfo xinfo) {
    this.updateState(state, /* clearFlags = */ 0);
    this.attach(scenarioStack);
    this._xinfo = xinfo;
    this._result_$eq(result);
  }

  @Override
  public Node<Object> argsCopy(Object generator) {
    if (generator instanceof LNodeFunction<?> lNodeFunction)
      //noinspection unchecked
      return (Node<Object>) lNodeFunction.toNodeWith(this);
    else if (generator instanceof Function1<?, ?>) {
      @SuppressWarnings("unchecked")
      var f = (Function1<Object, Node<Object>>) generator;
      return f.apply(entity());
    } else if (generator instanceof Function2<?, ?, ?>) {
      var args = args();
      @SuppressWarnings("unchecked")
      var f = (Function2<Object, Object, Node<Object>>) generator;
      return f.apply(entity(), args[0]);
    } else if (generator instanceof Function3<?, ?, ?, ?>) {
      var args = args();
      @SuppressWarnings("unchecked")
      var f = (Function3<Object, Object, Object, Node<Object>>) generator;
      return f.apply(entity(), args[0], args[1]);
    } else throw new GraphInInvalidState("Invalid generator!");
  }

  @Serial
  protected Object writeReplace() {
    return new HNCMoniker(this, entity());
  }
}
