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
package optimus.platform.inputs;

import java.util.Optional;
import java.util.function.BiConsumer;

/** Expose a way for distribution to iterate over all node inputs that are visible remotely. */
public abstract class EngineAwareNodeInputResolver extends NodeInputResolver {

  public static final EngineAwareNodeInputResolver EMPTY = new EmptyEngineAwareNodeInputResolver();

  public abstract void engineForwardingForEach(
      BiConsumer<NodeInput<Object>, NodeInputMapValue<Object>> callback);

  private static final class EmptyEngineAwareNodeInputResolver
      extends EngineAwareNodeInputResolver {

    @Override
    public <T> Optional<T> resolveNodeInput(NodeInput<T> nodeInput) {
      return nodeInput.defaultValue();
    }

    @Override
    public void engineForwardingForEach(
        BiConsumer<NodeInput<Object>, NodeInputMapValue<Object>> callback) {}
  }
}
