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

import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;
import optimus.platform.inputs.loaders.LoaderSource;
import optimus.platform.inputs.typesafe.SourceCastableMap;

/**
 * A pure Java abstract class used to resolve parameters. <br>
 * Needed because <i>interop</i> and <i>distribution</i> share a lot of GSF setup code.
 *
 * @implNote <i>interop</i> is forbidden from depending on Scala, so this interface is used to allow
 *     access to scenario stacks
 */
public abstract class NodeInputResolver {

  public static final NodeInputResolver EMPTY = new EmptyNodeInputResolver();

  /**
   * @param nodeInput the identifier of the {@link NodeInput}
   * @param <T> the type of the value associated with the parameter
   * @return an {@link Optional} describing the value associated with the {@link NodeInput}
   */
  public abstract <T> Optional<T> resolveNodeInput(NodeInput<T> nodeInput);

  /**
   * @param nodeInput the identifier of the {@link NodeInput}
   * @param <T> the type of the value associated with the parameter
   * @return the value associated with the node input
   * @throws IllegalArgumentException if there is no binding for the node input
   */
  public final <T> T resolveNodeInputOrThrow(NodeInput<T> nodeInput) {
    Optional<T> res = resolveNodeInput(nodeInput);
    if (!res.isPresent())
      throw new IllegalArgumentException("Could not find a binding to " + nodeInput.name());
    return res.get();
  }

  /**
   * A wrapper which combines a {@link NodeInput} of type {@code T} with a value of the same type.
   */
  public interface Entry<T> extends Serializable {
    NodeInput<T> nodeInput();

    NodeInputMapValue<T> sourcedInput();
  }

  public static <T> Entry<T> entry(NodeInput<T> nodeInput, NodeInputMapValue<T> nodeInputMapValue) {
    return new EntryImpl<>(nodeInput, nodeInputMapValue);
  }

  private static class EntryImpl<T> implements Entry<T> {

    private final NodeInput<T> nodeInput;
    private final NodeInputMapValue<T> nodeInputMapValue;

    public EntryImpl(NodeInput<T> nodeInput, NodeInputMapValue<T> nodeInputMapValue) {
      this.nodeInput = nodeInput;
      this.nodeInputMapValue = nodeInputMapValue;
    }

    public NodeInput<T> nodeInput() {
      return nodeInput;
    }

    public NodeInputMapValue<T> sourcedInput() {
      return nodeInputMapValue;
    }

    @Override
    public int hashCode() {
      return 31 * (31 + Objects.hashCode(nodeInput)) + Objects.hashCode(nodeInputMapValue.value());
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj instanceof EntryImpl) {
        EntryImpl that = (EntryImpl) obj;
        return Objects.equals(nodeInputMapValue.value(), that.nodeInputMapValue.value())
            && Objects.equals(nodeInput, that.nodeInput);
      } else return false;
    }
  }

  private static final class EmptyNodeInputResolver extends NodeInputResolver {

    @Override
    public <T> Optional<T> resolveNodeInput(NodeInput<T> nodeInput) {
      return nodeInput.defaultValue();
    }
  }
}
