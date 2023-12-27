/**
 * Morgan Stanley makes this available to you under the Apache License, Version 2.0 (the "License").
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0. See the
 * NOTICE file distributed with this work for additional information regarding copyright ownership.
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package optimus.platform.inputs.loaders;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;

import optimus.platform.inputs.EngineAwareNodeInputResolver;
import optimus.platform.inputs.NodeInput;
import optimus.platform.inputs.NodeInputs;
import optimus.platform.inputs.NodeInputMapValue;

/**
 * A naive pure java implementation of persistent {@link NodeInputStorage}. Should only really be
 * used from interop.
 *
 * @see NodeInputStorage
 * @implNote For a detailed explanation of the algorithm, please see {@code FrozenNodeInputMap}.
 */
public class JavaNodeInputStorage extends NodeInputStorage<EngineAwareNodeInputResolver> {

  public static final JavaNodeInputStorage EMPTY =
      new JavaNodeInputStorage(new HashMap<>(), new HashMap<>());

  private final Map<NodeInput<?>, VisibleValue<?>> localInputs;
  private final Map<NodeInput<?>, EngineValue> engineSpecificInputs;
  private final EngineAwareNodeInputResolver nodeInputResolver = new Resolver();

  private JavaNodeInputStorage(
      Map<NodeInput<?>, VisibleValue<?>> localInputs,
      Map<NodeInput<?>, EngineValue> engineSpecificInputs) {
    this.localInputs = Collections.unmodifiableMap(localInputs);
    this.engineSpecificInputs = Collections.unmodifiableMap(engineSpecificInputs);
  }

  @Override
  public EngineAwareNodeInputResolver underlying() {
    return nodeInputResolver;
  }

  /**
   * @implNote For a detailed explanation of the algorithm, please see {@code FrozenNodeInputMap}
   */
  @Override
  public <T> JavaNodeInputStorage appendLocalInput(
      NodeInput<T> input, T value, boolean forwardToEngineDefault, LoaderSource source) {
    forwardToEngineDefault = forwardToEngineDefault && !NodeInputs.isNeverForwarding(input);
    Map<NodeInput<?>, EngineValue> engineSpecificInputsCopy = engineSpecificInputs;
    EngineValue content = engineSpecificInputsCopy.get(input);
    boolean forceForwardToEngine = false;
    if (content != null && Objects.equals(content.value(), value)) {
      engineSpecificInputsCopy = new HashMap<>(engineSpecificInputsCopy);
      engineSpecificInputsCopy.remove(input);
      forceForwardToEngine = true; // can't have a never forwarding input on the engine anyways
    }
    Map<NodeInput<?>, VisibleValue<?>> localInputsCopy = localInputs;
    @SuppressWarnings("unchecked")
    VisibleValue<T> currContent = (VisibleValue<T>) localInputs.get(input);
    if (currContent != null) {
      NodeInputMapValue<T> nodeInputMapValue =
          input.combine(currContent.source(), currContent.value(), source, value);
      T newValue = nodeInputMapValue.value();
      boolean newForwardToEngine = forceForwardToEngine || currContent.forwardToEngine;
      LoaderSource newSource = nodeInputMapValue.source();

      if (!Objects.equals(newValue, currContent.value())
          || !Objects.equals(newSource, currContent.source()) || !Objects.equals(currContent.forwardToEngine, newForwardToEngine)) {
        localInputsCopy = new HashMap<>(localInputsCopy);
        localInputsCopy.put(input, new VisibleValue<>(newSource, newValue, newForwardToEngine));
      }
    } else {
      boolean forwardToEngine =
          (NodeInputs.isAlwaysForwarding(input) || forwardToEngineDefault)
              && !engineSpecificInputsCopy.containsKey(input);
      localInputsCopy = new HashMap<>(localInputsCopy);
      localInputsCopy.put(
          input,
          new VisibleValue<>(
              source, value, forceForwardToEngine || forwardToEngine));
    }
    return new JavaNodeInputStorage(localInputsCopy, engineSpecificInputsCopy);
  }

  private <T> JavaNodeInputStorage addToEngineMap(
      Map<NodeInput<?>, VisibleValue<?>> localInputsCopy,
      NodeInput<T> input,
      T value,
      LoaderSource source) {
    Map<NodeInput<?>, EngineValue> engineSpecificInputsCopy = new HashMap<>(engineSpecificInputs);
    EngineValue currValue = engineSpecificInputsCopy.get(input);
    if (currValue != null) {
      NodeInputMapValue<T> nodeInputMapValue =
          input.combine(currValue.source(), (T) currValue.value(), source, value);
      if (Objects.equals(currValue.value(), value) && Objects.equals(currValue.source(), source))
        return this;
      else
        engineSpecificInputsCopy.put(
            input, new EngineValue(nodeInputMapValue.source(), nodeInputMapValue.value()));
    } else {
      engineSpecificInputsCopy.put(input, new EngineValue(source, value));
    }
    return new JavaNodeInputStorage(localInputsCopy, engineSpecificInputsCopy);
  }

  /**
   * @implNote For a detailed explanation of the algorithm, please see {@code FrozenNodeInputMap}
   */
  @Override
  <T> JavaNodeInputStorage appendEngineSpecificInput(
      NodeInput<T> input, T value, LoaderSource source) {
    checkAppendToEngine(input);
    Map<NodeInput<?>, VisibleValue<?>> localInputsCopy = localInputs;

    VisibleValue<?> content = localInputsCopy.get(input);
    if (content != null) {
      if (Objects.equals(content.value(), value)) {
        LoaderSource winningSource =
            input.combine(content.source(), (T) content.value(), source, value).source();
        if (content.forwardToEngine && winningSource == content.source()) return this;
        else {
          localInputsCopy = new HashMap<>(localInputsCopy);
          localInputsCopy.put(
              input,
              new VisibleValue<>(winningSource, content.value(), true));
          return new JavaNodeInputStorage(localInputsCopy, engineSpecificInputs);
        }
      } else {
        localInputsCopy = new HashMap<>(localInputsCopy);
        localInputsCopy.put(
            input,
            new VisibleValue<>(content.source(), content.value(), false));
      }
    }
    return addToEngineMap(localInputsCopy, input, value, source);
  }

  @Override
  public int hashCode() {
    return Objects.hash(localInputs, engineSpecificInputs);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null || getClass() != obj.getClass()) return false;
    JavaNodeInputStorage that = (JavaNodeInputStorage) obj;
    return Objects.equals(localInputs, that.localInputs)
        && Objects.equals(engineSpecificInputs, that.engineSpecificInputs);
  }

  <T> Optional<NodeInputMapValue<T>> TEST_ONLY_resolveNodeInputWithSource(NodeInput<T> nodeInput) {
    return Optional.ofNullable((VisibleValue<T>) localInputs.get(nodeInput));
  }

  public class Resolver extends EngineAwareNodeInputResolver {

    @Override
    public <T> Optional<T> resolveNodeInput(NodeInput<T> nodeInput) {
      @SuppressWarnings("unchecked")
      VisibleValue<T> content = (VisibleValue<T>) localInputs.get(nodeInput);
      return content != null ? Optional.of(content.value()) : nodeInput.defaultValue();
    }

    @Override
    public void engineForwardingForEach(
        BiConsumer<NodeInput<Object>, NodeInputMapValue<Object>> callback) {
      localInputs.forEach(
          (input, context) -> {
            if (context.forwardToEngine) {
              if (engineSpecificInputs.containsKey(input)) {
                throw new IllegalStateException(
                    "Invariant broken, there should never be two conflicting inputs both trying to forward to engine");
              }
              @SuppressWarnings("unchecked")
              NodeInput<Object> castInput = (NodeInput<Object>) input;
              callback.accept(
                  castInput, new NodeInputMapValue<>(context.source(), context.value()));
            }
          });
      engineSpecificInputs.forEach(
          (input, engineValue) -> {
            @SuppressWarnings("unchecked")
            NodeInput<Object> castInput = (NodeInput<Object>) input;
            callback.accept(
                castInput, new NodeInputMapValue<>(engineValue.source(), engineValue.value()));
          });
    }
  }

  private static final class VisibleValue<T> extends NodeInputMapValue<T> {
    private final boolean forwardToEngine;

    private VisibleValue(LoaderSource source, T value, boolean forwardToEngine) {
      super(source, value);
      this.forwardToEngine = forwardToEngine;
    }
  }

  private static final class EngineValue extends NodeInputMapValue<Object> {

    private EngineValue(LoaderSource source, Object value) {
      super(source, value);
    }
  }
}
