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
package optimus.platform.inputs.typesafe;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import optimus.platform.inputs.NodeInput;
import optimus.platform.inputs.NodeInputResolver;
import optimus.platform.inputs.NodeInputMapValue;
import optimus.platform.inputs.loaders.LoaderSource;
import optimus.platform.inputs.registry.Source;

/**
 * For every {@link Source.Key} key of type {@code ?, V} stored in this map, the corresponding
 * {@link Entry} value is of type {@code V, ?}.
 */
public class SourceCastableMap {

  private final Map<Object, Object> underlyingMap = new HashMap<>();

  public <V, T> void put(Source<?, V, T> source, NodeInput<T> nodeInput) {
    underlyingMap.put(source.key(), new Entry<>(nodeInput, source.mapper()));
  }

  public <V> Optional<Entry<V, ?>> get(Source.Key<?, V> key) {
    Entry<V, ?> entry = (Entry<V, ?>) underlyingMap.get(key);
    return Optional.ofNullable(entry);
  }

  public static final class Entry<V, T> {

    private final NodeInput<T> nodeInput;
    private final Function<V, T> converter;

    private Entry(NodeInput<T> nodeInput, Function<V, T> converter) {
      this.nodeInput = nodeInput;
      this.converter = converter;
    }

    public NodeInput<T> nodeInput() {
      return nodeInput;
    }

    public NodeInputResolver.Entry<T> asNodeInputWithValue(V value, LoaderSource source) {
      return NodeInputResolver.entry(
          nodeInput, new NodeInputMapValue<>(source, converter.apply(value)));
    }
  }
}
