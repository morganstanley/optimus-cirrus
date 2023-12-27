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
package optimus.platform.inputs.loaders;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import optimus.platform.inputs.EngineAwareNodeInputResolver;
import optimus.platform.inputs.NodeInput;
import optimus.platform.inputs.NodeInputResolver;
import optimus.platform.inputs.NodeInputMapValue;
import optimus.platform.inputs.dist.DistNodeInputs;
import optimus.platform.inputs.registry.Registry;
import optimus.platform.inputs.registry.Source;

public class OptimusDistJavaOptsParser {

  public static final String OPTIMUS_DIST_JAVA_OPTS = "OPTIMUS_DIST_JAVA_OPTS";

  public static EngineAwareNodeInputResolver parseFromEnv() {
    return parse(JavaNodeInputStorage.EMPTY, System.getenv(OPTIMUS_DIST_JAVA_OPTS)).underlying();
  }

  /**
   * Parses a string containing JVM properties into a {@link NodeInputStorage}, marking the inputs
   * as engine specific.
   *
   * @param result the {@link NodeInputStorage} where to accumulate parsed {@link NodeInput}
   * @param value the value associated with the environment value OPTIMUS_DIST_JAVA_OPTS
   * @return a {@link NodeInputStorage}
   */
  public static <N> NodeInputStorage<N> parse(NodeInputStorage<N> result, String value) {
    for (NodeInputResolver.Entry<?> entry : parseFromString(value)) {
      result = result.appendEngineSpecificInput(entry);
    }
    return result;
  }

  /**
   * Parses a string containing JVM properties into a {@link NodeInputStorage}, marking the inputs
   * as local.
   *
   * @param value the value associated with the environment value OPTIMUS_DIST_JAVA_OPTS
   * @return a {@link Set} of parsed {@link NodeInputResolver.Entry}
   */
  public static <N> NodeInputStorage<N> parseAsLocal(NodeInputStorage<N> result, String value) {
    for (NodeInputResolver.Entry<?> entry : parseFromString(value)) {
      result = result.appendLocalInput(entry, true);
    }
    return result;
  }

  private static Set<NodeInputResolver.Entry<?>> parseFromString(String value) {
    Set<NodeInputResolver.Entry<?>> result = new HashSet<>();
    if (value == null || value.trim().isEmpty()) return result;
    String[] properties = value.replaceAll("\\s+", " ").trim().split(" ");
    for (String property : properties) {
      property = property.trim();
      if (property.isEmpty()) continue;
      String systemProperty = property;
      Optional<NodeInputResolver.Entry<?>> entry = parseIfJvmProperty(systemProperty);
      if (!entry.isPresent()) entry = parseIfSystemProperty(systemProperty);
      if (entry.isPresent()) {
        result.add(entry.get());
      } else {
        result.add(
            NodeInputResolver.entry(
                DistNodeInputs.newOptimusDistJavaFlag(systemProperty),
                new NodeInputMapValue<>(LoaderSource.ENVIRONMENT_VARIABLES, systemProperty)));
      }
    }
    return result;
  }

  private static Optional<NodeInputResolver.Entry<?>> lookup(
      Source.Key<String, String> key, String value) {
    return Registry.TheHugeRegistryOfAllOptimusProperties.lookup(key)
        .map(e -> e.asNodeInputWithValue(value, LoaderSource.ENVIRONMENT_VARIABLES));
  }

  /** Parses -Xmx. Can be extended to explicitly parse -Xms, -XX:+, -XX:- if needed */
  private static Optional<NodeInputResolver.Entry<?>> parseIfJvmProperty(String property) {
    if (property.startsWith("-Xmx")) {
      String key = property.substring(4); // extract key
      String value = property.substring(4); // extract value (incl. memory size)
      return lookup(Source.JavaProperty.key(key), value);
    }
    return Optional.empty();
  }

  /** Parses -Dkey=value */
  private static Optional<NodeInputResolver.Entry<?>> parseIfSystemProperty(String property) {
    if (property.startsWith("-D")) {
      String[] keyAndValue =
          property.substring(2).split("=", 2); // remove "-D", and split *exactly once* at "="
      if (keyAndValue.length != 2)
        throw new IllegalStateException("Could not parse property " + property);
      return lookup(Source.JavaProperty.key(keyAndValue[0]), keyAndValue[1]);
    }
    return Optional.empty();
  }
}
