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

import java.util.List;
import java.util.Optional;

import optimus.platform.inputs.NodeInput;
import optimus.platform.inputs.dist.DistNodeInputs;
import optimus.platform.inputs.registry.Registry;
import optimus.platform.inputs.registry.Source;
import optimus.platform.inputs.typesafe.SourceCastableMap;

public class EngineEnvParser {

  /**
   * Parses a string containing environment variables properties.
   *
   * @param result the {@link NodeInputStorage} where to accumulate parsed {@link NodeInput}
   * @param value the value associated with the java property {@link
   *     optimus.platform.inputs.registry.parameters.DistInputs.SourceNames#EngineEnv}
   * @return a {@link NodeInputStorage}
   */
  public static <N> NodeInputStorage<N> parse(
      NodeInputStorage<N> result, String value, LoaderSource source) {
    if (value == null || value.trim().isEmpty()) return result;
    List<String> environmentVariables = Source.stringToStringList(value);
    for (String environmentVariable : environmentVariables) {
      String[] keyAndValue = environmentVariable.split("=", 2); // split *exactly once* at "="
      if (keyAndValue.length != 2)
        throw new IllegalArgumentException(
            "Could not parse environment variable " + environmentVariable);
      Optional<SourceCastableMap.Entry<String, ?>> entry =
          Registry.TheHugeRegistryOfAllOptimusProperties.lookup(
              Source.EnvironmentVariable.key(keyAndValue[0]));
      if (entry.isPresent()) {
        result =
            result.appendEngineSpecificInput(
                entry.get().asNodeInputWithValue(keyAndValue[1], source));
      } else {
        result =
            result.appendEngineSpecificInput(
                DistNodeInputs.newOptimusDistEnvironmentVariable(keyAndValue[0]),
                keyAndValue[1],
                LoaderSource.ENVIRONMENT_VARIABLES);
      }
    }
    return result;
  }
}
