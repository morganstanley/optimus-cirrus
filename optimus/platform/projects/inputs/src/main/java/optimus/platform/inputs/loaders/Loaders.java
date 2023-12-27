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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import optimus.platform.inputs.NodeInputResolver;
import optimus.platform.inputs.dist.DistNodeInputs;
import optimus.platform.inputs.registry.Registry;
import optimus.platform.inputs.registry.Source;
import optimus.platform.inputs.registry.parameters.DistInputs;
import optimus.platform.inputs.typesafe.SourceCastableMap;

public class Loaders {

  public static <N> NodeInputStorage<N> environmentVariables(
      NodeInputStorage<N> result, Map<String, String> envVars) {
    for (Map.Entry<String, String> envVar : envVars.entrySet()) {
      String envVarKey = envVar.getKey();
      String envVarValue = envVar.getValue();
      Source.EnvironmentVariable.Key<String, String> originKey =
          Source.EnvironmentVariable.key(envVarKey);
      Optional<SourceCastableMap.Entry<String, ?>> nodeInput =
          Registry.TheHugeRegistryOfAllOptimusProperties.lookup(originKey);
      if (nodeInput.isPresent()) {
        result =
            result.appendLocalInput(
                nodeInput
                    .get()
                    .asNodeInputWithValue(envVarValue, LoaderSource.ENVIRONMENT_VARIABLES),
                false);
      } else if (envVarKey.equals(OptimusDistJavaOptsParser.OPTIMUS_DIST_JAVA_OPTS)) {
        result = OptimusDistJavaOptsParser.parse(result, envVarValue);
      } else if (envVarKey.startsWith("OPTIMUS_DIST")) {
        result =
            result.appendLocalInput(
                DistNodeInputs.newOptimusDistEnvironmentVariable(envVarKey),
                envVarValue,
                true,
                LoaderSource.ENVIRONMENT_VARIABLES);
      }
    }
    return result;
  }

  public static <N> NodeInputStorage<N> environmentVariables(NodeInputStorage<N> result) {
    return environmentVariables(result, System.getenv());
  }

  public static <N> NodeInputStorage<N> javaProperties(
      NodeInputStorage<N> result, Properties javaProperties) {
    return loadProperties(result, (Map) javaProperties, true, LoaderSource.FILE);
  }

  public static <N> NodeInputStorage<N> javaNonForwardingProperties(
      NodeInputStorage<N> result, Properties javaProperties) {
    return loadProperties(result, (Map) javaProperties, false, LoaderSource.FILE);
  }

  static <N> NodeInputStorage<N> systemProperties(
      NodeInputStorage<N> result, Map<String, String> systemProperties) {
    return loadProperties(result, systemProperties, false, LoaderSource.SYSTEM_PROPERTIES);
  }

  public static <N> NodeInputStorage<N> systemProperties(NodeInputStorage<N> result) {
    return systemProperties(result, (Map) System.getProperties());
  }

  public static <N> NodeInputStorage<N> fileProperties(NodeInputStorage<N> result, File file)
      throws IOException {
    Properties props = new Properties();
    props.load(new FileInputStream(file));
    return loadProperties(result, (Map) props, true, LoaderSource.FILE);
  }

  private static <N> NodeInputStorage<N> loadProperties(
      NodeInputStorage<N> result,
      Map<String, String> bindings,
      boolean forwardToEngine,
      LoaderSource source) {
    for (Map.Entry<String, String> entry : bindings.entrySet()) {
      String prefixedEngineEnv =
          DistInputs.SourceNames.OPTIMUS_DIST_GSF_PREFIX + DistInputs.SourceNames.EngineEnv;
      if (entry.getKey().equals(DistInputs.SourceNames.EngineEnv)
          || entry.getKey().equals(prefixedEngineEnv)) {
        result = EngineEnvParser.parse(result, entry.getValue(), source);
      } else {
        Source.Key<String, String> originKey = Source.JavaProperty.key(entry.getKey());
        Optional<SourceCastableMap.Entry<String, ?>> nodeInput =
            Registry.TheHugeRegistryOfAllOptimusProperties.lookup(originKey);
        if (nodeInput.isPresent()) {
          result =
              result.appendLocalInput(
                  nodeInput.get().asNodeInputWithValue(entry.getValue(), source), forwardToEngine);
        }
      }
    }
    return result;
  }
}
