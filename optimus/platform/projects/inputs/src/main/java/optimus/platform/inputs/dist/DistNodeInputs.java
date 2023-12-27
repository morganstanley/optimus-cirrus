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
package optimus.platform.inputs.dist;

import static optimus.platform.inputs.dist.GSFSections.GSFSection;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import optimus.platform.inputs.NodeInputs.ScopedSINodeInput;
import optimus.platform.inputs.NodeInputs.BaseScopedSIInput;
import optimus.platform.inputs.registry.CombinationStrategies;
import optimus.platform.inputs.registry.CombinationStrategies.CombinationStrategy;
import optimus.platform.inputs.registry.Source;
import optimus.platform.inputs.registry.parameters.DistInputs;
import optimus.platform.inputs.registry.parameters.GraphInputs;
import optimus.platform.inputs.EngineForwarding.Behavior;

public class DistNodeInputs {

  public static final String JVM_FLAG_DESC =
      "A JVM flag which needs to be set on the engine's process";
  public static final String ENV_VAR_DESC =
      "An environment variable that should be set on the engine's process";

  /**
   * Represents a Safe Java property, in {@code -Dkey=val} format. Normally these properties are
   * removed from {@code OPTIMUS_DIST_JAVA_OPTS} and serialized as part of scenario stack, but it is
   * necessary to identify them separately in order to forward them when using interop within
   * engines. Safe Java properties are those whose value is never cached, and therefore can be
   * safely looked up dynamically inside the scenario stack.
   */
  public static <T> ScopedSINodeInput<T> newOptimusSafeJavaOpt(
      String name,
      String description,
      Source<String, String, T> source,
      Function<T, String> invertAndConvert) {
    GSFSection<T, ?> gsfSection = GSFSections.newSafeJavaOpt(source.name(), invertAndConvert);
    return new BaseScopedSIInput<>(
        name,
        description,
        null,
        Arrays.asList(source),
        gsfSection,
        false,
        Behavior.DYNAMIC,
        CombinationStrategies.distCombinator(),
        false);
  }

  /**
   * Represents a Java property, in {@code -Dkey=val} format, that needs to be set on remote engines
   * because it is either:
   *
   * <ul>
   *   <li>specified in {@code OPTIMUS_DIST_JAVA_OPTS}, or
   *   <li>is a self-forwarding property (such as those in {@link GraphInputs})
   * </ul>
   */
  private static <T> ScopedSINodeInput<T> newOptimusDistJavaProperty(
      String name,
      String description,
      Behavior forwardingBehavior,
      String engineSpecName,
      Source<String, String, T> source,
      Function<T, String> invert,
      CombinationStrategy<T> comboStratgey) {
    GSFSection<T, ?> gsfSection = GSFSections.newEngineSpecJavaOpt(engineSpecName, invert);
    List<Source<?, ?, T>> sources = source != null ? Arrays.asList(source) : null;
    return new BaseScopedSIInput<>(
        name,
        description,
        null,
        sources,
        gsfSection,
        true,
        forwardingBehavior,
        comboStratgey,
        true);
  }

  /**
   * @see #newOptimusDistJavaProperty(String, String, Behavior, String, Source, Function,
   *     CombinationStrategy)
   */
  public static <T> ScopedSINodeInput<T> newAutoForwardingOptimusDistJavaProperty(
      String name,
      String description,
      Source<String, String, T> source,
      Function<T, String> invert,
      CombinationStrategy<T> comboStrategy) {
    return newOptimusDistJavaProperty(
        name, description, Behavior.ALWAYS, source.name(), source, invert, comboStrategy);
  }

  /**
   * Represents a JVM flag that needs to be set on remote engines because it is specified in {@code
   * OPTIMUS_DIST_JAVA_OPTS}.
   */
  public static ScopedSINodeInput<String> newOptimusDistJavaFlag(String name) {
    GSFSection<String, ?> gsfSection = GSFSections.newEngineSpecJavaFlag();
    return new BaseScopedSIInput<>(
        name,
        JVM_FLAG_DESC,
        null,
        null,
        gsfSection,
        true,
        Behavior.DYNAMIC,
        CombinationStrategies.distCombinator(),
        true);
  }

  /**
   * Represents an environment variable that needs to be set on remote engines because it is either:
   *
   * <ul>
   *   <li>set locally and prefixed with {@code OPTIMUS_DIST_}, or
   *   <li>set in {@link DistInputs.SourceNames.EngineEnv}
   * </ul>
   */
  private static <T> ScopedSINodeInput<T> newOptimusDistEnvironmentVariableWithDefault(
      String name,
      String description,
      T defaultValue,
      Behavior forwardingBehavior,
      String engineSpecName,
      Source<String, String, T> source,
      Function<T, String> invert,
      CombinationStrategy<T> comboStrategy) {
    GSFSection<T, ?> gsfSection = GSFSections.newEngineSpecEnvVar(engineSpecName, invert);
    List<Source<?, ?, T>> sources = source != null ? Arrays.asList(source) : null;
    return new BaseScopedSIInput<>(
        name,
        description,
        defaultValue,
        sources,
        gsfSection,
        true,
        forwardingBehavior,
        comboStrategy,
        true);
  }

  /**
   * @see #newOptimusDistEnvironmentVariableWithDefault(String, String, Object, Behavior, String,
   *     Source, Function, CombinationStrategy)
   */
  public static <T> ScopedSINodeInput<T> newAutoForwardingOptimusDistEnvironmentVariableWithDefault(
      String name,
      String description,
      T defaultValue,
      Source<String, String, T> source,
      Function<T, String> invert,
      CombinationStrategy<T> comboStrategy) {
    return newOptimusDistEnvironmentVariableWithDefault(
        name,
        description,
        defaultValue,
        Behavior.ALWAYS,
        source.name(),
        source,
        invert,
        comboStrategy);
  }

  /**
   * @see #newOptimusDistEnvironmentVariableWithDefault(String, String, Object, Behavior, String,
   *     Source, Function, CombinationStrategy)
   */
  public static ScopedSINodeInput<String> newOptimusDistEnvironmentVariable(String name) {
    return newOptimusDistEnvironmentVariableWithDefault(
        name,
        ENV_VAR_DESC,
        null,
        Behavior.DYNAMIC,
        name,
        null,
        String::toString,
        CombinationStrategies.distCombinator());
  }
}
