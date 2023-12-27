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
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import optimus.platform.inputs.dist.GSFSections;
import optimus.platform.inputs.dist.GSFSections.GSFSection;
import optimus.platform.inputs.loaders.LoaderSource;
import optimus.platform.inputs.registry.CombinationStrategies.CombinationStrategy;
import optimus.platform.inputs.registry.Registry;
import optimus.platform.inputs.registry.Source;
import optimus.platform.inputs.EngineForwarding.Behavior;
import optimus.platform.inputs.EngineForwarding.Serialization;

public class NodeInputs {
  /**
   * This marker interface <b>must</b> be implemented by any {@link NodeInput} which is non value
   * affecting (i.e. ScenarioIndependent)
   */
  public interface SINodeInput<T> extends NodeInput<T> {}

  /**
   * This marker interface <b>must</b> be implemented by any {@link NodeInput} which is non value
   * affecting and scoped (not a processInput so it will live on the SS)
   */
  public interface ScopedSINodeInput<T> extends SINodeInput<T> {}

  public abstract static class BaseNodeInput<T> implements NodeInput<T>, Serializable {

    protected final String name;
    private final String description;
    private final T defaultValue;
    private final List<Source<?, ?, T>> sources;
    private final GSFSection<T, ?> gsfSection;
    private final boolean requiresRestart;
    private final Behavior forwardingBehavior;
    private final CombinationStrategy<T> combinationStrategy;
    private final boolean isSerializedViaEngineSpec;

    public BaseNodeInput(
        String name,
        String description,
        T defaultValue,
        List<Source<?, ?, T>> sources,
        GSFSection<T, ?> gsfSection,
        boolean requiresRestart,
        Behavior forwardingBehavior,
        CombinationStrategy<T> combinationStrategy,
        boolean isSerializedViaEngineSpec) {
      this.name = name;
      this.description = description;
      this.defaultValue = defaultValue;
      this.sources = sources;
      this.gsfSection = gsfSection;
      this.requiresRestart = requiresRestart;
      this.forwardingBehavior = forwardingBehavior;
      assert combinationStrategy != null : "Must provide a combination strategy!";
      this.combinationStrategy = combinationStrategy;
      this.isSerializedViaEngineSpec = isSerializedViaEngineSpec;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public String description() {
      return description;
    }

    @Override
    public Optional<T> defaultValue() {
      return Optional.ofNullable(defaultValue);
    }

    @Override
    public List<Source<?, ?, T>> sources() {
      if (sources == null) {
        throw new IllegalStateException(
            "Should never be loaded, can only be created by internal code.");
      }
      return sources;
    }

    @Override
    public GSFSection<T, ?> gsfSection() {
      return gsfSection;
    }

    @Override
    public boolean requiresRestart() {
      return requiresRestart;
    }

    @Override
    public boolean affectsExecutionProcessWide() {
      return false;
    }

    @Override
    public final boolean affectsNodeResult() {
      return false;
    }

    @Override
    public final Serialization serializationStyle() {
      return this.isSerializedViaEngineSpec ? Serialization.ENGINE_SPEC : Serialization.JAVA;
    }

    @Override
    public Behavior engineForwardingBehavior() {
      return forwardingBehavior;
    }

    @Override
    public NodeInputMapValue<T> combine(
        LoaderSource currSource, T currValue, LoaderSource newSource, T newValue) {
      return combinationStrategy.choose(currSource, currValue, newSource, newValue);
    }

    @Override
    public String toString() {
      return String.format("%s: %s", name(), description());
    }

    protected Object writeReplace() {
      if (isSerializedViaEngineSpec)
        // should never get here since we filter out engine spec serialized inputs in
        // FrozenNodeInputMap
        throw new IllegalStateException("Cannot serialize transient input: " + name);
      return new Placeholder<>(name());
    }

    /**
     * Used to support serialization of {@link NodeInput}, so that the singleton instance is always
     * returned
     */
    private static class Placeholder<T> implements Serializable {

      private final String name;

      private Placeholder(String name) {
        this.name = name;
      }

      private Object readResolve() {
        return Registry.TheHugeRegistryOfAllOptimusProperties.lookupByName(name);
      }
    }
  }

  public static class BaseScopedSIInput<T> extends BaseNodeInput<T>
      implements ScopedSINodeInput<T> {

    public BaseScopedSIInput(
        String name,
        String description,
        T defaultValue,
        List<Source<?, ?, T>> sources,
        GSFSection<T, ?> gsfSection,
        boolean requiresRestart,
        Behavior forwardingBehavior,
        CombinationStrategy<T> combinationStrategy,
        boolean isTransient) {
      super(
          name,
          description,
          defaultValue,
          sources,
          gsfSection,
          requiresRestart,
          forwardingBehavior,
          combinationStrategy,
          isTransient);
    }
  }

  public static <T> ScopedSINodeInput<T> newSerializableWithDefault(
      String name,
      String description,
      T defaultValue,
      List<Source<?, ?, T>> sources,
      CombinationStrategy<T> combinationStrategy) {
    return new BaseScopedSIInput<>(
        name,
        description,
        defaultValue,
        sources,
        GSFSections.none(),
        false,
        Behavior.DYNAMIC,
        combinationStrategy,
        false);
  }

  public static <T> ScopedSINodeInput<T> newSerializable(
      String name,
      String description,
      List<Source<?, ?, T>> sources,
      CombinationStrategy<T> combinationStrategy) {
    return newSerializableWithDefault(name, description, null, sources, combinationStrategy);
  }

  public static <T> ScopedSINodeInput<T> newTransient(
      String name,
      String description,
      Source<?, ?, T> source,
      CombinationStrategy<T> combinationStrategy) {
    return newTransientWithDefault(name, description, null, source, combinationStrategy);
  }

  public static <T> ScopedSINodeInput<T> newTransient(
      String name,
      String description,
      List<Source<?, ?, T>> sources,
      EngineForwarding.Behavior forwardingBehavior,
      CombinationStrategy<T> combinationStrategy) {
    return new BaseScopedSIInput<>(
        name,
        description,
        null,
        sources,
        GSFSections.none(),
        false,
        forwardingBehavior,
        combinationStrategy,
        true);
  }

  public static <T> ScopedSINodeInput<T> newTransientWithDefault(
      String name,
      String description,
      T defaultValue,
      Source<?, ?, T> source,
      CombinationStrategy<T> combinationStrategy) {
    return new BaseScopedSIInput<>(
        name,
        description,
        defaultValue,
        Arrays.asList(source),
        GSFSections.none(),
        false,
        Behavior.DYNAMIC,
        combinationStrategy,
        true);
  }

  public static <T> ScopedSINodeInput<T> newTransientJavaOptWithDefault(
      String name,
      String description,
      T defaultValue,
      String engineSpecName,
      Function<T, String> invert,
      List<Source<?, ?, T>> sources,
      CombinationStrategy<T> combinationStrategy) {
    GSFSection<T, ?> gsfSection = GSFSections.newEngineSpecJavaOpt(engineSpecName, invert);
    return new BaseScopedSIInput<>(
        name,
        description,
        defaultValue,
        sources,
        gsfSection,
        false,
        Behavior.DYNAMIC,
        combinationStrategy,
        true);
  }

  public static <T> ScopedSINodeInput<T> newTransientJavaOpt(
      String name,
      String description,
      String engineSpecName,
      Function<T, String> invert,
      List<Source<?, ?, T>> sources,
      CombinationStrategy<T> combinationStrategy) {
    return newTransientJavaOptWithDefault(
        name, description, null, engineSpecName, invert, sources, combinationStrategy);
  }

  public static <T> ScopedSINodeInput<T> newTransientJavaOptWithDefault(
      String name,
      String description,
      T defaultValue,
      String engineSpecName,
      Function<T, String> invert,
      Source<?, ?, T> source,
      CombinationStrategy<T> comboStrategy) {
    return newTransientJavaOptWithDefault(
        name,
        description,
        defaultValue,
        engineSpecName,
        invert,
        Arrays.asList(source),
        comboStrategy);
  }

  public static <T> ScopedSINodeInput<T> newTransientJavaOpt(
      String name,
      String description,
      String engineSpecName,
      Function<T, String> invert,
      Source<?, ?, T> source,
      CombinationStrategy<T> comboStrategy) {
    return newTransientJavaOptWithDefault(
        name, description, null, engineSpecName, invert, source, comboStrategy);
  }

  public static final boolean isJavaSerialized(NodeInput<?> input) {
    return input.serializationStyle() == EngineForwarding.Serialization.JAVA;
  }

  public static final boolean isNeverForwarding(NodeInput<?> input) {
    return input.engineForwardingBehavior() == EngineForwarding.Behavior.NEVER;
  }

  public static final boolean isAlwaysForwarding(NodeInput<?> input) {
    return input.engineForwardingBehavior() == EngineForwarding.Behavior.ALWAYS;
  }
}
