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

import java.util.List;
import java.util.Optional;

import optimus.platform.inputs.dist.GSFSections.GSFSection;
import optimus.platform.inputs.loaders.LoaderSource;
import optimus.platform.inputs.registry.Source;

/**
 * {@link NodeInput} represents an (optionally scoped) computation input that lives within the
 * scenario stack. It is designed to generalize plugin tags and support more advanced use cases that
 * are currently not possible. <br>
 * <b>Why would this be needed?</b> <br>
 * TL;DR: there are a lot of "hidden" inputs which live outside the scenario stack, and therefore
 * cannot be tracked. Tracking these inputs is important to support reproducibility, the ability to
 * re-run computations, and upcoming features in distribution and graph. Unfortunately plugin tags,
 * in their current behaviour, exhibit some limitations that make them unsuitable to represent some
 * of these inputs. {@link NodeInput} aims to fill the lack of a solution in this niche. <br>
 * In distribution, a node initially supposed to run in environment A, executes in an engine with
 * environment B. Currently environment B is only an approximation of environment A, created by a
 * launcher script that is provided by users. It would be desirable to automatically create a
 * replica environment A', but it is impossible because distribution code relies on inputs which are
 * not tracked by Optimus (environment variables, system properties, distribution configuration
 * files & process state such as plugins). It is therefore also impossible to re-run a distributed
 * node just by capturing its node identity, whereas it is possible for a regular node within graph.
 * <br>
 * Separately, there is also a need to support scoped inputs with a custom conflict resolution
 * logic. Plugins are not suitable because they have global scoping (their value cannot change
 * depending on, for instance, the currently executing node), and on a conflict they always override
 * the previous value. For @job, we may want a weight-based strategy to determine whether
 * a @job-node should publish to DAL or not. To enable plugins and configure their parameters, we
 * may want different values depending on which node is querying for the parameter. A similar
 * approach is also required for optconf.
 *
 * @param <T> the type of the value associated with this {@link NodeInput}
 */
public interface NodeInput<T> {

  /**
   * Can be used for pretty-printing.
   *
   * @return a name associated with the {@link NodeInput}
   */
  String name();

  /**
   * @return a description of the {@link NodeInput}'s purpose
   */
  String description();

  /**
   * @return an {@link Optional} describing the default value for the {@link NodeInput}
   */
  default Optional<T> defaultValue() {
    return Optional.empty();
  }

  /**
   * @return the list of {@link Source} from where the value of the {@link NodeInput} can be loaded
   */
  List<Source<?, ?, T>> sources();

  /**
   * @return {@code true} if the {@link NodeInput} affects value
   */
  boolean affectsNodeResult();

  /**
   * @return {@link EngineForwarding.Serialization}
   * @apiNote Some parameters only make sense in the context of a specific machine (eg: {@code
   *     TREADMILL_APP} and {@code SYS_LOC}).
   * @implNote We do not enforce that type {@code T} needs to be {@link java.io.Serializable},
   *     because it makes interop with Scala difficult as primitives are not serializable. This
   *     choice is not really an issue since {@link java.io.Serializable} is inherently not typesafe
   *     anyway, as we can have serializable classes with non-serializable members and serializable
   *     objects not extend {@link java.io.Serializable}.
   */
  EngineForwarding.Serialization serializationStyle();

  /**
   * @return the GSF section where the parameter should reside
   * @implNote If this method returns a valid engine spec section, it does not imply that the {@link
   *     NodeInput} should be set on the engine spec. That is determined by {@link
   *     #requiresRestart()} and depends on where the {@link NodeInput} was actually loaded from.
   */
  GSFSection<T, ?> gsfSection();

  /**
   * Determines whether the value associated with this {@link NodeInput} requires a process restart
   * to change.
   *
   * @implNote This method can return false, even if {@link #gsfSection()} is defined as requiring
   *     restart. That is because a transient {@link NodeInput}, which normally would not be present
   *     in the engine spec, can be set within {@code OPTIMUS_DIST_JAVA_OPTS} and therefore will end
   *     up on the engine spec.
   */
  boolean requiresRestart();

  /**
   * @return {@link EngineForwarding.Behavior}
   */
  EngineForwarding.Behavior engineForwardingBehavior();

  /**
   * Used to determine the value associated with this {@link NodeInput}, when there is more than one
   * value in scope.
   *
   * @param currValue First value to combine
   * @param newValue Second value to combine
   * @return the value that replaces the two inputs
   * @apiNote This method <b>must</b> be associative. Guaranteeing associativity allows combination
   *     of inputs without worrying about traversal order
   */
  default NodeInputMapValue<T> combine(LoaderSource l1, T currValue, LoaderSource l2, T newValue) {
    throw new IllegalStateException("Cannot combine values for " + getClass().getSimpleName());
  }

  /**
   * This method determines whether the input is a {@link ProcessSINodeInput} or {@link
   * ScopedSINodeInput} based on the return value specified below
   *
   * @return true if the input affects the entire process and false otherwise
   */
  boolean affectsExecutionProcessWide();
}
