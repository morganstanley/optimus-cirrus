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
package optimus.platform.inputs

import optimus.platform.inputs.EngineForwarding.Behavior
import optimus.platform.inputs.StateApplicators.StateApplicator
import optimus.platform.inputs.dist.GSFSections.GSFSection
import optimus.platform.inputs.registry.CombinationStrategies._
import optimus.platform.inputs.registry.Source

object ProcessInputs {
  def newTransientWithoutDefaultNoSideEffects[T >: Null <: AnyRef](
      name: String,
      description: String,
      source: Source[_, _, T],
      gsfSection: GSFSection[T, _],
      combinationStrategy: CombinationStrategy[T] = graphCombinator[T]): DefaultTransientProcessSINodeInput[T] =
    newTransientNoSideEffects[T](name, description, null, source, gsfSection, combinationStrategy)

  def newTransientNoSideEffects[T >: Null <: AnyRef](
      name: String,
      description: String,
      default: T,
      source: Source[_, _, T],
      gsfSection: GSFSection[T, _],
      combinationStrategy: CombinationStrategy[T] = graphCombinator[T]): DefaultTransientProcessSINodeInput[T] =
    new DefaultTransientProcessSINodeInput[T](
      name,
      description,
      defaultValue = default,
      sources = List(source),
      gsfSection,
      combinationStrategy = combinationStrategy,
      forwardingBehavior = Behavior.DYNAMIC,
      StateApplicators.emptyApplicator
    )

  def newSerializableNoDefaultNoSideEffects[T >: Null <: AnyRef](
      name: String,
      description: String,
      source: Source[_, _, T],
      combinationStrategy: CombinationStrategy[T] = graphCombinator[T]): DefaultSerializableProcessSINodeInput[T] =
    newSerializableNoSideEffects(name, description, null, source)

  def newSerializableNoSideEffects[T >: Null <: AnyRef](
      name: String,
      description: String,
      default: T,
      source: Source[_, _, T]): DefaultSerializableProcessSINodeInput[T] =
    newSerializable(name, description, default, source, StateApplicators.emptyApplicator)

  def newSerializable[T >: Null <: AnyRef](
      name: String,
      description: String,
      default: T,
      source: Source[_, _, T],
      applicator: StateApplicator): DefaultSerializableProcessSINodeInput[T] =
    newSerializableFromSources(name, description, default, List(source), applicator)

  def newSerializableFromSources[T >: Null <: AnyRef](
      name: String,
      description: String,
      default: T,
      sources: List[Source[_, _, T]],
      applicator: StateApplicator,
      combinationStrategy: CombinationStrategy[T] = graphCombinator[T]()): DefaultSerializableProcessSINodeInput[T] =
    newSerializableFromSources(name, description, default, sources, applicator, Behavior.DYNAMIC, combinationStrategy)

  def newSerializableFromSources[T >: Null <: AnyRef](
      name: String,
      description: String,
      default: T,
      sources: List[Source[_, _, T]],
      applicator: StateApplicator,
      forwardingBehavior: Behavior,
      combinationStrategy: CombinationStrategy[T]): DefaultSerializableProcessSINodeInput[T] =
    new DefaultSerializableProcessSINodeInput[T](
      name,
      description,
      defaultValue = default,
      sources = sources,
      requiresRestart = false,
      combinationStrategy = combinationStrategy,
      forwardingBehavior = forwardingBehavior,
      applicator
    )

  def newSerializableNoDefault[T >: Null <: AnyRef](
      name: String,
      description: String,
      source: Source[_, _, T],
      applicator: StateApplicator): DefaultSerializableProcessSINodeInput[T] =
    newSerializable(name, description, null, source, applicator)

  def newSerializableNoDefaultWithBehavior[T >: Null <: AnyRef](
      name: String,
      description: String,
      source: Source[_, _, T],
      applicator: StateApplicator,
      forwardingBehavior: Behavior): DefaultSerializableProcessSINodeInput[T] =
    newSerializableFromSources(name, description, null, List(source), applicator, forwardingBehavior, graphCombinator())
}
