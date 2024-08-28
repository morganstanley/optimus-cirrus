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
import optimus.platform.inputs.NodeInputs.BaseNodeInput
import optimus.platform.inputs.NodeInputs.SINodeInput
import optimus.platform.inputs.StateApplicators.StateApplicator
import optimus.platform.inputs.dist.GSFSections
import optimus.platform.inputs.loaders.LoaderSource
import optimus.platform.inputs.registry.CombinationStrategies.CombinationStrategy
import optimus.platform.inputs.registry.Source

import scala.jdk.CollectionConverters._

/**
 * Base class for node inputs that affect the entire process. These inputs will be stored in a [[ProcessSINodeInputMap]]
 * held by the [[ProcessState]] object. Unlike scoped inputs these can only be defined in Scala code
 * @apiNote
 *   all process graph inputs should be defined within [[ProcessGraphInputs]]
 */
trait ProcessSINodeInput[T] extends SINodeInput[T] {
  self: BaseNodeInput[T] =>
  def stateApplicator: StateApplicator

  override final def affectsExecutionProcessWide(): Boolean = true

}

/**
 * Mixin trait for optimized inputs
 * @apiNote
 *   Making something optimized will set a MostlyConstant value to be accessed later to allow for inlining. However, if
 *   this variable changes we need to throw away all of the jitted code from before. So this should only be mixed in if
 *   the alternative (not inlining) causes a large performance degradation (i.e. the code that reads this input is on a
 *   hot code path)
 * @implNote
 *   this can only be accessed through where ever the [[StateApplicator]] stores the value (in a static field in Java
 *   class)
 */
sealed trait NonStandardProcessSINodeInput[T]

/**
 * Mixin trait for non-optimized inputs, this can only be accessed through input.currentValue which will get the value
 * of the input on the [[ProcessState]] or its default value
 *
 * @impleNote
 *   any non-optimized input can be accessed using inputName.currentValue()
 */
sealed trait DefaultProcessSINodeInput[T] { self: ProcessSINodeInput[T] =>
  final def currentValue(): Option[T] = ProcessState.getIfPresent(this)

  // safe to use if the input has a default value
  final def currentValueOrThrow(): T = currentValue().get

  // this will only use default if the node input is not present in process state and it does not have a default already defined
  final def currentValue(deflt: T): T = currentValue().getOrElse(deflt)
  // this will use the defualt if the node input is not present in the process state
  final def currentSetValueOrElse(deflt: T): T = ProcessState.getIfPresentWithoutDefault(this).getOrElse(deflt)

  // gets the source that the current input was loaded with if we specified anything at all
  private[optimus] final def currentSource: Option[LoaderSource] = ProcessState.getSourceIfPresentWithoutDefault(this)
}

sealed abstract class SerializableProcessSINodeInput[T](
    name: String,
    description: String,
    defaultValue: T,
    sources: List[Source[_, _, T]],
    requiresRestart: Boolean,
    combinationStrategy: CombinationStrategy[T],
    override val engineForwardingBehavior: Behavior,
    val stateApplicator: StateApplicator)
    extends BaseNodeInput(
      name,
      description,
      defaultValue,
      sources.asJava,
      GSFSections.none[T],
      requiresRestart,
      Behavior.DYNAMIC,
      combinationStrategy,
      false)
    with ProcessSINodeInput[T]

class NonStandardSerializableProcessSINodeInput[T](
    name: String,
    description: String,
    defaultValue: T,
    sources: List[Source[_, _, T]],
    requiresRestart: Boolean,
    combinationStrategy: CombinationStrategy[T],
    forwardingBehavior: Behavior,
    stateApplicator: StateApplicator)
    extends SerializableProcessSINodeInput(
      name,
      description,
      defaultValue,
      sources,
      requiresRestart,
      combinationStrategy,
      forwardingBehavior,
      stateApplicator)
    with NonStandardProcessSINodeInput[T]

class DefaultSerializableProcessSINodeInput[T](
    name: String,
    description: String,
    defaultValue: T,
    sources: List[Source[_, _, T]],
    requiresRestart: Boolean,
    combinationStrategy: CombinationStrategy[T],
    forwardingBehavior: Behavior,
    stateApplicator: StateApplicator)
    extends SerializableProcessSINodeInput(
      name,
      description,
      defaultValue,
      sources,
      requiresRestart,
      combinationStrategy,
      forwardingBehavior,
      stateApplicator)
    with DefaultProcessSINodeInput[T]

sealed abstract class TransientProcessSINodeInput[T](
    name: String,
    description: String,
    defaultValue: T,
    sources: List[Source[_, _, T]],
    gsfSection: GSFSections.GSFSection[T, _],
    combinationStrategy: CombinationStrategy[T],
    forwardingBehavior: Behavior,
    val stateApplicator: StateApplicator)
    extends BaseNodeInput[T](
      name,
      description,
      defaultValue,
      sources.asJava,
      gsfSection,
      true,
      forwardingBehavior,
      combinationStrategy,
      true)
    with ProcessSINodeInput[T]

class NonStandardTransientProcessSINodeInput[T](
    name: String,
    description: String,
    defaultValue: T,
    sources: List[Source[_, _, T]],
    gsfSection: GSFSections.GSFSection[T, _],
    combinationStrategy: CombinationStrategy[T],
    forwardingBehavior: Behavior,
    stateApplicator: StateApplicator)
    extends TransientProcessSINodeInput(
      name,
      description,
      defaultValue,
      sources,
      gsfSection,
      combinationStrategy,
      forwardingBehavior,
      stateApplicator)
    with NonStandardProcessSINodeInput[T]

class DefaultTransientProcessSINodeInput[T](
    name: String,
    description: String,
    defaultValue: T,
    sources: List[Source[_, _, T]],
    gsfSection: GSFSections.GSFSection[T, _],
    combinationStrategy: CombinationStrategy[T],
    forwardingBehavior: Behavior,
    stateApplicator: StateApplicator)
    extends TransientProcessSINodeInput(
      name,
      description,
      defaultValue,
      sources,
      gsfSection,
      combinationStrategy,
      forwardingBehavior,
      stateApplicator)
    with DefaultProcessSINodeInput[T]
