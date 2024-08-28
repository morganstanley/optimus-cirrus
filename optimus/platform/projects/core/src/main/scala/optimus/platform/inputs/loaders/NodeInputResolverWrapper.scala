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
package optimus.platform.inputs.loaders

import optimus.platform.inputs.EngineAwareNodeInputResolver
import optimus.platform.inputs.NodeInput
import optimus.platform.inputs.NodeInputMapValue
import optimus.platform.inputs.NodeInputs.ScopedSINodeInput
import optimus.platform.inputs.ProcessState
import optimus.platform.inputs.ProcessSINodeInput

import java.util.Optional
import java.util.function.BiConsumer
import scala.compat.java8.OptionConverters._

/**
 * A wrapper class around a frozenNodeInputMap and the ProcessState that implements [[EngineAwareNodeInputResolver]]. A
 * resolver is needed for distribution purposes but there is no reason that [[FrozenNodeInputMap]] or the process state
 * should have any relation to a resolver. Therefore, this class is used whenever we need to create a
 * [[NodeInputResolver]] for an Optimus application. It will look through the frozen node input map and also the process
 * inputs defined in [[ProcessState.currentState]]
 *
 * @implNote
 *   we don't need to take a copy of the Settings object that holds all optimized process inputs since this is only used
 *   to send nodes off for distribution at which point setState will have already been called and everything in Settings
 *   will have been set to the right values (so there will never be a race between the values in ProcessMap and the
 *   values inside of Settings
 */
private[optimus] final case class NodeInputResolverWrapper(private val frozenNodeInputMap: FrozenNodeInputMap)
    extends EngineAwareNodeInputResolver {
  override def resolveNodeInput[T](nodeInput: NodeInput[T]): Optional[T] =
    (nodeInput match {
      case scopedSINodeInput: ScopedSINodeInput[T]   => frozenNodeInputMap.getInput(scopedSINodeInput)
      case processSINodeInput: ProcessSINodeInput[T] => ProcessState.getIfPresent(processSINodeInput)
      case _                                         => None
    }).asJava

  override def engineForwardingForEach(callback: BiConsumer[NodeInput[AnyRef], NodeInputMapValue[AnyRef]]): Unit = {
    frozenNodeInputMap.engineForwardingForEach(callback)
    ProcessState.engineForwardingForEach(callback)
  }
}
