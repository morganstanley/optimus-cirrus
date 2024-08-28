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

import optimus.platform.EvaluationContext
import optimus.platform.inputs.EngineAwareNodeInputResolver
import optimus.platform.inputs.NodeInput
import optimus.platform.inputs.loaders.OptimusNodeInputStorage.OptimusStorageUnderlying

import java.util.Properties

object OptimusSafeSystemPropertiesUtils {

  def extractFrom(frozenNodeInputMap: FrozenNodeInputMap): NodeInputStorage[EngineAwareNodeInputResolver] = {
    OptimusNodeInputStorage.empty.copy(frozenNodeInputMap = frozenNodeInputMap).map {
      case OptimusStorageUnderlying(frozen, _) => NodeInputResolverWrapper(frozen)
    }
  }

  def extractFromCurrentScenarioStack(properties: Properties): NodeInputStorage[EngineAwareNodeInputResolver] = {
    val frozenNodeInputMap = EvaluationContext.scenarioStack.siParams.nodeInputs.freeze
    val otherProps = Loaders.javaNonForwardingProperties(OptimusNodeInputStorage.empty, properties)
    val finalNodeInputMap = otherProps.underlying().frozenNodeInputMap.mergeWith(frozenNodeInputMap)
    extractFrom(finalNodeInputMap)
  }

  private implicit class NodeInputStorageOps[A](val nodeInputStorage: NodeInputStorage[A]) extends AnyVal {
    def map[B](fn: A => B): NodeInputStorage[B] = new NodeInputStorage[B] {
      override def underlying(): B = fn(nodeInputStorage.underlying())
      override def appendLocalInput[T](
          input: NodeInput[T],
          value: T,
          forwardToEngine: Boolean,
          source: LoaderSource): NodeInputStorage[B] =
        nodeInputStorage.appendLocalInput(input, value, forwardToEngine, source).map(fn)
      override def appendEngineSpecificInput[T](
          input: NodeInput[T],
          value: T,
          source: LoaderSource): NodeInputStorage[B] = {
        NodeInputStorage.checkAppendToEngine(input)
        nodeInputStorage.appendEngineSpecificInput(input, value, source).map(fn)
      }
    }
  }

}
