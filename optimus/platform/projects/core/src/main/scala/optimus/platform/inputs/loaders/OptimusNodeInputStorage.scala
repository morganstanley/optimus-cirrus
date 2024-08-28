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

import optimus.platform.inputs.NodeInput
import optimus.platform.inputs.NodeInputResolver
import optimus.platform.inputs.NodeInputs
import optimus.platform.inputs.NodeInputs.ScopedSINodeInput
import optimus.platform.inputs.ProcessSINodeInput

/** @inheritdoc */
final case class OptimusNodeInputStorage private (
    private val frozenNodeInputMap: FrozenNodeInputMap,
    private val processSINodeInputMap: ProcessSINodeInputMap)
    extends NodeInputStorage[OptimusNodeInputStorage.OptimusStorageUnderlying] {
  import OptimusNodeInputStorage.{OptimusStorageUnderlying, filterProcessInput}

  private[platform] def appendCmdLineInput[T](e: NodeInputResolver.Entry[T]): OptimusNodeInputStorage =
    appendLocalInput(e.nodeInput, e.sourcedInput.value, forwardToEngine = true, LoaderSource.CMDLINE)

  override private[loaders] def appendLocalInput[T](
      input: NodeInput[T],
      value: T,
      forwardToEngine: Boolean,
      source: LoaderSource): OptimusNodeInputStorage =
    input match {
      case processInput: ProcessSINodeInput[T] =>
        if (filterProcessInput(processInput))
          copy(frozenNodeInputMap, processSINodeInputMap.appendLocalInput(processInput, value, source, forwardToEngine))
        else this
      case scopedSINodeInput: ScopedSINodeInput[T] =>
        if (!scopedSINodeInput.affectsExecutionProcessWide)
          copy(
            frozenNodeInputMap.appendLocalInput(scopedSINodeInput, value, source, forwardToEngine),
            processSINodeInputMap)
        else this
      case _ =>
        throw new IllegalArgumentException(
          "OptimusNodeInputStorage only contains ScopedSINodeInputs and ProcessSINodeInputs!")
    }

  override private[loaders] def appendEngineSpecificInput[T](
      input: NodeInput[T],
      value: T,
      source: LoaderSource): OptimusNodeInputStorage = {
    NodeInputStorage.checkAppendToEngine(input)
    input match {
      case processInput: ProcessSINodeInput[T] =>
        if (filterProcessInput(processInput))
          copy(frozenNodeInputMap, processSINodeInputMap.appendEngineSpecificInput(processInput, value, source))
        else this
      case scopedSINodeInput: ScopedSINodeInput[T] =>
        if (!scopedSINodeInput.affectsExecutionProcessWide)
          copy(frozenNodeInputMap.appendEngineSpecificInput(scopedSINodeInput, value, source), processSINodeInputMap)
        else this
      case _ =>
        throw new IllegalArgumentException(
          "OptimusNodeInputStorage only contains ScopedSINodeInputs and ProcessSINodeInputs!")
    }
  }

  override def underlying: OptimusStorageUnderlying =
    OptimusStorageUnderlying(frozenNodeInputMap, processSINodeInputMap)
}

object OptimusNodeInputStorage {
  // serializable inputs that require restart need explicit support from dist first
  private def filterProcessInput[T](processInput: ProcessSINodeInput[T]): Boolean =
    !(NodeInputs.isJavaSerialized(processInput) && processInput
      .requiresRestart()) && processInput.affectsExecutionProcessWide

  private[optimus] final case class OptimusStorageUnderlying(
      frozenNodeInputMap: FrozenNodeInputMap,
      processSINodeInputMap: ProcessSINodeInputMap)

  private[platform] def apply(underlying: OptimusStorageUnderlying): OptimusNodeInputStorage =
    OptimusNodeInputStorage(underlying.frozenNodeInputMap, underlying.processSINodeInputMap)

  val empty: OptimusNodeInputStorage = OptimusNodeInputStorage(FrozenNodeInputMap.empty, ProcessSINodeInputMap.empty)

  private[optimus] def loadDefaults: NodeInputStorage[OptimusStorageUnderlying] =
    Loaders.environmentVariables(Loaders.systemProperties(OptimusNodeInputStorage.empty))

  private[optimus] def loadScopedState: FrozenNodeInputMap = loadDefaults.underlying().frozenNodeInputMap
}
