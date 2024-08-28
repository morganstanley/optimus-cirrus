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
package optimus.dist.inputs

import optimus.platform.inputs.loaders.FoldedNodeInput
import optimus.platform.inputs.loaders.ProcessSINodeInputMap

/**
 * Class used for dist to get access to the process state when serializing a node task
 * @implNote
 *   anything in optimus can create this class but it will be effectively useless unless created within the dist project
 *   since otherwise the only value of the class can't be read
 */
private[optimus] final case class ProcessStateSnapshot private[inputs] (
    private[dist] val currentState: ProcessSINodeInputMap) {
  private[optimus] def foldLeft[B](initial: B)(op: (B, FoldedNodeInput) => B): B = currentState.foldLeft(initial)(op)
}
