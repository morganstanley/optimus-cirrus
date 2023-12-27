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
package optimus.buildtool.compilers

import optimus.buildtool.artifacts.CompilationMessage
import optimus.buildtool.artifacts.InMemoryMessagesArtifact
import optimus.buildtool.artifacts.InternalArtifactId
import optimus.buildtool.artifacts.MessageArtifactType
import optimus.buildtool.config.ScopeId
import optimus.buildtool.trace.MessageTrace

import scala.collection.immutable.Seq
class CompilationException(
    val scopeId: ScopeId,
    message: String,
    val otherMessages: Seq[CompilationMessage],
    cause: Throwable,
    val messageType: MessageArtifactType,
    val taskCategory: MessageTrace
) extends RuntimeException(message, cause) {

  def artifactId = InternalArtifactId(scopeId, messageType, None)

  def artifact = InMemoryMessagesArtifact(
    artifactId,
    CompilationMessage.error(this) +: otherMessages,
    taskCategory
  )
}
