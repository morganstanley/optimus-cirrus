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

import optimus.buildtool.artifacts.AnalysisArtifact
import optimus.buildtool.artifacts.ArtifactType
import optimus.buildtool.artifacts.CompilerMessagesArtifact
import optimus.buildtool.artifacts.InternalArtifactId
import optimus.buildtool.artifacts.InternalClassFileArtifact
import optimus.buildtool.artifacts.MessageArtifactType
import optimus.buildtool.artifacts.SignatureArtifact
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.JsonAsset
import optimus.buildtool.trace.Scala
import optimus.buildtool.trace.Signatures
import optimus.platform._

import scala.collection.immutable.Seq

final case class CompilerOutput(
    classes: Option[InternalClassFileArtifact],
    messages: CompilerMessagesArtifact,
    analysis: Option[AnalysisArtifact]
) {
  @node def watchForDeletion(): CompilerOutput = {
    classes.foreach(_.watchForDeletion())
    messages.watchForDeletion()
    analysis.foreach(_.watchForDeletion())
    this
  }

  def isEmpty: Boolean = classes.isEmpty && messages.messages.isEmpty && analysis.isEmpty
}

object CompilerOutput {
  @node def empty(scopeId: ScopeId, messageType: MessageArtifactType, messageFile: JsonAsset): CompilerOutput = {
    val messages = CompilerMessagesArtifact.create(
      InternalArtifactId(scopeId, messageType, None),
      messageFile,
      Seq(),
      Scala,
      incremental = false)
    messages.storeJson()
    CompilerOutput(None, messages, None)
  }
}

final case class SignatureCompilerOutput(
    signatures: Option[SignatureArtifact],
    messages: CompilerMessagesArtifact,
    analysis: Option[AnalysisArtifact]
) {
  @node def watchForDeletion(): SignatureCompilerOutput = {
    signatures.foreach(_.watchForDeletion())
    messages.watchForDeletion()
    analysis.foreach(_.watchForDeletion())
    this
  }
}
object SignatureCompilerOutput {
  def empty(scopeId: ScopeId, messageFile: JsonAsset): SignatureCompilerOutput = {
    // Watched via `outputVersions` in `AsyncScalaCompiler.output`/`AsyncScalaCompiler.signatureOutput`, which
    // are the leaf nodes that call this code
    val messages =
      CompilerMessagesArtifact.unwatched(
        InternalArtifactId(scopeId, ArtifactType.SignatureMessages, None),
        messageFile,
        Seq(),
        Signatures,
        incremental = false)
    messages.storeJson()
    SignatureCompilerOutput(None, messages, None)
  }
}
