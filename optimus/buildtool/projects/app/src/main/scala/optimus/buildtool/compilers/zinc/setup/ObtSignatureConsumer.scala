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
package optimus.buildtool.compilers.zinc.setup

import optimus.buildtool.artifacts.AnalysisArtifact
import optimus.buildtool.artifacts.CompilerMessagesArtifact
import optimus.buildtool.artifacts.InternalArtifactId
import optimus.buildtool.artifacts.SignatureArtifact
import optimus.buildtool.artifacts.{ArtifactType => AT}
import optimus.buildtool.compilers.SignatureCompilerOutput
import optimus.buildtool.compilers.SyncCompiler
import optimus.buildtool.compilers.SyncCompiler.PathPair
import optimus.buildtool.compilers.Task
import optimus.buildtool.compilers.zinc.ZincCompilerFactory
import optimus.buildtool.config.ScopeId
import optimus.buildtool.trace.MessageTrace
import optimus.buildtool.trace.Signatures
import optimus.buildtool.utils.Hashing
import optimus.buildtool.utils.Utils
import optimus.buildtool.utils.{Jars => JarUtils}
import optimus.platform._

import scala.collection.immutable.Seq
import scala.util.Failure
import scala.util.Success
import scala.util.Try

object ObtSignatureConsumer {
  private val log = msjava.slf4jutils.scalalog.getLogger(this)
}

class ObtSignatureConsumer(
    scopeId: ScopeId,
    traceType: MessageTrace,
    activeTask: => Task,
    settings: ZincCompilerFactory,
    inputs: SyncCompiler.Inputs,
    incremental: Boolean,
    signatureJar: PathPair,
    signatureAnalysisJar: PathPair,
    signatureCompilerOutputConsumer: Try[SignatureCompilerOutput] => Unit
) extends (() => Unit) {
  import ObtSignatureConsumer._

  private val prefix = Utils.logPrefix(scopeId, traceType)
  private val cancelScope = EvaluationContext.cancelScope

  override def apply(): Unit = {
    activeTask.trace.reportProgress("storing signatures")
    log.debug(s"${prefix}Signature callback: ${signatureJar.tempPath}")
    // important not to move the file if we were cancelled during compilation because it might be incomplete
    val signatureOutput = if (!cancelScope.isCancelled) {
      val signatureMessages = CompilerMessagesArtifact.unwatched(
        InternalArtifactId(scopeId, AT.SignatureMessages, None),
        Utils.outputPathForType(inputs.outPath, AT.SignatureMessages).asJson,
        Seq(),
        Signatures,
        incremental
      )
      signatureMessages.storeJson()

      // Watched via `outputVersions` in `AsyncScalaCompiler.output`/`AsyncScalaCompiler.signatureOutput`, which
      // are the leaf nodes that call this code
      val signatures = {
        // we incrementally rewrite these and it's much cheaper if they aren't compressed
        JarUtils.stampJarWithConsistentHash(signatureJar.tempPath, compress = false)
        signatureJar.moveTempToFinal()
        SignatureArtifact.unwatched(
          InternalArtifactId(scopeId, AT.JavaAndScalaSignatures, None),
          signatureJar.finalPath,
          Hashing.hashFileOrDirectoryContent(signatureJar.finalPath),
          incremental
        )
      }
      val signatureAnalysis = if (inputs.saveAnalysis) {
        // don't stamp with content hash, since we don't ever use analysis artifacts as part of a fingerprint
        signatureAnalysisJar.moveTempToFinal()
        Some(
          AnalysisArtifact.unwatched(
            InternalArtifactId(scopeId, AT.SignatureAnalysis, None),
            signatureAnalysisJar.finalPath,
            incremental
          )
        )
      } else None
      Success(SignatureCompilerOutput(Some(signatures), signatureMessages, signatureAnalysis))
    } else Failure(cancelScope.cancellationCause)
    signatureCompilerOutputConsumer(signatureOutput)
  }
}
