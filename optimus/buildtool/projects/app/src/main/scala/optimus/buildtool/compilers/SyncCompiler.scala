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

import java.util.UUID
import optimus.buildtool.artifacts.AnalysisArtifact
import optimus.buildtool.artifacts.Artifact
import optimus.buildtool.artifacts.ClassFileArtifact
import optimus.buildtool.artifacts.{ArtifactType => AT}
import optimus.buildtool.compilers.SyncCompiler.Inputs
import optimus.buildtool.config.JavacConfiguration
import optimus.buildtool.config.NamingConventions
import optimus.buildtool.config.ScalacConfiguration
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.JarAsset
import optimus.buildtool.files.SourceUnitId
import optimus.buildtool.format.MischiefArgs
import optimus.buildtool.trace.MessageTrace
import optimus.buildtool.utils.AssetUtils
import optimus.buildtool.utils.HashedContent
import optimus.buildtool.utils.Hide
import optimus.buildtool.utils.Utils
import optimus.platform._
import optimus.platform.AdvancedUtils.Throttle

import scala.collection.immutable.Seq
import scala.collection.immutable.SortedMap
import scala.util.Try

/**
 * Base trait for scala compilers in OBT.
 *
 * Handles setting up the compiler arguments and delegating to the real compiler, and makes the compiler writes atomic
 * by redirecting output to temporary jars which are then atomically moved to the target location.
 *
 * Note that all of this code is synchronous and off-graph - asynchronicity is handled by the AsyncScalaCompiler.
 */
private[buildtool] trait SyncCompiler {
  def compile(
      inputs: Inputs,
      signatureCompilerOutputConsumer: Option[Try[SignatureCompilerOutput] => Unit],
      activeTask: => Task
  ): CompilerOutput

  def cancel(): Unit
}

private[buildtool] object SyncCompiler {
  final case class Inputs(
      sourceFiles: SortedMap[SourceUnitId, HashedContent],
      fingerprintHash: String,
      bestPreviousAnalysis: Hide[Option[AnalysisArtifact]],
      outPath: JarAsset,
      signatureOutPath: Option[JarAsset],
      scalacConfig: ScalacConfiguration,
      javacConfig: JavacConfiguration,
      inputArtifacts: Seq[Artifact],
      pluginArtifacts: Seq[Seq[ClassFileArtifact]],
      outlineTypesOnly: Boolean,
      saveAnalysisFiles: Boolean, // note that if mischief is active we never save
      containsPlugin: Boolean,
      containsMacros: Boolean,
      mischief: Option[MischiefArgs]
  ) {
    def outputFile(tpe: AT): FileAsset = Utils.outputPathForType(outPath, tpe)
    val saveAnalysis: Boolean = saveAnalysisFiles && mischief.isEmpty
  }

  final case class PathPair private (uuid: UUID, tempPath: JarAsset, finalPath: JarAsset) {
    def moveTempToFinal(): Unit = AssetUtils.atomicallyMove(tempPath, finalPath)
    def forType(tpe: AT): PathPair = {
      PathPair(
        uuid = uuid,
        tempPath = Utils.outputPathForType(tempPath, tpe).asJar,
        finalPath = Utils.outputPathForType(finalPath, tpe).asJar
      )
    }
  }
  object PathPair {
    def apply(uuid: UUID, finalPath: JarAsset): PathPair =
      PathPair(uuid = uuid, tempPath = NamingConventions.tempFor(uuid, finalPath).asJar, finalPath = finalPath)
  }

  /**
   * ScalaC params with these prefixes are ignored when computing input fingerprints because they don't affect the
   * output of the compiler and it's useful for macro and compiler developers to be able to add these parameters without
   * needing to rebuild everything
   */
  val purelyDiagnosticScalaParamPrefixes: Seq[String] = Seq[String](
    "-Xdev",
    "-Xprint:",
    "-Xlog-",
    "-Ylog:",
    "-Ybrowse:",
    "-Ycheck:",
    "-Yshow:",
    "-Yshow-",
    "-Ystop-",
    "-Ydebug",
    "-Ymacro-debug-",
    "-Ystatistics:",
    "-Yprofile-")

}

trait SyncCompilerFactory {
  val instanceThrottle: Option[Throttle]
  val sizeThrottle: Option[Throttle]

  def fingerprint(traceType: MessageTrace): Seq[String]
  def newCompiler(scopeId: ScopeId, traceType: MessageTrace): SyncCompiler
  @async def throttled[T](weight: Int)(f: NodeFunction0NN[T]): T = instanceThrottle match {
    case Some(it) => it(asNode(() => sizeThrottled(weight)(f)))
    case None     => sizeThrottled(weight)(f)
  }
  @async private def sizeThrottled[T](weight: Int)(f: NodeFunction0NN[T]): T = sizeThrottle match {
    case Some(st) => st(f, NodeFunction1.identity[T], nodeWeight = weight)
    case None     => f()
  }
}
