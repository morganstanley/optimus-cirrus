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
package optimus.buildtool.generators

import java.nio.file.Files
import optimus.buildtool.artifacts.Artifact
import optimus.buildtool.artifacts.ClassFileArtifact
import optimus.buildtool.artifacts.FingerprintArtifact
import optimus.buildtool.artifacts.GeneratedSourceArtifact
import optimus.buildtool.compilers.AsyncClassFileCompiler
import optimus.buildtool.compilers.SyncCompiler
import optimus.buildtool.config.JavacConfiguration
import optimus.buildtool.config.NamingConventions
import optimus.buildtool.config.ScalacConfiguration
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Directory
import optimus.buildtool.files.Directory.PathFilter
import optimus.buildtool.files.JarAsset
import optimus.buildtool.files.RelativePath
import optimus.buildtool.files.SourceUnitId
import optimus.buildtool.files.SourceFolder
import optimus.buildtool.generators.sandboxed.SandboxedFiles
import optimus.buildtool.generators.sandboxed.SandboxedInputs
import optimus.buildtool.scope.CompilationScope
import optimus.buildtool.utils.AsyncUtils
import optimus.buildtool.utils.CompilePathBuilder
import optimus.buildtool.utils.Hide
import optimus.buildtool.utils.Utils
import optimus.platform._

import scala.collection.immutable.{IndexedSeq, Seq}

@entity class CppBridgeGenerator(scalac: AsyncClassFileCompiler, pathBuilder: CompilePathBuilder)
    extends SourceGenerator {
  override val generatorType: String = "cpp-bridge"

  override type Inputs = CppBridgeGenerator.Inputs

  override def templateType(configuration: Map[String, String]): PathFilter = SourceFolder.isScalaOrJavaSourceFile

  @node override def dependencies(scope: CompilationScope): IndexedSeq[Artifact] =
    scope.upstream.allCompileDependencies.apar.flatMap(_.resolution) ++
      scope.upstream.signaturesForOurCompiler

  @async override protected def _inputs(
      templates: SandboxedInputs,
      configuration: Map[String, String],
      scope: CompilationScope
  ): Inputs = {
    import templates._

    val externalFiles = allExternalContent.keySet
    require(
      externalFiles.isEmpty,
      s"External folders not supported for cpp-bridge source generation: ${externalFiles.mkString(", ")}"
    )

    val dependenciesFingerprint = scope.scalaDependenciesFingerprint

    val fingerprintHash = templates.hashFingerprint(configuration, dependenciesFingerprint)

    val allInputArtifacts: Seq[Artifact] = scope.upstream.signaturesForOurCompiler ++
      scope.upstream.allCompileDependencies.apar.flatMap(_.transitiveExternalDependencies)

    CppBridgeGenerator.Inputs(
      templates.toFiles,
      fingerprintHash,
      scope.config.scalacConfig,
      scope.config.javacConfig,
      allInputArtifacts.apar.map(scope.dependencyCopier.atomicallyDepCopyArtifactsIfMissing),
      scope.pluginArtifacts.apar.map(
        _.apar.map(scope.dependencyCopier.atomicallyDepCopyClassFileArtifactsIfMissing)
      ),
      configuration
    )
  }

  @async override def _generateSource(
      scopeId: ScopeId,
      inputs: Inputs,
      writer: ArtifactWriter
  ): Option[GeneratedSourceArtifact] = writer.atomicallyWrite { tempJar =>
    val tempDir = tempJar.parent.resolveDir(tempJar.name.stripSuffix(".jar"))
    Utils.createDirectories(tempDir)

    // we don't actually care about the compiled artifacts created, so just put them in a
    // temporary location which we'll delete after the compilation. they do need to be in the `scala` directory
    // though otherwise PathedArtifact validation will complain.
    val tempOutputJar =
      NamingConventions.tempFor(pathBuilder.scalaOutPath(scopeId, inputs.fingerprint.hash)).asJar

    val scalaOutDir = tempDir.resolveDir(CppBridgeGenerator.ScalaPath)
    val cppOutDir = tempDir.resolveDir(CppBridgeGenerator.CppPath)

    AsyncUtils.asyncTry {
      val messages = scalac
        .messages(
          scopeId.copy(tpe = s"${scopeId.tpe}-${tpe.name}"),
          asNode(() => scalacInputs(scopeId, inputs, tempOutputJar, scalaOutDir, cppOutDir))
        )
        .messages

      writer.createArtifact(tempDir, tempJar, messages, CppBridgeGenerator.ScalaPath)()
    } asyncFinally {
      if (tempOutputJar.existsUnsafe) Files.delete(tempOutputJar.path)
    }
  }

  @async private def scalacInputs(
      scopeId: ScopeId,
      inputs: Inputs,
      tempOutputJar: JarAsset,
      scalaOutDir: Directory,
      cppOutDir: Directory
  ): SyncCompiler.Inputs = {
    import inputs._

    val sourceFiles = files.internalContent().map { case (k, v) => (k: SourceUnitId) -> v }

    val pluginOptions = inputs.configuration ++ Map(
      "out" -> scalaOutDir.pathString,
      "outcpp" -> cppOutDir.pathString
    ) ++ (if (!inputs.configuration.contains("project")) Map("project" -> scopeId.module) else Map.empty)

    val bridgeParam = s"-P:${pluginOptions.map { case (k, v) => s"bridge:$k:$v" }.mkString(",")}"
    val options = inputs.scalacConfig.options :+ bridgeParam
    val scalaParams = inputs.scalacConfig.copy(options = options)

    SyncCompiler.Inputs(
      sourceFiles = sourceFiles,
      fingerprint = fingerprint,
      bestPreviousAnalysis = Hide(None),
      outPath = tempOutputJar,
      signatureOutPath = None,
      scalacConfig = scalaParams,
      javacConfig = javacConfig,
      inputArtifacts = allInputArtifacts,
      pluginArtifacts = pluginArtifacts,
      outlineTypesOnly = false,
      saveAnalysisFiles = false,
      containsPlugin = false,
      containsAgent = false,
      containsMacros = false,
      mischief = None
    )
  }
}

object CppBridgeGenerator {
  val ScalaPath: RelativePath = RelativePath("src/scala")
  val CppPath: RelativePath = RelativePath("src/cpp")

  final case class Inputs(
      files: SandboxedFiles,
      fingerprint: FingerprintArtifact,
      scalacConfig: ScalacConfiguration,
      javacConfig: JavacConfiguration,
      allInputArtifacts: Seq[Artifact],
      pluginArtifacts: Seq[Seq[ClassFileArtifact]],
      configuration: Map[String, String]
  ) extends SourceGenerator.Inputs
}
