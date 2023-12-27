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
import optimus.buildtool.artifacts.ArtifactType
import optimus.buildtool.artifacts.ClassFileArtifact
import optimus.buildtool.artifacts.GeneratedSourceArtifact
import optimus.buildtool.artifacts.GeneratedSourceArtifactType
import optimus.buildtool.compilers.AsyncClassFileCompiler
import optimus.buildtool.compilers.SyncCompiler
import optimus.buildtool.config.JavacConfiguration
import optimus.buildtool.config.NamingConventions
import optimus.buildtool.config.ScalacConfiguration
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Directory
import optimus.buildtool.files.Directory.PathFilter
import optimus.buildtool.files.JarAsset
import optimus.buildtool.files.ReactiveDirectory
import optimus.buildtool.files.RelativePath
import optimus.buildtool.files.SourceUnitId
import optimus.buildtool.files.SourceFolder
import optimus.buildtool.scope.CompilationScope
import optimus.buildtool.trace.GenerateSource
import optimus.buildtool.trace.ObtTrace
import optimus.buildtool.utils.AsyncUtils
import optimus.buildtool.utils.HashedContent
import optimus.buildtool.utils.Hide
import optimus.buildtool.utils.TypeClasses._
import optimus.buildtool.utils.Utils
import optimus.platform._

import scala.collection.immutable.Seq
import scala.collection.immutable.SortedMap

@entity class CppBridgeGenerator(scalac: AsyncClassFileCompiler) extends SourceGenerator {
  override val artifactType: GeneratedSourceArtifactType = ArtifactType.CppBridge

  override type Inputs = CppBridgeGenerator.Inputs

  @node override def dependencies(scope: CompilationScope): Seq[Artifact] =
    scope.upstream.allCompileDependencies.apar.map(_.transitiveExternalDependencies) ++
      scope.upstream.signaturesForOurCompiler

  @node override protected def _inputs(
      name: String,
      internalFolders: Seq[SourceFolder],
      externalFolders: Seq[ReactiveDirectory],
      sourceFilter: PathFilter,
      configuration: Map[String, String],
      scope: CompilationScope
  ): Inputs = {
    require(
      externalFolders.isEmpty,
      s"External folders not supported for cpp-bridge source generation: ${externalFolders.mkString(",")}"
    )

    val filter = sourceFilter && SourceFolder.isScalaOrJavaSourceFile
    val templateContent = internalFolders.apar.map(_.findSourceFiles(filter)).merge[SourceUnitId]

    val templatesFingerprint = scope.fingerprint(templateContent, s"Template:$name")
    val configFingerprint = configuration.toSeq.map { case (k, v) => s"[Config:$name]$k=$v" }.sorted
    val dependenciesFingerprint = scope.scalaDependenciesFingerprint
    val fingerprint = templatesFingerprint ++ configFingerprint ++ dependenciesFingerprint

    val fingerprintHash = scope.hasher.hashFingerprint(fingerprint, ArtifactType.GenerationFingerprint)

    val allInputArtifacts = scope.upstream.signaturesForOurCompiler ++
      scope.upstream.allCompileDependencies.apar.flatMap(_.transitiveExternalDependencies.result.resolvedArtifacts)

    CppBridgeGenerator.Inputs(
      name,
      templateContent,
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

  @node override def containsRelevantSources(inputs: NodeFunction0[Inputs]): Boolean =
    inputs().templateContent.nonEmpty

  @node override def generateSource(
      scopeId: ScopeId,
      inputs: NodeFunction0[Inputs],
      outputJar: JarAsset
  ): Option[GeneratedSourceArtifact] = {
    ObtTrace.traceTask(scopeId, GenerateSource) {
      val resolvedInputs = inputs()

      // we don't actually care about the compiled artifacts created, so just put them in a temporary location which
      // we'll delete after the compile
      val placeholderOutputJar = NamingConventions.tempFor(outputJar).asJar
      Utils.atomicallyWrite(outputJar) { tempOut =>
        val tempJar = JarAsset(tempOut)
        val tempDir = tempJar.parent.resolveDir(tempJar.name.stripSuffix(".jar"))
        Utils.createDirectories(tempDir)
        val scalaOutDir = tempDir.resolveDir(CppBridgeGenerator.ScalaPath)
        val cppOutDir = tempDir.resolveDir(CppBridgeGenerator.CppPath)

        val msgArtifact = scalac.messages(
          scopeId.copy(tpe = s"${scopeId.tpe}-${tpe.name}"),
          asNode(() => scalacInputs(scopeId, inputs, scalaOutDir, cppOutDir, placeholderOutputJar))
        )

        val artifact = GeneratedSourceArtifact.create(
          scopeId,
          resolvedInputs.generatorName,
          artifactType,
          outputJar,
          CppBridgeGenerator.ScalaPath,
          msgArtifact.messages
        )
        AsyncUtils.asyncTry {
          SourceGenerator.createJar(
            resolvedInputs.generatorName,
            CppBridgeGenerator.ScalaPath,
            artifact.messages,
            artifact.hasErrors,
            tempJar,
            tempDir
          )()
        } asyncFinally {
          if (placeholderOutputJar.existsUnsafe) Files.delete(placeholderOutputJar.path)
        }
        Some(artifact)
      }
    }
  }

  @node private def scalacInputs(
      scopeId: ScopeId,
      inputs: NodeFunction0[Inputs],
      scalaOutDir: Directory,
      cppOutDir: Directory,
      dummyOutputJar: JarAsset
  ): SyncCompiler.Inputs = {
    val resolvedInputs = inputs()

    val pluginOptions = resolvedInputs.configuration ++ Map(
      "out" -> scalaOutDir.pathString,
      "outcpp" -> cppOutDir.pathString
    ) ++ (if (!resolvedInputs.configuration.contains("project")) Map("project" -> scopeId.module) else Map.empty)

    val bridgeParam = s"-P:${pluginOptions.map { case (k, v) => s"bridge:$k:$v" }.mkString(",")}"
    val options = resolvedInputs.scalacConfig.options :+ bridgeParam
    val scalaParams = resolvedInputs.scalacConfig.copy(options = options)

    SyncCompiler.Inputs(
      sourceFiles = resolvedInputs.templateContent,
      fingerprintHash = resolvedInputs.fingerprintHash,
      bestPreviousAnalysis = Hide(None),
      outPath = dummyOutputJar,
      signatureOutPath = None,
      scalacConfig = scalaParams,
      javacConfig = resolvedInputs.javacConfig,
      inputArtifacts = resolvedInputs.allInputArtifacts,
      pluginArtifacts = resolvedInputs.pluginArtifacts,
      outlineTypesOnly = false,
      saveAnalysisFiles = false,
      containsPlugin = false,
      containsMacros = false,
      mischief = None
    )
  }
}

object CppBridgeGenerator {
  val ScalaPath: RelativePath = RelativePath("src/scala")
  val CppPath: RelativePath = RelativePath("src/cpp")

  final case class Inputs(
      generatorName: String,
      templateContent: SortedMap[SourceUnitId, HashedContent],
      fingerprintHash: String,
      scalacConfig: ScalacConfiguration,
      javacConfig: JavacConfiguration,
      allInputArtifacts: Seq[Artifact],
      pluginArtifacts: Seq[Seq[ClassFileArtifact]],
      configuration: Map[String, String]
  ) extends SourceGenerator.Inputs

  import optimus.buildtool.cache.NodeCaching.reallyBigCache

  // since _inputs holds the template files and the hash, it's important that it's frozen for the duration of
  // a compilation (so that we're sure what we hashed is what we used for generation)
  _inputs.setCustomCache(reallyBigCache)
}
