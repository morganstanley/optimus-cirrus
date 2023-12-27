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
package optimus.buildtool.scope.partial

import optimus.buildtool.app.IncrementalMode
import optimus.buildtool.artifacts.AnalysisArtifact
import optimus.buildtool.artifacts.Artifact
import optimus.buildtool.artifacts.ArtifactType.ScalaAnalysis
import optimus.buildtool.artifacts.ClassFileArtifact
import optimus.buildtool.artifacts.{ArtifactType => AT}
import optimus.buildtool.compilers.AsyncClassFileCompiler
import optimus.buildtool.compilers.SyncCompiler
import optimus.buildtool.compilers.zinc.AnalysisLocator
import optimus.buildtool.scope.CompilationScope
import optimus.buildtool.scope.sources.JavaAndScalaCompilationSources
import optimus.buildtool.utils.Hide
import optimus.platform._

import scala.collection.immutable.Seq

@entity class ScalaCompilationInputs(
    scope: CompilationScope,
    val sources: JavaAndScalaCompilationSources,
    val analysisLocator: Option[AnalysisLocator],
    incrementalMode: IncrementalMode
) {
  import scope._
  import sources._

  @node private[partial] def upstreamArtifacts: Seq[Artifact] = upstream.signaturesForOurCompiler

  private[partial] val scalacInputsN: NodeFunction0[SyncCompiler.Inputs] = asNode(() => scalacInputs)

  @node private def scalacInputs: SyncCompiler.Inputs = {
    val fingerprintHash = compilationInputsHash
    val id = sources.id

    val bestPreviousAnalysis = analysisLocator.flatMap { locator =>
      locator.findAnalysis(
        id,
        ScalaAnalysis,
        incrementalMode.zincIncrementalMode,
        config.usePipelining
      )
    }
    val incremental = bestPreviousAnalysis.isDefined
    val signatureOutPath =
      if (config.usePipelining) Some(pathBuilder.signatureOutPath(id, fingerprintHash, incremental))
      else None

    /**
     * IMPORTANT: if you add any new inputs which could affect the compiler output then make sure to incorporate them in
     * the hash calculation in [[CompilationScope.scalaDependenciesFingerprint]]
     */
    SyncCompiler.Inputs(
      sourceFiles = compilationSources,
      fingerprintHash = fingerprintHash,
      bestPreviousAnalysis = Hide(bestPreviousAnalysis),
      outPath = pathBuilder.scalaOutPath(id, fingerprintHash, incremental),
      signatureOutPath = signatureOutPath,
      scalacConfig = config.scalacConfig,
      javacConfig = config.javacConfig,
      inputArtifacts = scalaInputArtifacts.apar.map(dependencyCopier.atomicallyDepCopyArtifactsIfMissing),
      pluginArtifacts =
        pluginArtifacts.apar.map(_.apar.map(dependencyCopier.atomicallyDepCopyClassFileArtifactsIfMissing)),
      outlineTypesOnly = false,
      saveAnalysisFiles = true,
      containsPlugin = config.containsPlugin,
      containsMacros = config.containsMacros,
      mischief = scope.mischief
    )
  }
}

/** Inherited by ScalaScopeCompilation and SignatureScopeCompilation. */
@entity private[partial] trait ScalaScopedCompilationBase extends PartialScopedClassCompilation {
  protected def compilationInputs: ScalaCompilationInputs
  override protected def sources: JavaAndScalaCompilationSources = compilationInputs.sources

  @node override protected def upstreamArtifacts: Seq[Artifact] = compilationInputs.upstreamArtifacts

  // (the plugins can have errors too)
  @node override protected def prerequisites: Seq[Artifact] =
    super.prerequisites ++ scope.upstream.pluginsForOurCompiler

  // Note that the NF0 needs to be created in ScalaCompilationInputs to ensure that two similar NodeFunctions
  // from SignatureScopedCompilation and ScalaScopedCompilation compare equal (to avoid recompilation)
  protected def scalacInputsN: NodeFunction0[SyncCompiler.Inputs] = compilationInputs.scalacInputsN
}

@entity private[scope] class ScalaScopedCompilation(
    override protected val scope: CompilationScope,
    scalac: AsyncClassFileCompiler,
    override protected val compilationInputs: ScalaCompilationInputs
) extends ScalaScopedCompilationBase {
  import compilationInputs._
  import scope._

  @node override protected def containsRelevantSources: Boolean = sources.containsScala

  @node def messages: Seq[Artifact] = compile(AT.ScalaMessages, None)(Some(scalac.messages(id, scalacInputsN)))

  @node def classes: Seq[Artifact] =
    compile(AT.Scala, None)(scalac.classes(id, scalacInputsN)).apar.map {
      // Cached artifacts don't know if they contain plugins or macros, so we need to update them here. This
      // can be removed if the cached artifacts are changed to include that information.
      case c: ClassFileArtifact =>
        c.copy(containsPlugin = config.containsPlugin, containsOrUsedByMacros = config.containsMacros)
      case x => x
    }

  @node def analysis: Seq[Artifact] = analysisWithLocator.analysis

  @node def locator: Seq[Artifact] = analysisWithLocator.locator.toIndexedSeq

  @node protected def scalaAnalysis: Seq[Artifact] = compile(AT.ScalaAnalysis, None)(scalac.analysis(id, scalacInputsN))

  @node def analysisWithLocator: AnalysisWithLocator = {
    val locator = analysisLocator.flatMap { l =>
      // Save locators for a good build even if we got a cache hit, to ensure we
      // capture the current commit hash and build time.
      if (scalaAnalysis.exists(_.isInstanceOf[AnalysisArtifact]))
        l.saveLocator(id, AT.ScalaAnalysis, pathBuilder, sources.compilationInputsHash)
      else None
    }
    AnalysisWithLocator(scalaAnalysis, locator)
  }

}

object ScalaScopedCompilation {
  import optimus.buildtool.cache.NodeCaching.optimizerCache

  // every call to scalaFinalAnalysis updates the locators (potentially remotely as well as locally), so avoid
  // calling this repeatedly where possible
  analysis.setCustomCache(optimizerCache)
}
