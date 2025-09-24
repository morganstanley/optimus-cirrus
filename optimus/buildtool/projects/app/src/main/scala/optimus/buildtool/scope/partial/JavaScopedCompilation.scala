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
import optimus.buildtool.artifacts.Artifact
import optimus.buildtool.artifacts.ArtifactType.JavaAnalysis
import optimus.buildtool.artifacts.ExternalClassFileArtifact
import optimus.buildtool.artifacts.FingerprintArtifact
import optimus.buildtool.artifacts.{ArtifactType => AT}
import optimus.buildtool.compilers.AsyncClassFileCompiler
import optimus.buildtool.compilers.SyncCompiler
import optimus.buildtool.compilers.zinc.AnalysisLocator
import optimus.buildtool.compilers.zinc.ZincIncrementalMode
import optimus.buildtool.config.AfsNamingConventions
import optimus.buildtool.config.ScalacConfiguration
import optimus.buildtool.files.JarAsset
import optimus.buildtool.resolvers.DependencyCopier
import optimus.buildtool.scope.CompilationScope
import optimus.buildtool.scope.sources.JavaAndScalaCompilationSources
import optimus.buildtool.utils.Hide
import optimus.buildtool.utils.OptimusBuildToolProperties
import optimus.buildtool.utils.OsUtils
import optimus.buildtool.utils.Utils.distinctLast
import optimus.platform._

import scala.collection.immutable.{IndexedSeq, Seq}

@entity object JavaScopedCompilation {
  object Config {
    val incrementalJava: Boolean = OptimusBuildToolProperties.getOrTrue("incrementalJava")
  }

  @node def updateNetworkPaths(dependencyCopier: DependencyCopier, artifacts: Seq[Artifact]): Seq[Artifact] = {
    artifacts.apar.map { a =>
      dependencyCopier.atomicallyDepCopyArtifactsIfMissing(a) match {
        case c: ExternalClassFileArtifact if OsUtils.isWindows =>
          c.copyWithUpdatedAssets(
            file = replaceUncPath(c.file),
            source = c.source.map(replaceUncPath),
            javadoc = c.javadoc.map(replaceUncPath)
          )
        case a => a
      }
    }
  }

  // Javac can't cope with UNC paths, so convert to mapped drive path
  private def replaceUncPath(j: JarAsset): JarAsset =
    if (j.pathString.startsWith(AfsNamingConventions.AfsRootStr))
      JarAsset(
        j.fileSystem.getPath(
          j.pathString.replaceFirst(AfsNamingConventions.AfsRootStr, AfsNamingConventions.AfsRootMapping)))
    else j
}

@entity private[scope] class JavaScopedCompilation(
    override protected val scope: CompilationScope,
    override protected val sources: JavaAndScalaCompilationSources,
    javac: AsyncClassFileCompiler,
    analysisLocator: Option[AnalysisLocator],
    incrementalMode: IncrementalMode,
    signatures: SignatureScopedCompilation,
    scala: ScalaScopedCompilation
) extends PartialScopedClassCompilation {
  import scope._

  @node override protected def containsRelevantSources: Boolean = sources.containsJava

  @node override protected def upstreamArtifacts: IndexedSeq[Artifact] = distinctLast {
    val scalaClasses = scala.classes.filter(!_.isInstanceOf[FingerprintArtifact])
    val scalaMessages = scala.messages.filter(!_.isInstanceOf[FingerprintArtifact])
    // if we don't have scala classes, then we don't need the signature analysis for them
    val scalaArtifacts =
      if (scalaClasses.nonEmpty && config.usePipelining) signatures.analysis ++ scalaClasses ++ scalaMessages
      else if (scalaClasses.nonEmpty) scalaClasses ++ scalaMessages ++ scala.analysis
      else scalaMessages
    scalaArtifacts.filter(!_.isInstanceOf[FingerprintArtifact]) ++ upstream.classesForOurCompiler
  }.toVector

  @node def messages: IndexedSeq[Artifact] = compile(AT.JavaMessages, None)(Some(javac.messages(id, javacInputsN)))

  @node def classes: IndexedSeq[Artifact] = compile(AT.Java, None)(javac.classes(id, javacInputsN))

  @node def analysis: IndexedSeq[Artifact] =
    analysisWithLocator(javaAnalysis, AT.JavaAnalysis, analysisLocator).analysis.toVector

  @node def locator: IndexedSeq[Artifact] =
    analysisWithLocator(javaAnalysis, AT.JavaAnalysis, analysisLocator).locator.toVector

  @node protected def javaAnalysis: IndexedSeq[Artifact] =
    compile(AT.JavaAnalysis, None)(javac.analysis(id, javacInputsN))

  private val javacInputsN = asNode(() => javacInputs)

  @node private def javacInputs: SyncCompiler.Inputs = {
    val fingerprint = sources.compilationFingerprint

    val bestPreviousAnalysis = analysisLocator.flatMap { l =>
      val mode =
        if (JavaScopedCompilation.Config.incrementalJava) incrementalMode.zincIncrementalMode
        else ZincIncrementalMode.DryRun
      l.findAnalysis(id, JavaAnalysis, mode, requireSignatures = false)
    }
    val inputArtifacts =
      upstreamArtifacts ++ sources.externalCompileDependencies.apar.flatMap(_.result.resolvedArtifacts)

    SyncCompiler.Inputs(
      sourceFiles = sources.javaSources,
      fingerprint = fingerprint,
      bestPreviousAnalysis = Hide(bestPreviousAnalysis),
      outPath = pathBuilder.javaOutPath(id, fingerprint.hash),
      signatureOutPath = None,
      scalacConfig = ScalacConfiguration.empty,
      javacConfig = config.javacConfig,
      inputArtifacts = JavaScopedCompilation.updateNetworkPaths(dependencyCopier, inputArtifacts),
      pluginArtifacts = Nil,
      outlineTypesOnly = false,
      saveAnalysisFiles = true,
      containsPlugin = false,
      containsAgent = false,
      containsMacros = false,
      mischief = scope.mischief
    )
  }

}
