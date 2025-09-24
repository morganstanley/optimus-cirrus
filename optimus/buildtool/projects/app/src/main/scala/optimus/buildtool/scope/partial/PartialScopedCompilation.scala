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

import optimus.buildtool.artifacts.AnalysisArtifact
import optimus.buildtool.artifacts.AnalysisArtifactType
import optimus.buildtool.artifacts.Artifact
import optimus.buildtool.artifacts.ArtifactType
import optimus.buildtool.artifacts.CachedArtifactType
import optimus.buildtool.artifacts.ClassFileArtifact
import optimus.buildtool.artifacts.FingerprintArtifact
import optimus.buildtool.artifacts.LocatorArtifact
import optimus.buildtool.compilers.zinc.AnalysisLocator
import optimus.buildtool.scope.CompilationScope
import optimus.buildtool.scope.ScopedCompilation
import optimus.buildtool.scope.sources.CompilationSources
import optimus.buildtool.scope.sources.JavaAndScalaCompilationSources
import optimus.core.needsPluginAlwaysAutoAsyncArgs
import optimus.platform._
import optimus.platform.annotations.alwaysAutoAsyncArgs

import java.time.Instant
import scala.collection.immutable.IndexedSeq

@entity private[partial] trait PartialScopedCompilation {
  type ArtifactTypeBound <: ArtifactType
  protected def scope: CompilationScope

  @node protected def upstreamArtifacts: IndexedSeq[Artifact]

  @node protected def prerequisites: IndexedSeq[Artifact] = upstreamArtifacts
  @node final protected def upstreamErrors: Option[Seq[Artifact]] = Artifact.onlyErrors(prerequisites)

  @node protected def containsRelevantSources: Boolean
  @node protected final def shouldCompile[A <: ArtifactType](tpe: A): Boolean =
    ScopedCompilation.generate(tpe) && containsRelevantSources

  @node protected def fingerprintArtifact: Option[FingerprintArtifact] = None

  @alwaysAutoAsyncArgs
  protected final def compile[A <: ArtifactTypeBound](tpe: A, discriminator: Option[String])(
      f: => Option[A#A]
  ): IndexedSeq[Artifact] = needsPluginAlwaysAutoAsyncArgs
  @node protected def compile$NF[A <: ArtifactTypeBound](tpe: A, discriminator: Option[String])(
      f: NodeFunction0[Option[A#A]]
  ): Seq[Artifact] = {
    if (shouldCompile(tpe)) upstreamErrors.getOrElse {
      markClassFileArtifacts(doCompile(tpe, discriminator)(f)) ++ fingerprintArtifact
    }
    else fingerprintArtifact.toIndexedSeq
  }

  @node protected def doCompile[A <: ArtifactTypeBound](tpe: A, discriminator: Option[String])(
      f: NodeFunction0[Option[A#A]]
  ): Seq[Artifact] = f().toIndexedSeq

  @node protected final def markClassFileArtifacts(artifacts: Iterable[Artifact]): IndexedSeq[Artifact] =
    artifacts.apar.map {
      // Cached artifacts don't know if they contain plugins or macros, so we need to update them here. This
      // can be removed if the cached artifacts are changed to include that information.
      case c: ClassFileArtifact =>
        c.copy(
          containsPlugin = scope.config.containsPlugin,
          containsAgent = scope.config.containsAgent,
          containsOrUsedByMacros = scope.config.containsMacros)
      case x => x
    }.toIndexedSeq

  override def toString: String = s"${getClass.getSimpleName}(${scope.id})"
}

@entity private[partial] trait CachedPartialScopedCompilation extends PartialScopedCompilation {
  override type ArtifactTypeBound = CachedArtifactType

  protected def sources: CompilationSources

  @node override protected final def fingerprintArtifact: Option[FingerprintArtifact] = Some(fingerprint)
  @node protected def fingerprint: FingerprintArtifact = sources.compilationFingerprint

  @node override protected def doCompile[A <: ArtifactTypeBound](tpe: A, discriminator: Option[String])(
      f: NodeFunction0[Option[A#A]]
  ): Seq[Artifact] = scope.cached$NF(tpe, discriminator, fingerprint.hash)(f)
}

private[buildtool] final case class AnalysisWithLocator(analysis: Seq[Artifact], locator: Option[LocatorArtifact])

@entity private[partial] trait PartialScopedClassCompilation extends CachedPartialScopedCompilation {
  protected def sources: JavaAndScalaCompilationSources

  // generatedSourceArtifacts and externalCompileDependencies can have errors too
  @node override protected def prerequisites: IndexedSeq[Artifact] =
    sources.generatedSourceArtifacts ++ sources.externalCompileDependencies ++ super.prerequisites

  @node protected def analysisWithLocator(
      analysis: Seq[Artifact],
      at: AnalysisArtifactType,
      analysisLocator: Option[AnalysisLocator]
  ): AnalysisWithLocator = {
    val locator = analysisLocator.flatMap { l =>
      // A couple of subtleties here:
      // 1. We save locators for a good build even if we get a local/remote cache hit, to
      //    ensure we record the fact that this is the most recent build for this commit (we don't have
      //    to do this for node cache hits since DependencyTracker invalidation means we'll never
      //    get us a node cache hit for anything other than the most recent previous build).
      // 2. `saveLocator` is not RT, so we make the return of this method (`analysisWithLocator`) RT by
      //    stripping out the timestamp part of the `LocatorArtifact` (which would otherwise be set to now).
      if (analysis.exists(_.isInstanceOf[AnalysisArtifact])) {
        l.saveLocator(scope.id, at, scope.pathBuilder, sources.compilationFingerprint.hash)
          .map(_.withTimestamp(Instant.EPOCH))
      } else None
    }
    AnalysisWithLocator(analysis, locator)
  }

}
