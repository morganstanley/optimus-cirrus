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
package optimus.buildtool.scope

import optimus.buildtool.app.CompilationNodeFactory
import optimus.buildtool.artifacts.ArtifactType.CompileOnlyResolution
import optimus.buildtool.artifacts.ArtifactType.CompileResolution
import optimus.buildtool.artifacts.ArtifactType.RuntimeResolution
import optimus.buildtool.artifacts.InternalArtifactId
import optimus.buildtool.artifacts.ResolutionArtifact
import optimus.buildtool.artifacts.ResolutionArtifactType
import optimus.buildtool.cache.ArtifactCache
import optimus.buildtool.config.Dependencies
import optimus.buildtool.config.DependencyDefinition
import optimus.buildtool.config.DependencyDefinitions
import optimus.buildtool.config.HasScopeId
import optimus.buildtool.config.NativeDependencyDefinition
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Asset
import optimus.buildtool.resolvers.ExternalDependencyResolver
import optimus.buildtool.trace.ObtTrace
import optimus.buildtool.utils.CompilePathBuilder
import optimus.buildtool.utils.Utils.distinctLast
import optimus.platform._

import scala.collection.immutable.Seq

@entity class ScopeDependencies(
    val id: ScopeId,
    dependencies: Dependencies,
    externalNativeDependencies: Seq[NativeDependencyDefinition],
    val tpe: ResolutionArtifactType,
    pathBuilder: CompilePathBuilder,
    externalDependencyResolver: ExternalDependencyResolver,
    scopedCompilationFactory: CompilationNodeFactory,
    cache: ArtifactCache,
    hasher: FingerprintHasher
) extends HasScopeId {

  @node def directScopeDependencies: Seq[CompilationNode] = {
    internalDependencyIds.distinct
      .sortBy(_.toString)
      .apar
      .flatMap(scopedCompilationFactory.lookupScope)
  }

  @node def transitiveScopeDependencies: Seq[CompilationNode] =
    distinctLast(
      directScopeDependencies.apar
        .flatMap { d =>
          d +: ScopeDependencies.dependencies(tpe, d).transitiveScopeDependencies
        })

  @node def transitiveExternalDependencyIds: DependencyDefinitions = {
    val upstreamExtDeps =
      transitiveScopeDependencies.apar.flatMap(ScopeDependencies.dependencies(tpe, _).externalDependencyIds)
    DependencyDefinitions(directIds = distinctLast(externalDependencyIds), indirectIds = distinctLast(upstreamExtDeps))
  }

  /** All JNI paths, both from our explicit declaration of native dependencies and from ivy files. */
  @node def transitiveJniPaths: Seq[String] =
    transitiveExternalDependencies.result.jniPaths ++ transitiveNativeDependencies.flatMap(_.paths)

  @node def transitiveExtraFiles: Seq[Asset] = transitiveNativeDependencies.flatMap(_.extraPaths)

  @node private def transitiveNativeDependencies: Seq[NativeDependencyDefinition] =
    (externalNativeDependencies
      ++ transitiveScopeDependencies.apar.flatMap(_.runtimeDependencies.transitiveNativeDependencies)).distinct

  @node def externalInputsHash: String = {
    hasher.hashFingerprint(
      externalDependencyResolver.fingerprintDependencies(transitiveExternalDependencyIds.all),
      tpe.fingerprintType
    )
  }

  @node def transitiveExternalDependencies: ResolutionArtifact = {
    val fingerprintHash = externalInputsHash
    val result = cache.getOrCompute[ResolutionArtifactType](id, tpe, None, fingerprintHash) {
      ObtTrace.traceTask(id, tpe.category) {
        val tpeStr = tpe.name.replace('-', ' ')
        log.info(s"[$id] Starting $tpeStr")
        val resolved = externalDependencyResolver.resolveDependencies(transitiveExternalDependencyIds)

        val artifact = ResolutionArtifact.create(
          InternalArtifactId(id, tpe, None),
          resolved,
          pathBuilder.outputPathFor(id, fingerprintHash, tpe, None, incremental = false).asJson,
          tpe.category)
        artifact.storeJson()
        log.info(s"[$id] Completed $tpeStr")
        Some(artifact)
      }
    }
    // (should never get None because if it was missing in cache we compute it)
    result.getOrElse(throw new IllegalStateException("Resolution didn't exist"))
  }

  @node private def internalDependencyIds: Seq[ScopeId] = dependencies.internal

  @node private def externalDependencyIds: Seq[DependencyDefinition] = dependencies.external

  override def toString: String = s"ScopeDependencies($id)"
}

object ScopeDependencies {
  private def dependencies(tpe: ResolutionArtifactType, scope: CompilationNode): ScopeDependencies = tpe match {
    // for CompileOnly, we want the transitive Compile deps NOT the transitive CompileOnly deps
    case CompileResolution | CompileOnlyResolution => scope.upstream.compileDependencies
    case RuntimeResolution                         => scope.runtimeDependencies
  }

  import optimus.buildtool.cache.NodeCaching.reallyBigCache

  // avoiding resolving external dependencies multiple times as this could make our build slower than necessary
  transitiveExternalDependencies.setCustomCache(reallyBigCache)
}
