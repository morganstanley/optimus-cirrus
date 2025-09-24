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

import optimus.buildtool.artifacts.Artifact
import optimus.buildtool.artifacts.ArtifactType.PathingFingerprint
import optimus.buildtool.artifacts.{ArtifactType => AT}
import optimus.buildtool.artifacts.FingerprintArtifact
import optimus.buildtool.artifacts.PathingArtifact
import optimus.buildtool.compilers.ManifestGenerator
import optimus.buildtool.files.JarAsset
import optimus.buildtool.scope.CompilationScope
import optimus.buildtool.scope.ScopedCompilation
import optimus.buildtool.trace.ObtTrace
import optimus.buildtool.trace.Pathing
import optimus.buildtool.utils.AssetUtils
import optimus.buildtool.utils.Jars
import optimus.platform._

import java.util.jar
import scala.collection.immutable.IndexedSeq

// Note: PathingScopedCompilation doesn't extend CachedPartialScopedCompilation; we don't want remote caching
// of pathing jars (since they contain workspace-specific paths)
@entity class PathingScopedCompilation(
    override protected val scope: CompilationScope,
    generator: ManifestGenerator,
    scala: ScalaScopedCompilation,
    java: JavaScopedCompilation,
    resources: ResourcePackaging,
    jmh: JmhScopedCompilation
) extends PartialScopedCompilation {

  override type ArtifactTypeBound = AT

  @node protected def containsRelevantSources: Boolean = runtimeArtifacts.nonEmpty
  @node protected def upstreamArtifacts: IndexedSeq[Artifact] =
    scope.upstream.artifactsForOurRuntime ++ scope.upstream.agentsForOurRuntime

  @node override protected def fingerprintArtifact: Option[FingerprintArtifact] = Some(fingerprint)

  @node def pathing: IndexedSeq[Artifact] = compile(AT.Pathing, None) {
    Some(PathingScopedCompilation.pathing(generator, scope, runtimeArtifacts, agentArtifacts))
  }

  @node private def fingerprint: FingerprintArtifact =
    PathingScopedCompilation.fingerprint(generator, scope, runtimeArtifacts, agentArtifacts)

  @node private def runtimeArtifacts =
    scala.classes ++ java.classes ++ resources.resources ++ jmh.classes ++ scope.upstream.artifactsForOurRuntime

  @node private def agentArtifacts = scope.upstream.agentsForOurRuntime

}

@entity object PathingScopedCompilation {

  @node def artifacts(
      generator: ManifestGenerator,
      scope: CompilationScope,
      allRuntimeArtifacts: IndexedSeq[Artifact],
      allAgentArtifacts: IndexedSeq[Artifact]
  ): IndexedSeq[Artifact] = if (ScopedCompilation.generate(AT.Pathing)) {
    Artifact.onlyErrors(allRuntimeArtifacts ++ allAgentArtifacts).getOrElse {
      val f = fingerprint(generator, scope, allRuntimeArtifacts, allAgentArtifacts)
      val p = pathing(generator, scope, allRuntimeArtifacts, allAgentArtifacts)
      Vector(f, p)
    }
  } else Vector()

  @node private def pathing(
      generator: ManifestGenerator,
      scope: CompilationScope,
      allRuntimeArtifacts: IndexedSeq[Artifact],
      allAgentArtifacts: IndexedSeq[Artifact]
  ): PathingArtifact = {
    val f = fingerprint(generator, scope, allRuntimeArtifacts, allAgentArtifacts)
    val m = manifest(generator, scope, allRuntimeArtifacts, allAgentArtifacts)
    val jarPath =
      scope.pathBuilder.outputPathFor(scope.id, f.hash, AT.Pathing, None)
    AssetUtils.atomicallyWriteIfMissing(jarPath) { tmpName =>
      ObtTrace.traceTask(scope.id, Pathing) { Jars.writeManifestJar(JarAsset(tmpName), m) }
    }
    AT.Pathing.fromAsset(scope.id, jarPath)
  }

  @node private def fingerprint(
      generator: ManifestGenerator,
      scope: CompilationScope,
      allRuntimeArtifacts: IndexedSeq[Artifact],
      allAgentArtifacts: IndexedSeq[Artifact]
  ): FingerprintArtifact = {
    val m = manifest(generator, scope, allRuntimeArtifacts, allAgentArtifacts)
    val fingerprint = Jars.fingerprint(m)
    scope.hasher.hashFingerprint(fingerprint, PathingFingerprint)
  }

  @node private def manifest(
      generator: ManifestGenerator,
      scope: CompilationScope,
      allRuntimeArtifacts: IndexedSeq[Artifact],
      allAgentArtifacts: IndexedSeq[Artifact]
  ): jar.Manifest = generator.manifest(
    scope.id,
    scope.config,
    allRuntimeArtifacts,
    allAgentArtifacts,
    scope.upstream.runtimeDependencies
  )
}
