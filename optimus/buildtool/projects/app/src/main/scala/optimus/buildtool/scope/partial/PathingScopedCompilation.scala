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
import scala.collection.immutable.Seq

// Note: PathingScopedCompilation doesn't extend PartialScopedCompilation; we don't want remote caching
// of pathing jars (since they contain workspace-specific paths)
@entity class PathingScopedCompilation(
    scope: CompilationScope,
    generator: ManifestGenerator,
    scala: ScalaScopedCompilation,
    java: JavaScopedCompilation,
    resources: ResourcePackaging,
    jmh: JmhScopedCompilation,
    cppFallback: Boolean
) {

  @node def pathing: Seq[Artifact] = PathingScopedCompilation.artifacts(generator, scope, runtimeArtifacts)

  @node private def upstreamArtifacts: Seq[Artifact] = scope.upstream.artifactsForOurRuntime

  @node private def runtimeArtifacts =
    scala.classes ++ java.classes ++ resources.resources ++ jmh.classes ++ upstreamArtifacts

}

@entity object PathingScopedCompilation {

  @node def artifacts(
      generator: ManifestGenerator,
      scope: CompilationScope,
      allRuntimeArtifacts: Seq[Artifact]
  ): Seq[Artifact] = if (ScopedCompilation.generate(AT.Pathing)) {
    val manifest = generator.manifest(
      scope.id,
      scope.config,
      allRuntimeArtifacts,
      scope.upstream.runtimeDependencies,
      scope.upstream.agentsForOurRuntimeArtifacts)
    val f = fingerprint(manifest, scope)
    val p = pathing(manifest, f, scope)
    Seq(f, p)
  } else Nil

  @node private def fingerprint(manifest: jar.Manifest, scope: CompilationScope): FingerprintArtifact = {
    val fingerprint = Jars.fingerprint(manifest)
    scope.hasher.hashFingerprint(fingerprint, PathingFingerprint)
  }

  @node private def pathing(
      manifest: jar.Manifest,
      f: FingerprintArtifact,
      scope: CompilationScope
  ): PathingArtifact = {
    val jarPath =
      scope.pathBuilder.outputPathFor(scope.id, f.hash, AT.Pathing, None)
    AssetUtils.atomicallyWriteIfMissing(jarPath) { tmpName =>
      ObtTrace.traceTask(scope.id, Pathing) { Jars.writeManifestJar(JarAsset(tmpName), manifest) }
    }
    AT.Pathing.fromAsset(scope.id, jarPath)
  }
}
