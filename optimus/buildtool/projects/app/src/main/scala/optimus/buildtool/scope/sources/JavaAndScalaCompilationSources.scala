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
package optimus.buildtool.scope.sources

import optimus.buildtool.artifacts.Artifact
import optimus.buildtool.artifacts.ArtifactType
import optimus.buildtool.artifacts.FingerprintArtifact
import optimus.buildtool.artifacts.ResolutionArtifact
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.SourceUnitId
import optimus.buildtool.scope.CompilationScope
import optimus.buildtool.utils.HashedContent
import optimus.platform._

import scala.collection.immutable.Seq
import scala.collection.immutable.SortedMap
import optimus.scalacompat.collection._

private[sources] final case class HashedJavaAndScalaSources(
    content: Seq[(String, SortedMap[SourceUnitId, HashedContent])],
    generatedSourceArtifacts: Seq[Artifact],
    externalCompileDependencies: Seq[ResolutionArtifact],
    fingerprintHash: String,
    fingerprintArtifact: Option[FingerprintArtifact]
) extends HashedSources

@entity class JavaAndScalaCompilationSources(
    scope: CompilationScope,
    source: SourceCompilationSources
) extends CompilationSources {

  override def id: ScopeId = scope.id

  @node def fingerprintArtifact: Option[FingerprintArtifact] = hashedSources.fingerprintArtifact
  @node def externalCompileDependencies: Seq[ResolutionArtifact] = hashedSources.externalCompileDependencies

  @node protected def hashedSources: HashedJavaAndScalaSources = {
    // the source files could be changing while the build is running (e.g. developer is still editing files in the IDE),
    // so we are careful here to ensure that the source file content that we pass in to the compiler is the same
    // as the content that we hash. hashedSources is part of the reallyBigCache to ensure they don't get evicted mid-build.
    val sourceFileContent = source.content

    val externalDeps = scope.upstream.allCompileDependencies.apar.map(_.transitiveExternalDependencies)

    val compilationInputsFingerprint: Seq[String] = {
      val sourceFingerprints = source.fingerprint

      val dependenciesFingerprints = scope.scalaDependenciesFingerprint
      val javaParamFingerprints = scope.fingerprintParams(scope.config.javacConfig.resolvedOptions, "MiscJavaParam")
      val javaWarningsFingerprints = scope.config.javacConfig.warnings.fingerprint.map(s => s"[JavaWarnings]$s")

      sourceFingerprints ++ dependenciesFingerprints ++ javaParamFingerprints ++ javaWarningsFingerprints
    }

    val fingerprintHash =
      scope.hasher.hashFingerprint(compilationInputsFingerprint, ArtifactType.CompilationFingerprint)
    val fingerprintArtifact =
      scope.hasher.fingerprintArtifact(compilationInputsFingerprint, ArtifactType.CompilationFingerprint)

    HashedJavaAndScalaSources(
      sourceFileContent,
      source.generatedSourceArtifacts,
      externalDeps,
      fingerprintHash,
      fingerprintArtifact
    )
  }

  @node def containsScala: Boolean = compilationSources.keys.exists(_.suffix == "scala")

  @node def containsJava: Boolean = compilationSources.keys.exists(_.suffix == "java")
  @node def javaSources: SortedMap[SourceUnitId, HashedContent] = compilationSources.filterKeysNow(_.suffix == "java")
}

object JavaAndScalaCompilationSources {
  import optimus.buildtool.cache.NodeCaching._

  // since hashedSources holds the source files and the hash, it's important that
  // it's frozen for the duration of a compilation (so that we're sure what we hashed is what we compiled)
  hashedSources.setCustomCache(reallyBigCache)
}
