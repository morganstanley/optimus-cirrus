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
import optimus.buildtool.artifacts.GeneratedSourceArtifact
import optimus.buildtool.cache.NodeCaching.optimizerCache
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.SourceFolder
import optimus.buildtool.files.SourceUnitId
import optimus.buildtool.scope.CompilationScope
import optimus.buildtool.scope.SourceGeneration
import optimus.buildtool.trace.HashSources
import optimus.buildtool.trace.ObtTrace
import optimus.buildtool.utils.HashedContent
import optimus.buildtool.utils.TypeClasses._
import optimus.platform._
import optimus.scalacompat.collection._
import optimus.utils.DuplicateKeyException

import scala.collection.compat._
import scala.collection.immutable.Seq
import scala.collection.immutable.SortedMap

private[sources] final case class FingerprintedSources(
    content: Seq[(String, SortedMap[SourceUnitId, HashedContent])],
    generatedSourceArtifacts: Seq[Artifact],
    fingerprint: Seq[String],
    fingerprintHash: String
) extends HashedSources

@entity class SourceCompilationSources(
    scope: CompilationScope,
    sourceGeneration: SourceGeneration
) extends CompilationSources {

  override def id: ScopeId = scope.id

  @node def fingerprint: Seq[String] = hashedSources.fingerprint

  @node protected def hashedSources: FingerprintedSources = {
    // the source files could be changing while the build is running (e.g. developer is still editing files in the IDE),
    // so we are careful here to ensure that the source file content that we pass in to the compiler is the same
    // as the content that we hash. hashedSources is part of the reallyBigCache to ensure they don't get evicted mid-build.
    val sourceFileContent = this.sourceFileContent

    val sourceFingerprint = sourceFileContent.content.apar.flatMap { case (tpe, content) =>
      scope.fingerprint(content, tpe)
    }
    val fingerprintHash = scope.hasher.hashFingerprint(sourceFingerprint, ArtifactType.SourceFingerprint)

    FingerprintedSources(
      sourceFileContent.content,
      sourceFileContent.generatedSourceArtifacts,
      sourceFingerprint,
      fingerprintHash
    )
  }

  @node private def sourceFileContent: SourceFileContent = {
    val generatedSources = sourceGeneration.generatedSources
    ObtTrace.traceTask(id, HashSources) {
      val generatedSourceContent = generatedSources.apar.collect { case s: GeneratedSourceArtifact =>
        s"Generated:${s.tpe.name}" -> s.hashedContent(SourceFolder.isScalaOrJavaSourceFile)
      }

      val staticSourceContent = staticContent

      checkForDuplicatePaths(Seq(staticSourceContent), generatedSourceContent)

      val content = staticSourceContent +: generatedSourceContent
      SourceFileContent(content, generatedSources)
    }
  }

  @node private[sources] def staticContent: (String, SortedMap[SourceUnitId, HashedContent]) = {
    val sourceExclusions = scope.config.sourceExclusions
    val content = scope.sourceFolders.apar
      .map { f =>
        f.scalaAndJavaSourceFiles.filterKeysNow { id =>
          sourceExclusions.forall(!_.pattern.matcher(id.sourceFolderToFilePath.pathString).matches)
        }
      }
      .merge[SourceUnitId]
    "Source" -> content
  }

  // Ensure we don't have duplicated source paths between static and generated source content. Note that within
  // each source type we allow duplicates, since in some cases different generators may create some of the same files.
  private def checkForDuplicatePaths(
      staticSourceContent: Seq[(String, SortedMap[SourceUnitId, HashedContent])],
      generatedSourceContent: Seq[(String, SortedMap[SourceUnitId, HashedContent])]
  ): Unit = {
    def distinctSourcePaths(content: Seq[(String, SortedMap[SourceUnitId, HashedContent])]): Seq[String] =
      content.flatMap { case (_, ids) => ids.keySet }.map(_.sourceFolderToFilePath.pathString).distinct

    val staticPaths = distinctSourcePaths(staticSourceContent)
    val generatedPaths = distinctSourcePaths(generatedSourceContent)

    val duplicatePaths = (staticPaths ++ generatedPaths).groupBy(identity).filter { case (_, ps) => ps.size > 1 }
    if (duplicatePaths.nonEmpty)
      throw new DuplicateKeyException(duplicatePaths.keys.to(Seq).sorted, "Duplicate generated source file paths")
  }

}

object SourceCompilationSources {
  import optimus.buildtool.cache.NodeCaching.reallyBigCache

  // since hashedSources holds the source files and the hash, it's important that it's frozen for the duration of a
  // compilation (so that we're sure what we hashed is what we compiled)
  hashedSources.setCustomCache(reallyBigCache)

  // don't rehash source files unless we really need to (purely for performance reasons, hence
  // why it uses the optimizerCache rather than reallyBigCache)
  sourceFileContent.setCustomCache(optimizerCache)
}
