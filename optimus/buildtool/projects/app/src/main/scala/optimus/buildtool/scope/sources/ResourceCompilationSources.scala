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

import optimus.buildtool.artifacts.ArtifactType
import optimus.buildtool.artifacts.FingerprintArtifactType
import optimus.buildtool.artifacts.GeneratedSourceArtifact
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.SourceFolder.isResourceFile
import optimus.buildtool.files.SourceUnitId
import optimus.buildtool.scope.CompilationScope
import optimus.buildtool.scope.SourceGeneration
import optimus.buildtool.trace.CategoryTrace
import optimus.buildtool.trace.HashResources
import optimus.buildtool.trace.HashArchiveContents
import optimus.buildtool.trace.ObtTrace
import optimus.buildtool.utils.HashedContent
import optimus.buildtool.utils.TypeClasses._
import optimus.platform._

import scala.collection.immutable.Seq
import scala.collection.immutable.SortedMap

@entity trait ResourceCompilationSources extends CompilationSources {
  protected val scope: CompilationScope

  protected val hashTask: CategoryTrace
  protected val fingerprintType: FingerprintArtifactType

  @node protected def hashedSources: HashedSources = {
    // the resource files could be changing while the build is running (e.g. developer is still editing files in the IDE),
    // so we are careful here to ensure that the source file content that we pass in to the compiler is the same
    // as the content that we hash. hashedSources is part of the reallyBigCache to ensure they don't get evicted mid-build.
    val sourceContent = this.sourceContent

    val resourceFingerprint = sourceContent.content.apar.flatMap { case (tpe, content) =>
      scope.fingerprint(content, tpe)
    }
    val tokensFingerprint = scope.config.resourceTokens.map { case (k, v) => s"[Token]$k=$v" }
    val fingerprintHash = scope.hasher.hashFingerprint(resourceFingerprint ++ tokensFingerprint, fingerprintType)

    HashedSourcesImpl(
      sourceContent.content,
      sourceContent.generatedSourceArtifacts,
      fingerprintHash
    )
  }

  @node protected def sourceContent: SourceFileContent

}

object ResourceCompilationSources {
  import optimus.buildtool.cache.NodeCaching.reallyBigCache

  // since hashedSources holds the file content and the hash, it's important that it's frozen for the duration of a
  // compilation (so that we're sure what we hashed is what we compiled)
  hashedSources.setCustomCache(reallyBigCache)
}

@entity class ResourceCompilationSourcesImpl(
    protected val scope: CompilationScope,
    sourceGeneration: SourceGeneration
) extends ResourceCompilationSources {

  override def id: ScopeId = scope.id

  override protected val fingerprintType: FingerprintArtifactType = ArtifactType.ResourceFingerprint
  override protected val hashTask: CategoryTrace = HashResources

  @node override protected def sourceContent: SourceFileContent = {
    val generatedSources = sourceGeneration.generatedSources
    ObtTrace.traceTask(id, hashTask) {
      val generatedSourceContent = generatedSources.apar.collect { case s: GeneratedSourceArtifact =>
        s"Generated:${s.tpe.name}" -> s.hashedContent(isResourceFile)
      }

      SourceFileContent(staticContent +: generatedSourceContent, generatedSources)
    }
  }

  @node private[sources] def staticContent: (String, SortedMap[SourceUnitId, HashedContent]) = {
    val content =
      scope.resourceFolders.apar.map(_.resources()).merge[SourceUnitId] ++
        scope.sourceFolders.apar.map(_.resources(includeSources = false)).merge[SourceUnitId]
    "Resource" -> content
  }
}

@entity class ArchivePackageSources(protected val scope: CompilationScope) extends ResourceCompilationSources {

  override def id: ScopeId = scope.id

  protected val fingerprintType: FingerprintArtifactType = ArtifactType.ArchiveFingerprint
  override protected val hashTask: CategoryTrace = HashArchiveContents

  @node @node override protected def sourceContent: SourceFileContent = {
    val content = scope.archiveContentFolders.apar.map(_.resources()).merge[SourceUnitId]
    SourceFileContent(Seq("Archive" -> content), Nil)
  }
}
