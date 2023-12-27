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
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.SourceUnitId
import optimus.buildtool.scope.CompilationScope
import optimus.buildtool.utils.HashedContent
import optimus.buildtool.utils.TypeClasses._
import optimus.platform._
import optimus.scalacompat.collection._

import scala.collection.immutable.Seq
import scala.collection.immutable.SortedMap

@entity class CppCompilationSources(val osVersion: String, scope: CompilationScope) extends CompilationSources {

  override def id: ScopeId = scope.id

  @node protected def hashedSources: HashedSourcesImpl = {
    val fingerprintHash = scope.hasher.hashFingerprint(fingerprint, ArtifactType.CppFingerprint)
    HashedSourcesImpl(Seq("Source" -> sourceContent), Nil, fingerprintHash)
  }

  @node private def sourceContent: SortedMap[SourceUnitId, HashedContent] = {
    val sourceExclusions = scope.config.sourceExclusions
    scope.sourceFolders.apar
      .map { f =>
        f.cppSourceOrHeaderFiles.filterKeysNow { id =>
          sourceExclusions.forall(!_.pattern.matcher(id.sourceFolderToFilePath.pathString).matches)
        }
      }
      .merge[SourceUnitId]
  }

  @node private def fingerprint: Seq[String] = {
    import scope._

    val sourceFingerprint = scope.fingerprint(sourceContent, "Source")
    val inputArtifacts = fingerprintDeps(upstream.cppForOurOsCompiler(osVersion), "Dependency")
    val cppDeps = config.cppConfig(osVersion).fingerprint
    sourceFingerprint ++ inputArtifacts ++ cppDeps
  }

}

object CppCompilationSources {
  import optimus.buildtool.cache.NodeCaching._

  // since hashedSources holds the source files and the hash, it's important that
  // it's frozen for the duration of a compilation (so that we're sure what we hashed is what we compiled)
  hashedSources.setCustomCache(reallyBigCache)
}
