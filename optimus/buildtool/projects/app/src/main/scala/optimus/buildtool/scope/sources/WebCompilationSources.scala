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
import optimus.buildtool.files.Directory.PredicateFilter
import optimus.buildtool.files.SourceUnitId
import optimus.buildtool.scope.CompilationScope
import optimus.buildtool.utils.TypeClasses._
import optimus.platform._

import java.nio.file.Paths
import scala.collection.immutable.Seq

@entity class WebCompilationSources(scope: CompilationScope) extends CompilationSources {

  override def id: ScopeId = scope.id

  @node override protected def hashedSources: HashedSources = {
    val tpe = "Source"
    val rootPath = scope.webSourceFolders.headOption
      .map(f => f.workspaceSourceRoot.resolveDir(f.workspaceSrcRootToSourceFolderPath).path)
      .getOrElse(Paths.get(""))
    val hashedFiles = scope.webSourceFolders.apar
      .map { f =>
        val fileFilter = PredicateFilter { path =>
          val firstFolder = rootPath.relativize(path).subpath(0, 1).toString
          !WebCompilationSources.ignoredFolders.contains(firstFolder)
        }
        f.findSourceFiles(fileFilter)
      }
      .merge[SourceUnitId]
    val fingerprint = scope.fingerprint(hashedFiles, tpe) ++ scope.webDependenciesFingerprint
    val fingerprintHash = scope.hasher.hashFingerprint(fingerprint, ArtifactType.WebFingerprint)
    HashedSourcesImpl(Seq(tpe -> hashedFiles), Nil, fingerprintHash)
  }
}

object WebCompilationSources {
  import optimus.buildtool.cache.NodeCaching.reallyBigCache

  // since hashedSources holds the source files and the hash, it's important that
  // it's frozen for the duration of a compilation (so that we're sure what we hashed is what we compiled)
  hashedSources.setCustomCache(reallyBigCache)

  private val ignoredFolders = Set("node_modules", ".idea")
}
