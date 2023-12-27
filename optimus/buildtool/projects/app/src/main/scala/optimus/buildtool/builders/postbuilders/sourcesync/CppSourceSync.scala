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
package optimus.buildtool.builders.postbuilders.sourcesync

import java.nio.file.Paths

import optimus.buildtool.artifacts.GeneratedSourceArtifact
import optimus.buildtool.config.ScopeConfiguration
import optimus.buildtool.config.ScopeConfigurationSource
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Directory
import optimus.buildtool.generators.CppBridgeGenerator
import optimus.platform._

class CppSourceSync(
    protected val scopeConfigSource: ScopeConfigurationSource,
    targets: Map[ScopeId, Directory]
) extends SourceSync {

  override protected val descriptor: String = "generated C++ sources"

  @node override protected def source(artifact: GeneratedSourceArtifact, jarRoot: Directory): Option[Directory] =
    Some(jarRoot.resolveDir(CppBridgeGenerator.CppPath))

  @node override protected def target(id: ScopeId, scopeConfig: ScopeConfiguration): Option[Directory] =
    targets.get(id)
}

object CppSourceSync {
  @node def apply(
      scopeConfigSource: ScopeConfigurationSource,
      installDir: Directory,
      installVersion: String,
      scopeTargets: Map[String, String]
  ): CppSourceSync = {
    val targets = scopeTargets.apar.flatMap { case (partialId, dirStr) =>
      val scopes = scopeConfigSource.resolveScopes(partialId)
      scopes.map { scopeId =>
        val resolvedDirStr = dirStr
          .replace("$meta", scopeId.meta)
          .replace("$bundle", scopeId.bundle)
          .replace("$module", scopeId.module)
          .replace("$type", scopeId.tpe)
          .replace("$installVersion", installVersion)
        val path = Paths.get(resolvedDirStr)
        val dir = if (path.isAbsolute) Directory(path) else installDir.resolveDir(resolvedDirStr)
        scopeId -> dir
      }
    }
    new CppSourceSync(scopeConfigSource, targets)
  }
}
