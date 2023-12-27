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

import optimus.buildtool.artifacts.GeneratedSourceArtifact
import optimus.buildtool.config.NamingConventions
import optimus.buildtool.config.ScopeConfiguration
import optimus.buildtool.config.ScopeConfigurationSource
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Directory
import optimus.platform._

/**
 * Copies generated scala sources back into the workspace source root. This isn't needed for OBT to build the code (it
 * just reads generated sources directly from the GeneratedSourceArtifacts), but is useful for IntelliJ to correctly
 * syntax highlight and navigate the code (since it doesn't know about the GeneratedSourceArtifacts).
 */
class GeneratedScalaSourceSync(protected val scopeConfigSource: ScopeConfigurationSource) extends SourceSync {

  override protected val descriptor: String = "generated scala sources"

  @node override protected def source(artifact: GeneratedSourceArtifact, jarRoot: Directory): Option[Directory] =
    Some(jarRoot.resolveDir(artifact.sourcePath))

  @node override protected def target(id: ScopeId, scopeConfig: ScopeConfiguration): Option[Directory] =
    if (scopeConfigSource.local(id))
      Some(GeneratedScalaSourceSync.generatedRoot(scopeConfig.paths.absScopeRoot))
    else
      None // we're probably running in a sparse workspace, in which case we don't want the generated sources locally
}

object GeneratedScalaSourceSync {
  def generatedRoot(scopeRoot: Directory): Directory = scopeRoot.resolveDir(NamingConventions.GeneratedObt)
}
