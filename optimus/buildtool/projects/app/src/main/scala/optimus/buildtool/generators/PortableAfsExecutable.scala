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
package optimus.buildtool.generators

import optimus.buildtool.config.DependencyDefinition
import optimus.buildtool.config.NamingConventions
import optimus.buildtool.files.FileAsset
import optimus.buildtool.scope.CompilationScope
import optimus.buildtool.utils.Utils

import java.nio.file.Paths

final case class PortableAfsExecutable(
    windows: AfsExecutable,
    linux: AfsExecutable
) {

  /**
   * Returns the AFS executable corresponding to the current platform, modified by whatever is in the config file.
   */
  def configured(config: Map[String, String]): AfsExecutable = {
    // could conceivably be extended to other platforms...
    val (default, platform) = if (Utils.isWindows) (windows, "windows") else (linux, "linux")

    def get(key: String, default: String): String = config.getOrElse(s"$key.$platform", default)

    default.copy(
      metaDir = get("execMetaDir", default.metaDir),
      projectDir = get("execProjectDir", default.projectDir),
      executablePath = get("execPath", default.executablePath),
      variant = config.get("variant")
    )
  }
}

final case class AfsExecutable(
    metaDir: String,
    projectDir: String,
    executablePath: String,
    variant: Option[String]
) {
  // TODO (OPTIMUS-55793) AFS roots should be read from config
  def file(version: String): FileAsset =
    FileAsset(Paths.get(s"${NamingConventions.AfsDistStr}$metaDir/PROJ/$projectDir/$version/$executablePath"))

  def dependencyDefinition(scope: CompilationScope): DependencyDefinition =
    scope.externalDependencyResolver.dependencyDefinitions
      .find(d => d.group == metaDir && d.name == projectDir && d.variant.map(_.name) == variant)
      .getOrElse {
        val suffix = variant.map(v => s".$v").getOrElse("")
        throw new IllegalArgumentException(s"No central dependency found for ${metaDir}.${projectDir}$suffix")
      }
}
