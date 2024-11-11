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
package optimus.buildtool.builders.postbuilders.metadata

import optimus.buildtool.config.DependencyDefinition
import optimus.buildtool.config.DockerImage
import optimus.buildtool.config.StratoConfig
import optimus.buildtool.files.Directory
import optimus.buildtool.files.InstallPathBuilder
import optimus.buildtool.files.RelativePath
import optimus.buildtool.resolvers.WebDependencyResolver

final case class MetadataSettings(
    installPathBuilder: InstallPathBuilder,
    extraLibs: Seq[DependencyDefinition],
    webDependencyResolver: WebDependencyResolver,
    leafDir: RelativePath,
    installVersion: String,
    obtVersion: String,
    javaVersion: String,
    buildId: String,
    generatePoms: Boolean,
    dockerDir: Directory,
    images: Set[DockerImage]
)

object MetadataSettings {

  def apply(
      stratoConfig: StratoConfig,
      extraLibs: Seq[DependencyDefinition],
      webDependencyResolver: WebDependencyResolver,
      installDir: Directory,
      dockerDir: Directory,
      installVersion: String,
      leafDir: RelativePath,
      buildId: String,
      generatePoms: Boolean,
      images: Set[DockerImage]): MetadataSettings = {
    val javaVersion =
      Seq(stratoConfig.config.getString("javaProject"), stratoConfig.config.getString("javaShortVersion")).mkString("-")
    val obtVersion = stratoConfig.config.getString("obt-version")
    val installPathBuilder = InstallPathBuilder.dev(installDir, installVersion)
    new MetadataSettings(
      installPathBuilder,
      extraLibs,
      webDependencyResolver,
      leafDir,
      installVersion,
      obtVersion,
      javaVersion,
      buildId,
      generatePoms,
      dockerDir,
      images
    )
  }
}
