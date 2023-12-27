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
package optimus.buildtool.format

import com.typesafe.config.Config
import optimus.buildtool.config.ElectronConfiguration
import optimus.buildtool.config.NamingConventions

import scala.collection.compat._
import scala.collection.immutable.Seq
import scala.jdk.CollectionConverters._

object ElectronConfigurationCompiler {
  import ConfigUtils._

  private val ElectronConfig = "electron"
  private val LibsKey = "libs"
  private val MavenKey = NamingConventions.MavenLibsKey
  private val NpmCommandTemplateKey = "npmCommandTemplate"
  private val NpmBuildCommandsKey = "npmBuildCommands"

  def load(config: Config, origin: ObtFile): Result[Option[ElectronConfiguration]] = {
    Result.tryWith(origin, config) {
      Result.optional(config.hasPath(ElectronConfig)) {
        val electron = config.getConfig(ElectronConfig)
        val electronLibs = electron.stringListOrEmpty(LibsKey) ++ electron.stringListOrEmpty(MavenKey)
        val npmCommandTemplate = electron.stringMapOrEmpty(NpmCommandTemplateKey, origin)
        npmCommandTemplate
          .map { t =>
            val npmBuildCommands = {
              if (electron.hasPath(NpmBuildCommandsKey))
                Some(electron.getStringList(NpmBuildCommandsKey).asScala.to(Seq))
              else None
            }
            ElectronConfiguration(t, electronLibs, npmBuildCommands)
          }
          .withProblems(electron.checkExtraProperties(origin, Keys.electronProperties))
      }
    }
  }
}
