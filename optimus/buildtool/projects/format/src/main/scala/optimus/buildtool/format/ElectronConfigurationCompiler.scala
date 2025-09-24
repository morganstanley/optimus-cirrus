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
import optimus.buildtool.config.NamingConventions.LibsKey
import optimus.buildtool.config.NpmConfiguration._
import optimus.buildtool.config.NpmConfiguration.NpmBuildMode._

import scala.collection.compat._
import scala.jdk.CollectionConverters._

object ElectronConfigurationCompiler {
  import ConfigUtils._

  private val ElectronConfig = "electron"
  private val ExecutablesKey = "executables"

  def load(config: Config, origin: ObtFile): Result[Option[ElectronConfiguration]] =
    Result.tryWith(origin, config) {
      Result.optional(config.hasPath(ElectronConfig)) {
        val web = config.getConfig(ElectronConfig)
        val executables = web.stringListOrEmpty(ExecutablesKey)
        web.getString(ModeKey) match {
          case Production.name =>
            val npmCommandTemplate = web.stringMapOrEmpty(NpmCommandTemplateKey, origin)
            val libs = web.stringListOrEmpty(LibsKey) ++ web.stringListOrEmpty(MavenKey)
            npmCommandTemplate
              .map { t =>
                val npmBuildCommands =
                  if (web.hasPath(NpmBuildCommandsKey)) Some(web.getStringList(NpmBuildCommandsKey).asScala.to(Seq))
                  else None
                ElectronConfiguration(Production, executables, libs, t, npmBuildCommands)
              }
              .withProblems(web.checkExtraProperties(origin, Keys.electronProperties))

          case Development.name =>
            Success(ElectronConfiguration(Development, executables, Seq.empty, Map.empty, None))
              .withProblems(web.checkExtraProperties(origin, Keys.electronProperties))

          case other =>
            origin.failure(web.getValue(ModeKey), s"Invalid mode: $other, mode must be one of [prod, test, dev]")
        }
      }
    }
}
