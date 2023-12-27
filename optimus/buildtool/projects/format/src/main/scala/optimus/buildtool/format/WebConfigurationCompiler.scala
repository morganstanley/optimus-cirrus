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
import optimus.buildtool.config.NamingConventions
import optimus.buildtool.config.NpmConfiguration.NpmBuildMode._
import optimus.buildtool.config.WebConfiguration

import scala.collection.compat._
import scala.collection.immutable.Seq
import scala.jdk.CollectionConverters._

object WebConfigurationCompiler {
  import ConfigUtils._

  private val WebConfig = "web"
  private val ModeKey = "mode"
  private val LibsKey = "libs"
  private val MavenKey = NamingConventions.MavenLibsKey
  private val NpmCommandTemplateKey = "npmCommandTemplate"
  private val NpmBuildCommandsKey = "npmBuildCommands"

  def load(config: Config, origin: ObtFile): Result[Option[WebConfiguration]] =
    Result.tryWith(origin, config) {
      Result.optional(config.hasPath(WebConfig)) {
        val web = config.getConfig(WebConfig)
        web.getString(ModeKey) match {
          case "prod" =>
            val npmCommandTemplate = web.stringMapOrEmpty(NpmCommandTemplateKey, origin)
            val webLibs =
              web.stringListOrEmpty(LibsKey) ++ web.stringListOrEmpty(MavenKey)
            npmCommandTemplate
              .map { t =>
                val npmBuildCommands =
                  if (web.hasPath(NpmBuildCommandsKey)) Some(web.getStringList(NpmBuildCommandsKey).asScala.to(Seq))
                  else None
                WebConfiguration(Production, webLibs, t, npmBuildCommands)
              }
              .withProblems(web.checkExtraProperties(origin, Keys.webProperties))

          case "test" =>
            if (web.hasPath(NpmBuildCommandsKey))
              origin.failure(web.getValue(NpmBuildCommandsKey), "test mode doesn't any run npm build commands")
            else
              Success(WebConfiguration(TestingResource, Seq.empty, Map.empty, None))
                .withProblems(web.checkExtraProperties(origin, Keys.webProperties))

          case "dev" =>
            Success(WebConfiguration(Development, Seq.empty, Map.empty, None))
              .withProblems(web.checkExtraProperties(origin, Keys.webProperties))

          case other =>
            origin.failure(web.getValue(ModeKey), s"Invalid mode: $other, mode must be one of [prod, test, dev]")
        }
      }
    }
}
