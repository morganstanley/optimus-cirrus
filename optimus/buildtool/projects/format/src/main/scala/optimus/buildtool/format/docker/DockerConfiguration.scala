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
package optimus.buildtool.format.docker

import com.typesafe.config.Config
import optimus.buildtool.format.Keys
import optimus.buildtool.format.ObtFile
import optimus.buildtool.format.Result
import optimus.buildtool.format.ConfigUtils._
import optimus.buildtool.format.Success

import java.nio.file.Path
import scala.collection.immutable.Seq

final case class DockerConfiguration(defaults: DockerDefaults, extraDependencies: Set[Path], excludes: Seq[String])

object DockerConfiguration {

  val Empty: DockerConfiguration =
    DockerConfiguration(defaults = DockerDefaults.Empty, extraDependencies = Set.empty, excludes = Seq.empty)

  def load(origin: ObtFile, config: Config, key: String): Result[DockerConfiguration] = {
    if (config.hasPath(key)) {
      val updatedConfig = config.getConfig(key)
      Result
        .tryWith(origin, updatedConfig) {
          for {
            extraDeps <- loadExtraDependencies(origin, updatedConfig, "extraDependencies")
            defaults <- DockerDefaults.load(origin, updatedConfig, "defaults")
          } yield DockerConfiguration(
            defaults = defaults,
            extraDependencies = extraDeps,
            excludes = updatedConfig.stringListOrEmpty("excludes")
          )
        }
        .withProblems(updatedConfig.checkExtraProperties(origin, Keys.dockerConfiguration))
    } else Success(DockerConfiguration.Empty)
  }

  private def loadExtraDependencies(origin: ObtFile, config: Config, key: String): Result[Set[Path]] =
    if (config.hasPath(key)) {
      Result
        .sequence {
          config.values(key).map { configValue =>
            config.absolutePath(configValue, origin)
          }
        }
        .map(_.toSet)
    } else Success(Set.empty)
}
