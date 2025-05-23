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
import optimus.buildtool.format.ConfigUtils.ConfOps
import optimus.buildtool.format.ObtFile
import optimus.buildtool.format.Result
import optimus.buildtool.format.Success

final case class DockerDefaults(registry: Option[String], baseImage: Option[String], imageSysName: Option[String])

object DockerDefaults {

  val Empty: DockerDefaults = DockerDefaults(registry = None, baseImage = None, imageSysName = None)

  def load(origin: ObtFile, config: Config, key: String): Result[DockerDefaults] =
    if (config.hasPath(key)) {
      val updatedConfig = config.getConfig(key)
      Result
        .tryWith(origin, updatedConfig) {
          Success(
            DockerDefaults(
              updatedConfig.optionalString("registry"),
              updatedConfig.optionalString("baseImage"),
              updatedConfig.optionalString("imageSysName")))
        }
        .withProblems(updatedConfig.checkExtraProperties(origin, Keys.dockerDefaults))
    } else Success(DockerDefaults.Empty)
}
