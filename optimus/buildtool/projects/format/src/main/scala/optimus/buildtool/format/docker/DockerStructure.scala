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
import optimus.buildtool.format.DockerConfig
import optimus.buildtool.format.ObtFile
import optimus.buildtool.format.Result

final case class DockerStructure(configuration: DockerConfiguration, images: Set[ImageDefinition])

object DockerStructure {

  val Empty = DockerStructure(configuration = DockerConfiguration.Empty, images = Set.empty)

  private val origin = DockerConfig
  private val images = "images"
  private val configuration = "configuration"

  def load(loader: ObtFile.Loader): Result[DockerStructure] =
    loader(origin).flatMap(load)

  def load(config: Config): Result[DockerStructure] = for {
    dockerConfig <- DockerConfiguration.load(origin, config, configuration)
    imageDefs <- ImageDefinition.load(origin, config, images)
  } yield DockerStructure(
    dockerConfig,
    imageDefs
  )

}
