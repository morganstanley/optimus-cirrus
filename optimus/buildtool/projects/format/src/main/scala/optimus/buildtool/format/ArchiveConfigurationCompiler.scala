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
import com.typesafe.config.ConfigException.BadValue
import optimus.buildtool.config.ArchiveConfiguration
import optimus.buildtool.config.ArchiveType
import optimus.buildtool.dependencies.JvmDependenciesLoader

object ArchiveConfigurationCompiler {
  import ConfigUtils._

  private val archiveConfig = "archive"

  def load(config: Config, origin: ObtFile): Result[Option[ArchiveConfiguration]] =
    Result
      .tryWith(origin, config) {
        Result.optional(config.hasPath(archiveConfig)) {
          val archive = config.getConfig(archiveConfig)
          val cfg = for {
            tokens <- archive.stringMapOrEmpty("tokens", origin)
            excludes <- JvmDependenciesLoader.readExcludes(archive, origin)
          } yield {
            val tpeString = archive.getString("type")
            val tpe = ArchiveType
              .parse(tpeString)
              .getOrElse(
                throw new BadValue(archive.getValue("type").origin(), "type", tpeString)
              )

            ArchiveConfiguration(tpe, tokens, excludes.toSet)
          }
          cfg
            .withProblems(archive.checkExtraProperties(origin, Keys.archiveProperties))
        }
      }

}
