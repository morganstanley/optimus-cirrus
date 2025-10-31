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
package optimus.buildtool.config

import com.typesafe.config.Config
import optimus.buildtool.files.Directory
import optimus.buildtool.files.RelativePath
import optimus.buildtool.format.ObtFile
import optimus.buildtool.format.Result
import optimus.buildtool.format.RuntimeDependencyCache
import optimus.buildtool.format.Success
import optimus.buildtool.format.ConfigUtils._

import java.nio.file.Paths
import scala.jdk.CollectionConverters._

final case class RuntimeDependencyCacheConfiguration(
    cacheRootPath: Option[Directory],
    cachePrefix: Option[String],
    baseArtifactPath: Option[RelativePath],
    releaseRevisions: Map[String, Char]
)

object RuntimeDependencyCacheConfiguration {

  val Empty = RuntimeDependencyCacheConfiguration(None, None, None, Map.empty)

  private val origin = RuntimeDependencyCache

  private val cacheRootPathKey = "runtime-dependency-cache-path"
  private val cachePrefixKey = "cache-prefix"
  private val baseArtifactPathKey = "base-artifact-path"
  private val releaseRevisionsKey = "cached-releases-revisions"

  private val releaseRevisionPathKey = "path"
  private val releaseRevisionKey = "release-revision"

  def load(loader: ObtFile.Loader): Result[RuntimeDependencyCacheConfiguration] =
    loader(origin).flatMap(load)

  private def isHexadecimal(char: Char): Boolean =
    char >= '0' && char <= '9' || (char >= 'A' || char >= 'a') && (char <= 'F' || char <= 'f')

  def load(config: Config): Result[RuntimeDependencyCacheConfiguration] =
    Result.tryWith(origin, config) {

      val cacheRootPath: Option[Directory] =
        config.optionalString(cacheRootPathKey).map(path => Directory(Paths.get(path)))

      val cachePrefix: Option[String] = config.optionalString(cachePrefixKey)

      val baseArtifactPath: Option[RelativePath] =
        config.optionalString(baseArtifactPathKey).map(RelativePath.apply)

      val releaseOverrides: Set[(String, Char)] =
        if (config.hasPath(releaseRevisionsKey)) {
          config
            .getConfigList(releaseRevisionsKey)
            .asScala
            .map { releaseOverride =>
              val depKey = releaseOverride.getString(releaseRevisionPathKey)
              val depVersionOverride = releaseOverride.getString(releaseRevisionKey)
              assert(
                depVersionOverride.length == 1 && isHexadecimal(depVersionOverride.head),
                s"Incorrect version override for $depKey")
              depKey -> depVersionOverride.head
            }
            .toSet
        } else Set.empty
      Success(RuntimeDependencyCacheConfiguration(cacheRootPath, cachePrefix, baseArtifactPath, releaseOverrides.toMap))
    }

}
