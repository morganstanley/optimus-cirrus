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
import optimus.buildtool.format.FingerprintsDiffConfig
import optimus.buildtool.format.ObtFile
import optimus.buildtool.format.Result
import optimus.buildtool.format.Success

import scala.jdk.CollectionConverters._

final case class FingerprintsDiffConfiguration(
    supportedExtensions: Set[String],
    unsupportedFiles: Set[String],
    ignoredDiffs: Set[FingerprintDiffId]
)

final case class FingerprintDiffId(category: String, id: String)

object FingerprintsDiffConfiguration {

  val Empty: FingerprintsDiffConfiguration = FingerprintsDiffConfiguration(Set.empty, Set.empty, Set.empty)

  private val origin = FingerprintsDiffConfig
  private val supportedExtensionsKey = "supportedExtensions"
  private val unsupportedFilesKey = "unsupportedFiles"
  private val ignoredDiffsKey = "ignoredDiffs"

  def load(loader: ObtFile.Loader): Result[FingerprintsDiffConfiguration] =
    loader(origin).flatMap(load)

  private def load(config: Config) = {
    def stringSet(key: String): Set[String] =
      if (config.hasPath(key)) config.getStringList(key).asScala.toSet
      else Set.empty

    def ignoredDiffs: Set[FingerprintDiffId] = {
      val ignoredDiffs: Set[Config] =
        if (config.hasPath(ignoredDiffsKey)) config.getObjectList(ignoredDiffsKey).asScala.map(_.toConfig).toSet
        else Set.empty
      ignoredDiffs.map(e => FingerprintDiffId(e.getString("category"), e.getString("id")))
    }

    Result.tryWith(origin, config) {
      Success(
        FingerprintsDiffConfiguration(stringSet(supportedExtensionsKey), stringSet(unsupportedFilesKey), ignoredDiffs))
    }
  }

}
