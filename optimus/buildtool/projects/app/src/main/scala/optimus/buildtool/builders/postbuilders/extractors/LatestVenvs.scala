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

package optimus.buildtool.builders.postbuilders.extractors
import optimus.buildtool.artifacts.PythonArtifact
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.FileAsset
import optimus.buildtool.utils.AssetUtils
import optimus.platform.util.Log

import java.nio.file.Files
import java.nio.file.Path
import scala.jdk.CollectionConverters._

final case class LatestVenvs(
    mapping: Map[ScopeId, Path]
) {
  def update(artifact: PythonArtifact, venv: Path): LatestVenvs =
    copy(mapping = mapping + (artifact.scopeId -> venv))

  def writeToFile(file: FileAsset): Unit = {
    val mappingEntries = mapping.map { case (scope, path) =>
      s"$scope,$path"
    }

    AssetUtils.atomicallyWrite(file, replaceIfExists = true) { path =>
      Files.write(path, mappingEntries.mkString(System.lineSeparator).getBytes)
    }
  }
}

object LatestVenvs extends Log {
  def load(file: FileAsset): LatestVenvs = {
    val map = if (file.exists) {
      Files
        .readAllLines(file.path)
        .asScala
        .map { line =>
          line.split(",", 2) match {
            case Array(id, path) =>
              val scopeId = ScopeId.parse(id)
              scopeId -> Path.of(path)
          }
        }
        .toMap
    } else {
      log.warn(s"Mapping for the latest venvs ${file.path.toString} not found. Empty mapping will be used instead.")
      Map.empty[ScopeId, Path]
    }

    LatestVenvs(map)
  }

}
