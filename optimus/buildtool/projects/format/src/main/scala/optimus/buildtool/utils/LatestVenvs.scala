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

package optimus.buildtool.utils

import com.opencsv.CSVReader
import com.opencsv.CSVWriter
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.FileAsset

import java.nio.file.Files
import java.nio.file.Path
import org.slf4j.LoggerFactory.getLogger

import java.io.StringReader
import java.io.StringWriter
import java.nio.charset.StandardCharsets

final case class LatestVenv(venv: Path, tpa: Path)

final case class LatestVenvs(
    mapping: Map[ScopeId, LatestVenv]
) {

  def update(scopeId: ScopeId, tpa: Path, venv: Path): LatestVenvs =
    copy(mapping = mapping + (scopeId -> LatestVenv(venv.toAbsolutePath, tpa.toAbsolutePath)))

  def writeToFile(file: FileAsset): Unit = {
    val stringWriter = new StringWriter()
    val writer = new CSVWriter(stringWriter)
    try {
      mapping.foreach { case (scope, LatestVenv(venv, artifact)) =>
        writer.writeNext(Array(scope.toString, venv.toAbsolutePath.toString, artifact.toAbsolutePath.toString))
      }

      AssetUtils.atomicallyWrite(file, replaceIfExists = true) { path =>
        Files.createDirectories(path.getParent)
        Files.write(path, stringWriter.toString.getBytes)
      }

    } finally {
      writer.close()
    }
  }
}

object LatestVenvs {
  private val log = getLogger(this.getClass)

  def defaultLatestVenvFile(buildDir: Path): Path =
    buildDir.resolve("venvs").resolve("latest-venvs.txt")

  def loadFromDefaultFile(buildDir: Path): LatestVenvs = loadFromFile(defaultLatestVenvFile(buildDir))

  def loadFromFile(file: Path): LatestVenvs = {
    if (Files.exists(file)) {
      parse(new String(Files.readAllBytes(file), StandardCharsets.UTF_8))
    } else {
      log.warn(s"Mapping for the latest venvs ${file.toString} not found. Empty mapping will be used instead.")
      LatestVenvs(Map.empty[ScopeId, LatestVenv])
    }
  }

  def parse(content: String): LatestVenvs = {
    val reader = new CSVReader(new StringReader(content))
    try {
      val map =
        Iterator
          .continually(reader.readNext())
          .takeWhile(_ != null)
          .flatMap {
            case Array(id, venv, artifact) =>
              val scopeId = ScopeId.parse(id)
              Some(scopeId -> LatestVenv(Path.of(venv), Path.of(artifact)))
            case line =>
              log.warn(s"Skipping wrong latestVenv mapping: $line")
              None
          }
          .toMap
      LatestVenvs(map)
    } finally {
      reader.close()
    }
  }
}
