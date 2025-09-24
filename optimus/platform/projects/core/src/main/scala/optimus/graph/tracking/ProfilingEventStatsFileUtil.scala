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
package optimus.graph.tracking

import optimus.breadcrumbs.crumbs.ProfiledEventCause
import optimus.graph.diagnostics.JsonMapper
import spray.json.JsNumber
import spray.json.JsString
import spray.json.JsValue

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardOpenOption

object ProfilingEventStatsFileUtil {

  private val profStatsBreadcrumbsDumpFileName = "ProfilingStatsBreadcrumbs.txt"
  private val profSingleEventBreadcrumbsDumpFileName = "ProfilingSingleEventBreadcrumbs.txt"
  private val testOutputDir: String = System.getProperty("optimus.ui.test.outputDir", "")
  private def isTest: Boolean = testOutputDir.nonEmpty && Files.exists(Paths.get(testOutputDir))
  private val breadcrumbsDir: Option[Path] =
    if (isTest) Some(Paths.get(testOutputDir, "breadcrumbs")) else None

  private[optimus] def profStatsBreadcrumbsDumpFilePath: Path =
    breadcrumbsDir.get.resolve(profStatsBreadcrumbsDumpFileName)
  private[optimus] def profSingleEventBreadcrumbsDumpFilePath: Path =
    breadcrumbsDir.get.resolve(profSingleEventBreadcrumbsDumpFileName)

  breadcrumbsDir.map { dir =>
    if (Files.notExists(dir)) Files.createDirectory(dir)
    if (Files.notExists(profStatsBreadcrumbsDumpFilePath)) {
      Files.createFile(profStatsBreadcrumbsDumpFilePath)
    }
    if (Files.notExists(profSingleEventBreadcrumbsDumpFilePath)) {
      Files.createFile(profSingleEventBreadcrumbsDumpFilePath)
    }
  }

  private def convertJsValue(value: JsValue): Any = {
    value match {
      case JsNumber(n) => n.doubleValue
      case JsString(n) => n
      case _           => value
    }
  }

  def dumpStatsBreadcrumbsData(
      fileContent: Map[String, JsValue],
      durationStats: Map[String, JsValue],
      uiWorkerStats: Map[String, JsValue],
      metaData: Seq[Map[String, JsValue]]): Path = {
    if (breadcrumbsDir.isDefined) {
      val convertedMap: Map[String, Any] = fileContent.map { case (key, value) =>
        if (key == "durationStatistics") key -> durationStats.map { case (key, value) => key -> convertJsValue(value) }
        else if (key == "uiWorkerStats") key -> uiWorkerStats.map { case (key, value) => key -> convertJsValue(value) }
        else if (key == "metaData") key -> metaData.map { _.map { case (key, value) => key -> convertJsValue(value) } }
        else key -> convertJsValue(value)
      }
      Files.write(
        profStatsBreadcrumbsDumpFilePath,
        (JsonMapper.mapper.writeValueAsString(convertedMap) + System.lineSeparator()).getBytes,
        StandardOpenOption.APPEND
      )
    } else {
      throw new IllegalStateException("Dump Breadcrumbs only supported in test")
    }
  }

  def dumpSingleEventData(profiledEvent: ProfiledEventCause): Unit = {
    if (breadcrumbsDir.isDefined) {
      Files.write(
        profSingleEventBreadcrumbsDumpFilePath,
        (JsonMapper.mapper.writeValueAsString(profiledEvent) + System.lineSeparator()).getBytes,
        StandardOpenOption.APPEND
      )
    } else {
      throw new IllegalStateException("Dump Breadcrumbs only supported in test")
    }

  }
}
