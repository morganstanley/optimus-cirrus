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
package optimus.buildtool.builders.reporter

import msjava.slf4jutils.scalalog.Logger
import msjava.slf4jutils.scalalog.getLogger
import optimus.buildtool.builders.BuildResult.CompletedBuildResult
import optimus.buildtool.builders.postbuilders.codereview.CodeReviewSettings
import optimus.buildtool.builders.postbuilders.metadata.MetadataSettings
import optimus.buildtool.builders.postinstallers.uploaders.ArchiveUtil
import optimus.buildtool.builders.postinstallers.uploaders.ArchiveUtil.ArchiveEntry
import optimus.buildtool.config.ObtConfig
import optimus.buildtool.files.Directory
import optimus.platform._

import java.nio.file.Files
import scala.collection.immutable.Seq

class MessageReporter(
    obtConfig: ObtConfig,
    errorsDir: Option[Directory],
    warningsDir: Option[Directory],
    lookupsDir: Option[Directory],
    codeReviewSettings: Option[CodeReviewSettings],
    metadataSettings: Option[MetadataSettings]
) {

  protected val log: Logger = getLogger(this)
  private val csvReporter = new CsvReporter(obtConfig)
  private val jsonReporter = new JsonReporter(obtConfig, codeReviewSettings, metadataSettings)
  private val lookupReporter = new LookupReporter(obtConfig)

  @async def writeReports(buildResult: CompletedBuildResult): Unit =
    apar(
      writeErrorReport(buildResult),
      writeLookupReport(buildResult),
      writeOptimusWarningReports(buildResult),
      writeCodeReviewAnalysis(buildResult),
      jsonReporter.writeMetadataReports(buildResult)
    )

  @async private def writeLookupReport(buildResult: CompletedBuildResult): Unit = {
    lookupsDir.foreach { dir =>
      lookupReporter.discrepancies(buildResult.messageArtifacts) match {
        case Some(discrepancies) =>
          if (discrepancies.nonEmpty) {
            val file = HtmlReporter.writeTrackerReport(dir, discrepancies)
            log.info(s"Unused dependencies report generated - see ${file.pathString}")
          } else {
            log.info("No unused dependencies to report.")
          }
        case None =>
          log.warn(
            "Zinc lookup tracker report was requested but no lookups were made. "
              + "Is it possible that no scopes were compiled at all?")
      }
    }
  }

  @async private def writeErrorReport(buildResult: CompletedBuildResult): Unit = errorsDir.foreach { dir =>
    val artifactsErrors = buildResult.errorsByArtifact
    if (artifactsErrors.nonEmpty) {
      val file = HtmlReporter.writeErrorReport(dir, artifactsErrors)
      log.info(s"Compilation errors report generated - see ${file.pathString}")
    }
  }

  @async private def writeOptimusWarningReports(buildResult: CompletedBuildResult): Unit = warningsDir.foreach { dir =>
    val msgs = buildResult.messageArtifacts
    val filesToReport = csvReporter.writeOptimusWarningReports(dir, msgs) ++ csvReporter.writeAlertReport(dir, msgs)
    if (filesToReport.nonEmpty) {
      val zipAsset = dir.resolveFile("warnings-report.zip")
      val entries = filesToReport.toCompress.map(f => ArchiveEntry(f, dir.relativize(f)))
      ArchiveUtil.populateZip(zipAsset, entries)
      entries.foreach(f => Files.delete(f.file.path))
      val files = Seq(zipAsset) ++ filesToReport.notToCompress
      val summary = HtmlReporter.writeAssetSummaryPage(dir, files, "Optimus Warnings Report")
      log.info(s"${files.size} Optimus warnings reports generated - see ${summary.pathString}")
    }
  }

  @async private def writeCodeReviewAnalysis(buildResult: CompletedBuildResult): Unit = {
    val msgs = buildResult.messageArtifacts
    jsonReporter.writeAnalysis(msgs, buildResult.modifiedFiles).foreach { analysis =>
      log.info(s"Code review compiler report generated - see ${analysis.pathString}")
    }
  }
}
