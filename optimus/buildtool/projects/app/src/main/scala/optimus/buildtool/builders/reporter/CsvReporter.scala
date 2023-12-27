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

import java.nio.file.Files
import optimus.buildtool.artifacts.CompilationMessage
import optimus.buildtool.artifacts.MessagesArtifact
import optimus.buildtool.builders.reporter.codeanalysis.AlertCount
import optimus.buildtool.builders.reporter.codeanalysis.AlertCountAnalysis
import optimus.buildtool.builders.reporter.codeanalysis.AlertLoc
import optimus.buildtool.config.ModuleId
import optimus.buildtool.config.ObtConfig
import optimus.buildtool.config.ScopeId.RootScopeId
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.Pathed
import optimus.buildtool.utils.SimpleCsvWriter
import optimus.platform._

import scala.collection.immutable.Seq
import org.apache.commons.io.FilenameUtils
import org.apache.commons.lang3.StringUtils

class CsvReporter(obtConfig: ObtConfig) {

  @async def writeAlertReport(dir: Directory, msgs: Seq[MessagesArtifact]): FilesToReport = {
    val scopedWarnings = toScopedMessages(msgs, CsvReporter.predicateFnOr).filter(_.msg.isWarning)
    val allWarnings = writeCsvfile(dir, scopedWarnings, "all-warnings.csv")(CompilationMessageCsvWriter)
    FilesToReport(toCompress = allWarnings.toIndexedSeq)
  }

  @node private def toScopedMessages(
      msgs: Seq[MessagesArtifact],
      filterPredicate: CompilationMessage => Boolean): Seq[ScopedCompilationMessage] =
    msgs.apar.flatMap { messageArtifact =>
      val scopeId = messageArtifact.id.scopeId
      val owner = if (scopeId == RootScopeId) None else Some(obtConfig.owner(scopeId))
      messageArtifact.messages.collect {
        case m if filterPredicate(m) =>
          ScopedCompilationMessage(owner, scopeId, m)
      }
    }

  @async def writeOptimusWarningReports(dir: Directory, msgs: Seq[MessagesArtifact]): FilesToReport = {
    val alertCountAnalysis = AlertCountAnalysis(msgs, CsvReporter.predicateFnAnd)
    val alertCountsPerModule = alertCountAnalysis.generateModuleReports
    val unusedIgnoredReport = writeUnusedIgnoredOptimusWarningReport(dir, alertCountsPerModule).toIndexedSeq
    // the unusedIgnoredReport file is very useful, so we are going to put it outside the tar
    // this is usually very small, but extremely useful for the graph team to remove the warnings ignore safely
    FilesToReport(
      toCompress = Nil,
      notToCompress = unusedIgnoredReport
    )
  }

  @async private def writeUnusedIgnoredOptimusWarningReport(
      dir: Directory,
      alertCountsPerModule: Map[ModuleId, Seq[AlertCount]]): Option[FileAsset] = {
    // no need to differentiate between different types of scopes
    val ignoredUnusedAlertsPerModule =
      obtConfig.compilationScopeIds.groupBy(ModuleId.apply).apar.flatMap { case (module, scopeIds) =>
        val ignoredOptimusAlerts = scopeIds.apar.flatMap(id => obtConfig.ignoredOptimusAlerts(id))
        val alertCounts = alertCountsPerModule.getOrElse(module, Seq.empty).toSet
        val unusedIgnored = ignoredOptimusAlerts.diff(alertCounts.flatMap(_.alarmId))
        if (unusedIgnored.nonEmpty) Some(module -> unusedIgnored) else None
      }
    writeCsvfile(dir, ignoredUnusedAlertsPerModule, "ignored-unused-warnings.csv")(UnusedIgnoredAlertsCsvWriter)
  }

  @async private def writeCsvfile[T](dir: Directory, entities: Iterable[T], fileName: String)(
      writer: SimpleCsvWriter[T]): Option[FileAsset] =
    if (entities.isEmpty) None
    else
      Some {
        val fileAsset = dir.resolveFile(fileName)
        Files.createDirectories(fileAsset.parent.path)
        writer.writeCsvFile(fileAsset.path.toFile, entities)
        fileAsset
      }
}

object CsvReporter {
  val predicateFnAnd: CompilationMessage => Boolean = m => m.isWarning && m.alarmId.isDefined
  val predicateFnOr: CompilationMessage => Boolean = m => m.isWarning || m.alarmId.isDefined

  def toStackTraceFormat(alertLoc: AlertLoc): String = {
    val name = Pathed.name(alertLoc.loc.filepath)
    s"${alertLoc.module}.${FilenameUtils.removeExtension(name)}.($name:${alertLoc.loc.startLine})"
  }
}

private[this] final case class ScopedCompilationMessage(
    owner: Option[String],
    scopeId: ScopeId,
    msg: CompilationMessage)

private[this] object CompilationMessageCsvWriter extends SimpleCsvWriter[ScopedCompilationMessage] {
  import optimus.utils.WarningsReportHeaders._
  override protected def defaultFilename: String = "all-alerts.csv"

  override protected def fieldExtractors: Seq[(String, ScopedCompilationMessage => Any)] =
    Seq(
      SEVERITY_HEADER -> { m =>
        if (m.msg.isSuppressed) "IGNORED" else m.msg.severity
      },
      ID_HEADER -> { _.msg.alarmId },
      SOURCE_PATH_HEADER -> { _.msg.pos.map(_.filepath) },
      "Meta" -> { _.scopeId.meta },
      "Project" -> { _.scopeId.bundle },
      "Module" -> { _.scopeId.module },
      "Scope" -> { _.scopeId.tpe },
      "Filename" -> { _.msg.pos.map(p => Pathed.name(p.filepath)) },
      "Line" -> { _.msg.pos.map(_.startLine) },
      "Column" -> { _.msg.pos.map(_.startColumn) },
      "Message" -> { m => StringUtils.abbreviate(m.msg.msg, "...", 500) },
      "Clickable Reference" -> { _.msg.pos.map(p => s".(${Pathed.name(p.filepath)}:${p.startLine})") },
      SOURCE_COL_HEADER -> { _.msg.pos.map(p => FilenameUtils.getExtension(p.filepath)) },
      "Owner" -> { _.owner }
    )
}

private[this] object AlertCountCsvWriter extends SimpleCsvWriter[AlertCount] {
  override protected def defaultFilename: String = "all-alerts-count.csv"

  override protected def fieldExtractors: Seq[(String, AlertCount => Any)] =
    Seq("ID" -> { _.alarmId }, "Count" -> { _.count })
}

private[this] object StyleAlertLocCsvWriter extends SimpleCsvWriter[AlertLoc] {
  override protected def defaultFilename: String = "all-alerts-style-locs.csv"

  override protected def fieldExtractors: Seq[(String, AlertLoc => Any)] =
    Seq(
      "ID" -> { _.alarmId },
      "Location" -> { alertLoc =>
        CsvReporter.toStackTraceFormat(alertLoc)
      })
}

private[this] object UnusedIgnoredAlertsCsvWriter extends SimpleCsvWriter[(ModuleId, Set[String])] {
  override protected def defaultFilename: String = "all-ignored-unused-alerts.csv"

  override protected def fieldExtractors: Seq[(String, ((ModuleId, Set[String])) => Any)] =
    Seq(
      "Module" -> { case (module, _) => module },
      "Unused but Ignored" -> { case (_, alertIds) =>
        alertIds.mkString(";")
      })
}
