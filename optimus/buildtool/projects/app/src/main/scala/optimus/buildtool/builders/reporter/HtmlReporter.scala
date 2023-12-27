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

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import optimus.buildtool.artifacts.CompilationMessage
import optimus.buildtool.artifacts.MessagesArtifact
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Asset
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.RelativePath

import scala.xml.Elem

object HtmlReporter {

  def writeAssetSummaryPage(dir: Directory, assets: Seq[Asset], title: String): FileAsset = {
    val htmlContent = generateAssetReport(assets.map(dir.relativize), title)
    writeHtmlfile(dir, htmlContent)
  }

  def writeErrorReport(dir: Directory, artifactsErrors: Seq[(MessagesArtifact, Seq[CompilationMessage])]): FileAsset = {
    val idErrorMsgs: Seq[(ScopeId, Seq[CompilationMessage])] = artifactsErrors.map { case (art, errs) =>
      (art.id.scopeId, errs)
    }
    val htmlContent = generateCompilationMessageReport(idErrorMsgs, "Compilation Error Summary")
    writeHtmlfile(dir, htmlContent)
  }

  def writeTrackerReport(dir: Directory, discrepancies: LookupReporter.Discrepancies): FileAsset = {
    val htmlContent = generateLookupTrackerReport(discrepancies, "Zinc Lookup Dependency Tracker")
    writeHtmlfile(dir, htmlContent, "tracker.html")
  }

  private def writeHtmlfile(dir: Directory, html: Elem, fileName: String = "index.html"): FileAsset = {
    val dest = dir.path.resolve(fileName)
    val content = html.toString.getBytes(StandardCharsets.UTF_8)
    Files.createDirectories(dest.getParent)
    FileAsset(Files.write(dest, content))
  }

  private def generateCompilationMessageReport(
      msgsToReport: Seq[(ScopeId, Seq[CompilationMessage])],
      title: String,
      filterPredicate: CompilationMessage => Boolean = _.msg.nonEmpty): Elem = generateReport(title) {
    val data = msgsToReport.map { case (id, msgs) =>
      <div>
      <h2>{id}</h2>
      <ul>
         {msgs.collect { case m if filterPredicate(m) => htmlReport(m) }}
      </ul>
    </div>
    }
    <div>{data} </div>
  }

  private def generateLookupTrackerReport(discrepancies: LookupReporter.Discrepancies, title: String): Elem = {
    def writeSection(
        heading: String,
        discrepancies: Seq[LookupDiscrepancy],
        kind: LookupDiscrepancy.Kind): Option[Elem] = {
      val result = discrepancies.sortBy(_.where).collect {
        case LookupDiscrepancy.Internal(id, `kind`) =>
          <li>{id.toString}</li>
        case LookupDiscrepancy.External(id, `kind`) =>
          <li>{id.toString}</li>
      }

      if (result.nonEmpty)
        Some(<p>
          <h4>{heading}</h4>
          <ul>{result}</ul></p>)
      else None
    }

    generateReport(title) {
      val description = <div>Detected discrepancies between OBT configuration and actual Zinc compilation.
        <p>
          <b>Required</b> dependencies are used in the compilation but not present in the OBT configuration. These are
          imported transitively. Consider explicitly declaring them in the OBT configuration.
        </p>
        <p>
          <b>Unused</b> dependencies were declared but not used in the code. Consider removing them from the OBT
          configuration to enable faster compilation.
        </p>
      </div>

      val data = discrepancies.map { case (scopeId, discrepancies) =>
        <div><h2>{scopeId}</h2>
          {writeSection("Unused", discrepancies, LookupDiscrepancy.Unused).getOrElse("")}
          {writeSection("Required", discrepancies, LookupDiscrepancy.Required).getOrElse("")}
        </div>
      }

      <div>{description} {data}</div>
    }
  }

  private def htmlReport(msg: CompilationMessage): Elem = {
    val pos = msg.pos.map(p => s"${p.filepath}:${p.startLine}")
    val label = pos.getOrElse(msg.msg)
    val text = s"${pos.fold("")(s => s"$s: ")}${msg.msg}"
    <li>
      <div>{label}</div>
      <div>
        <pre>{text}</pre>
      </div>
    </li>
  }

  private def generateAssetReport(assets: Seq[RelativePath], title: String): Elem = generateReport(title) {
    <div>
      <ul>
        {assets.map(_.pathString).sorted.map(asset => <li><a href={asset} target="_blank">{asset}</a></li>)}
      </ul>
    </div>
  }

  private def generateReport(title: String)(content: => Elem): Elem = {
    <html>
      <head>
        <title>{title}</title>
      </head>
      <body>
        <h1>{title}</h1>
        <div>
          {content}
        </div>
      </body>
    </html>
  }

}
