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
package optimus.buildtool.compilers

import optimus.buildtool.artifacts.ArtifactType
import optimus.buildtool.artifacts.CompilationMessage
import optimus.buildtool.artifacts.CompilerMessagesArtifact
import optimus.buildtool.artifacts.InternalArtifactId
import optimus.buildtool.artifacts.MessagePosition
import optimus.buildtool.config.CodeFlaggingRule
import optimus.buildtool.config.NamingConventions
import optimus.buildtool.config.Pattern
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.JsonAsset
import optimus.buildtool.files.SourceUnitId
import optimus.buildtool.trace.ObtTrace
import optimus.buildtool.trace.RegexCodeFlagging
import optimus.buildtool.utils.HashedContent
import optimus.platform._

import scala.annotation.tailrec
import scala.collection.immutable.Seq
import scala.collection.immutable.SortedMap

@entity class RegexScanner {
  import RegexScanner._

  @node def messages(scopeId: ScopeId, inputs: NodeFunction0[Inputs]): CompilerMessagesArtifact = {
    val artifactId = InternalArtifactId(scopeId, ArtifactType.RegexMessages, None)
    val jsonFile = inputs().jsonFile
    val messages = scan(scopeId, inputs)
    val a = CompilerMessagesArtifact.create(artifactId, jsonFile, messages, RegexCodeFlagging, incremental = false)
    a.storeJson()
    a
  }

  @node def scan(scopeId: ScopeId, inputs: NodeFunction0[ScanInputs]): Seq[CompilationMessage] = {
    val resolvedInputs = inputs()
    import resolvedInputs._
    val trace = ObtTrace.startTask(scopeId, RegexCodeFlagging)
    val t = NodeTry {
      val messages = sourceFiles.toIndexedSeq.apar
        .flatMap { case (id, content) =>
          analyzeSource(id, content, rules)
        }
      trace.publishMessages(messages)
      messages
    }
    trace.completeFromTry(t.toTry)
    t.get
  }

  // extracted so that unchanged files need not be re-analyzed
  // note that HashedContent equality is cheap because it uses the SHA-2 hash of the actual content
  @node private def analyzeSource(
      id: SourceUnitId,
      content: HashedContent,
      rules: Seq[CodeFlaggingRule]
  ): Seq[CompilationMessage] = {
    if (NamingConventions.isBinaryExtension(id.suffix)) Nil // no need to scan binary files!
    else {
      val applicableRules = this.applicableRules(id, rules)
      // TODO (OPTIMUS-42169): this probably could be made faster / less memory-intensive by running the regexen
      // over the entire file and then computing an index range -> line number mapping rather than splitting into lines
      scanLines(id, content.utf8ContentAsString.split('\n').zipWithIndex.toIndexedSeq, applicableRules)
    }
  }

  // likewise extracted/nodulated to avoid more regex matching every time
  @node private def applicableRules(id: SourceUnitId, rules: Seq[CodeFlaggingRule]): Seq[CodeFlaggingRule] =
    rules.filter(_ matchesFile id.localRootToFilePath.toString)

  private def scanLines(
      id: SourceUnitId,
      lines: Seq[(String, Int)],
      applicableRules: Seq[CodeFlaggingRule]
  ): Seq[CompilationMessage] = {

    @tailrec def scanNextLine(
        remainingLines: Seq[(String, Int)],
        currentOffset: Int,
        accum: Seq[CompilationMessage],
        ignoring: Boolean = false
    ): Seq[CompilationMessage] = remainingLines match {
      case (line, _) +: _ if line.contains(IgnoreFileToken) =>
        // skip the file
        Nil
      case (line, _) +: tail if line.contains(IgnoreEndToken) =>
        // ignore this line but start scanning again from the next one
        scanNextLine(tail, currentOffset + line.length + 1, accum, ignoring = false)
      case (line, _) +: tail if ignoring || line.contains(IgnoreStartToken) =>
        // ignore this line and following ones until we see an end token
        scanNextLine(tail, currentOffset + line.length + 1, accum, ignoring = true)
      case (line, _) +: (nextLine, _) +: tail if line.contains(IgnoreLineToken) =>
        // skip a line
        scanNextLine(tail, currentOffset + line.length + nextLine.length + 2, accum, ignoring = false)
      case (line, lineNumber) +: tail =>
        val lineMessages = applicableRules.flatMap { codingRule =>
          val (excludes, includes) = codingRule.regexes.partition(_.exclude)
          val anyExcludeMatches = excludes.exists(_.regex.findAllMatchIn(line).nonEmpty)
          if (!anyExcludeMatches)
            generateCompilationMessageForIncludes(includes, line, lineNumber, currentOffset, codingRule, id)
          else Nil
        }
        // +1 to include the stripped '\n'
        scanNextLine(tail, currentOffset + line.length + 1, accum ++ lineMessages, ignoring = false)

      case Seq() =>
        accum
    }

    scanNextLine(lines, 0, Nil, ignoring = false)
  }

  private def generateCompilationMessageForIncludes(
      includes: Seq[Pattern],
      line: String,
      lineNumber: Int,
      offset: Int,
      codingRule: CodeFlaggingRule,
      id: SourceUnitId
  ): Seq[CompilationMessage] = {
    includes.flatMap { includeRules =>
      val includeMatches = includeRules.regex.findAllMatchIn(line)
      includeMatches.map { m =>
        val messagePosition = MessagePosition(
          filepath = id.localRootToFilePath.pathString,
          startLine = lineNumber + 1,
          startColumn = m.start + 1,
          endLine = lineNumber + 1,
          endColumn = m.end + 1,
          startPoint = offset + m.start,
          endPoint = offset + m.end
        )
        CompilationMessage(
          Some(messagePosition),
          codingRule.description,
          codingRule.severityLevel,
          Some(codingRule.title),
          isSuppressed = false,
          isNew = codingRule.isNew)
      }
    }
  }

}

object RegexScanner {
  trait ScanInputs {
    def sourceFiles: SortedMap[SourceUnitId, HashedContent]
    def rules: Seq[CodeFlaggingRule]
  }

  object ScanInputs {
    private final case class ScanInputsImpl(
        sourceFiles: SortedMap[SourceUnitId, HashedContent],
        rules: Seq[CodeFlaggingRule]
    ) extends ScanInputs
    def apply(
        sourceFiles: SortedMap[SourceUnitId, HashedContent],
        rules: Seq[CodeFlaggingRule]
    ): ScanInputs = ScanInputsImpl(sourceFiles, rules)
  }

  final case class Inputs(
      sourceFiles: SortedMap[SourceUnitId, HashedContent],
      rules: Seq[CodeFlaggingRule],
      jsonFile: JsonAsset
  ) extends ScanInputs

  // Note: Prefix isn't just a code style thing here - we deliberately split that out so that regex scanning
  // doesn't see these tokens and ignore the file!
  private val prefix = "regex-ignore"
  val IgnoreFileToken = s"$prefix-file"
  val IgnoreStartToken = s"$prefix-start"
  val IgnoreEndToken = s"$prefix-end"
  val IgnoreLineToken = s"$prefix-line"
}
