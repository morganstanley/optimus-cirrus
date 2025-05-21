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
import optimus.buildtool.artifacts.CompilationMessage.Severity
import optimus.buildtool.artifacts.CompilerMessagesArtifact
import optimus.buildtool.artifacts.InternalArtifactId
import optimus.buildtool.artifacts.MessagePosition
import optimus.buildtool.config.CodeFlaggingRule
import optimus.buildtool.config.Group
import optimus.buildtool.config.NamingConventions
import optimus.buildtool.config.Pattern
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.JsonAsset
import optimus.buildtool.files.SourceUnitId
import optimus.buildtool.trace.ObtTrace
import optimus.buildtool.trace.RegexCodeFlagging
import optimus.buildtool.utils.HashedContent
import optimus.graph.DiagnosticSettings.getBoolProperty
import optimus.platform._

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.LongAdder
import java.util.function.Function
import scala.collection.immutable.Seq
import scala.collection.immutable.SortedMap
import scala.collection.mutable

@entity object RegexScanner {
  import optimus.buildtool.cache.NodeCaching.reallyBigCache
  // This is the node through which scanning is initiated. It's very important that we don't lose these from
  // cache (while they are still running at least) because that can result in rescanning of the same scope
  // due to a (potentially large) race between checking if the output artifacts are on disk and actually writing
  // them there after scanning completes.
  messages_info.setCustomCache(reallyBigCache)
  val timingEnabled: Boolean = getBoolProperty("optimus.regex.timing", true)
  val ruleTimings = new ConcurrentHashMap[String, LongAdder]
  private val makeAdder: Function[String, LongAdder] = (_: String) => new LongAdder()

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
          analyzeSource(id, content, rules, scopeId)
        }
      trace.publishMessages(messages)
      messages
    }
    trace.completeFromTry(t.toTry)
    t.get
  }

  // extracted so that unchanged files need not be re-analyzed
  // note that HashedContent equality is cheap because it uses the SHA-2 hash of the actual content
  @node private[compilers] def analyzeSource(
      id: SourceUnitId,
      content: HashedContent,
      rules: Seq[CodeFlaggingRule],
      scopeId: ScopeId): Seq[CompilationMessage] = {
    if (NamingConventions.isBinaryExtension(id.suffix)) Nil // no need to scan binary files!
    else {
      val applicableRules = RegexScanner.applicableRules(id, rules, scopeId)
      // TODO (OPTIMUS-42169): this probably could be made faster / less memory-intensive by running the regexen
      // over the entire file and then computing an index range -> line number mapping rather than splitting into lines
      val fileContent = content.utf8ContentAsString
      val relevantRules = getRelevantRules(fileContent, applicableRules)
      if (relevantRules.nonEmpty) {
        scanLines(id, fileContent, relevantRules)
      } else Nil
    }
  }

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
  private val IgnoreFileToken = s"$prefix-file"
  private val IgnoreStartToken = s"$prefix-start"
  private val IgnoreEndToken = s"$prefix-end"
  private val IgnoreLineToken = s"$prefix-line"
  private val RuleToken = ":"

  @node private def matchesGroup(group: Group, fileName: String, scopeId: ScopeId, all: Boolean): Boolean =
    if (group.filePaths.nonEmpty) {
      if (all) group.filePathRegexes.forall(_.findFirstIn(fileName).isDefined)
      else
        group.filePathRegexes.exists(_.findFirstIn(fileName).isDefined)
    } else {
      group.inScopes.contains(scopeId)
    }

  @node private def applicableRules(
      id: SourceUnitId,
      rules: Seq[CodeFlaggingRule],
      scopeId: ScopeId): Seq[CodeFlaggingRule] = {
    val fileName = id.localRootToFilePath.toString

    rules.apar.filter(rule =>
      rule.filter match {
        case Some(filter) =>
          val all = filter.all
          val any = filter.any
          val excl = filter.exclude

          all.apar.forall(matchesGroup(_, fileName, scopeId, all = true)) &&
          (any.isEmpty || any.apar.exists(matchesGroup(_, fileName, scopeId, all = false))) &&
          !excl.apar.exists(matchesGroup(_, fileName, scopeId, all = false))

        case None => rule matchesFile fileName
      })
  }

  @node private def getRelevantRules(content: String, rules: Seq[CodeFlaggingRule]): Seq[CodeFlaggingRule] = {
    val (excludeOnly, toCheck) = rules.partition(_.includes.isEmpty)
    // specifically stop when we find one rule that matches, the biggest benefit from pre-scanning is not having to scan by line if there are no matching rules
    toCheck.dropWhile(rule => {
      val startTime = if (timingEnabled) System.nanoTime() else 0L
      val notRelevant = !rule.includes.exists(p => p.regex.findFirstMatchIn(content).isDefined)
      val endTime = if (timingEnabled) System.nanoTime() else 0L
      if (timingEnabled) ruleTimings.computeIfAbsent(rule.key, makeAdder).add(endTime - startTime)
      notRelevant
    }) ++ excludeOnly
  }

  // function to read in commented lines while not including them in the line count
  private def upToLineCapture(lines: Iterator[String], n: Int): String = {
    var counter = 0
    val builder = new StringBuilder

    while (counter != n && lines.hasNext) {
      val line = lines.next()
      val strippedLine = line.strip()

      if (!strippedLine.matches("""^(//|/\*|\*|#).*""")) {
        counter += 1
      }

      builder.append(line).append("\n")
    }
    builder.toString()
  }

  private def scanLines(
      id: SourceUnitId,
      content: String,
      applicableRules: Seq[CodeFlaggingRule]
  ): Seq[CompilationMessage] = applicableRules
    .groupBy(_.upToLine)
    .flatMap {
      case (None, rules) =>
        scanLines(id, content.linesIterator, rules)
      case (Some(n), rules) =>
        val collapsedLines = upToLineCapture(content.linesIterator, n)
        scanLines(id, Iterator(collapsedLines), rules)
    }
    .toIndexedSeq

  /** Retrieves rule keys separated by commas, eg: "regex-ignore*: todo-comments" -> Seq("todo-comments") */
  private def getRuleKeys(line: String, ruleToken: String): Seq[String] = {
    val regexIgnoreIndex = line.indexOf(ruleToken)
    val ruleKeyIndex = line.indexOf(":", regexIgnoreIndex)
    if (ruleKeyIndex >= 0) line.substring(ruleKeyIndex + 1).split(",").map(_.trim).toIndexedSeq
    else Seq.empty
  }

  /** Checks if the rule keys specified in the regex-ignore blocks are valid or already ignored by a previous ignore block */
  private def checkInvalidRules(
      lineNumber: Int,
      currentOffset: Int,
      id: SourceUnitId,
      ruleKeys: Seq[String],
      currApplicableRules: Seq[CodeFlaggingRule]): Seq[CompilationMessage] = {
    // Any rules that are not currently applicable are either invalid or already being ignored
    val invalidRuleKeys = ruleKeys.filterNot(currApplicableRules.map(_.key).contains(_))
    if (invalidRuleKeys.nonEmpty)
      generateCompilationMessageForSpecifiedRules(
        getMessagePosition(lineNumber, currentOffset, 0, 1, id),
        invalidRuleKeys,
        Severity.Warning,
        None
      )
    else Seq.empty
  }

  private def updateNonIgnoredRules(
      prevNonIgnoredRules: Seq[CodeFlaggingRule],
      ignoredRules: Seq[String]): Seq[CodeFlaggingRule] =
    prevNonIgnoredRules.filterNot(rule => ignoredRules.contains(rule.key))

  private def scanLines(
      id: SourceUnitId,
      it: Iterator[String],
      applicableRules: Seq[CodeFlaggingRule]
  ): Seq[CompilationMessage] = {
    // Performance sensitive, hence use of iterator etc.
    var lineNumber = 0
    var ignoring = false
    var lineWiseIgnore = false // Whether we have just seen a line-wise ignore
    val nonIgnoredRules = mutable.Stack[Seq[CodeFlaggingRule]]()
    nonIgnoredRules.push(applicableRules)
    // We're not able to determine whether a start block was specific or general from an end block
    // so specificBlockStack keeps track of whether start blocks are specific (true) or general (false)
    var specificBlockTracker = mutable.Stack[Boolean]()
    val messages = Vector.newBuilder[CompilationMessage]
    var currentOffset = 0
    while (it.hasNext) {
      val line = it.next()
      if (line.contains(IgnoreFileToken)) {
        // Ignore the entire file, discarding any messages accumulated already
        if (!line.contains(RuleToken)) return Seq()
        else {
          val ignoredRules = getRuleKeys(line, IgnoreFileToken)
          messages ++= checkInvalidRules(lineNumber, currentOffset, id, ignoredRules, nonIgnoredRules.head)
          nonIgnoredRules.push(updateNonIgnoredRules(nonIgnoredRules.head, ignoredRules))
        }
      } else if (line.contains(IgnoreEndToken)) {
        // ignore this line but start scanning again from the next one
        ignoring = false
        // Check ensures that we have seen a start token before the end token
        if (specificBlockTracker.nonEmpty) {
          // If the associated start block was specific, remove ignored rules from the stack
          if (specificBlockTracker.head) nonIgnoredRules.pop()
          specificBlockTracker.pop()
        }
      } else if (line.contains(IgnoreStartToken)) {
        if (!line.contains(RuleToken)) {
          ignoring = true
          specificBlockTracker.push(false)
        } else {
          val ignoredRules = getRuleKeys(line, IgnoreStartToken)
          messages ++= checkInvalidRules(lineNumber, currentOffset, id, ignoredRules, nonIgnoredRules.head)
          nonIgnoredRules.push(updateNonIgnoredRules(nonIgnoredRules.head, ignoredRules))
          specificBlockTracker.push(true)
        }
      } else if (line.contains(IgnoreLineToken)) {
        if (!line.contains(RuleToken)) {
          if (it.hasNext) {
            // Skip next line
            lineNumber += 1
            currentOffset += it.next().length
          }
        } else {
          val ignoredRules = getRuleKeys(line, IgnoreLineToken)
          messages ++= checkInvalidRules(lineNumber, currentOffset, id, ignoredRules, nonIgnoredRules.head)
          nonIgnoredRules.push(updateNonIgnoredRules(nonIgnoredRules.head, ignoredRules))
          lineWiseIgnore = true
        }
      } else if (!ignoring) {
        messages ++= scanLine(id, nonIgnoredRules.head, lineNumber, currentOffset, line)
        // If rules are specified on a line-wise ignore, add them back to the applicable rules
        if (lineWiseIgnore) {
          nonIgnoredRules.pop()
          lineWiseIgnore = false
        }
      }
      currentOffset += line.length // linesWithSeparators includes newline, no need to +1
      lineNumber += 1
    }
    messages.result()
  }

  private def scanLine(
      id: SourceUnitId,
      applicableRules: Seq[CodeFlaggingRule],
      lineNumber: Int,
      currentOffset: Int,
      line: String): Seq[CompilationMessage] = {
    applicableRules.flatMap { codingRule =>
      val startTime = if (timingEnabled) System.nanoTime() else 0L
      val (excludes, includes) = codingRule.regexes.partition(_.exclude)
      val anyExcludeMatches = excludes.exists(_.regex.findAllMatchIn(line).nonEmpty)
      val messages = if (!anyExcludeMatches) {
        if (includes.nonEmpty) {
          generateCompilationMessageForIncludes(includes, line, lineNumber, currentOffset, codingRule, id)
        } else {
          Seq(
            generateCompilationMessage(
              lineNumber = lineNumber,
              offset = currentOffset,
              start = 0,
              end = 1,
              customMessage = None,
              codingRule = codingRule,
              id = id
            ))
        }
      } else Nil
      val endTime = if (timingEnabled) System.nanoTime() else 0L
      if (timingEnabled) ruleTimings.computeIfAbsent(codingRule.key, makeAdder).add(endTime - startTime)
      messages
    }
  }

  private def generateCompilationMessageForIncludes(
      includes: Seq[Pattern],
      line: String,
      lineNumber: Int,
      offset: Int,
      codingRule: CodeFlaggingRule,
      id: SourceUnitId
  ): Seq[CompilationMessage] = {
    includes.flatMap { includePattern =>
      val includeMatches = includePattern.regex.findAllMatchIn(line)
      includeMatches.map { m =>
        generateCompilationMessage(
          lineNumber = lineNumber,
          offset = offset,
          start = m.start,
          end = m.end,
          customMessage = includePattern.message,
          codingRule = codingRule,
          id = id
        )
      }
    }
  }

  private def generateCompilationMessageForSpecifiedRules(
      pos: MessagePosition,
      invalidRuleKeys: Seq[String],
      severity: Severity,
      alarmId: Option[String]): Seq[CompilationMessage] =
    Seq(CompilationMessage(
      Some(pos),
      s"Rule key(s) are invalid or already being ignored: ${invalidRuleKeys.mkString(", ").replaceAll("[\n\r\"]", "")}",
      severity,
      alarmId))

  private def getMessagePosition(
      lineNumber: Int,
      offset: Int,
      start: Int,
      end: Int,
      id: SourceUnitId): MessagePosition = {
    MessagePosition(
      filepath = id.localRootToFilePath.pathString,
      startLine = lineNumber + 1,
      startColumn = start + 1,
      endLine = lineNumber + 1,
      endColumn = end + 1,
      startPoint = offset + start,
      endPoint = offset + end
    )
  }

  private def generateCompilationMessage(
      lineNumber: Int,
      offset: Int,
      start: Int,
      end: Int,
      customMessage: Option[String],
      codingRule: CodeFlaggingRule,
      id: SourceUnitId
  ): CompilationMessage = {
    val messagePosition = getMessagePosition(lineNumber, offset, start, end, id)

    CompilationMessage(
      Some(messagePosition),
      customMessage.getOrElse(codingRule.description),
      codingRule.severityLevel,
      Some(codingRule.title),
      isSuppressed = false,
      isNew = codingRule.isNew
    )
  }
}
