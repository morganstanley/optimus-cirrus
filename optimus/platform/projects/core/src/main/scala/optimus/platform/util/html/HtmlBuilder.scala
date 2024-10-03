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
package optimus.platform.util.html

import optimus.core.SparseBitSet
import optimus.core.TPDMask
import optimus.graph.DiagnosticSettings
import optimus.graph.NodeTrace
import optimus.platform.Tweak
import optimus.platform.util.PrettyStringBuilder

import scala.collection.mutable.ListBuffer

class HtmlBuilder(val cfg: DetailsConfig = DetailsConfig.default, val short: Boolean = false) {
  var tpdMask: TPDMask = _
  var infectionSet: SparseBitSet = _
  def hasInfectionSet: Boolean =
    NodeTrace.traceTweaks.getValue && (infectionSet ne null) && !DiagnosticSettings.traceTweaksOverflowDetected
  private var buffer = new ListBuffer[HtmlNode]
  private var indentLevel = 0
  private var newLineCount: Int = -1
  private def onNewLine: Boolean = newLineCount != 0

  override def toString: String = HtmlInterpreters.prod(buffer)
  def toStringWithLineBreaks: String = toString.replace("\n", "<br>")
  def toPlaintext: String = HtmlInterpreters.plaintext(buffer)
  def toPlaintextSeq: collection.Seq[String] = HtmlInterpreters.plaintextSeq(buffer)
  def toComponent = HtmlComponent(HtmlInterpreters.prod, buffer)
  def toComponent(htmlInterpreter: HtmlInterpreters.Type) = HtmlComponent(htmlInterpreter, buffer)

  def tweaksToWrite(allTweaks: Iterable[Tweak]): Iterable[Tweak] =
    if (hasInfectionSet && cfg.scenarioStackSmart) allTweaks.filter(x => infectionSet.contains(x.id)) else allTweaks

  /** if true (and traceTweaks is enabled), then tweak is relevant to calculation */
  def displayInColour(id: Int): Boolean = {
    // ssEffective checked here to match ScenarioStack writeHtml
    val highlightingDisabled = !(cfg.scenarioStackEffective && hasInfectionSet)
    highlightingDisabled || infectionSet.contains(id)
  }

  /**
   * extract all Link elements from the builder
   */
  def links: collection.Seq[Link] = HtmlInterpreters.links(buffer)

  private def unIndent(): Unit = {
    indentLevel -= 1
  }
  private def indent(): Unit = {
    indentLevel += 1
  }

  def add(htmlLeafNode: LeafHtmlNode): HtmlBuilder = {
    if (onNewLine && indentLevel > 0 && !htmlLeafNode.isEmpty) {
      buffer += Indent(indentLevel)
      newLineCount = 0
    }

    if ((htmlLeafNode ne NewLine) || !onNewLine)
      buffer += htmlLeafNode

    if (htmlLeafNode eq NewLine)
      newLineCount = 1
    else if (!htmlLeafNode.isEmpty)
      newLineCount = 0

    this
  }

  // User should never be able to add group nodes directly, only via the namedGroup/styledGroup helper methods
  private def add(groupHtmlNode: GroupHtmlNode): HtmlBuilder = {
    buffer += groupHtmlNode
    this
  }

  def ++=(htmlNode: LeafHtmlNode): HtmlBuilder = add(htmlNode)

  def noStyle(noStyleString: String): HtmlBuilder = add(NoStyle(noStyleString))
  def ++=(noStyleString: String): HtmlBuilder = noStyle(noStyleString)

  def noStyle(name: String, noStyleString: String): HtmlBuilder = add(NoStyleNamed(name, noStyleString))
  def ++=(namedNoStyleString: (String, String)): HtmlBuilder = noStyle(namedNoStyleString._1, namedNoStyleString._2)

  def freeform(freeformString: String): HtmlBuilder = {
    buffer.lastOption match {
      case Some(Freeform(contents)) => contents ++= freeformString
      case _                        => add(Freeform(freeformString))
    }
    this
  }
  def +=(freeformString: String): HtmlBuilder = freeform(freeformString)

  def buildFreeform[T](build: StringBuilder => T): HtmlBuilder = {
    val sb = new StringBuilder
    build(sb)
    add(Freeform(FreeFormStringBuilderContents(sb)))
  }

  // Ensure that we are on new line without adding a new line if we already are
  def newLine(): HtmlBuilder = {
    if (!onNewLine) add(NewLine)
    this
  }

  // Ensure the number of new lines since the previous content is as specified
  def newLines(number: Int = 1): HtmlBuilder = {
    assert(number >= 0)

    if (newLineCount >= 0 && newLineCount < number) {
      // Add specified number of new lines directly to the buffer
      (newLineCount until number).foreach { _ =>
        buffer += NewLine
      }

      newLineCount = number
    }

    this
  }

  // Forcefully add NewLine to buffer
  def forceNewLine(): HtmlBuilder = {
    buffer += NewLine
    newLineCount = Math.max(1, 1 + newLineCount)
    this
  }

  def removeSeparator(): HtmlBuilder = {
    buffer.lastOption match {
      case Some(ns: SeparableLeafHtmlNode) => ns.separated = false
      case _                               =>
    }
    this
  }

  private def buildLeaf(
      leafHtmlNodeConstructor: String => LeafHtmlNode,
      buildLeafNode: StringBuilder => Unit): HtmlBuilder = {
    val builder = new StringBuilder
    buildLeafNode(builder)
    add(leafHtmlNodeConstructor(builder.toString))
  }
  def buildLeaf(leafHtmlNodeConstructor: StringContents => LeafHtmlNode)(buildLeafNode: StringBuilder => Unit)(implicit
      d: DummyImplicit): HtmlBuilder =
    buildLeaf((x: String) => leafHtmlNodeConstructor(StringContents(x)), buildLeafNode)
  def buildLeaf(leafHtmlNodeConstructor: FreeFormStringBuilderContents => LeafHtmlNode)(
      buildLeafNode: StringBuilder => Unit)(implicit d1: DummyImplicit, d2: DummyImplicit): HtmlBuilder =
    buildLeaf((x: String) => leafHtmlNodeConstructor(FreeFormStringBuilderContents(x)), buildLeafNode)

  private def buildPrettyLeaf(
      leafHtmlNodeConstructor: String => LeafHtmlNode,
      buildLeafNode: PrettyStringBuilder => PrettyStringBuilder): HtmlBuilder = {
    val builder = new PrettyStringBuilder(false, short)
    val result = buildLeafNode(builder)
    add(leafHtmlNodeConstructor(result.toString))
  }
  def buildPrettyLeaf(
      leafHtmlNodeConstructor: StringContents => LeafHtmlNode,
      buildLeafNode: PrettyStringBuilder => PrettyStringBuilder)(implicit d: DummyImplicit): HtmlBuilder =
    buildPrettyLeaf((x: String) => leafHtmlNodeConstructor(StringContents(x)), buildLeafNode)
  def buildPrettyLeaf(
      leafHtmlNodeConstructor: FreeFormStringBuilderContents => LeafHtmlNode,
      buildLeafNode: PrettyStringBuilder => PrettyStringBuilder)(implicit
      d1: DummyImplicit,
      d2: DummyImplicit): HtmlBuilder =
    buildPrettyLeaf((x: String) => leafHtmlNodeConstructor(FreeFormStringBuilderContents(x)), buildLeafNode)

  private def addGroup[T](
      groupHtmlNode: collection.Seq[HtmlNode] => GroupHtmlNode,
      indented: Boolean,
      newLinesNo: Int,
      buildHtml: => T): T = {
    val outerBuffer = buffer
    val innerBuffer = new ListBuffer[HtmlNode]
    buffer = innerBuffer
    if (indented) indent()
    if (newLinesNo > 0) newLines(newLinesNo)
    val result = buildHtml
    buffer = outerBuffer
    add(groupHtmlNode(innerBuffer))
    if (indented) unIndent()
    if (newLinesNo > 0) newLines(newLinesNo)
    result
  }
  def namedGroup[T](groupName: String, indented: Boolean = false, separateBy: Int = 0)(buildHtml: => T): T = {
    addGroup(node => Group(groupName, node.toSeq: _*), indented, separateBy, buildHtml)
  }
  def styledGroup[T](
      styledGroup: collection.Seq[HtmlNode] => StyledGroupHtmlNode,
      indented: Boolean = false,
      separateBy: Int = 0)(buildHtml: => T): T = {
    addGroup(styledGroup, indented, separateBy, buildHtml)
  }

  // Indent the scoped builder code
  def indented[T](scoped: => T): T = {
    indent()
    val result = scoped
    unIndent()
    result
  }

  // Scoped builder code is guaranteed to start on a new line
  // Code after the scope is guaranteed to start on a new line
  def block[T](scoped: => T): T = {
    newLine()
    val result = scoped
    newLine()
    result
  }

  // Scoped builder code is guaranteed to start on a new line separated by at least specified number of lines from the
  // previous content
  def separated[T](by: Int)(scoped: => T): T = {
    newLines(by)
    val result = scoped
    newLines(by)
    result
  }

  // Scoped code is guaranteed to start on a new line with a higher indent level
  // Code after the scope is guaranteed to start on a new line with the extra indent level removed
  def indentedBlock[T](scoped: => T): T = {
    indent()
    newLine()
    val result = scoped
    unIndent()
    newLine()
    result
  }

  // Scoped code is guaranteed to start on a new line with a higher indent level
  // Code after the scope is guaranteed to start on a new line separated by at least specified number of lines from the
  // previous content
  def indentedSeparated[T](by: Int)(scoped: => T): T = {
    indent()
    newLines(by)
    val result = scoped
    unIndent()
    newLines(by)
    result
  }

  def scope[T](beforeScope: HtmlBuilder => Unit, afterScope: HtmlBuilder => Unit)(scoped: => T): T = {
    beforeScope(this)
    val result = scoped
    afterScope(this)
    result
  }

  def squareBracketsIndent[T](squareBracketsIndentedBlock: => T): T =
    scope(_.add(HtmlBuilder.squareBracketsOpen), _.add(HtmlBuilder.squareBracketsClose)) {
      indentedBlock(squareBracketsIndentedBlock)
    }
}

object HtmlBuilder {
  private val squareBracketsOpen = NoStyle.unseparated("[")
  private val squareBracketsClose = NoStyle("]")
}
