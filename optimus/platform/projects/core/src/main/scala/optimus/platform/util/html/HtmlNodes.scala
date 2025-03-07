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

import optimus.platform.util.PrettyStringBuilder
import org.apache.commons.text.StringEscapeUtils

/**
 * ***** Concrete GroupHtmlNodes ******
 */
// Unstyled named group
final case class Group(groupName: String, innerNodes: HtmlNode*) extends GroupHtmlNode {
  override def prettyGroupName: String =
    if (innerNodes.isEmpty) s"""Group("$groupName")""" else s"""Group("$groupName","""
  override def withContents(newContents: collection.Seq[HtmlNode]) = Group(groupName, newContents.toSeq: _*)
}

/**
 * ***** Concrete LeafHtmlNodes ******
 */
// Leaf nodes without contents
case object Empty extends EmptyLeafHtmlNode
case object NewLine extends EmptyLeafHtmlNode

// Leaf nodes with calculated contents
final case class Indent(indentLevel: Int) extends CalculatedLeafNode {
  override def prettyPrintCtor(sb: PrettyStringBuilder): PrettyStringBuilder = sb ++= s"Indent($indentLevel)"
}

// Hyperlink leaf node
final case class Link(
    href: String = "javascript:;",
    contents: StringContents = classOf[Link],
    handler: Option[() => Unit] = None)
    extends LeafHtmlNode {
  override def withDefaultContents = Link()
  override def isDescriptive = true
}

// Freeform text leaf node (for direct printing of text, i.e. not wrapped in <span>...</span>, no spacing at end, no HTML escaping)
final case class Freeform(contents: FreeFormStringBuilderContents = "[Freeform]") extends LeafHtmlNode {
  override def withDefaultContents = Freeform()
  override def isDescriptive = false
}

// Unstyled leaf nodes (wrapped in <span>...</span> and with space at end by default)
final case class NoStyle(contents: StringContents = classOf[NoStyle]) extends SeparableLeafHtmlNode {
  override def withDefaultStringContents = NoStyle()
  override def isDescriptive = false
  def named(name: String) = NoStyleNamed(name, contents)
}
object NoStyle {
  def unseparated(contents: StringContents): NoStyle = { val ns = NoStyle(contents); ns.separated = false; ns }
}
final case class NoStyleNamed(name: String, contents: StringContents) extends SeparableLeafHtmlNode {
  override def withDefaultStringContents: NoStyleNamed = NoStyleNamed.default(name)
  override def isDescriptive = true // This will be constructed with a useful name
  override def prettyPrintCtor(sb: PrettyStringBuilder): PrettyStringBuilder =
    sb ++= s"""NoStyleNamed.default("$name")"""
}
object NoStyleNamed {
  def apply(name: String): StringContents => NoStyleNamed = NoStyleNamed(name, _)
  def unseparated(name: String): StringContents => NoStyleNamed = { contents =>
    val ns = NoStyleNamed(name, contents); ns.separated = false; ns
  }
  def default(name: String) = NoStyleNamed(name, s"[${classOf[NoStyleNamed].getSimpleName}($name)]")
}

// Styled leaf nodes (content always wrapped in <span>...</span> and with space at the end by default)
final case class NodeName(contents: StringContents = classOf[NodeName]) extends StyledLeafHtmlNode {
  override def css = "font-family: 'Consolas', 'Courier New', monospace; color: green;"
  override def withDefaultStringContents = NodeName()
}
final case class NodeIndex(contents: StringContents = classOf[NodeIndex]) extends StyledLeafHtmlNode {
  override def css = "font-weight: bold;"
  override def withDefaultStringContents = NodeIndex()
}
final case class NodeContent(contents: StringContents = classOf[NodeContent]) extends StyledLeafHtmlNode {
  override def css = "font-family: 'Consolas', 'Courier New', monospace;"
  override def withDefaultStringContents = NodeContent()
}
final case class NodeState(contents: StringContents = classOf[NodeState]) extends StyledLeafHtmlNode {
  override def css = "color: blue;"
  override def withDefaultStringContents = NodeState()
}
final case class TweakHeader(contents: StringContents = classOf[TweakHeader]) extends StyledLeafHtmlNode {
  override def css = "font-weight: bold;"
  override def withDefaultStringContents = TweakHeader()
}
final case class TweakTargetStyle(contents: StringContents = classOf[TweakVal]) extends StyledLeafHtmlNode {
  override def css = "color: black;"
  override def withDefaultStringContents = TweakVal()
}
final case class TweakVal(contents: StringContents = classOf[TweakVal]) extends StyledLeafHtmlNode {
  override def css = "font-style: italic; color: blue;"
  override def withDefaultStringContents = TweakVal()
}

/** purple, for tweaks that are not relevant to the specific calculation (when traceTweaks is enabled) but are for the NTI */
final case class TweakMid(contents: StringContents = classOf[TweakMid]) extends StyledLeafHtmlNode {
  override def css = "color: purple;"
  override def withDefaultStringContents = TweakMid()
}

/** Greyed out, for tweaks that are not relevant to the calculation (when traceTweaks is enabled) */
final case class TweakLow(contents: StringContents = classOf[TweakLow]) extends StyledLeafHtmlNode {
  override def css = "color: silver;"
  override def withDefaultStringContents = TweakLow()
}
final case class Break(contents: StringContents = classOf[Break]) extends StyledLeafHtmlNode {
  override def css = "background-color: #d0e4fe; display: block;"
  override def withDefaultStringContents = Break()
}
final case class TimeHeader(contents: StringContents = classOf[TimeHeader]) extends StyledLeafHtmlNode {
  override def css = "background-color: #eae8e8; font-weight: bold;"
  override def withDefaultStringContents = TimeHeader()
}
final case class TimeValue(contents: StringContents = classOf[TimeValue]) extends StyledLeafHtmlNode {
  override def css = "font-style: italic;"
  override def withDefaultStringContents = TimeValue()
}
final case class SStackHeader(contents: StringContents = classOf[SStackHeader]) extends StyledLeafHtmlNode {
  override def css = "background-color: #99dd8e; font-weight: bold;"
  override def withDefaultStringContents = SStackHeader()
}
final case class ArgumentHeader(contents: StringContents = classOf[ArgumentHeader]) extends StyledLeafHtmlNode {
  override def css = "background-color: #c9c9ff; font-weight: bold;"
  override def withDefaultStringContents = ArgumentHeader()
}
final case class EntityHeader(contents: StringContents = classOf[EntityHeader]) extends StyledLeafHtmlNode {
  override def css = "background-color: #fae81b; font-weight: bold;"
  override def withDefaultStringContents = EntityHeader()
}
final case class EntityClassName(contents: StringContents = classOf[EntityClassName]) extends StyledLeafHtmlNode {
  override def css = "font-style: italic;"
  override def withDefaultStringContents = EntityClassName()
}
final case class Annotation(contents: StringContents = classOf[Annotation]) extends StyledLeafHtmlNode {
  override def css = "background-color: #AAEEA2; color: blue; vertical-align: super;"
  override def withDefaultStringContents = Annotation()
}
final case class Bold(contents: StringContents = classOf[Bold]) extends StyledLeafHtmlNode {
  override def css = "font-weight: bold;"
  override def withDefaultStringContents = Bold()
  override def isDescriptive = false // Bold is not descriptive of contents
}
object PreFormatted {
  def apply(str: String, wrap: Boolean = true) = new PreFormatted(StringContents(str), wrap)
}
final case class PreFormatted(contents: StringContents, wrap: Boolean) extends StyledLeafHtmlNode {
  // Note: there is a bug in javax.swing.text.html.InlineView.setPropertiesFromAttributes which means that
  // white-space: pre isn't supported, only "nowrap".
  //
  // Also it won't attempt to break within words at all.
  override def css = "font-family: monospace;" + (if (!wrap) " white-space: nowrap;" else "")
  override def withDefaultStringContents = PreFormatted(classOf[PreFormatted], false)
}

/**
 * ***** Underlying traits ******
 */
sealed trait HtmlNodeContents {
  def isEmpty: Boolean
}
final case class StringContents(str: String) extends HtmlNodeContents {
  override def toString: String = StringEscapeUtils.escapeHtml4(str)
  override def isEmpty: Boolean = str.isEmpty
}
object StringContents {
  val Empty = StringContents("")
  implicit def stringToStringContents(contents: String): StringContents = new StringContents(contents)
  implicit def defaultContents(clazz: Class[_]): StringContents =
    new StringContents("[" + clazz.getSimpleName + "]")
}
final case class FreeFormStringBuilderContents(stringBuilder: StringBuilder) extends HtmlNodeContents {
  override def toString: String = stringBuilder.toString
  def ++=(str: String): StringBuilder = stringBuilder.append(str)
  override def isEmpty: Boolean = stringBuilder.isEmpty
}
object FreeFormStringBuilderContents {
  def apply(contents: String): FreeFormStringBuilderContents =
    new FreeFormStringBuilderContents(new StringBuilder(contents))
  implicit def stringToStringBuilderContents(contents: String): FreeFormStringBuilderContents = apply(contents)
}
final case class MoreHtmlNodes(attrs: collection.Seq[HtmlNode]) extends HtmlNodeContents {
  override def toString: String = attrs.toString
  override def isEmpty: Boolean = attrs.isEmpty || attrs.forall(_.contents.isEmpty)
}
object MoreHtmlNodes {
  val Empty = MoreHtmlNodes(Nil)
  def apply(attrs: collection.Seq[HtmlNode]): MoreHtmlNodes = new MoreHtmlNodes(attrs)
  implicit def attributesToMoreHtmlNodes(contents: collection.Seq[HtmlNode]): MoreHtmlNodes = new MoreHtmlNodes(
    contents)
}
object EmptyContents extends HtmlNodeContents {
  override def isEmpty: Boolean = true
  override def toString: String = throw new Exception("EmptyContents.toString should never be called")
}
object CalculatedContents extends HtmlNodeContents {
  override def isEmpty: Boolean = false
  override def toString: String = throw new Exception("CalculatedContents.toString should never be called")
}

sealed trait HtmlNode {
  // Print name of attribute for structure inspection
  def prettyPrintCtor(sb: PrettyStringBuilder): PrettyStringBuilder
  // HtmlNode either wraps a string or more attributes
  def contents: HtmlNodeContents
  // Normalise the contents
  def withDefaultContents: HtmlNode

  def isEmpty: Boolean = contents.isEmpty

  def flatten: collection.Seq[HtmlNode]
  def withoutNodes(predicate: HtmlNode => Boolean): Option[HtmlNode]
  def isDescriptive: Boolean
  def collapse: HtmlNode
  def collapseIf(groupPredicate: GroupHtmlNode => Boolean): HtmlNode
}

sealed trait Styled {
  def css: String
}

// Wraps more HtmlNodes as contents
sealed trait GroupHtmlNode extends HtmlNode {
  // Constructor arguments to be overridden by concrete implementations
  def groupName: String
  def innerNodes: collection.Seq[HtmlNode]
  final override def contents: MoreHtmlNodes = innerNodes

  // Clone with new contents
  def withContents(newContents: collection.Seq[HtmlNode]): GroupHtmlNode

  // How is the group name printed
  def prettyGroupName: String

  // Final def's
  final override def prettyPrintCtor(sb: PrettyStringBuilder): PrettyStringBuilder = {
    sb ++= prettyGroupName
    if (innerNodes.nonEmpty) {
      sb.indent()
      sb.endln()
      var first = true
      innerNodes.foreach { htmlNode =>
        if (!first) sb.appendln(",")
        first = false
        htmlNode.prettyPrintCtor(sb)
      }
      sb.unIndent()
      sb.endln().append(")")
    }
    sb
  }

  final override def withDefaultContents: GroupHtmlNode =
    withContents(innerNodes.map(_.withDefaultContents))

  final override def flatten: collection.Seq[HtmlNode] = innerNodes.flatMap(_.flatten)
  final override def withoutNodes(predicate: HtmlNode => Boolean): Option[GroupHtmlNode] =
    if (predicate(this)) None else Some(withContents(innerNodes.flatMap(_.withoutNodes(predicate))))
  final override def isDescriptive = true

  final override def collapse: HtmlNode = withContents(Nil)

  final override def collapseIf(groupPredicate: GroupHtmlNode => Boolean): HtmlNode =
    if (groupPredicate(this)) collapse
    else withContents(innerNodes.map(_.collapseIf(groupPredicate)))
}

trait StyledGroupHtmlNode extends GroupHtmlNode with Styled {
  // Styled groups are only ever named after the concrete implementation
  final override def groupName: String = getClass.getSimpleName
  final override def prettyGroupName: String = if (innerNodes.isEmpty) groupName + "()" else groupName + "("
}

// Leaf HTML node wraps string contents
sealed trait LeafHtmlNode extends HtmlNode {
  // By default, name is just the default constructor for the class name
  override def prettyPrintCtor(sb: PrettyStringBuilder): PrettyStringBuilder =
    sb ++= this.getClass.getSimpleName ++= "()"

  // Final implementation for all LeafHtmlNodes
  final override def flatten: collection.Seq[HtmlNode] = collection.Seq(this)
  final override def withoutNodes(predicate: HtmlNode => Boolean): Option[LeafHtmlNode] =
    if (predicate(this)) None else Some(this)
  final override def collapse: HtmlNode = this
  final override def collapseIf(groupPredicate: GroupHtmlNode => Boolean): HtmlNode = this
}

sealed trait SeparableLeafHtmlNode extends LeafHtmlNode {
  final var separated: Boolean = true
  final override def withDefaultContents: SeparableLeafHtmlNode = {
    val default = withDefaultStringContents
    default.separated = separated
    default
  }
  def withDefaultStringContents: SeparableLeafHtmlNode
  final def noSeparator(): SeparableLeafHtmlNode = { separated = false; this }
  final def withSeparator(): SeparableLeafHtmlNode = { separated = true; this }
}

sealed trait StyledLeafHtmlNode extends SeparableLeafHtmlNode with Styled {
  // Styled nodes only wrap string contents
  def contents: StringContents

  // All styled nodes have useful names which tell us about their intended contents
  override def isDescriptive = true
}

// Leaf nodes without contents
sealed trait EmptyLeafHtmlNode extends LeafHtmlNode {
  // Empty nodes are usually represented by singleton objects
  // By default, name is just the name of the singleton object
  override def prettyPrintCtor(sb: PrettyStringBuilder): PrettyStringBuilder =
    sb ++= this.getClass.getSimpleName.stripSuffix("$")

  final def contents: EmptyContents.type = EmptyContents
  final def withDefaultContents: EmptyLeafHtmlNode = this

  // Nodes without contents don't tell us much about node contents
  final override def isDescriptive = false
}

// Leaf nodes with contents not specified by user code
sealed trait CalculatedLeafNode extends LeafHtmlNode {
  final def contents: CalculatedContents.type = CalculatedContents
  final def withDefaultContents: CalculatedLeafNode = this

  // Nodes with calculated contents don't tell us much about node contents
  final override def isDescriptive = false
}
