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

final case class HtmlComponent(interpreter: HtmlInterpreters.Type, htmlNodes: collection.Seq[HtmlNode]) {
  override def toString: String = interpreter(htmlNodes)

  def withHtmlNodes(newHtmlNodes: collection.Seq[HtmlNode]) = HtmlComponent(interpreter, newHtmlNodes)
  def withInterpreter(interpreter: HtmlInterpreters.Type) = HtmlComponent(interpreter, htmlNodes)
  def withDefaultContents = HtmlComponent(interpreter, htmlNodes.map(_.withDefaultContents))

  // Flatten to a list of LeafHtmlNodes
  def flatten = HtmlComponent(interpreter, htmlNodes.flatMap(_.flatten))

  // Remove nodes which satisfy the predicate
  def withoutNodes(predicate: HtmlNode => Boolean) =
    HtmlComponent(interpreter, htmlNodes.flatMap(_.withoutNodes(predicate)))

  // Keep groups, but remove all leaf nodes
  def withoutLeafNodes: HtmlComponent = withoutNodes(_.isInstanceOf[LeafHtmlNode])

  // Keep groups (always named), but remove generic leaf nodes which do not reveal much about contents (e.g. NoStyle)
  def withoutUndescriptive: HtmlComponent = withoutNodes(!_.isDescriptive)

  // Remove all children of all GroupHtmlNodes
  def collapseAllGroups = HtmlComponent(interpreter, htmlNodes.map(_.collapse))

  // Remove all children of GroupHtmlNodes matching general predicate
  def collapseGroupsIf(groupPredicate: GroupHtmlNode => Boolean) =
    HtmlComponent(interpreter, htmlNodes.map(_.collapseIf(groupPredicate)))

  // Remove all children of GroupHtmlNodes with specified groupName
  def collapseGroupsNamed(groupName: String) =
    HtmlComponent(interpreter, htmlNodes.map(_.collapseIf(_.groupName == groupName)))

  // Remove all children of GroupHtmlNodes with groupName matching predicate
  def collapseGroupsIfNamed(groupNamePredicate: String => Boolean) =
    HtmlComponent(interpreter, htmlNodes.map(_.collapseIf(n => groupNamePredicate(n.groupName))))

  def printStructure: String = HtmlComponent.printStructure(htmlNodes.toSeq: _*)
}

object HtmlComponent {
  def apply(htmlNodes: collection.Seq[HtmlNode]): HtmlComponent = new HtmlComponent(HtmlInterpreters.prod, htmlNodes)

  def printStructure(htmlNodes: HtmlNode*): String = {
    val sb = new PrettyStringBuilder
    var first = true
    htmlNodes.foreach { htmlNode =>
      if (!first) sb.appendln(",")
      first = false
      htmlNode.prettyPrintCtor(sb)
    }
    sb.toString
  }
}

object HtmlInterpreters {
  type Type = collection.Seq[HtmlNode] => String

  def prod(attributes: collection.Seq[HtmlNode]): String =
    html("&nbsp;", attributes, 0, null, prettyHtmlForTests = false)
  def test(attributes: collection.Seq[HtmlNode]): String =
    html("&nbsp;", attributes, 0, null, prettyHtmlForTests = true)

  /**
   * flatten and extract all Link elements
   */
  def links(attributes: collection.Seq[HtmlNode]): collection.Seq[Link] = attributes.flatMap {
    _.flatten.collect { case l: Link => l }
  }

  private def prettyIndent(groupLevel: Int) = "\n" + "\t" * (groupLevel + 1)
  private def nodeToHtmlString(sep: String, groupLevel: Int, pretty: Boolean): HtmlNode => String = {
    // Leaf nodes with separator
    case c: SeparableLeafHtmlNode =>
      c match {
        case (_: NoStyle | _: NoStyleNamed) =>
          if (c.separated) s"<span>${c.contents}</span>$sep"
          else s"<span>${c.contents}</span>"
        case style: StyledLeafHtmlNode =>
          if (c.separated) s"""<span style="${style.css}">${style.contents}</span>$sep"""
          else s"""<span style="${style.css}">${style.contents}</span>"""
      }

    // Unstyled group node
    case Group(name, children @ _*) =>
      var contents = html(sep, children, groupLevel + 1, name, pretty)
      if (pretty && contents.nonEmpty) contents += prettyIndent(groupLevel)
      s"""<span id="$name">$contents</span>"""

    // Empty leaf nodes
    case NewLine => "<br/>"
    case Empty   => "<span></span>"

    // Calculated leaf nodes
    case Indent(level) => "&nbsp;&nbsp;" * level

    // Special leaf nodes
    case Freeform(contents)      => contents.toString
    case Link(href, contents, _) => s"""<a href="$href">$contents</a>"""

    // Styled group nodes
    case styledGroup: StyledGroupHtmlNode =>
      var contents =
        html(sep, styledGroup.contents.attrs, groupLevel + 1, styledGroup.groupName, pretty)
      if (pretty && contents.nonEmpty) contents += prettyIndent(groupLevel)
      s"""<span id="${styledGroup.groupName}" style="${styledGroup.css}">$contents</span>"""
  }

  private def html(
      sep: String,
      attributes: collection.Seq[HtmlNode],
      groupLevel: Int,
      groupName: String,
      prettyHtmlForTests: Boolean): String = {
    val attributesAsHtml = attributes.map(nodeToHtmlString(sep, groupLevel, prettyHtmlForTests)).filterNot(_.isEmpty)

    val result =
      if (!prettyHtmlForTests) attributesAsHtml.mkString
      else if (attributesAsHtml.nonEmpty) {
        val indent = prettyIndent(groupLevel)
        indent + attributesAsHtml.mkString(indent)
      } else ""

    if (groupLevel == 0)
      s"<html>$result\n</html>"
    else
      result
  }

  def plaintext(attributes: collection.Seq[HtmlNode]): String = plaintext(" ", attributes).mkString
  def plaintextSeq(attributes: collection.Seq[HtmlNode]): collection.Seq[String] =
    plaintext(" ", attributes).filterNot(_ == "\n")

  private def nodeToPlaintext(sep: String, node: LeafHtmlNode): String = node match {
    // Leaf nodes with separator
    case c: SeparableLeafHtmlNode if c.separated => c.contents.toString + sep
    // Leaf nodes without separator
    case c @ (_: Freeform | _: Link | _: SeparableLeafHtmlNode) => c.contents.toString

    // Empty leaf nodes
    case NewLine => "\n"
    case Empty   => ""

    // Calculated leaf nodes
    case Indent(level) => "  " * level
  }

  private def plaintext(sep: String, attributes: collection.Seq[HtmlNode]): collection.Seq[String] = attributes
    .flatMap(p =>
      p match {
        case g: GroupHtmlNode => plaintext(sep, g.contents.attrs)
        case c: LeafHtmlNode  => Seq(nodeToPlaintext(sep, c))
      })
}
