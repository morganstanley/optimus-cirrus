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
package optimus.profiler.ui

import optimus.platform.util.html.HtmlNode
import optimus.platform.util.html.StyledGroupHtmlNode

// Node details pane
final case class NodeViewStyle(innerNodes: HtmlNode*) extends StyledGroupHtmlNode {
  override def css = s"font-family: consolas; font-size: ${Math.round(12 * Fonts.multiplier)}pt;"
  override def withContents(newContents: Seq[HtmlNode]) = NodeViewStyle(newContents: _*)
}
final case class NodeLineResult(innerNodes: HtmlNode*) extends StyledGroupHtmlNode {
  override def css = "color: #000090;"
  override def withContents(newContents: Seq[HtmlNode]) = NodeLineResult(newContents: _*)
}
final case class NodeChildrenTree(innerNodes: HtmlNode*) extends StyledGroupHtmlNode {
  override def css = s"font-family: calibri; font-size: ${Math.round(11 * Fonts.multiplier)}pt;"
  override def withContents(newContents: Seq[HtmlNode]) = NodeChildrenTree(newContents: _*)
}
