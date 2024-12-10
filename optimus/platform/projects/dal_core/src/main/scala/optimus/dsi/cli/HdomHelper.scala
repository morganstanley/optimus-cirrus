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
package optimus.dsi.cli

import msjava.hdom.Document
import msjava.hdom.Element
import msjava.hdom.output.XMLOutputter
// import msjava.hdom.xpath.HDOMXPathUtils
import msjava.msxml.xpath.MSXPathExpression

import scala.jdk.CollectionConverters._

trait HdomHelper {
  def findElements(
      hdom: Document,
      compiledxPath: MSXPathExpression,
      targetValue: Option[String] = None): List[Element] = ??? /* {
    val results = HDOMXPathUtils.getAllNodes(compiledxPath, hdom).asScala.asInstanceOf[Iterable[Element]]

    if (targetValue.isDefined) {
      results.filter(e => e.getValueAsString().equals(targetValue.get)).toList
    } else {
      results.toList
    }
  } */

  def elementToString(element: Element): String = ??? /* {
    val outputter = getXmlOutputter()
    outputter.outputString(element)
  } */

  def documentToString(doc: Document): String = ??? /* {
    val outputter = getXmlOutputter()
    outputter.outputString(doc)
  } */

  private def getXmlOutputter(): XMLOutputter = ??? /* {
    val outputter = new XMLOutputter()
    outputter.setIndent("  ")
    outputter.setNewlines(true)
    outputter.setLineSeparator("\n")
    outputter.setTextNormalize(true)
    outputter.setOmitEncoding(true)
    outputter.setOmitDeclaration(true)
    outputter
  } */
}
