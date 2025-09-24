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
package optimus.platform.util

import msjava.hdom.Document
import msjava.hdom.transform.HDOMSource
import optimus.platform.util.xml.CustomXmlLoader

import scala.xml.Node

object XMLUtils {
  def extractAttribute(xmlAttr: String, node: Node): Option[String] = (node \ xmlAttr).headOption.map(_.text)
  def extractBoolean(xmlAttr: String, node: Node): Boolean = extractAttribute(xmlAttr, node).exists(_.toBoolean)

  def hdomToScala(doc: Document): Node = {
    val source = new HDOMSource(doc)
    CustomXmlLoader(xmlReader = Some(source.getXMLReader)).loadDocument(source.getInputSource).docElem
  }
}
