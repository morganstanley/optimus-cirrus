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
package optimus.platform.runtime

import java.io.StringReader

import javax.xml.parsers.DocumentBuilderFactory
import javax.xml.xpath.XPath
import javax.xml.xpath.XPathConstants
import javax.xml.xpath.XPathFactory
import org.w3c.dom.Document
import org.w3c.dom.Node
import org.w3c.dom.NodeList
import org.xml.sax.InputSource

import scala.collection.mutable

object XmlConfigurationResult {
  final case class ConfigurationNodeAttribute(prop: String, value: String)
}

abstract class XmlConfigurationResult(xml: String) {
  import XmlConfigurationResult.ConfigurationNodeAttribute
  class ConfigurationNode(
      val name: String,
      val attributes: Seq[ConfigurationNodeAttribute],
      private[optimus] var parent: Option[ConfigurationNode],
      val children: Seq[ConfigurationNode],
      val textContent: Option[String]) {
    def hasChildren: Boolean = children.nonEmpty
    def setParent(p: Option[ConfigurationNode]): Unit = {
      require(parent.isEmpty)
      parent = p
    }

    def getAttributeValue(prop: String): Option[String] = {
      // It's not worth optimizing this look-up since we don't expect deep
      // nesting and we don't expect to repeatedly query the parsed XML.
      attributes
        .find(_.prop == prop)
        .map(_.value)
        .orElse(
          parent.map(_.getAttributeValue(prop)).flatten
        )
    }

    def getOwnAttributeValue(prop: String): Option[String] = {
      attributes
        .find(_.prop == prop)
        .map(_.value)
    }

    def getOwnAttributeValueOrFail(prop: String): String = {
      getOwnAttributeValue(prop).getOrElse(throw new IllegalArgumentException(s"Expected attribute '$prop' to be set"))
    }
  }
  protected object ConfigurationNode {
    def apply(
        name: String,
        attributes: Seq[ConfigurationNodeAttribute],
        parent: Option[ConfigurationNode],
        children: Seq[ConfigurationNode],
        textContext: Option[String]): ConfigurationNode = {
      new ConfigurationNode(name, attributes, parent, children, textContext)
    }
    def apply(expression: String, n: Node): ConfigurationNode = {
      val cn: ConfigurationNode = buildWithoutParent(expression, n)
      setParentsRecursively(cn)
      cn
    }
    def buildWithoutParent(expression: String, n: Node): ConfigurationNode = {
      val name: String = n.getNodeName
      val attributes: mutable.ArrayBuffer[ConfigurationNodeAttribute] =
        mutable.ArrayBuffer.empty[ConfigurationNodeAttribute]
      var i: Int = 0
      if (n.hasAttributes) {
        while (i < n.getAttributes.getLength) {
          val attr = n.getAttributes.item(i)
          attributes.append(ConfigurationNodeAttribute(attr.getNodeName, attr.getNodeValue))
          i += 1
        }
      }
      val children: mutable.ArrayBuffer[ConfigurationNode] = mutable.ArrayBuffer.empty[ConfigurationNode]
      i = 0
      val nextExpression = s"$expression/node()"
      val nodeList: NodeList = getNodes(nextExpression)
      while (i < nodeList.getLength) {
        val child: Node = nodeList.item(i)
        if (child.getParentNode.equals(n) && child.getNodeName != "#text") {
          children.append(buildWithoutParent(nextExpression, child))
        }
        i += 1
      }
      apply(name, attributes, None, children, Option(n.getTextContent))
    }
    private def setParentsRecursively(cn: ConfigurationNode): Unit = {
      cn.children.foreach { child: ConfigurationNode =>
        child.setParent(Some(cn))
        setParentsRecursively(child)
      }
    }
  }

  private[this] val rootNodeName: String = "metroplex"
  private[this] lazy val xmlDocument: Document = {
    val dbf: DocumentBuilderFactory = DocumentBuilderFactory.newInstance()
    dbf.setIgnoringComments(true)
    dbf.newDocumentBuilder().parse(new InputSource(new StringReader(xml)))
  }
  private[this] val xPath: XPath = XPathFactory
    .newInstance()
    .newXPath()
  private[this] def getNodes(expression: String): NodeList = {
    xPath
      .compile(expression)
      .evaluate(xmlDocument, XPathConstants.NODESET)
      .asInstanceOf[NodeList]
  }

  protected[optimus] lazy val root = {
    val nodeList: NodeList = getNodes(rootNodeName)
    require(nodeList.getLength == 1)
    ConfigurationNode(rootNodeName, nodeList.item(0))
  }
}
