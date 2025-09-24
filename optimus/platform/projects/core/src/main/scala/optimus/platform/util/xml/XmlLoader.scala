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
package optimus.platform.util.xml

import org.xml.sax.XMLReader

import java.io.Writer
import javax.xml.parsers.SAXParserFactory
import scala.annotation.nowarn
import scala.xml.Elem
import scala.xml.MinimizeMode
import scala.xml.Node
import scala.xml.SAXParser
import scala.xml.XML
import scala.xml.dtd.DocType
import scala.xml.factory.XMLLoader
import scala.xml.parsing.FactoryAdapter
import scala.xml.parsing.NoBindingFactoryAdapter

/**
 * Use this object instead of `scala.xml.XML` for parsing / loading XML. Comments `<!-- note -->` are skipped.
 *
 * History: before version 2.1.0, scala-xml used to ignore comments when parsing / loding XML. Including comments
 * can break existing code that processes XML data.
 *
 * In particular, the `node.child` method includes comments nested within a node.
 * The wildcard projection `node \ "_"` also includes comment nodes.
 *
 * To include comments, use `CustomXmlLoader(skipComments = false).load`.
 */
object XmlLoader extends XMLLoader[Elem] {
  override def adapter: FactoryAdapter = new NoBindingFactoryAdapter() {
    override def comment(ch: Array[Char], start: Int, length: Int): Unit = ()
  }

  @nowarn("msg=10500 scala.xml.XML")
  val encoding: String = XML.encoding

  @nowarn("msg=10500 scala.xml.XML")
  def save(
      filename: String,
      node: Node,
      enc: String = "UTF-8",
      xmlDecl: Boolean = false,
      doctype: DocType = null
  ): Unit = XML.save(filename, node, enc, xmlDecl, doctype)

  @nowarn("msg=10500 scala.xml.XML")
  def write(
      w: Writer,
      node: Node,
      enc: String,
      xmlDecl: Boolean,
      doctype: DocType,
      minimizeTags: MinimizeMode.Value = MinimizeMode.Default
  ): Unit = XML.write(w, node, enc, xmlDecl, doctype, minimizeTags)
}

/**
 * This object builds a custom XML loader for [[scala.xml.Elem]]. It allows enabling certain security sensitive
 * XML features and it can be configured to include comments.
 *
 * The default [[scala.xml.XML]] loder object was hardened for security in scala-xml version 2.x. Its parser has all
 * features disabled that could potentially be exploited when loading untrusted XML files. For details and discussions
 * see https://github.com/scala/scala-xml/pull/177.
 *
 * Whenever possible, it's recommended to use the safe [[XmlLoader]] loader. However, when additional features
 * need to be supported (for example `<!DOCTYPE>` with `<!ENTITY name "value">` declarations), the [[CustomXmlLoader]]
 * object can be used to obtain a working loader.
 */
object CustomXmlLoader {
  def apply(
      secureProcessing: Boolean = true,
      loadExternalDTD: Boolean = false,
      disallowDoctypeDecl: Boolean = true,
      externalParameterEntities: Boolean = false,
      externalGeneralEntities: Boolean = false,
      resolveDTDUris: Boolean = false,
      xIncludeAware: Boolean = false,
      namespaceAware: Boolean = false,
      skipComments: Boolean = true,
      xmlReader: Option[XMLReader] = None): XMLLoader[Elem] = new XMLLoader[Elem] {

    private lazy val parserInstance = new ThreadLocal[SAXParser] {
      override def initialValue = {
        val parser = SAXParserFactory.newInstance()
        parser.setFeature("http://javax.xml.XMLConstants/feature/secure-processing", secureProcessing)
        parser.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", loadExternalDTD)
        parser.setFeature("http://apache.org/xml/features/disallow-doctype-decl", disallowDoctypeDecl)
        parser.setFeature("http://xml.org/sax/features/external-parameter-entities", externalParameterEntities)
        parser.setFeature("http://xml.org/sax/features/external-general-entities", externalGeneralEntities)
        parser.setFeature("http://xml.org/sax/features/resolve-dtd-uris", resolveDTDUris)
        parser.setXIncludeAware(xIncludeAware)
        parser.setNamespaceAware(namespaceAware)
        parser.newSAXParser()
      }
    }

    override def parser: SAXParser = parserInstance.get()

    override def adapter: FactoryAdapter =
      if (!skipComments) super.adapter
      else
        new NoBindingFactoryAdapter() {
          override def comment(ch: Array[Char], start: Int, length: Int): Unit = ()
        }

    override def reader: XMLReader = xmlReader.getOrElse(super.reader)
  }
}
