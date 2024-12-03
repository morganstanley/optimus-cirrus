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

import javax.xml.parsers.SAXParserFactory
import scala.xml.Elem
import scala.xml.SAXParser
import scala.xml.factory.XMLLoader

/**
 * This object builds an XML loader for [[scala.xml.Elem]] that has certain security sensitive features enabled.
 *
 * The default [[scala.xml.XML]] loder object was hardened for security in scala-xml version 2.x. Its parser has all
 * features disabled that could potentially be exploited when loading untrusted XML files. For details and discussions
 * see https://github.com/scala/scala-xml/pull/177.
 *
 * Whenever possible, it's recommended to use the safe `scala.xml.XML` loader. However, when additional features
 * need to be supported (for example `<!DOCTYPE>` with `<!ENTITY name "value">` declarations), the [[UnsafeXML]] object
 * can be used to obtain a working loader.
 */
object UnsafeXML {
  def apply(
      secureProcessing: Boolean = true,
      loadExternalDTD: Boolean = false,
      disallowDoctypeDecl: Boolean = true,
      externalParameterEntities: Boolean = false,
      externalGeneralEntities: Boolean = false,
      resolveDTDUris: Boolean = false,
      xIncludeAware: Boolean = false,
      namespaceAware: Boolean = false): XMLLoader[Elem] = new XMLLoader[Elem] {

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
  }
}
