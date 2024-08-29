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
import java.nio.charset.StandardCharsets
import javax.xml.XMLConstants
import javax.xml.transform.sax.SAXSource
import javax.xml.validation.Schema
import javax.xml.validation.SchemaFactory
import javax.xml.validation.Validator

import msjava.slf4jutils.scalalog.Logger
import msjava.slf4jutils.scalalog.getLogger
import org.apache.commons.io.IOUtils
import org.xml.sax.SAXException
import org.xml.sax.SAXParseException

import scala.xml.InputSource

object XmlConfigurationValidator {
  private val validationDir: String = "brokerconfigv2validation"
}

trait XmlConfigurationValidator {
  protected val log: Logger = getLogger(this)
  protected val xsdName: String

  protected def validationDir: String = XmlConfigurationValidator.validationDir
  protected lazy val xsd: String = {
    val resource: String = s"${validationDir}/${xsdName}.xsd"
    log.info(s"Getting xsd resource $resource")
    IOUtils.toString(getClass.getClassLoader.getResourceAsStream(resource), StandardCharsets.UTF_8)
  }

  private[optimus] def validate(xml: String): Boolean = {
    val factory: SchemaFactory =
      SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI)
    val schemaSource: SAXSource = new SAXSource(new InputSource(new StringReader(xsd)))
    val schema: Schema = factory.newSchema(schemaSource)
    val validator: Validator = schema.newValidator()
    val xmlSource: SAXSource = new SAXSource(new InputSource(new StringReader(xml)))
    try {
      validator.validate(xmlSource)
    } catch {
      case e: SAXParseException =>
        log.error(
          s"Failed to validate the specified XML at line ${e.getLineNumber}, column ${e.getColumnNumber} due to: ${e.getMessage}")
        return false
      case e: SAXException =>
        log.error(s"Failed to validate the specified XML due to: ${e.getMessage}")
        return false
    }
    true
  }
}
