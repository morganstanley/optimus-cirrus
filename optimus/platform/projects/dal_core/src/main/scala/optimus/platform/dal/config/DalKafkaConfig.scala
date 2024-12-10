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
package optimus.platform.dal.config

import msjava.hdom.Document
import msjava.hdom.Element
import msjava.hdom.output.XMLOutputter
import optimus.graph.DiagnosticSettings
import optimus.platform.IO._
import optimus.platform.runtime.XmlBasedConfiguration
import optimus.platform.runtime.ZkOpsTimer
import optimus.platform.runtime.ZkUtils
import optimus.platform.runtime.ZkXmlConfigurationLoader
import optimus.platform.utils.KerberosAuthentication
import org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG
import org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG
import org.apache.kafka.common.config.SaslConfigs.SASL_KERBEROS_SERVICE_NAME
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.xml.sax.SAXException

import java.io._
import java.util.Properties
import scala.jdk.CollectionConverters._

abstract class DalKafkaConfig(lookup: DalFeatureKafkaLookup) extends KerberosAuthentication {
  final protected val log: Logger = LoggerFactory.getLogger(this.getClass)

  final protected val kafkaPropXml = "kafka"
  final protected val kafkaConfig: XmlBasedConfiguration = initConfig
  final protected val kafkaBaseProperties: Properties = {
    val properties = new Properties()
    val kafkaFeatureString = lookup.featurePath.toString.substring(1)
    // e.g. optimus.platform.dal.config.DalKafkaConfig.uow
    val configProperty = s"optimus.platform.dal.config.DalKafkaConfig.$kafkaFeatureString"
    val configFile = DiagnosticSettings.getStringProperty(configProperty, "")
    if (configFile.nonEmpty && new File(configFile).exists()) {
      log.info(s"Loading DalKafkaConfig.$kafkaFeatureString properties from: $configFile")
      using(new FileInputStream(configFile)) { in => properties.load(in) }
    }
    properties
  }

  final protected val seedURI: String = getServersUri(kafkaConfig)
  final protected val securityProtocol: String = getXmlProperty(kafkaConfig, SECURITY_PROTOCOL_CONFIG)
  final protected val servicePrincipal: String = getXmlProperty(kafkaConfig, SASL_KERBEROS_SERVICE_NAME)

  override def isKerberized: Boolean = securityProtocol.startsWith("SASL")

  setupKerberosCredentials()

  protected def validate(xmlString: String): Boolean

  private def getServersUri(kafkaXmlConfig: XmlBasedConfiguration): String = ??? /* {
    val rootNode = kafkaXmlConfig.configs.head.getRootElement
    val childrenList = rootNode.getChildren(kafkaPropXml).get(0).getChildren().asScala.toList
    val servers = getServersFromConfig(childrenList)
    if (servers.isEmpty) {
      throw new IllegalArgumentException("Could not find servers Uri for instance for kafka ")
    }
    servers.head.getValueAsString
  } */

  private def getServersFromConfig(childrenList: List[Element]): List[Element] = ??? /* {
    val servers = childrenList.filter(child => child.getName == kafkaPropToXml(BOOTSTRAP_SERVERS_CONFIG))
    lookup.instance match {
      case Some(instance) => servers.filter(child => child.getAttributeValue("instance") == instance)
      case None           => servers
    }
  } */

  private def getProperty(config: XmlBasedConfiguration, path: String, default: => Option[String]): String =
    config.getString(path).orElse(default).getOrElse(throw new IllegalArgumentException(s"Missing property $path"))

  private def getXmlBasedConfiguration(configXmlDocument: Document): XmlBasedConfiguration = {
    new XmlBasedConfiguration {
      override val configs: Seq[Document] = Seq(configXmlDocument)
    }
  }

  private def initConfig: XmlBasedConfiguration = {
    val xmlDoc = getDocumentFromZk(lookup.connectionString)
    if (!validate(getStringFromDocument(xmlDoc)))
      throw new SAXException(s"Unable to Parse config Xml for Dal Kafka Config with Xsd Schema")
    getXmlBasedConfiguration(xmlDoc)
  }

  protected def getDocumentFromZk(connectionString: String): Document = {
    ZkXmlConfigurationLoader.readConfigDoc(connectionString, ZkUtils.getZkEnv(lookup.env), ZkOpsTimer.getDefault)
  }

  private def getStringFromDocument(configXmlDocument: Document): String = {
    try {
      val xmlOutputter: XMLOutputter = new XMLOutputter()
      val stringWriter: StringWriter = new StringWriter()
      // xmlOutputter.output(configXmlDocument, stringWriter)
      stringWriter.toString
    } catch {
      case ex: Exception => throw new Exception("Could not parse kafka Config xml", ex)
    }
  }

  private def getXmlProperty(config: XmlBasedConfiguration, tag: String): String = {
    getProperty(config, s"$kafkaPropXml.${kafkaPropToXml(tag)}", None)
  }

  final protected def kafkaPropToXml(string: String): String = {
    string.replace('.', '_')
  }
}
