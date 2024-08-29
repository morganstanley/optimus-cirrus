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
import msjava.hdom.output.XMLOutputter
import optimus.platform.runtime.XmlBasedConfiguration
import optimus.platform.runtime.ZkOpsTimer
import optimus.platform.runtime.ZkUtils
import optimus.platform.runtime.ZkXmlConfigurationLoader
import optimus.scalacompat.collection._
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.xml.sax.SAXException

import java.io.StringWriter
import scala.jdk.CollectionConverters._

/**
 * This class does not extend abstract DalKafkaConfig in order to to decentralise the control as the config is shared
 * outside of optimus platform. Also, the config is tailored as per the requirement to support multiple region as comma
 * separated values for kafka bootstrap servers discovery.
 *
 * @param lookup
 */
class DalKafkaSourceConfig(lookup: DalFeatureKafkaLookup) {
  final protected val log: Logger = LoggerFactory.getLogger(this.getClass)
  private val comma_separator = ","
  private val kafkaPropXml = "kafka"
  private val securityProtocolPropXml = "security_protocol"
  private val saslKerberosServiceNamePropXml = "sasl_kerberos_service_name"
  private val bootstrapServersPropXml = "bootstrap_servers"
  private val bootstrapServersPropSupportedRegionsAttributeXml = "supported_regions"
  private val kafkaStreamPropXml = "kafka_stream"
  private val streamPropIdAttributeXml = "id"
  private val streamPropRegionAttributeXml = "region"
  private val streamPropEntityAttributeXml = "entity"
  private val streamPropTopicAttributeXml = "topic"
  private val streamPropSoapServerAttributeXml = "reconSoapServer"
  private val streamPropParallelProcessingBatchSizeAttributeXml = "parallelProcessingBatchSize"
  private val zoneId = "zoneId"

  final protected val kafkaConfig: XmlBasedConfiguration = initConfig

  private def initConfig: XmlBasedConfiguration = {
    val xmlDoc = getDocumentFromZk(lookup.connectionString)
    validate(xmlDoc)
    getXmlBasedConfiguration(xmlDoc)
  }

  private def getServersTypes(configXmlDocument: Document): List[ServersType] = {
    val rootNode = configXmlDocument.getRootElement
    val kafkaProps = rootNode.getChildren(kafkaPropXml).get(0)
    val bootstrapServers = kafkaProps.getChildren(bootstrapServersPropXml).asScala
    val serversTypeList = bootstrapServers
      .map(el => {
        ServersType(el.getAttributeValue(bootstrapServersPropSupportedRegionsAttributeXml), el.getValueAsString)
      })
      .toList
    serversTypeList
  }

  final def getKafkaStreamsWithProperties(
      configXmlDocument: Document = getDocumentFromZk(lookup.connectionString)
  ): Map[KafkaStream, KafkaProp] = {
    val rootNode = configXmlDocument.getRootElement
    val kafkaProps = rootNode.getChildren(kafkaPropXml).get(0)

    getKafkaStreams(configXmlDocument).map { stream =>
      stream ->
        KafkaProp(
          securityProtocol = kafkaProps.getChildText(securityProtocolPropXml),
          saslKerberosServiceName = kafkaProps.getChildText(saslKerberosServiceNamePropXml),
          bootstrapServers = getServersTypes(configXmlDocument).filter(i => i.regions.contains(stream.region)).head
        )
    }.toMap
  }

  private final def getKafkaStreams(
      configXmlDocument: Document = getDocumentFromZk(lookup.connectionString)
  ): List[KafkaStream] = {
    val rootNode = configXmlDocument.getRootElement
    val streamList = rootNode.getChildren(kafkaStreamPropXml).get(0).getChildren().asScala
    val kafkaStreamList = streamList
      .map(el =>
        KafkaStream(
          el.getAttributeValue(streamPropIdAttributeXml),
          el.getAttributeValue(streamPropRegionAttributeXml),
          el.getAttributeValue(streamPropEntityAttributeXml),
          el.getAttributeValue(streamPropTopicAttributeXml),
          el.getAttributeValue(streamPropSoapServerAttributeXml),
          Option(el.getAttribute(streamPropParallelProcessingBatchSizeAttributeXml)).map(_.getValueAsInt).getOrElse(1),
          Option(el.getAttributeValue(zoneId)).map(DalZoneId(_))
        ))
      .toList
    kafkaStreamList
  }

  final protected def validate(configXmlDocument: Document): Unit = {
    if (!XmlKafkaSourceConfigValidator.validate(getStringFromDocument(configXmlDocument))) {
      val errorMessage = s"Unable to parse config XML for DAL Kafka config with XSD schema";
      log.error(errorMessage)
      throw new SAXException(errorMessage)
    } else {
      val regionsList = getServersTypes(configXmlDocument)
        .flatMap(i => i.regions.split(comma_separator))
        .groupBy(identity)
        .mapValuesNow(_.size)
      val duplicateRegions = regionsList.filter(r => r._2 > 1)
      val kafkaStreamsRegions = getKafkaStreams(configXmlDocument).map(_.region).distinct
      val missingRegions = kafkaStreamsRegions.diff(regionsList.keys.toSeq)
      if (missingRegions.nonEmpty) {
        val errorMessage =
          s"All kafka stream regions not present in DAL Kafka config XML. Required region(s): " + missingRegions
            .mkString(comma_separator)
        log.error(errorMessage)
        throw new SAXException(errorMessage)
      } else if (duplicateRegions.nonEmpty) {
        val errorMessage =
          s"Duplicate regions present in DAL Kafka config XML. Duplicate region(s): " +
            duplicateRegions.mkString(comma_separator)
        log.error(errorMessage)
        throw new SAXException(errorMessage)
      }
    }
  }

  protected def getDocumentFromZk(connectionString: String): Document = {
    ZkXmlConfigurationLoader.readConfigDoc(connectionString, ZkUtils.getZkEnv(lookup.env), ZkOpsTimer.getDefault)
  }

  private def getXmlBasedConfiguration(configXmlDocument: Document): XmlBasedConfiguration = {
    new XmlBasedConfiguration {
      override val configs: Seq[Document] = Seq(configXmlDocument)
    }
  }

  private def getStringFromDocument(configXmlDocument: Document): String = {
    try {
      val stringWriter: StringWriter = new StringWriter()
      new XMLOutputter().output(configXmlDocument, stringWriter)
      stringWriter.toString
    } catch {
      case ex: Exception => throw new Exception("Could not parse kafka config XML", ex)
    }
  }
}

private[optimus] final case class KafkaStream(
    streamId: String,
    region: String,
    entity: String,
    topic: String,
    reconSoapServer: String,
    parallelProcessingBatchSize: Int,
    zoneId: Option[DalZoneId]
)

private[optimus] final case class KafkaProp(
    securityProtocol: String,
    saslKerberosServiceName: String,
    bootstrapServers: ServersType
)

private[optimus] final case class ServersType(val regions: String, val bootstrapServers: String)
