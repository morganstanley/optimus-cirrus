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

import optimus.platform.runtime.XmlConfigurationValidator
import org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG
import org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG
import org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG
import org.apache.kafka.clients.producer.ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG
import org.apache.kafka.clients.producer.ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION
import org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG
import org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG
import org.apache.kafka.clients.producer.ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.CLIENT_ID_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.ISOLATION_LEVEL_CONFIG
import org.apache.kafka.clients.producer.ProducerConfig.TRANSACTIONAL_ID_CONFIG
import org.apache.kafka.common.config.SaslConfigs.SASL_KERBEROS_SERVICE_NAME

import java.util.Properties

class DalUowKafkaConfig(env: DalEnv) extends DalKafkaConfig(DalFeatureKafkaLookup(env, KafkaFeature.Uow)) {
  private val zookeeperPropXml = "zookeeper"
  private val zkPathPropXml = "path"
  private val replicationFactorPropXml = "replication_factor"

  private val uowPrefix = "optimus.dal.uow.kafka."
  private val valueSerializer = "org.apache.kafka.common.serialization.ByteArraySerializer"
  private val valueDeserializer = "org.apache.kafka.common.serialization.ByteArrayDeserializer"

  val zkPath: String = {
    kafkaConfig.getString(s"$zookeeperPropXml.$zkPathPropXml") match {
      case Some(zkPath) => zkPath
      case None         => throw new IllegalArgumentException(s"Missing xml property $zookeeperPropXml.$zkPathPropXml")
    }
  }

  val replicationFactor: Int = {
    kafkaConfig.getInt(s"$zookeeperPropXml.$replicationFactorPropXml") match {
      case Some(replicationFactor) => replicationFactor
      case None =>
        throw new IllegalArgumentException(s"Missing xml property $zookeeperPropXml.$replicationFactorPropXml")
    }
  }

  val kafkaPrincipal: String = servicePrincipal

  private def connectionProperties(includeServiceName: Boolean = true): Properties = {
    val prop = new Properties()
    kafkaBaseProperties.forEach((k, v) => prop.put(k, v))
    prop.put(BOOTSTRAP_SERVERS_CONFIG, seedURI)
    prop.put(SECURITY_PROTOCOL_CONFIG, securityProtocol)
    if (includeServiceName && isKerberized) {
      prop.put(SASL_KERBEROS_SERVICE_NAME, servicePrincipal)
    }
    prop
  }

  def producerProperties(withKey: Boolean, transactionId: Option[String]): Properties = {
    val prop = connectionProperties(includeServiceName = true)
    prop.put(ACKS_CONFIG, getProperty(ACKS_CONFIG, "all"))
    prop.put(ENABLE_IDEMPOTENCE_CONFIG, getProperty(ENABLE_IDEMPOTENCE_CONFIG, "true"))
    prop.put(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, getProperty(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1"))
    if (withKey) {
      require(transactionId.isDefined, "transaction id must be set for producers with key")
      prop.put(
        KEY_SERIALIZER_CLASS_CONFIG,
        getProperty(KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer"))
      prop.put(
        TRANSACTIONAL_ID_CONFIG,
        transactionId.get
      )
    } else {
      require(transactionId.isEmpty, "if key is not present transaction id must not be set")
      prop.put(
        KEY_SERIALIZER_CLASS_CONFIG,
        getProperty(KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.VoidSerializer"))
    }
    prop.put(VALUE_SERIALIZER_CLASS_CONFIG, getProperty(VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer))
    prop.put(DELIVERY_TIMEOUT_MS_CONFIG, getProperty(DELIVERY_TIMEOUT_MS_CONFIG, "2147483647"))
    prop
  }

  def consumerPropertiesForMonitoring(clientId: Option[String], withKey: Boolean): Properties = {
    val prop = connectionProperties(includeServiceName = true)
    if (withKey) {
      prop.put(
        KEY_DESERIALIZER_CLASS_CONFIG,
        getProperty(KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer"))
      prop.put(ISOLATION_LEVEL_CONFIG, getProperty(ISOLATION_LEVEL_CONFIG, "read_committed"))
    } else {
      prop.put(
        KEY_DESERIALIZER_CLASS_CONFIG,
        getProperty(KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.VoidDeserializer"))
    }
    prop.put(VALUE_DESERIALIZER_CLASS_CONFIG, getProperty(VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer))
    clientId match {
      case Some(clientId) => prop.put(CLIENT_ID_CONFIG, clientId)
      case None           =>
    }
    prop
  }

  def consumerProperties(groupId: String, withKey: Boolean): Properties =
    consumerProperties(groupId, None, withKey = withKey)

  def consumerProperties(
      groupId: String,
      clientId: Option[String],
      maxPollRecords: String = "1",
      withKey: Boolean): Properties = {
    val prop = consumerPropertiesForMonitoring(clientId, withKey)
    prop.put(GROUP_ID_CONFIG, groupId)
    prop.put(AUTO_OFFSET_RESET_CONFIG, getProperty(AUTO_OFFSET_RESET_CONFIG, "latest"))
    prop.put(ENABLE_AUTO_COMMIT_CONFIG, getProperty(ENABLE_AUTO_COMMIT_CONFIG, "false"))
    prop.put(MAX_POLL_RECORDS_CONFIG, maxPollRecords)
    prop.put(DEFAULT_API_TIMEOUT_MS_CONFIG, getProperty(DEFAULT_API_TIMEOUT_MS_CONFIG, "60000"))
    prop
  }

  def adminClientProperties: Properties = {
    connectionProperties()
  }

  final protected override def validate(xmlString: String): Boolean = {
    XmlDalUowConfigValidator.validate(xmlString)
  }

  private def getProperty(config: String, defaultValue: String): String = {
    System.getProperty(s"$uowPrefix$config", defaultValue)
  }
}

object XmlDalUowConfigValidator extends XmlConfigurationValidator {
  override protected val xsdName: String = "uow"
  override protected def validationDir: String = { "dal/config" }
}
