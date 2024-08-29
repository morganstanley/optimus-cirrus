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
package optimus.platform.dal.config.streams

import optimus.dsi.config.versioning.StreamsConfig
import optimus.platform.dal.config.DalEnv
import optimus.platform.dal.config.DalFeatureKafkaLookup
import optimus.platform.dal.config.DalKafkaConfig
import optimus.platform.dal.config.HostPort
import optimus.platform.dal.config.KafkaFeature
import optimus.platform.runtime.XmlConfigurationValidator
import org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG
import org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG
import org.apache.kafka.common.config.SaslConfigs.SASL_KERBEROS_SERVICE_NAME

trait KafkaConfigBase {
  val securityProtocol: String
  val servicePrincipal: String
  val bootstrapServers: Seq[HostPort]
  def connectionMap: Map[String, String] = {
    Map[String, String](BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers.map(_.hostport).mkString(",")) ++
      Map(SECURITY_PROTOCOL_CONFIG -> securityProtocol) ++
      Map(SASL_KERBEROS_SERVICE_NAME -> servicePrincipal)
  }
}

trait ClusterConfig

final case class StreamsClusterKafkaConfig(
    securityProtocol: String,
    servicePrincipal: String,
    bootstrapServers: Seq[HostPort])
    extends KafkaConfigBase

object StreamsClusterKafkaConfig {
  def apply(securityProtocol: String, servicePrincipal: String, bootstrapServers: String): StreamsClusterKafkaConfig =
    StreamsClusterKafkaConfig(securityProtocol, servicePrincipal, bootstrapServers.split(",").map(HostPort(_)))
}

sealed class StreamsClusterConfig(
    val kafkaConfig: StreamsClusterKafkaConfig,
    @volatile private var _streamableClassConfig: Option[Set[StreamsConfig]])
    extends ClusterConfig {
  def streamableClassConfig: Set[StreamsConfig] = {
    require(_streamableClassConfig.nonEmpty, s"streamableClasses must be set before accessing")
    _streamableClassConfig.get
  }
  def streamableClassNames: Set[String] = streamableClassConfig.map(_.className)

  // should be called exactly once after MessagesServer env initialization in runServer
  private[optimus] def setStreamableClassConfig(config: Set[StreamsConfig]): Unit =
    synchronized { _streamableClassConfig = Some(config) }
}

case object EmptyStreamsClusterConfig
    extends StreamsClusterConfig(StreamsClusterKafkaConfig("", "", Seq.empty[HostPort]), Some(Set.empty[StreamsConfig]))

class StreamsClusterConfigGenerator(env: DalEnv)
    extends DalKafkaConfig(DalFeatureKafkaLookup(env, KafkaFeature.Streams)) {

  override protected def validate(xmlString: String): Boolean =
    XmlStreamsConfigurationValidator.validate(xmlString)

  // streamableClasses set by optimus.dsi.messages.server.MessagesServer#runServer after intializing Evaluation Context
  val streamsClusterConfig: StreamsClusterConfig =
    new StreamsClusterConfig(
      StreamsClusterKafkaConfig(securityProtocol, servicePrincipal, seedURI.split(",").map(uri => HostPort(uri)).toSeq),
      None)
}

object XmlStreamsConfigurationValidator extends XmlConfigurationValidator {
  override protected val xsdName: String = "streams"
  override protected def validationDir: String = { "dal/config" }
}
