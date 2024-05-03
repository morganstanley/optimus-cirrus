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
package optimus.graph.diagnostics.kafka
import msjava.zkapi.internal.ZkaData
import optimus.platform.util.Log
import org.apache.kafka.clients.CommonClientConfigs

final case class KafkaConnectionConfig(topic: String, bootstrapServers: String) {
  def asProps: Map[String, Object] = Map[String, Object](
    CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers
  )
}

object KafkaSamplingProfiling extends Log {
  private[optimus] def createKafkaSettings(zkData: ZkaData): Option[KafkaConnectionConfig] = {
    def stringOr(o: Object): Option[String] = o match {
      case s: String => Some(s)
      case null      => None
      case _ =>
        log.error(s"Expected a String but got ${o.toString}")
        None
    }

    for {
      topic <- stringOr(zkData.get("topic"))
      kafkaServers <- stringOr(zkData.get("bootstrap.servers"))
    } yield KafkaConnectionConfig(topic, kafkaServers)
  }

  private[optimus] def allKafkaSettings(
      zkKafkaSettings: Option[KafkaConnectionConfig],
      extraKafkaSettings: Map[String, AnyRef]): Map[String, AnyRef] = {
    val zkConsumerProps = zkKafkaSettings.map(_.asProps)
    // keys from extraKafkaSettings will override those from zkConsumerProps, and that is desired
    zkConsumerProps.getOrElse(Map.empty) ++ extraKafkaSettings
  }
}
