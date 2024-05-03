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
import optimus.core.utils.ZkClient
import optimus.graph.diagnostics.kafka.KafkaSamplingProfiling.allKafkaSettings
import optimus.graph.diagnostics.zookeeper.SProfilingZKClient
import optimus.platform.util.Log
import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringSerializer

import scala.jdk.CollectionConverters._

final case class SamplingProfilingKafkaConfig(
    kafkaStackProducer: KafkaProducer[String, Array[Byte]],
    // TODO (OPTIMUS-62591): Add kafka breadcrumbs producer to sampling profiling
    kafkaSplunkProducer: KafkaProducer[String, Array[Byte]],
    topic: String)

object KafkaSPProducer extends Log {
  val defaultTopic = "sprofiling"

  private val props = Map[String, Object](
    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer],
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[ByteArraySerializer]
  )

  val messageCallback: Callback = (record: RecordMetadata, exception: Exception) => {
    import record._
    log.info(s"Message sent. Topic:$topic Partition: $partition Offset: $offset")
    if (exception != null) log.error(s"Encountered exception: ${exception.getMessage}")
  }

  def initializeProducer(): Option[SamplingProfilingKafkaConfig] = {
    log.info("Creating Kafka publisher for sampling profiling")
    // initialize zookeeper client
    // env here will have to change (following the crumbs kafka)
    val zkClient = ZkClient.initializeClient("dev")

    zkClient match {
      case Some(zclient) =>
        // parse and get kafka config data from zk
        val zkKafkaData = SProfilingZKClient.parseZkData(zclient)
        zkKafkaData.fold(Option.empty[SamplingProfilingKafkaConfig]) { zkData =>
          val kafkaSettings = KafkaSamplingProfiling.createKafkaSettings(zkData)

          // combine all kafka producer settings
          val producerSettings = allKafkaSettings(kafkaSettings, props)
          try {
            val stackProducer = new KafkaProducer[String, Array[Byte]](producerSettings.asJava)
            val topic = kafkaSettings.map(_.topic).getOrElse(defaultTopic)
            // change the null here when integrate with crumbs kafka producer
            Some(SamplingProfilingKafkaConfig(stackProducer, null, topic))
          } catch {
            case e: Exception =>
              log.error(s"Could not construct kafka producer for sampling prof", e.getMessage)
              None
          }
        }
      case _ =>
        log.error(s"No zookeeper client initialized!")
        None
    }
  }
}
