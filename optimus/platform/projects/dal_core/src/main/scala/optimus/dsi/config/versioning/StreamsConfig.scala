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
package optimus.dsi.config.versioning

import optimus.dsi.config.KafkaConfiguration
import org.apache.kafka.common.config.TopicConfig

import scala.jdk.CollectionConverters._
import scala.util.matching.Regex

/**
 * This is a duplicate class to optimus.dsi.config.StreamsConfig in dsi_session
 *
 * We need this class since the other class is the actual @stored @entity but needs to be used in dal_core
 * where @stored @entity is not supported since it does not depend on entityplugin which provides
 * the necessary compile time transformations. Therefore, we make this duplicate class to be used here and
 * convert between the two classes when needed.
 *
 * These classes should always be kept IN SYNC!!!
 */
private[optimus] final case class StreamsConfig(
    topicName: String,
    partitions: Int,
    cleanupPolicy: String,
    retentionPeriodDays: Int,
    deleteRetentionPeriodDays: Int,
    config: Map[String, String])
    extends KafkaConfiguration {
  override def toString: String = {
    val configStr = config
      .map { case (k, v) => s"$k${StreamsConfig.extraConfigKeyValueSeparator}$v" }
      .mkString(StreamsConfig.extraConfigSeparator)
    StreamsConfig.formatString.format(
      topicName,
      partitions,
      cleanupPolicy,
      retentionPeriodDays,
      deleteRetentionPeriodDays,
      configStr
    )
  }
}

private[optimus] object StreamsConfig {
  def apply(topic: String): StreamsConfig =
    StreamsConfig(
      topic,
      StreamsConfigConstants.defaultPartitions,
      StreamsConfigConstants.defaultCleanupPolicy,
      StreamsConfigConstants.defaultRetentionInDays,
      StreamsConfigConstants.defaultDeleteRetentionInDays,
      Map.empty[String, String]
    )

  // ensure these are consistent with one another
  private val patternString: String =
    """StreamsConfig\(topicName=(.*?),partitions=(.*?),cleanupPolicy=(.*?),retentionPeriodDays=(.*?),deleteRetentionPeriodDays=(.*?),config=Map\((.*?)\)\)"""
  val formatString: String =
    "StreamsConfig(topicName=%s,partitions=%d,cleanupPolicy=%s,retentionPeriodDays=%d,deleteRetentionPeriodDays=%d,config=Map(%s))"
  val pattern: Regex = patternString.r

  private[config] def misconfiguredException(s: String): IllegalArgumentException =
    new IllegalArgumentException(s"StreamsConfig misconfigured: $s. Expected format: $formatString")

  def fromString(s: String): StreamsConfig = s match {
    case pattern(topicName, partitions, cleanupPolicy, retentionPeriodDays, deleteRetentionPeriodDays, configStr) =>
      val config: Map[String, String] =
        if (configStr.trim.isEmpty) Map.empty
        else
          configStr
            .split(extraConfigSeparator)
            .map { pair =>
              try {
                val Array(key, value) = pair.split(extraConfigKeyValueSeparator, 2)
                key -> value
              } catch {
                case _: MatchError => throw misconfiguredException(s)
              }
            }
            .toMap

      StreamsConfig(
        topicName = topicName,
        partitions = partitions.toInt,
        cleanupPolicy = cleanupPolicy,
        retentionPeriodDays = retentionPeriodDays.toInt,
        deleteRetentionPeriodDays = deleteRetentionPeriodDays.toInt,
        config = config
      )
    case _ =>
      throw misconfiguredException(s)
  }

  // use semicolon as the common separator for a string containing a collection of StreamsConfig
  val configSeparator: String = ";"
  val extraConfigKeyValueSeparator: String = "->"
  val extraConfigSeparator: String = ","
  def getStringList(configs: Set[StreamsConfig]): String = configs.mkString(configSeparator)
}

object StreamsConfigConstants {
  val defaultPartitions: Int = 1
  val defaultDeleteRetentionInDays: Int = 21
  val defaultRetentionInDays: Int = 21
  val defaultCleanupPolicy: String = TopicConfig.CLEANUP_POLICY_DELETE
  private[config] val minInSyncReplicas = 3
  private[dsi] val replicationFactor: Short = 3
}
