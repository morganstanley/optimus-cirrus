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
    partitions: Int = StreamsConfigConstants.defaultPartitions,
    cleanupPolicy: String = StreamsConfigConstants.defaultCleanupPolicy,
    retentionPeriodDays: Int = StreamsConfigConstants.defaultRetentionInDays,
    deleteRetentionPeriodDays: Int = StreamsConfigConstants.defaultDeleteRetentionInDays,
    config: Map[String, String] = Map.empty)
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

  private val field = "field"
  private val value = "value"
  private val topicNameStr = "topicName"
  private val partitionsStr = "partitions"
  private val cleanupPolicyStr = "cleanupPolicy"
  private val retentionPeriodDaysStr = "retentionPeriodDays"
  private val deleteRetentionPeriodDaysStr = "deleteRetentionPeriodDays"
  private val configStr = "config"

  val formatString: String =
    s"StreamsConfig($topicNameStr=%s,$partitionsStr=%d,$cleanupPolicyStr=\"%s\",$retentionPeriodDaysStr=%d,$deleteRetentionPeriodDaysStr=%d,$configStr=Map(%s))"

  private[config] def misconfiguredException(s: String): IllegalArgumentException =
    new IllegalArgumentException(s"StreamsConfig misconfigured: $s. Expected format: $formatString")

  private val patternString: String =
    s"""StreamsConfig\\((?:$topicNameStr=[^,\\)]+)(?:,(?:$partitionsStr=\\d+|$cleanupPolicyStr="[^"]+"|$retentionPeriodDaysStr=\\d+|$deleteRetentionPeriodDaysStr=\\d+|$configStr=Map\\([^)]*\\)))*\\)"""

  private val fieldPattern: String =
    s"""(?:^StreamsConfig\\(|,)(?<$field>$topicNameStr|$partitionsStr|$cleanupPolicyStr|$retentionPeriodDaysStr|$deleteRetentionPeriodDaysStr|$configStr)=(?<$value>(?:"[^"]*"|Map\\([^)]*\\)|[^,)]+))"""

  private val pattern: Regex = patternString.r

  def fromString(s: String): StreamsConfig = {
    if (!pattern.matches(s)) throw misconfiguredException(s)

    val fields = fieldPattern.r.findAllMatchIn(s).map(m => m.group(field) -> m.group(value)).toMap

    val topicName = fields.getOrElse(topicNameStr, throw misconfiguredException(s))
    val partitions = fields.get(partitionsStr).map(_.toInt).getOrElse(StreamsConfigConstants.defaultPartitions)
    val cleanupPolicy = fields
      .get(cleanupPolicyStr)
      .map(_.stripPrefix("\"").stripSuffix("\""))
      .getOrElse(StreamsConfigConstants.defaultCleanupPolicy)
    val retentionPeriodDays = fields
      .get(retentionPeriodDaysStr)
      .map(_.toInt)
      .getOrElse(StreamsConfigConstants.defaultRetentionInDays)
    val deleteRetentionPeriodDays = fields
      .get(deleteRetentionPeriodDaysStr)
      .map(_.toInt)
      .getOrElse(StreamsConfigConstants.defaultDeleteRetentionInDays)
    val config = fields
      .get(configStr)
      .map { c =>
        c.stripPrefix("Map(")
          .stripSuffix(")")
          .split(extraConfigSeparator)
          .filter(_.nonEmpty)
          .map {
            case pair if pair.contains(StreamsConfig.extraConfigKeyValueSeparator) =>
              val Array(key, value) = pair.split(StreamsConfig.extraConfigKeyValueSeparator, 2)
              key.trim -> value.trim
            case _ =>
              throw StreamsConfig.misconfiguredException(s)
          }
          .toMap
      }
      .getOrElse(Map.empty)

    StreamsConfig(
      topicName = topicName,
      partitions = partitions,
      cleanupPolicy = cleanupPolicy,
      retentionPeriodDays = retentionPeriodDays,
      deleteRetentionPeriodDays = deleteRetentionPeriodDays,
      config = config
    )
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
