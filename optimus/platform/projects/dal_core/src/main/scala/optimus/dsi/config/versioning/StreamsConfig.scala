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
private[optimus] final class StreamsConfig private[config] (
    val topicName: String,
    val partitions: Int,
    val cleanupPolicy: String,
    val retentionPeriodDays: Int,
    val config: Map[String, String])
    extends KafkaConfiguration

object StreamsConfigConstants {
  val defaultPartitions: Int = 1
  val defaultRetentionInDays: Int = 21
  val defaultCleanupPolicy: String = TopicConfig.CLEANUP_POLICY_DELETE
  private[config] val minInSyncReplicas = 3
  private[dsi] val replicationFactor: Short = 3
}
