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
package optimus.platform.dal.pubsub

import optimus.dsi.partitioning.Partition
import optimus.dsi.partitioning.PartitionMap
import optimus.dsi.pubsub.Subscription
import optimus.platform.RuntimeEnvironment
import optimus.platform.TimeInterval
import optimus.platform.ValidTimeInterval
import optimus.platform.dal.DALPubSub
import optimus.platform.dal.DALPubSub.ClientStreamId
import optimus.platform.dal.DALPubSub.NotificationStream
import optimus.platform.dal.DSIResolver
import optimus.platform.dal.EntityResolverReadImpl
import optimus.platform.dal.PartitionMapAPI
import optimus.platform.dsi.bitemporal.CreatePubSubStream

import java.time.Instant
import java.util.UUID

trait PubSubOperations { this: DSIResolver with EntityResolverReadImpl =>

  private def createCommand(
      streamId: String,
      subs: Seq[Subscription],
      startTime: Option[Instant],
      endTime: Option[Instant]
  ): CreatePubSubStream =
    CreatePubSubStream(streamId, subs, startTime, endTime, Some(ValidTimeInterval.from(TimeInterval.Infinity)))

  def createPubSubStream(
      streamId: ClientStreamId,
      subscriptions: Set[Subscription],
      cb: DALPubSub.NotificationStreamCallback,
      startTime: Option[Instant],
      endTime: Option[Instant],
      partition: Partition,
      env: RuntimeEnvironment
  ): NotificationStream = {
    import PubSubHelper._
    implicit val partitionMap: PartitionMap = PartitionMapAPI.getPartitionMapFromRuntimeEnv(env)
    require(subscriptions.isEmpty || subscriptions.inferredPartition == partition)
    val newStreamId = UUID.randomUUID.toString
    val cmd = createCommand(newStreamId, subscriptions.toSeq, startTime, endTime)
    val pubSubRequest = PubSubClientRequest.CreateStream(cmd, streamId, cb, partition, env)
    executor.executePubSubRequest(dsi, pubSubRequest) match {
      case PubSubClientResponse.StreamCreationRequested(stream) => stream
      case PubSubClientResponse.VoidResponse =>
        throw new IllegalStateException("Expected a NotificationStream to be returned!")
    }
  }
}
