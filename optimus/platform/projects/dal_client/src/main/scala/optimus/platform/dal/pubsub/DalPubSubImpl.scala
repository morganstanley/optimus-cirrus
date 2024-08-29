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

import java.time.Instant

import optimus.dsi.partitioning.Partition
import optimus.dsi.pubsub.Subscription
import optimus.platform.RuntimeEnvironment
import optimus.platform.TimeInterval
import optimus.platform.dal.DALPubSub
import optimus.platform.dal.DALPubSub._

private[optimus] class DalPubSubImpl(
    val resolver: PubSubOperations,
    env: RuntimeEnvironment
) extends DALPubSub {

  override def createTickingNotificationStream(
      streamId: ClientStreamId,
      subscriptions: Set[Subscription],
      cb: NotificationStreamCallback,
      startTime: Option[Instant],
      endTime: Instant,
      partition: Partition
  ): NotificationStream = {
    startTime.foreach(st => require(endTime.isAfter(st), "End time must be after start time."))
    resolver.createPubSubStream(
      streamId = streamId,
      subscriptions = subscriptions,
      cb = cb,
      startTime = startTime,
      endTime = if (endTime == TimeInterval.Infinity) None else Some(endTime),
      partition = partition,
      env = env
    )
  }
}
