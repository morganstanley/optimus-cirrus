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
package optimus.dsi.pubsub

import java.time.Instant
import optimus.dsi.notification.NotificationEntry
import optimus.platform.dal.NotificationStream.SubscriptionIdType
import optimus.platform.storable.PersistentEntity

sealed trait NotificationMessage
sealed trait StreamEvent extends NotificationMessage
sealed trait GlobalEvent extends NotificationMessage
sealed trait DataMessage extends StreamEvent

/**
 * message is received when the requested stream creation is successful
 */
final case class StreamCreationSucceeded(tt: Instant, postChecks: Seq[SubscriptionIdType]) extends StreamEvent

/**
 * message is received when the requested stream is not successful
 */
final case class StreamCreationFailed(exception: Exception) extends StreamEvent

/**
 * message is received when stream creation is failed since subscriptions belonged to different partitions
 * i.e. the subscription that was requested is for a partition that is different to the partition associated with this
 * stream
 */
final case class MultiPartitionStreamCreationFailed(partitionedSubs: Map[String, Seq[Subscription.Id]])
    extends StreamEvent

/**
 * message is received when stream is changed successfully with update in subscriptions
 */
final case class SubscriptionChangeSucceeded(changeRequestId: Int, tt: Instant, postChecks: Seq[SubscriptionIdType])
    extends StreamEvent

/**
 * message is received when stream change is not successful
 */
final case class SubscriptionChangeFailed(changeRequestId: Int, tt: Instant) extends StreamEvent

/**
 * message is received when stream change request is failed since subscriptions belonged to different partitions
 * i.e. the subscription that was changed is for a partition that is different to the partition associated with this
 * stream
 */
final case class MultiPartitionSubscriptionChangeFailed(
    changeRequestId: Int,
    tt: Instant,
    partitionedSubs: Map[String, Seq[Subscription.Id]])
    extends StreamEvent

/**
 * message is received at the end of stream. Indicates that the stream is closed, and no more events will be published
 */
case object EndOfStreamMessage extends StreamEvent

/**
 * message is received when there is error in stream
 */
final case class StreamErrorMessage(th: Throwable) extends StreamEvent

/**
 * message is received on disconnection with stream. This is an informational message. The system will attempt
 * reconnection and if successful a StreamCreationSucceeded will be published, and if unsuccessful a
 * StreamCreationFailed
 */
final case class StreamDisconnectedMessage(th: Throwable) extends StreamEvent

/**
 * message is received at the start of state of the world event. An information message to indicate that a SOW has
 * started
 */
final case class SowStartMessage(tt: Instant) extends StreamEvent

/**
 * message is received at the end of state of the world event. An information message to indicate that a SOW has ended
 */
final case class SowEndMessage(tt: Instant) extends StreamEvent

/**
 * message is received at the start of historical catchup. An information message to indicate that we have started a
 * catchup
 */
final case class CatchupStartMessage(tt: Instant) extends StreamEvent

/**
 * message is received on completion of historical catchup. An information message to indicate that we have finished a
 * catchup
 */
final case class CatchupCompleteMessage(tt: Instant) extends StreamEvent

object DataNotificationMessage {
  def apply(
      tt: Instant,
      writeReqId: Option[String],
      data: Map[Subscription, Seq[NotificationEntry]]
  ): DataNotificationMessage = new DataNotificationMessage(tt, writeReqId, data)
  def unapply(dm: DataNotificationMessage): Some[(Instant, Map[Subscription.Id, Seq[NotificationEntry]])] = {
    Some((dm.tt, dm.data))
  }
}
final class DataNotificationMessage private (
    val tt: Instant,
    val writeReqId: Option[String],
    private[optimus] val fullData: Map[Subscription, Seq[NotificationEntry]]
) extends DataMessage {
  def data: Map[Subscription.Id, Seq[NotificationEntry]] = fullData map { case (k, v) => k.subId -> v }
  def needsClientSideFiltering: Boolean = fullData.exists { case (k, _) => k.hasClientSideFilter }
  override def equals(obj: Any): Boolean = obj match {
    case that: DataNotificationMessage => tt == that.tt && writeReqId == that.writeReqId && fullData == that.fullData
    case _                             => false
  }
  override def hashCode(): Int = 31 * tt.hashCode() + fullData.hashCode()
}

object SowNotificationMessage {
  def apply(tt: Instant, data: Map[Subscription, Seq[PersistentEntity]]): SowNotificationMessage = {
    new SowNotificationMessage(tt, data)
  }
  def unapply(dm: SowNotificationMessage): Some[(Instant, Map[Subscription.Id, Seq[PersistentEntity]])] = {
    Some((dm.tt, dm.data))
  }
}
final class SowNotificationMessage private (
    val tt: Instant,
    private[optimus] val fullData: Map[Subscription, Seq[PersistentEntity]]
) extends DataMessage {
  def data: Map[Subscription.Id, Seq[PersistentEntity]] = fullData.map { case (k, v) => k.subId -> v }
  def needsClientSideFiltering: Boolean = fullData.exists { case (k, _) => k.hasClientSideFilter }
  override def equals(obj: Any): Boolean = obj match {
    case that: SowNotificationMessage => tt == that.tt && fullData == that.fullData
    case _                            => false
  }
  override def hashCode(): Int = 31 * tt.hashCode() + fullData.hashCode()
}

final case class HeartbeatMessage(tt: Instant) extends DataMessage

/**
 * message signifies rep lag overlimit with lag time in milliseconds
 */
final case class ReplicationLagIncreaseEvent(lagTime: Long) extends GlobalEvent

/**
 * message signifies rep lag underlimit with lag time in milliseconds, this is received if rep lag has increased
 * previously
 */
final case class ReplicationLagDecreaseEvent(lagTime: Long) extends GlobalEvent

/**
 * message is received when the upstream lead writer or replication broker is changed
 */
case object UpstreamChangedEvent extends GlobalEvent

/**
 * message is received when there is disconnection with pubsub broker
 */
case object PubSubBrokerConnectEvent extends GlobalEvent

/**
 * message is received when connection is successful with with pubsub broker
 */
case object PubSubBrokerDisconnectEvent extends GlobalEvent

/**
 * message signifies delay in ticks for given entity types
 */
final case class PubSubDelayEvent(delayedTypes: Set[String]) extends GlobalEvent

/**
 * message signifies delay in ticks is over for given entity types
 */
final case class PubSubDelayOverEvent(recoveredTypes: Set[String]) extends GlobalEvent
