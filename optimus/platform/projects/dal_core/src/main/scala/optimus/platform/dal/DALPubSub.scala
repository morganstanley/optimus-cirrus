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
package optimus.platform.dal

import java.time.Instant

import optimus.dsi.partitioning.Partition
import optimus.dsi.pubsub._
import optimus.platform.dal

trait DALPubSub {
  import DALPubSub._

  /**
   * Creates a notification stream using the subscriptions and the stream would eventually start ticking. This is an
   * async api, which would fire the message to the broker and return with NotificationStream object which has the
   * StreamId. This streamId can be used for correlating notification messages to a stream in case the same callback
   * object is used with multiple streams.
   *
   * The success/failure would be notified via the NotificationStreamCallback#notify(StreamId, StreamCreationSucceeded)
   * or NotificationStreamCallback#notify(StreamId, StreamCreationFailed)
   *
   * Only after receiving the StreamCreationSucceeded, the other DataMessages will be sent to the callback object.
   *
   * The broker will reject subscription if startTime is before start of available worklog (as of now worklog is kept
   * for last 28 days).
   *
   * startTime == None means that the broker chooses whatever start time is most convenient for it to service and ticks
   * from there onwards.
   *
   * endTime maybe +Inf. vt is assumed to be 'latest' like OpenVtRange.
   */
  def createTickingNotificationStream(
      streamId: ClientStreamId,
      subscriptions: Set[Subscription],
      cb: DALPubSub.NotificationStreamCallback,
      startTime: Option[Instant],
      endTime: Instant,
      partition: Partition): DALPubSub.NotificationStream
}

object DALPubSub {

  type ClientStreamId = Int

  trait NotificationStreamCallback extends dal.NotificationStreamCallback[GlobalEvent, StreamEvent, ClientStreamId] {

    /**
     * The notify callback gets the Global events This callback is never called concurrently for a specific
     * NotificationStream. The streamId can be used in case the same callback is used with multiple NotificationStreams.
     *
     * This callback is invoked in the connection thread and not in the stream specific thread
     */
    override def notifyGlobalEvent(id: ClientStreamId, msg: GlobalEvent): Unit

    /**
     * The notify callback gets the StreamEvent, which could also be DataMessage.
     *
     * The DataMessage will always be in the increasing order of 'tt'. The HeartbeatMessage will always be in
     * non-decreasing order of 'tt' and one can get DataMessage and Heartbeat with same 'tt'.
     *
     * This callback is invoked in a separate thread dedicated per NotificationStream and more than one invocations will
     * never happen concurrently.
     *
     * The streamId can be used in case the same callback is used with multiple NotificationStreams.
     * @param id
     * @param msg
     */
    override def notifyStreamEvent(id: ClientStreamId, msg: StreamEvent): Unit
  }

  /**
   * A notification stream connects to the notification broker to receive notifications for matching subscriptions.
   *
   * The notification stream can be created with a startTime. The startTime cannot be before the start of available
   * worklog. On the other hand if the notification stream is not created with a startTime, then broker chooses whatever
   * startTime is most convenient.
   *
   * The notification stream invokes the streamCallback with NotificationMessages both ControlMessage and DataMessage.
   * The DataMessage will always be in the increasing order of 'tt'.
   */
  trait NotificationStream extends dal.NotificationStream[Subscription, ClientStreamId] {

    /**
     * A unique id for the stream for client-side only. This is added so that if client wants to share the same callback
     * for multiple streams, they can distinguish pubsub events using this id.
     */
    override val id: ClientStreamId

    /**
     * The current set of subscriptions. Initialized with the subscriptions passed during stream creation. It would
     * change with using changeSubscription api. The change won't reflect immediately as changeSubscription api is an
     * async api. But it will reflect before the callback object receives the
     * SubscriptionChangeSucceeded/SubscriptionChangeFailed message.
     */
    override def currentSubscriptions: Set[Subscription]

    /**
     * Method to change the subscriptions of this stream. This is an async api, which would fire the message to the
     * broker and return. The changeId passed to method should be used to correlate the success/failure message which is
     * received asynchronously via NotificationStreamCallback#notify(StreamId, SubscriptionChangeSucceeded) or
     * NotificationStreamCallback#notify(StreamId, SubscriptionChangeFailed)
     *
     * Only after receiving the SubscriptionChangeSucceeded, the other DataMessages will be sent to the callback object.
     *
     * Note since it is adding subscriptions to an existing stream, it will use the stream's current "tt". If the
     * requirement is to start with older â€˜ttâ€™ then one needs to create a new notification stream. Applicable to SOW
     * as well.
     *
     * Either of the arguments can be empty, both empty would be a noop.
     */
    override def changeSubscription(
        changeId: Int,
        subscriptionsToAdd: Set[Subscription],
        subscriptionsToRemove: Set[Subscription.Id] = Set.empty): Unit = throw new UnsupportedOperationException

    // Unsubscribes from broker.
    override def close(): Unit
  }
}
