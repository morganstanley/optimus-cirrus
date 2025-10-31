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

  trait NotificationStreamCallback extends dal.NotificationStreamCallback[GlobalEvent, StreamEvent, ClientStreamId]

  trait NotificationStream extends dal.NotificationStream[Subscription, ClientStreamId]
}
