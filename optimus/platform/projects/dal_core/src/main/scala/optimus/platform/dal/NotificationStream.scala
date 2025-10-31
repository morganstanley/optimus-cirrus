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

object NotificationStream {
  type ClientStreamId = Int
  type SubscriptionIdType = Int
}

/**
 * Base class for DAL notifications.
 */
trait NotificationStream[SubscriptionType, ClientStreamIdType] {
  import NotificationStream._

  /**
   * A unique id for the stream for client-side only. This is added so that if client wants to share the same callback
   * for multiple streams, they can distinguish events using this id.
   */
  def id: ClientStreamIdType

  /**
   * Current active subscriptions.
   *
   * Initially contains the subscriptions the stream was created with, and is updated with [[changeSubscription]].
   */
  def currentSubscriptions: Set[SubscriptionType]

  /**
   * Add or remove subscriptions.
   *
   * This is an asynchronous API. The changeId passed to method should be used to correlate the success/failure message
   * which is received asynchronously via [[NotificationStreamCallback.notifyStreamEvent]] with `SubscriptionChangeSucceeded` or `SubscriptionChangeFailed` events.
   *
   * Data from new subscriptions is received only after the subscription change messages.
   *
   * Adding a subscription to an existing stream uses the current stream `tt`. You'll need to create new
   * [[NotificationStream]] if you want to subscribe using a different `tt`.
   *
   * Either of `subscriptionsToAdd` or `subscriptionsToRemove` can be empty. If both arguments are empty this call is a
   * no-op.
   */
  def changeSubscription(
      changeId: Int,
      subscriptionsToAdd: Set[SubscriptionType],
      subscriptionsToRemove: Set[SubscriptionIdType] = Set.empty
  ): Unit = throw new UnsupportedOperationException

  /**
   * Terminate the notification stream. No new data will be received after this call.
   */
  def close(): Unit
}

/**
 * The "reader" side of a [[NotificationStream]].
 *
 * A [[NotificationStreamCallback]] is registered with a [[NotificationStream]] when creating it. Events in the stream
 * are received via calls to [[notifyGlobalEvent]] and [[notifyStreamEvent]].
 *
 * This interface is separate from [[NotificationStream]] for testability and to allow subscribing to multiple
 * notification streams using a single callback.
 */
trait NotificationStreamCallback[GlobalEventType, StreamEventType, ClientStreamIdType] {

  /**
   * Notify of a global event, an event that may affect more than one stream.
   *
   * This callback is invoked in the connection thread and never called concurrently. The `id` parameter can be used
   * by the implementation if multiple streams are processed with a single callback.
   */
  def notifyGlobalEvent(id: ClientStreamIdType, msg: GlobalEventType): Unit

  /**
   * Notify of an event for the current stream. This is also where data is received.
   *
   * This callback is invoked in the thread of the current notification stream, and never called concurrently. The `id`
   * parameter can be used by the implementation if multiple streams are processed with a single callback.
   */
  def notifyStreamEvent(id: ClientStreamIdType, msg: StreamEventType): Unit

  // TODO (OPTIMUS-80295): The two methods below should not be necessary. See comment in DALPSStreamManager about it.
  /**
   * Called on the notifyStreamEvent thread on first run.
   */
  def setupThread(id: ClientStreamIdType): Unit = ()

  /**
   * Called on the notifyStreamEvent thread when we shut down.
   */
  def teardownThread(id: ClientStreamIdType): Unit = ()
}
