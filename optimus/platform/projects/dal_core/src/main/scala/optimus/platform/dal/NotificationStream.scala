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

trait NotificationStream[SubscriptionType, ClientStreamIdType] {
  import NotificationStream._

  /**
   * A unique id for the stream for client-side only. This is added so that if client wants to share the same callback
   * for multiple streams, they can distinguish events using this id.
   */
  def id: ClientStreamIdType

  def currentSubscriptions: Set[SubscriptionType]

  def changeSubscription(
      changeId: Int,
      subscriptionsToAdd: Set[SubscriptionType],
      subscriptionsToRemove: Set[SubscriptionIdType] = Set.empty
  ): Unit = throw new UnsupportedOperationException

  def close(): Unit
}

trait NotificationStreamCallback[GlobalEventType, StreamEventType, ClientStreamIdType] {
  import NotificationStream._

  def notifyGlobalEvent(id: ClientStreamIdType, msg: GlobalEventType): Unit
  def notifyStreamEvent(id: ClientStreamIdType, msg: StreamEventType): Unit

  /**
   * This will be invoked *only once* to setup the thread used to invoke "notifyStreamEvent" callback.
   */
  def setupThread(id: ClientStreamIdType): Unit = { /* User can override this */ }

  /**
   * This will be invoked when the thread used to invoke "notifyStreamEvent" callback is stopped/shutdown.
   */
  def teardownThread(id: ClientStreamIdType): Unit = { /* User can override this  */ }
}
