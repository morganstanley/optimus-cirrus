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
package optimus.platform.reactive.pubsub

import java.time.Instant
import optimus.dsi.notification.NotificationEntry
import optimus.dsi.pubsub.Subscription
import optimus.platform.temporalSurface.cache.DalPSNotificationSliceCache

private[optimus] trait ErefSubscription {
  val subscriptionId: Int
  def remove(): Unit
}

private[optimus] trait NotificationEntryHandler {
  def handleNotificationEntries(tt: Instant, notificationEntries: Seq[NotificationEntry]): Unit
}

private[optimus] trait StreamManager {
  def implicitErefCache: Option[DalPSNotificationSliceCache]
}

private[optimus] trait PSStreamManager extends StreamManager {
  def createErefSubscription(subscription: Subscription, callback: NotificationEntryHandler): ErefSubscription
}
