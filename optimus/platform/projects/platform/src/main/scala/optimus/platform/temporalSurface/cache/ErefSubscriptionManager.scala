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
package optimus.platform.temporalSurface.cache

import java.time.Instant

import optimus.dsi.partitioning.Partition
import optimus.platform.storable.{Entity, EntityReference}
import optimus.platform.temporalSurface.SubscriptionID
import optimus.platform.reactive.handlers.EntityNotificationUpdate

import scala.concurrent.Future

trait NotificationUpdateCallback {
  def onNotificationUpdate(update: EntityNotificationUpdate): Unit
  def onTransactionTimeUpdate(txTime: Instant): Unit
}

trait ErefSubscriptionManager {
  // TODO (OPTIMUS-24534): Do not return Future after external pubsub deco, do not need startTime parameter after CPS deco
  def subscribeAsync(
      eref: EntityReference,
      clazz: Class[_],
      partition: Partition,
      callback: NotificationUpdateCallback,
      startTime: Instant): Future[SubscriptionID]

  private[optimus] def unsubscribeAsync(eref: EntityReference, partition: Partition): Future[Unit]
}
