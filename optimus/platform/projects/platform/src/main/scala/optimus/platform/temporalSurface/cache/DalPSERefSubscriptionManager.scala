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

import java.util.concurrent.ConcurrentHashMap
import java.time.Instant

import msjava.slf4jutils.scalalog._
import optimus.dsi.notification.NotificationEntry
import optimus.dsi.partitioning.Partition
import optimus.dsi.pubsub.Subscription
import optimus.platform.storable.EntityReference
import optimus.platform.temporalSurface.SubscriptionID
import optimus.platform.reactive.handlers.EntityNotificationUpdate
import optimus.platform.reactive.pubsub.ErefSubscription
import optimus.platform.reactive.pubsub.NotificationEntryHandler
import optimus.platform.reactive.pubsub.PSStreamManager

import scala.collection.mutable
import scala.concurrent.Future
import scala.util.control.NonFatal

object DalPSERefSubscriptionManager {
  val log: Logger = getLogger(getClass)
}

class DalPSERefSubscriptionManager(
    notificationCallback: NotificationUpdateCallback,
    subscriptionManager: PSStreamManager
) extends ErefSubscriptionManager
    with NotificationEntryHandler {
  import DalPSERefSubscriptionManager._

  /**
   * Since unsubscribes happen some time after an entity is actually GCed, it seems possible that subscribe could be
   * called on the same EntityReference before the previous subscription has been removed:
   *
   * (1) Subscribe(e1), Subscribe(e1), Unsubscribe(e1)
   *
   * This means it is not possible to store a simple map of EntityReference -> current Subscription, since if case (1)
   * occurs then we would end up with no subscription at the end.
   *
   * The solution is to store a map of (EntityReference -> list non-unsubscribed Subscriptions). When subscribing we
   * simply append to the list, and when unsubscribing we drop the first element in the list
   *
   * Then if case (1) occurs, after the second subscribe we would end up with:
   *
   * (eref1 -> List(sub1, sub2))
   *
   * And after the unsubscribe happens we would be left with:
   *
   * (eref1 -> List(sub2))
   *
   * We use a ConcurrentHashMap to ensure visibility of updates between subscribes and unsubscribes, and we synchronise
   * on the list access to prevent concurrent updates to a specific eref's subscriptions.
   */
  private val subscriptions = new ConcurrentHashMap[EntityReference, mutable.ListBuffer[ErefSubscription]]()

  // Approx count of total number of subscriptions, for tests
  def subscriptionsCount: Int = {
    var count = 0
    subscriptions.forEach((_, subs) => count += subs.size)
    count
  }

  def handleNotificationEntries(tt: Instant, notificationEntries: Seq[NotificationEntry]): Unit = {
    notificationCallback.onNotificationUpdate(
      EntityNotificationUpdate.fromNotificationEntries(tt, notificationEntries)
    )
  }

  override def subscribeAsync(
      eref: EntityReference,
      clazz: Class[_],
      partition: Partition,
      callback: NotificationUpdateCallback,
      startTime: Instant): Future[SubscriptionID] = {
    try {
      val subscription = Subscription(eref, clazz)
      val erefSubscription = subscriptionManager.createErefSubscription(subscription, this)
      var subscriptionBuf = subscriptions.get(eref)
      if (subscriptionBuf == null) {
        subscriptionBuf = mutable.ListBuffer()
        subscriptions.put(eref, subscriptionBuf)
      }
      subscriptionBuf.synchronized {
        subscriptionBuf.append(erefSubscription)
      }
      Future.successful(SubscriptionID(subscription.subId))
    } catch {
      case NonFatal(e) => {
        log.error("unhandled exception", e)
        Future.failed(e)
      }
      case t: Throwable =>
        log.error("ignored exception", t)
        throw t;
    }
  }

  override def unsubscribeAsync(eref: EntityReference, partition: Partition): Future[Unit] = {
    try {
      val subscriptionBuf = subscriptions.get(eref)
      if (subscriptionBuf != null) {
        subscriptionBuf.synchronized {
          subscriptionBuf.remove(0).remove()
        }
        Future.successful(())
      } else {

        /**
         * This case is expected when unsubscribing since we do a tree traversal over all of the temporal context's
         * child surfaces attempting to unsubscribe.
         *
         * See [[optimus.graph.tracking.DalPsEntityKeyWeakReferenceManager.removeErefSubscriptions]] for more details
         */
        log.debug(s"No subscription registered for given eref: $eref")
        Future.successful(())
      }
    } catch {
      case NonFatal(e) => {
        log.error("unhandled exception", e)
        Future.failed(e)
      }
      case t: Throwable =>
        log.error("ignored exception", t)
        throw t;

    }
  }
}
