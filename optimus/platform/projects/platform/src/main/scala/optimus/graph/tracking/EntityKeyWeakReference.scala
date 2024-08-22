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
package optimus.graph.tracking

import java.lang.ref.ReferenceQueue
import java.lang.ref.WeakReference
import java.util.concurrent.atomic.AtomicInteger
import javax.management.Notification

import optimus.platform.storable.Entity
import optimus.platform._
import msjava.slf4jutils.scalalog._
import optimus.core.CoreHelpers
import optimus.platform.temporalSurface.TemporalSurface

/**
 * A WeakReference[Entity] used as keys of Map, while delegate the semantic of equality to be ent's equality This is a
 * workaround to java WeakReference whose equality is object identity
 *
 * Implement note: we don't need temporal context of the entity as part of equality because this class will be
 * maintained in a map defined per optimus.graph.tracking.DependencyTrackerTemporalContexts.TrackingTemporalContextImpl
 *
 * @param ent
 *   The entity referenced, used in gc listener to unsubscribe
 * @param mgr
 *   Object containing reference queue which will be enqueued entities by GC, the entities should get unsubscribe by gc
 *   listener
 */
final private[optimus] class EntityKeyWeakReference(
    ent: Entity,
    val owner: DependencyTrackerTemporalContexts#TrackingTemporalContextImpl,
    mgr: EntityKeyWeakReferenceManager
) extends WeakReference(ent, mgr.refQ) {
  assert(!ent.dal$isTemporary, s"shouldn't create an EntityWeakReference for a heap entity: $key")
  val key = ent.dal$entityRef
  val tc = ent.dal$temporalContext
  val clazz = ent.getClass()

  // TODO (OPTIMUS-24534): Retire this after external pub/sub Deco
  val partition = partitionMapForNotification.partitionForType(ent.getClass())
  override val hashCode = ent.hashCode()
  override def equals(obj: scala.Any): Boolean = obj match {
    case that: EntityKeyWeakReference => this.key == that.key
    case _                            => false
  }
}

private[optimus] trait EntityKeyWeakReferenceManager {
  val refQ: ReferenceQueue[Entity]
}

private[optimus] object DalPsEntityKeyWeakReferenceManager extends EntityKeyWeakReferenceManager {
  private val log = getLogger(getClass)

  val refQ: ReferenceQueue[Entity] = new ReferenceQueue[Entity]

  val totalReferencesProcessed = new AtomicInteger()

  private val gcListener = new javax.management.NotificationListener {
    override def handleNotification(notif: Notification, handback: AnyRef): Unit = {
      var count = 0

      var next = refQ.poll()
      while (next != null) {
        next match {
          case ewr: EntityKeyWeakReference =>
            log.debug(s"Should unsubscribe implicit subscribed eref: ${ewr.key} of partition [${ewr.partition}] on GC")
            removeErefSubscriptions(ewr.owner.surface, ewr)
            count += 1
            totalReferencesProcessed.incrementAndGet()
          case _ => log.error(s"Expected EntityWeakReference but found $next")
        }
        next = refQ.poll()
      }

      if (count > 0)
        log.info(s"After a GC Collection $count ticking entities are no longer reachable, and will be unsubscribed")
    }
  }

  /**
   * We track entity key weak references in the temporal context, which might be a branch context or a leaf context.
   *
   * We manage eref subscriptions at the leaf temporal surface level
   *
   * Starting at a temporal context we check whether we are a leaf context, in which case we get the surface's eref
   * subscription manager and remove the subscription from there. If we are a branch context, then we iterate over all
   * of the branch's children and repeat this process.
   *
   * The result is that we will try to unsubscribe at all the child leaf surfaces' eref subscription managers.
   */
  private def removeErefSubscriptions(ts: TemporalSurface, entityWeakRef: EntityKeyWeakReference): Unit = ts match {
    case t: TrackingLeafTemporalSurface =>
      t.implicitErefCacheIfTracking.foreach(
        _.erefSubscriptionManager.unsubscribeAsync(entityWeakRef.key, entityWeakRef.partition))
    case t: DependencyTrackerTemporalContexts#TrackingBranchTemporalContext =>
      t.children.foreach(removeErefSubscriptions(_, entityWeakRef))
    case s if s.canTick =>
      log.error(s"Unhandled tickable surface type: $s")
    case _ => // non-tickable surface so no eref subscriptions here
  }

  CoreHelpers.registerGCListener(gcListener)
}
