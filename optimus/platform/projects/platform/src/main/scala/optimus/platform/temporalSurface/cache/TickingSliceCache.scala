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

import com.google.common.cache.CacheBuilder

import java.util.concurrent.PriorityBlockingQueue
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.time.Instant
import optimus.entity.EntityInfoRegistry
import optimus.graph.tracking.TrackingLeafTemporalSurface
import optimus.platform.{TimeInterval, ValidTimeInterval, impure}
import optimus.platform.dal.QueryTemporality
import optimus.platform.storable.{Entity, EntityReference, PersistentEntity, SerializedKey}
import optimus.platform.temporalSurface.{TemporalSurfaceCache, TemporalSurfaceCacheManager}
import optimus.platform.temporalSurface.operations.{DataQueryByEntityReference, QueryByKey, TemporalSurfaceQuery}

import scala.annotation.tailrec
import scala.collection.immutable.SortedMap
import scala.collection.mutable.ArrayBuffer
import optimus.platform.reactive.handlers.EntityNotificationUpdate
import optimus.platform.reactive.handlers.NotificationUpdate
import msjava.slf4jutils.scalalog._
import optimus.dsi.partitioning.Partition
import optimus.platform.reactive.pubsub.PSStreamManager
import optimus.platform.storable.SortedPropertyValues

import scala.concurrent.Future
import optimus.platform.temporalSurface.SubscriptionID

class TimeSegmentData[T <: AnyRef](segments: (ValidTimeInterval, T)*) {
  private[optimus] val data = ArrayBuffer[(ValidTimeInterval, T)](segments: _*)

  private def overlapped(segment: ValidTimeInterval, current: ValidTimeInterval): Boolean = {
    segment.from.isBefore(current.to) && segment.to.isAfter(current.from)
  }

  def get(vt: Instant): T = {
    getOption(vt).getOrElse(null.asInstanceOf[T])
  }

  def getOption(vt: Instant): Option[T] = {
    @tailrec def binarySearch(start: Int, end: Int): Int = {
      if (start > end) -1
      else {
        val mid = start + (end - start) / 2
        val cur = data(mid)._1
        if (
          !vt.isBefore(cur.from) && (cur.to.isAfter(
            vt) || (cur.to == TimeInterval.Infinity && vt == TimeInterval.Infinity))
        )
          mid
        else if (vt.isBefore(cur.from))
          binarySearch(start, mid - 1)
        else
          binarySearch(mid + 1, end)
      }
    }
    val pos = binarySearch(0, data.length - 1)

    if (pos == -1) None
    else Some(data(pos)._2)
  }

  final def put(vtInterval: ValidTimeInterval, payload: T): this.type = put((vtInterval, payload))

  @impure def put(segment: (ValidTimeInterval, T)): this.type = {
    val seg = segment._1
    require(seg.from isBefore seg.to, s"Valid time interval ${seg} is empty or invalid (from < to)")
    val (pos, overlap) = binarySearch(segment._1)
    if (overlap) {
      val current = data(pos)
      val cur = current._1
      require(
        !cur.from.isAfter(seg.from) && !cur.to.isBefore(seg.to),
        s"According to the current DAL notification, updated segment ${seg} should be inside current segment ${cur}"
      )
      if (seg.from == cur.from) {
        if (seg.to == cur.to) {
          // replace
          data(pos) = segment
        } else if (seg.to.isBefore(cur.to)) {
          // split to [seg.from, seg.to) [seg.to, cur.to)
          data(pos) = segment
          data.insert(pos + 1, (ValidTimeInterval(seg.to, cur.to), current._2))
        } else {
          require(false) // guarded by base assumption
        }
      } else if (seg.from.isAfter(cur.from)) {
        if (seg.to == cur.to) {
          data(pos) = (ValidTimeInterval(cur.from, seg.from), current._2)
          data.insert(pos + 1, segment)
        } else {
          require(
            false,
            s"In current DAL notification, if the start points of the timeslices ($seg, $cur) are not the same, then the end points must be the same")
        }
      } else {
        require(false) // guarded by base assumption
      }
    } else {
      data.insert(pos, segment)
    }

    this
  }

  @impure def invalidate(seg: ValidTimeInterval): this.type = {
    require(seg.from isBefore seg.to, s"Valid time interval ${seg} is empty or invalid (from < to)")
    if (seg == ValidTimeInterval.max) { // Obliterate
      clear()
    } else {
      val (pos, overlap) = binarySearch(seg)
      if (overlap) {
        val current = data(pos)
        val cur = current._1
        if (overlapped(seg, cur)) {
          // base assumption
          require(
            !cur.from.isAfter(seg.from) && !cur.to.isBefore(seg.to),
            s"According to the current DAL notification, updated segment ${seg} should be inside current segment ${cur}"
          )
          if (seg.from == cur.from) {
            if (seg.to == cur.to) {
              // replace
              data.remove(pos)
            } else if (seg.to.isBefore(cur.to)) {
              // split to invalidated [seg.from, seg.to) and new [seg.to, cur.to)
              data(pos) = (ValidTimeInterval(seg.to, cur.to), current._2)
            } else {
              require(false) // guarded by base assumption
            }
          } else if (seg.from.isAfter(cur.from)) {
            if (seg.to == cur.to) {
              data(pos) = (ValidTimeInterval(cur.from, seg.from), current._2)
            } else {
              require(
                false,
                s"In current DAL notification, if the start points of the timeslices ($seg, $cur) are not the same, then the end points must be the same")
            }
          } else {
            require(false) // guarded by base assumption
          }
        }
      }
    }
    this
  }

  @impure private[optimus] def clear(): Unit = {
    data.clear()
  }

  protected def binarySearch(vt: ValidTimeInterval): (Int, Boolean) = {
    @tailrec def helper(start: Int, end: Int): (Int, Boolean) = {
      if (start > end) (start, false)
      else {
        val mid = start + (end - start) / 2
        val cur = data(mid)._1
        if (overlapped(vt, cur))
          (mid, true)
        else if (!cur.from.isBefore(vt.to))
          helper(start, mid - 1)
        else
          helper(mid + 1, end)
      }
    }
    helper(0, data.length - 1)
  }
}

trait TickingSliceCache extends TemporalSurfaceCache {
  var currentTxTime: Instant = null
  private[this] val lock = new ReentrantReadWriteLock(true)

  protected final def updateTime(
      newTime: Instant,
      fn: (Instant, (EntityReference) => Boolean) => Set[EntityReference],
      filter: (EntityReference) => Boolean): Set[EntityReference] = {
    lock.writeLock.lock
    try {
      // allow updateTime to be called on the same txTime, which is a no-op
      require(currentTxTime == null || !currentTxTime.isAfter(newTime))

      // TODO (OPTIMUS-0000): this is only a valid test while VT is fixed to infinity
      if (currentTxTime == null || newTime.isAfter(currentTxTime)) {
        if (currentTxTime != null) cacheManager.removeMonotemporalCache(currentTxTime, this)

        val result = fn(newTime, filter)
        currentTxTime = newTime
        cacheManager.addMonotemporalCache(currentTxTime, this)
        result
      } else Set()
    } catch {
      case t: Throwable =>
        // remove from the cache manager as we dont know the state
        if (currentTxTime != null) cacheManager.removeMonotemporalCache(currentTxTime, this)
        currentTxTime = null
        throw t
    } finally lock.writeLock.unlock
  }

  def readAccess[T](timePoint: Instant, fn: => Option[T]): Option[T] = {
    lock.readLock.lock
    try if (timePoint == currentTxTime) fn else None
    finally lock.readLock.unlock
  }
}

object TickingSliceCache {
  object timeSegmentFactory extends java.util.concurrent.Callable[TimeSegmentData[PersistentEntity]] {
    override def call() = new TimeSegmentData[PersistentEntity]
  }
}

class DalPSNotificationSliceCache(
    trackingLeaf: TrackingLeafTemporalSurface,
    cacheManager: TemporalSurfaceCacheManager,
    streamManager: PSStreamManager
) extends NotificationSliceCache(trackingLeaf, cacheManager) {
  import NotificationSliceCache.logger

  override val erefSubscriptionManager = new DalPSERefSubscriptionManager(this, streamManager)

  override def onNotificationUpdate(update: EntityNotificationUpdate): Unit = {
    // This check is different than for external pubsub because we allow receiving multiple updates at the same TT
    if ((lastSeenTxTime eq null) || !update.tt.isBefore(lastSeenTxTime)) {
      lastSeenTxTime = update.tt
      processNotification(update)
    } else {
      logger.warn(s"received a eref notification update with earlier tt: ${update.tt}")
      logger.trace(s"received a eref notification update $update")
    }
  }
}

// Slice cache is a special case of bitemporal cache, it contains a single txTime bound, which is equivalant to
// [txTime, Any)
// ValidTime intervals which the application is interested in will be cached, otherwise it will dropped
abstract class NotificationSliceCache(
    val trackingLeaf: TrackingLeafTemporalSurface,
    protected val cacheManager: TemporalSurfaceCacheManager,
) extends TickingSliceCache
    with TickingCache
    with NotificationUpdateCallback {
  import NotificationSliceCache.logger

  val erefSubscriptionManager: ErefSubscriptionManager
  private val entitySlices = CacheBuilder.newBuilder().build[EntityReference, TimeSegmentData[PersistentEntity]]()
  private val serializedKeyIndexedSlices =
    CacheBuilder.newBuilder.build[SerializedKey, TimeSegmentData[Seq[EntityReference]]]()

  override def timeAdvanced(
      trackingLeaf: TrackingLeafTemporalSurface,
      tt: Instant,
      filter: (EntityReference) => Boolean) = {
    require(trackingLeaf == this.trackingLeaf)
    updateTime(tt, processQueue, filter)
  }

  // TODO (OPTIMUS-20681): consider what queue to use
  // the order of insertion is I think guaranteed to be tt ordered
  // does it make sense to have a bounded queue - what happens if the bounds are exceeded?
  // should cacheUpdate be naturally ordered - at least the comparator should be shared
  // I think that the queue should be an ArrayBlockingQueue if sized, or a ConcurrentLinkedDeque if not ( Deque allows poll/reinsert rather than peek/take
  private val queue = new PriorityBlockingQueue[NotificationUpdate](
    1000,
    (o1: NotificationUpdate, o2: NotificationUpdate) => o1.compare(o2))

  def hasPotentialUpdate(tt: Instant): Boolean = {
    val next = queue.peek()
    val ret = next != null && !next.tt.isAfter(tt)
    logger.debug(s"hasPotentialUpdate - $tt = $ret ${next}")
    ret
  }

  def processNotification(update: NotificationUpdate): Unit = {
    logger.debug(s"processNotification ${update.tt} - $update  ")
    queue.add(update)
  }

  def processQueue(at: Instant, filter: (EntityReference) => Boolean): Set[EntityReference] =
    processQueueRec(at, filter, Set[EntityReference]())

  private def generateKeys(e: PersistentEntity): Iterable[SerializedKey] = {
    // similar to the logic in EntitySerializer, but we don't have upcasting class logic
    val info = EntityInfoRegistry.getClassInfo(e.className)
    info.keys map { key =>
      val typeName = key.storableClass.getName
      val properties = SortedPropertyValues(key.propertyNames, e.serialized.properties)
      new SerializedKey(typeName, properties, key.unique, key.indexed)
    }
  }

  @tailrec private def processQueueRec(
      at: Instant,
      filter: (EntityReference) => Boolean,
      changed: Set[EntityReference]): Set[EntityReference] = {
    val next = queue.peek()
    if (next != null && !next.tt.isAfter(at)) {
      // deliberate hide the peeked value
      // if there is a race and this is not the peeked value than this value should be
      // fine to use because of the ordering of the queue
      val next = queue.take

      val affected = next match {
        case entityCacheUpdate: EntityNotificationUpdate =>
          entityCacheUpdate.deletedEntities foreach { e =>
            val segmentData = entitySlices.get(e.entityRef, TickingSliceCache.timeSegmentFactory)
            if (e.vtInterval == ValidTimeInterval.max) {
              // for obliterate
              require(
                entityCacheUpdate.changedEntities.forall(_.entityRef != e.entityRef),
                s"Entity $e is obliterated, but it also exists in the updated entities collection in the same transaction"
              )
            }
            segmentData.invalidate(e.vtInterval)

            generateKeys(e) foreach { k =>
              val segmentData = serializedKeyIndexedSlices.get(k, NotificationSliceCache.timeSegmentSeqFactory)
              segmentData.invalidate(e.vtInterval)
            }
          }

          entityCacheUpdate.changedEntities foreach { e =>
            // trackingLeaf.asInstanceOf[TemporalContext].deserialize(e, DSIStorageInfo.fromPersistentEntity(e))
            val segmentData = entitySlices.get(e.entityRef, TickingSliceCache.timeSegmentFactory)
            segmentData.put(e.vtInterval, e)

            // TODO (OPTIMUS-0000): revisit cases for unique index
            generateKeys(e) foreach { k =>
              val segmentData = serializedKeyIndexedSlices.get(k, NotificationSliceCache.timeSegmentSeqFactory)
              segmentData.put(e.vtInterval, Seq(e.entityRef))
            }
          }

          entityCacheUpdate.deletedEntities.map(_.entityRef) ++ entityCacheUpdate.changedEntities.map(_.entityRef)
      }
      processQueueRec(at, filter, changed ++ affected)
    } else {
      changed
    }
  }

  override def getItemData(operation: TemporalSurfaceQuery)(
      sourceTemporality: operation.TemporalityType,
      itemTemporality: operation.TemporalityType): Option[Map[operation.ItemKey, operation.ItemData]] = {
    def cast(o1: TemporalSurfaceQuery)(
        result: Option[Map[o1.ItemKey, o1.ItemData]]): Option[Map[operation.ItemKey, operation.ItemData]] = {
      require(o1 eq operation)
      result.asInstanceOf[Option[Map[operation.ItemKey, operation.ItemData]]]
    }
    operation match {
      case eq: DataQueryByEntityReference[_] if sourceTemporality == itemTemporality =>
        val pe = getSingleItemData(eq)(sourceTemporality.asInstanceOf[eq.TemporalityType], eq.eRef)
        val result: Option[Map[eq.ItemKey, eq.ItemData]] = pe map { pe =>
          Map(eq.eRef -> pe)
        }
        cast(eq)(result)
      case _ => None
    }
  }

  override def getPersistentEntity(
      eRef: EntityReference,
      temporality: QueryTemporality.At): Option[PersistentEntity] = {
    readAccess(
      temporality.txTime, {
        Option(entitySlices.getIfPresent(eRef)) flatMap { segmentData =>
          segmentData.getOption(temporality.validTime)
        }
      })
  }

  override def getSingleItemData(operation: TemporalSurfaceQuery)(
      temporality: operation.TemporalityType,
      key: operation.ItemKey): Option[operation.ItemData] =
    getPersistentEntity(key, temporality)

  override def getItemKeys(operation: TemporalSurfaceQuery)(
      sourceTemporality: operation.TemporalityType): Option[Seq[operation.ItemKey]] = {
    operation match {
      case QueryByKey(key) =>
        val result = sourceTemporality match {
          case QueryTemporality.At(vt, tt) =>
            readAccess(
              tt, {
                Option(serializedKeyIndexedSlices.getIfPresent(key)) flatMap (_.getOption(vt))
              })
          case _ =>
            throw new UnsupportedOperationException("Need both valid time and transaction time to query item keys")
        }
        result
      case _ =>
        None
    }
  }

  var lastSeenTxTime: Instant = _

  // From NotificationUpdateCallback
  override def onTransactionTimeUpdate(txTime: Instant): Unit = {
    lastSeenTxTime = txTime
  }

  def asyncCacheIfAbsent(eref: EntityReference, clazz: Class[_], partition: Partition): Future[SubscriptionID] =
    erefSubscriptionManager.subscribeAsync(eref, clazz, partition, this, trackingLeaf.currentTemporality.txTime)
}

object NotificationSliceCache {
  val logger = getLogger[NotificationSliceCache]

  private object timeSegmentSeqFactory extends java.util.concurrent.Callable[TimeSegmentData[Seq[EntityReference]]] {
    override def call() = new TimeSegmentData[Seq[EntityReference]]
  }
}
