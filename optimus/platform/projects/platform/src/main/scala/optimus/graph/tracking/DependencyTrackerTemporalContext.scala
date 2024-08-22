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

import optimus.core.MonitoringBreadcrumbs

import java.time.Instant
import java.util
import java.util.Collections
import java.util.Objects
import java.util.concurrent.locks.ReentrantLock
import javax.annotation.concurrent.GuardedBy
import optimus.dsi.partitioning.DefaultPartition
import optimus.dsi.partitioning.Partition
import optimus.graph.AlreadyCompletedPropertyNode
import optimus.graph.GraphInInvalidState
import optimus.graph.NodeKey
import optimus.graph.NodeTaskInfo
import optimus.graph.Settings
import optimus.platform.dal.DSIStorageInfo
import optimus.platform.dal.QueryTemporality
import optimus.platform.pickling.PickledMapWrapper
import optimus.platform.pickling.ReflectiveEntityPickling
import optimus.platform.storable.Entity
import optimus.platform.storable.EntityReference
import optimus.platform.storable.PersistentEntity
import optimus.platform.storable.StorageInfo
import optimus.platform.temporalSurface.LeafTemporalSurface
import optimus.platform.temporalSurface.TickableTemporalContext
import optimus.platform.temporalSurface._
import optimus.platform.temporalSurface.cache.NotificationSliceCache
import optimus.platform.temporalSurface.impl._
import optimus.platform._
import optimus.platform.reactive.pubsub.StreamManager
import optimus.platform.temporalSurface.operations.TemporalSurfaceQuery

import optimus.scalacompat.collection._
import optimus.utils.MacroUtils.SourceLocation

// A TrackingLeafTemporalSurface may be a TemporalContext or a TemporalSurface within a TickableTemporalContext
// this is the junction point (ie an implementation) between the DependencyTracker implementation of Tickable...
// We need to deserialize and splat the entities on this TemporalContext
private[optimus] sealed trait TrackingLeafTemporalSurface extends LeafTemporalSurfaceImpl {
  private[optimus] def currentTemporality: QueryTemporality.At

  private[optimus] val partition = {
    val partitions = matcher.partitions

    if (partitions.size == 1) partitions.head
    else if (partitions.isEmpty) DefaultPartition
    else
      throw new RuntimeException(
        s"""One ticking temporal surface can only tick one partition, but the $matcher includes $partitions""")
  }

  /**
   * advance times to the specified time
   * @param time
   *   the time to advance to
   * @param filter
   *   filter to apply to returned values
   * @return
   *   the set of entities that have ticked, with return true when applied to the filter
   */
  @impure private[optimus] def advanceTimes(time: Instant, filter: EntityReference => Boolean): Set[EntityReference]
  private[optimus] def prepareTemporality(futureTickTime: Instant): Unit

  /**
   * indicates is there is any data change tt cannot be after the updates received and enqueued
   */
  private[optimus] def hasPotentialUpdate(tt: Instant): Boolean

  /**
   * Returns the NotificationSliceCache if we are currently tracking erefs, or None otherwise.
   */
  private[optimus] def implicitErefCacheIfTracking: Option[NotificationSliceCache]
}

trait CanTrack {
  // TODO (OPTIMUS-21217): This is a total hack. What we actually want is for TrackingTemporalContextImpl to
  //  disappear entirely, but this will require fairly big changes that we want to make carefully.
  //
  // Most usage of TrackingTemporalContextImpl look like this,
  //
  //   scenario(At.now, TickableBranchContext(...)) { some reactive binds ... }
  //
  // TrackingTemporalContextImpl is needed here because the binding code requires a tickable temporal context.
  // However, in the vast majority of cases (fixed and frozen bindings) the context doesn't need to be tracking
  // one at all. We don't really want to create yet another kind of Temporal Branch (what would we even call it!)
  // so instead we add more global mutable state.
  //
  // We start by assuming that the tickable branch context wont actually tick and set it to start tracking only if
  // it is used to bind a true ticking query.
  def turnOnTrackingForThisContext(): Unit = synchronized {
    if (isActuallyTracking) return // already on
    MonitoringBreadcrumbs.sendTrackingTemporalContextTurnedOn(false)
    _t = true // set to on
  }

  private var _t = false
  protected def isActuallyTracking = _t
  protected def assertIsActuallyTracking(): Unit = {
    if (_t) return
    synchronized {
      if (_t) return
      MonitoringBreadcrumbs.sendTrackingTemporalContextTurnedOn(true)
      _t = true
      throw new GraphInInvalidState(
        "Attempted to perform an operation that requires tracking on a temporal context in the non-tracking state")
    }
  }
}

private[optimus] trait TrackingTemporalContext {
  tts: TemporalSurface with TemporalSurfaceImpl with TemporalContextImpl =>

  // This is unsound because of https://docs.scala-lang.org/scala3/reference/dropped-features/type-projection.html
  // and should be changed
  type TickControl = Map[TrackingLeafTemporalSurface, Instant]

  @entersGraph private[optimus] def advanceSurfaces(advanceTo: TickControl): Unit
  final def surface: TemporalSurfaceImpl with TemporalSurface = this
  def isTrackingEref: Boolean
}

object DependencyTrackerTemporalContexts {
  private class WitnessNode(tracker: TemporalContext, entity: Entity)
      extends AlreadyCompletedPropertyNode[TemporalContext](
        tracker,
        entity,
        NodeTaskInfo.WitnessReactiveTemporalContext) {
    final override def argsHash: Int = tracker.##
    final override def argsEquals(other: NodeKey[_]): Boolean = {
      other match {
        case cNode: WitnessNode => cNode.result eq result
        case _                  => false
      }
    }

    private def writeReplace(): AnyRef = throw new GraphInInvalidState(
      "Attempted to serialize a WitnessTemporalContext node")
  }
}

trait DependencyTrackerTemporalContexts { tracker: TemporalDependencyTracker =>

  final def createTickableBranchTemporalSurface(
      scope: TemporalSurfaceScopeMatcher,
      children: List[TemporalSurface],
      tag: Option[String],
      sourceLocation: SourceLocation): TrackingBranchTemporalSurface =
    new TrackingBranchTemporalSurface(scope, children, tag, sourceLocation)

  final def createTickableBranchTemporalContext(
      scope: TemporalSurfaceScopeMatcher,
      children: List[TemporalSurface],
      tag: Option[String],
      sourceLocation: SourceLocation): TrackingBranchTemporalContext = {
    val result = new TrackingBranchTemporalContext(scope, children, tag, sourceLocation)

    if (Settings.traceCreateTemporalContext) {
      TemporalContextTrace.traceCreated(result)
    }
    result
  }

  final def createTickableLeafTemporalSurface(
      matcher: TemporalSurfaceMatcher,
      transactionTime: Instant,
      endTransactionTime: Instant,
      tag: Option[String],
      sourceLocation: SourceLocation): TrackingLeafTemporalSurface =
    new TrackingLeafTemporalSurfaceImpl(matcher, transactionTime, endTransactionTime, tag, sourceLocation)

  final def createTickableLeafTemporalContext(
      matcher: TemporalSurfaceMatcher,
      transactionTime: Instant,
      tag: Option[String],
      sourceLocation: SourceLocation): TickableTemporalContext =
    createTickableLeafTemporalContext(matcher, transactionTime, TimeInterval.Infinity, tag, sourceLocation)

  final def createTickableLeafTemporalContext(
      matcher: TemporalSurfaceMatcher,
      transactionTime: Instant,
      endTransactionTime: Instant,
      tag: Option[String],
      sourceLocation: SourceLocation): TickableTemporalContext = {
    val result = new TrackingLeafTemporalContextImpl(matcher, transactionTime, endTransactionTime, tag, sourceLocation)

    if (Settings.traceCreateTemporalContext) {
      TemporalContextTrace.traceCreated(result)
    }
    result
  }

  private[optimus] trait TrackingTemporalSurface { tts: TemporalSurface with TemporalSurfaceImpl =>
    private[optimus] final def enclosingTrackingScenario: TemporalDependencyTracker = tracker
  }

  private[optimus] trait TrackingTemporalContextImpl
      extends TrackingTemporalContext
      with TrackingTemporalSurface
      with CanTrack {
    tts: TemporalSurface with TemporalSurfaceImpl with TemporalContextImpl =>

    private def resolver = tracker.scenarioStack.ssShared.environment.entityResolver
    private[optimus] override def resolverOption = Some(resolver)

    def isTrackingEref: Boolean = !trackedEntities.isEmpty

    private val trackedEntities =
      Collections.synchronizedMap(new util.WeakHashMap[EntityReference, EntityKeyWeakReference]())

    private[optimus] val referenceManager = DalPsEntityKeyWeakReferenceManager

    private def propertyKey(e: Entity) = new DependencyTrackerTemporalContexts.WitnessNode(this, e)

    override final def witnessVersion(e: Entity): Unit = {
      if (!isActuallyTracking) return // not tracking, dont care

      e.optimus$requireConstructorComplete()
      val currentNode = EvaluationContext.currentNode
      val eref = e.dal$entityRef
      // called for side effects: the EntityKeyWeakReference dropping out of queue is used to unsubscribe from eref
      // queries.
      trackedEntities.computeIfAbsent(eref, (_) => new EntityKeyWeakReference(e, this, referenceManager))
      // You might think that it would be better to use the ScenarioStack tweakable tracker instead of the one on dt
      // root, but you would be wrong. We might be in an SI stack, or in an InBackground snapshot stack, or in a
      // concurrent child, all of which would have different tweakable trackers than the one we will attempt to
      // invalidate over. This is in part why this whole tracking thing is broken. (OPTIMUS-65625)
      dt.root.tweakableTracker.onTweakableNodeUsedBy(propertyKey(e), currentNode)
    }

    protected[this] def invalidateOnTick(e: Entity): Unit = {
      assertIsActuallyTracking()
      trackedEntities.remove(e.dal$entityRef)
      dt.root.tweakableTracker.invalidateByKey(propertyKey(e), null)
    }

    @node private def loadPersistentEntity(
        ts: LeafTemporalSurfaceImpl,
        eRef: EntityReference,
        temporality: QueryTemporality.At) = {
      val cached = ts.getCachedPersistentEntity(eRef, temporality)
      if (cached.isDefined) cached.get
      else {
        // maybe we are projecting an entity to a temporality where it does not exist
        log.warn(
          "We should not go to the DAL to loadPersistentEntity. We should cache some information as to the start co-ordinates for a eRef")
        val flat = TemporalContext(temporality.validTime, temporality.txTime)
        resolver
          .getPersistentEntityByRef(eRef, flat)
          .getOrElse(throw new IllegalStateException(s"cannot locate eRef $eRef to tick at time $temporality"))
      }
    }

    @entersGraph private[optimus] def advanceSurfaces(advanceTo: TrackingTemporalContext#TickControl): Unit = {
      assertIsActuallyTracking()
      val filter = trackedEntities.keySet().contains _

      val changedRefs = advanceTo.foldLeft(Set[EntityReference]()) { case (refs, (surface, queryTemporality)) =>
        refs ++ surface.advanceTimes(queryTemporality, filter)
      }
      log.debug(s"changedRefs: $changedRefs")

      // for each of the entity references we need to determine the current mapping
      // for the moment this is just the mapping that it has ticked to
      // but will be more complex in the very near future
      val changesToApply: Map[EntityReference, PersistentEntity] = changedRefs.apar.map { eRef =>
        val (ts, _) = advanceTo.head
        eRef -> loadPersistentEntity(ts, eRef, ts.currentTemporality)
      }(Map.breakOut)

      changesToApply foreach { case (_, pe) =>
        val storageInfo = DSIStorageInfo.fromPersistentEntity(pe)
        deserializeImpl(pe, storageInfo, createIfNotUsed = false)
      }
    }

    @node private[optimus] override def deserialize(pe: PersistentEntity, storageInfo: StorageInfo): Entity =
      deserializeImpl(pe, storageInfo, createIfNotUsed = true)

    @node private def deserializeImpl(
        pe: PersistentEntity,
        storageInfo: StorageInfo,
        createIfNotUsed: Boolean): Entity = {
      val ent =
        if (isActuallyTracking) getOrLoadEntityAndTrack(pe, storageInfo, createIfNotUsed)
        else load(pe, storageInfo)

      if (ent != null) {
        val si = ent.dal$storageInfo
        assert(ent.dal$entityRef == pe.entityRef)

        si match {
          case d: DSIStorageInfo if d.lockToken.value != pe.lockToken || d.versionedRef != pe.versionedRef =>
            // need to perform update.
            if (d.versionedRef != pe.versionedRef) {
              ReflectiveEntityPickling.instance
                .unpickleUpdate(new PickledMapWrapper(pe.serialized.properties, ent.dal$temporalContext), ent)
            }
            if (d.lockToken.value != pe.lockToken) {
              ent.dal$storageInfoUpdate(storageInfo)
            }
            invalidateOnTick(ent)
          case _: DSIStorageInfo => // lockToken and versionedRef same so no need to update
          case _ =>
            throw new IllegalStateException(
              "TrackingScenarioTemporalContext cannot deal with an entity with storage info " + si)
        }
      }
      ent
    }

    @node private def getOrLoadEntityAndTrack(
        persistentEntity: PersistentEntity,
        storageInfo: StorageInfo,
        createIfNotUsed: Boolean
    ): Entity = {
      val eRef = persistentEntity.entityRef
      var existingValue = trackedEntities.get(eRef)
      val existingEntity = if (existingValue eq null) null else existingValue.get()
      if ((existingEntity ne null) || !createIfNotUsed) existingEntity
      else {
        // it's important that we don't hold the lock while loading as versioning code or $init methods can refer to
        // other LPRs that causes further loads/tracks. These loads can be stolen by another thread causing a deadlock
        val loadedEntity = load(persistentEntity, storageInfo)
        assert(loadedEntity ne null)

        var result: Entity = null
        do {
          val newValue = new EntityKeyWeakReference(loadedEntity, this, referenceManager)

          val replacedOrInserted = replaceOrInsert(eRef, existingValue, newValue)
          result =
            if (replacedOrInserted) loadedEntity
            else {
              // If the replace/insert failed then another thread updated the entry while the current thread was loading:
              // 1 - If the entry is null then it must have been invalidated since we called replaceOrInsert
              // 2 - If the entry is not null but the entity is null then it must have been GC-ed
              // 3 - If the entry is not null and the entity is equal to the loaded entity then we simply lost the race
              // 4 - If the entry is not null and the entity is not equal to the loaded entity then we have loaded the
              //     entity twice, so we log a warning
              // In cases 1,2,4 we return null to immediately retry, else we return existingEntity

              existingValue = trackedEntities.get(eRef)
              val existingEntity = if (existingValue eq null) {
                log.info(s"loadedEntity $eRef ${loadedEntity.getClass} was invalidated. Retrying")
                null
              } else existingValue.get()
              if (existingEntity ne null) {
                if (existingEntity eq loadedEntity) existingEntity
                else {
                  log.warn(
                    s"loadedEntity $eRef ${loadedEntity.getClass} duplicate load detected. " +
                      s"Maybe some inefficiency caused by versioning. Contact graph team to investigate")
                  assert(
                    loadedEntity.dal$entityRef == existingEntity.dal$entityRef,
                    s"loadedEntity.entityRef vs existing ${loadedEntity.dal$entityRef} ${existingEntity.dal$entityRef}"
                  )
                  assert(
                    loadedEntity.dal$temporalContext == existingEntity.dal$temporalContext,
                    s"loadedEntity.temporalContext vs existing ${loadedEntity.dal$temporalContext} ${existingEntity.dal$temporalContext}"
                  )
                  assert(
                    loadedEntity.dal$storageInfo == existingEntity.dal$storageInfo,
                    s"loadedEntity.storageInfo vs existing ${loadedEntity.dal$storageInfo} ${existingEntity.dal$storageInfo}"
                  )
                  null
                }
              } else {
                log.info(s"loadedEntity $eRef ${loadedEntity.getClass} GC-ed. Retrying")
                null
              }
            }
        } while (result eq null)
        result
      }
    }

    // Map.replace isn't enough because it returns false if the key doesn't exist
    private def replaceOrInsert(
        eRef: EntityReference,
        existingValue: EntityKeyWeakReference,
        newValue: EntityKeyWeakReference
    ): Boolean = {
      trackedEntities.synchronized {
        (trackedEntities.containsKey(eRef) && (trackedEntities.get(eRef) ne existingValue)) || {
          trackedEntities.put(eRef, newValue)
          true
        }
      }
    }
  }

  final private[optimus] class TrackingBranchTemporalSurface(
      val scope: TemporalSurfaceScopeMatcher,
      val children: List[TemporalSurface],
      override protected[optimus] val tag: Option[String],
      override protected[optimus] val sourceLocation: SourceLocation)
      extends BranchTemporalSurfaceImpl
      with TrackingTemporalSurface {

    def canTick: Boolean = true

    final type surfaceForFrozenType = TemporalSurface
    override type childType = TemporalSurface
    type leafForFrozenType = FixedTemporalSurface with BranchTemporalSurface

    override def frozen(tickingTts: Map[Partition, Instant])(implicit
        sourceLocation: SourceLocation): FixedTemporalSurface with BranchTemporalSurface = {
      val newChildren = children map { _.frozen(tickingTts) }
      new FixedBranchTemporalSurfaceImpl(scope, newChildren, tag, sourceLocation)
    }

    override protected def canEqual(ots: TemporalSurface): Boolean = ots.isInstanceOf[TrackingBranchTemporalSurface]
  }

  private[optimus] final class TrackingBranchTemporalContext(
      val scope: TemporalSurfaceScopeMatcher,
      val children: List[TemporalSurface],
      override protected[optimus] val tag: Option[String],
      override protected[optimus] val sourceLocation: SourceLocation)
      extends BranchTemporalSurfaceImpl
      with TickableTemporalContext
      with TemporalContextImpl
      with TrackingTemporalContextImpl {

    type childType = TemporalSurface
    type surfaceForFrozenType = FixedTemporalContextImpl
    type leafForFrozenType = FixedTemporalSurface with BranchTemporalSurface

    override def frozen(tickingTts: Map[Partition, Instant])(implicit
        sourceLocation: SourceLocation): FixedTemporalContextImpl with FixedBranchTemporalContext = {
      val newChildren = children map { _.frozen(tickingTts) }
      FixedBranchTemporalContext(scope, newChildren, tag)
    }

    override protected def canEqual(ots: TemporalSurface): Boolean = ots.isInstanceOf[TrackingBranchTemporalContext]
  }

  private[optimus] trait TrackingLeafTemporalSurfaceBase
      extends TrackingTemporalSurface
      with LeafTemporalSurfaceImpl
      with TrackingLeafTemporalSurface
      with CanTrack {
    private[optimus] val initialTransactionTime: Instant
    private[optimus] val endTransactionTime: Instant

    override private[optimus] def implicitErefCacheIfTracking = synchronized {
      // So what happens if we need one but don't have one? Not sure!
      _streamManager.filter(_ => isActuallyTracking).flatMap(_.implicitErefCache)
    }

    protected object DynamicTransactionTimeContext extends TransactionTimeContext {
      var txTime: Instant = initialTransactionTime
      def frozen = FixedTransactionTimeContext(txTime)
      private[optimus] override def getTTForEvent(cls: String) =
        throw new UnsupportedOperationException("Cannot call getTTForEvent on ticking TemporalContext")
    }
    private val temporalityLock = new ReentrantLock

    @volatile private var _streamManager = Option.empty[StreamManager]
    private[optimus] def getStreamManager(ifAbsent: => StreamManager): StreamManager = synchronized {
      _streamManager match {
        case Some(existing) => existing
        case None =>
          val newSm = ifAbsent
          _streamManager = Some(newSm)
          newSm
      }
    }

    private[optimus] override def currentTemporalityFor(query: TemporalSurfaceQuery): query.TemporalityType = {
      // DAL queries against ticking contexts are not RT and are not tracked correctly for invalidation,
      // so we'd like to see how much this happens
      if (isActuallyTracking) MonitoringBreadcrumbs.sendDalAccessWithTickingContextCrumb(query)
      currentTemporality
    }

    @GuardedBy("temporalityLock")
    private var _currentTemporality: Option[QueryTemporality.At] = None
    protected[optimus] override def currentTemporality: QueryTemporality.At = {
      temporalityLock.lock()
      try {
        if (_currentTemporality.isEmpty) {
          _currentTemporality = Some(
            QueryTemporality.At(validTime = TimeInterval.Infinity, txTime = DynamicTransactionTimeContext.txTime))
        }
        val res = _currentTemporality.get
        log.debug(s"currentTemporality: $res, DynamicTxTimeContext: ${DynamicTransactionTimeContext.txTime}")
        res
      } finally {
        temporalityLock.unlock()
      }
    }

    @impure override private[optimus] def advanceTimes(
        tt: Instant,
        filter: EntityReference => Boolean): Set[EntityReference] = {
      // We need to deserialize some PersistentEntity, which needs graph context
      temporalityLock.lock()
      try {
        log.debug(s"$tt set ${_currentTemporality} to None")
        _currentTemporality = None
      } finally {
        temporalityLock.unlock()
      }

      if (!tt.isBefore(DynamicTransactionTimeContext.txTime)) {
        log.debug(s"$tt updated the DynamicTransactionTimeContext.txTime")
        DynamicTransactionTimeContext.txTime = tt
        implicitErefCacheIfTracking.map(_.timeAdvanced(this, tt, filter)).getOrElse(Set.empty)
      } else {
        throw new IllegalStateException(s"cant take time backwards to $tt!")
      }
    }

    override private[optimus] def prepareTemporality(futureTime: Instant): Unit = {
      require(
        !futureTime.isBefore(DynamicTransactionTimeContext.txTime),
        s"futureTime($futureTime) should be " +
          s"after TrackingLeafTemporalSurface time: ${DynamicTransactionTimeContext.txTime}"
      )
    }

    override private[optimus] def hasPotentialUpdate(tt: Instant) =
      implicitErefCacheIfTracking.exists(_.hasPotentialUpdate(tt))

    override private[optimus] def getCachedPersistentEntity(
        eRef: EntityReference,
        temporality: QueryTemporality.At): Option[PersistentEntity] =
      implicitErefCacheIfTracking.flatMap(_.getPersistentEntity(eRef, temporality))
  }

  private[optimus] final class TrackingLeafTemporalSurfaceImpl(
      val matcher: TemporalSurfaceMatcher,
      val initialTransactionTime: Instant,
      val endTransactionTime: Instant,
      override private[optimus] val tag: Option[String],
      override private[optimus] val sourceLocation: SourceLocation)
      extends TrackingLeafTemporalSurfaceBase {

    def canTick: Boolean = true

    type surfaceForFrozenType = TemporalSurface
    type leafForFrozenType = LeafTemporalSurface

    private[optimus] def frozen(tickingTts: Map[Partition, Instant])(implicit sourceLocation: SourceLocation) = {
      val newTag = tag.map(tag => "$$Frozen from " + tag)
      TemporalSurfaceDefinition.FixedLeafSurface(
        matcher,
        TimeInterval.Infinity,
        // note that when we are preparing the entities for a given tick, txTime hasn't yet been updated, so we rely
        // on the new tt being passed in
        tickingTts.getOrElse(partition, DynamicTransactionTimeContext.txTime),
        newTag
      )
    }
    override protected def canEqual(ots: TemporalSurface): Boolean = ots.isInstanceOf[TrackingLeafTemporalSurfaceImpl]

  }

  private[optimus] final class TrackingLeafTemporalContextImpl(
      val matcher: TemporalSurfaceMatcher,
      private[optimus] val initialTransactionTime: Instant,
      private[optimus] val endTransactionTime: Instant,
      override private[optimus] val tag: Option[String],
      override private[optimus] val sourceLocation: SourceLocation)
      extends TrackingLeafTemporalSurfaceBase
      with TickableTemporalContext
      with TemporalContextImpl
      with TrackingTemporalContextImpl {

    type surfaceForFrozenType = FixedTemporalContextImpl
    type leafForFrozenType = LeafTemporalSurface

    private[optimus] def frozen(tickingTts: Map[Partition, Instant])(implicit sourceLocation: SourceLocation) = {
      val newTag = tag.map(tag => "$$Frozen from " + tag)
      TemporalSurfaceDefinition.FixedLeafContext(
        matcher,
        TimeInterval.Infinity,
        // note that when we are preparing the entities for a given tick, txTime hasn't yet been updated, so we rely
        // on the new tt being passed in
        tickingTts.getOrElse(partition, DynamicTransactionTimeContext.txTime),
        newTag
      )
    }
    override protected def canEqual(ots: TemporalSurface): Boolean = ots.isInstanceOf[TrackingLeafTemporalContextImpl]
  }
}
