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
import java.util.concurrent.locks.ReentrantLock
import javax.annotation.concurrent.GuardedBy
import optimus.dsi.partitioning.DefaultPartition
import optimus.dsi.partitioning.Partition
import optimus.graph.AlreadyCompletedPropertyNode
import optimus.graph.DiagnosticSettings
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

  private[optimus] def prepareTemporality(futureTickTime: Instant): Unit
}

private[optimus] trait TrackingTemporalContext {
  tts: TemporalSurface with TemporalSurfaceImpl with TemporalContextImpl =>

  // This is unsound because of https://docs.scala-lang.org/scala3/reference/dropped-features/type-projection.html
  // and should be changed
  type TickControl = Map[TrackingLeafTemporalSurface, Instant]
  final def surface: TemporalSurfaceImpl with TemporalSurface = this
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

  private[optimus] trait TrackingTemporalContextImpl extends TrackingTemporalContext with TrackingTemporalSurface {
    tts: TemporalSurface with TemporalSurfaceImpl with TemporalContextImpl =>

    private def resolver = tracker.scenarioStack.ssShared.environment.entityResolver
    private[optimus] override def resolverOption = Some(resolver)

    @node private[optimus] override def deserialize(pe: PersistentEntity, storageInfo: StorageInfo): Entity = {
      load(pe, storageInfo)
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
      with TrackingLeafTemporalSurface {
    private[optimus] val initialTransactionTime: Instant
    private[optimus] val endTransactionTime: Instant

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

    private[optimus] override def currentTemporalityFor(query: TemporalSurfaceQuery): query.TemporalityType =
      currentTemporality

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

    override private[optimus] def prepareTemporality(futureTime: Instant): Unit = {
      require(
        !futureTime.isBefore(DynamicTransactionTimeContext.txTime),
        s"futureTime($futureTime) should be " +
          s"after TrackingLeafTemporalSurface time: ${DynamicTransactionTimeContext.txTime}"
      )
    }
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
