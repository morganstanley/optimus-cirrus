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

import java.time.Instant
import optimus.dsi.partitioning.DefaultPartition
import optimus.dsi.partitioning.Partition
import optimus.graph.Settings
import optimus.platform.dal.QueryTemporality
import optimus.platform.storable.Entity
import optimus.platform.storable.PersistentEntity
import optimus.platform.storable.StorageInfo
import optimus.platform.temporalSurface.LeafTemporalSurface
import optimus.platform.temporalSurface.TickableTemporalContext
import optimus.platform.temporalSurface._
import optimus.platform.temporalSurface.impl._
import optimus.platform._
import optimus.scalacompat.collection._
import optimus.utils.MacroUtils.SourceLocation

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

private[optimus] sealed trait TrackingTemporalContext {
  tts: TemporalSurface with TemporalSurfaceImpl with TemporalContextImpl =>
  final def surface: TemporalSurfaceImpl with TemporalSurface = this
}

object TickableContexts {

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

  private[optimus] sealed trait TrackingTemporalContextImpl extends TrackingTemporalContext {
    tts: TemporalSurface with TemporalSurfaceImpl with TemporalContextImpl =>

    @node private[optimus] override def deserialize(pe: PersistentEntity, storageInfo: StorageInfo): Entity = {
      load(pe, storageInfo)
    }
  }

  final private[optimus] class TrackingBranchTemporalSurface(
      val scope: TemporalSurfaceScopeMatcher,
      val children: List[TemporalSurface],
      override protected[optimus] val tag: Option[String],
      override protected[optimus] val sourceLocation: SourceLocation)
      extends BranchTemporalSurfaceImpl {

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
      extends LeafTemporalSurfaceImpl
      with TrackingLeafTemporalSurface {
    private[optimus] val initialTransactionTime: Instant
    private[optimus] val endTransactionTime: Instant

    protected[optimus] val currentTemporality: QueryTemporality.At = {
      QueryTemporality.At(validTime = TimeInterval.Infinity, txTime = initialTransactionTime)
    }

    override private[optimus] def prepareTemporality(futureTime: Instant): Unit = {
      require(
        !futureTime.isBefore(initialTransactionTime),
        s"futureTime($futureTime) should be " +
          s"after TrackingLeafTemporalSurface time: ${initialTransactionTime}"
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
        tickingTts.getOrElse(partition, initialTransactionTime),
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
        tickingTts.getOrElse(partition, initialTransactionTime),
        newTag
      )
    }
    override protected def canEqual(ots: TemporalSurface): Boolean = ots.isInstanceOf[TrackingLeafTemporalContextImpl]
  }
}
