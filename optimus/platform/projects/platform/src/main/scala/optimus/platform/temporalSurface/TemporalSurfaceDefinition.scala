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
package optimus.platform.temporalSurface

import scala.annotation.nowarn
import java.time.Instant

import optimus.graph.tracking.DependencyTracker
import optimus.graph.tracking.TemporalDependencyTracker
import optimus.platform._
import optimus.platform.temporalSurface.impl._
import optimus.platform.storable.EntityCompanionBase
import optimus.utils.MacroUtils.SourceLocation

/**
 * this object defines an API for building Temporal Surfaces and Temporal Contexts
 *
 * The expected use is
 * {{{
 * import TemporalSurfaceDefinition.*
 * ...
 * val myContext = FixedleafContext(TemporalSurfaceMatchers.all, vt, tt)
 * }}}
 */
object TemporalSurfaceDefinition {

  private[TemporalSurfaceDefinition] def trackingScenario: TemporalDependencyTracker = {
    new TemporalDependencyTracker(DependencyTracker.requireTopDependencyTracker())
  }

  object FixedLeaf {

    /**
     * FixedLeaf is a leaf of a temporal surface. It has a single associated temporal coordinates. It may [or may not]
     * be a temporal context depending on __isTemporalContext__. The temporal coordinates (specified by __vt__ and
     * __tt__) cannot change.
     * @param isTemporalContext
     *   should the returned surface be a temporal context
     * @param matcher
     *   the matcher for the temporal context
     * @param vt
     *   the valid time for the surface
     * @param tt
     *   the transaction time for the surface
     */
    def apply(
        isTemporalContext: Boolean,
        matcher: TemporalSurfaceMatcher,
        vt: Instant,
        tt: Instant,
        tag: Option[String] = None)(implicit sourceLocation: SourceLocation): FixedTemporalSurface =
      if (isTemporalContext) FixedLeafContext(matcher, vt, tt, tag) else FixedLeafSurface(matcher, vt, tt, tag)
  }
  object FixedLeafSurface {

    /**
     * FixedLeafSurface is a leaf of a temporal surface. It has a single associated temporal coordinates. It will not be
     * a temporal context. The temporal coordinates (specified by __vt__ and __tt__) cannot change.
     * @param matcher
     *   the matcher for the temporal context
     * @param vt
     *   the valid time for the surface
     * @param tt
     *   the transaction time for the surface
     */
    def apply(matcher: TemporalSurfaceMatcher, vt: Instant, tt: Instant, tag: Option[String] = None)(implicit
        sourceLocation: SourceLocation): FixedTemporalSurface with LeafTemporalSurface =
      FlatTemporalSurface(matcher, vt, tt, tag)
  }

  def fixedLeafContext(vt: Instant, tt: Instant, tag: Option[String] = None): TemporalContext = {
    FixedLeafContext(TemporalSurfaceMatchers.all, vt, tt, tag)
  }

  object FixedLeafContext {

    /**
     * FixedLeafContext is a leaf of a temporal surface. If has a single associated temporal coordinates. It is also a
     * temporal context The temporal coordinates (specified by __vtc__ and __ttc__) cannot move
     * @param matcher
     *   the matcher for the temporal context
     * @param vt
     *   the valid time for the surface
     * @param tt
     *   the transaction time for the surface
     */
    def apply(matcher: TemporalSurfaceMatcher, vt: Instant, tt: Instant, tag: Option[String] = None)(implicit
        sourceLocation: SourceLocation): FixedTemporalContextImpl with FixedLeafTemporalContext =
      FlatTemporalContext(matcher, vt, tt, tag)
  }

  object TickableLeafSurface {

    /**
     * TickableLeafSurface is a leaf of a temporal surface. If has a single associated temporal coordinates. It is not a
     * temporal context The temporal coordinates (specified by __vtc__ and __ttc__) can change via reactive ticking
     * features, and is under the control of the current tracking scenario
     * @param matcher
     *   the matcher for the temporal context
     * @param tt
     *   the initial transactionTime for the surface
     */
    def apply(matcher: TemporalSurfaceMatcher, tt: Instant, tag: Option[String] = None)(implicit
        sourceLocation: SourceLocation): LeafTemporalSurface = {
      apply(matcher, tt, TimeInterval.Infinity, tag)(sourceLocation)
    }

    def apply(matcher: TemporalSurfaceMatcher, tt: Instant, endTt: Instant, tag: Option[String])(implicit
        sourceLocation: SourceLocation): LeafTemporalSurface = {
      trackingScenario.createTickableLeafTemporalSurface(matcher, tt, endTt, tag, sourceLocation)
    }
  }

  object TickableLeafContext {

    /**
     * TickableLeafContext is a leaf of a temporal surface. If has a single associated temporal coordinates. It is also
     * a temporal context The temporal coordinates (specified by __vtc__ and __ttc__) can change via reactive ticking
     * features, and is under the control of the current tracking scenario
     * @param matcher
     *   the matcher for the temporal context
     * @param tt
     *   the initial transactionTime for the surface. validTime is Infinity
     */
    def apply(matcher: TemporalSurfaceMatcher, tt: Instant, tag: Option[String] = None)(implicit
        sourceLocation: SourceLocation): TickableTemporalContext = {
      apply(matcher, tt, TimeInterval.Infinity, tag)(sourceLocation)
    }

    def apply(matcher: TemporalSurfaceMatcher, tt: Instant, endTt: Instant, tag: Option[String])(implicit
        sourceLocation: SourceLocation): TickableTemporalContext = {
      trackingScenario.createTickableLeafTemporalContext(matcher, tt, endTt, tag, sourceLocation)
    }
  }

  def defaultTickableContext(tt: Instant, endTt: Instant = TimeInterval.Infinity, tag: Option[String] = None)(implicit
      sourceLocation: SourceLocation): TickableTemporalContext = {
    val partitions = partitionMapForNotification.allPartitions
    val children = partitions.map { p =>
      trackingScenario.createTickableLeafTemporalSurface(
        TemporalSurfaceMatchers.default(p),
        tt,
        endTt,
        tag,
        sourceLocation)
    }
    trackingScenario.createTickableBranchTemporalContext(
      TemporalSurfaceMatchers.allScope,
      children.toList,
      tag,
      sourceLocation)
  }

  object FixedBranchSurface {

    /**
     * FixedBranchSurface is a branch of a temporal surface. It contains other [[FixedTemporalSurface]]s, but has no
     * associated temporal coordinates. It is not a temporal context.
     * @param scope
     *   the scope of the branch. If None then the scope is the union of the children __nodes__. This can be used to
     *   limit the tree walking and also to enforce structure
     * @param nodes
     *   the sub-structure, other [[FixedTemporalSurface]]s
     */
    def apply(
        scope: TemporalSurfaceScopeMatcher = TemporalSurfaceMatchers.allScope,
        nodes: List[FixedTemporalSurface],
        tag: Option[String] = None)(implicit
        sourceLocation: SourceLocation): FixedTemporalSurface with BranchTemporalSurface =
      new FixedBranchTemporalSurfaceImpl(scope, nodes, tag, sourceLocation)
  }
  object FixedBranchContext {

    /**
     * FixedBranchContext is a branch of a temporal surface. It contains other [[FixedTemporalSurface]]s, but has no
     * associated temporal coordinates. It is also a temporal context.
     * @param scope
     *   the scope of the branch. If defaultScope then the scope is the union of the children __nodes__. This can be used to
     *   limit the tree walking and also to enforce structure
     * @param nodes
     *   the sub-structure, other [[FixedTemporalSurface]]s
     */
    def apply(
        scope: TemporalSurfaceScopeMatcher = TemporalSurfaceMatchers.allScope,
        nodes: List[FixedTemporalSurface],
        tag: Option[String] = None)(implicit sourceLocation: SourceLocation): FixedBranchTemporalContext =
      FixedBranchTemporalContext(scope, nodes, tag)
  }

  object BranchSurface {

    /**
     * BranchSurface is a branch of a temporal surface. It contains other [[TemporalSurface]]s, but has no associated
     * temporal coordinates. It is not a temporal context.
     * @param scope
     *   the scope of the branch. If None then the scope is the union of the children __nodes__. This can be used to
     *   limit the tree walking and also to enforce structure
     * @param nodes
     *   the sub-structure, other [[TemporalSurface]]s
     */
    def apply(
        scope: TemporalSurfaceScopeMatcher = TemporalSurfaceMatchers.allScope,
        nodes: List[TemporalSurface],
        tag: Option[String] = None)(implicit sourceLocation: SourceLocation): BranchTemporalSurface = {
      if (nodes forall (_.isInstanceOf[FixedTemporalSurface]))
        FixedBranchSurface(scope, nodes map (_.asInstanceOf[FixedTemporalSurface]), tag)
      else trackingScenario.createTickableBranchTemporalSurface(scope, nodes, tag, sourceLocation)
    }
  }

  object TickableBranchContext {
    def apply(tt: Instant)(implicit sourceLocation: SourceLocation): TickableTemporalContext =
      apply(tt, TimeInterval.Infinity, None)(sourceLocation)

    def apply(tt: Instant, endTt: Instant)(implicit sourceLocation: SourceLocation): TickableTemporalContext =
      apply(tt, endTt, None)(sourceLocation)

    def apply(tt: Instant, tag: Option[String])(implicit sourceLocation: SourceLocation): TickableTemporalContext =
      apply(tt, TimeInterval.Infinity, tag)(sourceLocation)

    def apply(tt: Instant, endTt: Instant, tag: Option[String])(implicit
        sourceLocation: SourceLocation): TickableTemporalContext = {
      val partitions = partitionMapForNotification.allPartitions
      val children = partitions.map { p =>
        trackingScenario.createTickableLeafTemporalSurface(
          TemporalSurfaceMatchers.default(p),
          tt,
          endTt,
          tag,
          sourceLocation)
      }
      apply(children.toList, tag)(sourceLocation)
    }

    def apply(nodes: List[TemporalSurface], tag: Option[String] = None)(implicit
        sourceLocation: SourceLocation): TickableTemporalContext =
      trackingScenario.createTickableBranchTemporalContext(TemporalSurfaceMatchers.allScope, nodes, tag, sourceLocation)

    def apply(
        start: Instant,
        firstType: EntityCompanionBase[_],
        otherTypes: EntityCompanionBase[_]*): TickableTemporalContext =
      apply(start, TimeInterval.Infinity, firstType, otherTypes: _*)

    def apply(
        start: Instant,
        endTt: Instant,
        firstType: EntityCompanionBase[_],
        otherTypes: EntityCompanionBase[_]*): TickableTemporalContext = {

      val types: Seq[EntityCompanionBase[_]] = otherTypes :+ firstType

      TickableBranchContext(
        TemporalSurfaceMatchers.matchersByPartition(types).map(m => TickableLeafSurface(m, start, endTt, None)).toList
          ::: FixedLeafSurface(TemporalSurfaceMatchers.all, start, start) :: Nil
      )
    }
  }

  object BranchContext {

    /**
     * BranchSurface is a branch of a temporal surface. It contains other [[TemporalSurface]]s, but has no associated
     * temporal coordinates. It is not a temporal context.
     * @param scope
     *   the scope of the branch. If None then the scope is the union of the children __nodes__. This can be used to
     *   limit the tree walking and also to enforce structure
     * @param nodes
     *   the sub-structure, other [[TemporalSurface]]s
     */
    def apply(
        scope: TemporalSurfaceScopeMatcher = TemporalSurfaceMatchers.allScope,
        nodes: List[TemporalSurface],
        tag: Option[String] = None)(implicit sourceLocation: SourceLocation): TemporalContext = {
      if (nodes forall (_.isInstanceOf[FixedTemporalSurface]))
        FixedBranchContext(scope, nodes map (_.asInstanceOf[FixedTemporalSurface]), tag)
      else trackingScenario.createTickableBranchTemporalContext(scope, nodes, tag, sourceLocation)
    }
  }
  object TraceSurface {

    /**
     * Create a surface that can have data access to {{{traced}}}
     * @param traced
     *   the content whose access it traced
     * @param tag
     *   used for debug in the [[TemporalContextTrace]]. Does not affect functionality in temporal context code.
     * @return
     *   a temporal surface that allows detection of data access
     */
    @nowarn("msg=10500 optimus.platform.temporalSurface.impl.FixedTracingTemporalSurface")
    def apply(traced: FixedTemporalSurface, tag: Option[String])(implicit sourceLocation: SourceLocation) =
      new FixedTracingTemporalSurface(traced, tag, sourceLocation)
  }
}
