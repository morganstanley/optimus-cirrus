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

import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger
import msjava.slf4jutils.scalalog.Logger
import optimus.dsi.partitioning.Partition
import optimus.graph.NodeFuture
import optimus.platform.TemporalContext
import optimus.platform.annotations.nodeSync
import optimus.platform.dal.EntityResolver
import optimus.platform.temporalSurface.impl.EntityQueryData
import optimus.platform.temporalSurface.operations.MatchSourceQuery
import optimus.platform.temporalSurface.operations.TemporalSurfaceQuery
import optimus.platform.temporalSurface.tsprofiler.TemporalSurfaceProfilingDataManager
import optimus.platform.temporalSurface.tsprofiler.TemporalSurfaceProfilingData
import optimus.platform.util.Log
import optimus.utils.MacroUtils.SourceLocation

trait TemporalSurface extends Serializable {
  type childType <: TemporalSurface

  val id: Int = TemporalSurface.idGen.incrementAndGet

  protected def log: Logger = TemporalSurface.logger

  /**
   * indicates if the surface can accept delegation (if it is a context). This is guaranteed to be the same as
   * .isInstanceOf[TemporalContext]
   */
  def acceptDelegation: Boolean = false // isContext

  // default behaviour for surfaces - overridden when we want to chain TemporalSurfaceContexts
  def addToChain(temporalContextChain: List[TemporalContext]): List[TemporalContext] = temporalContextChain

  /**
   * indicates if the surface can tick.
   */
  def canTick: Boolean

  /**
   * indicates if the surface is a Leaf. This is guaranteed to be the same as .isInstanceOf[LeafTemporalSurface]
   */
  def isLeaf: Boolean

  /**
   * child surfaces of the surface. Guaranteed to be empty if .isInstanceOf[LeafTemporalSurface]
   */
  def children: List[childType]

  /**
   * associated tag of the surface
   */
  private[optimus] def tag: Option[String]

  /**
   * associated location tag of the surface (source location)
   */
  private[optimus] def sourceLocation: SourceLocation

  type leafForFrozenType <: TemporalSurface
  type surfaceForFrozenType <: TemporalSurface
  final type fixedType = leafForFrozenType with surfaceForFrozenType with FixedTemporalSurface

  private[optimus] def frozen(tickingTts: Map[Partition, Instant] = Map.empty)(implicit
      sourceLocation: SourceLocation): fixedType
  private[optimus] def resolverOption: Option[EntityResolver]

  /**
   * Finds all (or only the first) LeafTemporalSurfaces which possibly match the query operation. This includes leaves
   * which might or might not match (some or all parts of) the query based on the actual data in the DAL.
   *
   * N.B. unlike [queryTemporalSurface] this method doesn't (always) return enough information to figure out exactly
   * which temporalities would apply to which entities (in the case where there is any data dependency in the matchers)
   *
   * @return
   *   a tuple containing the overall match result (AlwaysMatch, CantTell or NeverMatch) and a list of
   *   EntityTemporalInformation for each possibly matching leaf surface
   */
  @nodeSync
  private[optimus] final def findPossiblyMatchingLeaves(
      operation: TemporalSurfaceQuery,
      firstOnly: Boolean): (MatchSourceQuery, List[EntityTemporalInformation]) = {
    val currentTs = TemporalSurfaceProfilingDataManager.maybeUpdateProfilingTSData(this.asInstanceOf[TemporalContext])
    findPossiblyMatchingLeavesImpl(operation, addToChain(Nil), firstOnly, currentTs)
  }
  private[optimus] final def findPossiblyMatchingLeaves$queued(
      operation: TemporalSurfaceQuery,
      firstOnly: Boolean): NodeFuture[(MatchSourceQuery, List[EntityTemporalInformation])] = {
    val currentTs = TemporalSurfaceProfilingDataManager.maybeUpdateProfilingTSData(this.asInstanceOf[TemporalContext])
    findPossiblyMatchingLeavesImpl$queued(operation, addToChain(Nil), firstOnly, currentTs)
  }

  /**
   * implements [findPossiblyMatchingLeaves]
   */
  @nodeSync
  private[temporalSurface] def findPossiblyMatchingLeavesImpl(
      operation: TemporalSurfaceQuery,
      temporalContextChain: List[TemporalContext],
      firstOnly: Boolean,
      tsProf: Option[TemporalSurfaceProfilingData]): (MatchSourceQuery, List[EntityTemporalInformation])
  private[temporalSurface] def findPossiblyMatchingLeavesImpl$queued(
      operation: TemporalSurfaceQuery,
      temporalContextChain: List[TemporalContext],
      firstOnly: Boolean,
      tsProf: Option[TemporalSurfaceProfilingData]): NodeFuture[(MatchSourceQuery, List[EntityTemporalInformation])]

  /**
   * Queries the temporal surface and returns a QueryData containing the temporality results for the query.
   *
   * N.B. unlike findPossiblyMatchingLeaves (which returns only basic data about leaves which might match), the
   * QueryData contains all of the information needed to figure out exact matches, and may include NodeFunctions which
   * need to be evaluated later to figure out exactly which entities match each temporality.
   *
   * N.B. unlike [[TemporalContext#dataAccess]], this method doesn't actually run the query itself against the DAL
   *
   * @param callingScope
   *   all enclosing TemporalSurfaces that we have traversed so far
   * @param operation
   *   the query operation
   * @param temporalContextChain
   *   all enclosing TemporalContexts that we have traversed while considerDelegation was true
   * @param considerDelegation
   *   true iff temporalContextChain should be updated when recursing to children
   * @param tsProf
   *   profiling data associated with this temporal surface
   */
  @nodeSync
  private[temporalSurface] def queryTemporalSurface(
      callingScope: List[TemporalSurface],
      operation: EntityQueryData,
      temporalContextChain: List[TemporalContext],
      considerDelegation: Boolean,
      tsProf: Option[TemporalSurfaceProfilingData]): EntityQueryData
  private[temporalSurface] def queryTemporalSurface$queued(
      callingScope: List[TemporalSurface],
      operation: EntityQueryData,
      temporalContextChain: List[TemporalContext],
      considerDelegation: Boolean,
      tsProf: Option[TemporalSurfaceProfilingData]): NodeFuture[EntityQueryData]

  override final def toString: String = oneLineSummary

  def oneLineDescription: String = description("", detail = true)
  def oneLineSummary: String = description("", detail = false)
  def details: String = description("\n\t", detail = true)

  protected def showId = true

  protected def descriptionDetail(sep: String): String
  private[optimus] def minimalDescription: String =
    s"(id=$id, tag=${tag.getOrElse("")}, sourceLocation=$sourceLocation)"
  protected def descriptionSummary(sep: String): String = ""
  def description(sep: String, detail: Boolean): String = {
    val tagString = if (!detail || tag.isEmpty) "" else " tag:" + tag.getOrElse("") + " "
    s"${if (canTick) "Tickable" else "Fixed"}${if (isLeaf) "Leaf" else "Branch"}${if (acceptDelegation) "Context"
      else "Surface"} {$sep${if (showId || detail) "id=" + id + " " else ""}$tagString${if (detail) descriptionDetail(sep)
      else descriptionSummary(sep)}}"
  }

  @transient private[this] var _hashCode: Int = _ // OPT cache hash code calculation
  override final def hashCode: Int = {
    if (_hashCode == 0) { // use 0 as a sentinel value for not-yet-computed hash code
      var result = hashCodeImpl
      // in the nigh-inconceivable case that our hash code should be 0, use -1 instead to avoid recomputation
      if (result == 0) result = -1
      _hashCode = result
    }
    _hashCode
  }
  protected[this] def hashCodeImpl: Int = {
    val seed = 17
    var res = 1
    res = res * seed + (if (acceptDelegation) 0 else 1)
    res = res * seed + (if (canTick) id else 0)
    res = res * seed + children.hashCode
    res
  }

  protected def extendEquals(ots: TemporalSurface): Boolean = true
  protected def canEqual(ots: TemporalSurface): Boolean
  override final def equals(o: Any): Boolean = o match {
    case ots: TemporalSurface =>
      if (ots eq this) true
      else // ticking TS have identity equality
      if (canTick || ots.canTick) false
      else {
        canEqual(ots) &&
        acceptDelegation == ots.acceptDelegation &&
        children == ots.children &&
        extendEquals(ots)
      }
    case _ => false
  }
}
object TemporalSurface extends Log {
  def logger: Logger = log
  private val idGen = new AtomicInteger
}

trait TickableTemporalContext extends TemporalContext {
  override val canTick = true
}

trait FixedTemporalSurface extends TemporalSurface {
  type childType <: FixedTemporalSurface
  override final val canTick = false
  type leafForFrozenType = this.type
  type surfaceForFrozenType = this.type
  override final private[optimus] def frozen(tickingTts: Map[Partition, Instant])(implicit
      sourceLocation: SourceLocation) = this
}

trait LeafTemporalSurface extends TemporalSurface {
  type childType <: Nothing
  type leafForFrozenType <: LeafTemporalSurface
  override final def children: Nil.type = Nil
  override final def isLeaf = true
  val matcher: TemporalSurfaceMatcher

  private[optimus] def currentTemporalityFor(operation: TemporalSurfaceQuery): operation.TemporalityType

  override protected def descriptionDetail(sep: String) = ""

  override protected[this] def hashCodeImpl: Int = {
    val seed = 17
    super.hashCodeImpl * seed + matcher.hashCode
  }
  override def extendEquals(ots: TemporalSurface): Boolean =
    if (!super.extendEquals(ots)) false
    else
      ots match {
        case lts: LeafTemporalSurface =>
          matcher == lts.matcher
        case _ => false
      }

}

trait BranchTemporalSurface extends TemporalSurface {
  type branchForFrozenType = BranchTemporalSurface

  /**
   * matcher for scope. This is a quick filter for the rest of the branch. if supplied then the query is matched against
   * the scope to reduce the walking of the rest of the tree and based on the response of
   * [[TemporalSurfaceScopeMatcher.matchScope]] which returns a [[MatchScope]]
   *   - [[AlwaysMatch]] => the children are required to have a [[CompleteMatchAssignment]]
   *   - [[NeverMatch]] => the children are ignored - this branch is not concerned with the query
   *   - [[CantTell]] => depends on the individual items returned by the query
   *   - [[ErrorMatch]] => the children are ignored - the result is an error if None then scope is the same as the 'sum'
   *     of the children if Some then this temporal surface only considers values in the defined scope
   */
  val scope: TemporalSurfaceScopeMatcher

  assert(children.nonEmpty, s"a branch temporal ${if (acceptDelegation) "context" else "surface"} must have children")
  override final def isLeaf = false
  override protected def descriptionDetail(sep: String): String = {
    def indent(text: String) = {
      text.replace("\n", "\n\t\t")
    }
    val sb = new StringBuilder
    sb.append(",").append { sep }.append("children=[")
    if (children.nonEmpty) {
      sb.append(indent(children.head.description(sep, detail = true)))
      children.tail foreach { child =>
        sb.append(",").append(sep)
        sb.append(indent(child.description(sep, detail = true)))
      }
    }
    sb.append("]").toString
  }
  override protected[this] def hashCodeImpl: Int = {
    val seed = 17
    super.hashCodeImpl * seed + scope.hashCode
  }
  override def extendEquals(ots: TemporalSurface): Boolean =
    if (!super.extendEquals(ots)) false
    else
      ots match {
        case bts: BranchTemporalSurface =>
          scope == bts.scope
        case _ => false
      }

}

//bottom level traits -
//Fixed/Tickable
//Surface/Context
//Branch/Leaf

/**
 * a traditional flat DAL surface as a temporal context
 */
trait FixedLeafTemporalContext extends FixedTemporalSurface with LeafTemporalSurface with TemporalContext
