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
package optimus.platform.temporalSurface.operations

import optimus.graph.NodeFuture
import optimus.platform.TemporalContext
import optimus.platform.annotations.nodeSync
import optimus.platform.dal.QueryTemporality
import optimus.platform.storable.StorageInfo
import optimus.platform.storable.EntityReference
import optimus.platform.storable.PersistentEntity
import optimus.platform.temporalSurface.TemporalSurface
import optimus.platform.temporalSurface.LeafTemporalSurface
import optimus.platform.temporalSurface.impl.TemporalSurfaceDataAccess

trait TemporalSurfaceQuery {
  type TemporalityType = QueryTemporality.At
  type ItemData = PersistentEntity
  type ItemKey = EntityReference
  type ResultType <: AnyRef

  @nodeSync def toResult(
      contextGenerator: ItemKey => (TemporalContext, ItemData),
      keys: Iterable[ItemKey],
      storageInfo: Option[StorageInfo] = None): ResultType
  def toResult$queued(
      contextGenerator: ItemKey => (TemporalContext, ItemData),
      keys: Iterable[ItemKey],
      storageInfo: Option[StorageInfo] = None): NodeFuture[ResultType] = null

  def itemKeyName = "EntityReference"
  def itemDataName = "PersistentEntity"
  def temporalityTypeName = "At"

  val dataAccess: TemporalSurfaceDataAccess
}

trait TemporalSurfaceQueryWithClass extends TemporalSurfaceQuery {
  def targetClass: Class[_]
}

sealed trait OperationAssignment[+QT <: QueryTemporality, +DATA] {
  val source: List[TemporalSurface]
  val assignmentId: Int
  def matchAssignment: MatchAssignment
  val context: List[TemporalContext]
}

object OperationAssignment {
  def unapply[QT <: QueryTemporality, DATA](x: OperationAssignment[QT, DATA]) = {
    if (x == null) None else Some((x.source, x.assignmentId, x.matchAssignment, x.context))
  }
}
final case class OperationTemporalAssignment[+QT <: QueryTemporality](
    source: List[TemporalSurface],
    assignmentId: Int,
    context: List[TemporalContext],
    temporalitySource: LeafTemporalSurface)
    extends OperationAssignment[QT, Nothing] {
  override def matchAssignment: MatchAssignment = AlwaysMatch
}

final case class OperationDataAssignment[QT <: QueryTemporality, DATA](
    source: List[TemporalSurface],
    assignmentId: Int,
    context: List[TemporalContext],
    temporalitySource: LeafTemporalSurface,
    data: DATA)
    extends OperationAssignment[QT, DATA] {
  override def matchAssignment: MatchAssignment = AlwaysMatch
}

final case class OperationErrorAssignment(
    source: List[TemporalSurface],
    assignmentId: Int,
    context: List[TemporalContext])
    extends OperationAssignment[Nothing, Nothing] {
  def matchAssignment = ErrorMatch

}

// Composition Rule for MatchResult/MatchSourceQuery/MatchScope etc (all of the MatchEnums)
// keep this in alignment with TestMatchCombinations.scala
//
//     andThen     | AlwaysMatch     CantTell    NeverMatch  ErrorMatch
// --------------------------------------------------------------------
//  AlwaysMatch    | AlwaysMatch     CantTell    NeverMatch  ErrorMatch
//  CantTell       | CantTell        CantTell    NeverMatch  ErrorMatch
//  NeverMatch     | NeverMatch      NeverMatch  NeverMatch  ErrorMatch
//  ErrorMatch     | ErrorMatch      ErrorMatch  ErrorMatch  ErrorMatch
//
//
//     orElse      | AlwaysMatch  CantTell        NeverMatch      ErrorMatch
// --------------------------------------------------------------------------
//  AlwaysMatch    | AlwaysMatch  AlwaysMatch     AlwaysMatch     AlwaysMatch
//  CantTell       | AlwaysMatch  CantTell        CantTell        ErrorMatch
//  NeverMatch     | AlwaysMatch  CantTell        NeverMatch      ErrorMatch
//  ErrorMatch     | ErrorMatch   ErrorMatch      ErrorMatch      ErrorMatch
private[optimus] sealed trait MatchEnum extends Serializable {
  protected def andThen0[U >: this.type](other: U): U
  protected def orElse0[U >: this.type](other: U): U
}

/**
 * result of [[TemporalSurfaceMatcher.matchQuery]]
 */
sealed trait MatchResult extends MatchEnum {
  def andThen(rhs: MatchResult): MatchResult = andThen0(rhs)
  def orElse(rhs: MatchResult): MatchResult = orElse0(rhs)
}

/**
 * result of [[MatchTable.tryMatch]]
 */
sealed trait ClassMatchResult extends MatchResult {
  def andThen(rhs: ClassMatchResult): ClassMatchResult = andThen0(rhs)
  def orElse(rhs: ClassMatchResult): ClassMatchResult = orElse0(rhs)
}

/**
 * result of [[TemporalSurfaceMatcher.matchSourceQuery]]
 */
sealed trait MatchSourceQuery extends MatchEnum {
  def andThen(rhs: MatchSourceQuery): MatchSourceQuery = andThen0(rhs)
  def orElse(rhs: MatchSourceQuery): MatchSourceQuery = orElse0(rhs)
}

/**
 * result of [[TemporalSurfaceScopeMatcher.matchScope]
 */
sealed trait MatchScope extends MatchEnum {
  def andThen(rhs: MatchScope): MatchScope = andThen0(rhs)
  def orElse(rhs: MatchScope): MatchScope = orElse0(rhs)
}

/**
 * result of [[TemporalSurfaceScopeMatcher.matchItemScope]. A subset of [[MatchScope]]
 */
sealed trait MatchItemScope extends MatchScope {
  private[optimus] def andThen(rhs: MatchItemScope): MatchItemScope = andThen0(rhs)
  private[optimus] def orElse(rhs: MatchItemScope): MatchItemScope = orElse0(rhs)
}

/**
 * result of [[TemporalSurfaceMatcher.matchItem]]. As subset of the [[MatchResult]] also used to identify the subset of
 * the MatchResult that are an answer worth recording - they assign something
 */
sealed trait MatchAssignment extends MatchResult {
  def complete: Boolean
  def andThen(rhs: MatchAssignment): MatchAssignment = andThen0(rhs)
  def orElse(rhs: MatchAssignment): MatchAssignment = orElse0(rhs)
}
sealed trait SimpleMatchResult extends MatchResult {
  private[optimus] def andThen(other: SimpleMatchResult): SimpleMatchResult = andThen0(other)
  private[optimus] def orElse(other: SimpleMatchResult): SimpleMatchResult = orElse0(other)
}
sealed trait CompleteMatchAssignment extends MatchAssignment {
  final override def complete = true
}
sealed trait IncompleteMatchAssignment extends MatchAssignment {
  final override def complete = false
}

/**
 * always match for all time
 *
 * For a fixed temporal surface, this indicates that all the query results matches the temporal surface matcher
 * conditions. We know the result set won't change because the temporal surface QueryTemporality is fixed.
 *
 * For ticking temporal surface, this indicates that the query condition is included in the temporal surface matcher
 * conditions. This may be: * Key query V.S. Class/Key matcher of that key * Index query V.S. Class/Index matcher of
 * that exact index * EntityReference query V.S. Class/Key matcher if the entity for the key has the same entity
 * reference If we cannot determine if the query condition matches the temporal surface matcher condition, we cannot be
 * sure that it will AlwaysMatch by the temporal surface because when the surface ticks forward, the result set may
 * change.
 */
case object AlwaysMatch
    extends MatchResult
    with SimpleMatchResult
    with CompleteMatchAssignment
    with MatchItemScope
    with ClassMatchResult
    with MatchSourceQuery {
  override protected def andThen0[U >: this.type](rhs: U): U = rhs
  override protected def orElse0[U >: this.type](rhs: U): U = this
}

/**
 * never match for all time
 *
 * For a fixed temporal surface, this indicates that none of the query results matches the temporal surface matcher
 * conditions.
 *
 * For a ticking temporal surface, this only happens when it's sure that the query condition isn't included in the
 * temporal surface matcher conditions. The two won't have a intersection area regardless of the temporal coordinates.
 * Otherwise, if none of the query results matches the temporal surface matcher, we cannot say that is NeverMatch
 * because as the temporal surface ticks, the result set may change and it may match the matchers if the two intersect.
 */
case object NeverMatch
    extends MatchResult
    with SimpleMatchResult
    with MatchItemScope
    with MatchSourceQuery
    with ClassMatchResult {
  override protected def andThen0[U >: this.type](rhs: U): U = rhs match {
    case ErrorMatch => rhs
    case _          => this
  }
  override protected def orElse0[U >: this.type](rhs: U): U = rhs
}

/**
 * Not enough information to tell yet. The result will depend on the data in the DAL This may be because the query
 * return multiple data entities and some may or may not is included, or just that the metadata about the query is
 * incomplete ( we dont know the type from the entity reference)
 */
case object CantTell extends MatchResult with MatchScope with MatchSourceQuery with ClassMatchResult {
  override protected def andThen0[U >: this.type](rhs: U): U = rhs match {
    case NeverMatch | ErrorMatch => rhs
    case _                       => this
  }
  override protected def orElse0[U >: this.type](rhs: U): U = rhs match {
    case NeverMatch => this
    case _          => rhs
  }
}

/**
 * Error occurred when we perform the match. It has the highest priority when composed with other
 * MatchResult/MatchItemScope
 */
case object ErrorMatch extends MatchResult with SimpleMatchResult with CompleteMatchAssignment with MatchItemScope {
  override def andThen0[U >: this.type](other: U): U = this
  override def orElse0[U >: this.type](other: U): U = this
}
