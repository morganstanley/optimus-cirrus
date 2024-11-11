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

import optimus.platform._
import optimus.platform.storable._

import optimus.platform.dal.DSIStorageInfo
import optimus.platform.TemporalContext
import optimus.platform.AsyncImplicits._
import optimus.platform.temporalSurface.impl.TemporalSurfaceDataAccess
import java.io.Serializable

import optimus.platform.temporalSurface.impl.BranchTemporalSurfaceImpl
import optimus.platform.temporalSurface.impl.LeafTemporalSurfaceImpl
import optimus.scalacompat.collection._

sealed trait TemporalSurfaceQueryImpl extends TemporalSurfaceQuery
sealed trait TemporalSurfaceEntityListDataAccessOperation[T <: Entity]
    extends TemporalSurfaceQueryImpl
    with TemporalSurfaceQueryWithClass {
  type ResultType = List[T]
  @node final override def toResult(
      contextGenerator: ItemKey => (TemporalContext, ItemData),
      keys: Iterable[ItemKey], // keys may be FinalTypedRefs, e.g.
      storageInfo: Option[StorageInfo] = None): List[T] = {
    keys.apar.map {
      key => // deserialization of entity here with FinaltypedRef keys: <~ TmpCtxtImpl#startDataAccess <~ pluginHack.start
        val (tc, pe: PersistentEntity) = contextGenerator(key)
        if (tc != null && pe != null) // if storageInfo==None, calls global
          tc.deserialize(pe, storageInfo.getOrElse(DSIStorageInfo.fromPersistentEntity(pe))).asInstanceOf[T]
        else throw new IllegalStateException(s"cannot load eRef $key : tc = $tc, pe=$pe")
    }(List.breakOut)
  }
}
sealed trait TemporalSurfaceEntityOptionDataAccessOperation[T <: Entity]
    extends TemporalSurfaceQueryImpl
    with TemporalSurfaceQueryWithClass {
  type ResultType = Option[T]
  @node final override def toResult(
      contextGenerator: ItemKey => (TemporalContext, ItemData),
      keys: Iterable[ItemKey],
      storageInfo: Option[StorageInfo] = None): Option[T] = {
    if (keys.isEmpty) None
    else if (keys.tail.isEmpty) {
      // requires a single item key (assigned or unassigned)
      val head = keys.head
      val (tc, pe) = contextGenerator(head)
      if (tc == null || pe == null || pe == PersistentEntity.Null) None
      else Some(tc.deserialize(pe, storageInfo.getOrElse(DSIStorageInfo.fromPersistentEntity(pe))).asInstanceOf[T])
    } else throw new IllegalArgumentException(s"cannot generate an option from a map of size ${keys.size} - $keys")
  }
}
sealed trait DataQueryByEntityReference[T <: Entity]
    extends TemporalSurfaceQueryImpl
    with TemporalSurfaceQueryWithClass {
  val eRef: ItemKey
  def targetClass: Class[T]
  def classIsConcreteType: Boolean
  def reason: EntityReferenceQueryReason
}
sealed trait QueryByKey[E <: Entity] extends EntityClassBasedQuery[E] {
  val key: SerializedKey
}
object QueryByKey {
  def unapply[E <: Entity](query: QueryByKey[E]) = if (query == null) None else Some(query.key)
}
sealed trait EntityClassBasedQuery[E <: Entity] extends TemporalSurfaceQueryImpl with TemporalSurfaceQueryWithClass {
  def targetClass: Class[E]
}

final case class QueryByClass[E <: Entity](
    dataAccess: TemporalSurfaceDataAccess,
    override val targetClass: Class[E],
    entitledOnly: Boolean = false)
    extends TemporalSurfaceEntityListDataAccessOperation[E]
    with EntityClassBasedQuery[E]

final case class QueryByEntityReference[T <: Entity](
    dataAccess: TemporalSurfaceDataAccess,
    query: EntityReferenceQuery[T])
    extends TemporalSurfaceEntityOptionDataAccessOperation[T]
    with DataQueryByEntityReference[T] {
  override def targetClass: Class[T] = query.clazz
  override def classIsConcreteType: Boolean = query.classIsConcreteType
  override def reason: EntityReferenceQueryReason = query.reason
  override val eRef: EntityReference = query.eRef
}

object EntityReferenceQuery {
  def apply(eRef: EntityReference) =
    new EntityReferenceQuery[Entity](eRef, classOf[Entity], false, EntityReferenceQueryReason.Unknown)
  def apply[T <: Entity](
      eRef: EntityReference,
      clazz: Class[T],
      classIsConcreteType: Boolean,
      reason: EntityReferenceQueryReason) = new EntityReferenceQuery(eRef, clazz, classIsConcreteType, reason)
}
final class EntityReferenceQuery[T <: Entity] private (
    val eRef: EntityReference,
    // Note clazz, classIsConcreteType and reason do not participate in equals
    val clazz: Class[T],
    val classIsConcreteType: Boolean,
    val reason: EntityReferenceQueryReason)
    extends Serializable {
  override def equals(obj: scala.Any): Boolean =
    obj match {
      case ref: EntityReferenceQuery[_] => eRef == ref.eRef
      case _                            => false
    }

  override def hashCode(): Int = eRef.hashCode()
  override def toString: String = s"$eRef $clazz concrete:$classIsConcreteType $reason"
}

/**
 * like QueryByEntityReference, but queries a persistent entity
 */
final case class QueryPersistentEntityByEntityReference(dataAccess: TemporalSurfaceDataAccess, eRef: EntityReference)
    extends TemporalSurfaceQuery
    with DataQueryByEntityReference[Entity] {
  type ResultType = Option[PersistentEntity]
  @node final override def toResult(
      contextGenerator: ItemKey => (TemporalContext, ItemData),
      keys: Iterable[ItemKey],
      storageInfo: Option[StorageInfo] = None) = {
    if (keys.isEmpty) None
    else if (keys.tail.isEmpty) Option(contextGenerator(keys.head)._2)
    // required only one assigned or unassigned item
    else throw new IllegalArgumentException(s"cannot generate an option from with keys size ${keys.size} - $keys")
  }
  override def targetClass: Class[Entity] = classOf[Entity]
  override def classIsConcreteType = false
  // Persistent entity queries are not common, so no analysis of the reason at the moment
  // if it becomes common then we can extends the reasons to support this
  override def reason: EntityReferenceQueryReason = EntityReferenceQueryReason.Unknown
}

object QueryByNonUniqueKey {
  def apply[E <: Entity](
      dataAccess: TemporalSurfaceDataAccess,
      key: NonUniqueKey[E],
      erefs: Set[EntityReference]): QueryByNonUniqueKey[E] = apply(dataAccess, key.toSerializedKey, erefs)
  def apply[E <: Entity](
      dataAccess: TemporalSurfaceDataAccess,
      key: SerializedKey,
      erefs: Set[EntityReference] = Set.empty,
      entitledOnly: Boolean = false): QueryByNonUniqueKey[E] = {
    QueryByNonUniqueKey(dataAccess, key, Class.forName(key.typeName).asInstanceOf[Class[E]], erefs, entitledOnly)
  }
}
final case class QueryByNonUniqueKey[E <: Entity](
    dataAccess: TemporalSurfaceDataAccess,
    key: SerializedKey,
    override val targetClass: Class[E],
    erefs: Set[EntityReference],
    entitledOnly: Boolean)
    extends TemporalSurfaceEntityListDataAccessOperation[E]
    with QueryByKey[E]

object QueryByUniqueKey {
  def apply[E <: Entity](dataAccess: TemporalSurfaceDataAccess, key: UniqueKey[E]): QueryByUniqueKey[E] =
    apply(dataAccess, key.toSerializedKey, entitledOnly = false)
  def apply[E <: Entity](
      dataAccess: TemporalSurfaceDataAccess,
      key: UniqueKey[E],
      entitledOnly: Boolean): QueryByUniqueKey[E] = apply(dataAccess, key.toSerializedKey, entitledOnly)
  def apply[E <: Entity](dataAccess: TemporalSurfaceDataAccess, key: SerializedKey): QueryByUniqueKey[E] =
    apply(dataAccess, key, entitledOnly = false)
  def apply[E <: Entity](
      dataAccess: TemporalSurfaceDataAccess,
      key: SerializedKey,
      entitledOnly: Boolean): QueryByUniqueKey[E] = {
    QueryByUniqueKey(dataAccess, key, Class.forName(key.typeName).asInstanceOf[Class[E]], entitledOnly)
  }
}
final case class QueryByUniqueKey[E <: Entity](
    dataAccess: TemporalSurfaceDataAccess,
    key: SerializedKey,
    override val targetClass: Class[E],
    entitledOnly: Boolean)
    extends TemporalSurfaceEntityOptionDataAccessOperation[E]
    with QueryByKey[E]

final case class QueryByLinkage[E <: Entity](
    dataAccess: TemporalSurfaceDataAccess,
    override val targetClass: Class[E],
    lookupSrc: EntityReference,
    propName: String)
    extends TemporalSurfaceEntityListDataAccessOperation[E]
    with EntityClassBasedQuery[E] {}

/**
 * QueryTree is used to get the QueryTemporarity for PriQL query
 */
private[optimus /*platform*/ ] sealed trait QueryTree extends TemporalSurfaceQueryImpl {
  def left: TemporalSurfaceQuery
  def right: TemporalSurfaceQuery

  @async protected def getMatchResult[T](matchFunc: NodeFunction1[TemporalSurfaceQuery, T]): T

  @async def doScopeMatch(ts: BranchTemporalSurfaceImpl): MatchScope =
    getMatchResult[MatchScope](asNode { q =>
      doQueryScopeMatch(q, ts)
    })

  @async private def doQueryScopeMatch(query: TemporalSurfaceQuery, ts: BranchTemporalSurfaceImpl): MatchScope =
    query match {
      case qt: QueryTree           => qt.doScopeMatch(ts)
      case q: TemporalSurfaceQuery => ts.scope.matchScope(q, ts)
    }

  @async def doMatch(ts: LeafTemporalSurfaceImpl): MatchResult =
    getMatchResult[MatchResult](asNode { q =>
      doQueryMatch(q, ts)
    })

  @async private def doQueryMatch(query: TemporalSurfaceQuery, ts: LeafTemporalSurfaceImpl): MatchResult = query match {
    case qt: QueryTree => qt.doMatch(ts)
    case query         => ts.matcher.matchQuery(query, ts)
  }

  @node final override def toResult(
      contextGenerator: (EntityReference) => (TemporalContext, PersistentEntity),
      keys: Iterable[EntityReference],
      storageInfo: Option[StorageInfo]): ResultType = ???
  override val dataAccess: TemporalSurfaceDataAccess = null
}

private[optimus] final case class AndTemporalSurfaceQuery(left: TemporalSurfaceQuery, right: TemporalSurfaceQuery)
    extends QueryTree {

  /*
   * For AND (&&) operator
   *
   * Always   && Always   = Always
   * Always   && CantTell = Always
   * Always   && Never    = Never // Error ?
   * CantTell && CantTell = CantTell
   * CantTell && Never    = Never
   * Never    && Never    = Never
   */
  @async protected def getMatchResult[T](matchFunc: NodeFunction1[TemporalSurfaceQuery, T]): T = {
    val leftRes = matchFunc(left)
    if (leftRes == NeverMatch) leftRes // one Never must be Never, no need to calc the other half
    else {
      val rightRes = matchFunc(right)
      (leftRes, rightRes) match {
        case (_, NeverMatch)      => rightRes // Never => Never
        case (_, AlwaysMatch)     => rightRes // left is not Never, right is Always => Always
        case (CantTell, CantTell) =>
          // if the left and right side of the query considered in isolation are CantTell, try combining them because
          // together they might be specific enough to be provably convered by the matcher
          val merged = mergeAdjacentKeyQueries
          if (merged ne this) matchFunc(merged)
          else rightRes
        case (AlwaysMatch, CantTell) => leftRes
        case s                       => throw new IllegalArgumentException(s"Invalid match state: $s")
      }
    }
  }

  private def mergeAdjacentKeyQueries: TemporalSurfaceQuery = {
    def visit(t: TemporalSurfaceQuery) = t match {
      case a: AndTemporalSurfaceQuery => a.mergeAdjacentKeyQueries
      case t => t
    }
    // visiting in post-order so that any adjacent keys in our children will have already been merged
    (visit(left), visit(right)) match {
      // if both sides are query by key on the same class, try treating it as a compound key (we don't actually need
      // there to be a compound key in the DAL, we're just checking if a compound key query would be covered by our
      // client-side TS filter)
      case (l: QueryByKey[_], r: QueryByKey[_]) if l.targetClass == r.targetClass =>
        val lProps = l.key.properties
        val rProps = r.key.properties
        // if there are any common properties then they must have the same value
        if (lProps.keySet.intersect(rProps.keySet).forall(k => lProps(k) == rProps(k))) {
          val combinedProperties = SortedPropertyValues(lProps.toSeq ++ rProps.toSeq)
          // Unique vs NonUnique doesn't matter here - only the key properties will be used
           QueryByUniqueKey(l.dataAccess, l.key.copy(combinedProperties), l.targetClass, entitledOnly = false)
        }
        else this
      case _ => this
    }
  }
}

// the OrTemporalSurfaceQuery instance will contain status, which means we can't reuse the instances for resolveQueryTemporality method
private[optimus] final case class OrTemporalSurfaceQuery(left: TemporalSurfaceQuery, right: TemporalSurfaceQuery)
    extends QueryTree {

  /*
   * For OR(||) operator
   *
   * Always   || Always   = Always
   * Always   || CantTell = CantTell
   * Always   || Never    = CantTell
   * CantTell || CantTell = CantTell
   * CantTell || Never    = CantTell
   * Never    || Never    = Never
   */
  @async protected def getMatchResult[T](matchFunc: NodeFunction1[TemporalSurfaceQuery, T]): T = {
    val leftRes = matchFunc(left)
    if (leftRes == CantTell) leftRes // one CantTell must be CantTell, no need to calc the other half
    else {
      val rightRes = matchFunc(right)
      (leftRes, rightRes) match {
        case (_, CantTell)    => rightRes // CantTell => CantTell
        case (l, r) if l == r => rightRes // left == right (Always/Never)
        case (_, _)           => CantTell.asInstanceOf[T] // left != right, (Always || Never -> CantTell)
      }
    }
  }
}
