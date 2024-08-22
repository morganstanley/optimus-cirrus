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

import java.io.IOException
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.util.concurrent.ConcurrentHashMap
import optimus.dsi.partitioning.DefaultPartition
import optimus.dsi.partitioning.Partition
import optimus.platform.internal.KnownClassInfo
import optimus.platform.internal.UnknownClassInfo
import optimus.platform.storable.Entity
import optimus.platform.storable.EntityCompanionBase
import optimus.platform.storable.EntityReference
import optimus.platform.storable.PersistentEntity
import optimus.platform.temporalSurface.impl.TemporalSurfaceMatcherImpl
import optimus.platform.temporalSurface.operations._
import optimus.platform.util.ClientEntityHierarchy
import optimus.platform._
import optimus.platform.dsi.bitemporal.proto.TemporalSurfaceMatcherT
import optimus.platform.priql.NamespaceWrapper
import optimus.platform.priql.RelationElementWrapper
import optimus.platform.relational.dal.persistence.DalRelTreeEncoder
import optimus.platform.relational.namespace.Namespace
import optimus.platform.relational.namespace.NamespaceMultiRelation
import optimus.platform.relational.reactive._
import optimus.platform.relational.tree.RelationElement
import optimus.platform.relational.reactive.ReactiveQueryProcessorT
import optimus.platform.storable.SortedPropertyValues

import scala.annotation.tailrec

object TemporalSurfaceMatchers {
  def all: TemporalSurfaceMatcher = DataFreeTemporalSurfaceMatchers.all
  def allScope: TemporalSurfaceScopeMatcher = DataFreeTemporalSurfaceMatchers.allScope
  def none: TemporalSurfaceMatcher = DataFreeTemporalSurfaceMatchers.none

  private[optimus] def default(partition: Partition) = ConstTemporalSurfaceMatcherWithPartition(partition)

  private[this] val instance = new TemporalSurfaceMatchers

  def forClass(types: EntityCompanionBase[_]*): ClassTemporalSurfaceMatcher = instance.forClass(types: _*)

  def forEntityClass(types: Set[Class[_ <: Entity]]): ClassTemporalSurfaceMatcher = {
    instance.forRawClass(types)
  }

  def forQuery(queries: Query[Any]*): TemporalSurfaceMatcher = instance.forQuery(queries: _*)

  def matchersByPartition(types: Seq[EntityCompanionBase[_]]): Seq[ClassTemporalSurfaceMatcher] = {
    // check that all of the supplied classes are @stored
    types foreach { companion =>
      require(companion.info.isStorable, s"${companion.info.runtimeClass.getName} is not @stored")
    }
    // separate by partition
    // two partitions must tick independently, and the partition classes dont overlap
    val byPartition = types.groupBy { companion =>
      partitionMapForNotification.partitionForType(companion.info.runtimeClass.getName)
    }

    byPartition.values.map(types => forClass(types: _*)).toSeq
  }
}

private[optimus] trait WithHierarchyManager {
  def hierarchy = ClientEntityHierarchy.hierarchy
}

// In cases where we need just a WithHierarchyManager, use an object.
object HierarchyManager extends WithHierarchyManager

private[optimus] class TemporalSurfaceMatchers extends WithHierarchyManager with Serializable {
  def forClass(types: EntityCompanionBase[_]*): ClassTemporalSurfaceMatcher = {
    val classes = types.map(_.info.runtimeClass)
    buildClassBased(classes.toSet, Set.empty)
  }

  def forRawClass(classes: Set[Class[_ <: Entity]]): ClassTemporalSurfaceMatcher = {
    buildClassBased(classes, Set.empty)
  }

  def forQuery(queries0: Query[Any]*): TemporalSurfaceMatcher = {
    val (namespaces, classes, relations, queries) = partitionQueries(queries0)

    val classMatcher: Option[TemporalSurfaceMatcherImpl] =
      if (namespaces.nonEmpty || classes.nonEmpty) {
        Some(buildClassBased(classes, namespaces))
      } else None
    val relationMatcher: Option[TemporalSurfaceMatcherImpl] =
      if (relations.nonEmpty)
        Some(QueryBasedTemporalSurfaceMatchers.buildQueryBasedTemporalSurfaceMatchers(queries, relations))
      else None

    val matchers = classMatcher.toList ::: relationMatcher.toList
    DisjunctiveTemporalSurfaceMatcher.build(matchers)
  }

  private def partitionQueries(queries0: Seq[Query[Any]]) = {
    val namespaces = Set.newBuilder[Namespace[_]]
    val classes = Set.newBuilder[Class[_ <: Entity]]
    val relations = List.newBuilder[ReactiveQueryProcessor]
    val queries = List.newBuilder[Query[Any]]

    queries0 foreach { q =>
      q.element match {
        case ns: NamespaceMultiRelation[_] =>
          namespaces += ns.source
        case elem =>
          val processed = ReactiveQueryProcessor.buildFromRelation(elem)
          if (processed.allFilters.isEmpty) {
            classes += processed.entityInfo.runtimeClass
          } else {
            queries += q
            relations += processed
          }
      }
    }
    (namespaces.result(), classes.result(), relations.result(), queries.result())
  }

  protected def buildClassBased(classes: Set[Class[_ <: Entity]], namespaces: Set[Namespace[_]]) =
    ClassTemporalSurfaceMatcher(classes, namespaces, this)
}

private[temporalSurface] object DisjunctiveTemporalSurfaceMatcher {
  def build(matchers: Seq[TemporalSurfaceMatcher]): TemporalSurfaceMatcher = matchers match {
    case zero if zero.isEmpty             => TemporalSurfaceMatchers.none
    case one if one.lengthCompare(1) == 0 => one.head
    case many                             => many.foldLeft(empty)(_ || _)
  }
  private[temporalSurface] val empty =
    new DisjunctiveTemporalSurfaceMatcher(ClassTemporalSurfaceMatcher.empty, QueryBasedTemporalSurfaceMatchers.empty)
}

private[temporalSurface] final case class DisjunctiveTemporalSurfaceMatcher(
    classically: ClassTemporalSurfaceMatcher,
    querulously: QueryBasedTemporalSurfaceMatchers
) extends TemporalSurfaceMatcherImpl {

  @node override def matchQuery(op: TemporalSurfaceQuery, ts: TemporalSurface): MatchResult = {
    val c = classically.matchQuery(op, ts)
    if (c == AlwaysMatch) c
    else if (querulously.isEmpty) NeverMatch
    else c orElse querulously.matchQuery(op, ts)
  }

  @node override def matchSourceQuery(op: TemporalSurfaceQuery, ts: TemporalSurface): MatchSourceQuery = {
    val c = classically.matchSourceQuery(op, ts)
    if (c == AlwaysMatch) c
    else if (querulously.isEmpty) NeverMatch
    else c orElse querulously.matchSourceQuery(op, ts)
  }

  @node override def matchItem(op: TemporalSurfaceQuery, ts: TemporalSurface)(
      key: op.ItemKey): Option[(MatchAssignment, Option[op.ItemData])] = {
    classically.matchItem(op, ts)(key) match {
      case None                       => querulously.matchItem(op, ts)(key)
      case c @ Some((AlwaysMatch, _)) => c
      case c @ Some((assignment, data)) =>
        querulously.matchItem(op, ts)(key) match {
          case Some((assignment1, data1)) => Some((assignment orElse assignment1, data orElse data1))
          case None                       => c
        }
    }
  }

  override def matchesNamespace(namespace: String): Boolean = {
    classically.matchesNamespace(namespace) ||
    querulously.matchesNamespace(namespace)
  }

  override def asQueries: Seq[Query[Any]] = querulously.asQueries
  override def partitions: Set[Partition] = classically.partitions | querulously.partitions
  override def ||(other: TemporalSurfaceMatcher): DisjunctiveTemporalSurfaceMatcher = other match {
    case that: DisjunctiveTemporalSurfaceMatcher =>
      val classically = {
        val classes = this.classically.classes | that.classically.classes
        val namespaces = this.classically.namespaces | that.classically.namespaces
        ClassTemporalSurfaceMatcher(classes, namespaces, this.classically.hierarchyManager)
      }
      val querulously = {
        val parsedRelations = this.querulously.parsedRelations ++ that.querulously.parsedRelations
        val asQueries = this.querulously.asQueries ++ that.querulously.asQueries
        val rqps = this.querulously.reactiveQueryProcessors ++ that.querulously.reactiveQueryProcessors
        new QueryBasedTemporalSurfaceMatchers(parsedRelations)(asQueries, rqps)
      }
      new DisjunctiveTemporalSurfaceMatcher(classically, querulously)
    case that: ClassTemporalSurfaceMatcher =>
      val classically = {
        val classes = this.classically.classes | that.classes
        val namespaces = this.classically.namespaces | that.namespaces
        ClassTemporalSurfaceMatcher(classes, namespaces, this.classically.hierarchyManager)
      }
      new DisjunctiveTemporalSurfaceMatcher(classically, querulously)
    case that: QueryBasedTemporalSurfaceMatchers =>
      val querulously = {
        val parsedRelations = this.querulously.parsedRelations ++ that.parsedRelations
        val asQueries = this.querulously.asQueries ++ that.asQueries
        val rqps = this.querulously.reactiveQueryProcessors ++ that.reactiveQueryProcessors
        new QueryBasedTemporalSurfaceMatchers(parsedRelations)(asQueries, rqps)
      }
      new DisjunctiveTemporalSurfaceMatcher(classically, querulously)
    case _ =>
      throw new UnsupportedOperationException(s"disjunction with custom matcher ${other.getClass} not supported")
  }

}

object ClassBasedTemporalSurfaceMatcher {
  private[this] val _always = Some(AlwaysMatch -> None)
  private def always[T]: Option[(MatchAssignment, Option[T])] = _always
}

abstract class ClassBasedTemporalSurfaceMatcher extends TemporalSurfaceMatcherImpl {
  import ClassBasedTemporalSurfaceMatcher.always

  @node override def matchQuery(operation: TemporalSurfaceQuery, temporalSurface: TemporalSurface): MatchResult = {
    operation match {
      case q: TemporalSurfaceQueryImpl =>
        q match {
          case eq: DataQueryByEntityReference[_] =>
            matchItem(eq, temporalSurface)(eq.eRef) map { _._1 } getOrElse NeverMatch
          case classBased: EntityClassBasedQuery[t] => matchClass(classBased.targetClass)
          case tree: QueryTree =>
            throw new IllegalArgumentException(s"Unexpected query type ${tree.getClass}")
        }
      // all TemporalSurfaceQuery should extend (sealed) TemporalSurfaceQueryImpl
      case q: TemporalSurfaceQuery =>
        throw new IllegalArgumentException(s"Illegal query type - should extend TemporalSurfaceQueryImpl ${q.getClass}")
    }
  }
  @node override def matchSourceQuery(
      operation: TemporalSurfaceQuery,
      temporalSurface: TemporalSurface): MatchSourceQuery = {
    operation match {
      case q: TemporalSurfaceQueryImpl =>
        q match {
          case eq: DataQueryByEntityReference[_] =>
            val concreteOnly = eq.targetClass == classOf[Entity]
            matchSourceEref(eq, temporalSurface, eq.eRef, concreteOnly, eq.reason)
          case classBased: EntityClassBasedQuery[t] =>
            matchItemClass(classBased.targetClass) match {
              case AlwaysMatch => AlwaysMatch
              case CantTell    => CantTell
              case _           => NeverMatch
            }
          case tree: QueryTree =>
            throw new IllegalArgumentException(s"Unexpected query type ${tree.getClass}")
        }
      // all TemporalSurfaceQuery should extend (sealed) TemporalSurfaceQueryImpl
      case q: TemporalSurfaceQuery =>
        throw new IllegalArgumentException(s"Illegal query type - should extend TemporalSurfaceQueryImpl ${q.getClass}")
    }
  }
  @node def matchSourceEref(
      operation: TemporalSurfaceQuery,
      temporalSurface: TemporalSurface,
      eRef: EntityReference,
      concreteOnly: Boolean,
      reason: EntityReferenceQueryReason): MatchSourceQuery = {
    val classInfo = operation.dataAccess.getClassInfo(operation, temporalSurface, eRef, concreteOnly, reason)
    classInfo match {
      case u: UnknownClassInfo =>
        throw new IllegalStateException(s"Cannot load class information for eref ${eRef} ${u.description} ")
      case KnownClassInfo(cls, true) =>
        matchItemClass(cls) match {
          case AlwaysMatch => AlwaysMatch
          case _           => NeverMatch
        }
      case KnownClassInfo(cls, false) =>
        // Specification class only
        require(!concreteOnly)

        matchClass(cls) match {
          case AlwaysMatch             => AlwaysMatch
          case NeverMatch | ErrorMatch => NeverMatch
          case CantTell                => matchSourceEref(operation, temporalSurface, eRef, true, reason)
        }
      case x => throw new MatchError(x)
    }
  }

  @node override def matchItem(operation: TemporalSurfaceQuery, temporalSurface: TemporalSurface)(
      key: operation.ItemKey): Option[(MatchAssignment, Option[operation.ItemData])] = {
    val reason = operation match {
      case eq: DataQueryByEntityReference[_] => eq.reason
      case _                                 => EntityReferenceQueryReason.Internal
    }

    matchSourceEref(operation, temporalSurface, key, false, reason) match {
      case AlwaysMatch => always
      case _           => None
    }
  }

  def matchItemClass[T <: Entity](cls: Class[T]): MatchResult
  def matchClass[T <: Entity](cls: Class[T]): MatchResult
}

private[temporalSurface] final case class ConstTemporalSurfaceMatcherWithPartition(partition: Partition)
    extends ClassBasedTemporalSurfaceMatcher
    with TemporalSurfaceMatcherT {
  override def matchClass[T <: Entity](cls: Class[T]) = {
    if (partitionMapForNotification.partitionForType(cls) == partition) AlwaysMatch
    else NeverMatch
  }
  override private[optimus] def partitions = Set(partition)

  override def toString = s"ConstTemporalSurfaceMatcherWithPartition[partition = ${partition.name}]"

  override def matchItemClass[T <: Entity](cls: Class[T]) = matchClass(cls)

  override val hashCode = partition.hashCode
  override def equals(other: Any) = other match {
    case ConstTemporalSurfaceMatcherWithPartition(p) => p == partition
    case _                                           => false
  }
  private[optimus] override def asQueries = Seq.empty

  private[this] val namespaceCache: ConcurrentHashMap[String, Boolean] = new ConcurrentHashMap[String, Boolean]()
  override private[optimus] def matchesNamespace(namespace: String): Boolean =
    namespaceCache.computeIfAbsent(
      namespace,
      { ns =>
        val map = partitionMapForNotification
        // if there are no partitions defined (e.g. in mock DAL), everything is in default partition
        if (map.partitions.isEmpty) partition == DefaultPartition
        else map.partitionTypeRefMap(partition).exists(_.startsWith(ns))
      }
    )
}

private[temporalSurface] object ClassTemporalSurfaceMatcher {
  def apply(classes: Set[Class[_ <: Entity]], namespaces: Set[Namespace[_]], hierarchyManager: WithHierarchyManager) = {
    val allNames: Set[String] = classes map (_.getName)
    val matchTable =
      MatchTable.build(namespaces.map(n => (n.namespace -> n.includesSubPackage)), allNames, hierarchyManager.hierarchy)
    new ClassTemporalSurfaceMatcher(classes, allNames, namespaces, matchTable)
  }
  private[temporalSurface] val empty = this(Set.empty, Set.empty, HierarchyManager)
}

private[temporalSurface] class ClassTemporalSurfaceMatcher(
    @transient private var _classes: Set[Class[_ <: Entity]],
    @transient private var _allNames: Set[String],
    val namespaces: Set[Namespace[_]],
    protected[temporalSurface] val matchTable: MatchTable)
    extends ClassBasedTemporalSurfaceMatcher
    with TemporalSurfaceMatcherT
    with TemporalSurfaceScopeMatcher {

  def matchClass[T <: Entity](cls: Class[T]): MatchResult = matchTable.tryMatch(cls)

  private[this] val namespaceCache: ConcurrentHashMap[String, Boolean] = new ConcurrentHashMap[String, Boolean]()
  private[optimus] override def matchesNamespace(namespace: String): Boolean =
    namespaceCache.computeIfAbsent(
      namespace,
      ns => matchTable.matchesNamespace(ns)
    )

  override def classes = _classes

  private[optimus] def partitions: Set[Partition] = allNames.map(partitionMapForNotification.partitionForType)

  def allNames: Set[String] = _allNames

  override def namespaceWrappers: Set[NamespaceWrapper] = {
    namespaces.map { n =>
      NamespaceWrapper(n.namespace, n.includesSubPackage)
    }
  }

  private[optimus] def hierarchyManager = HierarchyManager

  override def toString = s"ClassTemporalSurfaceMatcher[classes=$allNames, namespaces=$namespaces]"

  override def matchItemClass[T <: Entity](cls: Class[T]): MatchResult = {
    val result = matchClass(cls)
    result match {
      case AlwaysMatch => AlwaysMatch
      case _           => NeverMatch
    }
  }

  private def calcHashCode() = allNames.hashCode + namespaces.hashCode
  @transient private[this] var _hashCode = calcHashCode()
  override def hashCode = _hashCode
  override def equals(other: Any) = other match {
    case cm: ClassTemporalSurfaceMatcher => cm.allNames == allNames && cm.namespaces == namespaces
    case _                               => false
  }
  private[optimus] override def asQueries = {
    Seq.empty
  }

  @throws(classOf[IOException])
  private def writeObject(out: ObjectOutputStream): Unit = {
    out.defaultWriteObject()
    val sortedClassesNames = allNames.toList.sorted
    out.writeObject(sortedClassesNames)
  }

  @throws(classOf[IOException])
  private def readObject(in: ObjectInputStream): Unit = {
    in.defaultReadObject()

    val sortedClassNames = in.readObject().asInstanceOf[List[String]]
    _allNames = sortedClassNames.toSet

    val classLoader = hierarchyManager.hierarchy.classLoader
    _classes = _allNames.map(classLoader.loadClass(_)).asInstanceOf[Set[Class[_ <: Entity]]]
    _hashCode = calcHashCode()
  }

  @node override def matchScope(operation: TemporalSurfaceQuery, temporalSurface: TemporalSurface): MatchScope =
    matchQuery(operation, temporalSurface) match {
      case AlwaysMatch => AlwaysMatch
      case NeverMatch  => NeverMatch
      case _           => CantTell
    }

  @node override def matchItemScope(operation: TemporalSurfaceQuery, temporalSurface: TemporalSurface)(
      key: operation.ItemKey): MatchItemScope =
    matchItem(operation, temporalSurface)(key) match {
      case Some((assign: MatchAssignment, _)) =>
        assign match {
          case AlwaysMatch => AlwaysMatch
          case ErrorMatch  => ErrorMatch
        }
      case None => NeverMatch
    }
}

private[temporalSurface] final case class PreparedMatchInfo(
    clazz: Class[_],
    conditions: Seq[PersistentEntityCondition],
    matchTable: MatchTable)

object QueryBasedTemporalSurfaceMatchers {
  private[optimus /*platform*/ ] def buildFromReactiveQueryProcessors(rqps: Seq[ReactiveQueryProcessor]) =
    buildQueryBasedTemporalSurfaceMatchers(Nil, rqps)

  private[temporalSurface] def buildQueryBasedTemporalSurfaceMatchers(
      queries: Seq[Query[Any]],
      rqps: Seq[ReactiveQueryProcessor]) = {
    val parsedRelations = rqps.map { p =>
      val allFilters = p.allFilters
      PreparedMatchInfo(
        p.entityInfo.runtimeClass,
        allFilters.map(PersistentEntityCondition.parseCondition(_)),
        MatchTable.build(Set.empty, Set(p.entityInfo.runtimeClass.getName), HierarchyManager.hierarchy)
      )
    }
    new QueryBasedTemporalSurfaceMatchers(parsedRelations)(queries, rqps)
  }
  private[temporalSurface] val empty = new QueryBasedTemporalSurfaceMatchers(Nil)(Nil, Nil)
}

// We use two parameter list for QueryBasedTemporalSurfaceMatchers case class,
// because only the first param list will be used to the instances' hashcode and equality
//
// the equality is defined by the PreparedMatchInfo, because:
// 1. the asQueries is only used inside cachePolicy - and nothing depends on it
// 2. the reactiveQueryProcessors are only used in TemporalSurfaceSerializer, and have require it's empty
// 3. only the parsedRelations define the matcher's behavior
private[temporalSurface] final case class QueryBasedTemporalSurfaceMatchers private[temporalSurface] (
    private[temporalSurface] val parsedRelations: Seq[PreparedMatchInfo])(
    @transient private[optimus] override val asQueries: Seq[Query[Any]],
    @transient override val reactiveQueryProcessors: Seq[ReactiveQueryProcessorT])
    extends TemporalSurfaceMatcherImpl
    with TemporalSurfaceMatcherT {

  /** [[MatchTable]] checking whether any of the queries could possibly match */
  // preCheckClassMatch used to be a @transient lazy val that was causing a (rare) deadlock.
  // We have now relaxed this so that the value is cached, but without a lock.
  @volatile @transient private var cachedPreCheckClassMatch: MatchTable = _
  private[this] def preCheckClassMatch(): MatchTable = {
    if (cachedPreCheckClassMatch eq null) {
      cachedPreCheckClassMatch = MatchTable.union(parsedRelations.map(_.matchTable))
    }
    cachedPreCheckClassMatch
  }

  // false == doesn't match; true == might match
  private[this] def preCheck(op: TemporalSurfaceQuery, temporalSurface: TemporalSurface): Boolean = op match {
    case cb: EntityClassBasedQuery[_] =>
      preCheckClassMatch().tryMatch(cb.targetClass) match {
        case AlwaysMatch | CantTell => true
        case NeverMatch             => false
      }
    case er: DataQueryByEntityReference[_] =>
      op.dataAccess.getClassInfo(op, temporalSurface, er.eRef, false, er.reason) match {
        case KnownClassInfo(cls, _) =>
          preCheckClassMatch().tryMatch(cls) match {
            case AlwaysMatch | CantTell => true
            case NeverMatch             => false
          }
        case _ => true
      }
    case _ => true
  }

  private[temporalSurface] def isEmpty: Boolean = parsedRelations.isEmpty

  override private[optimus] def partitions: Set[Partition] =
    parsedRelations.map(r => partitionMapForNotification.partitionForType(r.clazz)).toSet

  private def matchClassWithProperty(targetClass: Class[_], properties: SortedPropertyValues): MatchResult = {
    @tailrec def quickMatch(matchInfos: Iterator[PreparedMatchInfo], result: MatchResult = NeverMatch): MatchResult = {
      import PersistentEntityCheckResult._
      if (matchInfos.isEmpty) result
      else {
        val matchInfo = matchInfos.next()
        val matchTableResult = matchInfo.matchTable.tryMatch(targetClass)
        val currentResult = matchTableResult.andThen {
          val cResult = matchInfo.conditions.tail.foldLeft(matchInfo.conditions.head.check(properties))((acc, v) => {
            acc.and(v.check(properties))
          })
          cResult match {
            case MATCHED          => AlwaysMatch
            case UNMATCHED        => NeverMatch
            case MISSING_PROPERTY => CantTell
          }
        }
        val combined = result.orElse(currentResult)
        if (combined == AlwaysMatch || combined == ErrorMatch) combined
        else quickMatch(matchInfos, combined)
      }
    }

    quickMatch(parsedRelations.iterator)
  }

  @node def matchQuery(operation: TemporalSurfaceQuery, temporalSurface: TemporalSurface): MatchResult = {
    if (!preCheck(operation, temporalSurface)) NeverMatch
    else {
      operation match {
        case q: TemporalSurfaceQueryImpl =>
          q match {
            case eq: DataQueryByEntityReference[_] =>
              matchItem(eq, temporalSurface)(eq.eRef) map { _._1 } getOrElse NeverMatch
            case _: QueryByLinkage[t] =>
              CantTell // we know the class is correct based on preCheck (so it's not just an optimization anymore)
            case classBased: QueryByClass[_] =>
              matchClassWithProperty(classBased.targetClass, SortedPropertyValues.empty)
            case classBased: QueryByKey[_] =>
              matchClassWithProperty(classBased.targetClass, classBased.key.properties)
            case tree: QueryTree =>
              throw new IllegalArgumentException(s"Unexpected query type ${tree.getClass}")
          }
        // all TemporalSurfaceQuery should extend (sealed) TemporalSurfaceQueryImpl
        case q: TemporalSurfaceQuery =>
          throw new IllegalArgumentException(
            s"Illegal query type - should extend TemporalSurfaceQueryImpl ${q.getClass}")
      }
    }
  }

  @node def matchSourceQuery(operation: TemporalSurfaceQuery, temporalSurface: TemporalSurface): MatchSourceQuery = {
    matchQuery(operation, temporalSurface) match {
      case AlwaysMatch => AlwaysMatch
      case CantTell    => CantTell
      case _           => NeverMatch
    }
  }

  def matchPersistentEntity(data: PersistentEntity): MatchResult = {
    val targetClass = Class.forName(data.className)
    val properties = data.properties
    @tailrec def quickMatch(matchInfos: Iterator[PreparedMatchInfo], result: MatchResult = NeverMatch): MatchResult = {
      import PersistentEntityCheckResult._

      if (matchInfos.isEmpty) result
      else {
        val matchInfo = matchInfos.next()
        val classMatchResult: MatchResult =
          if (matchInfo.clazz.isAssignableFrom(targetClass)) AlwaysMatch
          else NeverMatch
        val currentResult = classMatchResult.andThen {
          val cResult = matchInfo.conditions.tail.foldLeft(matchInfo.conditions.head.check(properties))((acc, v) => {
            acc.and(v.check(properties))
          })
          cResult match {
            case MATCHED          => AlwaysMatch
            case UNMATCHED        => NeverMatch
            case MISSING_PROPERTY => CantTell
          }
        }
        val combined = result.orElse(currentResult)
        if (combined == AlwaysMatch || combined == ErrorMatch) combined
        else quickMatch(matchInfos, combined)
      }
    }

    quickMatch(parsedRelations.iterator)
  }

  @node def matchItem(operation: TemporalSurfaceQuery, temporalSurface: TemporalSurface)(
      key: operation.ItemKey): Option[(MatchAssignment, Option[operation.ItemData])] = {
    require(!temporalSurface.canTick, "QueryBasedTemporalSurfaceMatchers are unsupported with ticking TemporalSurfaces")
    operation match {
      case query: TemporalSurfaceQuery =>
        temporalSurface match {
          case leaf: LeafTemporalSurface =>
            val data = query.dataAccess.getSingleItemData(query, leaf)(key.asInstanceOf[query.ItemKey])
            if (data == null || data == PersistentEntity.Null) {
              None
            } else {
              matchPersistentEntity(data) match {
                case AlwaysMatch => Some(AlwaysMatch -> Some(data.asInstanceOf[operation.ItemData]))
                case _           => None
              }
            }
          case _ => throw new IllegalStateException("matchItem on " + temporalSurface)
        }
    }
  }

  override def relationElementWrappers: Seq[Seq[RelationElementWrapper]] = {
    val encoder = new DalRelTreeEncoder()
    reactiveQueryProcessors.map(_.allFilters).map { res: List[RelationElement] =>
      res.map { re: RelationElement =>
        RelationElementWrapper(encoder.encode(re).toByteArray)
      }
    }
  }

  private[this] val namespaceCache: ConcurrentHashMap[String, Boolean] = new ConcurrentHashMap[String, Boolean]()
  override private[optimus] def matchesNamespace(namespace: String): Boolean =
    namespaceCache.computeIfAbsent(namespace, ns => parsedRelations.exists(r => r.matchTable.matchesNamespace(ns)))
}
