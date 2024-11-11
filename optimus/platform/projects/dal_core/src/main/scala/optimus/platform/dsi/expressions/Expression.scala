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
package optimus.platform.dsi.expressions

import optimus.entity.EntityInfoRegistry
import optimus.platform.dsi.bitemporal.DSIQueryTemporality
import optimus.platform.dsi.bitemporal.ExpressionQueryCommand
import optimus.platform.dsi.bitemporal.QueryPlan
import optimus.platform.storable.SerializedKey
import optimus.platform.storable.{Entity => StorableEntity}

import scala.annotation.tailrec
import scala.collection.mutable

/**
 * The common query language for DAL, these APIs are not intended to be used by the client directly.
 *
 * Use case 1: val q1 = from(Manager) // we might contain a full list of selected properties? the inferred return type
 * is PersistentEntity val manager = Select(Entity("Manager", at = tc))
 *
 * Use case 2: val q2 = from(Employee).filter(e => e.age > 1 && e.name.startsWith("J")) val employee =
 * Entity("Employee", at = tc) val employeeWithFilter = Select( from = employee, where = Some(AndAlso(
 * GreaterThan(Property(employee.id, "age"), Constant(1, TypeCode.Int)), // we will need a supported function list
 * Function("String.StartsWith", List(Property(employee.id, "name"), Constant("J", TypeCode.String))) )) )
 *
 * Use case 3: val q3 = q1.innerJoin(q2).on(_.id == _.manager) val join = Select(Join( joinType = JoinType.InnerJoin,
 * left = manager, right = employeeWithFilter, on = Equal(Property.key(manager.id, "id"),
 * Property.index(employeeWithFilter.id, "manager")) ))
 *
 * Use case 4: val q4 = q1.sum(_.salary) val sumQuery = Select(// we will combine the extra Get in reducer from =
 * manager, // when properties is set, we should fall back to a common data structure like sql ResultSet properties =
 * List(PropertyDef("_a1", Aggregate(AggregateType.Sum, Some(Property(manager.id, "salary"))))) )
 *
 * Use case 5: ...findByReference(entityRef, tc) val anyEntity = Entity("*", at = tc) val findByRef = Select( from =
 * anyEntity, where = Some(Equal(Property.special(anyEntity.id, "dal$entityRef"), Constant("EMDFTSeMgY4RCMDPBQ==",
 * TypeCode.Reference))) )
 *
 * Use case 6: ...enumerateKeys(indexInfo, qt, rtt) on employee val enumerate1 = Select( from = employee, properties =
 * List(PropertyDef("_keys", Property.key(employee.id, "id", "name")) /*SerializedKey*/) )
 *
 * Use case 7: val q5 = q1.sortBy(_.age).takeRight(10) val sortBy = Select( from = manager, sortBy =
 * List(SortByDef(SortType.Desc, Property(manager.id, "age")))), take = Some(Constant(10, TypeCode.Int), reverse = true
 * )
 */
trait Expression

object Expression {
  def flattenAndAlsoConditions(ex: Expression): List[Expression] = {
    if (ex eq null) Nil
    else {
      var s: List[Expression] = ex :: Nil
      val b = new mutable.ListBuffer[Expression]
      while (s.nonEmpty) {
        val pop = s.head
        s = s.tail
        pop match {
          case Binary(BinaryOperator.AndAlso, left, right) =>
            s = left :: right :: s
          case e => b += e
        }
      }
      b.result()
    }
  }

  def balancedAnd(conds: Seq[Expression]): Expression = {
    balancedBinaryWithOp(BinaryOperator.AndAlso, conds)
  }

  def balancedOr(conds: Seq[Expression]): Expression = {
    balancedBinaryWithOp(BinaryOperator.OrElse, conds)
  }

  @tailrec
  private def balancedBinaryWithOp(op: BinaryOperator, conds: Seq[Expression]): Expression = {
    if (conds.length == 1) {
      conds.head
    } else {
      balancedBinaryWithOp(op, conds.grouped(2).map(_.reduce(Binary(op, _, _))).toVector)
    }
  }
}

final case class Member(name: String, owner: Expression) extends Expression {
  override def toString: String = s"$owner.$name"
}
final case class Constant(value: Any, typeCode: TypeCode) extends Expression {
  override def toString: String = s"$value"
}
final case class Function(method: String, arguments: List[Expression]) extends Expression {
  override def toString: String = s"$method(${arguments.mkString(",")})"
}
final case class Binary(op: BinaryOperator, left: Expression, right: Expression) extends Expression {
  override def toString: String = s"$op($left,$right)"
}
final case class Unary(op: UnaryOperator, e: Expression) extends Expression {
  override def toString: String = s"$op($e)"
}
final case class Condition(check: Expression, ifTrue: Expression, ifFalse: Expression) extends Expression {
  override def toString: String = s"if($check) $ifTrue else $ifFalse"
}
final case class Aggregate(aggregateType: AggregateType, arguments: List[Expression] = Nil, isDistinct: Boolean = false)
    extends Expression {
  override def toString: String = s"$aggregateType(${arguments.mkString(",")})"
}

/**
 * Direct property of a QuerySource (with id)
 *
 * @param owner
 *   Reference thd id of Get/Entity/Event
 * @param propType
 * @param names
 */
final case class Property(propType: PropertyType, names: Seq[String], owner: Id) extends Expression {
  override def toString: String = {
    s"${if (propType == PropertyType.Default) "" else propType}Property(${names.mkString("[", ",", "]")})"
  }
}
// denote that a query's result is a scalar value
final case class Scalar(query: Select) extends Expression
final case class Exists(query: Select) extends Expression
final case class In(e: Expression, values: Either[Select, List[Expression]]) extends Expression

sealed trait UpdateKind
case object DeltaUpdate extends UpdateKind
case object AllRects extends UpdateKind

trait QuerySource extends Expression
final case class Entity(name: String, when: DSIQueryTemporality, superClasses: Seq[String], id: Id = Id())
    extends QuerySource {
  override def toString: String = s"Entity($name, $when)"
}

final case class EntityBitemporalSpace(
    name: String,
    when: DSIQueryTemporality.BitempRange,
    kind: UpdateKind,
    superClasses: Seq[String],
    id: Id = Id())
    extends QuerySource {
  override def toString: String = s"EntityBitemporalSpace($name, $when)"
}

final case class Event(name: String, when: DSIQueryTemporality, id: Id = Id()) extends QuerySource {
  override def toString: String = s"Event($name, $when)"
}
final case class Join(joinType: JoinType, left: QuerySource, right: QuerySource, on: Option[Expression])
    extends QuerySource
final case class Linkage(name: String, when: DSIQueryTemporality, id: Id = Id()) extends QuerySource {
  override def toString: String = s"Linkage($name, $when)"
}
final case class Embeddable(entity: String, property: String, tag: String, id: Id = Id()) extends QuerySource

/**
 * Sql like Select expression, currently DAL only support 'from' and 'where' with Equals & In condition. The other
 * operators (e.g. sortBy, groupBy) are place holders and there is no guarantee to support them.
 */
final case class Select(
    from: QuerySource,
    properties: List[PropertyDef] = Nil /*Nil means all?*/,
    where: Option[Expression] = None,
    sortBy: List[SortByDef] = Nil,
    groupBy: List[Expression] = Nil,
    take: Option[Expression] = None,
    skip: Option[Expression] = None,
    isDistinct: Boolean = false,
    reverse: Boolean = false,
    id: Id = Id())
    extends QuerySource {

  private lazy val prettyPrint = {
    val sb = new StringBuilder()
    sb.append(s"Select(from=$from)")
    if (properties.nonEmpty)
      sb.append(s",props=$properties")
    where.foreach(t => sb.append(s",where=$t"))
    if (sortBy.nonEmpty)
      sb.append(s",sortBy=$sortBy")
    if (groupBy.nonEmpty)
      sb.append(s",groupBy=$groupBy")
    take.foreach(t => sb.append(s",take=$t"))
    skip.foreach(t => sb.append(s",skip=$t"))
    if (isDistinct)
      sb.append(",isDistinct=true")
    if (reverse)
      sb.append(",reverse=true")
    sb.append(")")
    sb.result()
  }

  override def toString: String = prettyPrint
}

final case class PropertyDef(name: String, e: Expression)
final case class SortByDef(sortType: SortType, e: Expression)

sealed trait PropertyType
object PropertyType {
  case object Default extends PropertyType
  case object Key extends PropertyType
  case object UniqueIndex extends PropertyType
  case object Index extends PropertyType
  case object Special extends PropertyType
}

sealed trait JoinType
object JoinType {
  case object InnerJoin extends JoinType
  case object LeftOuter extends JoinType
  case object RightOuter extends JoinType
  case object FullOuter extends JoinType
  case object CrossApply extends JoinType
  case object CrossJoin extends JoinType
  case object OuterApply extends JoinType
}

sealed trait SortType
object SortType {
  case object Asc extends SortType
  case object Desc extends SortType
}

// used to describe the DAL supported type of pickled value
sealed trait TypeCode
object TypeCode {
  // reference FieldProto in dsi.proto
  case object Int extends TypeCode
  case object String extends TypeCode
  case object Double extends TypeCode
  case object Boolean extends TypeCode
  case object Char extends TypeCode
  case object Long extends TypeCode
  case object Short extends TypeCode
  case object Byte extends TypeCode
  case object Blob extends TypeCode
  case object ZonedDateTime extends TypeCode
  case object LocalDate extends TypeCode
  case object LocalTime extends TypeCode
  case object OffsetTime extends TypeCode
  case object ZoneId extends TypeCode
  case object Period extends TypeCode
  case object Sequence extends TypeCode
  case object Tuple2 extends TypeCode
  case object Map extends TypeCode
  case object Reference extends TypeCode
  case object None extends TypeCode
  case object SerializedKey extends TypeCode

  case object PersistentEntity extends TypeCode
  case object BusinessEvent extends TypeCode
  case object Result extends TypeCode
  case object Decimal extends TypeCode
  case object VersionedReference extends TypeCode
  case object Rectangle extends TypeCode
}

sealed trait AggregateType
object AggregateType {
  case object Sum extends AggregateType
  case object Count extends AggregateType
  case object Average extends AggregateType
  case object Min extends AggregateType
  case object Max extends AggregateType
  case object Distinct extends AggregateType
  case object ArrayAgg extends AggregateType
  case object Var_samp extends AggregateType
  case object Var_pop extends AggregateType
  case object Stddev_samp extends AggregateType
  case object Stddev_pop extends AggregateType
  case object Corr extends AggregateType // correlation coefficient
}

sealed trait BinaryOperator
object BinaryOperator {
  case object AndAlso extends BinaryOperator
  case object OrElse extends BinaryOperator
  case object Equal extends BinaryOperator
  case object NotEqual extends BinaryOperator
  case object GreaterThan extends BinaryOperator
  case object GreaterThanOrEqual extends BinaryOperator
  case object LessThan extends BinaryOperator
  case object LessThanOrEqual extends BinaryOperator

  case object Add extends BinaryOperator
  case object Subtract extends BinaryOperator
  case object Multiply extends BinaryOperator
  case object Divide extends BinaryOperator
  case object Modulo extends BinaryOperator
}

sealed trait UnaryOperator
object UnaryOperator {
  case object Not extends UnaryOperator
  // "_.name == None" will be expressed as: IsEmpty(Property("name"))
  case object IsEmpty extends UnaryOperator
  case object Negate extends UnaryOperator
}

/**
 * Unique id
 */
sealed class Id private () {
  override def toString: String = s"Id@$hashCode"
}
object Id {
  val EmptyId = new Id()
  def apply(): Id = new Id()
}

object Property {
  def apply(owner: Id, name: String): Property = {
    Property(PropertyType.Default, Seq(name), owner)
  }

  def key(owner: Id, names: String*): Property = {
    Property(PropertyType.Key, names, owner)
  }

  def uniqueIndex(owner: Id, names: String*): Property = {
    Property(PropertyType.UniqueIndex, names, owner)
  }

  def index(owner: Id, names: String*): Property = {
    Property(PropertyType.Index, names, owner)
  }

  def special(owner: Id, name: String): Property = {
    Property(PropertyType.Special, Seq(name), owner)
  }
}

object In {
  def apply(e: Expression, values: List[Expression]): In = In(e, Right(values))
  def apply(e: Expression, query: Select): In = In(e, Left(query))
}

object ExpressionHelper {
  def buildExpression(keys: Seq[SerializedKey], eid: Id): Expression = {
    def fromSerializedKey(sk: SerializedKey) = {
      (sk.unique, sk.indexed) match {
        case (true, false)  => Property.key(eid, sk.properties.keys.toSeq: _*)
        case (true, true)   => Property.uniqueIndex(eid, sk.properties.keys.toSeq: _*)
        case (false, true)  => Property.index(eid, sk.properties.keys.toSeq: _*)
        case (false, false) => throw new IllegalArgumentException(s"illegal ${sk} passed")
      }
    }
    keys match {
      case Seq(k) => Binary(BinaryOperator.Equal, fromSerializedKey(k), Constant(k, TypeCode.SerializedKey))
      case ks =>
        val values = ks map (Constant(_, TypeCode.SerializedKey))
        In(fromSerializedKey(ks.head), values.toList)
    }
  }

  def buildExpressionQueryCommand[T <: StorableEntity](
      keysSeq: Seq[Seq[SerializedKey]],
      entityCls: Class[T],
      range: DSIQueryTemporality.OpenVtTxRange): ExpressionQueryCommand = {
    require(keysSeq.nonEmpty && keysSeq.forall(_.nonEmpty))
    val eid = Id()

    val info = EntityInfoRegistry.getClassInfo(entityCls.getName)
    val entityClass = entityCls.getName
    val parentEntities: Seq[String] = (info.baseTypes - info).iterator.map { _.runtimeClass.getName }.toIndexedSeq
    val querySource = Entity(entityClass, range, parentEntities, eid)
    val andExpressions: Seq[Expression] = keysSeq map { containsKeys =>
      // check if each key is part of the EntityInfo
      // check if all keys for contains are on same property
      require(
        containsKeys
          .map { k =>
            require(
              info.indexes.exists { i =>
                k.properties.equalNames(
                  i.propertyNames) && i.storableClass.getName == k.typeName && i.indexed == k.indexed && i.unique == k.unique
              },
              s"key:${k} is not part of the entity:${entityClass}"
            )
            (k.typeName, k.indexed, k.unique, k.properties.keys)
          }
          .toSet
          .size == 1,
        s"invalid contains query: ${containsKeys.mkString(";")}"
      )

      ExpressionHelper.buildExpression(containsKeys, eid)
    }
    val where = andExpressions.reduceLeft((l, r) => Binary(BinaryOperator.AndAlso, l, r))
    val select = Select(querySource, Nil, Some(where))
    ExpressionQueryCommand(select, QueryPlan.Default, entitledOnly = false)
  }
}
