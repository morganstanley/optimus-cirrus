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
package optimus.platform.relational.data.tree

import optimus.platform._
import optimus.platform.cm.Knowable
import optimus.platform.pickling.PickledInputStream
import optimus.platform.pickling.Unpickler
import optimus.platform.relational.KeyPropagationPolicy
import optimus.platform.relational.data.mapping.MappingEntity
import optimus.platform.relational.data.mapping.MemberInfo
import optimus.platform.relational.tree.ConditionalElement
import optimus.platform.relational.tree.ElementFactory
import optimus.platform.relational.tree.FuncElement
import optimus.platform.relational.tree.LambdaElement
import optimus.platform.relational.tree.MethodCallee
import optimus.platform.relational.tree.RelationElement
import optimus.platform.relational.tree.RuntimeMethodDescriptor
import optimus.platform.relational.tree.TypeInfo
import optimus.platform.storable.EntityCompanionBase
import optimus.platform.storable.StorableReference

abstract class DbRelationElement(elemType: DbElementType, typeInfo: TypeInfo[_])
    extends RelationElement(elemType, typeInfo) {}

abstract class AliasedElement(elemType: DbElementType, typeInfo: TypeInfo[_], val alias: TableAlias)
    extends DbRelationElement(elemType, typeInfo) {}

class TableElement(alias: TableAlias, val entity: MappingEntity, val name: String)
    extends AliasedElement(DbElementType.Table, TypeInfo.UNIT, alias) {
  override def toString(): String = s"T($name)"
}

class AggregateElement(
    typeInfo: TypeInfo[_],
    val aggregateName: String,
    val arguments: List[RelationElement],
    val isDistinct: Boolean)
    extends DbRelationElement(DbElementType.Aggregate, typeInfo) {}

object AggregateElement {
  def unapply(agg: AggregateElement) = Some(agg.rowTypeInfo, agg.aggregateName, agg.arguments, agg.isDistinct)
}

class ColumnElement(typeInfo: TypeInfo[_], val alias: TableAlias, val name: String, val columnInfo: ColumnInfo)
    extends DbRelationElement(DbElementType.Column, typeInfo) {
  override def toString(): String = s"$alias.C($name)"

  override def hashCode: Int = alias.hashCode + name.hashCode
  override def equals(o: Any): Boolean = o match {
    case c: ColumnElement => (c eq this) || (c.name == name && c.alias == alias)
    case _                => false
  }
}

class OuterJoinedElement(val test: RelationElement, val element: RelationElement, val defaultValue: RelationElement)
    extends DbRelationElement(DbElementType.OuterJoined, element.rowTypeInfo) {}

trait ColumnInfo {
  def columnType: ColumnType
  def unpickler: Option[Unpickler[_]]
}

object ColumnInfo {
  val Calculated = ColumnInfoImpl(ColumnType.Calculated, None)

  final case class ColumnInfoImpl(columnType: ColumnType, unpickler: Option[Unpickler[_]]) extends ColumnInfo

  def from(e: RelationElement): ColumnInfo = {
    e match {
      case c: ColumnElement => c.columnInfo
      case _                => ColumnInfo.Calculated
    }
  }

  def apply(columnType: ColumnType, unpickler: Option[Unpickler[_]]) = ColumnInfoImpl(columnType, unpickler)
}

trait ColumnType
object ColumnType {
  case object Key extends ColumnType
  case object UniqueIndex extends ColumnType
  case object Index extends ColumnType
  case object Default extends ColumnType
  case object EntityRef extends ColumnType
  case object Calculated extends ColumnType
}

final case class NamedValueElement(val name: String, val value: RelationElement)
    extends DbRelationElement(DbElementType.NamedValue, value.rowTypeInfo) {}

class SelectElement(
    alias: TableAlias,
    val columns: List[ColumnDeclaration],
    val from: RelationElement,
    val where: RelationElement,
    val orderBy: List[OrderDeclaration],
    val groupBy: List[RelationElement],
    val skip: RelationElement,
    val take: RelationElement,
    val isDistinct: Boolean,
    val reverse: Boolean)
    extends AliasedElement(DbElementType.Select, TypeInfo.UNIT, alias) {}

class JoinElement(
    val joinType: JoinType,
    val left: RelationElement,
    val right: RelationElement,
    val condition: RelationElement)
    extends DbRelationElement(DbElementType.Join, TypeInfo.UNIT) {}

// when 'viaCollection' is defined, ProjectionElement models a sequence otherwise it models a Query.
class ProjectionElement(
    val select: SelectElement,
    val projector: RelationElement,
    val key: RelationKey[_],
    val keyPolicy: KeyPropagationPolicy,
    val aggregator: LambdaElement = null,
    val viaCollection: Option[Class[_]] = None,
    val entitledOnly: Boolean)
    extends DbRelationElement(DbElementType.Projection, projector.projectedType()) {
  val isSingleton: Boolean = (aggregator ne null) && aggregator.body.rowTypeInfo.clazz == projector.rowTypeInfo.clazz
}

object ProjectionElement {
  def unapply(proj: ProjectionElement) = Some(proj.select, proj.projector, proj.aggregator)
}

final case class DynamicObjectElement(source: RelationElement)
    extends DbRelationElement(DbElementType.DynamicObject, DynamicObject.typeTag)

class DbEntityElement(val entity: MappingEntity, val element: RelationElement)
    extends DbRelationElement(DbElementType.DbEntity, element.projectedType()) {}

class ContainsElement(val element: RelationElement, val values: Either[ScalarElement, List[RelationElement]])
    extends DbRelationElement(DbElementType.Contains, TypeInfo.BOOLEAN) {}

class DALHeapEntityElement(
    val companion: EntityCompanionBase[_],
    typeInfo: TypeInfo[_],
    val members: List[RelationElement],
    val memberNames: List[String])
    extends DbRelationElement(DbElementType.DALHeapEntity, typeInfo) {
  def isStorable: Boolean = companion.info.isStorable
}

final case class EmbeddableCollectionElement(info: MemberInfo, element: RelationElement, foreignKey: RelationElement)
    extends DbRelationElement(DbElementType.EmbeddableCollection, info.memberType)

class EmbeddableCaseClassElement(
    val owner: TypeInfo[_],
    val ownerProperty: String,
    typeInfo: TypeInfo[_],
    val members: List[RelationElement],
    val memberNames: List[String])
    extends DbRelationElement(DbElementType.EmbeddableCaseClass, typeInfo)

final case class TupleElement(elements: List[RelationElement])
    extends DbRelationElement(DbElementType.Tuple, TypeInfo.mkTuple(elements.map(_.projectedType()))) {
  override def toString(): String = s"Tuple(${elements.mkString(", ")})"
}

final case class OptionElement(element: RelationElement)
    extends DbRelationElement(DbElementType.Option, TypeInfo(classOf[Option[_]], element.rowTypeInfo)) {
  override def toString(): String = s"Option($element)"
}

object OptionElement {
  def unapply(e: RelationElement): Option[RelationElement] = e match {
    case o: OptionElement =>
      Some(o.element)
    case ConditionalElement(test, OptionElement(o1), OptionElement(o2), t) =>
      Some(ElementFactory.condition(test, o1, o2, t.typeParams.head))
    case _ =>
      None
  }

  def wrapWithOptionElement(referenceType: TypeInfo[_], e: RelationElement): RelationElement = {
    if (referenceType.clazz == classOf[Option[_]] || referenceType.clazz == classOf[Some[_]])
      OptionElement(wrapWithOptionElement(referenceType.typeParams.head, e))
    else
      e
  }

  def wrapWithOptionElement(reference: RelationElement, e: RelationElement): RelationElement = reference match {
    case OptionElement(x) => wrapWithOptionElement(x, OptionElement(e))
    case _                => e
  }

  def underlying(e: RelationElement): RelationElement = e match {
    case OptionElement(x) => underlying(x)
    case _                => e
  }

  def underlyingValue(o: Any): Any = o match {
    case Some(x) => underlyingValue(x)
    case _       => o
  }
}

object KnowableValueElement {
  def apply(kn: RelationElement): RelationElement = {
    val returnType = kn.rowTypeInfo.typeParams.head
    // we introduce a virtual "def value" method here, we will also handle it in reducer
    val method = new RuntimeMethodDescriptor(kn.rowTypeInfo, "value", returnType)
    ElementFactory.call(kn, method, Nil)
  }

  def unapply(kv: RelationElement): Option[RelationElement] = {
    kv match {
      case FuncElement(mc: MethodCallee, Nil, inst)
          if mc.method.declaringType <:< classOf[Knowable[_]] && mc.method.name == "value" =>
        Some(inst)
      case _ => None
    }
  }
}

abstract class SubqueryElement protected (elemType: DbElementType, typeInfo: TypeInfo[_], val select: SelectElement)
    extends DbRelationElement(elemType, typeInfo) {}

class ScalarElement(typeInfo: TypeInfo[_], s: SelectElement)
    extends SubqueryElement(DbElementType.Scalar, typeInfo, s) {}

class ExistsElement(s: SelectElement) extends SubqueryElement(DbElementType.Exists, TypeInfo.BOOLEAN, s) {}

class AggregateSubqueryElement(
    val groupAlias: TableAlias,
    val aggregateInGroupSelect: RelationElement,
    val aggregateAsSubquery: ScalarElement)
    extends DbRelationElement(DbElementType.AggregateSubquery, aggregateAsSubquery.rowTypeInfo) {}

sealed class TableAlias {
  override def toString(): String = "A:" + this.hashCode
}

final case class ColumnDeclaration(val name: String, val element: RelationElement)

object ColumnDeclaration {
  def getAvailableColumnName(columns: Iterable[ColumnDeclaration], baseName: String): String = {
    var name = baseName
    var n = 0
    while (columns.exists(c => c.name == name)) {
      name = baseName + n
      n += 1
    }
    name
  }
}

sealed trait SortDirection
object SortDirection {
  case object Ascending extends SortDirection
  case object Descending extends SortDirection
}

final case class OrderDeclaration(val direction: SortDirection, val element: RelationElement)

final class DbPickledInputStream(var properties: Map[String, Any], val temporalContext: TemporalContext)
    extends PickledInputStream
    with Serializable {

  def reference: StorableReference = null

  @inline private[this] final def setIfFound(key: String, set: Any => Unit): Boolean = {
    val a = properties.get(key)
    if (a.isDefined) {
      set(a.get)
      true
    } else {
      false
    }
  }

  final def seek[T](k: String, unpickler: Unpickler[T]): Boolean = {
    // Seek should only called from entity constructors on properties that are known to be nonblocking (barring bugs).
    // However, the unpickler code path is necessarily async (unless we duplicate the logic everywhere).
    // In order to support assertAsync tests that load from the DAL, suppress syncStack failure during entity unpickling.
    // Note that we turn it off here instead of in EntitySerialization because we DO want to catch entity constructors
    // that call into async nodes, e.g.:
    //   @entity class Foo { val badProperty = blockingCall }
    setIfFound(
      k,
      { v =>
        value = AdvancedUtils.suppressSyncStackDetection { unpickler.unpickle(v, this) }
      })
  }

  def seekRaw(k: String): Boolean = {
    setIfFound(
      k,
      { v =>
        value = v
      })
  }

  def seekChar(k: String): Boolean = {
    setIfFound(
      k,
      { v =>
        charValue = v.asInstanceOf[Char]; value = v
      })
  }

  def seekDouble(k: String): Boolean = {
    setIfFound(
      k,
      { v =>
        doubleValue = v.asInstanceOf[Double]; value = v
      })
  }

  def seekFloat(k: String): Boolean = {
    setIfFound(
      k,
      { v =>
        floatValue = v.asInstanceOf[Float]; value = v
      })
  }

  def seekInt(k: String): Boolean = {
    setIfFound(
      k,
      { v =>
        intValue = v.asInstanceOf[Int]; value = v
      })
  }

  def seekLong(k: String): Boolean = {
    setIfFound(
      k,
      { v =>
        longValue = v.asInstanceOf[Long]; value = v
      })
  }

  var charValue: Char = _
  var doubleValue: Double = _
  var floatValue: Float = _
  var intValue: Int = _
  var longValue: Long = _
  var value: Any = _
}

object DbPickledInputStream {
  def apply(properties: Map[String, Any]) = new DbPickledInputStream(properties, null)
  def apply(properties: Map[String, Any], temporalContext: TemporalContext) =
    new DbPickledInputStream(properties, temporalContext)
  val EmptyInputStream = new DbPickledInputStream(Map.empty, null)
}
