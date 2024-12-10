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
package optimus.platform.relational.reactive.filter

import optimus.utils.datetime.ZoneIds
import java.time._

import net.iharder.Base64
import optimus.platform.pickling._
import optimus.platform.storable.{Entity, EntityImpl, EntityReference, ModuleEntityToken}
import optimus.platform.dsi.bitemporal._
import optimus.platform.dsi.bitemporal.proto.Dsi._
import optimus.platform.relational.reactive.{
  FilterClassOption,
  MemberProperty,
  UnsupportedFilterCondition,
  ValueProperty,
  defaultFilterClassOption => _
}
import optimus.utils.datetime.LocalDateOps

import optimus.scalacompat.collection.IterableLike

sealed trait Condition {
  def and(right: Condition): Condition = {
    if (right eq Empty) this
    else {
      this match {
        case Compound(ands, ors) if ors.isEmpty => Compound(ands :+ right, ors)
        case Empty                              => right
        case _                                  => Compound(Seq(this, right), Seq.empty)
      }
    }
  }

  def or(right: Condition): Condition = {
    if (right eq Empty) this
    else {
      this match {
        case Compound(ands, ors) if ands.isEmpty => Compound(ands, ors :+ right)
        case Empty                               => right
        case _                                   => Compound(Seq.empty, Seq(this, right))
      }
    }
  }

  def toString(quoted: Boolean): String = toString
}

object Condition {
  import BinaryOperator._
  /* def fieldFor(tpe: NotificationMessageProto.Type): String = tpe match {
    case NotificationMessageProto.Type.HEARTBEAT          => "heart_beat"
    case NotificationMessageProto.Type.RESET_STATE        => "reset_state"
    case NotificationMessageProto.Type.BEGIN_TRANSACTION  => "begin_transaction"
    case NotificationMessageProto.Type.END_TRANSACTION    => "end_transaction"
    case NotificationMessageProto.Type.NOTIFICATION_ENTRY => "notification_entry"
    case NotificationMessageProto.Type.OBLITERATE_MESSAGE => "obliterate_message"
  }

  def ofType(tpe: NotificationMessageProto.Type)(implicit context: Context): Condition = tpe match {
    case NotificationMessageProto.Type.RESET_STATE =>
      // PQF has problems detecting non-leaf node is null or not. We need to check state of type here
      if (context ne null)
        Binary("type", EQ, tpe).and(
          Binary(s"${fieldFor(NotificationMessageProto.Type.RESET_STATE)}.context.type", IS, null)
            .or(ofContext(tpe, context)))
      else Binary("type", EQ, tpe)
    case _ =>
      if (context ne null) Binary("type", EQ, tpe).and(ofContext(tpe, context))
      else Binary("type", EQ, tpe)
  }

  private def ofContext(tpe: NotificationMessageProto.Type, ctx: Context) = {
    val field = fieldFor(tpe)
    ctx match {
      case NamedContext(name) =>
        Compound(
          Seq(Binary(s"$field.context.type", EQ, ContextProto.Type.NAMED), Binary(s"$field.context.name", EQ, name)))
      case SharedContext(name) =>
        Compound(
          Seq(
            Binary(s"$field.context.type", EQ, ContextProto.Type.SHARED),
            Binary(s"$field.context.shared_name", EQ, name)))
      case UniqueContext(uuid) =>
        Compound(
          Seq(Binary(s"$field.context.type", EQ, ContextProto.Type.UNIQUE), Binary(s"$field.context.uuid", EQ, uuid)))
      case DefaultContext => Binary(s"$field.context.type", EQ, ContextProto.Type.DEFAULT)
    }
  } */
  def ofClass(clazz: Class[_])(implicit filterClassOption: FilterClassOption.Value): Binary = {
    import FilterClassOption._
    filterClassOption match {
      case Hierarchical =>
        Binary("notification_entry.segment.serialized_entity.types", EQ, clazz.getName)
      case Explicit =>
        Binary("notification_entry.segment.serialized_entity.class_name", EQ, clazz.getName)
    }
  }
  def ofEntityReference(eref: EntityReference): Binary = {
    Binary("notification_entry.segment.serialized_entity.entity_ref_string", EQ, Base64.encodeBytes(eref.data))
  }
}

object Empty extends Condition {
  override def toString = ""
}

final case class Binary(
    val property: String,
    val operator: BinaryOperator.Value,
    val value: Any,
    val reverse: Boolean = false)
    extends Condition {
  override val toString = {
    import BinaryOperator._
    val op = operator.repr
    val v =
      if (value == null) "NULL"
      else if (Binary.primitiveTypes(value.getClass)) s"$value"
      else s""""${value}""""
    if (reverse) s"$v $op $property"
    else s"$property $op $v"
  }
}

object Binary {
  val primitiveTypes = Set[Class[_]](
    classOf[Byte],
    classOf[Char],
    classOf[Short],
    classOf[Int],
    classOf[Long],
    classOf[Double],
    classOf[Float],
    classOf[java.lang.Byte],
    classOf[java.lang.Character],
    classOf[java.lang.Short],
    classOf[java.lang.Integer],
    classOf[java.lang.Long],
    classOf[java.lang.Double],
    classOf[java.lang.Float]
  )

  val stringTraverableTypes = Set[Class[_]](
    classOf[Boolean],
    classOf[ZoneId],
    classOf[LocalDate],
    classOf[LocalTime],
    classOf[String],
    classOf[EntityImpl],
    classOf[ZonedDateTime]
  )
}

final case class Compound(val andConditions: Seq[Condition], val orConditions: Seq[Condition] = Seq.empty)
    extends Condition {
  override val toString: String = toString(true)

  override def toString(quote: Boolean): String = {
    val ret =
      if (andConditions.isEmpty) s"${orConditions.mkString(" OR ")}"
      else if (orConditions.isEmpty) s"${andConditions.mkString(" AND ")}"
      else {
        assert(orConditions.size == 1)
        s"${andConditions.mkString(" AND ")} OR ${orConditions.head}"
      }
    if (quote) s"($ret)"
    else ret
  }
}

final case class PropertyCondition(
    val prop: MemberProperty,
    val valueProp: ValueProperty,
    val operator: BinaryOperator.Value = BinaryOperator.EQ,
    reverse: Boolean = false)
    extends Condition {
  // import FieldProto.Type._

  import scala.collection.mutable

  private val name = prop.name
  private val value = valueProp.value

  final class CPSFilterConditionBuilder(pickler: Pickler[_], opt: BinaryOperator.Value)
      extends AbstractPickledOutputStream {
    os =>
    private[this] val buffer = new StringBuilder()
    private val op = if (reverse) opt.reverse.repr else opt.repr

    protected[this] var writeContext: WriteContextStack = new ValueWriteContext(prefix, name)
    private[this] var deepestLevel: Int = 0

    sealed trait WriteContextStack extends WriteContext {
      def parent: WriteContextStack
      val valuePrefix: String
      val currentLevel: Int
      def currentField: Option[String]
      def getResult(): String

      if (currentLevel > deepestLevel)
        deepestLevel = currentLevel
    }

    final class ValueWriteContext(val valuePrefix: String, valueName: String) extends WriteContextStack {
      val currentLevel: Int = 0

      private[this] var res: String = _
      def parent = throw new UnsupportedOperationException

      def getResult(): String = res

      def writeFieldName(k: String) = throw new UnsupportedOperationException

      override def writeBoolean(data: Boolean): Unit = ()
      override def writeChar(data: Char): Unit = ()
      override def writeDouble(data: Double): Unit = ()
      override def writeFloat(data: Float): Unit = ()
      override def writeInt(data: Int): Unit = ()
      override def writeLong(data: Long): Unit = ()
      override def writeRawObject(data: AnyRef) = ??? /* res = data match {
        case lt: LocalTime =>
          s"""$valuePrefix.children[type = "$LOCAL_TIME" AND associated_key = "$valueName"].long_value ${op} ${lt.toNanoOfDay}"""
        case dt: LocalDate =>
          s"""$valuePrefix.children[type = "$LOCAL_DATE" AND associated_key = "$valueName"].long_value ${op} ${LocalDateOps
              .toModifiedJulianDay(dt)}"""
        case zid: ZoneId =>
          s"""$valuePrefix.children[type = "$ZONE_ID" AND associated_key = "$valueName"].string_value ${op} "${zid.getId}""""
        case str: String =>
          // special case for embeddable:
          // if property name is _tag, its actually the class name of an embeddable, should always use `=` operator
          val transformedOp = if (valueName != "_tag") op else "="
          s"""$valuePrefix.children[type = "$STRING" AND associated_key = "$valueName"].string_value ${transformedOp} "$str""""

        case id: java.lang.Integer =>
          s"""$valuePrefix.children[type = "$INT" AND associated_key = "$valueName"].int_value ${op} $data"""
        case char: java.lang.Character =>
          s"""$valuePrefix.children[type = "$CHAR" AND associated_key = "$valueName"].int_value ${op} $data"""
        case f: java.lang.Float =>
          s"""$valuePrefix.children[type = "$DOUBLE" AND associated_key = "$valueName"].double_value ${op} $data"""
        case d: java.lang.Double =>
          s"""$valuePrefix.children[type = "$DOUBLE" AND associated_key = "$valueName"].double_value ${op} $data"""
        case l: java.lang.Long =>
          s"""$valuePrefix.children[type = "$LONG" AND associated_key = "$valueName"].long_value ${op} $data"""
        case b: java.lang.Boolean =>
          s"""$valuePrefix.children[type = "$BOOLEAN" AND associated_key = "$valueName"].bool_value ${op} "$data""""
        case eref: EntityReference =>
          s"""$valuePrefix.children[type = "$ENTITY_REF" AND associated_key = "$valueName"].string_value ${op} "$eref""""
        case met: ModuleEntityToken =>
          s"""$valuePrefix.children[type = "$MODULE_ENTITY_TOKEN" AND associated_key = "$valueName"].string_value ${op} "${met.className}""""
        case zdt: ZonedDateTime =>
          val longValueFilter =
            s"""$valuePrefix.children[type = "$ZONE_DATE_TIME" AND associated_key = "$valueName"].long_value ${op} ${DateTimeSerialization
                .fromInstant(zdt.withZoneSameInstant(ZoneIds.UTC).toInstant)}"""
          val stringValueFilter =
            s"""$valuePrefix.children[type = "$ZONE_DATE_TIME" AND associated_key = "$valueName"].string_value ${op} "${zdt.getZone.getId}" """
          opt match {
            case BinaryOperator.EQ => s"""(${longValueFilter} AND ${stringValueFilter})"""
            case BinaryOperator.NE => s"""(${longValueFilter} OR ${stringValueFilter})"""
            case _ => throw new UnsupportedFilterCondition(s"Unsupported filter operator ${op} on ${ZONE_DATE_TIME}")
          }
        case _ =>
          throw new UnsupportedFilterCondition(s"Unsupported filtering on ${valueName} of type ${data.getClass}")
      } */

      def currentField: Option[String] = Some(valueName)
    }

    final class MapWriteContext(val valuePrefix: String, val parent: WriteContextStack, val currentLevel: Int)
        extends WriteContextStack {
      private[this] var valueWriter: ValueWriteContext = null
      private[this] val conditions: mutable.ListBuffer[String] = mutable.ListBuffer.empty[String]

      def getResult(): String = {
        pickler match {
          case _: OptionPickler[_] if opt == BinaryOperator.NE =>
            s"""((${conditions.mkString(" AND ")}) OR $valuePrefix is null)"""
          case _ =>
            conditions.mkString(" AND ")
        }
      }

      def writeFieldName(k: String): Unit = {
        valueWriter = new ValueWriteContext(valuePrefix, k)
      }

      override def writeBoolean(data: Boolean): Unit = {
        require(valueWriter ne null)
        valueWriter.writeBoolean(data)
        conditions += valueWriter.getResult()
        valueWriter = null
      }
      override def writeChar(data: Char): Unit = {
        require(valueWriter ne null)
        valueWriter.writeChar(data)
        conditions += valueWriter.getResult()
        valueWriter = null
      }
      override def writeDouble(data: Double): Unit = {
        require(valueWriter ne null)
        valueWriter.writeDouble(data)
        conditions += valueWriter.getResult()
        valueWriter = null
      }
      override def writeFloat(data: Float): Unit = {
        require(valueWriter ne null)
        valueWriter.writeFloat(data)
        conditions += valueWriter.getResult()
        valueWriter = null
      }
      override def writeInt(data: Int): Unit = {
        require(valueWriter ne null)
        valueWriter.writeInt(data)
        conditions += valueWriter.getResult()
        valueWriter = null
      }
      override def writeLong(data: Long): Unit = {
        require(valueWriter ne null)
        valueWriter.writeLong(data)
        conditions += valueWriter.getResult()
        valueWriter = null
      }
      override def writeRawObject(data: AnyRef): Unit = {
        require(valueWriter ne null)
        valueWriter.writeRawObject(data)
        conditions += valueWriter.getResult()
        valueWriter = null
      }

      def add(cond: String) = conditions += cond
      def currentField: Option[String] = valueWriter.currentField
    }

    final class ArrayWriteContext(val valuePrefix: String, val parent: WriteContextStack, val currentLevel: Int)
        extends WriteContextStack {
      import scala.collection.mutable.ArrayBuffer

      private[this] val buf = ArrayBuffer[Any]()
      private var tpe: Any/* : FieldProto.Type */ = _
      private var valueType: String = _

      def getResult(): String = {
        if (operator == BinaryOperator.EQ) {
          if (!buf.isEmpty) { // For Some(_)
            s"""$valuePrefix.children[type = "$tpe"].${valueType} $op ${buf.result().mkString(",")}"""
          } else { // For None
            s"""$valuePrefix.children is null"""
          }
        } else if (operator == BinaryOperator.NE) {
          if (!buf.isEmpty) { // For Some(_)
            s"""($valuePrefix.children[type = "$tpe"].${valueType} $op ${buf.result().mkString(",")} OR """ +
              s"""$valuePrefix.children is null)"""
          } else { // For None
            s"""$valuePrefix.children is not null"""
          }
        } else if (operator == BinaryOperator.IN) {
          s"""$valuePrefix.children[type = "$tpe" AND associated_key = "$name"].${valueType} ${op} (${buf
              .result()
              .mkString(",")})"""
        } else
          throw new UnsupportedOperationException
      }

      def writeFieldName(k: String): Unit =
        throw new UnsupportedOperationException("Sequences do not support named fields")

      override def writeBoolean(data: Boolean): Unit = ()
      override def writeChar(data: Char): Unit = ()
      override def writeDouble(data: Double): Unit = ()
      override def writeFloat(data: Float): Unit = ()
      override def writeInt(data: Int): Unit = ()
      override def writeLong(data: Long): Unit = ()
      override def writeRawObject(data: AnyRef) = ??? /* data match {
        case lt: LocalTime =>
          tpe = LOCAL_TIME
          valueType = "long_value"
          buf += lt.toNanoOfDay

        case dt: LocalDate =>
          tpe = LOCAL_DATE
          valueType = "long_value"
          buf += LocalDateOps.toModifiedJulianDay(dt)

        case zid: ZoneId =>
          tpe = ZONE_ID
          valueType = "string_value"
          buf += s""""${zid.getId}""""

        case zdt: ZonedDateTime =>
          tpe = ZONE_DATE_TIME
          valueType = "long_value"
          buf += s"""${DateTimeSerialization.fromInstant(zdt.withZoneSameInstant(ZoneIds.UTC).toInstant)}"""

        case str: String =>
          tpe = STRING
          valueType = "string_value"
          buf += s""""$str""""

        case id: java.lang.Integer =>
          tpe = INT
          valueType = "int_value"
          buf += data

        case char: java.lang.Character =>
          tpe = CHAR
          valueType = "int_value"
          buf += data

        case f: java.lang.Float =>
          tpe = DOUBLE
          valueType = "double_value"
          buf += data

        case d: java.lang.Double =>
          tpe = DOUBLE
          valueType = "double_value"
          buf += data

        case l: java.lang.Long =>
          tpe = LONG
          valueType = "long_value"
          buf += data

        case b: java.lang.Boolean =>
          tpe = BOOLEAN
          valueType = "bool_value"
          buf += s""""$data""""

        case eref: EntityReference =>
          tpe = ENTITY_REF
          valueType = "string_value"
          buf += s""""$eref""""

        case moduleToken: ModuleEntityToken =>
          tpe = MODULE_ENTITY_TOKEN
          valueType = "string_value"
          buf += s""""${moduleToken.className}""""

        case unspported =>
          throw new UnsupportedFilterCondition(s"Unsupported filtering on ${name} of type ${data.getClass}")
      } */

      def currentField: Option[String] = parent.currentField
    }

    override def writeFieldName(k: String) = writeContext.writeFieldName(k)

    override def writeStartArray(): Unit = {
      val prefix = if (opt == BinaryOperator.IN) {
        writeContext.valuePrefix
      } else {
        if (writeContext.currentLevel > 0 && writeContext.currentField == writeContext.parent.currentField) {
          // coping nested Option rhs
          writeContext.valuePrefix + """.children[type = "SEQ"]"""
        } else {
          // coping Option field in Embeddable/Entity
          writeContext.valuePrefix + s""".children[type = "SEQ" AND associated_key = "${writeContext.currentField.get}"]"""
        }
      }

      writeContext = new ArrayWriteContext(prefix, writeContext, writeContext.currentLevel + 1)
    }

    override def writeStartObject(): Unit = {
      val prefix = pickler match {
        case _: OptionPickler[_] =>
          // this is Option[Embeddable]
          s"""${writeContext.valuePrefix}.children[type = "MAP"]"""
        case _ =>
          // normal map
          s"""${writeContext.valuePrefix}.children[type = "MAP" AND associated_key = "${writeContext.currentField.get}"]"""
      }
      writeContext = new MapWriteContext(prefix, writeContext, writeContext.currentLevel + 1)
    }

    override def writeEndArray(): Unit = {
      if (writeContext.currentLevel == deepestLevel)
        buffer.append(writeContext.getResult())
      writeContext = writeContext.parent
    }

    override def writeEndObject(): Unit = {
      writeContext.parent match {
        case mwc: MapWriteContext =>
          // handle the @embeddable contains another embeddable
          mwc.add(writeContext.getResult())
        case other =>
          if (buffer.nonEmpty)
            buffer.append(" AND ")
          buffer.append(writeContext.getResult())
      }
      writeContext = writeContext.parent
    }

    override def writeBoolean(data: Boolean) = writeContext.writeBoolean(data)
    override def writeChar(data: Char) = writeContext.writeInt(data)
    override def writeDouble(data: Double) = writeContext.writeDouble(data)
    override def writeFloat(data: Float) = writeContext.writeFloat(data)
    override def writeInt(data: Int) = writeContext.writeInt(data)
    override def writeLong(data: Long) = writeContext.writeLong(data)
    override def writeRawObject(data: AnyRef) = {
      writeContext.writeRawObject(data)
      if (writeContext.isInstanceOf[ValueWriteContext])
        buffer.append(writeContext.getResult())
    }

    override def writeEntity(entity: Entity) = {
      val obj =
        if (entity.$isModule)
          ModuleEntityToken(entity.getClass.getName)
        else if (getEntityRef(entity) eq null)
          throw new TemporaryEntityException(entity, writeContext.currentField.orNull)
        else
          getEntityRef(entity)

      writeRawObject(obj)
    }

    def getEntityRef(entity: Entity) = Option(entity.dal$entityRef).getOrElse(null)

    def build(): String = buffer.toString()
  }

  private val typeMisMatchErrorFmt = """Type mismatch between property type %s of %s and %s"""
  private val prefix = prop.prefix

  private def checkType(propClz: Class[_], valueClz: Class[_]): Unit = {
    if (!propClz.isAssignableFrom(valueClz) && javaType(propClz) != valueClz)
      throw new UnsupportedFilterCondition(typeMisMatchErrorFmt.format(propClz.getName, name, valueClz.getName))
  }

  private def genCondtion(pickler: Pickler[_], v: Any, opt: BinaryOperator.Value): String = {
    val builder = new CPSFilterConditionBuilder(pickler, opt)
    pickler.asInstanceOf[Pickler[Any]].pickle(v, builder)
    builder.build()
  }

  override val toString = {
    val propType = prop.propType
    if (operator != BinaryOperator.IN) {
      // handle common cases
      val valueType = value.getClass
      checkType(propType, valueType)
      genCondtion(prop.pickler, value, operator)
    } else {
      // handle contains case (the operator must be IN)
      value match {
        case it: IterableLike[_, _] =>
          if (it.headOption.isDefined) {
            val valueType = it.head.getClass
            checkType(propType, valueType)

            if (prop.isEmbeddable) {
              // the embeddable contains can't be transformed into in operation
              // instead, we have to use 'or' to combine the conditions together
              val conds: List[String] = it.toList.map(v => genCondtion(prop.pickler, v, BinaryOperator.EQ))
              s"""(${conds.mkString(" OR ")})"""
            } else {
              // for the simple types, we just generate something like: prop IN (...)
              genCondtion(DefaultPicklers.iterablePickler(prop.pickler), value, operator)
            }

          } else throw new UnsupportedFilterCondition("filter on empty collection won't return any value")
      }
    }
  }

  // a work around for !classOf[java.lang.Integer].isAssignableFrom(classOf[scala.Int])
  private def javaType(clazz: Class[_]): Class[_] = {
    if (clazz == classOf[Int])
      classOf[java.lang.Integer]
    else if (clazz == classOf[String])
      classOf[java.lang.String]
    else if (clazz == classOf[Double])
      classOf[java.lang.Double]
    else if (clazz == classOf[Boolean])
      classOf[java.lang.Boolean]
    else if (clazz == classOf[Long])
      classOf[java.lang.Long]
    else if (clazz == classOf[Short])
      classOf[java.lang.Short]
    else if (clazz == classOf[Byte])
      classOf[java.lang.Byte]
    else
      clazz
  }
}
