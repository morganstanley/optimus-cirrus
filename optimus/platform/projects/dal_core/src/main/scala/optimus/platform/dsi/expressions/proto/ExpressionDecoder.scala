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
package optimus.platform.dsi.expressions.proto

import java.time.Instant

import optimus.platform.TimeInterval
import optimus.platform.ValidTimeInterval
import optimus.platform.dsi.bitemporal.DSIQueryTemporality
import optimus.platform.dsi.bitemporal.proto.InstantSerializer
import optimus.platform.dsi.bitemporal.proto.ProtoPickleSerializer
import optimus.platform.dsi.bitemporal.proto.SerializedKeySerializer
import optimus.platform.dsi.expressions._
import optimus.platform.dsi.expressions.proto.Expressions._
import optimus.platform.dsi.bitemporal.proto.Dsi.{FieldProto, SerializedKeyProto}

import scala.jdk.CollectionConverters._

/**
 * decode GPB ExpressionProto to Expression
 */
trait ExpressionDecoder {
  def decode(proto: ExpressionProto): Expression = ??? /* {
    import ExpressionProto.Type
    proto.getType match {
      case Type.Entity            => decode(proto.getEntity)
      case Type.Event             => decode(proto.getEvent)
      case Type.Property          => decode(proto.getProperty)
      case Type.Constant          => decode(proto.getConstant)
      case Type.Binary            => decode(proto.getBinary)
      case Type.In                => decode(proto.getIn)
      case Type.Select            => decode(proto.getSelect)
      case Type.Join              => decode(proto.getJoin)
      case Type.Member            => decode(proto.getMember)
      case Type.Function          => decode(proto.getFunction)
      case Type.Unary             => decode(proto.getUnary)
      case Type.Condition         => decode(proto.getCondition)
      case Type.Aggregate         => decode(proto.getAggregate)
      case Type.Scalar            => decode(proto.getScalar)
      case Type.Exists            => decode(proto.getExists)
      case Type.Linkage           => decode(proto.getLinkage)
      case Type.Embeddable        => decode(proto.getEmbeddable)
      case Type.EntityBitempSpace => decode(proto.getEntityBitempSpace)
    }
  } */

  protected def decode(proto: EntityBitempSpaceProto.Kind): UpdateKind = ??? /* {
    import EntityBitempSpaceProto.Kind
    proto match {
      case Kind.Delta => DeltaUpdate
      case Kind.All   => AllRects
    }
  } */

  protected def decode(proto: EntityBitempSpaceProto): EntityBitemporalSpace = ??? /* {
    EntityBitemporalSpace(
      proto.getName,
      decode(proto.getWhen).asInstanceOf[DSIQueryTemporality.BitempRange],
      decode(proto.getKind),
      proto.getSuperClassesList.asScala,
      decode(proto.getId)
    )
  } */

  protected def decode(proto: EntityProto): Entity = ??? /* {
    Entity(proto.getName, decode(proto.getWhen), proto.getSuperClassesList.asScala, decode(proto.getId))
  } */

  protected def decode(proto: EventProto): Event = ??? /* {
    Event(proto.getName, decode(proto.getWhen), decode(proto.getId))
  } */

  protected def decode(proto: LinkageProto): Linkage = ??? /* {
    Linkage(proto.getName, decode(proto.getWhen), decode(proto.getId))
  } */

  protected def decode(proto: EmbeddableProto): Embeddable = ??? /* {
    Embeddable(proto.getEntity, proto.getProperty, proto.getTag, decode(proto.getId))
  } */

  protected def decode(proto: PropertyProto): Property = ??? /* {
    Property(propType = decode(proto.getPropType), names = proto.getNamesList.asScala, owner = decode(proto.getOwner))
  } */

  protected def decode(proto: ConstantProto): Constant = ??? /* {
    val typeCode = decode(proto.getTypeCode)
    val value = typeCode match {
      case TypeCode.SerializedKey => SerializedKeySerializer.deserialize(SerializedKeyProto.parseFrom(proto.getValue))
      case _                      => ProtoPickleSerializer.protoToProperties(FieldProto.parseFrom(proto.getValue))
    }
    Constant(value, decode(proto.getTypeCode))
  } */

  protected def decode(proto: BinaryProto): Binary = ??? /* {
    Binary(
      op = decode(proto.getOperator),
      left = decode(proto.getLeft),
      right = decode(proto.getRight)
    )
  } */

  protected def decode(proto: InProto): In = ??? /* {
    val e = decode(proto.getExpression)
    if (proto.hasSelect)
      In(e, decode(proto.getSelect))
    else {
      val v: List[Expression] = proto.getValuesList.asScala.iterator.map(decode(_)).toList
      In(e, v)
    }
  } */

  protected def decode(proto: SelectProto): Select = ??? /* {
    Select(
      from = deserializeQuerySource(proto.getFrom),
      properties = proto.getPropertiesList.asScala.iterator.map(decode(_)).toList,
      where = if (proto.hasWhere) Some(decode(proto.getWhere)) else None,
      sortBy = proto.getSortByList.asScala.iterator.map(decode(_)).toList,
      groupBy = proto.getGroupByList.asScala.iterator.map(decode(_)).toList,
      take = if (proto.hasTake) Some(decode(proto.getTake)) else None,
      skip = if (proto.hasSkip) Some(decode(proto.getSkip)) else None,
      isDistinct = proto.getDistinct,
      reverse = proto.getReverse,
      id = decode(proto.getId)
    )
  } */

  private def deserializeQuerySource(e: ExpressionProto): QuerySource = {
    decode(e).asInstanceOf[QuerySource]
  }

  protected def decode(proto: JoinProto): Join = ??? /* {
    Join(
      joinType = decode(proto.getJoinType),
      left = deserializeQuerySource(proto.getLeft),
      right = deserializeQuerySource(proto.getRight),
      on = if (proto.hasOn) Some(decode(proto.getOn)) else None
    )
  } */

  protected def decode(proto: MemberProto): Member = ??? /* {
    Member(proto.getName, decode(proto.getOwner))
  } */

  protected def decode(proto: FunctionProto): Function = ??? /* {
    Function(
      method = proto.getMethod,
      arguments = proto.getArgumentsList.asScala.iterator.map(decode(_)).toList
    )
  } */

  protected def decode(proto: UnaryProto): Unary = ??? /* {
    Unary(
      op = decode(proto.getOperator),
      e = decode(proto.getExpression)
    )
  } */

  protected def decode(proto: ConditionProto): Condition = ??? /* {
    Condition(
      check = decode(proto.getCheck),
      ifTrue = decode(proto.getIfTrue),
      ifFalse = decode(proto.getIfFalse)
    )
  } */

  protected def decode(proto: AggregateProto): Aggregate = ??? /* {
    Aggregate(
      aggregateType = decode(proto.getAggregateType),
      arguments = proto.getArgumentsList.asScala.iterator.map(decode(_)).toList,
      proto.getDistinct
    )
  } */

  protected def decode(proto: ScalarProto): Scalar = ??? /* {
    Scalar(decode(proto.getQuery))
  } */

  protected def decode(proto: ExistsProto): Exists = ??? /* {
    Exists(decode(proto.getQuery))
  } */

  protected def decode(typeCode: TypeCodeProto): TypeCode = ??? /* {
    typeCode match {
      case TypeCodeProto.Int                => TypeCode.Int
      case TypeCodeProto.String             => TypeCode.String
      case TypeCodeProto.Double             => TypeCode.Double
      case TypeCodeProto.Boolean            => TypeCode.Boolean
      case TypeCodeProto.Char               => TypeCode.Char
      case TypeCodeProto.Long               => TypeCode.Long
      case TypeCodeProto.Short              => TypeCode.Short
      case TypeCodeProto.Byte               => TypeCode.Byte
      case TypeCodeProto.Blob               => TypeCode.Blob
      case TypeCodeProto.ZonedDateTime      => TypeCode.ZonedDateTime
      case TypeCodeProto.LocalDate          => TypeCode.LocalDate
      case TypeCodeProto.LocalTime          => TypeCode.LocalTime
      case TypeCodeProto.OffsetTime         => TypeCode.OffsetTime
      case TypeCodeProto.ZoneId             => TypeCode.ZoneId
      case TypeCodeProto.Period             => TypeCode.Period
      case TypeCodeProto.Sequence           => TypeCode.Sequence
      case TypeCodeProto.Tuple2             => TypeCode.Tuple2
      case TypeCodeProto.Map                => TypeCode.Map
      case TypeCodeProto.Reference          => TypeCode.Reference
      case TypeCodeProto.None               => TypeCode.None
      case TypeCodeProto.SerializedKey      => TypeCode.SerializedKey
      case TypeCodeProto.PersistentEntity   => TypeCode.PersistentEntity
      case TypeCodeProto.BusinessEvent      => TypeCode.BusinessEvent
      case TypeCodeProto.Result             => TypeCode.Result
      case TypeCodeProto.Decimal            => TypeCode.Decimal
      case TypeCodeProto.VersionedReference => TypeCode.VersionedReference
      case TypeCodeProto.Rectangle          => TypeCode.Rectangle
    }
  } */

  /* protected def decode(op: BinaryProto.BinaryOperator): BinaryOperator = {
    op match {
      case BinaryProto.BinaryOperator.AndAlso            => BinaryOperator.AndAlso
      case BinaryProto.BinaryOperator.OrElse             => BinaryOperator.OrElse
      case BinaryProto.BinaryOperator.Equal              => BinaryOperator.Equal
      case BinaryProto.BinaryOperator.NotEqual           => BinaryOperator.NotEqual
      case BinaryProto.BinaryOperator.GreaterThan        => BinaryOperator.GreaterThan
      case BinaryProto.BinaryOperator.GreaterThanOrEqual => BinaryOperator.GreaterThanOrEqual
      case BinaryProto.BinaryOperator.LessThan           => BinaryOperator.LessThan
      case BinaryProto.BinaryOperator.LessThanOrEqual    => BinaryOperator.LessThanOrEqual
      case BinaryProto.BinaryOperator.Add                => BinaryOperator.Add
      case BinaryProto.BinaryOperator.Subtract           => BinaryOperator.Subtract
      case BinaryProto.BinaryOperator.Multiply           => BinaryOperator.Multiply
      case BinaryProto.BinaryOperator.Divide             => BinaryOperator.Divide
      case BinaryProto.BinaryOperator.Modulo             => BinaryOperator.Modulo
    }
  }

  protected def decode(op: UnaryProto.UnaryOperator): UnaryOperator = {
    op match {
      case UnaryProto.UnaryOperator.Negate  => UnaryOperator.Negate
      case UnaryProto.UnaryOperator.IsEmpty => UnaryOperator.IsEmpty
      case UnaryProto.UnaryOperator.Not     => UnaryOperator.Not
    }
  }

  protected def decode(aggregateType: AggregateProto.AggregateType): AggregateType = {
    aggregateType match {
      case AggregateProto.AggregateType.Sum         => AggregateType.Sum
      case AggregateProto.AggregateType.Count       => AggregateType.Count
      case AggregateProto.AggregateType.Average     => AggregateType.Average
      case AggregateProto.AggregateType.Min         => AggregateType.Min
      case AggregateProto.AggregateType.Max         => AggregateType.Max
      case AggregateProto.AggregateType.Distinct    => AggregateType.Distinct
      case AggregateProto.AggregateType.ArrayAgg    => AggregateType.ArrayAgg
      case AggregateProto.AggregateType.Var_samp    => AggregateType.Var_samp
      case AggregateProto.AggregateType.Var_pop     => AggregateType.Var_pop
      case AggregateProto.AggregateType.Stddev_samp => AggregateType.Stddev_samp
      case AggregateProto.AggregateType.Stddev_pop  => AggregateType.Stddev_pop
      case AggregateProto.AggregateType.Corr        => AggregateType.Corr
    }
  }

  protected def decode(joinType: JoinProto.JoinType): JoinType = {
    joinType match {
      case JoinProto.JoinType.InnerJoin  => JoinType.InnerJoin
      case JoinProto.JoinType.LeftOuter  => JoinType.LeftOuter
      case JoinProto.JoinType.RightOuter => JoinType.RightOuter
      case JoinProto.JoinType.FullOuter  => JoinType.FullOuter
      case JoinProto.JoinType.CrossApply => JoinType.CrossApply
      case JoinProto.JoinType.CrossJoin  => JoinType.CrossJoin
      case JoinProto.JoinType.OuterApply => JoinType.OuterApply
    }
  }

  protected def decode(propType: PropertyProto.PropertyType): PropertyType = {
    propType match {
      case PropertyProto.PropertyType.Default     => PropertyType.Default
      case PropertyProto.PropertyType.Key         => PropertyType.Key
      case PropertyProto.PropertyType.UniqueIndex => PropertyType.UniqueIndex
      case PropertyProto.PropertyType.Index       => PropertyType.Index
      case PropertyProto.PropertyType.Special     => PropertyType.Special
    }
  } */

  protected def decode(proto: TemporalityProto): DSIQueryTemporality = ??? /* 
    import TemporalityProto.Type._
    proto.getType match {
      case At        => DSIQueryTemporality.At(decode(proto.getValidTime), decode(proto.getTxTime))
      case ValidTime => DSIQueryTemporality.ValidTime(decode(proto.getValidTime), readTxTime = decode(proto.getTxTime))
      case TxTime    => DSIQueryTemporality.TxTime(decode(proto.getTxTime))
      case All       => DSIQueryTemporality.All(readTxTime = decode(proto.getTxTime))
      case TxRange   => DSIQueryTemporality.TxRange(decode(proto.getTxRange))
      case BitempRange => {
        val inRange = proto.hasInRange() match {
          case true => proto.getInRange
          case _    => false
        }
        DSIQueryTemporality.BitempRange(
          toTimeInterval(decode(proto.getValidTimeRange)),
          decode(proto.getTxRange),
          inRange)
      }
      case OpenVtTxRange => DSIQueryTemporality.OpenVtTxRange(decode(proto.getOpenVtTxRange))
    }
  } */

  private def toTimeInterval(vt: ValidTimeInterval): TimeInterval = {
    // DSIQueryTemporality.BitempRange requires TimeInterval
    TimeInterval(vt.from, vt.to)
  }

  protected def decode(proto: ValidTimeIntervalProto): ValidTimeInterval = ??? /* {
    ValidTimeInterval(decode(proto.getFrom), decode(proto.getTo))
  } */

  protected def decode(proto: TimeIntervalProto): TimeInterval = ??? /* {
    TimeInterval(decode(proto.getFrom), decode(proto.getTo))
  } */

  protected def decode(proto: InstantProto): Instant = ??? /* {
    InstantSerializer.ofEpochSecond(proto.getSeconds, proto.getNano)
  } */

  protected def decode(proto: PropertyDefProto): PropertyDef = ??? /* {
    PropertyDef(proto.getName, decode(proto.getExpression))
  } */

  protected def decode(proto: SortByDefProto): SortByDef = ??? /* {
    val sortType = proto.getSortType match {
      case SortByDefProto.SortType.Asc  => SortType.Asc
      case SortByDefProto.SortType.Desc => SortType.Desc
    }
    SortByDef(sortType, decode(proto.getExpression))
  } */

  protected def decode(id: Int): Id
}
