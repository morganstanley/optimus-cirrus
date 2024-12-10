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
import optimus.platform.dsi.bitemporal.proto.ProtoPickleSerializer
import optimus.platform.dsi.bitemporal.proto.SerializedKeySerializer
import optimus.platform.dsi.expressions._
import optimus.platform.dsi.expressions.proto.Expressions._
import optimus.platform.storable.SerializedKey

import scala.jdk.CollectionConverters._

/**
 * encode Expression to GPB ExpressionProto
 */
trait ExpressionEncoder {
  def encode(obj: Expression): ExpressionProto = ??? /* {
    val b = ExpressionProto.newBuilder
    obj match {
      case e: Entity =>
        b.setType(ExpressionProto.Type.Entity).setEntity(encode(e))
      case c: EntityBitemporalSpace =>
        b.setType(ExpressionProto.Type.EntityBitempSpace).setEntityBitempSpace(encode(c))
      case e: Event =>
        b.setType(ExpressionProto.Type.Event).setEvent(encode(e))
      case e: Property =>
        b.setType(ExpressionProto.Type.Property).setProperty(encode(e))
      case e: Constant =>
        b.setType(ExpressionProto.Type.Constant).setConstant(encode(e))
      case e: Binary =>
        b.setType(ExpressionProto.Type.Binary).setBinary(encode(e))
      case e: In =>
        b.setType(ExpressionProto.Type.In).setIn(encode(e))
      case e: Select =>
        b.setType(ExpressionProto.Type.Select).setSelect(encode(e))
      case e: Join =>
        b.setType(ExpressionProto.Type.Join).setJoin(encode(e))
      case e: Member =>
        b.setType(ExpressionProto.Type.Member).setMember(encode(e))
      case e: Function =>
        b.setType(ExpressionProto.Type.Function).setFunction(encode(e))
      case e: Unary =>
        b.setType(ExpressionProto.Type.Unary).setUnary(encode(e))
      case e: Condition =>
        b.setType(ExpressionProto.Type.Condition).setCondition(encode(e))
      case e: Aggregate =>
        b.setType(ExpressionProto.Type.Aggregate).setAggregate(encode(e))
      case e: Scalar =>
        b.setType(ExpressionProto.Type.Scalar).setScalar(encode(e))
      case e: Exists =>
        b.setType(ExpressionProto.Type.Exists).setExists(encode(e))
      case e: Linkage =>
        b.setType(ExpressionProto.Type.Linkage).setLinkage(encode(e))
      case e: Embeddable =>
        b.setType(ExpressionProto.Type.Embeddable).setEmbeddable(encode(e))
    }
    b.build()
  } */

  protected def encode(obj: EntityBitemporalSpace): EntityBitempSpaceProto = ??? /* {
    import EntityBitempSpaceProto.Kind
    val kind = obj.kind match {
      case AllRects    => Kind.All
      case DeltaUpdate => Kind.Delta
    }
    EntityBitempSpaceProto
      .newBuilder()
      .setName(obj.name)
      .setId(encode(obj.id))
      .setWhen(encode(obj.when))
      .setKind(kind)
      .addAllSuperClasses(obj.superClasses.asJava)
      .build()
  } */

  protected def encode(obj: Entity): EntityProto = ??? /* {
    import EntityBitempSpaceProto.Kind
    val kind = obj.kind match {
      case AllRects    => Kind.All
      case DeltaUpdate => Kind.Delta
    }
    EntityBitempSpaceProto
      .newBuilder()
      .setName(obj.name)
      .setId(encode(obj.id))
      .setWhen(encode(obj.when))
      .setKind(kind)
      .addAllSuperClasses(obj.superClasses.asJava)
      .build()
  } */

  protected def encode(obj: Event): EventProto = ??? /* {
    EventProto.newBuilder
      .setName(obj.name)
      .setId(encode(obj.id))
      .setWhen(encode(obj.when))
      .build()
  } */

  protected def encode(obj: Linkage): LinkageProto = ??? /* {
    LinkageProto.newBuilder
      .setName(obj.name)
      .setId(encode(obj.id))
      .setWhen(encode(obj.when))
      .build()
  } */

  protected def encode(obj: Embeddable): EmbeddableProto = ??? /* {
    EmbeddableProto.newBuilder
      .setId(encode(obj.id))
      .setEntity(obj.entity)
      .setProperty(obj.property)
      .setTag(obj.tag)
      .build()
  } */

  protected def encode(obj: Property): PropertyProto = ??? /* {
    PropertyProto.newBuilder
      .setOwner(encode(obj.owner))
      .setPropType(encode(obj.propType))
      .addAllNames(obj.names.asJava)
      .build()
  } */

  protected def encode(obj: Constant): ConstantProto = ??? /* {
    val value = obj.value match {
      case s: SerializedKey => SerializedKeySerializer.serialize(s)
      case _                => ProtoPickleSerializer.propertiesToProto(obj.value)
    }
    ConstantProto.newBuilder
      .setTypeCode(encode(obj.typeCode))
      .setValue(value.toByteString)
      .build()
  } */

  protected def encode(obj: Binary): BinaryProto = ??? /*{
    BinaryProto.newBuilder
      .setOperator(encode(obj.op))
      .setLeft(encode(obj.left))
      .setRight(encode(obj.right))
      .build()
  } */

  protected def encode(obj: In): InProto = ??? /* {
    val b = InProto.newBuilder
    obj.values match {
      case Left(select) => b.setSelect(encode(select))
      case Right(list)  => b.addAllValues(list.map(encode(_)).asJava)
    }
    b.setExpression(encode(obj.e)).build()
  } */

  protected def encode(obj: Select): SelectProto = ??? /* {
    val b = SelectProto.newBuilder
      .setFrom(encode(obj.from))
      .setId(encode(obj.id))
      .setDistinct(obj.isDistinct)
      .setReverse(obj.reverse)
      .addAllProperties(obj.properties.map(encode(_)).asJava)
      .addAllGroupBy(obj.groupBy.map(encode(_)).asJava)
      .addAllSortBy(obj.sortBy.map(encode(_)).asJava)
    obj.where.foreach(t => b.setWhere(encode(t)))
    obj.take.foreach(t => b.setTake(encode(t)))
    obj.skip.foreach(t => b.setSkip(encode(t)))
    b.build()
  } */

  protected def encode(obj: Join): JoinProto = ??? /* {
    val b = JoinProto.newBuilder
      .setJoinType(encode(obj.joinType))
      .setLeft(encode(obj.left))
      .setRight(encode(obj.right))
    obj.on.foreach(on => b.setOn(encode(on)))
    b.build()
  } */

  protected def encode(obj: Member): MemberProto = ??? /* {
    MemberProto.newBuilder
      .setName(obj.name)
      .setOwner(encode(obj.owner))
      .build()
  } */

  protected def encode(obj: Function): FunctionProto = ??? /* {
    FunctionProto.newBuilder
      .setMethod(obj.method)
      .addAllArguments(obj.arguments.map(encode(_)).asJava)
      .build()
  } */

  protected def encode(obj: Unary): UnaryProto = ??? /* {
    UnaryProto.newBuilder
      .setOperator(encode(obj.op))
      .setExpression(encode(obj.e))
      .build()
  } */

  protected def encode(obj: Condition): ConditionProto = ??? /* {
    ConditionProto.newBuilder
      .setCheck(encode(obj.check))
      .setIfTrue(encode(obj.ifTrue))
      .setIfFalse(encode(obj.ifFalse))
      .build()
  } */

  protected def encode(obj: Aggregate): AggregateProto = ??? /* {
    AggregateProto.newBuilder
      .setAggregateType(encode(obj.aggregateType))
      .addAllArguments(obj.arguments.map(encode(_)).asJava)
      .setDistinct(obj.isDistinct)
      .build()
  } */

  protected def encode(obj: Scalar): ScalarProto = ??? /* {
    ScalarProto.newBuilder
      .setQuery(encode(obj.query))
      .build()
  } */

  protected def encode(obj: Exists): ExistsProto = ??? /* {
    ExistsProto.newBuilder
      .setQuery(encode(obj.query))
      .build()
  } */

  protected def encode(typeCode: TypeCode): TypeCodeProto = ??? /* {
    typeCode match {
      case TypeCode.Int                => TypeCodeProto.Int
      case TypeCode.String             => TypeCodeProto.String
      case TypeCode.Double             => TypeCodeProto.Double
      case TypeCode.Boolean            => TypeCodeProto.Boolean
      case TypeCode.Char               => TypeCodeProto.Char
      case TypeCode.Long               => TypeCodeProto.Long
      case TypeCode.Short              => TypeCodeProto.Short
      case TypeCode.Byte               => TypeCodeProto.Byte
      case TypeCode.Blob               => TypeCodeProto.Blob
      case TypeCode.ZonedDateTime      => TypeCodeProto.ZonedDateTime
      case TypeCode.LocalDate          => TypeCodeProto.LocalDate
      case TypeCode.LocalTime          => TypeCodeProto.LocalTime
      case TypeCode.OffsetTime         => TypeCodeProto.OffsetTime
      case TypeCode.ZoneId             => TypeCodeProto.ZoneId
      case TypeCode.Period             => TypeCodeProto.Period
      case TypeCode.Sequence           => TypeCodeProto.Sequence
      case TypeCode.Tuple2             => TypeCodeProto.Tuple2
      case TypeCode.Map                => TypeCodeProto.Map
      case TypeCode.Reference          => TypeCodeProto.Reference
      case TypeCode.None               => TypeCodeProto.None
      case TypeCode.SerializedKey      => TypeCodeProto.SerializedKey
      case TypeCode.PersistentEntity   => TypeCodeProto.PersistentEntity
      case TypeCode.BusinessEvent      => TypeCodeProto.BusinessEvent
      case TypeCode.Result             => TypeCodeProto.Result
      case TypeCode.Decimal            => TypeCodeProto.Decimal
      case TypeCode.VersionedReference => TypeCodeProto.VersionedReference
      case TypeCode.Rectangle          => TypeCodeProto.Rectangle
    }
  } */

  /* protected def encode(op: BinaryOperator): BinaryProto.BinaryOperator = {
    op match {
      case BinaryOperator.AndAlso            => BinaryProto.BinaryOperator.AndAlso
      case BinaryOperator.OrElse             => BinaryProto.BinaryOperator.OrElse
      case BinaryOperator.Equal              => BinaryProto.BinaryOperator.Equal
      case BinaryOperator.NotEqual           => BinaryProto.BinaryOperator.NotEqual
      case BinaryOperator.GreaterThan        => BinaryProto.BinaryOperator.GreaterThan
      case BinaryOperator.GreaterThanOrEqual => BinaryProto.BinaryOperator.GreaterThanOrEqual
      case BinaryOperator.LessThan           => BinaryProto.BinaryOperator.LessThan
      case BinaryOperator.LessThanOrEqual    => BinaryProto.BinaryOperator.LessThanOrEqual
      case BinaryOperator.Add                => BinaryProto.BinaryOperator.Add
      case BinaryOperator.Subtract           => BinaryProto.BinaryOperator.Subtract
      case BinaryOperator.Multiply           => BinaryProto.BinaryOperator.Multiply
      case BinaryOperator.Divide             => BinaryProto.BinaryOperator.Divide
      case BinaryOperator.Modulo             => BinaryProto.BinaryOperator.Modulo
    }
  }

  protected def encode(op: UnaryOperator): UnaryProto.UnaryOperator = {
    op match {
      case UnaryOperator.Negate  => UnaryProto.UnaryOperator.Negate
      case UnaryOperator.IsEmpty => UnaryProto.UnaryOperator.IsEmpty
      case UnaryOperator.Not     => UnaryProto.UnaryOperator.Not
    }
  }

  protected def encode(aggregateType: AggregateType): AggregateProto.AggregateType = {
    aggregateType match {
      case AggregateType.Sum         => AggregateProto.AggregateType.Sum
      case AggregateType.Count       => AggregateProto.AggregateType.Count
      case AggregateType.Average     => AggregateProto.AggregateType.Average
      case AggregateType.Min         => AggregateProto.AggregateType.Min
      case AggregateType.Max         => AggregateProto.AggregateType.Max
      case AggregateType.Distinct    => AggregateProto.AggregateType.Distinct
      case AggregateType.ArrayAgg    => AggregateProto.AggregateType.ArrayAgg
      case AggregateType.Var_samp    => AggregateProto.AggregateType.Var_samp
      case AggregateType.Var_pop     => AggregateProto.AggregateType.Var_pop
      case AggregateType.Stddev_samp => AggregateProto.AggregateType.Stddev_samp
      case AggregateType.Stddev_pop  => AggregateProto.AggregateType.Stddev_pop
      case AggregateType.Corr        => AggregateProto.AggregateType.Corr
    }
  }

  protected def encode(joinType: JoinType): JoinProto.JoinType = {
    joinType match {
      case JoinType.InnerJoin  => JoinProto.JoinType.InnerJoin
      case JoinType.LeftOuter  => JoinProto.JoinType.LeftOuter
      case JoinType.RightOuter => JoinProto.JoinType.RightOuter
      case JoinType.FullOuter  => JoinProto.JoinType.FullOuter
      case JoinType.CrossApply => JoinProto.JoinType.CrossApply
      case JoinType.CrossJoin  => JoinProto.JoinType.CrossJoin
      case JoinType.OuterApply => JoinProto.JoinType.OuterApply
    }
  }

  protected def encode(propType: PropertyType): PropertyProto.PropertyType = {
    propType match {
      case PropertyType.Default     => PropertyProto.PropertyType.Default
      case PropertyType.Key         => PropertyProto.PropertyType.Key
      case PropertyType.UniqueIndex => PropertyProto.PropertyType.UniqueIndex
      case PropertyType.Index       => PropertyProto.PropertyType.Index
      case PropertyType.Special     => PropertyProto.PropertyType.Special
    }
  } */

  protected def encode(obj: DSIQueryTemporality): TemporalityProto = ??? /* {
    val b = TemporalityProto.newBuilder
    obj match {
      case DSIQueryTemporality.At(validTime, txTime) =>
        b.setType(TemporalityProto.Type.At).setValidTime(encode(validTime)).setTxTime(encode(txTime))
      case DSIQueryTemporality.ValidTime(validTime, readTxTime) =>
        b.setType(TemporalityProto.Type.ValidTime).setValidTime(encode(validTime)).setTxTime(encode(readTxTime))
      case DSIQueryTemporality.TxTime(txTime) =>
        b.setType(TemporalityProto.Type.TxTime).setTxTime(encode(txTime))
      case DSIQueryTemporality.All(readTxTime) =>
        b.setType(TemporalityProto.Type.All).setTxTime(encode(readTxTime))
      case DSIQueryTemporality.TxRange(range) =>
        b.setType(TemporalityProto.Type.TxRange).setTxRange(encode(range))
      case DSIQueryTemporality.BitempRange(vtRange, ttRange, inRange) =>
        b.setType(TemporalityProto.Type.BitempRange)
          .setValidTimeRange(encode(toValidTimeInterval(vtRange)))
          .setTxRange(encode(ttRange))
          .setInRange(inRange)
      case DSIQueryTemporality.OpenVtTxRange(range) =>
        b.setType(TemporalityProto.Type.OpenVtTxRange).setOpenVtTxRange(encode(range))

    }
    b.build()
  } */

  private def toValidTimeInterval(i: TimeInterval): ValidTimeInterval = {
    ValidTimeInterval(i.from, i.to)
  }

  protected def encode(obj: ValidTimeInterval): ValidTimeIntervalProto = ??? /* {
    ValidTimeIntervalProto.newBuilder
      .setFrom(encode(obj.from))
      .setTo(encode(obj.to))
      .build()
  } */

  protected def encode(obj: TimeInterval): TimeIntervalProto = ??? /* {
    TimeIntervalProto.newBuilder
      .setFrom(encode(obj.from))
      .setTo(encode(obj.to))
      .build()
  } */

  protected def encode(obj: Instant): InstantProto = ??? /* {
    InstantProto.newBuilder
      .setNano(obj.getNano)
      .setSeconds(obj.getEpochSecond)
      .build()
  } */

  protected def encode(obj: PropertyDef): PropertyDefProto = ??? /* {
    PropertyDefProto.newBuilder
      .setName(obj.name)
      .setExpression(encode(obj.e))
      .build()
  } */

  protected def encode(obj: SortByDef): SortByDefProto = ??? /* {
    val sortType = obj.sortType match {
      case SortType.Asc  => SortByDefProto.SortType.Asc
      case SortType.Desc => SortByDefProto.SortType.Desc
    }
    SortByDefProto.newBuilder
      .setSortType(sortType)
      .setExpression(encode(obj.e))
      .build()
  } */

  protected def encode(id: Id): Int
}
