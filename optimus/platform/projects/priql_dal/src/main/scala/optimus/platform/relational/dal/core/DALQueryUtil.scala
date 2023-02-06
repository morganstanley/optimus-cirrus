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
package optimus.platform.relational.dal.core

import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.time.LocalTime
import java.time.OffsetTime
import java.time.Period
import java.time.YearMonth
import java.time.ZoneId
import java.time.ZonedDateTime
import optimus.platform._
import optimus.platform.dal.QueryTemporalityFinder
import optimus.platform.BusinessEvent
import optimus.platform.TemporalContext
import optimus.platform.dsi.bitemporal.DSIQueryTemporality
import optimus.platform.dsi.bitemporal.Result
import optimus.platform.dsi.expressions.Binary
import optimus.platform.dsi.expressions.BinaryOperator.AndAlso
import optimus.platform.dsi.expressions.BinaryOperator.Equal
import optimus.platform.dsi.expressions.Constant
import optimus.platform.dsi.expressions.Expression
import optimus.platform.dsi.expressions.In
import optimus.platform.dsi.expressions.Property
import optimus.platform.dsi.expressions.TypeCode
import optimus.platform.pickling.ImmutableByteArray
import optimus.platform.relational.InvalidTemporalContextException
import optimus.platform.relational.RelationalUnsupportedException
import optimus.platform.storable.BusinessEventReference
import optimus.platform.storable.EntityReference
import optimus.platform.storable.PersistentEntity
import optimus.platform.storable.SerializedKey
import optimus.platform.storable.{Entity => OptimusEntity}
import optimus.platform.storable.VersionedReference
import optimus.platform.temporalSurface.operations.AndTemporalSurfaceQuery
import optimus.platform.temporalSurface.operations.OrTemporalSurfaceQuery
import optimus.platform.temporalSurface.operations.QueryByClass
import optimus.platform.temporalSurface.operations.QueryByNonUniqueKey
import optimus.platform.temporalSurface.operations.QueryByUniqueKey
import optimus.platform.temporalSurface.operations.TemporalSurfaceQuery

import scala.collection.Map

trait DALQueryUtil {
  val typeCode: PartialFunction[Any, TypeCode] = {
    case _: Int                                       => TypeCode.Int
    case _: String                                    => TypeCode.String
    case _: Double                                    => TypeCode.Double
    case _: Boolean                                   => TypeCode.Boolean
    case _: Char                                      => TypeCode.Char
    case _: Long                                      => TypeCode.Long
    case _: Short                                     => TypeCode.Short
    case _: Byte                                      => TypeCode.Byte
    case _: Seq[_]                                    => TypeCode.Sequence
    case _: Array[Byte]                               => TypeCode.Sequence
    case _: ImmutableByteArray                        => TypeCode.Sequence
    case _: ZonedDateTime                             => TypeCode.ZonedDateTime
    case _: LocalDate                                 => TypeCode.LocalDate
    case _: LocalTime                                 => TypeCode.LocalTime
    case _: OffsetTime                                => TypeCode.OffsetTime
    case _: ZoneId                                    => TypeCode.ZoneId
    case _: Period                                    => TypeCode.Period
    case _: Map[_, _]                                 => TypeCode.Map
    case _: EntityReference                           => TypeCode.Reference
    case _: BusinessEventReference                    => TypeCode.Reference
    case _: VersionedReference                        => TypeCode.VersionedReference
    case _: SerializedKey                             => TypeCode.SerializedKey
    case _: PersistentEntity                          => TypeCode.PersistentEntity
    case _: BusinessEvent                             => TypeCode.BusinessEvent
    case _: Result                                    => TypeCode.Result
    case _: BigDecimal                                => TypeCode.Decimal
    case _ @(_: Instant | _: Duration | _: YearMonth) => TypeCode.Sequence
    case None                                         => TypeCode.None
  }

  @async def dalTemporalContext(
      loadContext: TemporalContext,
      clazz: Class[_ <: OptimusEntity],
      expression: Option[Expression]): DSIQueryTemporality.At = {
    val tsQuery = toTemporalSurfaceQuery(clazz, expression)
    QueryTemporalityFinder.findQueryTemporality(loadContext, tsQuery)
  }

  def toTemporalSurfaceQuery(clazz: Class[_ <: OptimusEntity], expression: Option[Expression]): TemporalSurfaceQuery = {
    expression map { ex =>
      toTemporalSurfaceQuery(ex, clazz)
    } getOrElse {
      toTemporalSurfaceQuery(clazz)
    }
  }

  def toTemporalSurfaceQuery[T <: OptimusEntity](entity: Class[T]): TemporalSurfaceQuery = {
    QueryByClass(null, entity)
  }

  private def toTemporalSurfaceQuery[T <: OptimusEntity](
      sk: SerializedKey,
      realClass: Class[T]): TemporalSurfaceQuery = {
    if (sk.unique) new QueryByUniqueKey(null, sk, realClass, false)
    else new QueryByNonUniqueKey(null, sk, realClass, Set.empty, false)
  }

  private def toTemporalSurfaceQuery[T <: OptimusEntity](
      where: Expression,
      realClass: Class[T]): TemporalSurfaceQuery = {
    where match {
      case Binary(AndAlso, l, r) =>
        AndTemporalSurfaceQuery(toTemporalSurfaceQuery[T](l, realClass), toTemporalSurfaceQuery[T](r, realClass))
      case Binary(Equal, _: Property, Constant(sk: SerializedKey, _)) =>
        toTemporalSurfaceQuery(sk, realClass)
      case In(_: Property, Right(v)) =>
        v.map { case Constant(sk: SerializedKey, _) => toTemporalSurfaceQuery(sk, realClass) }
          .reduce((x, y) => OrTemporalSurfaceQuery(x, y))
      case x =>
        throw new RelationalUnsupportedException(s"Unsupported filter condition: $x")
    }
  }

  class ConstFormatter(loadCtx: TemporalContext) extends DALExpressionVisitor {
    override protected def visitRichConstant(c: RichConstant): Expression = {
      c.index map { idx =>
        Constant(idx.makeKey(checkLoadContext(c.value, loadCtx)).toSerializedKey, TypeCode.SerializedKey)
      } getOrElse {
        Constant(c.value, typeCode(c.value))
      }
    }
  }

  object ConstFormatter {
    def format(ex: Expression, loadCtx: TemporalContext): Expression = {
      new ConstFormatter(loadCtx).visit(ex)
    }
  }

  def checkLoadContext(value: Any, loadCtx: TemporalContext, allowHeapEntity: Boolean = true): Any = {
    value match {
      case e: OptimusEntity if !e.dal$isTemporary =>
        if (loadCtx != null && e.dal$temporalContext != loadCtx)
          throw new InvalidTemporalContextException(
            s""" Temporal context of entity (${e.dal$temporalContext})
               | is not equal to the current load context ($loadCtx).
               | Entity: $e""".stripMargin.replaceAll("\n", "")
          )
      case _: OptimusEntity if !allowHeapEntity =>
        // we have to support heap entity for historical reason, e.g. @stored @entity case object New
        throw new RelationalUnsupportedException("Cannot query heap entity on server side.")
      case _ =>
    }
    value
  }
}
