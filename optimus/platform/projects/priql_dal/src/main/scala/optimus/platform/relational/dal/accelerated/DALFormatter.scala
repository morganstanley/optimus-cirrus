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
package optimus.platform.relational.dal.accelerated

import java.time.Instant
import java.time.LocalDate
import java.time.LocalTime
import java.time.ZonedDateTime
import optimus.platform.BusinessEvent
import optimus.platform.cm.Knowable
import optimus.platform.dsi.bitemporal.QueryPlan
import optimus.platform.dsi.expressions.{Entity => _, _}
import optimus.platform.relational.RelationalUnsupportedException
import optimus.platform.relational.dal.DALProvider
import optimus.platform.relational.dal.core.DALFormatterBase
import optimus.platform.relational.dal.core.DALFormatterHelper
import optimus.platform.relational.dal.core.ExpressionQuery
import optimus.platform.relational.dal.core.RichConstant
import optimus.platform.relational.data.tree._
import optimus.platform.relational.tree._
import optimus.platform.storable.Entity
import optimus.platform.storable.SerializedKey
import optimus.platform.util.ZonedDateTimeOps

class DALFormatter extends DALFormatterBase {
  import DALFormatterHelper._

  override def visitElement(e: RelationElement): RelationElement = {
    if (e eq null) null
    else {
      e.elementType match {
        case DbElementType.Table | DbElementType.Column | DbElementType.Select | DbElementType.NamedValue |
            DbElementType.Join | DbElementType.Projection | DbElementType.Contains | DbElementType.Aggregate |
            DbElementType.AggregateSubquery | DbElementType.Scalar | ElementType.ConstValue | DbElementType.Exists |
            ElementType.MemberRef | ElementType.TernaryExpression | ElementType.New | ElementType.BinaryExpression |
            ElementType.ForteFuncCall | ElementType.UnaryExpression | ElementType.TypeIs =>
          super.visitElement(e)
        case _ =>
          throw new RelationalUnsupportedException(s"The relation element of type ${e.elementType} is not supported")
      }
    }
  }

  override def handleUnaryExpression(unary: UnaryExpressionElement): RelationElement = {
    if (unary.op == UnaryExpressionType.CONVERT) visitElement(unary.element)
    else super.handleUnaryExpression(unary)
  }

  protected override def handleBinaryExpression(binary: BinaryExpressionElement): RelationElement = {
    val l = getExpression(visitElement(binary.left))
    val r = getExpression(visitElement(binary.right))
    val op = binaryOperator(binary.op)
    val lt = TypeInfo.underlying(binary.left.rowTypeInfo)
    val rt = TypeInfo.underlying(binary.right.rowTypeInfo)

    val e = if (op == BinaryOperator.Equal || op == BinaryOperator.NotEqual) {
      val verb = if (op == BinaryOperator.Equal) "equals" else "notEquals"
      if (lt <:< classOf[LocalDate] || rt <:< classOf[LocalDate]) {
        Function(s"date.$verb", List(l, r))
      } else if (lt <:< classOf[LocalTime] || rt <:< classOf[LocalTime]) {
        Function(s"time.$verb", List(l, r))
      } else if (lt <:< classOf[ZonedDateTime] || rt <:< classOf[ZonedDateTime]) {
        Function(s"datetimez.$verb", List(l, r))
      } else if (
        (lt eq DALProvider.EntityRefType) || (rt eq DALProvider.EntityRefType)
        || lt <:< classOf[Entity] || rt <:< classOf[Entity]
      ) {
        Function(s"entity.$verb", List(l, r))
      } else if (lt <:< classOf[BusinessEvent] || rt <:< classOf[BusinessEvent]) {
        Function(s"event.$verb", List(l, r))
      } else {
        Binary(op, l, r)
      }
    } else if (op == BinaryOperator.Add && (lt <:< classOf[String] || rt <:< classOf[String])) {
      Function("text.concat", List(l, r))
    } else {
      Binary(op, l, r)
    }
    ExpressionElement(e)
  }

  protected override def handleContains(contains: ContainsElement): RelationElement = {
    val expr = getExpression(visitElement(contains.element))
    val typeInfo = contains.element.rowTypeInfo
    val c = contains.values match {
      case Left(v) =>
        val values = getExpression(visitElement(v)).asInstanceOf[Scalar]
        if (typeInfo <:< classOf[LocalDate]) {
          Function("date.in", expr :: values :: Nil)
        } else if (typeInfo <:< classOf[LocalTime]) {
          Function("time.in", expr :: values :: Nil)
        } else if ((typeInfo eq DALProvider.EntityRefType) || typeInfo <:< classOf[Entity]) {
          Function(s"entity.in", expr :: values :: Nil)
        } else if (typeInfo <:< classOf[BusinessEvent]) {
          Function(s"event.in", expr :: values :: Nil)
        } else In(expr, values.query)

      case Right(v) =>
        val values = visitElementList(v).map(getExpression)
        if (typeInfo <:< classOf[LocalDate]) {
          Function("date.in", expr :: values)
        } else if (typeInfo <:< classOf[LocalTime]) {
          Function("time.in", expr :: values)
        } else if ((typeInfo eq DALProvider.EntityRefType) || typeInfo <:< classOf[Entity]) {
          Function(s"entity.in", expr :: values)
        } else if (typeInfo <:< classOf[BusinessEvent]) {
          Function(s"event.in", expr :: values)
        } else In(expr, values)
    }

    ExpressionElement(c)
  }

  protected override def handleFuncCall(func: FuncElement): RelationElement = {
    val mc = func.callee.asInstanceOf[MethodCallee]
    val desc = mc.method
    val declType = desc.declaringType
    val inst = getExpression(visitElement(func.instance))
    val args = visitElementList(func.arguments).map(getExpression)
    val fe = if (declType <:< classOf[String]) {
      Function(s"text.${desc.name}", inst :: args)
    } else if (declType <:< classOf[Number]) desc.name match {
      case "doubleValue" | "intValue" | "longValue" | "shortValue" | "floatValue" | "byteValue" => inst
      case _ =>
        throw new RelationalUnsupportedException(s"The func call '${desc.name}' is not supported")
    }
    else if (declType <:< BigDecimal.getClass) desc.name match {
      case "double2bigDecimal" | "long2bigDecimal" | "int2bigDecimal" => args.head
      case _ =>
        throw new RelationalUnsupportedException(s"The func call '${desc.name}' is not supported")
    }
    else if (TypeInfo.isOption(declType) && desc.name == "value") {
      Function(s"option.${desc.name}", inst :: args)
    } else if (declType <:< classOf[Knowable[_]]) {
      Function(s"knowable.${desc.name}", inst :: args)
    } else if (declType <:< classOf[ZonedDateTime]) {
      Function(s"datetimez.${desc.name}", inst :: args)
    } else if (declType <:< ZonedDateTimeOps.getClass && desc.name == "equalInstant") {
      Function(s"datetimez.equalInstant", args)
    } else if (declType <:< classOf[LocalDate]) {
      Function(s"date.${desc.name}", inst :: args)
    } else if (declType <:< classOf[Instant]) {
      Function(s"instant.${desc.name}", inst :: args)
    } else if (declType <:< classOf[LocalTime]) {
      Function(s"time.${desc.name}", inst :: args)
    } else if (declType <:< DALProvider.EntityRefType) {
      Function(s"convert.${desc.name}", args)
    } else if (declType <:< classOf[Iterable[_]]) {
      if (desc.name == "contains") {
        var skCount = 0
        val newArgs = args.collect {
          // this is only possible from SerializedKeyBasedIndexOptimizer
          case rc @ RichConstant(_: SerializedKey, _, _) =>
            skCount += 1
            rc
          case RichConstant(value, typeInfo, _) =>
            RichConstant(Seq(value), new TypeInfo[Seq[_]](Seq(classOf[Seq[_]]), Nil, Nil, Seq(typeInfo)))
          case t => t
        }
        if (skCount == 0)
          Function(JsonbOps.Contains.Name, inst :: newArgs)
        else {
          require(skCount == newArgs.size)
          Function(CollectionOps.Contains.Name, inst :: newArgs)
        }
      } else Function(s"jsonb.${desc.name}", inst :: args)
    } else if (
      (declType eq TypeInfo.UNIT) && (desc.name == ConvertOps.ToJsonbArray.Name || desc.name == ConvertOps.ToText.Name)
    ) {
      Function(desc.name, args)
    } else {
      throw new RelationalUnsupportedException(s"The func call '${desc.name}' is not supported")
    }
    ExpressionElement(fe)
  }

  protected override def handleColumn(column: ColumnElement): RelationElement = {
    if (
      column.name.startsWith(DALProvider.StorageTxTime)
      || column.name.startsWith(DALProvider.VersionedRef)
      || column.name.startsWith(DALProvider.InitiatingEvent)
    ) {
      ExpressionElement(Property(PropertyType.Special, Seq(column.name), aliasIdMap.get(column.alias)))
    } else super.handleColumn(column)
  }

  protected override def handleTypeIs(typeIs: TypeIsElement): RelationElement = {
    val ele = getExpression(visitElement(typeIs.element))
    ExpressionElement(
      Function(EntityOps.TypeIs.Name, List(ele, Constant(typeIs.targetType.runtimeClassName, TypeCode.String))))
  }
}

object DALFormatter {
  def format(element: RelationElement): ExpressionQuery = {
    val formatter = new DALFormatter
    val ele = formatter.visitElement(element)
    ExpressionQuery(formatter.getExpression(ele), QueryPlan.Accelerated)
  }
}
