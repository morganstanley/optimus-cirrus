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

import java.util

import optimus.platform.dsi.expressions.{JoinType => ExprJoinType, _}
import optimus.platform.dsi.expressions.PropertyType
import optimus.platform.relational.RelationalException
import optimus.platform.relational.RelationalUnsupportedException
import optimus.platform.relational.data.tree._
import optimus.platform.relational.tree._

abstract class DALFormatterBase extends DbQueryTreeVisitor {
  import DALFormatterHelper._

  protected val aliasIdMap = new util.HashMap[TableAlias, Id]

  protected case class ExpressionElement(ex: Expression) extends RelationElement(Expression, TypeInfo.UNIT)
  protected case object Expression extends ElementType

  protected override def handleMemberRef(m: MemberElement): RelationElement = {
    throw new RelationalUnsupportedException(s"The member access '${m.memberName}' is not supported")
  }

  protected override def handleConstValue(element: ConstValueElement): RelationElement = {
    ExpressionElement(RichConstant(element.value, element.rowTypeInfo))
  }

  protected override def handleContains(contains: ContainsElement): RelationElement = {
    val expr = getExpression(visitElement(contains.element))
    val values = contains.values match {
      case Left(v)  => Left(getExpression(visitElement(v)).asInstanceOf[Scalar].query)
      case Right(v) => Right(visitElementList(v).map(getExpression))
    }
    ExpressionElement(In(expr, values))
  }

  protected override def handleBinaryExpression(binary: BinaryExpressionElement): RelationElement = {
    val l = getExpression(visitElement(binary.left))
    val r = getExpression(visitElement(binary.right))
    val op = binaryOperator(binary.op)
    ExpressionElement(Binary(op, l, r))
  }

  protected override def handleSelect(select: SelectElement): RelationElement = {
    addAliases(select.from)

    val fromExpr = getExpression(visitSource(select.from))

    val whereExpr = getExpression(visitElement(select.where))

    val columns = if (select.columns == null) Nil else visitColumnDeclarations(select.columns)
    val propertyDefs = columns.map(d => PropertyDef(d.name, getExpression(d.element)))

    val groupBy = if (select.groupBy == null) Nil else visitElementList(select.groupBy)
    val groupByExpr = groupBy.map(getExpression)

    val orderBy = if (select.orderBy == null) Nil else visitOrderBy(select.orderBy)
    val orderByExpr = orderBy.map {
      case OrderDeclaration(SortDirection.Ascending, ExpressionElement(expr))  => SortByDef(SortType.Asc, expr)
      case OrderDeclaration(SortDirection.Descending, ExpressionElement(expr)) => SortByDef(SortType.Desc, expr)
      case _ => throw new IllegalArgumentException(s"unexpected or misformatted order declaration: ${orderBy}")
    }

    val takeExpr = getExpression(visitElement(select.take))
    val skipExpr = getExpression(visitElement(select.skip))

    val selectId = if (aliasIdMap.containsKey(select.alias)) aliasIdMap.get(select.alias) else Id()
    val selectExpr = Select(
      fromExpr.asInstanceOf[QuerySource],
      propertyDefs,
      Option(whereExpr),
      orderByExpr,
      groupByExpr,
      Option(takeExpr),
      Option(skipExpr),
      select.isDistinct,
      select.reverse,
      selectId
    )

    ExpressionElement(selectExpr)
  }

  protected override def handleTable(table: TableElement): RelationElement = {
    // We deferred to fill in the temporality until when we send the query to DAL in DALAccExecutionProvider.
    if (aliasIdMap.containsKey(table.alias)) {
      val entity = table.entity match {
        case de: DALMappingEntity => de.format(aliasIdMap.get(table.alias))
        case _                    => throw new RelationalException("Only DALMappingEntity is support")
      }
      ExpressionElement(entity)
    } else {
      throw new RelationalException(s"Can't find owner for the column: ${table.name}")
    }
  }

  protected override def handleColumn(column: ColumnElement): RelationElement = {
    if (aliasIdMap.containsKey(column.alias)) {
      val pType = propertyType(column.columnInfo.columnType)
      val property = Property(pType, Seq(column.name), aliasIdMap.get(column.alias))
      ExpressionElement(property)
    } else {
      throw new RelationalException(s"Can't find owner for the column: ${column.name}")
    }
  }

  protected override def handleNamedValue(value: NamedValueElement): RelationElement = {
    ExpressionElement(NamedValue(value.name))
  }

  protected override def handleAggregate(aggregate: AggregateElement): RelationElement = {
    val argsExpr = visitElementList(aggregate.arguments).map(getExpression _)
    val aggExpr = Aggregate(aggregateType(aggregate.aggregateName), argsExpr, aggregate.isDistinct)
    ExpressionElement(aggExpr)
  }

  protected override def handleJoin(join: JoinElement): RelationElement = {
    val leftExpr = getExpression(visitSource(join.left))
    val rightExpr = getExpression(visitSource(join.right))
    val conditionExpr = getExpression(visitElement(join.condition))
    val joinExpr = Join(
      joinType(join.joinType),
      leftExpr.asInstanceOf[QuerySource],
      rightExpr.asInstanceOf[QuerySource],
      Option(conditionExpr))
    ExpressionElement(joinExpr)
  }

  protected override def handleScalar(scalar: ScalarElement): RelationElement = {
    val ExpressionElement(select: Select) = visitElement(scalar.select)
    ExpressionElement(Scalar(select))
  }

  protected override def handleExists(exists: ExistsElement): RelationElement = {
    val ExpressionElement(select: Select) = visitElement(exists.select)
    ExpressionElement(Exists(select))
  }

  protected override def handleProjection(proj: ProjectionElement): RelationElement = {
    proj.projector match {
      case e: ColumnElement =>
        visitElement(proj.select)
      case _ =>
        throw new RelationalException("Non-scala projections cannot be translated to SQL")
    }
  }

  protected override def handleConditional(c: ConditionalElement): RelationElement = {
    val test = getExpression(visitElement(c.test))
    val ifTrue = getExpression(visitElement(c.ifTrue))
    val ifFalse = getExpression(visitElement(c.ifFalse))
    ExpressionElement(Condition(test, ifTrue, ifFalse))
  }

  protected override def handleFuncCall(func: FuncElement): RelationElement = {
    throw new RelationalUnsupportedException(s"The func call '${func.callee.name}' is not supported")
  }

  protected override def handleTypeIs(typeIs: TypeIsElement): RelationElement = {
    throw new RelationalUnsupportedException(s"The 'isInstanceOf' is not supported")
  }

  protected final def addAlias(alias: TableAlias): Unit = {
    if (aliasIdMap.get(alias) eq null) {
      val id = Id()
      aliasIdMap.put(alias, id)
    }
  }

  protected final def addAliases(element: RelationElement): Unit = {
    element match {
      case e: AliasedElement =>
        addAlias(e.alias)

      case e: JoinElement =>
        addAliases(e.left)
        addAliases(e.right)

      case _ =>
    }
  }

  protected final def getExpression(element: RelationElement): Expression = {
    element match {
      case null                    => null
      case ExpressionElement(expr) => expr
      case _                       => throw new RelationalException(s"Cannot convert $element to an Expression.")
    }
  }
}

object DALFormatterHelper {
  import BinaryExpressionType._

  // Get BinaryOperator according to BinaryExpressionType.
  val binaryOperator: PartialFunction[BinaryExpressionType, BinaryOperator] = {
    case BinaryExpressionType.EQ      => BinaryOperator.Equal
    case BinaryExpressionType.NE      => BinaryOperator.NotEqual
    case BinaryExpressionType.LT      => BinaryOperator.LessThan
    case BinaryExpressionType.LE      => BinaryOperator.LessThanOrEqual
    case BinaryExpressionType.GT      => BinaryOperator.GreaterThan
    case BinaryExpressionType.GE      => BinaryOperator.GreaterThanOrEqual
    case BinaryExpressionType.BOOLAND => BinaryOperator.AndAlso
    case BinaryExpressionType.BOOLOR  => BinaryOperator.OrElse
    case BinaryExpressionType.PLUS    => BinaryOperator.Add
    case BinaryExpressionType.MINUS   => BinaryOperator.Subtract
    case BinaryExpressionType.MUL     => BinaryOperator.Multiply
    case BinaryExpressionType.DIV     => BinaryOperator.Divide
    case BinaryExpressionType.MODULO  => BinaryOperator.Modulo
  }

  // Get AggregateType according to aggregate name.
  val aggregateType: PartialFunction[String, AggregateType] = {
    case "average"      => AggregateType.Average
    case "count"        => AggregateType.Count
    case "distinct"     => AggregateType.Distinct
    case "max"          => AggregateType.Max
    case "min"          => AggregateType.Min
    case "sum"          => AggregateType.Sum
    case "array_agg"    => AggregateType.ArrayAgg
    case "variance"     => AggregateType.Var_samp
    case "variance_pop" => AggregateType.Var_pop
    case "stddev"       => AggregateType.Stddev_samp
    case "stddev_pop"   => AggregateType.Stddev_pop
    case "corr"         => AggregateType.Corr
  }

  // Get Expression JoinType according to join type.
  val joinType: PartialFunction[JoinType, ExprJoinType] = {
    case JoinType.InnerJoin  => ExprJoinType.InnerJoin
    case JoinType.LeftOuter  => ExprJoinType.LeftOuter
    case JoinType.RightOuter => ExprJoinType.RightOuter
    case JoinType.FullOuter  => ExprJoinType.FullOuter
    case JoinType.CrossApply => ExprJoinType.CrossApply
    case JoinType.CrossJoin  => ExprJoinType.CrossJoin
    case JoinType.OuterApply => ExprJoinType.OuterApply
  }

  val propertyType: PartialFunction[ColumnType, PropertyType] = {
    case ColumnType.EntityRef                       => PropertyType.Special
    case ColumnType.Key                             => PropertyType.Key
    case ColumnType.UniqueIndex                     => PropertyType.UniqueIndex
    case ColumnType.Index                           => PropertyType.Index
    case ColumnType.Default | ColumnType.Calculated => PropertyType.Default
  }
}
