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

import optimus.platform.dsi.bitemporal.QueryPlan
import optimus.platform.dsi.expressions.Binary
import optimus.platform.dsi.expressions.BinaryOperator
import optimus.platform.dsi.expressions.CollectionOps
import optimus.platform.dsi.expressions.Function
import optimus.platform.dsi.expressions.In
import optimus.platform.dsi.expressions.Property
import optimus.platform.dsi.expressions.PropertyType
import optimus.platform.relational.RelationalException
import optimus.platform.relational.RelationalUnsupportedException
import optimus.platform.relational.dal.DALProvider
import optimus.platform.relational.data.tree.ColumnElement
import optimus.platform.relational.data.tree.ContainsElement
import optimus.platform.relational.data.tree.DbElementType
import optimus.platform.relational.tree._
import optimus.platform.storable.Storable

class DALFormatter extends DALFormatterBase {
  override def visitElement(e: RelationElement): RelationElement = {
    if (e eq null) null
    else {
      e.elementType match {
        case DbElementType.Table | DbElementType.Column | DbElementType.Select | DbElementType.Contains |
            DbElementType.Aggregate | ElementType.ConstValue | ElementType.BinaryExpression |
            ElementType.ForteFuncCall =>
          super.visitElement(e)
        case _ =>
          throw new RelationalUnsupportedException(s"The relation element of type ${e.elementType} is not supported")
      }
    }
  }

  protected override def handleFuncCall(func: FuncElement): RelationElement = {
    val mc = func.callee.asInstanceOf[MethodCallee]
    val desc = mc.method
    val declType = desc.declaringType
    if (declType <:< DALProvider.EntityRefType) {
      val args = visitElementList(func.arguments).map(getExpression)
      ExpressionElement(Function(s"convert.${desc.name}", args))
    } else if (declType <:< classOf[Iterable[_]] && desc.name == "contains") {
      (func.instance, func.arguments) match {
        case (c: ColumnElement, List(v: ConstValueElement)) =>
          val inst = getExpression(visitElement(c))
          val args = List(RichConstant(v.value, v.rowTypeInfo, getIndexInfo(c)))
          ExpressionElement(Function(CollectionOps.Contains.Name, inst :: args))
        case _ => super.handleFuncCall(func)
      }
    } else
      super.handleFuncCall(func)
  }

  protected override def handleColumn(column: ColumnElement): RelationElement = {
    if (
      column.name == DALProvider.EntityRef || column.name == DALProvider.StorageTxTime ||
      column.name == DALProvider.VersionedRef
    ) {
      ExpressionElement(Property(PropertyType.Special, Seq(column.name), aliasIdMap.get(column.alias)))
    } else super.handleColumn(column)
  }

  protected def getIndexInfo[T <: Storable](c: ColumnElement): Option[IndexColumnInfo] = {
    c.columnInfo match {
      case i: IndexColumnInfo => Some(i)
      case _                  => None
    }
  }

  protected override def handleBinaryExpression(binary: BinaryExpressionElement): RelationElement = {
    binary match {
      case BinaryExpressionElement(BinaryExpressionType.EQ, c: ColumnElement, v: ConstValueElement, _) =>
        val l = getExpression(visitElement(c))
        val r = RichConstant(v.value, v.rowTypeInfo, getIndexInfo(c))
        ExpressionElement(Binary(BinaryOperator.Equal, l, r))
      case BinaryExpressionElement(BinaryExpressionType.EQ, v: ConstValueElement, c: ColumnElement, _) =>
        val l = getExpression(visitElement(c))
        val r = RichConstant(v.value, v.rowTypeInfo, getIndexInfo(c))
        ExpressionElement(Binary(BinaryOperator.Equal, l, r))
      case _ =>
        super.handleBinaryExpression(binary)
    }
  }

  protected override def handleContains(contains: ContainsElement): RelationElement = {
    contains.element match {
      case c: ColumnElement =>
        val ex = getExpression(visitElement(c))
        val index = getIndexInfo(c)
        val values = contains.values match {
          case Left(_) =>
            throw new RelationalException("Expect ConstValueElement list in ContainsElement but got ScalarElement")
          case Right(v) => v.map { case c: ConstValueElement => RichConstant(c.value, c.rowTypeInfo, index) }
        }
        ExpressionElement(In(ex, values))
      case _ =>
        super.handleContains(contains)
    }
  }
}

object DALFormatter {
  def format(element: RelationElement): ExpressionQuery = {
    val formatter = new DALFormatter
    val e = formatter.visitElement(element)
    ExpressionQuery(formatter.getExpression(e), QueryPlan.Default)
  }
}

class DALRegisteredIndexFormatter extends DALFormatter {

  protected override def handleBinaryExpression(binary: BinaryExpressionElement): RelationElement = {
    binary match {
      case BinaryExpressionElement(
            BinaryExpressionType.GT | BinaryExpressionType.GE | BinaryExpressionType.LT | BinaryExpressionType.LE,
            c: ColumnElement,
            v: ConstValueElement,
            _) =>
        val l = getExpression(visitElement(c))
        val r = RichConstant(v.value, v.rowTypeInfo, getIndexInfo(c))
        ExpressionElement(Binary(DALFormatterHelper.binaryOperator(binary.op), l, r))
      case BinaryExpressionElement(
            BinaryExpressionType.GT | BinaryExpressionType.GE | BinaryExpressionType.LT | BinaryExpressionType.LE,
            v: ConstValueElement,
            c: ColumnElement,
            _) =>
        val l = getExpression(visitElement(c))
        val r = RichConstant(v.value, v.rowTypeInfo, getIndexInfo(c))
        ExpressionElement(Binary(DALFormatterHelper.binaryOperator(binary.op), l, r))
      case _ =>
        super.handleBinaryExpression(binary)
    }
  }
}

object DALRegisteredIndexFormatter {
  def format(element: RelationElement): ExpressionQuery = {
    val formatter = new DALRegisteredIndexFormatter
    val e = formatter.visitElement(element)
    ExpressionQuery(formatter.getExpression(e), QueryPlan.Default)
  }
}
