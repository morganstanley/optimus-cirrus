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

import optimus.entity.IndexInfo
import optimus.platform.dsi.bitemporal.QueryPlan
import optimus.platform.dsi.expressions.Binary
import optimus.platform.dsi.expressions.BinaryOperator
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
    val inst = getExpression(visitElement(func.instance))
    val args = visitElementList(func.arguments).map(getExpression)
    if (declType <:< DALProvider.EntityRefType)
      ExpressionElement(Function(s"convert.${desc.name}", args))
    else
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

  private def getIndexInfo[T <: Storable](c: ColumnElement): Option[IndexInfo[T, Any]] = {
    c.columnInfo match {
      case i: IndexColumnInfo => Some(i.index.asInstanceOf[IndexInfo[T, Any]])
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
