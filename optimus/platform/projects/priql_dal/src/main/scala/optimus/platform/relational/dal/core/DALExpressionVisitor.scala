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

import optimus.entity.IndexInfo
import optimus.platform._
import optimus.platform.dsi.expressions.Constant
import optimus.platform.dsi.expressions.Expression
import optimus.platform.dsi.expressions.ExpressionVisitor
import optimus.platform.dsi.expressions.Function
import optimus.platform.dsi.expressions.OptionOps
import optimus.platform.dsi.expressions.TypeCode
import optimus.platform.relational.AsyncValueHolder
import optimus.platform.relational.RelationalException
import optimus.platform.relational.data.QueryParameter
import optimus.platform.relational.tree.TypeInfo

// Used to represent NamedValueElement. Will be replaced by real value when executed.
private[dal] final case class NamedValue(name: String) extends Expression

// Used to represent Const value with TypeInfo. Will be pickled when executed.
private[dal] final case class RichConstant(value: Any, rowTypeInfo: TypeInfo[_], index: Option[IndexColumnInfo] = None)
    extends Expression

private[dal] class DALExpressionVisitor extends ExpressionVisitor {
  override def visit(ex: Expression): Expression = {
    ex match {
      case e: NamedValue   => visitNamedValue(e)
      case e: RichConstant => visitRichConstant(e)
      case _               => super.visit(ex)
    }
  }

  protected def visitNamedValue(namedValue: NamedValue): Expression = {
    namedValue
  }

  protected def visitRichConstant(c: RichConstant): Expression = {
    c
  }
}

private class ParameterReplacer private (paramMap: List[(QueryParameter, Any)]) extends DALExpressionVisitor {
  protected override def visitNamedValue(namedValue: NamedValue): Expression = {
    paramMap.find(p => p._1.name == namedValue.name).map { case (param, value) =>
      RichConstant(value, param.typeInfo)
    } getOrElse (throw new RelationalException(s"Can't find ${namedValue.name} in the parameters."))
  }
}

object ParameterReplacer {
  def replace(paramMap: List[(QueryParameter, Any)], expr: Expression): Expression = {
    val replacer = new ParameterReplacer(paramMap)
    replacer.visit(expr)
  }
}

object AsyncValueEvaluator {
  import optimus.platform.AsyncImplicits._

  def prepareForDisplay(e: Expression): Expression = {
    val richConstants = new RichConstantCollector().collect(e)
    val parpared = new util.IdentityHashMap[RichConstant, Expression]
    richConstants.foreach {
      case r @ RichConstant(_: AsyncValueHolder[_], _, _) =>
        parpared.put(r, Constant(r.value.toString, TypeCode.String))
      case r =>
        parpared.put(r, r)
    }
    new RichConstantReplacer(parpared).visit(OptionValueRemover.remove(e))
  }

  @async def evaluate(e: Expression): Expression = {
    val richConstants = new RichConstantCollector().collect(e)
    val evaluatedTuples = richConstants.apar.map {
      case r @ RichConstant(h: AsyncValueHolder[_], _, _) => r -> r.copy(value = h.evaluate)
      case r                                              => r -> r
    }
    val evaluated = new util.IdentityHashMap[RichConstant, Expression]
    evaluatedTuples.foreach(t => evaluated.put(t._1, t._2))
    new RichConstantReplacer(evaluated).visit(e)
  }

  private class RichConstantCollector extends DALExpressionVisitor {
    private var richConstants = List.empty[RichConstant]
    override protected def visitRichConstant(c: RichConstant): Expression = {
      richConstants = c :: richConstants
      super.visitRichConstant(c)
    }

    def collect(e: Expression): Seq[RichConstant] = {
      visit(e)
      richConstants
    }
  }

  private class RichConstantReplacer(val richConstantLookup: util.IdentityHashMap[RichConstant, Expression])
      extends DALExpressionVisitor {
    override protected def visitRichConstant(c: RichConstant): Expression = {
      richConstantLookup.get(c)
    }
  }

  // only used for print query plan since we do not evaluate asyncValue for display
  private object OptionValueRemover extends DALExpressionVisitor {
    def remove(e: Expression): Expression = visit(e)

    override protected def visitFunction(f: Function): Expression = {
      f match {
        case Function(OptionOps.Value.Name, List(c @ RichConstant(_: AsyncValueHolder[_], _, _))) => c
        case _ => super.visitFunction(f)
      }
    }
  }
}
