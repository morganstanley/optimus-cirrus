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

import optimus.entity.EntityInfoRegistry
import optimus.graph.DiagnosticSettings
import optimus.graph.Node
import optimus.platform._
import optimus.platform.dal.DalAPI
import optimus.platform.dal.EntityResolverReadImpl
import optimus.platform.pickling.PropertyMapOutputStream
import optimus.platform.AsyncImplicits._
import optimus.platform._
import optimus.platform.dsi.bitemporal.DSIQueryTemporality
import optimus.platform.dsi.bitemporal.ExpressionQueryCommand
import optimus.platform.dsi.bitemporal.QueryPlan
import optimus.platform.dsi.expressions._
import optimus.platform.dsi.expressions.{Entity => EntityExpression}
import optimus.platform.relational.RelationalUnsupportedException
import optimus.platform.relational.dal.core.AsyncValueEvaluator
import optimus.platform.relational.dal.core.DALExecutionProvider
import optimus.platform.relational.dal.core.DALExpressionVisitor
import optimus.platform.relational.dal.core.RichConstant
import optimus.platform.relational.data.FieldReader
import optimus.platform.relational.data.tree.OptionElement
import optimus.platform.relational.inmemory.IterableSource
import optimus.platform.relational.tree.ExecuteOptions
import optimus.platform.relational.tree.QueryExplainItem
import optimus.platform.relational.tree.TypeInfo
import optimus.platform.storable.SerializedKey
import optimus.platform.temporalSurface.impl.TemporalContextImpl
import optimus.platform.temporalSurface.operations.QueryByClass

import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable

class DALAccExecutionProvider[T](
    ex: Expression,
    nf: Either[FieldReader => Node[T], FieldReader => T],
    shape: TypeInfo[_],
    key: RelationKey[_],
    da: DalAPI,
    executeOptions: ExecuteOptions)
    extends DALExecutionProvider(ex, QueryPlan.Accelerated, nf, shape, key, da, executeOptions)
    with IterableSource[T] {
  import DALAccExecutionProvider._

  if (executeOptions.entitledOnly)
    throw new RelationalUnsupportedException("EntitledOnly is not supported for acc priql queries")

  override def getProviderName: String = "DALAccExecutionProvider"

  override def makeKey(newKey: RelationKey[_]): DALAccExecutionProvider[T] = {
    new DALAccExecutionProvider(expression, nodeOrFunc, rowTypeInfo, newKey, dalApi, executeOptions)
  }

  override def fillQueryExplainItem(level_id: Integer, table: mutable.ListBuffer[QueryExplainItem]): Unit = {
    val displayExpr = AsyncValueEvaluator.prepareForDisplay(expression)
    val forDisplay = ConstFormatter.format(displayExpr, null, true)
    val detail = ExpressionSqlFormatter.format(forDisplay, new DefaultConstFormatter, PrintThreshold)
    table += QueryExplainItem(level_id, "DALAccess(Accelerated)", "-", detail, 0)
  }

  @async override def get(): Iterable[T] = {
    val resolver = EvaluationContext.env.entityResolver.asInstanceOf[EntityResolverReadImpl]
    getViaExpression(resolver)
  }

  @async override def buildServerSideCommand(): ExpressionQueryCommand = {
    // Pickle const value in Expression.
    val pickledEx = ConstFormatter.format(AsyncValueEvaluator.evaluate(expression), dalApi.loadContext)

    // Fill DSIQueryTemporality to Entity and Linkage in expression.
    val expr = QuerySourceCollector.collect(pickledEx, { case x @ (_: Entity | _: Linkage) => x })
    val temporalityMap = expr
      .collect {
        case entity: EntityExpression => entity.name
        case linkage: Linkage         => linkage.name
      }
      .toSet
      .apar
      .map(name => {
        val queryByClass = QueryByClass(null, EntityInfoRegistry.getClassInfo(name).runtimeClass)
        val tc = dalApi.loadContext.asInstanceOf[TemporalContextImpl]
        val temporality = tc.operationTemporalityFor(queryByClass)
        val at = DSIQueryTemporality.At(temporality.validTime, temporality.txTime)
        (name, at)
      })
      .toMap

    val newExpr = expr.map(e =>
      e match {
        case entity: EntityExpression =>
          EntityExpression(entity.name, temporalityMap(entity.name), entity.superClasses, entity.id)
        case linkage: Linkage => Linkage(linkage.name, temporalityMap(linkage.name), linkage.id)
      })
    val e = ExpressionReplacer.replace(pickledEx, expr, newExpr)
    ExpressionQueryCommand(e, QueryPlan.Accelerated, entitledOnly = false)
  }
}

object DALAccExecutionProvider {
  import DALExecutionProvider._

  private val defaultPrintThreshold = AbbreviationThreshold(
    Option(DiagnosticSettings.getIntProperty("optimus.priql.print.expressionListAbbreviationThreshold", 10)),
    Option(DiagnosticSettings.getIntProperty("optimus.priql.print.expressionWhereAbbreviationThreshold", 500))
  )
  private val printThreshold = new AtomicReference(defaultPrintThreshold)
  private[optimus] def setPrintThreshold(t: AbbreviationThreshold) = printThreshold.set(t)
  private[optimus] def resetPrintThreshold() = printThreshold.set(defaultPrintThreshold)
  def PrintThreshold = printThreshold.get

  class ConstFormatter(loadContext: TemporalContext, val forDisplay: Boolean) extends DALExpressionVisitor {
    protected override def visitFunction(f: Function): Expression = {
      (f.method, f.arguments, visitExpressionList(f.arguments)) match {
        case ("knowable.value", _, List(Constant(s: Seq[_], _))) =>
          if (s.size == 1) Constant(null, TypeCode.None) else Constant(s.last, typeCode(s.last))
        case ("option.value", RichConstant(valueOpt: Option[_], _, _) :: Nil, visitedArgs) =>
          if (valueOpt == None) Function("option.value", visitedArgs)
          else {
            val result = Function("option.value", visitedArgs)
            val pickler = PicklerSelector.getPickler(valueOpt.get)
            if (pickler.isInstanceOf[CaseObjectPickler[_]] && !forDisplay)
              Function("convert.toJsonb", List(Function("convert.toText", List(result))))
            else
              result
          }
        case (m, _, List(Constant(e1, _), Constant(e2, _))) if m.endsWith(".equals") =>
          Constant(e1 == e2, TypeCode.Boolean)
        case (m, _, List(Constant(e1, _), Constant(e2, _))) if m.endsWith(".notEquals") =>
          Constant(e1 != e2, TypeCode.Boolean)
        case (_, _, args) => updateFunction(f, args)
      }
    }

    override def visitBinary(b: Binary): Expression = {
      import BinaryOperator._

      (b.op, visit(b.left), visit(b.right)) match {
        case (Equal, Constant(v1, _), Constant(v2, _))    => Constant(v1 == v2, TypeCode.Boolean)
        case (NotEqual, Constant(v1, _), Constant(v2, _)) => Constant(v1 != v2, TypeCode.Boolean)
        case (OrElse, l, r @ Constant(v: Boolean, _))     => if (v) r else l
        case (OrElse, l @ Constant(v: Boolean, _), r)     => if (v) l else r
        case (AndAlso, l, r @ Constant(v: Boolean, _))    => if (v) l else r
        case (AndAlso, l @ Constant(v: Boolean, _), r)    => if (v) r else l
        case (_, l, r)                                    => updateBinary(b, l, r)
      }
    }

    protected override def visitRichConstant(c: RichConstant): Expression = {
      if (c.value == null) Constant(c.value, TypeCode.None)
      else if (c.value.isInstanceOf[SerializedKey]) Constant(c.value, TypeCode.SerializedKey)
      else {
        val underlyingValue = OptionElement.underlyingValue(c.value)
        checkLoadContext(underlyingValue, loadContext, false)
        val pickler = PicklerSelector.getPickler(c.value)
        // Get pickled value using pickler
        val v = PropertyMapOutputStream.pickledValue(c.value, pickler)

        if (pickler.isInstanceOf[CaseObjectPickler[_]]) {
          if (forDisplay) Constant(v, typeCode(v))
          else Function("convert.toJsonb", List(Function("convert.toText", List(Constant(v, typeCode(v))))))
        } else Constant(v, typeCode(v))
      }
    }
  }

  object ConstFormatter {
    def format(e: Expression, loadContext: TemporalContext, forDisplay: Boolean = false): Expression = {
      val c = new ConstFormatter(loadContext, forDisplay)
      c.visit(e)
    }
  }
}
