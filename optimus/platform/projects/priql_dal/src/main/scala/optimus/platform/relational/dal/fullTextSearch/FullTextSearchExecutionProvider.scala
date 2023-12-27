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
package optimus.platform.relational.dal.fullTextSearch

import optimus.entity.EntityInfoRegistry
import optimus.graph.Node
import optimus.platform.dal.DalAPI
import optimus.platform.dal.EntityResolverReadImpl
import optimus.platform.dsi.bitemporal.ExpressionQueryCommand
import optimus.platform.dsi.bitemporal.QueryPlan
import optimus.platform.dsi.bitemporal.DSIQueryTemporality
import optimus.platform.dsi.expressions._
import optimus.platform.dsi.expressions.{Entity => EntityExpression}
import optimus.platform.AsyncImplicits._
import optimus.platform._
import optimus.platform.pickling.PropertyMapOutputStream
import optimus.platform.relational.dal.accelerated.PicklerSelector
import optimus.platform.relational.dal.core.AsyncValueEvaluator
import optimus.platform.relational.dal.core.DALExecutionProvider
import optimus.platform.relational.dal.core.DALExpressionVisitor
import optimus.platform.relational.dal.core.RichConstant
import optimus.platform.relational.data.FieldReader
import optimus.platform.relational.inmemory.IterableSource
import optimus.platform.relational.tree.ExecuteOptions
import optimus.platform.relational.tree.QueryExplainItem
import optimus.platform.relational.tree.TypeInfo
import optimus.platform.relational.data.tree.OptionElement
import optimus.platform.temporalSurface.impl.TemporalContextImpl
import optimus.platform.temporalSurface.operations.QueryByClass

import scala.collection.mutable

class FullTextSearchExecutionProvider[T](
    ex: Expression,
    nf: Either[FieldReader => Node[T], FieldReader => T],
    shape: TypeInfo[_],
    key: RelationKey[_],
    da: DalAPI,
    executeOptions: ExecuteOptions)
    extends DALExecutionProvider(ex, QueryPlan.FullTextSearch, nf, shape, key, da, executeOptions)
    with IterableSource[T] {
  import FullTextSearchExecutionProvider._

  override def getProviderName: String = "FullTextSearchExecutionProvider"

  override def makeKey(newKey: RelationKey[_]): FullTextSearchExecutionProvider[T] = {
    new FullTextSearchExecutionProvider(expression, nodeOrFunc, rowTypeInfo, newKey, dalApi, executeOptions)
  }

  @async override def get(): Iterable[T] = {
    val resolver = EvaluationContext.env.entityResolver.asInstanceOf[EntityResolverReadImpl]
    getViaExpression(resolver)
  }

  @async override def buildServerSideCommand(): ExpressionQueryCommand = {
    // Pickle const value in Expression.
    val pickledEx = ConstFormatter.format(AsyncValueEvaluator.evaluate(expression), dalApi.loadContext)

    // Fill DSIQueryTemporality to Entity
    val expr = QuerySourceCollector.collect(pickledEx, { case x @ (_: Entity ) => x })
    val temporalityMap = expr
      .collect { case entity: EntityExpression =>
        entity.name
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
      })
    val e = ExpressionReplacer.replace(pickledEx, expr, newExpr)
    ExpressionQueryCommand(e, QueryPlan.FullTextSearch, executeOptions.entitledOnly)
  }

  override def fillQueryExplainItem(level_id: Integer, table: mutable.ListBuffer[QueryExplainItem]): Unit = {
    val displayExpr = AsyncValueEvaluator.prepareForDisplay(expression)
    val sel @ Select(e: Entity, _, _, _, Nil, _, None, _, _, _) = ConstFormatter.format(displayExpr, null)
    val detail = ExpressionAtlasSearchFormatter.format(sel)
    table += new QueryExplainItem(
      level_id,
      "DALAccess(FullTextSearch)",
      rowTypeInfo.name,
      if (detail.length == 0) "TableScan" else detail,
      0)
  }
}

object FullTextSearchExecutionProvider {
  import DALExecutionProvider._

  class ConstFormatter(loadContext: TemporalContext, val forDisplay: Boolean) extends DALExpressionVisitor {
    protected override def visitFunction(f: Function): Expression = {
      (f.method, visitExpressionList(f.arguments)) match {
        case (m, List(Constant(e1, _), Constant(e2, _))) if m.endsWith(".equals") =>
          Constant(e1 == e2, TypeCode.Boolean)
        case (m, List(Constant(e1, _), Constant(e2, _))) if m.endsWith(".notEquals") =>
          Constant(e1 != e2, TypeCode.Boolean)
        case (_, args) => updateFunction(f, args)
      }
    }

    protected override def visitRichConstant(c: RichConstant): Expression = {
      if (c.value == null) Constant(c.value, TypeCode.None)
      else {
        val underlyingValue = OptionElement.underlyingValue(c.value)
        checkLoadContext(underlyingValue, loadContext, false)
        val pickler = PicklerSelector.getPickler(c.value)
        // Get pickled value using pickler
        val v = PropertyMapOutputStream.pickledValue(c.value, pickler)

        Constant(v, typeCode(c.value))
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
