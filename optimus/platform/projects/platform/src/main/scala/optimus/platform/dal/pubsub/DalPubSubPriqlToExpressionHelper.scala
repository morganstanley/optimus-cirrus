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
package optimus.platform.dal.pubsub

import optimus.dsi.pubsub.Subscription
import optimus.platform.storable.{Entity => DALEntity}
import optimus.platform._
import optimus.platform.dsi.bitemporal.DSIQueryTemporality
import optimus.platform.dsi.expressions._
import optimus.platform.relational.ElementLocator
import optimus.platform.relational.RelationalUnsupportedException
import optimus.platform.relational.dal.DALProvider
import optimus.platform.relational.dal.core.AsyncValueEvaluator
import optimus.platform.relational.dal.core.DALExecutionProvider.ConstFormatter
import optimus.platform.relational.dal.pubsub.PubSubExecutionProvider
import optimus.platform.relational.dal.pubsub.PubSubReducer

object DalPubSubPriqlToExpressionHelper {

  private val tc = TemporalContext(TimeInterval.Infinity, TimeInterval.Infinity)

  def buildExpression[T <: DALEntity](q: Query[T]): Expression = buildExpressionAndEntitledOnly(q)._1

  /**
   * Resolved Expression / ClientFunction / EntitledOnly properties from PriQL query.
   * @return (Expression, ClientFunction (Filter predicate), EntitledOnly)
   */
  def buildExpressionAndEntitledOnly[T <: DALEntity](
      q: Query[T]
  ): (Expression, Option[NodeFunction1[Any, Boolean]], Boolean) = {
    val List(dp: DALProvider) = ElementLocator.locate(q.element, e => e.isInstanceOf[DALProvider])
    val reducedElem = new PubSubReducer(dp).reduce(q.element)
    val pubSubExecutionProvider = reducedElem match {
      case ex: PubSubExecutionProvider[_] => ex
      case x => throw new RelationalUnsupportedException(s"Unexpected execution provider $x")
    }
    val clientFunction = pubSubExecutionProvider.clientFilter
    // This temporal context is not used by PubSub broker in any way, it is only required for the Expression serialization.
    // If we do not give a value, it uses null and fails.
    // In case a subscription uses another entity as a value in the expression, the serialization code requires the
    // loadContext of that entity and the temporalContext of the expression to be same.
    val tctx = if (EvaluationContext.isInitialised) loadContext else tc
    val select = AsyncValueEvaluator.evaluate(pubSubExecutionProvider.expression).asInstanceOf[Select]
    val entity = select.from.asInstanceOf[Entity]
    (
      ConstFormatter.format(
        select.copy(
          sortBy = Nil,
          take = None,
          from = entity.copy(when = DSIQueryTemporality.At(TimeInterval.Infinity, TimeInterval.Infinity))),
        tctx),
      clientFunction,
      pubSubExecutionProvider.executeOptions.entitledOnly)
  }

  /**
   * Create a Subscription by given PriQL query and other parameters.
   *
   * @param query
   *   a PriQL Query
   * @param entitledOnly
   *   a function that given query parameter's entitledOnly state, update it based on other configuration to give the
   *   final entitled only state. This is optional, and by default the PriQL query's entitledOnly state is used as
   *   Subscription's entitledOnly state.
   */
  def toSubscription[T <: DALEntity](
      query: Query[T],
      includeSow: Boolean = false,
      entitledOnly: Boolean => Boolean = x => x
  ): Subscription = {
    val (expr, cf, eo) = buildExpressionAndEntitledOnly(query)
    Subscription(expr, cf, includeSow = includeSow, entitledOnly = entitledOnly(eo))
  }
}
