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
package optimus.examples.platform03.relational

import optimus.platform._
import optimus.platform.relational._

object FlatMap extends LegacyOptimusApp {

  final case class FlatMapClass(val symbol: String, val quantity: Int)
  @entity class FlatMapEntity(val portfolio: String, val percentage: Int)

  val query_cls = from(Data.getPositions()).flatMap(x =>
    Set(FlatMapClass(x.symbol + "-one", x.quantity), FlatMapClass(x.symbol + "-two", x.quantity)))
  val query_entity = from(Data.getPositions())
    .groupBy(_.portfolio)
    .flatMap(x => {
      val total = x._2.sum(_.quantity)
      x._2.map(r => FlatMapEntity(x._1, r.quantity * 100 / total))
    })

  val cls_result = Query.execute(query_cls)
  for (o <- cls_result)
    println(o.symbol + " | " + o.quantity)
  val entity_result = Query.execute(query_entity)
  for (o <- entity_result)
    println(o.portfolio + " | " + o.percentage)

}
