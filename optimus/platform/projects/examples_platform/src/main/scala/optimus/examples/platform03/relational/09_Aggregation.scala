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

object Aggregation extends LegacyOptimusApp {

  // the following query makes use of may be aggregate functions
  val byPositions =
    for ((po, posGrp) <- Data.getPositionValues().groupBy(p => p.portfolio))
      yield new {
        val portfolio = po
        val maxQuantity = posGrp.map(_.quantity).max
        val numPositions = posGrp.count
      }

  val res = Query.execute(byPositions)
  res.foreach(o =>
    println(
      " portfolio=" + o.portfolio
        + " , maxQuantity=" + o.maxQuantity + " , numPositions=" + o.numPositions))

}
