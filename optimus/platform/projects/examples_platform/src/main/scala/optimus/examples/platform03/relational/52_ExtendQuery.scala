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

object ExtendQuery extends LegacyOptimusApp {

  // This example demonstrates 'extend', 'extendTyped' , 'cast'  methods

  untypedExtendExample

  typedExtendExample

  def untypedExtendExample = {
    // This example shows using untyped extend operator to extend Relation

    val posQ = from(Data.getPositions())
    val posValueQ = withPositionValue(posQ).filter(_.get("portfolio") == "David")

    val castQ = posValueQ.shapeTo(d =>
      PositionValue(
        d.get("portfolio").asInstanceOf[String],
        d.get("symbol").asInstanceOf[String],
        d.get("quantity").asInstanceOf[Int],
        d.get("unitValue").asInstanceOf[Float],
        d.get("positionValue").asInstanceOf[Float]
      ))

    val res = Query.execute(castQ)
    res.foreach(println(_))
  }

  def withPositionValue[T <: { def quantity: Int }](relation: Query[T]): Query[DynamicObject] = {
    relation
      .extend("unitValue", d => 100f)
      .extend("positionValue", d => d.get("quantity").asInstanceOf[Int] * d.get("unitValue").asInstanceOf[Float])
  }

  def typedExtendExample = {
    // This example shows using typed extend operator to extend Relation

    // Convert type from class Position to trait PositionBase
    val posBaseSet = Data.getPositions().asInstanceOf[Set[PositionBase]]
    val posValueQ =
      from(posBaseSet).extendTyped(p =>
        new Value { def unitValue = 100f; def positionValue = (p.quantity * 100).toFloat })
    val q = for (pv <- posValueQ if pv.positionValue > 100000) yield pv
    val res = Query.execute(posValueQ)
    res.foreach(println(_))
  }

  trait Value {
    def unitValue: Float
    def positionValue: Float
  }
}
