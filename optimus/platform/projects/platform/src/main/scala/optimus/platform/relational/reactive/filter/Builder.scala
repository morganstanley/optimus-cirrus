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
package optimus.platform.relational.reactive.filter

final case class Builder(
    private var condition: Condition = null,
    proto: String = "optimus.platform.dsi.bitemporal.proto.NotificationMessageProto") {
  import BinaryOperator._

  def and(cond: Condition) = {
    if (condition == null)
      condition = cond
    else
      condition = condition.and(cond)
    this
  }

  def or(cond: Condition) = {
    if (condition == null)
      condition = cond
    else
      condition = condition.or(cond)
    this
  }

  def build() = {
    s"${proto}::${condition}"
  }
}
