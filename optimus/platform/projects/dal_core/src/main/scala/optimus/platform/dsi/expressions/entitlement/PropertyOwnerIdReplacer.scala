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
package optimus.platform.dsi.expressions.entitlement

import optimus.platform.dsi.expressions.Expression
import optimus.platform.dsi.expressions.ExpressionVisitor
import optimus.platform.dsi.expressions.Id
import optimus.platform.dsi.expressions.Property

private class PropertyOwnerIdReplacer private (val searchFor: Set[Id], val targetId: Id) extends ExpressionVisitor {

  override def visitProperty(p: Property): Expression = {
    if (searchFor.contains(p.owner)) Property(p.propType, p.names, targetId)
    else p
  }
}

private[entitlement] object PropertyOwnerIdReplacer {
  def replace(e: Expression, searchFor: Set[Id], targetId: Id): Expression = {
    new PropertyOwnerIdReplacer(searchFor, targetId).visit(e)
  }
}
