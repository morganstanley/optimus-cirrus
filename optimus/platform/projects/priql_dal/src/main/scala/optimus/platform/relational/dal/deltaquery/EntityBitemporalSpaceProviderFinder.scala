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
package optimus.platform.relational.dal.deltaquery

import optimus.platform.dsi.expressions.Expression
import optimus.platform.dsi.expressions.Select
import optimus.platform.relational.RelationalException
import optimus.platform.relational.RelationalUnsupportedException
import optimus.platform.relational.TypeMismatchException
import optimus.platform.relational.tree.MethodElement
import optimus.platform.relational.tree.ProviderRelation
import optimus.platform.relational.tree.QueryMethod.ARRANGE
import optimus.platform.relational.tree.QueryTreeVisitor
import optimus.platform.relational.tree.RelationElement

object EntityBitemporalSpaceProviderFinder {
  def find(element: RelationElement): DALEntityBitemporalSpaceProvider =
    new EntityBitemporalSpaceProviderFinder(element).find
}
private class EntityBitemporalSpaceProviderFinder(element: RelationElement) extends QueryTreeVisitor {

  private[this] var updateExecutioner: DALEntityBitemporalSpaceProvider = null

  private def validate(expression: Expression) = {
    expression match {
      case s @ Select(_, _, where, _, _, _, _, _, _, _) =>
        if (where.isEmpty) {
          throw new RelationalUnsupportedException(
            "Query without filters on indexed field is not supported by delta API")
        }
      case s => throw new TypeMismatchException("Unexpected type $s")
    }
  }

  override def handleMethod(method: MethodElement): RelationElement = {
    if (method.methodCode != ARRANGE)
      throw new RelationalException(s"Expected method: ARRANGE, found: ${method.methodCode}")
    super.handleMethod(method)
  }

  override def handleQuerySrc(element: ProviderRelation): RelationElement = {
    element match {
      case d: DALEntityBitemporalSpaceProvider => {
        validate(d.expression)
        updateExecutioner = d
        super.handleQuerySrc(element)
      }
      case _ => super.handleQuerySrc(element)
    }
  }

  // if updateExecutioner is found, exit quickly
  override def visitElement(element: RelationElement): RelationElement = {
    if (updateExecutioner == null) super.visitElement(element) else element
  }

  private def find = {
    visitElement(element)
    if (updateExecutioner == null)
      throw new RelationalException("Could not find DalUpdatesProvider")
    updateExecutioner
  }

}
