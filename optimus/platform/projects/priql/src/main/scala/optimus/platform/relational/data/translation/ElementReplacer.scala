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
package optimus.platform.relational.data.translation

import optimus.platform.relational.data.tree.DbQueryTreeVisitor
import optimus.platform.relational.tree.RelationElement
import optimus.platform.relational.RelationalException

class ElementReplacer private (val searchFor: List[RelationElement], val replaceWith: List[RelationElement])
    extends DbQueryTreeVisitor {
  if (searchFor.size != replaceWith.size)
    throw new RelationalException("Invalid argument, searchFor and replaceWith should have a same size.")

  private val lookup: Map[RelationElement, RelationElement] = searchFor.iterator.zip(replaceWith.iterator).toMap

  override def visitElement(element: RelationElement): RelationElement = {
    lookup.getOrElse(element, super.visitElement(element))
  }
}

object ElementReplacer {
  def replace(
      e: RelationElement,
      searchFor: List[RelationElement],
      replaceWith: List[RelationElement]): RelationElement = {
    new ElementReplacer(searchFor, replaceWith).visitElement(e)
  }
}
