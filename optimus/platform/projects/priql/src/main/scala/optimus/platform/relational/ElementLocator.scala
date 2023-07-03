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
package optimus.platform.relational

import optimus.platform.relational.data.tree.DbQueryTreeVisitor
import optimus.platform.relational.tree.RelationElement

import scala.collection.mutable

private[platform] class ElementLocator(private val p: RelationElement => Boolean) extends DbQueryTreeVisitor {
  private[this] val results = new mutable.HashSet[RelationElement]()

  def locate(e: RelationElement): List[RelationElement] = {
    visitElement(e)
    results.toList
  }

  override def visitElement(element: RelationElement): RelationElement = {
    if (element != null && p(element))
      results.add(element)
    super.visitElement(element)
  }
}

private[optimus] object ElementLocator {
  def locate(e: RelationElement, p: RelationElement => Boolean): List[RelationElement] = {
    new ElementLocator(p).locate(e)
  }
}
