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
import optimus.platform.relational.data.tree.ColumnElement
import optimus.platform.relational.data.tree.SelectElement
import optimus.platform.relational.data.tree.ProjectionElement
import optimus.platform.relational.RelationalException
import scala.collection.immutable.HashSet

/*
 * Removes one or more SelectElement's by rewriting the expression tree to not include them, promoting
 * their from clause expressions and rewriting any column elements that may have referenced them to now
 * reference the underlying data directly.
 */
class SubqueryRemover(selectsToRemove: Seq[SelectElement]) extends DbQueryTreeVisitor {
  private val selects = HashSet[SelectElement](selectsToRemove: _*)
  private val map = selectsToRemove.map(s => (s.alias, s.columns.map(c => (c.name, c.element)).toMap)).toMap

  protected override def handleSelect(select: SelectElement): RelationElement = {
    if (selects.contains(select))
      visitElement(select.from)
    else
      super.handleSelect(select)
  }

  protected override def handleColumn(column: ColumnElement): RelationElement = {
    val nameMap = map.getOrElse(column.alias, null)
    if (nameMap eq null) column
    else {
      val e = nameMap.getOrElse(column.name, null)
      if (e eq null)
        throw new RelationalException("Reference to undefined column")
      visitElement(e)
    }
  }
}

object SubqueryRemover {
  def remove(outerSelect: SelectElement, selectsToRemove: SelectElement*): SelectElement = {
    new SubqueryRemover(selectsToRemove).visitElement(outerSelect).asInstanceOf[SelectElement]
  }

  def remove(proj: ProjectionElement, selectsToRemove: SelectElement*): ProjectionElement = {
    new SubqueryRemover(selectsToRemove).visitElement(proj).asInstanceOf[ProjectionElement]
  }
}
