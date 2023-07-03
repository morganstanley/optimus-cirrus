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

import optimus.platform.relational.data.tree.ColumnDeclaration
import optimus.platform.relational.data.tree.ColumnElement
import optimus.platform.relational.data.tree.DbQueryTreeVisitor
import optimus.platform.relational.data.tree.SelectElement
import optimus.platform.relational.data.tree.TableElement
import optimus.platform.relational.tree.RelationElement

class SourceColumnFinder(via: ColumnElement) extends DbQueryTreeVisitor {
  private var source: Option[TableElement] = None
  private var column: ColumnElement = via

  def find(e: RelationElement): Option[ColumnElement] = {
    visitElement(e)
    source.map(_ => column)
  }

  protected override def handleSelect(select: SelectElement): RelationElement = {
    if (select.alias == column.alias) {
      select.columns.find(d => d.name == column.name) match {
        case Some(ColumnDeclaration(_, c: ColumnElement)) =>
          column = c
          visitElement(select.from)
        case _ =>
      }
      select
    } else super.handleSelect(select)
  }

  protected override def handleTable(table: TableElement): RelationElement = {
    if (table.alias == column.alias)
      source = Some(table)
    table
  }
}

object SourceColumnFinder {
  def findSourceColumn(from: RelationElement, decl: ColumnDeclaration): Option[ColumnElement] = {
    decl.element match {
      case c: ColumnElement => findSourceColumn(from, c)
      case _                => None
    }
  }

  // find the source column from a TableElement (ignore scalar & computed columns)
  def findSourceColumn(from: RelationElement, c: ColumnElement): Option[ColumnElement] = {
    new SourceColumnFinder(c).find(from)
  }
}
