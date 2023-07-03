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
package optimus.platform.relational.data.tree

import optimus.platform.relational.tree.RelationElement

import scala.collection.mutable

class DbQueryTreeDuplicator protected () extends DbQueryTreeVisitor {
  private[this] val map = new mutable.HashMap[TableAlias, TableAlias]

  override protected def handleTable(table: TableElement): RelationElement = {
    val newAlias = new TableAlias()
    map.put(table.alias, newAlias)
    new TableElement(newAlias, table.entity, table.name)
  }

  override protected def handleSelect(select: SelectElement): RelationElement = {
    val newAlias = new TableAlias()
    map.put(select.alias, newAlias)
    val s = super.handleSelect(select).asInstanceOf[SelectElement]
    new SelectElement(
      newAlias,
      s.columns,
      s.from,
      s.where,
      s.orderBy,
      s.groupBy,
      s.skip,
      s.take,
      s.isDistinct,
      s.reverse)
  }

  override protected def handleColumn(column: ColumnElement): RelationElement = {
    map.get(column.alias).map { alias =>
      new ColumnElement(column.rowTypeInfo, alias, column.name, column.columnInfo)
    } getOrElse (column)
  }
}

object DbQueryTreeDuplicator {
  def duplicate(e: RelationElement): RelationElement = {
    new DbQueryTreeDuplicator().visitElement(e)
  }
}
