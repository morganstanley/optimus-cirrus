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
package optimus.profiler.ui

import scala.collection.mutable.ArrayBuffer
import scala.collection.compat._

class TableViewColumn[T](val prototype: TableColumn[T], var width: Int, var filter: String) {
  def this(prototype: TableColumn[T]) = this(prototype, prototype.width, null)

  // Temp values for display only
  var index: Int = _
  var total: AnyRef = _
  var totalSelected: AnyRef = _
  var selectedCount = 0
}

object TableView {
  type TableColumns[T] = ArrayBuffer[TableViewColumn[T]]
  val deltaSymbol: String = "\u0394"

  def getGroupSuffix(i: Int): String = s"Group ${i}"

  def containsDiffColumn[T](columns: TableColumns[T]): Boolean = {
    columns.exists(_.prototype.name.startsWith(deltaSymbol))
  }

  private def addComparisonDataColIfNeccesary[T](
      newColumns: ArrayBuffer[TableViewColumn[T]],
      addComparisonData: Boolean,
      c: TableViewColumn[T],
      newPrototypeCompareDataColumn: TableColumn[T]): Unit = {
    // First check is to see if the user wants the comparison data
    // Second check is to see if the column type has a comparisonDataColumn defined on it
    if (addComparisonData && (newPrototypeCompareDataColumn ne null)) {
      if (!c.prototype.name.endsWith(getGroupSuffix(1))) c.prototype.name += getGroupSuffix(1)
      newColumns += new TableViewColumn[T](newPrototypeCompareDataColumn, c.width, null)
    }
  }

  def diffViewFrom[T](
      prefixColumn: TableColumn[T],
      columns: TableColumns[T],
      addComparisonData: Boolean = false): TableColumns[T] = {
    val newColumns = new TableColumns[T]()

    if (prefixColumn != null)
      newColumns += new TableViewColumn(prefixColumn)

    val columnsWithoutDelta = removeDeltaColumns(columns)
    for (c <- columnsWithoutDelta) {
      newColumns += c

      val newPrototypeCompareDataColumn = c.prototype.comparisonDataColumn(c.prototype.getHeaderColor)
      addComparisonDataColIfNeccesary(newColumns, addComparisonData, c, newPrototypeCompareDataColumn)

      val newPrototypeDeltaColumn = c.prototype.compareColumn
      if (newPrototypeDeltaColumn ne null) {
        newColumns += new TableViewColumn[T](newPrototypeDeltaColumn, c.width, null)
      }
    }
    newColumns
  }

  def removeDeltaColumns[T](columns: TableColumns[T]): TableColumns[T] = {
    columns.filterNot(_.prototype.name.startsWith(deltaSymbol))
  }

  def removeComparisonDataColumns[T](columns: TableColumns[T]): TableColumns[T] = {
    val colsToKeep = columns.filterNot(_.prototype.name.endsWith(getGroupSuffix(2)))

    // remove group name from original columns (no group 2 showing now)
    colsToKeep
      .filterNot(_.prototype.name.startsWith(deltaSymbol))
      .foreach(removeGroup1SuffixIfExists)
    colsToKeep
  }

  private def removeGroup1SuffixIfExists[T](c: TableViewColumn[T]): Unit = {
    val colName = c.prototype.name
    val suffixLength = getGroupSuffix(1).length
    if (colName.endsWith(getGroupSuffix(1)))
      c.prototype.name = colName.slice(0, colName.length - suffixLength)
  }

  def from[T](columns: ArrayBuffer[TableColumn[T]]): ArrayBuffer[TableViewColumn[T]] = {
    columns.iterator.map(c => new TableViewColumn[T](c)).to(ArrayBuffer)
  }
}
