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

import java.awt.Color
import java.util.regex.Pattern
import java.util.regex.Pattern._
import javax.swing.table.TableCellRenderer
import optimus.graph.NodeTaskInfo
import optimus.graph.diagnostics.PNodeTask
import optimus.graph.diagnostics.PNodeTaskInfo
import optimus.platform.util.Log
import optimus.profiler.ui.HotspotsTable.nodeInfoSectionColor
import optimus.profiler.ui.NPTableRenderer.TimeSubRange
import optimus.profiler.ui.NPTableRenderer.emptyTimeSubRange

object TableColumn extends Log {
  val flagsTableColumn = new TableColumnFlags("Flags", 40)

  type SummaryType = SummaryType.Value
  object SummaryType extends Enumeration {
    val None, Total, Count = Value
  }

  def parseStringFilterEx(filter: String): String => Boolean =
    try {
      if ((filter eq null) || filter.isEmpty) null
      else {
        val filterArr = filter.split(";")
        val filterRegExp = filterArr map { flt =>
          val negate = flt.startsWith("!")
          val pattern2 = flt.substring(if (negate) 1 else 0)
          val pattern3 = if (pattern2.contains("*")) pattern2 else "*" + pattern2 + "*"
          val usePattern = pattern3.replace(".", "\\.").replace("*", ".*")
          (negate, Pattern.compile(usePattern, CASE_INSENSITIVE | DOTALL))
        }
        v: String =>
          (v ne null) && filterRegExp.exists { case (negate, regexp) =>
            val r = regexp.matcher(v).matches
            if (negate) !r else r
          }
      }
    } catch {
      case _: Exception =>
        log.error(s"Failed to parse regex string: $filter")
        _ => true
    }
}

object TableColumnCategories {
  val Other: Category = Category("Other", sortingID = Integer.MAX_VALUE) // always goes last
}

final case class Category(name: String, sortingID: Int = 0)

abstract class TableColumn[RType](var name: String, val width: Int = 0) {
  private var _category: Category = TableColumnCategories.Other
  final def category: Category = _category
  final def setCategory(c: Category): Unit = _category = c
  def valueOf(row: RType): Any
  def valueOf(row: RType, row2: RType): Any = valueOf(row)
  def getCellRenderer: TableCellRenderer = null
  def getCellCustomColor(selected: Boolean, row: RType, row2: RType): Color = null
  def getHeaderColor: Color = null
  def parsedFilter(filter: String): (RType, RType) => Boolean = null
  def toolTip: String = null
  def compareColumn: TableColumn[RType] = null
  def sort(data: Iterable[RType], order: Boolean): Iterable[RType] = data
  // if you want to show the data alongside the deltas
  def comparisonDataColumn(color: Color): TableColumn[RType] = null
  def sortByThisColumn: Boolean = false
  // most columns can be deselected, Name column in NodeTreeView can't (because it breaks grouping icon)
  def allowDeselecting: Boolean = true

  def summaryType: TableColumn.SummaryType = TableColumn.SummaryType.Total
  def displayRowCountsInSummary: Boolean = false
  def computeSummary(table: NPTable[RType], indexes: Seq[Int]): AnyRef = {
    val rows = table.getTableRows
    val r = valueOf(table.getTableRows(0)) match {
      case _: Int    => indexes.foldLeft(0)((sum, i) => valueOf(rows(i)).asInstanceOf[Int] + sum)
      case _: Long   => indexes.foldLeft(0L)((sum, i) => valueOf(rows(i)).asInstanceOf[Long] + sum)
      case _: Double => indexes.foldLeft(0d)((sum, i) => valueOf(rows(i)).asInstanceOf[Double] + sum)
      case _         => ""
    }
    r.asInstanceOf[AnyRef]
  }
}

/*enables sorting when valueOf returns sortable types
 * VType is the try returned by valueOf()*/
abstract class SortableTableColumn[RType, VType](name: String, width: Int = 0, includeInComparison: Boolean = true)(
    implicit ev: VType => Ordered[VType])
    extends TableColumn[RType](name, width) {
  override def sort(data: Iterable[RType], sortByIncreasingOrder: Boolean): Iterable[RType] = {
    val seq = data match {
      case s: collection.Seq[RType] => s
      case i                        => i.toSeq
    }
    val sorted = seq.sortBy(valueOf(_))
    if (!sortByIncreasingOrder)
      sorted.reverse
    else sorted
  }

  override def valueOf(row: RType): VType
}

abstract class TableColumnString[RType](name: String, width: Int = 0, includeInComparison: Boolean = true)
    extends TableColumn[RType](name, width) {
  override def valueOf(row: RType): String = null
  override def valueOf(row: RType, row2: RType): String = valueOf(row)

  // fix the distortion of the formatting of JVM stack in the view of lost concurrency tab
  def convertToHTML(content: String): String = {
    val suffix = "<html>" + content.replace("\n", "<br>")
    if (content.split("\n").length % 2 == 1)
      suffix + "</html>"
    else
      suffix + "<br></html>"
  }

  override def parsedFilter(filter: String): (RType, RType) => Boolean = {
    val flt = TableColumn.parseStringFilterEx(filter)
    if (flt eq null) null
    else (row1, row2) => flt(valueOf(row1, row2))
  }

  def compareDataCol(parentColor: Color): TableColumnString[RType] =
    new TableColumnString[RType](s"${name}Group 2", width) {
      override def getCellRenderer: TableCellRenderer = NPTableRenderer.simpleRender
      override def valueOf(row: RType, row2: RType): String = TableColumnString.this.valueOf(row2)
      override def getHeaderColor: Color = parentColor
    }

  // includeInComparison is used because some string columns do not have comparison data between groups (like a name col)
  // but sometimes there will be like in the case where a boolean datatype uses a string col since there is not a boolean col
  override def comparisonDataColumn(parentColor: Color): TableColumn[RType] =
    if (includeInComparison) compareDataCol(parentColor) else null
}

object TableColumnFlags {
  final case class FlagBits(str: String, mask: Long, bits: Long, negBits: Long = 0) {
    def this(str: String, mask: Long) = this(str, mask, mask, 0)
  }
  private val flags = Vector(
    FlagBits("$", NodeTaskInfo.DONT_CACHE, 0, NodeTaskInfo.DONT_CACHE),
    new FlagBits("x", NodeTaskInfo.FAVOR_REUSE),
    new FlagBits("=", NodeTaskInfo.TWEAKABLE),
    new FlagBits("i", NodeTaskInfo.WAS_INSTANCE_TWEAKED),
    new FlagBits("p", NodeTaskInfo.WAS_PROPERTY_TWEAKED),
    new FlagBits("SI", NodeTaskInfo.SCENARIOINDEPENDENT),
    new FlagBits("a", NodeTaskInfo.ASYNC_NEEDED),
    new FlagBits("g", NodeTaskInfo.PROFILER_INTERNAL),
    new FlagBits("u", NodeTaskInfo.ATTRIBUTABLE_USER_CODE),
    new FlagBits("~", NodeTaskInfo.PROFILER_PROXY),
    new FlagBits("@", NodeTaskInfo.EXTERNALLY_CONFIGURED_CUSTOM_CACHE),
    new FlagBits("@", NodeTaskInfo.EXTERNALLY_CONFIGURED_POLICY),
    new FlagBits("UI", NodeTaskInfo.PROFILER_UI_CONFIGURED),
    new FlagBits("+", NodeTaskInfo.SHOULD_LOOKUP_PLUGIN),
    new FlagBits("R", NodeTaskInfo.GIVEN_RUNTIME_ENV)
  )
}

class TableColumnFlags(name: String, width: Int = 0) extends TableColumn[PNodeTaskInfo](name, width) {
  override def valueOf(row: PNodeTaskInfo): String = row.flagsAsString
  override def valueOf(row: PNodeTaskInfo, row2: PNodeTaskInfo): String = row.flagsAsString
  override def getCellRenderer: TableCellRenderer = NPTableRenderer.flagsRenderer
  override def getHeaderColor: Color = nodeInfoSectionColor
  override def parsedFilter(filter: String): (PNodeTaskInfo, PNodeTaskInfo) => Boolean =
    try {
      if ((filter eq null) || filter.isEmpty) null
      else {
        val filterArrStr = filter.split(";")
        val filters = filterArrStr.map(f => {
          var mask = 0L
          var bits = 0L
          var lastIndex = 0
          while (lastIndex < f.length) {
            val not = if (f.charAt(lastIndex) == '!') { lastIndex += 1; true }
            else false
            val flagInfo = TableColumnFlags.flags.find(fi => f.startsWith(fi.str, lastIndex))
            flagInfo.foreach { flag =>
              mask |= flag.mask
              bits |= (if (not) flag.negBits else flag.bits)
              lastIndex += flag.str.length
            }
            if (flagInfo.isEmpty) lastIndex += 1
          }
          (mask, bits)
        })

        (row1, row2) => {
          val v = row1.flags
          var i = 0
          var r = false
          while (i < filters.length && !r) {
            val (mask, bits) = filters(i)
            r = (mask & v) == bits
            i += 1
          }
          r
        }
      }
    } catch {
      case _: Throwable => null
    }
  override def compareColumn: TableColumn[PNodeTaskInfo] =
    new TableColumnFlags(TableView.deltaSymbol + name, width) {
      override def valueOf(row: PNodeTaskInfo, row2: PNodeTaskInfo): String = {
        val org = row.dup()
        org.flags = (org.flags ^ row2.flags) ^ NodeTaskInfo.DONT_CACHE
        org.flagsAsString
      }
    }
}

abstract class TableColumnUTC[RType](nano2utc: Long => String, name: String, width: Int = 0)
    extends SortableTableColumn[RType, String](name, width) {
  def nanos(row: RType): Long
  final override def valueOf(row: RType): String = nano2utc(nanos(row))
}

//Only for NodeTreeView/NodeTreeViewGroup
abstract class AggregatableTableColumnTime[RType <: NodeTreeView](name: String, width: Int = 0)
    extends TableColumnTime[RType](name, width) {
  override def valueOf(row: RType): Double =
    row match {
      case nodeTreeViewGroup: NodeTreeViewGroup => getAggregatedValue(nodeTreeViewGroup)
      case nodeTreeView: NodeTreeView           => getValueFromTask(nodeTreeView.task)
    }
  private def getAggregatedValue(nodeTreeViewGroup: NodeTreeViewGroup): Double =
    nodeTreeViewGroup.group.getAllNodeTasksOfChildren.map(getValueFromTask).sum
  def getValueFromTask(task: PNodeTask): Double
}

abstract class TableColumnTime[RType](name: String, width: Int = 0)
    extends SortableTableColumn[RType, Double](name, width) {
  override def valueOf(row: RType): Double = 0
  override def valueOf(row: RType, row2: RType): Double = valueOf(row)
  override def getCellRenderer: TableCellRenderer = NPTableRenderer.timeRenderer
  override def parsedFilter(filter: String): (RType, RType) => Boolean = {
    try {
      if ((filter eq null) || filter.isEmpty) null
      else {
        val filterArrStr = filter.split(";")
        val filters = filterArrStr.map(f => {
          val str = f.trim()
          if (str.startsWith(">=")) {
            val dbl = str.substring(2).toDouble
            (_: Double) >= dbl
          } else if (str.startsWith(">")) {
            val dbl = str.substring(1).toDouble
            (_: Double) > dbl
          } else if (str.startsWith("<=")) {
            val dbl = str.substring(2).toDouble
            (_: Double) <= dbl
          } else if (str.startsWith("<")) {
            val dbl = str.substring(1).toDouble
            (_: Double) < dbl
          } else if (str.startsWith("!")) {
            val dbl = str.substring(1).toDouble
            (_: Double) != dbl
          } else if (str.contains("..")) {
            val ind = str.indexOf("..")
            val r1 = str.substring(0, ind).toDouble
            val r2 = str.substring(ind + 2).toDouble
            d: Double => d >= r1 && d <= r2
          } else {
            // making sure the value being compared is in the same format as the value displayed on the ui
            (x: Double) => str == NPTableRenderer.timeFormat.format(x)
          }
        })

        (row1, row2) => {
          val v = valueOf(row1, row2)
          var i = 0
          var r = false
          while (i < filters.length && !r) {
            r = filters(i)(v)
            i += 1
          }
          r
        }
      }
    } catch {
      case _: Throwable => null
    }
  }
  override def compareColumn: TableColumn[RType] = new TableColumnTime[RType](TableView.deltaSymbol + name, width) {
    override def getCellRenderer: TableCellRenderer = NPTableRenderer.timeDiffRender
    override def valueOf(row: RType, row2: RType): Double =
      TableColumnTime.this.valueOf(row) - TableColumnTime.this.valueOf(row2)
  }
}

abstract class TableColumnCount[RType](name: String, width: Int = 0)
    extends SortableTableColumn[RType, Int](name, width) {
  override def valueOf(row: RType): Int = 0
  override def valueOf(row: RType, row2: RType): Int = valueOf(row)
  override def getCellRenderer: TableCellRenderer = NPTableRenderer.countRender
  override def parsedFilter(filter: String): (RType, RType) => Boolean = {
    try {
      if ((filter eq null) || filter.isEmpty) null
      else {

        val filterArrStr = filter.split(";")
        val filters = filterArrStr.map(f => {
          val str = f.trim()
          if (str.startsWith(">=")) {
            val dbl = str.substring(2).toInt
            (_: Int) >= dbl
          } else if (str.startsWith(">")) {
            val dbl = str.substring(1).toInt
            (_: Int) > dbl
          } else if (str.startsWith("<=")) {
            val dbl = str.substring(2).toInt
            (_: Int) <= dbl
          } else if (str.startsWith("<")) {
            val dbl = str.substring(1).toInt
            (_: Int) < dbl
          } else if (str.startsWith("!")) {
            val dbl = str.substring(1).toInt
            (_: Int) != dbl
          } else if (str.contains("..")) {
            val ind = str.indexOf("..")
            val r1 = str.substring(0, ind).toInt
            val r2 = str.substring(ind + 2).toInt
            d: Int => d >= r1 && d <= r2
          } else {
            val dbl = str.toInt
            (_: Int) == dbl
          }
        })

        (row1, row2) => {
          val v = valueOf(row1, row2)
          var i = 0
          var r = false
          while (i < filters.length && !r) {
            r = filters(i)(v)
            i += 1
          }
          r
        }
      }
    } catch {
      case _: Exception => null
    }
  }

  /** Creates a new column based on existing with 'delta' symbol prepended to name */
  override def compareColumn: TableColumn[RType] = new TableColumnCount[RType](TableView.deltaSymbol + name, width) {
    override def getCellRenderer: TableCellRenderer = NPTableRenderer.countDiffRender
    override def valueOf(row: RType, row2: RType): Int =
      TableColumnCount.this.valueOf(row) - TableColumnCount.this.valueOf(row2)
  }
}

abstract class TableColumnLong[RType](name: String, width: Int = 0)
    extends SortableTableColumn[RType, Long](name, width) {
  override def getCellRenderer: TableCellRenderer = NPTableRenderer.sizeRenderer
  override def valueOf(row: RType): Long = 0
  override def valueOf(row: RType, row2: RType): Long = valueOf(row)
  override def parsedFilter(filter: String): (RType, RType) => Boolean = {
    try {
      if ((filter eq null) || filter.isEmpty) null
      else {

        val filterArrStr = filter.split(";")
        val filters = filterArrStr.map(f => {
          val str = f.trim()
          if (str.startsWith(">=")) {
            val dbl = str.substring(2).toLong
            d: Long => d >= dbl
          } else if (str.startsWith(">")) {
            val dbl = str.substring(1).toLong
            d: Long => d > dbl
          } else if (str.startsWith("<=")) {
            val dbl = str.substring(2).toLong
            d: Long => d <= dbl
          } else if (str.startsWith("<")) {
            val dbl = str.substring(1).toLong
            d: Long => d < dbl
          } else if (str.startsWith("!")) {
            val dbl = str.substring(1).toLong
            (_: Long) != dbl
          } else if (str.contains("..")) {
            val ind = str.indexOf("..")
            val r1 = str.substring(0, ind).toLong
            val r2 = str.substring(ind + 2).toLong
            d: Long => d >= r1 && d <= r2
          } else {
            val dbl = str.toLong
            (_: Long) == dbl
          }
        })

        (row1, row2) => {
          val v = valueOf(row1, row2)
          var i = 0
          var r = false
          while (i < filters.length && !r) {
            r = filters(i)(v)
            i += 1
          }
          r
        }
      }
    } catch {
      case _: Throwable => null
    }
  }
  override def compareColumn: TableColumn[RType] = new TableColumnLong[RType](TableView.deltaSymbol + name, width) {
    override def getCellRenderer: TableCellRenderer = NPTableRenderer.sizeRenderer
    override def valueOf(row: RType, row2: RType): Long =
      TableColumnLong.this.valueOf(row) - TableColumnLong.this.valueOf(row2)
  }

  override def comparisonDataColumn(parentColor: Color): TableColumn[RType] =
    new TableColumnLong[RType](s"${name}Group 2", width) {
      override def getCellRenderer: TableCellRenderer = NPTableRenderer.sizeRenderer
      override def valueOf(row: RType, row2: RType): Long = TableColumnLong.this.valueOf(row2)
      override def getHeaderColor: Color = parentColor
    }
}

abstract class TableColumnTimeRange[RType](name: String, width: Int = 0) extends TableColumn[RType](name, width) {
  override def getCellRenderer: TableCellRenderer = NPTableRenderer.timeRangeTableCellRender
  override def summaryType: TableColumn.SummaryType = TableColumn.SummaryType.None
  override def valueOf(row: RType): TimeSubRange = emptyTimeSubRange
  override def valueOf(row: RType, row2: RType): TimeSubRange = valueOf(row)
}
