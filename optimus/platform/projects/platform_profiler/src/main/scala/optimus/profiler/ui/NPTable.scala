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

import java.awt.BasicStroke
import java.awt.BorderLayout
import java.awt.Color
import java.awt.Component
import java.awt.Point
import java.awt.event.MouseAdapter
import java.awt.event.MouseEvent
import java.util
import java.util.prefs.Preferences
import javax.swing.BorderFactory
import javax.swing.JPanel
import javax.swing.JScrollPane
import javax.swing.JTable
import javax.swing.SwingConstants
import javax.swing.ToolTipManager
import javax.swing.table.DefaultTableCellRenderer
import javax.swing.{ScrollPaneConstants => SPC}
import optimus.graph.GraphException
import optimus.graph.diagnostics.DbgPreference
import optimus.graph.diagnostics.PNodeTaskInfo
import optimus.graph.diagnostics.pgo.AutoPGOThresholds
import optimus.graph.diagnostics.pgo.DisableCache
import optimus.platform.util.Log
import optimus.profiler.ProfilerUI
import optimus.profiler.ui.common.DescendingOrderSorter
import optimus.profiler.ui.common.JCheckBoxMenuItem2
import optimus.profiler.ui.common.JMenu2
import optimus.profiler.ui.common.JMenuOps
import optimus.profiler.ui.common.JPopupMenu2

import java.awt.event.KeyAdapter
import java.awt.event.KeyEvent
import javax.swing.table.TableRowSorter
import scala.collection.immutable.ArraySeq
import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex

trait Filterable[T] { self: NPTable[T] =>
  def initialColumns: ArrayBuffer[TableColumn[T]]
  def initialViewColumns: ArrayBuffer[TableColumn[T]] = initialColumns
  setView(initialColumns, initialViewColumns)
  setFilterRow(true)
}

abstract class NPTable[RType] extends JPanel with Log {
  import NPTablePreferences._

  var onRowDoubleClicked: RType => Unit = _

  val pref: Preferences = Preferences.userNodeForPackage(this.getClass).node(this.getClass.getSimpleName.toLowerCase())

  private[ui] var emptyRow: RType = _
  private[ui] var prototypesColumns = ArrayBuffer[TableColumn[RType]]()
  private[ui] var viewColumns = ArrayBuffer[TableViewColumn[RType]]()

  protected var orgRowsC = new ArrayBuffer[RType]()
  protected var orgRows = new ArrayBuffer[RType]()

  protected var rows = new ArrayBuffer[RType]()
  protected var rowsC = new ArrayBuffer[RType]() // Compare 2 set

  protected var prefilter_rows = new ArrayBuffer[RType]()
  protected var prefilter_rowsC = new ArrayBuffer[RType]()

  private var isDisablingEnabled: Boolean = _
  def setDisablingEnabled(isEnabled: Boolean): Unit = isDisablingEnabled = isEnabled
  def getDisablingEnabled: Boolean = isDisablingEnabled

  private var cacheDecisionMaker = AutoPGOThresholds()
  def getCacheDecisionMaker: AutoPGOThresholds = cacheDecisionMaker
  def setCacheDecisionMaker(_cacheDecisionMaker: AutoPGOThresholds): Unit = cacheDecisionMaker = _cacheDecisionMaker

  def getDisabledNodes(cacheDecisionMaker: AutoPGOThresholds): ArrayBuffer[PNodeTaskInfo] = {
    NodeProfiler.collectProfile.filter(DisableCache.configure(_, cacheDecisionMaker))
  }

  def isDisabledRow(row: Int): Boolean = {
    val pn = getTableRows(row).asInstanceOf[PNodeTaskInfo]
    DisableCache.configure(pn, cacheDecisionMaker)
  }

  // overriders that don't want the summary table at the bottom
  // can't just sumTable.setVisible(false) because of the dataVP <-> summaryVP changeListeners (OPTIMUS-25710)
  protected def wantSummary = true

  private var rowFilter: Array[(RType, RType) => Boolean] = _
  private var multiRowFilter: Array[MultiColFilter[RType]] = _

  val dataTable = new NPSubTableData[RType](this)
  val headerTable = new NPSubTableHeader(this)
  val sumTable = new NPSubTableSummary(this)

  private[ui] def currentViewHasFilter: Boolean = rowFilter ne null

  // overriders that don't want sorting by default set this to false (eg NPTreeTables)
  protected def sortable = true

  def setFilterRow(enable: Boolean): Unit = {
    headerTable.setFilterRow(enable)
    sumTable.setPreferredScrollableViewportSize(sumTable.getPreferredSize)
  }

  private def init(): Unit = {
    // give people an opportunity to ponder longer tooltips
    ToolTipManager.sharedInstance.setDismissDelay(60000)

    if (sortable) {
      dataTable.setRowSorter(new TableRowSorter(dataTable.model))
      headerTable.setRowSorter(new DescendingOrderSorter(headerTable.model, dataTable.getRowSorter))
    }

    val scrollData = new JScrollPane(dataTable)
    val dataVP = scrollData.getViewport

    dataVP.setBackground(dataTable.getBackground)

    setLayout(new BorderLayout(0, 0))
    scrollData.setBorder(BorderFactory.createEmptyBorder())

    add(scrollData, BorderLayout.CENTER)

    def addScrollContainerForSubTable(table: NPSubTable[_], layout: AnyRef): Unit = {
      val scrollPane = new JScrollPane(table, SPC.VERTICAL_SCROLLBAR_NEVER, SPC.HORIZONTAL_SCROLLBAR_NEVER)
      table.setPreferredScrollableViewportSize(table.getPreferredSize)

      val summaryVP = scrollPane.getViewport
      dataVP.addChangeListener(_ => { summaryVP.setViewPosition(new Point(dataVP.getViewPosition.x, 0)) })
      summaryVP.addChangeListener(_ => {
        val newX = summaryVP.getViewPosition.x
        val newY = dataVP.getViewPosition.y
        dataVP.setViewPosition(new Point(newX, newY))
      })
      scrollPane.setBackground(table.getBackground)
      scrollPane.setBorder(BorderFactory.createEmptyBorder())
      add(scrollPane, layout)
    }
    addScrollContainerForSubTable(headerTable, BorderLayout.NORTH)
    if (wantSummary)
      addScrollContainerForSubTable(sumTable, BorderLayout.SOUTH)

    headerTable.getTableHeader.addMouseListener(new MouseAdapter {
      override def mouseReleased(e: MouseEvent): Unit = {
        if (e.getButton == MouseEvent.BUTTON3) {
          createHeaderMenu(e).show(NPTable.this, e.getX, e.getY)
          e.consume()
        }
      }

      // remember this column as the default one to sort by next time
      override def mouseClicked(e: MouseEvent): Unit = {
        val col = headerTable.columnAtPoint(e.getPoint)
        updatePreferredSortByColumn(col)
      }
    })

    dataTable.addMouseListener(new MouseAdapter {
      override def mousePressed(e: MouseEvent): Unit = onClick(e)
    })

    dataTable.addKeyListener(new KeyAdapter {
      override def keyReleased(e: KeyEvent): Unit = onKeyReleased(e)
    })
    dataTable.setShowVerticalLines(showVerticalLines.isSelected)
  }

  /** Override to let UI know how to set table data when table first becomes visible */
  def updateData(): Unit = {}

  /** Override to provide custom callback when a row is selected */
  def onClick(e: MouseEvent): Unit = {}

  /** Override to provide custom callback when a a key is released, e.g., when switching rows */
  def onKeyReleased(e: KeyEvent): Unit = {}

  /** Override to provide custom color per entire row (cell can override) */
  def getRowBackground(col: TableColumn[RType], row: RType): Color = null

  private def cmdFlipColumn(index: Int, visibleNow: Boolean, col: TableColumn[RType]): Unit = {
    val newView = viewColumns.sortBy(v => v.index)
    if (visibleNow)
      newView.remove(newView.indexWhere(c => c.prototype.name == col.name))
    else
      newView.insert(index, new TableViewColumn(col))
    setView(newView)
  }

  private def cmdSizeAllColumns(): Unit = {
    headerTable.columnModelListener.syncMargins = false
    var columnIndex = 0
    val columns = dataTable.getColumnCount
    while (columnIndex < columns) {
      cmdSizeColumn(columnIndex)
      columnIndex += 1
    }
    headerTable.columnModelListener.syncMargins = true
    headerTable.columnModelListener.columnMarginChanged(null)
  }

  private def cmdAutoSizeColumns(enabled: Boolean): Unit = {
    val mode = if (enabled) JTable.AUTO_RESIZE_ALL_COLUMNS else JTable.AUTO_RESIZE_OFF
    headerTable.setAutoResizeMode(mode)
  }

  private def cmdSizeColumn(columnIndex: Int): Unit = {
    val rowCount = dataTable.getRowCount
    var maxWidth = 10 // Don't size to less than 10 px
    var i = 0
    while (i < rowCount) {
      val renderer = dataTable.getCellRenderer(i, columnIndex)
      val component = dataTable.prepareRenderer(renderer, i, columnIndex)
      maxWidth = Math.max(component.getPreferredSize.width + 1, maxWidth)
      i += 1
    }
    val cmCol = headerTable.getColumnModel.getColumn(columnIndex)
    cmCol.setPreferredWidth(maxWidth)
    cmCol.setWidth(maxWidth)
  }

  // Important to actually auto-update the sizing behaviour
  private val autoSizeToolTip = "During all resize operations, proportionately resize all columns"
  private val autoSizeTitle = "Keep Resizing Columns Proportionally"
  private val showVerticalLinesText = "Show Vertical Grid Lines"
  private val showVerticalLinesToolTip = "Show/hide vertical grid lines between columns"
  private val autoSizeMenuItem =
    new JCheckBoxMenuItem2(autoSizeTitle, autoSizeToolTip, DbgPreference(autoSizeTitle, pref), cmdAutoSizeColumns)
  private val showVerticalLines =
    new JCheckBoxMenuItem2(
      showVerticalLinesText,
      showVerticalLinesToolTip,
      DbgPreference(showVerticalLinesText, pref),
      cmdToggleVerticalLines)

  private def createHeaderMenu(e: MouseEvent): JPopupMenu2 = {
    val menu = new JPopupMenu2
    val viewColumnMap = viewColumns.map(c => (c.prototype.name, c)).toMap
    val columnModel = dataTable.getColumnModel
    val index = columnModel.getColumnIndexAtX(e.getPoint.x)
    if (index >= 0) {
      menu.addMenu("Auto Size", "Size selected column to fit its content") { cmdSizeColumn(index) }
      menu.addMenu("Auto Size All Columns", "Size selected column to fit its content") { cmdSizeAllColumns() }
      menu.add(autoSizeMenuItem)
      menu.add(showVerticalLines)
      menu.addSeparator()
      val colsByCategory = prototypesColumns.groupBy(_.category)
      val noCategories = colsByCategory.keySet.size == 1 && (colsByCategory.keySet.head eq TableColumnCategories.Other)
      if (noCategories) {
        for (col <- prototypesColumns if col.allowDeselecting)
          add(menu, viewColumnMap, col, index)
      } else {
        val sortedCategories = colsByCategory.toList.sortBy(_._1.sortingID)
        for ((category, cols) <- sortedCategories) {
          val categoryMenu = new JMenu2(category.name)
          menu.add(categoryMenu)
          for (col <- cols.sortBy(_.name) if col.allowDeselecting)
            add(categoryMenu, viewColumnMap, col, index)
        }
      }
    }
    menu
  }

  init()

  private def cmdToggleVerticalLines(verticalLines: Boolean): Unit = {
    dataTable.setShowVerticalLines(verticalLines)
    refresh()
  }

  type ViewColumnMap = Map[String, TableViewColumn[RType]]

  private def add(menu: JMenuOps, viewColumnMap: ViewColumnMap, col: TableColumn[RType], index: Int): Unit = {
    val visibleNow = viewColumnMap.contains(col.name)
    menu.addCheckBoxMenu(col.name, col.toolTip, visibleNow) { cmdFlipColumn(index, visibleNow, col) }
  }

  private[ui] def viewChanged(): Unit = pref.put(viewPreferencesKey, buildViewPreferencesString(viewColumns))

  private def loadView(): ArrayBuffer[TableViewColumn[RType]] = {
    val loadedView = if (ProfilerUI.prefDisabledForTesting) null else pref.get(viewPreferencesKey, null)
    parsePreferences(loadedView, prototypesColumns)
  }

  private[ui] final def getTableColumns = viewColumns
  private[ui] final def getTableRows = rows
  private[ui] final def getTableRowsC = rowsC

  final def refresh(resetTotals: Boolean = true): Unit = {
    if (resetTotals) viewColumns foreach { _.total = null }
    dataTable.model.fireTableDataChanged()
    sumTable.model.fireTableDataChanged()
    headerTable.model.fireTableDataChanged()
  }

  private[ui] def updateFilterAndRefresh(): Unit = {
    updateFilter()
    rows = prefilter_rows
    rowsC = prefilter_rowsC
    filterOutRows()
    refresh()
    if (rows.isEmpty && currentViewHasFilter)
      showMessage("No nodes found! Try removing your filter")
  }

  def filterHelperForTesting(
      viewColumns: ArrayBuffer[TableViewColumn[RType]],
      rows: ArrayBuffer[RType],
      rowsC: ArrayBuffer[RType]): ArrayBuffer[RType] = {
    this.viewColumns = viewColumns
    this.rows = rows
    this.prefilter_rows = rows
    this.rowsC = rowsC
    updateFilter()
    filterOutRows()
    this.rows
  }

  private def multiColumnFilter(filterStr: String): MultiColFilter[RType] = {
    if (filterStr != null && filterStr.nonEmpty) {
      if (filterStr.startsWith("#")) {
        val symbol = List("<", ">", "=", "<=", ">=")
        if (symbol.exists(filterStr.contains(_))) {
          filterStr match {
            case NPTablePreferences.columnComparisonRegex(col1, col2, symbol, value) =>
              return MultiColFilter(col1, col2, symbol, value)
            case _ => None
          }
        }
      }
    }
    null
  }

  private def updateFilter(): Unit = {
    val r = new Array[(RType, RType) => Boolean](viewColumns.length)
    val m = new Array[MultiColFilter[RType]](viewColumns.length)
    var i = 0
    var j = 0
    var k = 0
    while (i < viewColumns.length) {
      val viewColumn = viewColumns(i)
      val pf: (RType, RType) => Boolean = viewColumn.prototype.parsedFilter(viewColumn.filter)
      val mf = multiColumnFilter(viewColumn.filter)
      if (pf ne null) {
        r(j) = pf
        j += 1
      }
      if (mf ne null) {
        m(k) = mf
        k += 1
      }
      i += 1
    }
    rowFilter = if (j == 0) null else util.Arrays.copyOf(r, j)
    multiRowFilter = if (k == 0) null else util.Arrays.copyOf(m, k)
  }

  def getSelection: RType = {
    val rowIndex = dataTable.getSelectionModel.getMaxSelectionIndex
    if (rowIndex == -1) null.asInstanceOf[RType]
    else {
      val index = dataTable.convertRowIndexToModel(rowIndex)
      if (index >= 0 && index < rows.size) rows(index)
      else null.asInstanceOf[RType]
    }
  }

  def getSelections: ArraySeq[RType] = {
    val srows = dataTable.getSelectedRows
    var i = 0
    while (i < srows.length) {
      srows(i) = dataTable.convertRowIndexToModel(srows(i))
      i += 1
    }
    val r = ArraySeq.untagged.newBuilder[RType]
    r.sizeHint(srows.length)
    i = 0
    while (i < srows.length) {
      r += rows(srows(i))
      i += 1
    }
    r.result()
  }

  /** Update prototype and create a default view from prototypes */
  def setView(prototypes: ArrayBuffer[TableColumn[RType]]): Unit = setView(prototypes, prototypes)
  def setView(prototypes: ArrayBuffer[TableColumn[RType]], viewPrototypes: ArrayBuffer[TableColumn[RType]]): Unit = {
    var view = if (prototypesColumns.isEmpty) {
      prototypesColumns = prototypes
      loadView()
    } else null

    if (view == null || view.isEmpty) view = TableView.from(viewPrototypes)
    setView(view)
  }

  def addComparisonDataColumns(prefixColumn: TableColumn[RType] = null): Unit =
    setView(TableView.diffViewFrom(prefixColumn, viewColumns, addComparisonData = true))

  def removeComparisonDataColumnsFromView(): Unit = setView(TableView.removeComparisonDataColumns(viewColumns))

  /** Compare view is the view that has at least one 'delta' column */
  def generateAndSwitchToComparisonView(regenerate: Boolean, prefixColumn: TableColumn[RType] = null): Unit = {
    if (regenerate || !TableView.containsDiffColumn(viewColumns))
      setView(TableView.diffViewFrom(prefixColumn, viewColumns))
  }

  /** Compare view is the view that has at least one 'delta' column */
  def removeDeltaColumnFromView(): Unit = {
    setView(TableView.removeDeltaColumns(viewColumns))
  }

  // if one of the columns was defined as the one to sort by by default, put that in the preferences. Note that if
  // more than one has sortByThisColumn as true, we'll just take the first one here
  private def setPreferredSortByColumn(): Unit = {
    val existingPref = pref.getInt(sortedColumnPreferencesKey, -1)
    if (existingPref > -1 && !ProfilerUI.prefDisabledForTesting) toggleSortOrder(existingPref)
    else {
      viewColumns.zipWithIndex.find { case (v, _) => v.prototype.sortByThisColumn }.foreach { case (_, i) =>
        updatePreferredSortByColumn(i)
        toggleSortOrder(i)
      }
    }
  }

  private def sortIndexInRange(i: Int): Boolean = i < viewColumns.length && i >= 0

  private def toggleSortOrder(i: Int): Unit = {
    val sorter = headerTable.getRowSorter
    if (sorter != null && sortIndexInRange(i)) sorter.toggleSortOrder(i)
  }

  private def updatePreferredSortByColumn(i: Int): Unit = {
    if (!sortIndexInRange(i))
      log.warn(s"Cannot set preferred sorted column index to $i (there are ${viewColumns.length} columns)")
    else pref.putInt(sortedColumnPreferencesKey, i)
  }

  def setView(newColumns: ArrayBuffer[TableViewColumn[RType]], refresh: Boolean = true): Unit = {
    viewColumns = newColumns
    updateFilter()
    headerTable.columnModelListener.syncMargins = false
    dataTable.model.fireTableStructureChanged()
    sumTable.model.fireTableStructureChanged()
    headerTable.model.fireTableStructureChanged()

    val cm = headerTable.getColumnModel
    var col = 0
    while (col < viewColumns.length) {
      val viewColumn = viewColumns(col)
      if (viewColumn.width > 0) {
        val cmc = cm.getColumn(col)

        cmc.setPreferredWidth(viewColumn.width)
        cmc.setWidth(viewColumn.width)
        cmc.setHeaderRenderer(new NPTableHeaderRenderer(viewColumn.prototype.getHeaderColor))
      }
      col += 1
    }
    headerTable.columnModelListener.syncMargins = true
    headerTable.columnModelListener.columnMarginChanged(null)
    setPreferredSortByColumn()
  }

  private def filterOutRows(): Unit = {

    def isMatching(r1: RType, r2: RType, rowIndex: Int) = {
      var i = 0
      var r = true
      while (rowFilter != null && i < rowFilter.length && r) {
        r = rowFilter(i)(r1, r2)
        i += 1
      }
      while (multiRowFilter != null && i < multiRowFilter.length && r) {
        r = multiRowFilter(i).evaluate(r1, dataTable, rowIndex)
        i += 1
      }
      r
    }

    if (((rowFilter ne null) || (multiRowFilter ne null)) && (prefilter_rows ne null)) {
      val nr = new ArrayBuffer[RType]()
      val nrc = new ArrayBuffer[RType]()

      var i = 0
      while (i < prefilter_rows.size) {
        if (prefilter_rowsC.isEmpty && isMatching(rows(i), emptyRow, i))
          nr += rows(i)
        else if (prefilter_rowsC.nonEmpty && isMatching(rows(i), rowsC(i), i)) {
          nr += rows(i)
          nrc += rowsC(i)
        }
        i += 1
      }
      rowsC = nrc
      rows = nr
    }

  }

  def setMessage(msg: String): Unit = dataTable.setMessage(msg)
  def showMessage(msg: String): Unit = dataTable.showMessage(msg)

  def setRowsQuiet(_rows: ArrayBuffer[RType], _rowsC: ArrayBuffer[RType]): Unit = {
    orgRows = if (_rows ne null) _rows else new ArrayBuffer[RType]
    orgRowsC = if (_rowsC ne null) _rowsC else new ArrayBuffer[RType]
    rows = orgRows
    rowsC = orgRowsC
    afterUpdateList()
    prefilter_rows = rows
    prefilter_rowsC = rowsC
  }

  protected def afterUpdateList(): Unit = {}
  protected def diffAnnotationColumn: TableColumn[RType] = null

  def setRows(_rows: ArrayBuffer[RType], _rowsC: ArrayBuffer[RType], switchToCompare: Boolean = false): Unit = {
    setRowsQuiet(_rows, _rowsC)
    filterOutRows()

    if (switchToCompare) {
      val addDiffAnnotationColumn = rows.size != orgRows.size || rowsC.size != orgRowsC.size
      generateAndSwitchToComparisonView(regenerate = true, if (addDiffAnnotationColumn) diffAnnotationColumn else null)
    }

    refresh()
  }

  def setCompareToList(list: ArrayBuffer[RType]): Unit = setRows(orgRows, list, switchToCompare = true)
  def setList(list: ArrayBuffer[RType]): Unit = setRows(list, orgRowsC)
  def setList(): Unit = setRows(orgRows, orgRowsC)
}

trait TimelineEvent {
  def startTimeNanos: Long
  def durationNanos: Long = 0L
  def endTimeNanos: Long = startTimeNanos + durationNanos
}

abstract class NPTimelineTable[RType <: TimelineEvent](val tline: NodeTimeLine) extends NPTable[RType] {
  final override def onClick(event: MouseEvent): Unit = highlightEventRange()
  final override def onKeyReleased(e: KeyEvent): Unit = highlightEventRange()
  private def highlightEventRange(): Unit = {
    val selection = getSelection
    if (selection != null) {
      tline.setHighlightRange(selection.startTimeNanos, selection.durationNanos)
    }
  }
}

private[profiler] object NPTablePreferences extends Log {
  val columnComparisonRegex: Regex = "#([0-9]+)-#([0-9]+)(<|>|<=|>=|=|==)([0-9]+)".r
  val viewPreferencesKey = "view"
  val viewPreferencesVersion = "v1"
  val viewPreferencesNumFields = 3 // name, width, filter

  val sortedColumnPreferencesKey = "defaultSortedColumnIndex"

  def buildViewPreferencesString[T](viewColumns: ArrayBuffer[TableViewColumn[T]]): String = {
    val sb = new StringBuilder
    sb ++= s"$viewPreferencesVersion;"
    val viewColumnSorted = viewColumns.sortBy(v => v.index)
    for (col <- viewColumnSorted) {
      sb ++= col.prototype.name
      sb += ';'
      sb ++= col.width.toString
      sb += ';'
      sb ++= (if (col.filter eq null) "" else col.filter)
      sb += ';'
    }
    sb.toString()
  }

  def parsePreferences[T](
      viewString: String,
      prototypesColumns: ArrayBuffer[TableColumn[T]]): ArrayBuffer[TableViewColumn[T]] = {
    val view = new ArrayBuffer[TableViewColumn[T]]()
    val viewColumnMap = prototypesColumns.map(c => (c.name, c)).toMap
    try {
      val values = viewString.split(";", -1)
      if (values(0) == viewPreferencesVersion) {
        var i = 1
        // split will end with an extra empty string at the end of last ';'
        while (i < values.length - 1) {
          val name = values(i)
          val width = values(i + 1).toInt
          val filter = if (values(i + 2) == "") null else values(i + 2)

          val prototype = viewColumnMap.get(name)
          if (prototype.isDefined) {
            view += new TableViewColumn[T](prototype.get, width, filter)
          }
          i += viewPreferencesNumFields // 3 values per column
        }
      } else throw new GraphException(s"Expected version $viewPreferencesVersion, actually found ${values(0)}")
    } catch {
      case e: GraphException => throw e // want to actually see this one
      case e: Throwable      => log.debug(s"Parsing view preferences failed: $e") // ignore any other exceptions thrown
    }
    view
  }
}

private class NPTableHeaderRenderer(colour: Color) extends DefaultTableCellRenderer {
  override def getTableCellRendererComponent(
      table: JTable,
      value: Any,
      isSelected: Boolean,
      hasFocus: Boolean,
      row: Int,
      column: Int): Component = {
    val component = super.getTableCellRendererComponent(table, value, isSelected, hasFocus, row, column)
    component match {
      case renderer: NPTableHeaderRenderer =>
        val border = BorderFactory.createStrokeBorder(new BasicStroke(0.1f))
        renderer.setBorder(border)
      case _ =>
    }
    component
  }

  setBackground(colour)
  setFont(NPTableRenderer.headerFont)
  setHorizontalAlignment(SwingConstants.CENTER)
}

final case class MultiColFilter[RType](col1: String, col2: String, symbol: String, value: String) {
  def cast(value: AnyRef): Long = {
    value match {
      case x: Integer          => x.longValue()
      case x: java.lang.Long   => x
      case x: java.lang.Double => x.longValue()
      case x: java.lang.Float  => x.longValue()
      case x: java.lang.String => x.toInt
      case _                   => 0
    }
  }

  def evaluate(r1: RType, dataTable: NPSubTableData[RType], rowIndex: Int): Boolean = {
    val col1Value = cast(dataTable.getModel.getValueAt(rowIndex, col1.toInt - 1))
    val col2Value = cast(dataTable.getModel.getValueAt(rowIndex, col2.toInt - 1))
    if (symbol == "<") {
      col1Value - col2Value < value.toLong
    } else if (symbol == ">") {
      col1Value - col2Value > value.toLong
    } else if (symbol == "=") {
      col1Value - col2Value == value.toLong
    } else if (symbol == ">=") {
      col1Value - col2Value >= value.toLong
    } else if (symbol == "<=") {
      col1Value - col2Value <= value.toLong
    } else if (symbol == "==") {
      col1Value - col2Value == value.toLong
    } else
      false
  }
}
