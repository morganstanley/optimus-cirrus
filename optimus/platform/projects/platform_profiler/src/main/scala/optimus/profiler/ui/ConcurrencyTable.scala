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

import javax.swing.Icon
import javax.swing.ImageIcon
import javax.swing.table.TableCellRenderer
import optimus.graph.diagnostics.NPTreeNode
import optimus.graph.diagnostics.NodeName
import optimus.graph.diagnostics.PNodeTask
import optimus.profiler.recipes.concurrency.MinimumUnitCostReport
import optimus.profiler.recipes.concurrency.MinimumUnitCostResult
import optimus.profiler.recipes.concurrency.SyncBranch
import optimus.profiler.recipes.concurrency.SyncPointReport

import scala.collection.mutable.ArrayBuffer

object ConcurrencyTable {

  private val allColumns = ArrayBuffer[TableColumn[ConcurrencyViewRow]](
    new TableColumn[ConcurrencyViewRow]("Name", 600) {
      override def valueOf(row: ConcurrencyViewRow): ConcurrencyViewRow = row
      override def getCellRenderer: TableCellRenderer = NPTableRenderer.treeRenderer
    },
    new TableColumnCount[ConcurrencyViewRow]("Max Scaling", 300) {
      override def valueOf(row: ConcurrencyViewRow): Int = row.maxScaling
    },
    new TableColumnCount[ConcurrencyViewRow]("Minimum Cost", 300) {
      override def valueOf(row: ConcurrencyViewRow): Int = row.cost
    }
  )

  def minimumUnitCostResultToRows(mcr: MinimumUnitCostResult): Iterable[ConcurrencyViewRow] = {
    val r = ArrayBuffer.from[ConcurrencyViewRow](mcr.syncPoints.map(new SyncPointNodeView(_)))
    r ++= mcr.sourceEntries.map(new ChildEntryNodeView(_))
    r
  }
}

abstract class ConcurrencyViewRow(children: Iterable[ConcurrencyViewRow]) extends NPTreeNode with DbgPrintSource {
  override def hasChildren: Boolean = children.nonEmpty
  override def getChildren: Iterable[NPTreeNode] = children
  def maxScaling: Int = 0
  def cost: Int = 0
  def taskOfInterestMask: Long = 0L
  override def task: PNodeTask = null
  open = true
  override def printSource(): Unit = if (task != null) task.printSource()
}

class MinimumUnitCostResultsView(altTitle: String, mcrs: MinimumUnitCostReport)
    extends ConcurrencyViewRow(mcrs.graphEntries.map(new MinimumUnitCostResultView(_))) {
  override def level: Int = 0
  override def title: String = if (altTitle == null || altTitle.isEmpty) "Concurrency Report" else altTitle
  override def maxScaling: Int = mcrs.maxScaling
  override val cost: Int = if (mcrs.statsPerEntry) 0 else mcrs.totalCost
  override def icon: Icon = Icons.home
  override def printSource(): Unit = NodeName.printSource(mcrs.functionOfInterest)
}

class MinimumUnitCostResultView(mcr: MinimumUnitCostResult)
    extends ConcurrencyViewRow(ConcurrencyTable.minimumUnitCostResultToRows(mcr)) {
  override def level: Int = 1
  override def title: String = "Entry task: " + mcr.graphEntry.toPrettyName(false, true, true)
  override def maxScaling: Int = mcr.maxScaling
  override def cost: Int = mcr.cost
  override def taskOfInterestMask: Long = mcr.reachableCostlyFunctions
  override def task: PNodeTask = mcr.graphEntry
  override def tooltipText: String = ""
}

class SyncPointNodeView(spr: SyncPointReport) extends ConcurrencyViewRow(spr.toTasks.map(new SyncBranchNodeView(_))) {
  override def level: Int = 2
  override def title: String = spr.fromTask.toPrettyName(false, true, true)
  override def icon: Icon = Icons.syncPoint
  override def task: PNodeTask = spr.fromTask
  override def taskOfInterestMask: Long = spr.reachableCostlyFunctions
  override def tooltipText: String = "<html>Synchronisation point, click to expand synchronous branches" +
    "<br><i>This is a limiting factor for concurrency, try to break up synchronous branches</i></br></html>"
}

class SyncBranchNodeView(val syncBranch: SyncBranch) extends ConcurrencyViewRow(Nil) {
  override def level: Int = 3
  override def task: PNodeTask = syncBranch.task
  override def taskOfInterestMask: Long = syncBranch.reachableCostlyFunctions
  override def title: String = task.toPrettyName(false, true, true)
  override def icon: Icon = Icons.syncBranch
  override def tooltipText: String = "Synchronous branch"
}

class ChildEntryNodeView(val syncBranch: SyncBranch) extends ConcurrencyViewRow(Nil) {
  override def level: Int = 2
  override def task: PNodeTask = syncBranch.task
  override def taskOfInterestMask: Long = syncBranch.reachableCostlyFunctions
  override def title: String = task.toPrettyName(false, true)
  override def icon: Icon = Icons.entryPoint
  override def tooltipText: String =
    "Synchronisation point, try to reduce number of entry points scheduled one at a time"
}

final class ConcurrencyTable extends NPTreeTable[ConcurrencyViewRow] {
  outer =>
  emptyRow = new SyncBranchNodeView(new SyncBranch(PNodeTask.fake, 0))

  private var mcrs: MinimumUnitCostReport = _

  setView(ConcurrencyTable.allColumns)

  private val menu = NodeMenu.create(outer)

  dataTable.setComponentPopupMenu(menu)

  def setCostResults(title: String, report: MinimumUnitCostReport): Unit = {
    if (report eq null) {
      mcrs = null
      setList()
    } else {
      mcrs = report
      setList(List(new MinimumUnitCostResultsView(title, mcrs)))
    }
  }

  def costResults: MinimumUnitCostReport = mcrs
}
