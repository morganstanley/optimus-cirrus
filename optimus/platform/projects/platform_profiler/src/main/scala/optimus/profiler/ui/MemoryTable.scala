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

import javax.swing.event.{PopupMenuEvent, PopupMenuListener}
import javax.swing.JFrame

import scala.collection.mutable.ArrayBuffer
import optimus.graph.cache.Reachability
import optimus.graph.outOfProcess.views.MemoryView
import optimus.profiler.ui.common.JPopupMenu2
import scala.collection.compat._

object MemoryTable {

  val regularView: ArrayBuffer[TableColumn[MemoryView]] = ArrayBuffer(
    new TableColumnCount[MemoryView]("Count", 40) {
      override def valueOf(row: MemoryView): Int = row.count
    },
    new TableColumnLong[MemoryView]("Instance Size", 40) {
      override def valueOf(row: MemoryView): Long = row.size / row.count
    },
    new TableColumnLong[MemoryView]("Total Size", 40) {
      override def valueOf(row: MemoryView): Long = row.size
    },
    new TableColumnLong[MemoryView]("Retained Size", 40) {
      override def valueOf(row: MemoryView): Long = row.retainedSize
    },
    new TableColumnLong[MemoryView]("Shared Size", 40) {
      override def valueOf(row: MemoryView): Long = row.sharedRetainedSize
    },
    new TableColumnString[MemoryView]("Finalizer", 40) {
      override def valueOf(row: MemoryView): String = row.finalizer
    },
    new TableColumnString[MemoryView]("Name") {
      override def valueOf(row: MemoryView): String = row.name
    },
    new TableColumnString[MemoryView]("Reachability") {
      override def valueOf(row: MemoryView): String = row.reachability
    }
  )
}

// pop-up MemoryTable to show contributors or root nodes of a selected cache entry
// TODO (OPTIMUS-0000): make it a proper filtered aggregate view instead of using rowsC as the unfiltered rowset and rows as filtered
class SubMemoryTable(rows: ArrayBuffer[MemoryView], origRows: ArrayBuffer[MemoryView], name: String, dir: Boolean)
    extends JFrame {
  setupComponents()
  def setupComponents(): Unit = {
    val title = (if (dir) "Contributors of " else "Root nodes of ") + name
    setTitle(title)
    setBounds(200, 100, 800, 300)
    val tbl = new MemoryTable()
    tbl.setRows(rows, origRows)
    setContentPane(tbl)
  }
}

class MemoryTable extends NPTable[MemoryView] with Filterable[MemoryView] {
  emptyRow = MemoryView(0, 0, 0, 0, "", "", "", null, null, null, null)
  override def initialColumns: ArrayBuffer[TableColumn[MemoryView]] = MemoryTable.regularView

  dataTable.setComponentPopupMenu(initPopupMenu)

  def initPopupMenu = {
    val menu = new JPopupMenu2

    val miContr = menu.addMenu(
      "Show contributors",
      for (pnti <- getSelections) {
        val rnames = pnti.retainedClassNames
        val rsizes = pnti.retainedClassSizes // count,size, count,size, count,size, ...
        val subrows = for (n <- 0 until rnames.size) yield {
          val classname = rnames(n)
          val r = rowsC.find(_.name == classname).get
          r.count = rsizes(n * 2)
          r.size = rsizes(n * 2 + 1)
          r
        }
        new SubMemoryTable(subrows.to(ArrayBuffer), rowsC, pnti.name, true).setVisible(true)
      }
    )

    val miSContr = menu.addMenu(
      "Show shared contributors",
      for (pnti <- getSelections) {
        val snames = pnti.sharedRetainedClassNames
        val ssizes = pnti.sharedClassSizes // count,size, count,size, count,size, ...
        val subrows = for (n <- 0 until snames.size) yield {
          val classname = snames(n)
          val r = rowsC.find(_.name == classname).get
          r.count = ssizes(n * 2)
          r.size = ssizes(n * 2 + 1)
          r
        }
        new SubMemoryTable(subrows.to(ArrayBuffer), rowsC, pnti.name, true).setVisible(true)
      }
    )

    val miRoots = menu.addMenu(
      "Show root nodes",
      for (pnti <- getSelections) {
        val subrows = rowsC filter (_.retainedClassNames contains pnti.name)
        // TODO (OPTIMUS-0000): recalculate counts when building subrows in case not every instance of some cached node has this particular contributor
        //      subrows map (_.cls.getName) map println
        new SubMemoryTable(subrows, rowsC, pnti.name, false).setVisible(true)
      }
    )

    menu.addPopupMenuListener(new PopupMenuListener {
      override def popupMenuCanceled(e: PopupMenuEvent): Unit = {}
      override def popupMenuWillBecomeInvisible(e: PopupMenuEvent): Unit = {}
      override def popupMenuWillBecomeVisible(e: PopupMenuEvent): Unit = {
        for (pnti <- getSelections) {
          miContr.setEnabled(!pnti.retainedClassNames.isEmpty)
          miSContr.setEnabled(!pnti.sharedRetainedClassNames.isEmpty)
          miRoots.setEnabled(pnti.reachability != Reachability.Value)
        }
      }
    })

    menu
  }
}
