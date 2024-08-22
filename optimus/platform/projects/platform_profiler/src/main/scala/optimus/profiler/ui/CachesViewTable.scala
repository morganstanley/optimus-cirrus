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

import optimus.graph.DiagnosticSettings
import optimus.graph.JMXConnection
import optimus.graph.NodeTask
import optimus.graph.cache.Caches
import optimus.graph.cache.CauseProfiler
import optimus.graph.diagnostics.PNodeTask
import optimus.graph.outOfProcess.views.CacheView
import optimus.graph.outOfProcess.views.MemoryView
import optimus.graph.outOfProcess.views.MemoryViewHelper
import optimus.profiler.DebuggerUI
import optimus.profiler.ui.common.JPanel2
import optimus.profiler.ui.common.JPopupMenu2
import optimus.profiler.ui.common.JToolBar2
import optimus.profiler.ui.common.ProvidesMenus
import optimus.profiler.ui.controls.SingleImageButton

import java.awt.BorderLayout
import java.util.{ArrayList => JArrayList}
import javax.swing.JOptionPane
import javax.swing.JTabbedPane
import scala.collection.mutable.ArrayBuffer

object CachesViewTable {
  private val regularView = ArrayBuffer(
    new TableColumnString[CacheView]("Name", 100) {
      override def valueOf(row: CacheView): String = row.name
    },
    new TableColumnLong[CacheView]("Max Size", 80) {
      override def valueOf(row: CacheView): Long = row.maxSize
    },
    new TableColumnLong[CacheView]("Current Size", 80) {
      override def valueOf(row: CacheView): Long = row.currentSize
    }
  )

  private val lockingColumns = ArrayBuffer(
    new TableColumnTime[CacheView]("Lock Contention Time (ms)", 80) {
      override def valueOf(row: CacheView): Double = row.profLockContentionTime * 1e-6
      override def toolTip = "Contention time on updateLock when ledger is updated (in milliseconds)"
    },
    new TableColumnCount[CacheView]("Attempts on lock", 80) {
      override def valueOf(row: CacheView): Int = row.profLockNumAttempts
      override def toolTip = "Number of times an attempt to take updateLock is made"
    },
    new TableColumnCount[CacheView]("Cache Batch Size", 80) {
      override def valueOf(row: CacheView): Int = row.profCacheBatchSize
      override def toolTip = "Cache batch size (set by optconf file or in CacheDefaults)"
    },
    new TableColumnCount[CacheView]("Cache Batch Size Padding", 80) {
      override def valueOf(row: CacheView): Int = row.profCacheBatchSizePadding
      override def toolTip = "Cache batch size padding (set by optconf file or in CacheDefaults)"
    }
  )

  private val evictionColumns = ArrayBuffer(
    new TableColumnCount[CacheView]("Evictions (Overflow)", 80) {
      override def valueOf(row: CacheView): Int = row.evictionOverflow
      override def toolTip = "Number of evictions caused by overflow"
    },
    new TableColumnCount[CacheView]("Evictions (Total)", 80) {
      override def valueOf(row: CacheView): Int = row.evictionTotal
      override def toolTip = "Total number of evictions"
    }
  )

  private val misc = ArrayBuffer[TableColumn[CacheView]](
    new TableColumnCount[CacheView]("Evictions (User cleared)", 200) {
      override def valueOf(row: CacheView): Int = row.evictionUserClear
    },
    new TableColumnCount[CacheView]("Evictions (GCMonitor)", 200) {
      override def valueOf(row: CacheView): Int = row.evictionGCMonitor
    },
    new TableColumnCount[CacheView]("Evictions (Invalid entries)", 200) {
      override def valueOf(row: CacheView): Int = row.evictionInvalidEntry
    }
  )

  private val allAvailable = regularView ++ lockingColumns ++ evictionColumns ++ misc
}

/** View current state of the caches */
class CachesViewTable extends NPTable[CacheView] with Filterable[CacheView] {
  dataTable.setComponentPopupMenu(initPopupMenu)

  private def initPopupMenu: JPopupMenu2 = {
    val menu = new JPopupMenu2
    menu.addMenu(
      "Show all nodes...", {
        val selectedCache = getSelection
        if (selectedCache ne null) {
          val nodesInCache = new JArrayList[NodeTask]()
          selectedCache.clear(n => { nodesInCache.add(n); false })
          DebuggerUI.browse(PNodeTask.fromLive(nodesInCache), ignoreFilter = true)
        }
      }
    )
    menu.addMenu(
      "Clear Cache", {
        val selectedCache = getSelection
        if (selectedCache ne null) {
          val name = selectedCache.name
          val perPropertyCache = Caches.getPropertyCache(name)
          if (perPropertyCache.nonEmpty) perPropertyCache.foreach(_.clear(CauseProfiler))
          else { // check in case this is a shared cache...
            Caches.getSharedCache(name).foreach(_.clear(CauseProfiler))
          }
        }
      }
    )
    menu
  }
  override def initialColumns: ArrayBuffer[TableColumn[CacheView]] = CachesViewTable.allAvailable
}

class CachesView(val offlineReview: Boolean) extends JPanel2(new BorderLayout()) with ProvidesMenus {
  val tabs = new JTabbedPane
  private val cacheTable = new CachesViewTable
  private val mem_table = new MemoryTable

  init()
  private def init(): Unit = {
    val toolBar = new JToolBar2()
    val enabled = !DiagnosticSettings.offlineReview
    val btnRefresh = SingleImageButton.createRefreshButton("Requery state of caches", enabled) { cmdRefresh() }
    toolBar.add(btnRefresh)
    toolBar.addSeparator()
    toolBar.addButton("GC", "Force garbage collection", enabled = !offlineReview) {
      GraphDebuggerUI.showMessage("GC")
      if (DiagnosticSettings.outOfProcess && JMXConnection.graph.getConsoleFlag) {
        JMXConnection.graph.gc()
      } else {
        Runtime.getRuntime.gc()
      }
    }
    toolBar.addSeparator()
    toolBar.addButton(
      "Accessible Memory",
      "This computes the sum total of all the memory " +
        "accessible directly or indirectly from the cache. This is an upper bound on the amount of memory" +
        "that would be cleared if the cache were cleared",
      enabled = !offlineReview
    ) {
      cmdComputeCacheMemory(false)
    }
    toolBar.addSeparator()
    toolBar.addButton(
      "Retained Memory",
      "This computes the memory actually held by the cache also referenced from somewhere else," +
        "Being \"retained\" means that its presence in the cache prevents this memory from being collected." +
        "If it were referenced anywhere else, simply removing it from the cache wouldn't free the object; " +
        "it is only freed once all references to it are gone." +
        "This is significantly slower than computing the accessible memory",
      enabled = !offlineReview
    ) {
      cmdComputeCacheMemory(true)
    }
    toolBar.addSeparator()
    tabs.add("Caches", cacheTable)
    tabs.add("Memory", mem_table)

    add(toolBar, BorderLayout.NORTH)
    add(tabs, BorderLayout.CENTER)
  }

  private final def cmdComputeCacheMemory(precise: Boolean): Unit = {
    try {
      val rows: ArrayBuffer[MemoryView] = MemoryViewHelper.getMemory(precise)
      mem_table.setRows(rows, rows)
      tabs.setSelectedComponent(mem_table)
    } catch {
      case _: UnsatisfiedLinkError =>
        val dllName =
          if (System.getProperty("os.name").toLowerCase().contains("windows")) "heapexp.dll" else "libheapexp.so"
        JOptionPane.showMessageDialog(
          this,
          s"Heap Explorer not loaded, please add $dllName to -agentpath:",
          "Could not Compute Memory Profile",
          JOptionPane.ERROR_MESSAGE);
    }
  }

  def cmdRefresh(): Unit = cacheTable.setList(NodeProfiler.refreshCacheView())
}
