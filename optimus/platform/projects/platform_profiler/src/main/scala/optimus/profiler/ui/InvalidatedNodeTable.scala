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

import msjava.slf4jutils.scalalog.Logger
import optimus.graph.NodeTaskInfo
import optimus.graph.NodeTrace
import optimus.graph.diagnostics.PNodeInvalidate
import optimus.profiler.ui.common.JPopupMenu2

import scala.jdk.CollectionConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.compat._

object InvalidatedNodeTable {
  val log: Logger = msjava.slf4jutils.scalalog.getLogger(this)
  val regularView: ArrayBuffer[TableColumn[PNodeInvalidate]] = ArrayBuffer(
    new TableColumnString[PNodeInvalidate]("Tweak Key", 900) {
      override def valueOf(row: PNodeInvalidate): String = row.title
    },
    new TableColumn[PNodeInvalidate]("Tracked", 150) {
      override def valueOf(row: PNodeInvalidate): Boolean = row.isTrackedForInvalidation
    },
    new TableColumnString[PNodeInvalidate]("Tweak To...") {
      override def valueOf(row: PNodeInvalidate): String =
        try {
          row.tweakTo
        } catch {
          case x: Exception =>
            log.warn("Tweak's toString threw exception", x)
            "? (toString failed)"
        }
    },
    new TableColumnCount[PNodeInvalidate]("Invalidated Nodes") {
      override def valueOf(row: PNodeInvalidate): Int = row.invalidatedNodeCount
    }
  )

  val dependentViewColumns: ArrayBuffer[TableColumn[PropertyAndCount]] = ArrayBuffer(
    new TableColumnCount[PropertyAndCount]("Count of invalidations", 100) {
      override def valueOf(row: PropertyAndCount): Int = row.count
    },
    new TableColumnString[PropertyAndCount]("Invalidated node", 900) {
      override def valueOf(row: PropertyAndCount): String = row.property.fullName
    }
  )

  def openTo(property: NodeTaskInfo): Unit = {
    val r = NodeTrace.getInvalidates.asScala.filter(i =>
      i.property == property ||
        i.invalidatedNodes.exists(_.exists(_ == property)))
    val ra = ArrayBuffer.empty ++ r
    val table = new InvalidatedNodeTable
    table.setList(ra)
    GraphDebuggerUI.addTab("Invalidated tweakables", table)
  }
}

final case class PropertyAndCount(property: NodeTaskInfo, count: Int)

class InvalidatedNodeTable extends NPTable[PNodeInvalidate] with Filterable[PNodeInvalidate] {
  emptyRow = PNodeInvalidate(null)
  override def initialColumns: ArrayBuffer[TableColumn[PNodeInvalidate]] = InvalidatedNodeTable.regularView

  NodeTrace.traceInvalidates.addCallback(enabled => {
    setMessage(if (!enabled) "Select 'Record Invalidates' to collect stats for UI and Reactive applications" else null)
  })

  dataTable.setComponentPopupMenu(initPopupMenu)

  private def initPopupMenu: JPopupMenu2 = {
    val menu = new JPopupMenu2
    menu.addMenu("Show invalidated nodes...", cmdShowInvalidatedNodes())
    TrackingMenu.addItems(menu, this)
    menu
  }

  private def cmdShowInvalidatedNodes(): Unit = {
    val counts = mutable.Map[NodeTaskInfo, Int]()
    getSelections.foreach { row =>
      row.invalidatedNodes.foreach {
        _.foreach { n =>
          counts.put(n, counts.getOrElse(n, 0) + 1)
        }
      }
    }
    val results: ArrayBuffer[PropertyAndCount] =
      counts.iterator.map { case (prop, count) => PropertyAndCount(prop, count) }.to(ArrayBuffer)
    val table = new InvalidatedNodeDependentTable(results)
    GraphDebuggerUI.addTab("Invalidated nodes", table)
  }
}

class InvalidatedNodeDependentTable(propertyCounts: ArrayBuffer[PropertyAndCount])
    extends NPTable[PropertyAndCount]
    with Filterable[PropertyAndCount] {

  setList(propertyCounts)
  override def initialColumns: ArrayBuffer[TableColumn[PropertyAndCount]] = InvalidatedNodeTable.dependentViewColumns
}
