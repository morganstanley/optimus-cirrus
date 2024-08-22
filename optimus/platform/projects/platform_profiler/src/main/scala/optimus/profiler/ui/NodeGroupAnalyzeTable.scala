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

import javax.swing.JPopupMenu.Separator
import javax.swing.table.TableCellRenderer
import optimus.debugger.browser.ui.NodeReview
import optimus.graph.NodeTaskInfo
import optimus.graph.diagnostics.PNodeTask
import optimus.profiler.DebuggerUI
import optimus.profiler.ui.common.JPopupMenu2
import optimus.profiler.ui.common.NodeGroup
import optimus.profiler.ui.common.NodeGroupAllInputs
import optimus.profiler.ui.common.NodeGroupArgs
import optimus.profiler.ui.common.NodeGroupCacheType
import optimus.profiler.ui.common.NodeGroupEntity
import optimus.profiler.ui.common.NodeGroupScenarioStack
import optimus.profiler.ui.common.NodeGroupTaskInfo
import optimus.profiler.ui.common.NodeGroupTaskResult
import optimus.profiler.ui.common.NodeGroupTotals

import scala.collection.mutable.ArrayBuffer

object NodeGroupAnalyzeTable {
  private val defView = ArrayBuffer(
    new TableColumn[NodeGroup]("Name", 400) {
      override def valueOf(row: NodeGroup): Any = row
      override def getCellRenderer: TableCellRenderer = NPTableRenderer.treeRenderer
    },
    new TableColumnCount[NodeGroup]("Started", 80) {
      override def valueOf(row: NodeGroup): Int = row.start
    },
    new TableColumnTime[NodeGroup]("Time (ms)", 80) {
      override def valueOf(row: NodeGroup): Double = row.computeTime * 1e-6
      override def toolTip: String = "Node self-time + self-time of all non-cached direct children"
    },
    new TableColumnTime[NodeGroup]("Save Time! (ms)", 80) {
      override def valueOf(row: NodeGroup): Double = row.saveTime * 1e-6
      override def toolTip: String =
        "Potential time saving if we'd got cache hits on this node (try configuring caches!)" +
          "<br>Calculated as saving per node start if we cached this node once and hit on every other request"
    }
  )

  def groupByName(group: Seq[NodeGroup]): String = group.map(_.name).mkString(" / ")
}

/**
 * A tree view that focuses/group interesting node for analysis together For example:
 * -Total
 * --Node Type
 * ----Node Result Value
 *
 * OR
 * -Total
 * -- Cache Type
 * -----Node Type
 * ------Node Result Value
 *
 * Step 1
 * -Total
 * -- Cache Type
 * -----Property Name aka TaskInfo
 * ------Node Result Value Count <=1 Args Entity EquivScenario <---
 */
class NodeGroupAnalyzeTable(nodeAnalyzer: NodeAnalyzer) extends NPTreeTable[NodeGroup] {
  import NodeGroupAnalyzeTable._

  emptyRow = new NodeGroupTotals()
  private var allTasks: List[PNodeTask] = _

  val grpByDefault = List(new NodeGroupCacheType(), new NodeGroupTaskInfo(), new NodeGroupTaskResult())
  val grpByDetails = List(
    new NodeGroupCacheType(),
    new NodeGroupTaskInfo(),
    new NodeGroupTaskResult(),
    new NodeGroupEntity()
  )
  val grpByDetails2 = List(
    new NodeGroupCacheType(),
    new NodeGroupTaskInfo(),
    new NodeGroupArgs(),
    new NodeGroupEntity(),
    new NodeGroupTaskResult()
  )
  val grpByXSLightCandidates = List(
    new NodeGroupCacheType(),
    new NodeGroupTaskInfo(),
    new NodeGroupTaskResult(),
    new NodeGroupAllInputs()
  )
  val grpByNodeInfo = List(
    new NodeGroupCacheType(),
    new NodeGroupTaskInfo(),
    new NodeGroupArgs(),
    new NodeGroupScenarioStack()
  )

  setView(defView)
  init()

  dataTable.getSelectionModel.addListSelectionListener { _ =>
    val sgrp = getSelection
    sgrp match {
      case sgrpr: NodeGroup => nodeAnalyzer.setSource(sgrpr.allNodeTasks.map(new NodeReview(_)))
      case _                =>
    }
  }

  private def init(): Unit = {
    val menu = new JPopupMenu2
    Seq(grpByDefault, grpByDetails, grpByDetails2, grpByXSLightCandidates, grpByNodeInfo).foreach { grp =>
      menu.addMenu(s"Group By: ${groupByName(grp)}", setSource(allTasks, grp))
    }
    val separator = new Separator
    menu.add(separator)
    val markXSL = menu.addMenu("Mark for XSFT") { selectedNodeTaskInfos().foreach(_.markAsProfilerUIConfigured) }

    var selectedNodeGroup: NodeGroup = null
    val markXSFTGroup = menu.addMenu("Mark All for XSFT") {
      if (selectedNodeGroup ne null) selectedNodeGroup.doAction()
    }

    menu.addOnPopup({
      val ntis = selectedNodeTaskInfos()
      if (ntis.nonEmpty) {
        separator.setVisible(true)
        markXSL.setEnabled(true)
        markXSL.setText(s"Mark ${ntis.size} properties for XSFT")
        markXSL.setVisible(true)
        markXSFTGroup.setVisible(false)
      } else {
        val gn = getSelection
        if (gn != null) {
          val menuItemsVisible = gn.modifyMenus(markXSFTGroup, markXSL)
          separator.setVisible(menuItemsVisible)
          selectedNodeGroup = gn
        }
      }
    })

    dataTable.setComponentPopupMenu(menu)
  }

  def nodeResultType: Class[_ <: AnyRef] = DebuggerUI.nodeResultType(allTasks)

  private def selectedNodeTaskInfos(): ArrayBuffer[NodeTaskInfo] = {
    getSelections.flatMap(ng =>
      ng.key match {
        case nti: NodeTaskInfo => Some(nti)
        case _                 => None
      })
  }

  def resetSource(): Unit = setSource(allTasks)

  def setSource(allTasks: List[PNodeTask]): Unit = setSource(allTasks, grpByDefault)

  private def setSource(allTasks: List[PNodeTask], prototypes: List[NodeGroup]): Unit = {
    this.allTasks = allTasks
    val root = new NodeGroupTotals()

    allTasks.foreach(t => if (t.isDoneWithResult) root.insert(t, prototypes))
    root.trimAndSort()
    setList(List(root))
  }
}
