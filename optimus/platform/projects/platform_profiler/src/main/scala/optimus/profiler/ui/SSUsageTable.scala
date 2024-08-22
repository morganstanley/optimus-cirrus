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

import optimus.graph.outOfProcess.views.ScenarioStackProfileView

import scala.collection.mutable.ArrayBuffer

object SSUsageTable {

  val regularView: ArrayBuffer[TableColumn[ScenarioStackProfileView]] = ArrayBuffer(
    new TableColumn[ScenarioStackProfileView]("Name", 300) {
      override def valueOf(row: ScenarioStackProfileView): ScenarioStackProfileView = row
      override def getCellRenderer: NPTableRenderer.TreeTableCellRenderer = NPTableRenderer.treeRenderer
    },
    new TableColumn[ScenarioStackProfileView]("Created", 100) {
      override def valueOf(row: ScenarioStackProfileView): Long = row.created
    },
    new TableColumn[ScenarioStackProfileView]("Re-used", 100) {
      override def valueOf(row: ScenarioStackProfileView): Long = row.reUsed
    },
    new TableColumn[ScenarioStackProfileView]("Self Time", 120) {
      override def valueOf(row: ScenarioStackProfileView): Double = row.selfTime
    },
    new TableColumnString[ScenarioStackProfileView]("By Name Tweaks", 100) {
      override def valueOf(row: ScenarioStackProfileView): String = row.byNameTweak
    },
    new TableColumnString[ScenarioStackProfileView]("Source") {
      override def valueOf(row: ScenarioStackProfileView): String = row.source
    }
  )
}

class SSUsageTable extends NPTreeTable[ScenarioStackProfileView] {
  emptyRow = ScenarioStackProfileView(0, null, 0, 0, 0, null, null)
  setView(SSUsageTable.regularView)
}
