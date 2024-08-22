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
import javax.swing.JTabbedPane
import optimus.graph.OGTrace
import optimus.graph.diagnostics.NPTreeNode
import optimus.graph.diagnostics.gridprofiler.GridProfiler
import optimus.graph.diagnostics.gridprofiler.GridProfiler.Metric
import optimus.graph.diagnostics.gridprofiler.GridProfilerData
import optimus.graph.diagnostics.gridprofiler.ProfilerScope

import scala.collection.{mutable => m}

class TaskRegistryEntry(override val title: String, val engine: String, val level: Int) extends NPTreeNode {
  final val children = m.HashSet.empty[TaskRegistryEntry]
  final def getChildren: Iterable[NPTreeNode] = children
  final def hasChildren: Boolean = children.nonEmpty
}

object TaskRegistryTable {
  private val regularView = m.ArrayBuffer(
    new TableColumn[TaskRegistryEntry]("Task", 200) {
      override def valueOf(row: TaskRegistryEntry): TaskRegistryEntry = row
      override def getCellRenderer: NPTableRenderer.TreeTableCellRenderer = NPTableRenderer.treeRenderer
    },
    new TableColumnString[TaskRegistryEntry]("Engine", 200) {
      override def valueOf(row: TaskRegistryEntry): String = row.engine
      override def toolTip = "Grid engine that executed this task"
    }
  )
}

class TaskRegistryTable extends NPTreeTable[TaskRegistryEntry] {
  emptyRow = new TaskRegistryEntry("", "", 0)
  setView(TaskRegistryTable.regularView)

  def initTaskTree(registry: Map[String, (String, Seq[String])]): Unit = {
    val entries = m.HashMap.empty[String, TaskRegistryEntry]

    for ((name, (engine, parents)) <- registry)
      entries.put(name, new TaskRegistryEntry(name, engine, parents.size))

    for ((name, entry) <- entries) {
      val parents = registry(name)._2
      if (parents.nonEmpty) {
        val directParent = parents.last
        entries.get(directParent) match {
          case Some(parentEntry) => parentEntry.children += entry
          case None              => log.warn(s"Parent $directParent of task $name not found in the list of tasks")
        }
      }
    }
    setList(entries.values.filter(_.level == 0))
  }
}

class ScopeRegistryEntry(override val title: String, val blockId: Int, val level: Int) extends NPTreeNode {
  final val children = m.HashSet.empty[ScopeRegistryEntry]
  final def getChildren: Iterable[NPTreeNode] = children
  final def hasChildren: Boolean = children.nonEmpty
}

object ScopeRegistryTable {
  private val regularView = m.ArrayBuffer[TableColumn[ScopeRegistryEntry]](
    new TableColumn[ScopeRegistryEntry]("Scope", 200) {
      override def valueOf(row: ScopeRegistryEntry): ScopeRegistryEntry = row
      override def getCellRenderer: NPTableRenderer.TreeTableCellRenderer = NPTableRenderer.treeRenderer
    },
    new TableColumnCount[ScopeRegistryEntry]("Block ID", 200) {
      override def valueOf(row: ScopeRegistryEntry): Int = row.blockId
      override def toolTip = "Internal Block ID for this scope"
    }
  )
}

class ScopeRegistryTable extends NPTreeTable[ScopeRegistryEntry] {
  emptyRow = new ScopeRegistryEntry("", 0, 0)
  setView(ScopeRegistryTable.regularView)

  def initScopeTree(registry: Seq[(Seq[ProfilerScope], Int)]): Unit = {
    val entries = m.HashMap.empty[Seq[ProfilerScope], ScopeRegistryEntry]

    for ((scope, block) <- registry)
      entries.put(
        scope,
        new ScopeRegistryEntry(if (scope.isEmpty) "(global scope)" else scope.last.text, block, scope.size))

    val root = entries(GridProfiler.globalScope)

    for ((scope, entry) <- entries) {
      scope.size match {
        case 0 => /* this is globalScope */
        case 1 =>
          root.children += entry
        case _ =>
          // scope.last is this scope's name, dropping it gives us the direct parent
          val directParent = scope.dropRight(1)
          entries.get(directParent) match {
            case Some(parentEntry) => parentEntry.children += entry
            case None              => log.warn(s"Parent scope $directParent of $scope not found in the list of scopes")
          }
      }
    }
    setList(Seq(root))
  }
}

final case class MetricsEntry(
    task: String,
    scope: Seq[String],
    id: Int,
    name: String,
    value: Any
)

object MetricsTable {
  private val regularView = m.ArrayBuffer(
    new TableColumnString[MetricsEntry]("Task", 100) {
      override def valueOf(row: MetricsEntry): String = row.task
    },
    new TableColumnString[MetricsEntry]("Scope", 100) {
      override def valueOf(row: MetricsEntry): String = row.scope.mkString(".")
    },
    new TableColumn[MetricsEntry]("Metric ID", 100) {
      override def valueOf(row: MetricsEntry): Int = row.id
    },
    new TableColumnString[MetricsEntry]("Metric Key", 200) {
      override def valueOf(row: MetricsEntry): String = row.name
    },
    new TableColumnString[MetricsEntry]("Value", 200) {
      override def valueOf(row: MetricsEntry): String = row.value.toString
    }
  )
}

class MetricsTable extends NPTable[MetricsEntry] {
  setView(MetricsTable.regularView)

  def init(): Unit = {
    val res = m.ArrayBuffer.empty[MetricsEntry]

    for (
      (scope, row) <- GridProfilerData.data;
      (task, kvs) <- row;
      (value, key) <- kvs.zipWithIndex if value != null
    ) {
      val scopeName = GridProfiler.blockIDToScope(scope).map(_.map(_.text)).getOrElse(Seq("UNKNOWN SCOPE " + scope))
      if (key < Metric.maxId) Metric(key) match {
        case Metric.SYNCSTACKS   => res += MetricsEntry(task, scopeName, key, "SYNCSTACKS", value)
        case Metric.NONCONCLOOPS => res += MetricsEntry(task, scopeName, key, "NONCONCLOOPS", value)
        case Metric.DALSTATS     => res += MetricsEntry(task, scopeName, key, "DALSTATS", value)
        case Metric.STALLTIME    => res += MetricsEntry(task, scopeName, key, "STALLTIME", value)
        case Metric.PROCESS      => res += MetricsEntry(task, scopeName, key, "PROCESS", value)
        case Metric.VTTTSTACKS   => res += MetricsEntry(task, scopeName, key, "VTTTSTACKS", value)
        case Metric.HOTSPOTS     => res += MetricsEntry(task, scopeName, key, "HOTSPOTS", value)
        case Metric.SCHEDULER    => res += MetricsEntry(task, scopeName, key, "SCHED", value)
        case Metric.WALLTIME     => res += MetricsEntry(task, scopeName, key, "WALLTIME", value)
      }
      else
        res += MetricsEntry(
          task,
          scopeName,
          key,
          GridProfiler.metricIDToName(key).getOrElse("UNKNOWN KEY " + key),
          value)
    }
    // live metrics (note, hotspots are also live, but they are already displayed in the main hotspots tab)
    res += MetricsEntry(
      GridProfiler.clientKey,
      GridProfiler.globalScope.map(_.text),
      Metric.SCHEDULER.id,
      "SCHED",
      OGTrace.getSchedulerProfiles)

    setList(res)
  }
}

class GridProfilerTable extends JTabbedPane {
  val scope_table = new ScopeRegistryTable()
  val task_table = new TaskRegistryTable()
  val metrics_table = new MetricsTable()

  add("Scopes", scope_table)
  add("Tasks", task_table) // Only works when aggregation is default (task-level)
  add("Metrics", metrics_table)

  def init(): Unit = {
    task_table.initTaskTree(GridProfiler.getTaskRegistry)
    scope_table.initScopeTree(GridProfiler.getScopeRegistry)
    metrics_table.init()
  }
}
