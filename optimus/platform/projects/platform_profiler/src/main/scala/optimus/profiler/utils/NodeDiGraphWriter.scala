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
package optimus.profiler.utils

import java.util.{ArrayList => JArrayList}
import java.util.{Collection => JCollection}
import java.util.{HashMap => JHashMap}
import java.util.{HashSet => JHashSet}

import optimus.graph.OGTraceReader
import optimus.graph.diagnostics.PNodeTask
import optimus.graph.diagnostics.PNodeTaskInfo
import optimus.platform.util.PrettyStringBuilder
import optimus.profiler.recipes.PNodeTaskInfoGrp

import scala.jdk.CollectionConverters._

object NodeDiGraphWriter {
  val TWEAKABLE_COLOUR: String = "red"
  val PROXY_SHAPE: String = "ellipse"
  val INTERNAL: String = "dotted"

  def fullNodeGraphToString(reader: OGTraceReader, config: DigraphConfig = DefaultConfig): String = {
    val writer = new NodeDiGraphWriter(reader, config)
    writer.visit(reader.getRawTasks())
  }

  def fullNodeGraphToString(hotspots: Map[String, PNodeTaskInfo], tasks: Map[String, PNodeTask]): String = {
    val writer = new ReconstructedNodeDiGraphWriter(hotspots, tasks)
    // start with one root node otherwise we end up revisiting already visited nodes
    writer.visit(List(tasks("n1")).asJava)
  }

  def compressedNodeGraphToString(reader: OGTraceReader, collapseNonInternal: Boolean): String = {
    val writer = new NodeDiGraphWriter(reader)
    writer.visitGrp(NodeGraphReducer.compress(reader, collapseNonInternal))
  }

  def write(graph: JArrayList[PNodeTaskInfo]): String = new NodePntiWriter(graph).visit(graph)

  def header(sb: PrettyStringBuilder): PrettyStringBuilder = {
    sb.appendln("digraph {")
    sb.appendln("graph[rankdir=LR ordering=out]")
    sb.appendln("node [fontsize=8 shape=plain style=solid]")
    sb.appendln("edge [fontsize=7]")
  }

  def footer(sb: PrettyStringBuilder): PrettyStringBuilder = sb.appendln("}")

  def attributes(
      sb: PrettyStringBuilder,
      tweakable: Boolean,
      proxy: Boolean,
      internal: Boolean): PrettyStringBuilder = {
    if (tweakable) sb.append(" color=" + TWEAKABLE_COLOUR)
    if (proxy) sb.append(" shape=" + PROXY_SHAPE)
    if (internal) sb.append(" style=" + INTERNAL)
    sb
  }
}

class ReconstructedNodeDiGraphWriter(val hotspots: Map[String, PNodeTaskInfo], val tasks: Map[String, PNodeTask])
    extends NodeDiGraphWriter(null) {
  override def getTaskInfo(id: Int): PNodeTaskInfo = {
    val nodeName = s"n$id"
    hotspots.getOrElse(nodeName, null)
  }

  override def getPntiName(pnti: PNodeTaskInfo): String = pnti.nti.name()
}

class DigraphConfig(
    val dontSkipIncompleteInfo: Boolean = false,
    val alignRoots: Boolean = true,
    val includeEdgeIndex: Boolean = false,
    val alignByName: Boolean = false)

case object DefaultConfig extends DigraphConfig
case object EnqueuerGraphConfig extends DigraphConfig(false, false, true, alignByName = true)

class NodeDiGraphWriter(reader: OGTraceReader, config: DigraphConfig = DefaultConfig) {
  private val sb = new PrettyStringBuilder
  private val ids = new JHashMap[Integer, Integer]
  private val nodes = new JHashSet[Integer]

  private def header(): Unit = NodeDiGraphWriter.header(sb)
  private def footer(): Unit = NodeDiGraphWriter.footer(sb)

  private def taskIDsToString(tasks: Iterable[PNodeTask]): PrettyStringBuilder = {
    val ids = new PrettyStringBuilder()
    val it = tasks.iterator
    while (it.hasNext) {
      ids.append("n" + idOf(it.next()))
      ids.append(" ")
    }
    ids
  }

  private def sameRank(sb: PrettyStringBuilder, tasks: Iterable[PNodeTask]): Unit = {
    val tasksIDs = taskIDsToString(tasks)
    sb ++= "{rank=same " ++= tasksIDs ++= "}"
    sb.endln()
  }

  def visit(roots: JCollection[PNodeTask]): String = {
    header()
    if (roots != null) {
      val it = roots.iterator()
      while (it.hasNext) visit(it.next())

      if (config.alignRoots)
        sameRank(sb, roots.asScala)

      if (config.alignByName)
        roots.asScala.groupBy(_.infoId()).foreach { grp =>
          if (grp._2.size > 1) // Create rank only for PNTIs with at least 2 tasks
            sameRank(sb, grp._2)
        }
    }
    footer()
    sb.toString
  }

  private def visit(task: PNodeTask): Unit = if (!ids.containsKey(task.id)) {
    val idFrom = idOf(task)
    val callees = task.getCalleesWithEnqueues
    if (!callees.isEmpty) {
      var i = 0
      while (i < callees.size()) {
        val child = callees.get(i)
        val idTo = idOf(child.id, child.infoId, markVisited = false)
        edge(idFrom, idTo, 1, i, callees.isEnqueue(i))
        visit(child)
        i += 1
      }
    }
  }

  def visitGrp(roots: JCollection[PNodeTaskInfoGrp]): String = {
    header()
    val it = roots.iterator()
    while (it.hasNext) {
      visit(it.next())
    }
    footer()
    sb.toString
  }

  def visit(roots: JArrayList[PNodeTaskInfo]): String = {
    header()
    val it = roots.iterator()
    while (it.hasNext) {
      visit(it.next())
    }
    footer()
    sb.toString
  }

  def visit(info: PNodeTaskInfo): Unit = if (!ids.containsKey(info.id)) { // Avoid cycles
    val idFrom = idOf(info.id, info.id)
    if (info.edges ne null) {
      val it = info.edges.iterator()
      while (it.hasNext) {
        val child = it.next()
        if (config.dontSkipIncompleteInfo || (child ne null)) {
          val idTo = idOf(child.id, child.id, markVisited = false)
          edge(idFrom, idTo, 1)
          visit(child)
        }
      }
    }
  }

  def hasPath(from: PNodeTaskInfo, to: PNodeTaskInfo): Boolean = {
    var found = false

    def hasPathRecursive(from: PNodeTaskInfo, to: PNodeTaskInfo): Boolean = {
      if (!ids.containsKey(from.id)) { // Avoid cycles
        idOf(from.id, from.id) // make sure from.id goes into the ids map (and mark visited)
        if (from.edges ne null) {
          val it = from.edges.iterator()
          while (it.hasNext) {
            val child = it.next()
            if (config.dontSkipIncompleteInfo || (child ne null)) {
              if (!found) {
                found = child.id == to.id
                hasPathRecursive(child, to)
              }
            }
          }
        }
      }
      found
    }

    ids.clear() // in case someone used the writer before
    hasPathRecursive(from, to)
  }

  def visit(grp: PNodeTaskInfoGrp): Unit = {
    val idFrom = idOf(grp)
    val it = grp.childrenMap.values().iterator()
    while (it.hasNext) {
      val child = it.next()
      if (config.dontSkipIncompleteInfo || child.completeInfo) {
        val visited = ids.containsKey(child.id)
        val idTo = idOf(child)
        edge(idFrom, idTo, child.incomingEdges)
        if (!visited) // Avoid cycles
          visit(child)
      }
    }
  }

  private def edge(
      idFrom: Integer,
      idTo: Integer,
      edgeCount: Int,
      edgeIndex: Int = -1,
      enqueueEdge: Boolean = false): Unit = {
    sb.append("n" + idFrom)
    sb.append("->")
    sb.append("n" + idTo)
    if (config.includeEdgeIndex && edgeIndex > -1)
      sb.append("[label=" + edgeIndex + "]")
    if (edgeCount > 1)
      sb.append("[label=" + edgeCount + "]")
    else if (enqueueEdge)
      sb.append("[style=" + NodeDiGraphWriter.INTERNAL + "]")
    sb.appendln(";")
  }

  private def idOf(grp: PNodeTaskInfoGrp): Integer = idOf(grp.id, grp.pnti.id)
  private def idOf(task: PNodeTask): Integer = idOf(task.id, task.infoId)

  def getTaskInfo(id: Int): PNodeTaskInfo = reader.getTaskInfo(id)
  def getPntiName(pnti: PNodeTaskInfo): String = pnti.fullNamePackageShortened()

  private def idOf(id: Integer, nti_id: Int, markVisited: Boolean = true): Integer = {
    var i: Integer = ids.get(id)
    if (i != null) { return i }
    i = ids.size + 1
    if (markVisited)
      ids.put(id, i)

    val pnti = if (nti_id < 0) null else getTaskInfo(nti_id)
    if (!nodes.contains(i) && (config.dontSkipIncompleteInfo || (pnti ne null))) {
      nodes.add(i)
      sb.append("n" + i)
      sb.append("[label=\"")

      val pntiName = if (pnti != null) getPntiName(pnti) else nti_id.toString
      sb.append(pntiName)
      sb.append("\"")

      if (pnti != null)
        NodeDiGraphWriter.attributes(sb, pnti.isDirectlyTweakable, pnti.isProfilerProxy, pnti.isInternal)
      sb.appendln("]")
    }
    i // return value
  }
}
