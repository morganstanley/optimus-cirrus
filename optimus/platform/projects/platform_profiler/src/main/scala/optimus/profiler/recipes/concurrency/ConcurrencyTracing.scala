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
package optimus.profiler.recipes.concurrency

import java.util.{ArrayDeque => JArrayDeque}
import java.util.{Arrays => JArrays}
import java.util.{ArrayList => JArrayList}
import msjava.slf4jutils.scalalog.getLogger
import optimus.core.EdgeList
import optimus.graph.DiagnosticSettings.getBoolProperty
import optimus.graph.NodeTaskInfo
import optimus.graph.OGTraceReader
import optimus.graph.diagnostics.NodeName
import optimus.graph.diagnostics.PNodeTask
import optimus.graph.diagnostics.PNodeTaskInfo
import optimus.graph.diagnostics.PNodeTaskRecorded
import optimus.graph.tracking.DependencyTrackerActionTask._
import optimus.profiler.extensions.ReaderExt.findInfoByName
import optimus.profiler.extensions.ReaderExt.getTasksByPnti
import optimus.profiler.recipes.NodeGraphVisitor

import scala.jdk.CollectionConverters._
import scala.collection.mutable.ArrayBuffer

object MinimumUnitCostReport {
  val empty = new MinimumUnitCostReport(
    PNodeTask.fakeInfo,
    false,
    ArrayBuffer.empty[MinimumUnitCostResult],
    ArrayBuffer.empty[PNodeTask],
    0,
    0)
}

/**
 * statsPerEntry and totalCost are related:
 *   - if statsPerEntry is false, totalCost is the true cost and accounts for reuse of nodes between graph entry points
 *   - if statsPerEntry is true, we don't report total cost at all because cost is the sum of costs across graph entries
 *     and can be MUCH higher than the true cost
 */
final class MinimumUnitCostReport(
    val functionOfInterest: PNodeTaskInfo,
    val statsPerEntry: Boolean,
    val graphEntries: ArrayBuffer[MinimumUnitCostResult],
    val tasksOfInterest: ArrayBuffer[PNodeTask],
    val maxScaling: Int,
    val totalCost: Int
)

/**
 * If this entry point represents a grouped entry point, sourceEntries are the sources. Note: only group if
 * config.groupByTime is true AND group is detected/needed
 */
final class MinimumUnitCostResult(
    val graphEntry: PNodeTask,
    val reachableCostlyFunctions: Long,
    val cost: Int,
    /** For a given function of interest how many of those nodes can be observed executing in parallel */
    val maxScaling: Int,
    val syncPoints: Array[SyncPointReport],
    val sourceEntries: Array[SyncBranch]
)

final case class SyncPointReport(fromTask: PNodeTask, reachableCostlyFunctions: Long, toTasks: Array[SyncBranch]) {
  def nodeName: NodeName = fromTask.nodeName
}

final class SyncBranch(val task: PNodeTask, val reachableCostlyFunctions: Long) {
  def nodeName: NodeName = task.nodeName
}

object ConcurrencyTracing {
  val default: ConcurrencyTracingConfig = ConcurrencyTracingConfig(statsPerEntry = false, groupByTime = false)
  val perEntry: ConcurrencyTracingConfig = ConcurrencyTracingConfig(statsPerEntry = true, groupByTime = false)

  private val log = getLogger(this)

  // [for debugging only] enable all verbose logging
  private val debugLog = getBoolProperty("optimus.profile.graphAlgos.dbgLog", false)

  // [for debugging only] only log state of the queue
  private val debugQueue = getBoolProperty("optimus.profile.graphAlgos.dbgQ", false)

  private val NeverEnqueued = Int.MinValue
  private val EnqueueAfterWait = Int.MaxValue
}

final case class ConcurrencyTracingConfig(
    statsPerEntry: Boolean = false,
    groupByTime: Boolean = false,
    groupByEarliestTask: Boolean = true, // all tasks in a group should overlap with the earliest task in that group
    minTime: Long = Long.MinValue,
    maxTime: Long = Long.MaxValue)

/**
 * Start with zero Pass the current count on visit to child Reaching interesting node (+1 to self) Collecting edge
 * current_cost = max(current, child_cost)
 */
final class ConcurrencyTracing(config: ConcurrencyTracingConfig) {
  import ConcurrencyTracing._

  /** Only used during computation and corresponds to SyncPointReport */
  final private class SyncPointBranch(val task: PNodeTask, val toTasks: ArrayBuffer[PNodeTask])

  final private class State(val task: PNodeTask) {
    var enqueued: Boolean = _ // Also, logically 'running' if you have an infinite number of CPUs
    var waiting: Boolean = _ // Can't be enqueued because it's waiting on the result of another node
    var completed: Boolean = _
    var backEdges: ArrayBuffer[State] = _ // i.e. waiters to notify of completion
    var edgesProcessed: Int = _ // counts how many edges visited in bfs (edgesProcessed == edges.size => done)
    var entryCost: Int = NeverEnqueued // (First enqueue cost)  This var can really be removed, but helps debugging
    var cost: Int = _
    var reachableCostlyFunctions: Long = _

    var firstCostlyTask: State = _
    var syncBranch: SyncPointBranch = _

    val infoId: Int = task.infoId

    def stateToString: String = {
      if (!debugLog) ""
      else {
        val sb = new StringBuilder
        sb.append("[")
        if (enqueued) sb.append("e")
        if (waiting) sb.append("w")
        if (completed) sb.append("c")
        sb.append("] (cost = ").append(cost).append(")")
        sb.toString
      }
    }

    def argsString: String = {
      val args = task.args
      if ((args ne null) && args.nonEmpty) s"(${args.mkString(", ")})" else ""
    }

    override def toString: String = s"${task.nodeName.name}$argsString $stateToString".trim
  }

  final private class Visitor(val costlyNode: PNodeTaskInfo) {
    private val visitedNodes = ArrayBuffer[PNodeTask]()
    private var states: Array[State] = new Array[State](8)
    val syncBranches: ArrayBuffer[SyncPointBranch] = ArrayBuffer[SyncPointBranch]()
    var maxScaling: Int = _

    private var costlyNodeEvaluating = new ArrayBuffer[State]() // Slow pokes hang out place
    private var bfsQ: JArrayDeque[State] = _

    /** Allows to associate additional data while walking */
    def stateOf(v: PNodeTask): State = {
      if (v.visitedID == 0) {
        visitedNodes += v // Off by 1, because 0 is effectively reserved to mean not visited
        v.visitedID = visitedNodes.size
      }
      val visitID = v.visitedID
      if (visitID >= states.length)
        states = JArrays.copyOf(states, Math.max(visitID + 64, states.length * 2))

      var data = states(visitID)
      if (data eq null) {
        data = new State(v)
        v.visitedID == states.length
        states(visitID) = data
      }
      data
    }

    /**
     * convert tasks to list of sync branches (which include extra data, reachableCostlyFunctions, collected during
     * visit)
     */
    private def syncBranch(tasks: ArrayBuffer[PNodeTask]): Array[SyncBranch] = {
      val r = new Array[SyncBranch](tasks.size)
      var i = 0
      while (i < r.length) {
        val task = tasks(i)
        r(i) = new SyncBranch(task, stateOf(task).reachableCostlyFunctions)
        i += 1
      }
      r
    }

    def syncPointsReport: Array[SyncPointReport] = {
      val r = new Array[SyncPointReport](syncBranches.size)
      var i = 0
      while (i < r.length) {
        val sb = syncBranches(i)
        r(i) = SyncPointReport(sb.task, stateOf(sb.task).reachableCostlyFunctions, syncBranch(sb.toTasks))
        i += 1
      }
      r
    }

    def enqueue(v: State, currentCost: Int): Unit = {
      // Just an assertion (without BFS this could fire depending on path taken)
      // effectively also checks for v.enqueued && currentCost != EnqueueAfterWait && v.entryCost != NeverEnqueued
      if (v.entryCost > currentCost)
        throw new IllegalStateException("How did we find a shorter path?")

      if (!v.enqueued && !v.completed && (!v.waiting || currentCost == EnqueueAfterWait)) {
        bfsQ.add(v)
        if (debugQueue) printQueue()
        v.enqueued = true
        if (debugLog) log.info(s"$v - enqueued")
        if (currentCost == EnqueueAfterWait) {
          if (v.entryCost == NeverEnqueued) throw new IllegalStateException()
        } else if (v.entryCost == NeverEnqueued) {
          v.entryCost = currentCost
          v.cost = currentCost
          if (debugLog) log.info(s"$v - updated entryCost and cost to currentCost = $currentCost")
        } else
          throw new IllegalStateException()
      }
    }

    def collect(parent: State, child: State): Unit = {
      if (debugLog) log.info(s"$parent - collecting child and setting cost to max of ${parent.cost}, ${child.cost}")
      parent.reachableCostlyFunctions |= child.reachableCostlyFunctions
      if (child.cost > parent.cost) {
        if (child.entryCost > parent.entryCost) {
          if (parent.syncBranch eq null) {
            val syncBranch = new SyncPointBranch(parent.task, new ArrayBuffer[PNodeTask]())
            parent.syncBranch = syncBranch
            if (parent.firstCostlyTask eq null) syncBranch.toTasks += parent.task // This is recursive case!
            else syncBranch.toTasks += parent.firstCostlyTask.task
            syncBranches += syncBranch
          }
          parent.syncBranch.toTasks += child.task
        }
        if (parent.firstCostlyTask eq null) parent.firstCostlyTask = child
        parent.cost = child.cost
      }
    }

    def printQueue(): Unit = log.info(s"Queue: [${bfsQ.toArray.mkString(", ")}]")

    def notifyWaiters(child: State): Unit = {
      if (child.backEdges ne null) {
        var i = 0
        while (i < child.backEdges.size) {
          val waiter = child.backEdges(i)
          collect(waiter, child)
          enqueue(waiter, EnqueueAfterWait)
          i += 1
        }
        child.backEdges = null
      }
    }

    def continueWith(child: State, waiter: State): Unit = {
      if (child.backEdges eq null)
        child.backEdges = new ArrayBuffer[State]()
      child.backEdges += waiter
    }

    def executeNode(v: State): Unit = {
      if (debugLog) log.info(s"$v - executing node, setting enqueued and waiting to false")
      v.enqueued = false
      v.waiting = false

      val edges = v.task.getCalleesWithEnqueues
      var suspend = false
      var currentEdgeIndex = v.edgesProcessed
      while (!suspend && currentEdgeIndex < edges.size) {
        val edge = edges.get(currentEdgeIndex)
        val e = stateOf(edge)
        if (debugLog) log.info(s"$v - processing edge $currentEdgeIndex $e")
        if (edges.isEnqueue(currentEdgeIndex)) {
          enqueue(e, v.cost)
        } else if (e.completed) { // this is a dependency (back) edge, so either we'll collect the cost from the child or 'suspend'
          collect(v, e)
        } else if (e.entryCost == NeverEnqueued) {
          v.edgesProcessed -= 1
          enqueue(e, v.cost)
        } else {
          if (debugLog) log.info(s"$e - continue with awaiter $v and suspend")
          continueWith(e, v)
          v.waiting = true
          suspend = true // break loop
        }
        v.edgesProcessed += 1
        currentEdgeIndex = v.edgesProcessed
      }

      if (!suspend && v.edgesProcessed == edges.size) {
        v.completed = true
        if (debugLog) log.info(s"$v - completed and notifying waiters")
        notifyWaiters(v)
      }
    }

    def evaluateGraphOf(start: PNodeTask, startCost: Int): Unit = {
      bfsQ = new JArrayDeque[State]()
      enqueue(stateOf(start), startCost)

      def evalAllInBFS(): Unit = {
        while (!bfsQ.isEmpty) {
          if (debugQueue) printQueue()
          val v = bfsQ.removeFirst()
          if (debugLog) log.info(s"$v - removed from BFS queue")
          if (debugQueue) printQueue()

          if (v.infoId == costlyNode.id) {
            costlyNodeEvaluating += v
            if (debugLog) log.info(s"$v - added costly node to costlyNodeEvaluating")
          } else
            executeNode(v)
        }
      }

      def evalAllInCostlyNodes(): Unit = {
        val nodes = costlyNodeEvaluating
        if (debugLog) log.info(s"Costly Nodes: [${nodes.mkString(", ")}]")
        costlyNodeEvaluating = new ArrayBuffer[State]() // Prepare new hang out place
        if (debugLog) log.info(s"maxScaling is currently $maxScaling, there are ${nodes.size} nodes evaluating at once")
        maxScaling = Math.max(maxScaling, nodes.size)
        if (debugLog) log.info(s"set maxScaling to $maxScaling")
        var i = 0
        while (i < nodes.size) {
          val v = nodes(i)
          // Attribute the entire cost (+1) in the beginning to the "start" of the costly node
          // Also avoids over-counting on continuation
          if (v.cost == v.entryCost)
            v.cost += 1
          if (debugLog) log.info(s"$v - incremented cost to ${v.cost}")
          executeNode(v)
          i += 1
        }
      }

      while (!bfsQ.isEmpty || costlyNodeEvaluating.nonEmpty) {
        evalAllInBFS()
        evalAllInCostlyNodes()
      }
    }

    def createMasksFor(tasksOfInterest: ArrayBuffer[PNodeTask]): Unit = {
      var i = 0
      while (i < tasksOfInterest.size) {
        val state = stateOf(tasksOfInterest(i))
        state.reachableCostlyFunctions = 1 << i
        i += 1
      }
    }

    /** Reset visited index in PNT! Do not forget to call this! */
    def resetVisited(): Unit = {
      var i = 0
      while (i < visitedNodes.size) {
        visitedNodes(i).visitedID = 0
        i += 1
      }
      states = new Array[State](8)
      visitedNodes.clear()
    }
  }

  /** Warning! Assumes entries are sorted by firstStartTime. */
  private def groupGraphEntryPoints(entries: ArrayBuffer[PNodeTask]): ArrayBuffer[PNodeTask] = {
    def getGroupTimeInterval(group: ArrayBuffer[PNodeTask]): (Long, Long) = {
      if (group.isEmpty)
        (Long.MaxValue, Long.MinValue)
      else if (config.groupByEarliestTask)
        (group(0).firstStartTime, group(0).completedTime)
      else
        (group.map(_.firstStartTime).min, group.map(_.completedTime).max)
    }

    val groups = ArrayBuffer[ArrayBuffer[PNodeTask]]()
    var grp = ArrayBuffer[PNodeTask]()
    val it = entries.iterator
    while (it.hasNext) {
      val task = it.next()
      val (grpStartTime, grpEndTime) = getGroupTimeInterval(grp)
      if (!task.isActive(grpStartTime, grpEndTime)) {
        if (grp.nonEmpty)
          groups += grp
        grp = ArrayBuffer[PNodeTask]()
      }
      grp += task
    }

    if (grp.nonEmpty)
      groups += grp

    val r = ArrayBuffer[PNodeTask]()
    val itGrp = groups.iterator
    while (itGrp.hasNext) {
      val grp = itGrp.next()
      if (grp.size == 1)
        r += grp.head
      else {
        val grpTask = new PNodeTaskRecorded(Int.MinValue)
        grpTask.info = PNodeTask.syncEntryGroupInfo
        grpTask.callees = EdgeList.newSeqOps(Int.MaxValue)
        grpTask.firstStartTime = Long.MaxValue
        grpTask.completedTime = Long.MinValue
        r += grpTask

        val it = grp.iterator
        while (it.hasNext) {
          val task = it.next()
          grpTask.callees.add(task)
          grpTask.firstStartTime = Math.min(grpTask.firstStartTime, task.firstStartTime)
          grpTask.completedTime = Math.max(grpTask.completedTime, task.completedTime)
        }
      }
    }
    r
  }

  private def isUIInternalNode(entry: PNodeTask): Boolean = {
    val internals = Set(NodeTaskInfo.UITrack, evaluate, evaluateImm, update)
    internals.map(_.nodeName).contains(entry.nodeName)
  }

  private def sourceEntriesAsSyncBranches(visitor: Visitor, entry: PNodeTask): Array[SyncBranch] = {
    if (entry.isInternal && !isUIInternalNode(entry)) {
      val r = ArrayBuffer.empty[SyncBranch]
      // internal nodes like schedulerAsyncCallback may have no callees
      if (entry.callees ne null) {
        val it = entry.callees.withoutEnqueues()
        while (it.hasNext) {
          val child = it.next()
          val state = visitor.stateOf(child)
          if (state.reachableCostlyFunctions != 0)
            r += new SyncBranch(child, state.reachableCostlyFunctions)
        }
      }
      r.toArray
    } else
      Array.empty[SyncBranch]
  }

  def minimumUnitCost(costFunctionPnti: PNodeTaskInfo, costTasks: JArrayList[PNodeTask]): MinimumUnitCostReport = {
    val tasksOfInterest = ArrayBuffer(costTasks.asScala: _*)

    val orgGraphEntryPoints = NodeGraphVisitor.sortedGraphEntryPointsOf(costTasks)
    val graphEntryPoints = if (config.groupByTime) groupGraphEntryPoints(orgGraphEntryPoints) else orgGraphEntryPoints
    val results = new ArrayBuffer[MinimumUnitCostResult]()
    var maxScaling = 0
    var lastCost = 0
    val visitor = new Visitor(costFunctionPnti)
    visitor.createMasksFor(tasksOfInterest)
    try {
      val it = graphEntryPoints.iterator
      while (it.hasNext) {
        val entry = it.next()
        visitor.evaluateGraphOf(entry, lastCost)
        val state = visitor.stateOf(entry)
        val sourceEntries = sourceEntriesAsSyncBranches(visitor, entry)
        results += createResult(entry, state, visitor, sourceEntries)
        maxScaling = Math.max(maxScaling, visitor.maxScaling)
        visitor.maxScaling = 0
        visitor.syncBranches.clear()
        if (config.statsPerEntry) {
          visitor.resetVisited()
          visitor.createMasksFor(tasksOfInterest)
        } else
          lastCost = state.cost
      }
    } finally {
      visitor.resetVisited()
    }
    val totalCost = if (config.statsPerEntry) 0 else lastCost
    new MinimumUnitCostReport(costFunctionPnti, config.statsPerEntry, results, tasksOfInterest, maxScaling, totalCost)
  }

  def minimumUnitCost(reader: OGTraceReader, costFunction: NodeName): MinimumUnitCostReport = {
    val costFunctionPnti = findInfoByName(reader, costFunction)
    if (costFunctionPnti eq null) {
      log.error(s"Couldn't find task $costFunction")
      MinimumUnitCostReport.empty
    } else {
      val costTasks = getTasksByPnti(reader, costFunctionPnti, config.minTime, config.maxTime)
      minimumUnitCost(costFunctionPnti, costTasks)
    }
  }

  private def createResult(task: PNodeTask, state: State, visitor: Visitor, branches: Array[SyncBranch]) =
    new MinimumUnitCostResult(
      task,
      state.reachableCostlyFunctions,
      state.cost,
      visitor.maxScaling,
      visitor.syncPointsReport,
      branches)
}
