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
package optimus.profiler.recipes

import java.util
import java.util.{ArrayDeque => JArrayDeque}
import java.util.{ArrayList => JArrayList}
import java.util.{Arrays => JArrays}
import java.util.{Iterator => JIterator}

import optimus.graph.NodeTaskInfo
import optimus.graph.NodeTrace
import optimus.graph.PropertyNode
import optimus.graph.diagnostics.NodeName
import optimus.graph.diagnostics.PNodeTask
import optimus.platform.Tweak
import optimus.profiler.recipes.NodeGraphVisitor.State
import optimus.profiler.recipes.NodeGraphVisitor.Observer

import scala.jdk.CollectionConverters._
import scala.collection.mutable.ArrayBuffer
import optimus.core.CoreHelpers.between

object NodeGraphVisitor {

  class Observer {
    var stopVisiting: Boolean = _ // Set to true to stop traversal
    def beforeAddingEdge(task: PNodeTask): Boolean = false // By default any vertex we saw we won't visit again
    def visit(task: PNodeTask): Unit = {} // Just continue adding relatives to be processed
  }
  class State(var pass: Int, var count: Int)

  // TaskA and TaskB are ordered already
  private def overlapAncestor(taskA: PNodeTask, taskB: PNodeTask, parent: PNodeTask): Boolean = {
    //  ancestor:       |------------------|
    //  children:  ----------|   |-----------
    between(parent.firstStartTime, parent.completedTime, taskA.completedTime) &&
    between(parent.firstStartTime, parent.completedTime, taskB.firstStartTime)
  }

  def commonParent(_taskA: PNodeTask, _taskB: PNodeTask): PNodeTask = {
    val (taskA, taskB) = if (_taskA.firstStartTime < _taskB.firstStartTime) (_taskA, _taskB) else (_taskB, _taskA)
    val visitor = new NodeGraphVisitor(visitParents = true)

    // Just visit all parents and mark them as visited in pass 1
    visitor.bfs(
      taskA,
      new Observer {
        override def visit(task: PNodeTask): Unit = visitor(task).pass = 1
      })

    var commonParent: PNodeTask = null
    visitor.bfs(
      taskB,
      new Observer {
        override def visit(task: PNodeTask): Unit = visitor(task).pass = 2
        override def beforeAddingEdge(task: PNodeTask): Boolean = {
          val state = visitor(task)
          if (state.pass == 1 && overlapAncestor(taskA, taskB, task)) {
            stopVisiting = true
            commonParent = task
            false
          } else
            state.pass != 2 // It's OK to revisit from pass 1
        }
      }
    )

    if (commonParent ne null) {
      println("Common parent:  " + NodeName.nameAndSource(commonParent))
      val pnt = commonParent
      val children = pnt.getCallees
      while (children.hasNext) {
        val child = children.next()
        val pass = visitor(child).pass
        if (pass > 0)
          println("  " + pass + ". " + NodeName.nameAndSource(child))
      }
    }

    visitor.resetVisited() // Cleanup
    commonParent // Return computed commonParent if any was found
  }

  def topParents(tasks: Seq[PNodeTask]): ArrayBuffer[PNodeTask] = {
    val r = new ArrayBuffer[PNodeTask]()
    val visitor = new NodeGraphVisitor(visitParents = true)
    val observer = new Observer {
      override def visit(task: PNodeTask): Unit = if (isEntryPoint(task)) r += task
    }

    for (task <- tasks) { visitor.bfs(task, observer) }

    visitor.resetVisited()
    r.sortBy(_.firstStartTime)
  }

  final class NodeToUTrackToInvalidate(val utrack: PropertyNode[_]) {
    val nodes = new ArrayBuffer[PNodeTask]()
    var invalidatingTweak: Tweak = _ // tweak that caused invalidation and recomputing of node
  }

  def nodeToUTrackToInvalidate(ntsks: ArrayBuffer[PNodeTask]): ArrayBuffer[NodeToUTrackToInvalidate] = {
    val visitor = new NodeGraphVisitor(visitParents = true)
    val utrackToNUI = new util.IdentityHashMap[PropertyNode[_], NodeToUTrackToInvalidate]()
    // Just visit all parents and collect utracks
    val taskIt = ntsks.iterator
    while (taskIt.hasNext) {
      val ntsk = taskIt.next()
      visitor.bfs(
        ntsk,
        new Observer {
          override def visit(pnt: PNodeTask): Unit = {
            if (pnt.isUITrack) {
              val task = pnt.getTask
              val utrack = task.asInstanceOf[PropertyNode[_]]
              var nui = utrackToNUI.get(utrack)
              if (nui eq null) {
                nui = new NodeToUTrackToInvalidate(utrack)
                utrackToNUI.put(utrack, nui)
              }
              nui.nodes += ntsk
            }
          }
        }
      )
      visitor.resetVisited() // Cleanup
    }

    val invalidates = NodeTrace.getInvalidates
    val it = invalidates.iterator
    while (it.hasNext) {
      val inv = it.next()
      val utracksIt = inv.getUTracks.iterator
      while (utracksIt.hasNext) {
        val utrack = utracksIt.next()
        val nui = utrackToNUI.get(utrack)
        if (nui ne null) nui.invalidatingTweak = inv.tweak
      }
    }

    ArrayBuffer(utrackToNUI.values.asScala.toSeq: _*)
  }

  /**
   * Walk graph using parent edges to find entry points. A task is an entry point if it has no callers (and is not the
   * start node itself) and actually enqueued something else, or it has only one caller (start)
   */
  private def isEntryPoint(task: PNodeTask): Boolean = {
    val callers = task.getCallers
    // with xsft some proxies never actually enqueue a real node because they are speculative and never requested
    // note this condition probably holds for non-proxies too
    if (task.isProxy && !task.hasEnqueueEdges)
      false
    else {
      // Make this faster by identifying start node instead of comparing to name
      (callers.isEmpty && NodeTaskInfo.Start.nodeName.name != task.nodeName.name) ||
      (callers.size == 1 && NodeTaskInfo.Start.nodeName.name == callers.get(0).nodeName.name)
    }
  }

  /** return graph entry points sorted by firstStartTime */
  def sortedGraphEntryPointsOf(costTasks: JArrayList[PNodeTask]): ArrayBuffer[PNodeTask] = {
    val r = new ArrayBuffer[PNodeTask]()
    val visitor = new NodeGraphVisitor(visitParents = true)
    val it = costTasks.iterator()
    while (it.hasNext) {
      visitor.bfs(
        it.next,
        new Observer {
          override def visit(task: PNodeTask): Unit = if (isEntryPoint(task)) r += task
        })
    }
    visitor.resetVisited()
    r.sortBy(_.firstStartTime)
  }
}

/** visitParents = true visits parents otherwise visit children */
class NodeGraphVisitor(var visitParents: Boolean) {
  private val visitedNodes = ArrayBuffer[PNodeTask]()
  private var associatedData: Array[State] = new Array[State](10)
  private var bfsQ: JArrayDeque[PNodeTask] = _
  private var observer: Observer = _

  /** Allows to associate additional data while walking */
  def apply(v: PNodeTask): State = {
    val visitID = v.visitedID
    if (visitID >= associatedData.length)
      associatedData = JArrays.copyOf(associatedData, Math.max(visitID + 64, associatedData.length * 2))
    var data = associatedData(visitID)
    if (data == null) {
      data = new State(0, 0)
      associatedData(visitID) = data
    }
    data
  }

  /** Returns true if the node was not visited before */
  private def markVisited(v: PNodeTask): Boolean = {
    val pnt = v
    if ((pnt eq null) || pnt.visitedID > 0) false
    else {
      visitedNodes += v
      pnt.visitedID = visitedNodes.size
      true
    }
  }

  private def relativesOf(pnt: PNodeTask): JIterator[PNodeTask] =
    if (visitParents) pnt.getCallers.iterator() else pnt.getCallees

  private def addRelatives(v: PNodeTask): Unit = {
    val pnt = v
    if (pnt ne null) {
      val relatives = relativesOf(pnt)
      while (relatives.hasNext) {
        val task = relatives.next()
        if (markVisited(task) || observer.beforeAddingEdge(task))
          bfsQ.addLast(task)
      }
    }
  }

  /** Reset visited index in PNT! Do not forget to call this! */
  def resetVisited(): Unit = {
    var i = 0
    while (i < visitedNodes.size) {
      visitedNodes(i).visitedID = 0
      i += 1
    }
    visitedNodes.clear()
  }

  def bfs(start: PNodeTask, observer: Observer): Unit = {
    this.observer = observer
    bfsQ = new JArrayDeque[PNodeTask]()
    bfsQ.add(start)
    markVisited(start)
    while (bfsQ.size() > 0 && !observer.stopVisiting) {
      val task = bfsQ.removeFirst()
      observer.visit(task)
      if (!observer.stopVisiting)
        addRelatives(task)
    }
  }
}
