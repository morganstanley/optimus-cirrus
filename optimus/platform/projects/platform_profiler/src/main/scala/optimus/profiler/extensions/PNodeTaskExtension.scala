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
package optimus.profiler.extensions

import optimus.debugger.browser.ui.GraphBrowserAPI
import optimus.graph.diagnostics.NodeName
import optimus.graph.diagnostics.PNodeTask
import optimus.platform.util.PrettyStringBuilder
import optimus.platform.storable.Entity
import optimus.profiler.DebuggerUI

import scala.collection.mutable.ArrayBuffer

object PNodeTaskExtension {

  private def fillCalleesWithoutInternal(task: PNodeTask, r: ArrayBuffer[PNodeTask]): Unit = {
    val it = task.getCallees
    while (it.hasNext) {
      val child = it.next()
      if (!child.isInternal)
        r += child
      else fillCalleesWithoutInternal(child, r)
    }
  }

  implicit class PNodeTaskExt(val task: PNodeTask) extends AnyVal {

    /**
     * returns an approximation of a stack trace (by selecting the first caller) for use in graph browser ui
     */
    private def callersToList: ArrayBuffer[PNodeTask] = callersToList(task, new ArrayBuffer[PNodeTask])

    private def callersToList(task: PNodeTask, list: ArrayBuffer[PNodeTask]): ArrayBuffer[PNodeTask] = {
      list += task
      val callers = task.getCallers
      if (!callers.isEmpty) { callersToList(callers.get(0), list) }
      list
    }

    /** prints an approximation of a stack trace (by selecting the first caller). Internal nodes skipped */
    def printStackTrace(): Unit = {
      val callers = callersToList
      val sb = new PrettyStringBuilder
      callers foreach { tsk =>
        val skip = tsk.isInternal || tsk.isTweakNode
        if (!skip) sb.appendln(tsk.nodeName.toString)
      }
      println(sb.toString) // need the output in console, not log
    }

    def invalidateAndRecomputeNode(clearCache: Boolean): PNodeTask = {
      val asNtsk = task.getTask
      if (asNtsk != null) GraphBrowserAPI.invalidateAndRecompute(asNtsk, clearCache)
      else task
    }

    def resultEntity: Entity = task.resultKey match {
      case e: Entity       => e
      case Some(e: Entity) => e
      case _               => null
    }

    def pkg: String = task.nodeName.pkgName

    def className: String = NodeName.cleanNodeClassName(task.nodeClass)

    def formatIdAndName: String = "#" + task.id + " " + task.nodeName.toString

    def getCalleesWithoutInternal: Iterator[PNodeTask] = {
      val r = new ArrayBuffer[PNodeTask]
      fillCalleesWithoutInternal(task, r)
      r.iterator
    }

    def safeResultAsString: String = DebuggerUI.underStackOf(task.scenarioStack) {
      task.resultDisplayString
    }
  }
}
