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
package optimus.graph.diagnostics

import java.util.function.Consumer
import optimus.core.needsPlugin
import optimus.graph.NodeTask
import optimus.graph.NodeTaskInfo
import optimus.graph.NodeTrace
import optimus.graph.diagnostics.pgo.Profiler
import optimus.graph.diagnostics.trace.OGEventsHotspotsWithFullTraceObserver
import optimus.graph.{NodeKey, OGTrace}
import optimus.platform.EvaluationContext
import optimus.platform.ScenarioStack
import optimus.platform.annotations.nodeLift
import optimus.platform.inputs.GraphInputConfiguration

object Debugger {
  private[optimus] val dbgBrkOnTestSucceeded = DbgPreference("dbgBrkOnTestSucceeded", default = false)
  private[optimus] val dbgBrkOnTestFailed = DbgPreference("dbgBrkOnTestFailed", default = true)
  private[optimus] val dbgBrkOnTestsCompleted = DbgPreference("dbgBrkOnTestsCompleted", default = false)

  // false by default because we don't want test behaviour to change depending on whether or not the
  // debugger console is attached - previously this was always true in TestManager
  private[optimus] val dbgResetProfileBeforeTest = DbgPreference("dbgResetProfileBeforeTest", default = false)
  private[optimus] val dbgShowAdvancedCmds = new NotifyingDbgPreference("dbgShowAdvancedCmds")

  /** supply a callback that takes a boolean indicating whether advanced commands are enabled */
  def registerAdvancedCommandChangedCallback(f: Boolean => Unit): Unit = dbgShowAdvancedCmds.addCallback(f)

  private var testDescription: String = _

  def onTestStarted(): Unit = {
    if (dbgResetProfileBeforeTest.get)
      Profiler.resetAll()
  }

  def onTestFinished(description: String): Unit = {
    val keepConsoleOpen = synchronized {
      if (testDescription != description) { // otherwise we failed the test and console is open already
        testDescription = description
        true
      } else false
    }
    if (keepConsoleOpen && dbgBrkOnTestSucceeded.get) {
      ProfilerUIControl.refresh()
      Debugger.brk(
        "Test passed, keeping debugger open since Suspend On Test Succeeded is checked.\nClick 'Go' to resume")
    }
  }

  def onTestFailed(description: String): Unit = {
    synchronized { testDescription = description } // set it here since this will be called before onTestFinished
    if (dbgBrkOnTestFailed.get) {
      ProfilerUIControl.refresh()
      Debugger.brk("Test failed, keeping debugger open since Suspend On Test Failed is checked.\nClick 'Go' to resume")
    }
  }

  def onAllTestsFinished(): Unit = {
    if (dbgBrkOnTestsCompleted.get) {
      ProfilerUIControl.refresh()
      Debugger.brk(
        "Tests completed, keeping debugger open since Suspend On All Tests Completed is checked.\nClick 'Go' to resume")
    }
  }

  def brk(msg: String): Unit = ProfilerUIControl.brk(msg)
  def brk(): Unit = ProfilerUIControl.brk()
  def screenshot(name: String): Unit = ProfilerUIControl.screenshot(name: String)

  def underStackOf[T](ssIn: ScenarioStack)(f: => T): T =
    ProfilerUIControl.underStackOf(ssIn)(f)

  /**
   * User can supply in the console a callback to post process nodes as they complete when tracing is on. One thing to
   * do here is to actually mark the node to be traced with its parents This case is common enough that we supply the
   * function to do that markNodesOfPropertyToBeTracedOnCompleted However a custom callback can mark only nodes that
   * have a particular result!
   */
  def setOnNodeCompleted(observer: Consumer[NodeTask]): Unit = {
    OGEventsHotspotsWithFullTraceObserver.OnNodeCompleted = observer
  }

  /** See comments above on setOnNodeCompleted */
  def markNodesOfPropertyToBeTracedOnCompleted(nti: NodeTaskInfo): Unit = {
    OGEventsHotspotsWithFullTraceObserver.OnNodeCompleted = task => {
      if (task.executionInfo eq nti) NodeTrace.markNodeAndParentsToBeTraced(task)
    }
  }

  @nodeLift
  def showNode[T](v: T): Any = needsPlugin
  def showNode$node[T](nodeKey: NodeKey[T]): T = {
    val mode = OGTrace.getTraceMode
    try {
      val node = EvaluationContext.lookupNode(nodeKey)
      val value = node.get
      val title = s"Children of ${ProfilerUIControl.formatName(node)}"
      ProfilerUIControl.addTreeBrowserTab(title, node)
      value
    } finally {
      GraphInputConfiguration.setTraceMode(mode)
    }
  }
}
