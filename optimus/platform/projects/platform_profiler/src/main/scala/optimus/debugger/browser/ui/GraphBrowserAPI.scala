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
package optimus.debugger.browser.ui

import optimus.graph.CancellationScope
import optimus.graph.Node
import optimus.graph.NodeTask
import optimus.graph.NodeTaskInfo
import optimus.graph.NodeTrace
import optimus.graph.OGSchedulerContext
import optimus.graph.OGTrace
import optimus.graph.RecordedTweakables
import optimus.graph.TweakTreeNode
import optimus.graph.XSDiffer
import optimus.graph.cache.CauseProfiler
import optimus.graph.diagnostics.GraphDebuggerViewable
import optimus.graph.diagnostics.PNodeTask
import optimus.graph.diagnostics.trace.OGTraceMode
import optimus.platform.AdvancedUtils
import optimus.platform.inputs.GraphInputConfiguration
import optimus.platform.storable.Entity
import optimus.profiler.DebuggerUI
import optimus.profiler.ui.DbgPrintSource

import java.util.{ArrayList => JArrayList}
import javax.swing.JComponent
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

private[optimus] object GraphBrowserAPI {
  def generateNodeReviewForDiff(lst: JArrayList[_ <: PNodeTask]): ArrayBuffer[NodeReview] = {
    val r = new ArrayBuffer[NodeReview](lst.size())
    for (task <- lst.asScala) {
      task.getTask match {
        case _: Node[_] => r += new NodeReview(task)
        case _          =>
      }
    }
    r
  }

  def invalidateAndRecompute(orig: NodeTask, clearCache: Boolean): PNodeTask = {
    val task = orig.cacheUnderlyingNode // never rerun a proxy node
    val nss = task.scenarioStack.withCancellationScopeRaw(CancellationScope.newScope())
    val newTask = task.prepareForExecutionIn(nss)
    if (clearCache) AdvancedUtils.clearCache(CauseProfiler, includeSI = false, includeLocal = true)

    val prevMode = OGTrace.getTraceMode
    if (prevMode == OGTraceMode.none)
      GraphInputConfiguration.setTraceMode(OGTraceMode.traceNodes)

    DebuggerUI.underStackOf(task.scenarioStack()) { OGSchedulerContext.current().runAndWait(newTask) }

    if (prevMode == OGTraceMode.none)
      GraphInputConfiguration.setTraceMode(OGTraceMode.none)

    NodeTrace.accessProfile(newTask)
  }

  def customView(entity: Entity): JComponent = entity match {
    case viewable: GraphDebuggerViewable => viewable.view
    case _                               => null
  }

  implicit class NodeInspectorImpl(val ntsk: NodeTask) extends AnyVal {
    /* Basic attributes */
    def nodeName(): String =
      nodeName(
        useEntityType = false,
        showKeys = false,
        showNodeState = false,
        simpleName = false,
        showCausalityID = false)

    def nodeName(
        useEntityType: Boolean,
        showKeys: Boolean,
        showNodeState: Boolean,
        simpleName: Boolean,
        showCausalityID: Boolean): String =
      ntsk.toPrettyName(useEntityType, showKeys, showNodeState, simpleName, showCausalityID)

    def resultAsString(truncate: Boolean = false): String =
      if (truncate) ntsk.resultAsString(DebuggerUI.maxCharsInInlineArg)
      else ntsk.resultAsString()

    def actOnResult(
        doneWithResult: Any => Unit,
        doneWithException: Throwable => Unit,
        notDone: String => Unit): Unit = {
      if (ntsk.isDoneWithException) doneWithException(ntsk.exception)
      else if (ntsk.isDoneWithResult) doneWithResult(ntsk.resultObject)
      else notDone(s"[...${ntsk.stateAsString}]")
    }

    def isInternal: Boolean = {
      val nti = ntsk.executionInfo
      nti.isInternal || (nti eq NodeTaskInfo.Default)
    }

    def scaledWallTime: Double = ntsk.getWallTime * 1e-6
    def scaledSelfTime: Double = ntsk.getSelfTime * 1e-6
  }
}

final class NodeReview(override val task: PNodeTask) extends DbgPrintSource {
  // We would be asking for these values too often, harder to compute values need to be cached
  val args: Array[AnyRef] = task.args
  private var _recordedTweakables: RecordedTweakables = _

  // Detected diffs
  var commonScenarioStack: Int = _
  var possibleReuse: Boolean = _
  val extraTweaks: mutable.Set[TweakTreeNode] = mutable.Set[TweakTreeNode]()
  val missingTweaks: mutable.Set[TweakTreeNode] = mutable.Set[TweakTreeNode]()
  val mismatchedTweaks = new ArrayBuffer[(TweakTreeNode, TweakTreeNode)]()

  private def clearAccumulatedTweakDiffs(): Unit = {
    extraTweaks.clear()
    missingTweaks.clear()
    mismatchedTweaks.clear()
  }

  def getCommonScenarioStackSize(row: NodeReview): Int =
    DebuggerUI.getCommonScenarioStackDepth(this.task.scenarioStack(), row.task.scenarioStack())

  def recordedTweakables: RecordedTweakables = {
    if (_recordedTweakables eq null) {
      val ntsk = task.getTask
      _recordedTweakables =
        if (ntsk.isXScenarioOwner)
          ntsk.scenarioStack().tweakableListener.asInstanceOf[RecordedTweakables]
        else
          DebuggerUI.collectTweaksAsTweakTreeNode(task)
    }
    _recordedTweakables
  }

  /** Recompute the difference against the provided row */
  def recomputeDiffs(row: NodeReview): Unit = {
    val ntsk = this.task.getTask
    if (row != null && ntsk != null) {
      val ntsk2 = row.task.getTask
      commonScenarioStack = getCommonScenarioStackSize(row)
      clearAccumulatedTweakDiffs()
      if (ntsk2 ne ntsk) {
        val rt1 = recordedTweakables
        val rt2 = row.recordedTweakables
        new XSDiffer(extraTweaks, missingTweaks, mismatchedTweaks, false).allXsDiff(rt1, rt2)
      }
      possibleReuse = (ntsk2 ne ntsk) && ntsk == ntsk2 && this.task.resultKey == row.task.resultKey &&
        missingTweaks.isEmpty && extraTweaks.isEmpty && mismatchedTweaks.isEmpty
    }
  }

  override def printSource(): Unit = task.printSource()

  def scaledWallTime: Double = task.wallTime * 1e-6
  def scaledSelfTime: Double = task.selfTime * 1e-6
}
