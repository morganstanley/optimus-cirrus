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
package optimus.profiler

import optimus.graph.DiagnosticSettings
import optimus.graph.JMXConnection
import optimus.graph.NodeTrace
import optimus.graph.diagnostics.GivenBlockProfile
import optimus.graph.diagnostics.PNodeInvalidate
import optimus.graph.diagnostics.PNodeTask
import optimus.graph.diagnostics.PNodeTaskInfo
import optimus.graph.diagnostics.WaitProfile
import optimus.graph.diagnostics.pgo.Profiler
import optimus.graph.diagnostics.pgo.Profiler.sstack_roots
import optimus.graph.outOfProcess.views.ScenarioStackProfileView
import optimus.graph.outOfProcess.views.ScenarioStackProfileViewHelper
import optimus.profiler.recipes.PNodeTaskInfoGrp

import java.util.prefs.Preferences
import java.util.{ArrayList => JArrayList}
import java.util.{IdentityHashMap => JIdentityHashMap}
import java.util.{Iterator => JIterator}
import scala.collection.compat._
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

object ProfilerUI {

  /** A number of tests assume no preferences are stored, this flags is to ensure that */
  val prefDisabledForTesting: Boolean = DiagnosticSettings.getBoolProperty("optimus.profiler.selfTest", false)

  /** Start off with the top level preferences that are not panel specific */
  val pref: Preferences = Preferences.userNodeForPackage(this.getClass)

  /** Public api for resetting waits */
  final def resetWaits(): Unit =
    if (DiagnosticSettings.outOfProcess) JMXConnection.graph.resetWaits() else NodeTrace.resetWaits()

  final def resetSStackUsage(): Unit = {
    def reset(seq: Iterable[GivenBlockProfile]): Unit = {
      for (p <- seq) {
        p.reset()
        reset(p.children.values().asScala)
      }
    }
    if (DiagnosticSettings.outOfProcess) JMXConnection.graph.resetSStackRoots()
    else reset(sstack_roots.values().asScala)
  }

  final def getSSRoots: Iterable[ScenarioStackProfileView] =
    ScenarioStackProfileViewHelper.createScenarioStackProfileView(Profiler.sstack_roots.values().asScala)

  final def getWaits: ArrayBuffer[WaitProfile] = NodeTrace.getWaits.asScala.to(ArrayBuffer)

  final def getInvalidates: ArrayBuffer[PNodeInvalidate] = NodeTrace.getInvalidates.asScala.to(ArrayBuffer)

  /** Collects node stack from the full node trace */
  final def getNodeStacks(all: JArrayList[PNodeTaskInfo], timeStamp: Long = 0L): Iterable[PNodeTaskInfoGrp] = {
    val allNodes = new JIdentityHashMap[AnyRef, AnyRef]() // Just to keep track of nodes we've seen
    def isCached(c: PNodeTask) = allNodes.put(c, c) ne null

    def walk(grp: PNodeTaskInfoGrp, callees: JIterator[PNodeTask]): Unit = {
      while (callees.hasNext) {
        val c = callees.next()
        val profileID = c.infoId
        val pnti = all.get(profileID)

        if ((pnti ne null) && (timeStamp == 0 || (c.firstStartTime <= timeStamp && timeStamp <= c.completedTime))) {
          var pntiGroup = grp.childrenMap.get(profileID)
          if (pntiGroup eq null) {
            pntiGroup = new PNodeTaskInfoGrp(pnti, grp.level + 1)
            grp.childrenMap.put(profileID, pntiGroup)
          }
          pntiGroup.count += 1
          pntiGroup.selfTime += c.selfTime
          val cached = isCached(c)
          if (!cached) {
            val cc = c.getCallees
            if (cc.hasNext)
              walk(pntiGroup, cc)
          }
        }
      }
    }

    val root = new PNodeTaskInfoGrp(new PNodeTaskInfo(-1, "", "root"), 0)
    for (r <- NodeTrace.getRoots.asScala) {
      val callees = r.getCallees
      if (callees ne null)
        walk(root, callees)
    }
    List(root)
  }
}
