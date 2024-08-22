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

import optimus.graph.OGTraceReader
import optimus.graph.diagnostics.PNodeTask
import optimus.graph.diagnostics.PNodeTaskInfo
import optimus.graph.diagnostics.PNodeTaskInfoUtils

import scala.collection.mutable.ArrayBuffer

object NodeStacksBFS {

  private[recipes] def elide(pnti: PNodeTaskInfo, skipNonCacheable: Boolean): Boolean = {
    if (pnti == null) return false
    (skipNonCacheable && !pnti.getCacheable) || pnti.isProfilerProxy
  }

  private def addNodes(r: PNodeTaskInfoGrp, pnti: PNodeTaskInfo, tasks: util.Collection[PNodeTask]): Unit = {
    if (r.groupedNodes eq null)
      r.groupedNodes = new ArrayBuffer[PNodeTask]()

    val it = tasks.iterator()
    while (it.hasNext) {
      val task = it.next()
      if (task.infoId == pnti.id) {
        r.groupedNodes += task
        r.count += 1
      }
    }
  }

  private def addLevelOfGroupForTask(r: PNodeTaskInfoGrp, allInfos: Array[PNodeTaskInfo], task: PNodeTask): Unit = {
    val parents = task.callers
    if (parents eq null) return

    val parentsIt = parents.iterator()

    while (parentsIt.hasNext) {
      val parent = parentsIt.next

      val parentPnti = allInfos(parent.infoId)
      if (elide(parentPnti, skipNonCacheable = false)) {
        addLevelOfGroupForTask(r, allInfos, parent)
      } else {
        var pntiGrp = r.childrenMap.get(parent.infoId)
        if (pntiGrp eq null) {
          pntiGrp = new PNodeTaskInfoGrp(parentPnti, r.level + 1)
          pntiGrp.groupedNodes = new ArrayBuffer[PNodeTask]()
          r.childrenMap.put(parent.infoId, pntiGrp)
        }
        pntiGrp.groupedNodes += parent
        pntiGrp.count += 1
        pntiGrp.selfTime += parent.selfTime
      }
    }
  }

  private def addLevelOfGroups(r: PNodeTaskInfoGrp, allInfos: Array[PNodeTaskInfo]): Unit = {
    val it = r.groupedNodes.iterator
    while (it.hasNext)
      addLevelOfGroupForTask(r, allInfos, it.next())

    r.trimOneLevel()
  }

  private def addLevelOfGroupsForChildren(r: PNodeTaskInfoGrp, allInfos: Array[PNodeTaskInfo]): Unit = {
    val it = r.getChildren.iterator
    while (it.hasNext) {
      val parent = it.next()
      addLevelOfGroups(parent, allInfos)
    }
  }

  def buildUpParentsStacksWithCompare(
      org: OGTraceReader,
      orgTop: PNodeTaskInfo,
      cmp: OGTraceReader,
      cmpTop: PNodeTaskInfo): PNodeTaskInfoGrp = {
    val oTaskInfos = org.getAllTaskInfosCombined
    val orgTasks = org.getRawTasks
    val orgGrp = new PNodeTaskInfoGrp(orgTop, 0)
    addNodes(orgGrp, orgTop, orgTasks)
    addLevelOfGroups(orgGrp, oTaskInfos)

    val cTaskInfos = cmp.getAllTaskInfosCombined
    val cmpTasks = cmp.getRawTasks
    val cmpGrp = new PNodeTaskInfoGrp(cmpTop, 0)
    addNodes(cmpGrp, cmpTop, cmpTasks)
    addLevelOfGroups(cmpGrp, cTaskInfos)

    def goMerge(orgGrp: PNodeTaskInfoGrp, cmpGrp: PNodeTaskInfoGrp): Unit = {
      orgGrp.cmpGrp = cmpGrp
      orgGrp.linkUpChildrenGroups()
      orgGrp.sortByDiffInEdgeCount()

      var topOrg = orgGrp
      var topCmp = orgGrp.cmpGrp
      while (topOrg.getChildren.nonEmpty) {
        // At this point topOrg/topCmp parent groups are set, nodes that went into those groups
        addLevelOfGroupsForChildren(topOrg, oTaskInfos)
        addLevelOfGroupsForChildren(topCmp, cTaskInfos)

        topOrg.linkUpChildrenGroups()
        topOrg.sortByDiffInEdgeCount()
        topOrg = topOrg.getChildren.head
        topCmp = topOrg.cmpGrp
      }
    }

    goMerge(orgGrp, cmpGrp)

    // Current code because it is only visiting the top difference does not need (or wants) to
    // mark previously visited nodes (they can be visited too early by another branch) but if one is to change
    // this, we would need to clean up the visitedID, so just uncomment the code below
    // Clean-up
    // def resetVisits(it: util.Iterator[PNodeTask]): Unit = while (it.hasNext) it.next().visitedID = 0
    // resetVisits(orgTasks.iterator())
    // resetVisits(cmpTasks.iterator())
    orgGrp
  }

}
