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

import optimus.graph.OGTraceReader
import optimus.graph.diagnostics.PNodeTaskInfo
import java.util.{ArrayList => JArrayList}
import java.util.{HashMap => JHashMap}

import optimus.profiler.ui.GraphConsole

import scala.collection.mutable.ArrayBuffer

object FindJumpsInCache {
  type PNTIList = JArrayList[PNodeTaskInfo]

  private def cacheAccessDiffIsLarge(diffPercent: Double, a: PNodeTaskInfo, b: PNodeTaskInfo): Boolean = {
    Math.abs(a.cacheAccesses - b.cacheAccesses).toDouble / a.cacheAccesses > diffPercent
  }

  private def startsDiffIsLarge(diffPercent: Double, a: PNodeTaskInfo, b: PNodeTaskInfo): Boolean = {
    Math.abs(a.start - b.start).toDouble / a.start > diffPercent
  }

  private class Worker(
      diffPercent: Double,
      minStarts: Int,
      minCacheTime: Long,
      orgReader: OGTraceReader,
      compareToReader: OGTraceReader) {
    private[this] val org = orgReader.getHotspots
    private[this] val compareTo = compareToReader.getHotspots

    private[this] val hashOfCompareTo = cacheableToHash(compareTo)
    private[this] val largeDiffs = ArrayBuffer[(PNodeTaskInfo, PNodeTaskInfo)]()

    /** Pull out cacheable PNTI into HashMap */
    private def cacheableToHash(pntis: JArrayList[PNodeTaskInfo]): JHashMap[PNodeTaskInfo, PNodeTaskInfo] = {
      val hash = new JHashMap[PNodeTaskInfo, PNodeTaskInfo]()
      val itCompare = pntis.iterator()
      while (itCompare.hasNext) {
        val pnti = itCompare.next()
        if (pnti.getCacheable)
          hash.put(pnti, pnti)
      }
      hash
    }

    private def includeInComparison(pnti: PNodeTaskInfo): Boolean = {
      pnti.getCacheable && pnti.start >= minStarts && pnti.cacheTime >= minCacheTime
    }

    private def findDiffs(): Unit = {
      val itOrg = org.iterator()
      while (itOrg.hasNext) {
        val pnti = itOrg.next()
        if (includeInComparison(pnti)) {
          val compareTo = hashOfCompareTo.get(pnti)
          if (compareTo eq null)
            GraphConsole.out("Unexpected missing task in the compare-to set " + pnti)
          else if (cacheAccessDiffIsLarge(diffPercent, pnti, compareTo))
            largeDiffs += pnti -> compareTo
        }
      }
    }

    private def findShockingDiff(grp: PNodeTaskInfoGrp): Unit = {
      val it = grp.getChildren.iterator
      while (it.hasNext) {
        val child = it.next()
        val orgPNTI = child.pnti
        val cmpPNTI = hashOfCompareTo.get(orgPNTI)
        if ((cmpPNTI ne null) && !startsDiffIsLarge(diffPercent, orgPNTI, cmpPNTI))
          return
        findShockingDiff(child)
      }
    }

    def report(): PNodeTaskInfoGrp = {
      findDiffs()
      if (largeDiffs.isEmpty) {
        GraphConsole.out("No big differences")
        null
      } else {
        val sortedDiffs = largeDiffs.sortBy { case (org, _) => -org.cacheAccesses }
        GraphConsole.out(s"Found ${sortedDiffs.size} diffs")
        val (topOrg, topCmp) = sortedDiffs.head
        GraphConsole.out("Top diff:" + topOrg)

        // Step 2
        // val grpOrg = NodeStacks.getParentsAsStack(orgReader, topOrg, compareToReader, topCmp, true)
        /// grpOrg.trimAndSortByDiffInEdgeCount()
        //  findShockingDiff(grp)
        NodeStacksBFS.buildUpParentsStacksWithCompare(orgReader, topOrg, compareToReader, topCmp)
      }
    }
  }

  def findJumpInCacheUsage(
      diffPercent: Double,
      minStarts: Int,
      minCacheTime: Long,
      org: OGTraceReader,
      compareTo: OGTraceReader): PNodeTaskInfoGrp = {
    val topGrp = new Worker(diffPercent, minStarts, minCacheTime, org, compareTo).report()
    topGrp.open = true
    topGrp
  }

}
