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

import optimus.graph.diagnostics.NPTreeNode
import optimus.graph.diagnostics.PNodeTaskInfo
import java.util.{HashMap => JHashMap}

import optimus.graph.diagnostics.PNodeTask

import scala.collection.mutable.ArrayBuffer
import scala.math.Ordering

object PNodeTaskInfoGrp {
  private var PNodeTaskInfoGrpCounter = 0
  private val emptyGrp = new PNodeTaskInfoGrp(new PNodeTaskInfo(-1), 0)
  private def nextGrpId: Int = { PNodeTaskInfoGrpCounter += 1; PNodeTaskInfoGrpCounter }
}

final class PNodeTaskInfoGrp(_pnti: PNodeTaskInfo, val level: Int) extends NPTreeNode {
  val id: Int = PNodeTaskInfoGrp.nextGrpId
  var count: Long = _
  var selfTime: Long = _
  var incomingEdges: Int = _ // Used to compress graph representations
  var cmpGrp: PNodeTaskInfoGrp = PNodeTaskInfoGrp.emptyGrp // Group used for comparisons

  def diffInCount: Long = count - cmpGrp.count

  /** Returns true if complete info is available */
  def completeInfo: Boolean = _pnti ne null

  var groupedNodes: ArrayBuffer[PNodeTask] = _ // While grouping temp store...

  private[this] var _childrenMap = new JHashMap[Integer, PNodeTaskInfoGrp]() // While grouping, null after ....
  protected var children: ArrayBuffer[PNodeTaskInfoGrp] = _ // After trim()

  def childrenMap: JHashMap[Integer, PNodeTaskInfoGrp] = _childrenMap

  def pnti: PNodeTaskInfo = if (_pnti ne null) _pnti else new PNodeTaskInfo(0, "Unknown", "NA")

  override def getChildren: Iterable[PNodeTaskInfoGrp] = children
  override def hasChildren: Boolean = (children ne null) && children.nonEmpty
  override def title: String = pnti.fullName()
  override def toString: String = title

  def childrenAsMap(): JHashMap[PNodeTaskInfo, PNodeTaskInfoGrp] = {
    val r = new JHashMap[PNodeTaskInfo, PNodeTaskInfoGrp]()
    if (_childrenMap ne null)
      _childrenMap.values().forEach(v => r.put(v.pnti, v))
    else if (children ne null)
      children.foreach(v => r.put(v.pnti, v))
    r
  }

  def diffInCountIsSmall(percent: Double): Boolean = {
    Math.abs(diffInCount.toDouble / count) < percent
  }

  def trimAndSortBy[B](extractor: PNodeTaskInfoGrp => B)(implicit ord: Ordering[B]): PNodeTaskInfoGrp = {
    if (childrenMap ne null) {
      children = new ArrayBuffer[PNodeTaskInfoGrp](childrenMap.size)
      val it = childrenMap.values().iterator()
      while (it.hasNext) {
        val grp = it.next()
        grp.trimAndSortBy(extractor)
        children += grp
      }
      children = children.sortBy(extractor)
      _childrenMap = null
    }
    this
  }

  def trimAndSortByTitle(): PNodeTaskInfoGrp = trimAndSortBy(_.title)
  def trimAndSortByEdgeCount(): PNodeTaskInfoGrp = trimAndSortBy(-_.count)
  def trimAndSortByDiffInEdgeCount(): PNodeTaskInfoGrp = trimAndSortBy(_.diffInCount)

  def sortByDiffInEdgeCount(): Unit = {
    if (children ne null)
      children = children.sortBy(_.diffInCount)
  }

  def trimOneLevel(): Unit = {
    if (childrenMap ne null) {
      children = new ArrayBuffer[PNodeTaskInfoGrp](childrenMap.size)
      val it = childrenMap.values().iterator()
      while (it.hasNext) {
        val grp = it.next()
        children += grp
      }
      _childrenMap = null
    }
  }

  def linkUpChildrenGroups(): Unit = {
    val cmpMap = cmpGrp.childrenAsMap()
    val it = children.iterator
    while (it.hasNext) {
      val child = it.next()
      val cmpChild = cmpMap.get(child.pnti)
      if (cmpChild ne null)
        child.cmpGrp = cmpChild
      // Consider handling missing match better
    }
  }

  def insertCompareGroups(cmp: PNodeTaskInfoGrp): Unit = {
    this.cmpGrp = cmp
    val cmpMap = cmp.childrenAsMap()
    val it = _childrenMap.values().iterator()
    while (it.hasNext) {
      val child = it.next()
      val cmpChild = cmpMap.get(child.pnti)
      if (cmpChild ne null)
        child.insertCompareGroups(cmpChild)
      // Consider handling missing match better
    }
  }
}
