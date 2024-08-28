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
package optimus.platform.temporalSurface.tsprofiler

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.annotation.JsonProperty
import optimus.platform.temporalSurface.TemporalSurface
import optimus.platform.temporalSurface.tsprofiler.TemporalSurfaceProfilingDataManager.asAtomicL
import optimus.platform.temporalSurface.tsprofiler.TemporalSurfaceProfilingDataManager.childrenProfOrder
import optimus.platform.temporalSurface.tsprofiler.TemporalSurfaceProfilingDataManager.initStartingValue

import scala.jdk.CollectionConverters._

final class IdentityWrapper[A](val a: A with Object) extends Serializable {
  override def hashCode: Int = System.identityHashCode(a)
  override def equals(obj: Any): Boolean =
    obj match {
      case idWrapper: IdentityWrapper[_] => a eq idWrapper.a
      case _                             => false
    }
}

final class TemporalSurfaceProfilingData(
    val ts: TemporalSurface = null,
    val visited: AtomicLong = initStartingValue,
    val hits: AtomicLong = initStartingValue,
    val dataDependantHits: AtomicLong =
      initStartingValue, // depends on what is stored in the DAL (assign individual case)
    val visitedChildren: AtomicLong = initStartingValue,
    val matchWalltime: AtomicLong = initStartingValue,
    val matcherHits: AtomicLong = initStartingValue,
    val walltime: AtomicLong = initStartingValue,
    val dataDependantWallTime: AtomicLong = initStartingValue,
    @JsonInclude(Include.NON_EMPTY)
    val children: ConcurrentHashMap[IdentityWrapper[TemporalSurface], TemporalSurfaceProfilingData] =
      new ConcurrentHashMap[IdentityWrapper[TemporalSurface], TemporalSurfaceProfilingData]())
    extends Serializable {

  @JsonProperty def getTs(): String =
    if (ts != null) ts.minimalDescription else "null"

  @JsonProperty def getChildren(): Seq[TemporalSurfaceProfilingData] = {
    val printOrder = childrenProfOrder(children, ts)

    for (currentTS <- printOrder)
      yield children.getOrDefault(new IdentityWrapper(currentTS), new TemporalSurfaceProfilingData(ts = currentTS))
  }

  def recordVisit() = visited.addAndGet(1)
  def recordHit() = hits.addAndGet(1)
  def recordMatcherHit() = matcherHits.addAndGet(1)
  def recordDataDependantHit() = dataDependantHits.addAndGet(1)
  def recordVisitedChildren() = visitedChildren.addAndGet(1)

  def recWallTime(time: Long): Unit = walltime.addAndGet(time)
  def recMatchWalltime(time: Long): Unit = matchWalltime.addAndGet(time)
  def recDataDepWalltime(time: Long): Unit = dataDependantWallTime.addAndGet(time)

  private def aggregateAsSummaryProfilingData(other: TemporalSurfaceProfilingData): (Long, Long) = {
    require(ts eq other.ts) // break if not equal - something went wrong in the way we constructed the profiling tree
    (sumVisitsTSProfTree(this) + sumVisitsTSProfTree(other), sumHitsTSProfTree(this) + sumHitsTSProfTree(other))
  }

  private def sumVisitsTSProfTree(root: TemporalSurfaceProfilingData): Long = {
    var total = root.visited.get()
    for (c <- root.children.values.asScala.toList)
      total += sumVisitsTSProfTree(c)
    total
  }

  private def sumHitsTSProfTree(root: TemporalSurfaceProfilingData): Long = {
    var total = root.hits.get()
    for (c <- root.children.values.asScala.toList)
      total += sumHitsTSProfTree(c)
    total
  }

  def aggregate(other: TemporalSurfaceProfilingData, topLevelStatsOnly: Boolean = false): TemporalSurfaceProfilingData =
    if (topLevelStatsOnly) {
      val (totalVisits, totalHits) = aggregateAsSummaryProfilingData(other)
      new TemporalSurfaceProfilingData(visited = asAtomicL(totalVisits), hits = asAtomicL(totalHits))
    } else aggregateProfilingData(other)

  private def aggregateProfilingData(other: TemporalSurfaceProfilingData): TemporalSurfaceProfilingData = {
    require(ts eq other.ts) // break if not equal - something went wrong in the way we constructed the profiling tree

    val rightChildren = other.children.keys.asScala.toArray
    // copy the right hand side TS prof
    val leftChildren = children.keys.asScala.toArray
    val prof2OnlyChildren = rightChildren.diff(leftChildren)

    val newChildren = new ConcurrentHashMap[IdentityWrapper[TemporalSurface], TemporalSurfaceProfilingData](children)
    for (rightChildTS <- prof2OnlyChildren) {
      newChildren.put(rightChildTS, other.children.get(rightChildTS))
    }

    val newTSProf = new TemporalSurfaceProfilingData(
      ts = ts,
      visited = asAtomicL(visited.get() + other.visited.get()),
      hits = asAtomicL(hits.get() + other.hits.get()),
      dataDependantHits = asAtomicL(dataDependantHits.get() + other.dataDependantHits.get()),
      visitedChildren = asAtomicL(visitedChildren.get() + other.visitedChildren.get()),
      matchWalltime = asAtomicL(matchWalltime.get() + other.matchWalltime.get()),
      walltime = asAtomicL(walltime.get() + other.walltime.get()),
      matcherHits = asAtomicL(matcherHits.get() + other.matcherHits.get()),
      dataDependantWallTime = asAtomicL(dataDependantWallTime.get() + other.dataDependantWallTime.get()),
      children = newChildren
    )

    // aggregate common children using same aggregator
    val commonChildrenTS = children.keys.asScala.toArray.intersect(rightChildren)
    for (childTS <- commonChildrenTS) {
      val prof1 = children.get(childTS)
      val prof2 = other.children.get(childTS)
      newTSProf.children.put(childTS, prof1.aggregateProfilingData(prof2))
    }
    newTSProf
  }

  override def toString: String = prettyPrintTSProf()
  private def indent(lvl: Int): String = s"${"-" * lvl}"
  def prettyPrintTSProf(lvl: Int = 0): String =
    s"""${indent(lvl)} ts = ${if (ts != null) ts.minimalDescription else "null"}
       |${indent(lvl)} visited = $visited
       |${indent(lvl)} hits = $hits
       |${indent(lvl)} visitedChildren = $visitedChildren
       |${indent(lvl)} matchWalltime = $matchWalltime
       |${indent(lvl)} matcherHits = $matcherHits
       |${indent(lvl)} walltime = $walltime
       |${indent(lvl)} dataDependantHits = $dataDependantHits
       |${indent(lvl)} dataDependantWallTime = $dataDependantWallTime
       |""".stripMargin
}
