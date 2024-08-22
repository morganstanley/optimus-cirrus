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
package optimus.profiler.ui

import optimus.graph.diagnostics.pgo.PNTIStats

import java.awt.Color
import optimus.profiler.ui.HotspotsTable.c_cacheBenefitToolTip
import optimus.profiler.ui.HotspotsTable.c_cacheTimeToolTip

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object DecisionDifferenceTable {
  private[ui] val c_name = new TableColumnString[PNTIStats]("Property/Node", 300, false) {
    override def valueOf(row: PNTIStats): String = row.name
    override def toolTip = "Property/Node"
    override def getHeaderColor: Color = HotspotsTable.nodeCacheSectionColor
  }

  private[ui] val c_cacheMisses = new TableColumnLong[PNTIStats]("CacheMisses", 100) {
    override def valueOf(row: PNTIStats): Long = row.cacheMiss
    override def toolTip = "Cache Misses"
    override def getHeaderColor: Color = HotspotsTable.nodeCacheSectionColor
  }
  private[ui] val c_cacheHit = new TableColumnLong[PNTIStats]("CacheHits", 100) {
    override def valueOf(row: PNTIStats): Long = row.cacheHit
    override def toolTip = "Cache hits"
    override def getHeaderColor: Color = HotspotsTable.nodeTimingSectionColor
  }
  private[ui] val c_cacheBenefit = new TableColumnLong[PNTIStats]("CacheBenefit", 100) {
    override def valueOf(row: PNTIStats): Long = row.cacheBenefit
    override def toolTip: String = c_cacheBenefitToolTip
    override def getHeaderColor: Color = HotspotsTable.nodeCacheSectionColor
  }
  private[ui] val c_cacheTime = new TableColumnLong[PNTIStats]("CacheTime", 100) {
    override def valueOf(row: PNTIStats): Long = row.cacheTime
    override def toolTip: String = c_cacheTimeToolTip
    override def getHeaderColor: Color = HotspotsTable.cacheTimingSectionColor
  }
  private[ui] val c_noHits = new TableColumnString[PNTIStats]("NoHits", 100) {
    override def valueOf(row: PNTIStats): String = row.noHits.toString
    override def toolTip = "Were there no cache hits?"
    override def getHeaderColor: Color = HotspotsTable.nodeInfoSectionColor
  }
  private[ui] val c_noBenefit = new TableColumnString[PNTIStats]("NoBenefit", 100) {
    override def valueOf(row: PNTIStats): String = row.noBenefit.toString
    override def toolTip = "Was the benefit for caching under a certain threshold?"
    override def getHeaderColor: Color = HotspotsTable.nodeInfoSectionColor
  }
  private[ui] val c_lowHitRatio = new TableColumnString[PNTIStats]("LowHitRatio", 100) {
    override def valueOf(row: PNTIStats): String = row.lowHitRatio.toString
    override def toolTip = "Was the ratio of cache hits to cache requests below a certain threshold?"
    override def getHeaderColor: Color = HotspotsTable.nodeInfoSectionColor
  }
  val columnsRequiredForOptconfDecisionDiff: ArrayBuffer[TableColumn[PNTIStats]] = ArrayBuffer(
    c_name,
    c_cacheMisses,
    c_cacheHit,
    c_cacheBenefit,
    c_cacheTime,
    c_noHits,
    c_noBenefit,
    c_lowHitRatio
  )
}

final class DecisionDifferenceTable extends NPTable[PNTIStats] {
  setViewColumns()

  def setViewColumns(): Unit = setView(DecisionDifferenceTable.columnsRequiredForOptconfDecisionDiff)

}
