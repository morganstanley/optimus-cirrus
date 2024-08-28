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
package optimus.platform.stats

import java.util.concurrent.atomic.{AtomicLong, AtomicInteger}

/**
 * Singleton to RECORD all object allocation information.
 *
 * WARNING: If the JAVA AGENT is not running, and if the ApplicationStatsAllocationAgent within this Java Agent has not
 * been activated, then there will be nothing here!
 *
 * The actual instrumentation is enabled within those packages, and they in turn call this 'sample' method to report
 * allocations at runtime. This structure is intended to de-couple the agent-side code and allow this code to compile
 * with the normal production libraries alone.
 *
 * If isSupported == false, then there is not even an Agent.
 *
 * If isSupported == true, but totalOfAll.longValue == 0, then it was never turned on
 *
 * It has to be done this way so that all of this mainline code will compile (and run) correctly whether or not the Java
 * Agent (allocation.jar) is part of the classpath.
 *
 * NOTICE: This does NOT attempt to synchronize and eliminate counting some objects which we allocate ourselves within
 * these traits themselves. It also allows the 'counts' to be incremented (by other threads) while we are processing.
 *
 * The assumption is that these are de-minimus compared to allocations by the process being monitored.
 */
object AppStatsObjectAllocation {
  private val appStats = AppStats

  /** Set to TRUE if the Java Agent is running, and ApplicationStatsAllocationAgent was started */
  val isSupported =
    try {
      Class.forName("com.google.monitoring.runtime.instrumentation.AllocationRecorder")
      true
    } catch {
      case _: Throwable => false
    }

  /** If this value is ZERO, then this was never activated */
  val allocTotal = new AtomicLong

  val allocMap = new scala.collection.mutable.HashMap[String, AtomicLong]

  // Add the TOTAL to the map, so it also gets reported
  allocMap("$$TOTAL$$") = allocTotal

  // Since we allocate an AtomicLong (and the Map allocates a holder object) for a new entry
  // Mainly to prevent infinite recursion, not so much for accuracy of the counts
  val recursionDepth = new AtomicInteger(0)

  def sampleAllocation(count: Int, desc: String, newObj: AnyRef, size: Long): Unit = {
    if (recursionDepth.get == 0) {
      allocTotal.incrementAndGet
      allocMap.get(desc) match {
        case Some(atomic) => atomic.incrementAndGet
        case None =>
          synchronized {
            recursionDepth.incrementAndGet
            allocMap.get(desc) match {
              case Some(atomic) => atomic.incrementAndGet
              case None         => allocMap(desc) = new AtomicLong(1)
            }
            recursionDepth.decrementAndGet
          }
      }
    }
  }

  def totalAllObjects: Long = allocTotal.longValue

  /** Get the list of counts sorted by the NAME */
  def byName(
      mapIn: scala.collection.mutable.Map[String, AtomicLong],
      descending: Boolean = false): List[(String, AtomicLong)] = synchronized { byName(mapIn.toList, descending) }

  def byName(lst: List[(String, AtomicLong)], descending: Boolean): List[(String, AtomicLong)] =
    lst.sortWith((A: (String, AtomicLong), B: (String, AtomicLong)) =>
      if (A._1.compareTo(B._1) < 0) !descending else descending)

  /** Get the list of counts sorted by the COUNT (asc or dsc) */
  def byCount(
      mapIn: scala.collection.mutable.Map[String, AtomicLong],
      descending: Boolean = false): List[(String, AtomicLong)] = synchronized { byCount(mapIn.toList, descending) }

  def byCount(lst: List[(String, AtomicLong)], descending: Boolean): List[(String, AtomicLong)] =
    lst.sortWith((A: (String, AtomicLong), B: (String, AtomicLong)) =>
      if (A._2.get < B._2.get) !descending else descending)

  // Put out JSON array with the object names as the variable names, counts as data
  def jsonArray(lst: List[(String, AtomicLong)]): String = {
    val sb = new StringBuilder(4196)

    lst.foreach {
      case (name, atomic) => {
        if (sb.length > 0) sb.append(',')
        sb.append('{')
          .append(AppStatsControl.u.jsonQuoted(name))
          .append(':')
          .append(atomic.longValue)
          .append('}')
      }
    }
    '[' + sb.toString + ']'
  }
}

/**
 * Record total object allocations, put out special JSON segment with resulting information.
 *
 * Records only the TOTAL for all object allocations as-of the request for JSON.
 */
trait AppStatsAllocationsTotal extends AppStats {
  private val appAlloc = AppStatsObjectAllocation

  override def jsonSegment(optionalData: AnyRef*): String = {
    val ourJSON = quoted("nowObjAllocations") + " : " +
      (if (appAlloc.isSupported) {
         if (appAlloc.totalAllObjects == 0) quoted("Supported but never Activated")
         else allocCounts
       } else quoted("NotSupported"))

    super.jsonSegment(optionalData: _*) + ourJSON + ','
  }
  private def allocCounts: String = appAlloc.jsonArray(appAlloc.byCount(appAlloc.allocMap, true))
}

/**
 * Record object allocations made between a probeStart....probeEnd sequence.
 *
 * These will be accumulated over multiple probeStart...probeEnd sequences.
 */
trait AppStatsAllocationsProbed extends AppStats {
  private val appAlloc = AppStatsObjectAllocation
  // Total up counts through multiple probeStart...probeEnd sequences
  val mapTotals = new scala.collection.mutable.HashMap[String, AtomicLong]
  // Save the values at probeStart. Don't need AtomicInteger since read-only after save
  val mapStart = new scala.collection.mutable.HashMap[String, Long]

  override protected def doTraitProbe(isStart: Boolean, lastProbe: Boolean, target: AnyRef*) = {
    super.doTraitProbe(isStart, lastProbe, target: _*)

    if (isStart) {
      // Replace entry with new count if already present
      appAlloc.allocMap.foreach { case (name, atomic) => mapStart(name) = atomic.get }
    } else {
      appAlloc.allocMap.foreach {
        case (name, atomic) => {
          var cnt = atomic.get
          // may or may not exist in the 'start' map. Compute 'cnt' as diff if exists
          mapStart.get(name) match {
            case Some(entry) => { cnt -= entry }
            case None        => { /* no entry, just use cnt */ }
          }
          // and may or may not exist in the totals map
          mapTotals.get(name) match {
            case Some(atomic) => atomic.addAndGet(cnt)
            case None         => mapTotals(name) = new AtomicLong(cnt)
          }
        }
      }
    }
  }
  override def jsonSegment(optionalData: AnyRef*): String = {
    val warn =
      if (isProbing)
        quoted("ttlObjAllocationsWARN") + ':' + quoted(
          "Accumulating allocation counts, but probeEnd has not been called") + ','
      else ""
    val ourJSON = quoted("ttlObjAllocations") + ':' +
      (if (appAlloc.isSupported) {
         if (appAlloc.totalAllObjects == 0) quoted("Supported but never Activated")
         else allocCounts
       } else quoted("NotSupported"))

    super.jsonSegment(optionalData: _*) + warn + ourJSON + ','
  }
  private def allocCounts: String = {
    var noZeros: List[(String, AtomicLong)] = Nil
    mapTotals.foreach { case (name, atomic) => { if (atomic.get > 0) noZeros ::= (name, atomic) } }
    val lst = appAlloc.byCount(noZeros, true)
    appAlloc.jsonArray(lst)
  }
}
