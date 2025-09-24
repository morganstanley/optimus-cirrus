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
package optimus.platform.pickling

import optimus.core.CoreHelpers
import optimus.graph.OGSchedulerContext
import optimus.graph.Settings
import optimus.interception.StrictEquality

import java.util.concurrent.atomic.AtomicIntegerArray

/**
 * The motivation for this class is to apply interning to where its fruitful.
 * The main idea is that values to be interned are fields of classes and whether or not interning is useful
 * is dependent on what the field is rather than the class of the value we are considering for interning.
 * The class takes the number of fields and expects the caller to refer to the fields by an index.
 * when maybeIntern is called, the number of maybeIntern calls for the provided index is incremented.
 * For a set number of calls (controlled by Settings.interningObservationThreshold) the hit/query ratio
 * is tracked and if it remains above Settings.hitRatioForInterning. the values for that index continue to be interned.
 * if the ratio is below this setting, interning is disabled for that index.
 * Interning can be fully turned off by setting Settings.hitRatioForInterning to a negative value.
 */
class StatsBasedInterner(fieldCount: Int) {
  private val stats = new AtomicIntegerArray(fieldCount * 2) // Pair of integers, first is queries second is hits
  private val profiler =
    if (Settings.profileInterningStats) new StatsProfilerImpl(fieldCount) else NoopStatsProfiler

  // TODO (OPTIMUS-78287): - See note referencing this JIRA in ReflectivePicklingImpl.unpickleFill regarding the need
  // for this check and when it can be removed
  private def isInNonInterningScope: Boolean = {
    val ec = OGSchedulerContext._TRACESUPPORT_unsafe_current()
    ec != null && ec.scenarioStack().picklingInterningDisabled
  }

  /**
   * Intern the value based on the statistics of the property corresponding to the provided
   * positional index
   */
  def maybeIntern(o: AnyRef, index: Int): AnyRef = {
    if (Settings.hitRatioForInterning < 0.0 || o.getClass.isPrimitive) o
    else {
      val queries = stats.incrementAndGet(index * 2)
      val hits = stats.get(index * 2 + 1)
      val worthInterning = hits.toDouble / queries > Settings.hitRatioForInterning
      profiler.newDecision(index, queries, worthInterning)
      if ((queries < Settings.interningObservationThreshold || worthInterning) && !isInNonInterningScope) {
        val interned = CoreHelpers.intern(o)
        if (interned ne o) stats.incrementAndGet(index * 2 + 1)
        interned
      } else o
    }
  }

  /**
   * Intern each value within the array based on the statistics of the property corresponding to the array index
   * position of the value
   */
  def maybeIntern(a: Array[AnyRef]): Array[AnyRef] = {
    var i = 0
    while (i < a.length) {
      val v = a(i)
      if (v != null && !v.getClass.isPrimitive) {
        a(i) = maybeIntern(v, i)
      }
      i += 1
    }
    a
  }

  def dumpStats(i: Int, sb: StringBuilder): Unit = {
    val queries = stats.get(i * 2)
    val hits = stats.get(i * 2 + 1)
    sb.append(",")
    sb.append(queries)
    sb.append(",")
    sb.append(hits)
    profiler.dumpStats(i, sb)
  }
}

private[platform] abstract class StatsProfiler {
  def newDecision(index: Int, queries: Int, worthInterning: Boolean): Unit = ()
  def dumpStats(index: Int, sb: StringBuilder): Unit = ()
}

private[platform] object StatsProfiler {
  val observeForFlipsAt: Int = 100
}

private[platform] final class StatsProfilerImpl(val args: Int) extends StatsProfiler {
  // Non-atomic here for now. These stats are not used for any decisions in the code
  // it's just to aid dev work.
  private val sameDecisionCnt = new Array[Int](args)
  private val lastDecision = new Array[Boolean](args)
  private val numFlips = new Array[Int](args)
  override def newDecision(index: Int, queries: Int, worthInterning: Boolean): Unit = {
    if (worthInterning != lastDecision(index)) {
      lastDecision(index) = worthInterning
      sameDecisionCnt(index) = queries
      if (queries > StatsProfiler.observeForFlipsAt) numFlips(index) += 1
    }
  }
  override def dumpStats(index: Int, sb: StringBuilder): Unit = {
    sb.append(",")
    sb.append(sameDecisionCnt(index))
    sb.append(",")
    sb.append(numFlips(index))
    sb.append(",")
    sb.append(lastDecision(index))
  }
}

object NoopStatsProfiler extends StatsProfiler

object NoopStatsBasedInterner extends StatsBasedInterner(0) {
  override def maybeIntern(o: AnyRef, index: Int): AnyRef = o
  override def maybeIntern(a: Array[AnyRef]): Array[AnyRef] = a
  override def dumpStats(i: Int, sb: StringBuilder): Unit = ()
}

object StatsBasedInterner {
  StrictEquality.ensureLoaded()
  def apply(propCount: Int, scope: Settings.InterningScope): StatsBasedInterner = {
    if (Settings.interningEnabledFor.contains(scope) && Settings.hitRatioForInterning >= 0.0)
      new StatsBasedInterner(propCount)
    else NoopStatsBasedInterner
  }
}
